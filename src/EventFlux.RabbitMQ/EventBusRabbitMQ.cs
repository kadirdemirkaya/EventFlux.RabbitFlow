using EventFlux.Abstractions;
using EventFlux.RabbitMQ.Abstractions;
using EventFlux.RabbitMQ.Context;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System.Net.Sockets;
using System.Text;

namespace EventFlux.RabbitMQ
{
    public class EventBusRabbitMQ : SubscribeProcessEvent, IEventBroker, IDisposable, IAsyncDisposable
    {
        const string appName = "EventFlux_RabbitFlow";
        const string RetryCountHeader = "x-retry-count";
        const string PublishSequenceHeader = "x-dotnet-pub-seq-no";

        private readonly IPersistenceConnection _persistentConnection;
        private readonly RabbitFlowOptions _options;
        private readonly string _serviceName;
        private readonly string _exchangeName;
        private readonly AsyncRetryPolicy _publishPolicy;
        private readonly SemaphoreSlim _consumerLock = new SemaphoreSlim(1, 1);

        private readonly Dictionary<string, string> _eventQueues = new Dictionary<string, string>();
        private readonly Dictionary<string, HashSet<string>> _eventExchanges = new Dictionary<string, HashSet<string>>();
        private readonly Dictionary<string, string> _consumerTags = new Dictionary<string, string>();
        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, string> _queueEvents = new System.Collections.Concurrent.ConcurrentDictionary<string, string>();
        private readonly CancellationTokenSource _disposeCts = new CancellationTokenSource();
        private readonly System.Collections.Concurrent.ConcurrentBag<IChannel> _publishChannels = new System.Collections.Concurrent.ConcurrentBag<IChannel>();
        private IChannel? _consumerChannel;
        private bool _disposed;

        internal Task ConsumerRecovery { get; private set; } = Task.CompletedTask;

        internal int MaxIdlePublishChannels { get; set; } = 16;

        internal Task SubscriptionRemoval { get; private set; } = Task.CompletedTask;

        internal Func<int, TimeSpan> ConsumerRestoreDelay { get; set; } = attempt => TimeSpan.FromSeconds(Math.Min(Math.Pow(2, attempt), 30));

        public EventBusRabbitMQ(IPersistenceConnection persistentConnection,
            ILogger<IEventBroker> logger,
            IServiceScopeFactory serviceScope,
            IEventBusSubscriptionsManager subsManager,
            IEventBus eventBus,
            IEventFluxContextAccessor contextAccessor,
            string serviceName,
            int retryCount = 5)
            : this(persistentConnection, logger, serviceScope, subsManager, contextAccessor, serviceName,
                new RabbitFlowOptions { RetryCount = retryCount })
        {
        }

        public EventBusRabbitMQ(IPersistenceConnection persistentConnection,
            ILogger<IEventBroker> logger,
            IServiceScopeFactory serviceScope,
            IEventBusSubscriptionsManager subsManager,
            IEventFluxContextAccessor contextAccessor,
            string serviceName,
            RabbitFlowOptions options)
            : base(logger, subsManager, serviceScope, appName, contextAccessor)
        {
            _persistentConnection = persistentConnection ?? throw new ArgumentNullException(nameof(persistentConnection));
            _serviceName = serviceName ?? throw new ArgumentNullException(nameof(serviceName));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _options.Validate();
            _exchangeName = _serviceName;

            _publishPolicy = Policy.Handle<BrokerUnreachableException>()
                .Or<SocketException>()
                .Or<AlreadyClosedException>()
                .Or<InvalidOperationException>(_ => !_persistentConnection.IsConnected)
                .Or<PublishException>(ex => !ex.IsReturn)
                .WaitAndRetryAsync(_options.RetryCount, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (ex, time) =>
                {
                    _logger.LogWarning(ex, "Could not publish event after {TimeOut}s ({ExceptionMessage})", $"{time.TotalSeconds:n1}", ex.Message);
                });

            _subsManager.OnEventRemoved += SubsManager_OnEventRemoved;

            if (_options.PublisherConfirms && _persistentConnection is not PersistenceConnection)
            {
                _logger.LogWarning("PublisherConfirms is enabled with a custom {ConnectionType}; confirmations only apply if it implements CreateChannelAsync(CreateChannelOptions, CancellationToken)", _persistentConnection.GetType().Name);
            }
        }

        public Task PublishAsync(IEventRequest @event)
            => PublishAsync(@event, _exchangeName, CancellationToken.None);

        public Task PublishAsync(IEventRequest @event, CancellationToken cancellationToken)
            => PublishAsync(@event, _exchangeName, cancellationToken);

        public Task PublishAsync(IEventRequest @event, string exchangeName)
            => PublishAsync(@event, exchangeName, CancellationToken.None);

        public async Task PublishAsync(IEventRequest @event, string exchangeName, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(@event);

            var eventName = @event.GetType().Name;
            var targetExchange = string.IsNullOrWhiteSpace(exchangeName) ? _exchangeName : exchangeName;
            var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(@event));
            var headers = CreateContextHeaders();

            await _publishPolicy.ExecuteAsync(async ct =>
            {
                await EnsureConnectedAsync(ct).ConfigureAwait(false);

                var channel = await RentPublishChannelAsync(ct).ConfigureAwait(false);
                var reusable = false;
                try
                {
                    await channel.ExchangeDeclareAsync(exchange: targetExchange, type: ExchangeType.Direct, cancellationToken: ct).ConfigureAwait(false);

                    var properties = new BasicProperties
                    {
                        DeliveryMode = DeliveryModes.Persistent,
                        Headers = headers
                    };

                    try
                    {
                        await channel.BasicPublishAsync(
                            exchange: targetExchange,
                            routingKey: eventName,
                            mandatory: true,
                            basicProperties: properties,
                            body: body,
                            cancellationToken: ct).ConfigureAwait(false);
                    }
                    catch (PublishException ex) when (ex.IsReturn)
                    {
                        _logger.LogWarning("{EventName} was published to {Exchange} but no queue is bound to it, the broker dropped the message", eventName, targetExchange);
                    }

                    reusable = true;
                }
                finally
                {
                    await ReturnPublishChannelAsync(channel, reusable).ConfigureAwait(false);
                }
            }, cancellationToken).ConfigureAwait(false);
        }

        public async Task SubscribeAsync<T, TH>(string exchangeName = null)
            where T : IEventRequest
            where TH : IEventHandler<T>
        {
            var eventName = _subsManager.GetEventKey<T>();

            if (!IsSubscribed(eventName, typeof(TH)))
            {
                _subsManager.AddSubscription<T, TH>();
            }

            await InternalSubscriptionAsync(eventName, exchangeName).ConfigureAwait(false);
        }

        public void Unsubscribe<T, TH>()
            where T : IEventRequest
            where TH : IEventHandler<T>
        {
            _subsManager.RemoveSubscription<T, TH>();
        }

        /// <summary>Cancels every consumer so no new messages are delivered. In-flight messages finish normally.</summary>
        public async Task StopConsumingAsync(CancellationToken cancellationToken = default)
        {
            await _consumerLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (_consumerChannel is { IsOpen: true })
                {
                    foreach (var consumerTag in _consumerTags.Values.Where(t => !string.IsNullOrEmpty(t)))
                    {
                        await _consumerChannel.BasicCancelAsync(consumerTag, cancellationToken: cancellationToken).ConfigureAwait(false);
                    }
                }

                _consumerTags.Clear();
            }
            finally
            {
                _consumerLock.Release();
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _disposeCts.Cancel();

            _subsManager.OnEventRemoved -= SubsManager_OnEventRemoved;
            DisposePublishChannelsAsync().GetAwaiter().GetResult();
            _consumerChannel?.Dispose();
            _subsManager.Clear();
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed) return;
            _disposed = true;
            _disposeCts.Cancel();

            _subsManager.OnEventRemoved -= SubsManager_OnEventRemoved;
            await DisposePublishChannelsAsync().ConfigureAwait(false);
            if (_consumerChannel != null)
            {
                await _consumerChannel.DisposeAsync().ConfigureAwait(false);
            }
            _subsManager.Clear();
        }

        internal async Task HandleDeliveryAsync(IChannel channel, BasicDeliverEventArgs eventArgs)
        {
            var eventName = string.IsNullOrEmpty(eventArgs.Exchange) && _queueEvents.TryGetValue(eventArgs.RoutingKey, out var queueEvent)
                ? queueEvent
                : eventArgs.RoutingKey;
            var message = Encoding.UTF8.GetString(eventArgs.Body.Span);
            var cancellationToken = eventArgs.CancellationToken;

            try
            {
                await ProcessEvent(eventName, message, WithoutTransportHeaders(eventArgs.BasicProperties.Headers), cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                _logger.LogWarning("Processing of {EventName} was cancelled, requeueing the message", eventName);
                await channel.BasicNackAsync(eventArgs.DeliveryTag, multiple: false, requeue: true).ConfigureAwait(false);
                return;
            }
            catch (Exception ex)
            {
                await HandleFailureAsync(channel, eventArgs, eventName, ex).ConfigureAwait(false);
                return;
            }

            await channel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false).ConfigureAwait(false);
        }

        private async Task HandleFailureAsync(IChannel channel, BasicDeliverEventArgs eventArgs, string eventName, Exception exception)
        {
            if (_options.LimitsDeliveries)
            {
                await HandleLimitedFailureAsync(channel, eventArgs, eventName, exception).ConfigureAwait(false);
                return;
            }

            if (_options.OnFailure == RabbitFlowFailureMode.DeadLetter)
            {
                try
                {
                    await PublishToDeadLetterAsync(channel, eventArgs, eventName, exception).ConfigureAwait(false);
                    await channel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false).ConfigureAwait(false);

                    _logger.LogError(exception, "Error processing {EventName}, the message was moved to the dead-letter queue", eventName);
                    return;
                }
                catch (Exception deadLetterException)
                {
                    _logger.LogError(deadLetterException, "Could not dead-letter {EventName}, requeueing the message", eventName);
                }
            }
            else
            {
                _logger.LogError(exception, "Error processing {EventName}, requeueing the message", eventName);
            }

            await channel.BasicNackAsync(eventArgs.DeliveryTag, multiple: false, requeue: true).ConfigureAwait(false);
        }

        private async Task HandleLimitedFailureAsync(IChannel channel, BasicDeliverEventArgs eventArgs, string eventName, Exception exception)
        {
            var retries = GetRetryCount(eventArgs.BasicProperties.Headers);
            var attempt = retries + 1;
            var queueName = $"{_serviceName}_{eventName}";

            try
            {
                if (attempt < _options.MaxDeliveryAttempts)
                {
                    await PublishRetryAsync(channel, eventArgs, queueName, retries + 1).ConfigureAwait(false);
                    await channel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false).ConfigureAwait(false);

                    _logger.LogWarning(exception, "Error processing {EventName} (attempt {Attempt} of {MaxAttempts}), retrying the message", eventName, attempt, _options.MaxDeliveryAttempts);
                    return;
                }

                await PublishToDeadLetterAsync(channel, eventArgs, eventName, exception).ConfigureAwait(false);
                await channel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false).ConfigureAwait(false);

                _logger.LogError(exception, "Error processing {EventName} (attempt {Attempt} of {MaxAttempts}), the message was moved to the dead-letter queue", eventName, attempt, _options.MaxDeliveryAttempts);
                return;
            }
            catch (Exception retryException)
            {
                _logger.LogError(retryException, "Could not retry or dead-letter {EventName}, requeueing the message", eventName);
            }

            await channel.BasicNackAsync(eventArgs.DeliveryTag, multiple: false, requeue: true).ConfigureAwait(false);
        }

        private async Task PublishRetryAsync(IChannel channel, BasicDeliverEventArgs eventArgs, string queueName, int retryCount)
        {
            var headers = CopyHeaders(eventArgs);
            headers[RetryCountHeader] = retryCount;

            var properties = new BasicProperties
            {
                DeliveryMode = DeliveryModes.Persistent,
                Headers = headers
            };

            var routingKey = queueName;
            if (_options.DelaysRedelivery)
            {
                properties.Expiration = ((long)_options.RedeliveryDelay.TotalMilliseconds).ToString(System.Globalization.CultureInfo.InvariantCulture);
                routingKey = RabbitFlowOptions.GetRetryQueue(queueName);
            }

            await channel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: routingKey,
                mandatory: true,
                basicProperties: properties,
                body: eventArgs.Body).ConfigureAwait(false);
        }

        private static Dictionary<string, object?> CopyHeaders(BasicDeliverEventArgs eventArgs)
        {
            var headers = eventArgs.BasicProperties.Headers is { } original
                ? original.Where(h => !IsBrokerDeathHeader(h.Key)).ToDictionary(h => h.Key, h => h.Value)
                : new Dictionary<string, object?>();

            if (!headers.ContainsKey(RetryCountHeader) || !headers.ContainsKey("x-original-exchange"))
            {
                headers["x-original-exchange"] = eventArgs.Exchange;
                headers["x-original-routing-key"] = eventArgs.RoutingKey;
            }

            return headers;
        }

        private static bool IsBrokerDeathHeader(string key)
            => key == "x-death" || key == PublishSequenceHeader || key.StartsWith("x-first-death-", StringComparison.Ordinal) || key.StartsWith("x-last-death-", StringComparison.Ordinal);

        private static IDictionary<string, object?>? WithoutTransportHeaders(IDictionary<string, object?>? headers)
        {
            if (headers == null || !headers.ContainsKey(PublishSequenceHeader)) return headers;

            return headers.Where(h => h.Key != PublishSequenceHeader).ToDictionary(h => h.Key, h => h.Value);
        }

        private Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken = default)
            => CreateChannelAsync(_options.PublisherConfirms, cancellationToken);

        private async Task<IChannel> RentPublishChannelAsync(CancellationToken cancellationToken)
        {
            while (_publishChannels.TryTake(out var pooled))
            {
                if (pooled.IsOpen) return pooled;

                await DisposeChannelAsync(pooled).ConfigureAwait(false);
            }

            return await CreateChannelAsync(cancellationToken).ConfigureAwait(false);
        }

        private async Task ReturnPublishChannelAsync(IChannel channel, bool reusable)
        {
            if (reusable && !_disposed && channel.IsOpen && _publishChannels.Count < MaxIdlePublishChannels)
            {
                _publishChannels.Add(channel);

                if (!_disposed) return;
                if (!_publishChannels.TryTake(out var taken)) return;
                channel = taken;
            }

            await DisposeChannelAsync(channel).ConfigureAwait(false);
        }

        private async Task DisposePublishChannelsAsync()
        {
            while (_publishChannels.TryTake(out var channel))
            {
                await DisposeChannelAsync(channel).ConfigureAwait(false);
            }
        }

        private async Task DisposeChannelAsync(IChannel channel)
        {
            try
            {
                await channel.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Could not dispose a RabbitMQ publish channel");
            }
        }

        private Task<IChannel> CreateChannelAsync(bool confirms, CancellationToken cancellationToken = default)
            => confirms
                ? _persistentConnection.CreateChannelAsync(new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true), cancellationToken)
                : _persistentConnection.CreateChannelAsync();

        internal static int GetRetryCount(IDictionary<string, object?>? headers)
        {
            if (headers == null || !headers.TryGetValue(RetryCountHeader, out var value)) return 0;

            return value switch
            {
                int i => Math.Max(i, 0),
                long l => (int)Math.Clamp(l, 0, int.MaxValue),
                short sh => Math.Max((int)sh, 0),
                byte b => b,
                byte[] bytes when int.TryParse(Encoding.UTF8.GetString(bytes), out var parsed) => Math.Max(parsed, 0),
                string text when int.TryParse(text, out var parsed) => Math.Max(parsed, 0),
                _ => 0
            };
        }

        private async Task PublishToDeadLetterAsync(IChannel channel, BasicDeliverEventArgs eventArgs, string eventName, Exception exception)
        {
            var headers = CopyHeaders(eventArgs);

            headers["x-exception-type"] = exception.GetType().FullName;
            headers["x-exception-message"] = exception.Message;

            var properties = new BasicProperties
            {
                DeliveryMode = DeliveryModes.Persistent,
                Headers = headers
            };

            await channel.BasicPublishAsync(
                exchange: _options.GetDeadLetterExchange(_serviceName),
                routingKey: eventName,
                mandatory: true,
                basicProperties: properties,
                body: eventArgs.Body).ConfigureAwait(false);
        }

        private bool IsSubscribed(string eventName, Type handlerType)
            => _subsManager.HasSubscriptionsForEvent(eventName)
               && _subsManager.GetHandlersForEvent(eventName).Any(s => s.HandlerType == handlerType);

        private Dictionary<string, object?>? CreateContextHeaders()
        {
            var context = _contextAccessor.Context;
            if (context == null || context.Items.Count == 0) return null;

            return context.Items.ToDictionary(item => item.Key, item => (object?)item.Value);
        }

        private async Task EnsureConnectedAsync(CancellationToken cancellationToken)
        {
            if (!_persistentConnection.IsConnected)
            {
                await _persistentConnection.TryConnectAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task InternalSubscriptionAsync(string eventName, string? exchangeName)
        {
            var targetExchange = string.IsNullOrWhiteSpace(exchangeName) ? _exchangeName : exchangeName;
            var queueName = $"{_serviceName}_{eventName}";

            await _consumerLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (!_eventExchanges.TryGetValue(eventName, out var exchanges))
                {
                    exchanges = new HashSet<string>();
                    _eventExchanges[eventName] = exchanges;
                }

                if (exchanges.Contains(targetExchange) && _consumerTags.ContainsKey(queueName)) return;

                await EnsureConnectedAsync(CancellationToken.None).ConfigureAwait(false);

                var channel = await _persistentConnection.CreateChannelAsync().ConfigureAwait(false);
                await using (channel.ConfigureAwait(false))
                {
                    await DeclareTopologyAsync(channel, eventName, queueName, new[] { targetExchange }).ConfigureAwait(false);
                }

                exchanges.Add(targetExchange);
                _eventQueues[eventName] = queueName;

                if (!_consumerTags.ContainsKey(queueName))
                {
                    await StartConsumerAsync(queueName).ConfigureAwait(false);
                }
            }
            finally
            {
                _consumerLock.Release();
            }
        }

        private async Task DeclareTopologyAsync(IChannel channel, string eventName, string queueName, IReadOnlyCollection<string> exchanges)
        {
            foreach (var exchange in exchanges)
            {
                await channel.ExchangeDeclareAsync(exchange: exchange, type: ExchangeType.Direct).ConfigureAwait(false);
            }

            await channel.QueueDeclareAsync(queue: queueName,
                                           durable: true,
                                           exclusive: false,
                                           autoDelete: false,
                                           arguments: null).ConfigureAwait(false);

            foreach (var exchange in exchanges)
            {
                await channel.QueueBindAsync(queue: queueName,
                                            exchange: exchange,
                                            routingKey: eventName).ConfigureAwait(false);
            }

            if (_options.UsesDeadLetter)
            {
                await DeclareDeadLetterAsync(channel, queueName, eventName).ConfigureAwait(false);
            }

            if (_options.DelaysRedelivery)
            {
                await channel.QueueDeclareAsync(queue: RabbitFlowOptions.GetRetryQueue(queueName),
                                               durable: true,
                                               exclusive: false,
                                               autoDelete: false,
                                               arguments: new Dictionary<string, object?>
                                               {
                                                   ["x-dead-letter-exchange"] = string.Empty,
                                                   ["x-dead-letter-routing-key"] = queueName
                                               }).ConfigureAwait(false);
            }

            _queueEvents[queueName] = eventName;
        }

        private async Task DeclareDeadLetterAsync(IChannel channel, string queueName, string eventName)
        {
            var deadLetterExchange = _options.GetDeadLetterExchange(_serviceName);
            var deadLetterQueue = RabbitFlowOptions.GetDeadLetterQueue(queueName);

            await channel.ExchangeDeclareAsync(exchange: deadLetterExchange, type: ExchangeType.Direct, durable: true).ConfigureAwait(false);
            await channel.QueueDeclareAsync(queue: deadLetterQueue, durable: true, exclusive: false, autoDelete: false, arguments: null).ConfigureAwait(false);
            await channel.QueueBindAsync(queue: deadLetterQueue, exchange: deadLetterExchange, routingKey: eventName).ConfigureAwait(false);
        }

        private async Task StartConsumerAsync(string queueName)
        {
            var channel = await GetConsumerChannelAsync().ConfigureAwait(false);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += Consumer_ReceivedAsync;

            var consumerTag = await channel.BasicConsumeAsync(queue: queueName, autoAck: false, consumer: consumer).ConfigureAwait(false);
            _consumerTags[queueName] = consumerTag ?? string.Empty;
        }

        private async Task<IChannel> GetConsumerChannelAsync()
        {
            if (_consumerChannel is { IsOpen: true }) return _consumerChannel;

            await EnsureConnectedAsync(CancellationToken.None).ConfigureAwait(false);

            _logger.LogTrace("Creating RabbitMQ consumer channel");

            var channel = await CreateChannelAsync(_options.PublisherConfirms || _options.UsesDeadLetter).ConfigureAwait(false);

            if (_options.PrefetchCount > 0)
            {
                await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: _options.PrefetchCount, global: false).ConfigureAwait(false);
            }

            channel.CallbackExceptionAsync += OnConsumerCallbackExceptionAsync;
            channel.ChannelShutdownAsync += OnConsumerChannelShutdownAsync;

            _consumerChannel = channel;
            return channel;
        }

        private Task OnConsumerCallbackExceptionAsync(object sender, CallbackExceptionEventArgs ea)
        {
            if (_disposed) return Task.CompletedTask;

            _logger.LogWarning(ea.Exception, "Recreating RabbitMQ consumer channel");

            ConsumerRecovery = Task.Run(() => RestartConsumersAsync(sender));
            return Task.CompletedTask;
        }

        private Task OnConsumerChannelShutdownAsync(object sender, ShutdownEventArgs reason)
        {
            if (_disposed || reason.Initiator == ShutdownInitiator.Application) return Task.CompletedTask;

            var channelOnly = _persistentConnection.IsConnected;
            if (!channelOnly && _persistentConnection is not PersistenceConnection { RecoversAutomatically: false })
            {
                return Task.CompletedTask;
            }

            _logger.LogWarning("RabbitMQ consumer channel was shut down ({ReplyText}), restarting consumers", reason.ReplyText);

            ConsumerRecovery = Task.Run(() => RestartConsumersAsync(sender));
            return Task.CompletedTask;
        }

        private async Task RestartConsumersAsync(object closedChannel)
        {
            await _consumerLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_disposed || _consumerChannel == null) return;
                if (!ReferenceEquals(closedChannel, _consumerChannel) && _consumerChannel.IsOpen) return;

                DropConsumerChannel();
            }
            finally
            {
                _consumerLock.Release();
            }

            for (var attempt = 1; !_disposed; attempt++)
            {
                try
                {
                    await RestoreConsumersAsync().ConfigureAwait(false);
                    return;
                }
                catch (Exception ex) when (!_disposed)
                {
                    var delay = ConsumerRestoreDelay(attempt);
                    _logger.LogError(ex, "Could not recreate RabbitMQ consumers, retrying in {Delay}s", $"{delay.TotalSeconds:n1}");

                    try
                    {
                        await Task.Delay(delay, _disposeCts.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                }
                catch (Exception)
                {
                    return;
                }
            }
        }

        private async Task RestoreConsumersAsync()
        {
            await _consumerLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_disposed) return;

                if (_consumerChannel is not { IsOpen: true })
                {
                    DropConsumerChannel();
                }

                await EnsureConnectedAsync(_disposeCts.Token).ConfigureAwait(false);

                var channel = await _persistentConnection.CreateChannelAsync().ConfigureAwait(false);
                await using (channel.ConfigureAwait(false))
                {
                    foreach (var (eventName, queueName) in _eventQueues)
                    {
                        IReadOnlyCollection<string> exchanges = _eventExchanges.TryGetValue(eventName, out var bound) ? bound : Array.Empty<string>();
                        await DeclareTopologyAsync(channel, eventName, queueName, exchanges).ConfigureAwait(false);
                    }
                }

                foreach (var queueName in _eventQueues.Values.Distinct().Where(q => !_consumerTags.ContainsKey(q)))
                {
                    await StartConsumerAsync(queueName).ConfigureAwait(false);
                }
            }
            finally
            {
                _consumerLock.Release();
            }
        }

        private void DropConsumerChannel()
        {
            if (_consumerChannel != null)
            {
                _consumerChannel.CallbackExceptionAsync -= OnConsumerCallbackExceptionAsync;
                _consumerChannel.ChannelShutdownAsync -= OnConsumerChannelShutdownAsync;

                try
                {
                    _consumerChannel.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Could not dispose the previous RabbitMQ consumer channel");
                }
            }

            _consumerChannel = null;
            _consumerTags.Clear();
        }

        private Task Consumer_ReceivedAsync(object sender, BasicDeliverEventArgs eventArgs)
            => HandleDeliveryAsync(((AsyncEventingBasicConsumer)sender).Channel, eventArgs);

        private void SubsManager_OnEventRemoved(object? sender, string eventName)
        {
            SubscriptionRemoval = RemoveSubscriptionAsync(eventName);
        }

        private async Task RemoveSubscriptionAsync(string eventName)
        {
            try
            {
                await _consumerLock.WaitAsync().ConfigureAwait(false);
                try
                {
                    if (!_eventQueues.TryGetValue(eventName, out var queueName)) return;
                    if (_subsManager.HasSubscriptionsForEvent(eventName)) return;

                    await EnsureConnectedAsync(CancellationToken.None).ConfigureAwait(false);

                    var channel = await _persistentConnection.CreateChannelAsync().ConfigureAwait(false);
                    await using var channelScope = channel.ConfigureAwait(false);

                    if (_eventExchanges.TryGetValue(eventName, out var exchanges))
                    {
                        foreach (var exchange in exchanges)
                        {
                            await channel.QueueUnbindAsync(queue: queueName, exchange: exchange, routingKey: eventName).ConfigureAwait(false);
                        }
                        _eventExchanges.Remove(eventName);
                    }

                    if (_consumerTags.Remove(queueName, out var consumerTag)
                        && !string.IsNullOrEmpty(consumerTag)
                        && _consumerChannel is { IsOpen: true })
                    {
                        await _consumerChannel.BasicCancelAsync(consumerTag).ConfigureAwait(false);
                    }

                    if (_options.DeleteQueueOnUnsubscribe)
                    {
                        await channel.QueueDeleteAsync(queue: queueName).ConfigureAwait(false);

                        if (_options.DelaysRedelivery)
                        {
                            await channel.QueueDeleteAsync(queue: RabbitFlowOptions.GetRetryQueue(queueName)).ConfigureAwait(false);
                        }
                    }

                    _eventQueues.Remove(eventName);
                }
                finally
                {
                    _consumerLock.Release();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Could not remove the RabbitMQ subscription for {EventName}", eventName);
            }
        }
    }
}
