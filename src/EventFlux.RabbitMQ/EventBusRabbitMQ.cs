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

        private readonly IPersistenceConnection _persistentConnection;
        private readonly RabbitFlowOptions _options;
        private readonly string _serviceName;
        private readonly string _exchangeName;
        private readonly AsyncRetryPolicy _publishPolicy;
        private readonly SemaphoreSlim _consumerLock = new SemaphoreSlim(1, 1);

        private readonly Dictionary<string, string> _eventQueues = new Dictionary<string, string>();
        private readonly Dictionary<string, HashSet<string>> _eventExchanges = new Dictionary<string, HashSet<string>>();
        private readonly Dictionary<string, string> _consumerTags = new Dictionary<string, string>();
        private IChannel? _consumerChannel;
        private bool _disposed;

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
            _exchangeName = _serviceName;

            _publishPolicy = Policy.Handle<BrokerUnreachableException>()
                .Or<SocketException>()
                .Or<AlreadyClosedException>()
                .WaitAndRetryAsync(_options.RetryCount, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (ex, time) =>
                {
                    _logger.LogWarning(ex, "Could not publish event after {TimeOut}s ({ExceptionMessage})", $"{time.TotalSeconds:n1}", ex.Message);
                });

            _subsManager.OnEventRemoved += SubsManager_OnEventRemoved;
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
                EnsureConnected();

                var channel = await _persistentConnection.CreateChannelAsync().ConfigureAwait(false);
                await using (channel.ConfigureAwait(false))
                {
                    await channel.ExchangeDeclareAsync(exchange: targetExchange, type: ExchangeType.Direct, cancellationToken: ct).ConfigureAwait(false);

                    var properties = new BasicProperties
                    {
                        DeliveryMode = DeliveryModes.Persistent,
                        Headers = headers
                    };

                    await channel.BasicPublishAsync(
                        exchange: targetExchange,
                        routingKey: eventName,
                        mandatory: true,
                        basicProperties: properties,
                        body: body,
                        cancellationToken: ct).ConfigureAwait(false);
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

            _subsManager.OnEventRemoved -= SubsManager_OnEventRemoved;
            _consumerChannel?.Dispose();
            _subsManager.Clear();
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed) return;
            _disposed = true;

            _subsManager.OnEventRemoved -= SubsManager_OnEventRemoved;
            if (_consumerChannel != null)
            {
                await _consumerChannel.DisposeAsync().ConfigureAwait(false);
            }
            _subsManager.Clear();
        }

        internal async Task HandleDeliveryAsync(IChannel channel, BasicDeliverEventArgs eventArgs)
        {
            var eventName = eventArgs.RoutingKey;
            var message = Encoding.UTF8.GetString(eventArgs.Body.Span);
            var cancellationToken = eventArgs.CancellationToken;

            try
            {
                await ProcessEvent(eventName, message, eventArgs.BasicProperties.Headers, cancellationToken).ConfigureAwait(false);
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

        private async Task PublishToDeadLetterAsync(IChannel channel, BasicDeliverEventArgs eventArgs, string eventName, Exception exception)
        {
            var headers = eventArgs.BasicProperties.Headers is { } original
                ? new Dictionary<string, object?>(original)
                : new Dictionary<string, object?>();

            headers["x-exception-type"] = exception.GetType().FullName;
            headers["x-exception-message"] = exception.Message;
            headers["x-original-exchange"] = eventArgs.Exchange;
            headers["x-original-routing-key"] = eventArgs.RoutingKey;

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

        private void EnsureConnected()
        {
            if (!_persistentConnection.IsConnected)
            {
                _persistentConnection.TryConnect();
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

                EnsureConnected();

                var channel = await _persistentConnection.CreateChannelAsync().ConfigureAwait(false);
                await using (channel.ConfigureAwait(false))
                {
                    await channel.ExchangeDeclareAsync(exchange: targetExchange, type: ExchangeType.Direct).ConfigureAwait(false);

                    await channel.QueueDeclareAsync(queue: queueName,
                                                   durable: true,
                                                   exclusive: false,
                                                   autoDelete: false,
                                                   arguments: null).ConfigureAwait(false);

                    await channel.QueueBindAsync(queue: queueName,
                                                exchange: targetExchange,
                                                routingKey: eventName).ConfigureAwait(false);

                    if (_options.OnFailure == RabbitFlowFailureMode.DeadLetter)
                    {
                        await DeclareDeadLetterAsync(channel, queueName, eventName).ConfigureAwait(false);
                    }
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

            EnsureConnected();

            _logger.LogTrace("Creating RabbitMQ consumer channel");

            var channel = await _persistentConnection.CreateChannelAsync().ConfigureAwait(false);

            if (_options.PrefetchCount > 0)
            {
                await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: _options.PrefetchCount, global: false).ConfigureAwait(false);
            }

            channel.CallbackExceptionAsync += OnConsumerCallbackExceptionAsync;

            _consumerChannel = channel;
            return channel;
        }

        private async Task OnConsumerCallbackExceptionAsync(object sender, CallbackExceptionEventArgs ea)
        {
            if (_disposed) return;

            _logger.LogWarning(ea.Exception, "Recreating RabbitMQ consumer channel");

            await _consumerLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (!ReferenceEquals(sender, _consumerChannel)) return;

                _consumerChannel?.Dispose();
                _consumerChannel = null;
                _consumerTags.Clear();

                foreach (var queueName in _eventQueues.Values.Distinct())
                {
                    await StartConsumerAsync(queueName).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Could not recreate RabbitMQ consumer channel");
            }
            finally
            {
                _consumerLock.Release();
            }
        }

        private Task Consumer_ReceivedAsync(object sender, BasicDeliverEventArgs eventArgs)
            => HandleDeliveryAsync(((AsyncEventingBasicConsumer)sender).Channel, eventArgs);

        private async void SubsManager_OnEventRemoved(object? sender, string eventName)
        {
            try
            {
                await _consumerLock.WaitAsync().ConfigureAwait(false);
                try
                {
                    if (!_eventQueues.TryGetValue(eventName, out var queueName)) return;

                    EnsureConnected();

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

                    if (!_subsManager.HasSubscriptionsForEvent(eventName))
                    {
                        if (_consumerTags.Remove(queueName, out var consumerTag)
                            && !string.IsNullOrEmpty(consumerTag)
                            && _consumerChannel is { IsOpen: true })
                        {
                            await _consumerChannel.BasicCancelAsync(consumerTag).ConfigureAwait(false);
                        }

                        await channel.QueueDeleteAsync(queue: queueName).ConfigureAwait(false);
                        _eventQueues.Remove(eventName);
                    }
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
