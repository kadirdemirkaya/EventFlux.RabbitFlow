using EventFlux.Abstractions;
using EventFlux.RabbitMQ.Abstractions;
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
    public class EventBusRabbitMQ : SubscribeProcessEvent, IEventBroker, IDisposable
    {
        const string appName = "EventFlux_RabbitFlow";

        private readonly IPersistenceConnection _persistentConnection;
        private readonly int _retryCount;
        private IChannel _consumerChannel;
        private readonly string _serviceName;
        private readonly string _exchangeName;
        private readonly IEventBus _eventBus;

        private readonly Dictionary<string, string> _eventQueues = new Dictionary<string, string>();

        public EventBusRabbitMQ(IPersistenceConnection persistentConnection,
            ILogger<IEventBroker> logger,
            IServiceScopeFactory serviceScope,
            IEventBusSubscriptionsManager subsManager,
            IEventBus eventBus,
            string serviceName,
            int retryCount = 5)
            : base(logger, subsManager, serviceScope, appName, eventBus)
        {
            _eventBus = eventBus;
            _persistentConnection = persistentConnection ?? throw new ArgumentNullException(nameof(persistentConnection));
            _serviceName = serviceName ?? throw new ArgumentNullException(nameof(serviceName));
            _exchangeName = _serviceName;
            _consumerChannel = CreateConsumerChannelAsync().GetAwaiter().GetResult();
            _retryCount = retryCount;
            _subsManager.OnEventRemoved += SubsManager_OnEventRemoved;
        }

        private async void SubsManager_OnEventRemoved(object sender, string eventName)
        {
            if (!_persistentConnection.IsConnected)
            {
                _persistentConnection.TryConnect();
            }

            using (var channel = await _persistentConnection.CreateChannelAsync())
            {
                if (_eventQueues.TryGetValue(eventName, out var qName))
                {
                    await channel.QueueUnbindAsync(queue: qName,
                        exchange: _exchangeName,
                        routingKey: eventName);

                    if (!_subsManager.HasSubscriptionsForEvent(eventName))
                    {
                        await channel.QueueDeleteAsync(queue: qName);
                        _eventQueues.Remove(eventName);
                    }
                }
            }
        }

        public async Task PublishAsync(IEventRequest @event)
        {
            if (!_persistentConnection.IsConnected)
            {
                _persistentConnection.TryConnect();
            }
            var policy = RetryPolicy.Handle<BrokerUnreachableException>()
                .Or<SocketException>()
                .WaitAndRetry(_retryCount, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (ex, time) =>
                {

                });

            var eventName = @event.GetType().Name;

            try
            {
                using (var channel = await _persistentConnection.CreateChannelAsync())
                {
                    await channel.ExchangeDeclareAsync(exchange: _exchangeName, type: "direct");

                    var message = JsonConvert.SerializeObject(@event);
                    var body = Encoding.UTF8.GetBytes(message);

                    await policy.Execute(async () =>
                    {
                        var properties = new BasicProperties();

                        properties.DeliveryMode = DeliveryModes.Persistent;

                        await channel.BasicPublishAsync(
                            exchange: _exchangeName,
                            routingKey: eventName,
                            mandatory: true,
                            basicProperties: properties,
                            body: body);
                    });
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

        public async Task PublishAsync(IEventRequest @event, string exchangeName)
        {
            if (!_persistentConnection.IsConnected)
            {
                _persistentConnection.TryConnect();
            }

            var policy = RetryPolicy.Handle<BrokerUnreachableException>()
                .Or<SocketException>()
                .WaitAndRetry(_retryCount, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (ex, time) =>
                {

                });

            var eventName = @event.GetType().Name;
            var targetExchange = string.IsNullOrWhiteSpace(exchangeName) ? _exchangeName : exchangeName;

            try
            {
                using (var channel = await _persistentConnection.CreateChannelAsync())
                {
                    await channel.ExchangeDeclareAsync(exchange: targetExchange, type: "direct");

                    var message = JsonConvert.SerializeObject(@event);
                    var body = Encoding.UTF8.GetBytes(message);

                    await policy.Execute(async () =>
                    {
                        var properties = new BasicProperties();

                        properties.DeliveryMode = DeliveryModes.Persistent;

                        await channel.BasicPublishAsync(
                            exchange: targetExchange,
                            routingKey: eventName,
                            mandatory: true,
                            basicProperties: properties,
                            body: body);
                    });
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

        public async Task SubscribeAsync<T, TH>(string exchangeName = null)
            where T : IEventRequest
            where TH : IEventHandler<T>
        {
            var eventName = _subsManager.GetEventKey<T>();
            await InternalSubscriptionAsync(eventName, exchangeName);
            _subsManager.AddSubscription<T, TH>();
        }

        private async Task InternalSubscriptionAsync(string eventName, string exchangeName = null)
        {
            if (_subsManager.HasSubscriptionsForEvent(eventName)) return;

            if (!_persistentConnection.IsConnected) _persistentConnection.TryConnect();

            var targetExchange = string.IsNullOrWhiteSpace(exchangeName) ? _exchangeName : exchangeName;

            using var channel = await _persistentConnection.CreateChannelAsync();

            await channel.ExchangeDeclareAsync(exchange: targetExchange, type: "direct");

            var queueName = $"{_serviceName}_{eventName}";

            await channel.QueueDeclareAsync(queue: queueName,
                                           durable: true,
                                           exclusive: false,
                                           autoDelete: false,
                                           arguments: null);

            await channel.QueueBindAsync(queue: queueName,
                                        exchange: targetExchange,
                                        routingKey: eventName);

            _eventQueues[eventName] = queueName;

            var consumer = new AsyncEventingBasicConsumer(_consumerChannel);
            consumer.ReceivedAsync += Consumer_ReceivedAsync;

            await _consumerChannel.BasicConsumeAsync(queue: queueName,
                                                     autoAck: false,
                                                     consumer: consumer);
        }

        public void Unsubscribe<T, TH>()
            where T : IEventRequest
            where TH : IEventHandler<T>
        {
            var eventName = _subsManager.GetEventKey<T>();

            _subsManager.RemoveSubscription<T, TH>();
        }

        public void Dispose()
        {
            if (_consumerChannel != null)
            {
                _consumerChannel.Dispose();
            }

            _subsManager.Clear();
        }

        private void StartBasicConsume()
        {
            if (_consumerChannel != null)
            {
                foreach (var kv in _eventQueues)
                {
                    var queueName = kv.Value;
                    var consumer = new AsyncEventingBasicConsumer(_consumerChannel);
                    consumer.ReceivedAsync += Consumer_ReceivedAsync;

                    _consumerChannel.BasicConsumeAsync(
                        queue: queueName,
                        autoAck: false,
                        consumer: consumer).GetAwaiter().GetResult();
                }
            }
            else
            {
                _logger.LogError("StartBasicConsume can't call on _consumerChannel == null");
            }
        }

        private async Task Consumer_ReceivedAsync(object sender, BasicDeliverEventArgs eventArgs)
        {
            var eventName = eventArgs.RoutingKey;
            var message = Encoding.UTF8.GetString(eventArgs.Body.Span);

            try
            {
                if (message.ToLowerInvariant().Contains("throw-fake-exception"))
                {
                    throw new InvalidOperationException($"Fake exception requested: \"{message}\"");
                }

                await base.ProcessEvent(eventName, message);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "ERROR Processing message \"{Message}\"", message);
            }

            await _consumerChannel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false);
        }
        private async Task<IChannel> CreateConsumerChannelAsync()
        {
            try
            {
                if (!_persistentConnection.IsConnected)
                {
                    _persistentConnection.TryConnect();
                }

                _logger.LogTrace("Creating RabbitMQ consumer channel");

                var channel = await _persistentConnection.CreateChannelAsync();

                await channel.ExchangeDeclareAsync(exchange: _exchangeName,
                                        type: "direct");

                channel.CallbackExceptionAsync += async (sender, ea) =>
                {
                    _logger.LogWarning(ea.Exception, "Recreating RabbitMQ consumer channel");

                    _consumerChannel.Dispose();
                    _consumerChannel = await CreateConsumerChannelAsync();

                    StartBasicConsume();
                };

                return channel;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }
    }
}
