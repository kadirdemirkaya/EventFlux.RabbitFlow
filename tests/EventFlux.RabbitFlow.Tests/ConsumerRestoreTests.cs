using EventFlux.RabbitFlow.Tests.Events;
using EventFlux.RabbitFlow.Tests.Fakes;
using EventFlux.RabbitMQ;
using EventFlux.RabbitMQ.Abstractions;
using EventFlux.RabbitMQ.Context;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using Xunit;

namespace EventFlux.RabbitFlow.Tests
{
    public class ConsumerRestoreTests
    {
        private static readonly string Queue = $"{TestHost.ServiceName}_{nameof(OrderPlaced)}";

        private static ShutdownEventArgs PeerShutdown()
            => new ShutdownEventArgs(ShutdownInitiator.Peer, 320, "CONNECTION_FORCED");

        private static ShutdownEventArgs ChannelClosedByBroker()
            => new ShutdownEventArgs(ShutdownInitiator.Peer, 406, "PRECONDITION_FAILED - unknown delivery tag 42", 60, 80);

        private static (ServiceProvider Provider, EventBusRabbitMQ Broker, IPersistenceConnection Connection) CreateBroker(
            FakeBroker fake, bool recoversAutomatically, RabbitFlowFailureMode onFailure = RabbitFlowFailureMode.Requeue, IPersistenceConnection? connection = null)
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton<HandledOrders>();
            services.AddEventFluxRabbitFlow(typeof(OrderPlaced).Assembly, fake.ConnectionFactory, TestHost.ServiceName);
            var provider = services.BuildServiceProvider();

            connection ??= new PersistenceConnection(fake.ConnectionFactory, NullLogger<PersistenceConnection>.Instance, 0, recoversAutomatically);
            var broker = new EventBusRabbitMQ(
                connection,
                provider.GetRequiredService<ILogger<IEventBroker>>(),
                provider.GetRequiredService<IServiceScopeFactory>(),
                new EventBusSubscriptionsManager(),
                provider.GetRequiredService<IEventFluxContextAccessor>(),
                TestHost.ServiceName,
                new RabbitFlowOptions { RetryCount = 0, OnFailure = onFailure })
            {
                ConsumerRestoreDelay = _ => TimeSpan.FromMilliseconds(50)
            };

            return (provider, broker, connection);
        }

        private static AsyncEventHandler<ShutdownEventArgs> ChannelShutdownHandler(FakeBroker fake)
            => (AsyncEventHandler<ShutdownEventArgs>)fake.ChannelRecorder.CallsTo("add_" + nameof(IChannel.ChannelShutdownAsync)).Last().Args[0]!;

        private static List<string> CallsAfter(FakeBroker fake, int skip)
            => fake.ChannelRecorder.Calls.Skip(skip).Select(c => c.Method.Name).ToList();

        private static async Task BreakConnectionAsync(FakeBroker fake)
        {
            var current = fake.Connections[^1];
            current.IsOpen = false;
            await current.RaiseAsync(nameof(IConnection.ConnectionShutdownAsync), PeerShutdown());
            await ChannelShutdownHandler(fake)(fake.Channel, PeerShutdown());
        }

        [Fact]
        public async Task Reconnect_WithoutAutomaticRecovery_RedeclaresExchangeQueueAndBindingBeforeConsuming()
        {
            var fake = new FakeBroker();
            var (provider, broker, _) = CreateBroker(fake, recoversAutomatically: false);
            await using var _p = provider;
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            var before = fake.ChannelRecorder.Calls.Count;

            await BreakConnectionAsync(fake);
            await broker.ConsumerRecovery;

            var calls = CallsAfter(fake, before);
            var consume = calls.IndexOf(nameof(IChannel.BasicConsumeAsync));
            Assert.True(calls.IndexOf(nameof(IChannel.ExchangeDeclareAsync)) is >= 0 and var e && e < consume, string.Join(",", calls));
            Assert.True(calls.IndexOf(nameof(IChannel.QueueDeclareAsync)) is >= 0 and var q && q < consume, string.Join(",", calls));
            Assert.True(calls.IndexOf(nameof(IChannel.QueueBindAsync)) is >= 0 and var b && b < consume, string.Join(",", calls));

            var bind = fake.ChannelRecorder.Calls.Skip(before).Single(c => c.Method.Name == nameof(IChannel.QueueBindAsync));
            Assert.Equal(Queue, bind.Arg<string>("queue"));
            Assert.Equal(TestHost.ServiceName, bind.Arg<string>("exchange"));
            Assert.Equal(nameof(OrderPlaced), bind.Arg<string>("routingKey"));
        }

        [Fact]
        public async Task Reconnect_RebindsEveryExchangeTheEventWasSubscribedTo()
        {
            var fake = new FakeBroker();
            var (provider, broker, _) = CreateBroker(fake, recoversAutomatically: false);
            await using var _p = provider;
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>("billing");
            var before = fake.ChannelRecorder.Calls.Count;

            await BreakConnectionAsync(fake);
            await broker.ConsumerRecovery;

            var binds = fake.ChannelRecorder.Calls.Skip(before)
                .Where(c => c.Method.Name == nameof(IChannel.QueueBindAsync))
                .Select(c => c.Arg<string>("exchange"))
                .OrderBy(x => x)
                .ToList();
            Assert.Equal(new[] { "billing", TestHost.ServiceName }, binds);
            Assert.Single(fake.ChannelRecorder.Calls.Skip(before), c => c.Method.Name == nameof(IChannel.BasicConsumeAsync));
        }

        [Fact]
        public async Task Reconnect_InDeadLetterMode_RedeclaresDeadLetterTopology()
        {
            var fake = new FakeBroker();
            var (provider, broker, _) = CreateBroker(fake, recoversAutomatically: false, RabbitFlowFailureMode.DeadLetter);
            await using var _p = provider;
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            var before = fake.ChannelRecorder.Calls.Count;

            await BreakConnectionAsync(fake);
            await broker.ConsumerRecovery;

            var queues = fake.ChannelRecorder.Calls.Skip(before)
                .Where(c => c.Method.Name == nameof(IChannel.QueueDeclareAsync))
                .Select(c => c.Arg<string>("queue"))
                .ToList();
            Assert.Contains(Queue, queues);
            Assert.Contains(RabbitFlowOptions.GetDeadLetterQueue(Queue), queues);
        }

        [Fact]
        public async Task Restore_WhenBrokerIsStillDown_KeepsRetryingUntilItIsBack()
        {
            var fake = new FakeBroker();
            var (provider, broker, _) = CreateBroker(fake, recoversAutomatically: false);
            await using var _p = provider;
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            for (var i = 0; i < 4; i++) fake.ConnectFailures.Enqueue(new BrokerUnreachableException(new IOException("refused")));

            await BreakConnectionAsync(fake);
            await broker.ConsumerRecovery.WaitAsync(TimeSpan.FromSeconds(10));

            Assert.Empty(fake.ConnectFailures);
            Assert.Equal(2, fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicConsumeAsync)).Count);
            Assert.True(fake.Connections[^1].IsOpen);
        }

        [Fact]
        public async Task Restore_StopsRetryingWhenTheBrokerIsDisposed()
        {
            var fake = new FakeBroker();
            var (provider, broker, _) = CreateBroker(fake, recoversAutomatically: false);
            await using var _p = provider;
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            broker.ConsumerRestoreDelay = _ => TimeSpan.FromMinutes(5);
            fake.ConnectFailures.Enqueue(new BrokerUnreachableException(new IOException("refused")));
            fake.ConnectFailures.Enqueue(new BrokerUnreachableException(new IOException("refused")));

            await BreakConnectionAsync(fake);
            await Task.Delay(200);
            await broker.DisposeAsync();

            await broker.ConsumerRecovery.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicConsumeAsync)));
        }

        [Fact]
        public async Task ChannelClosedByBroker_WithAutomaticRecovery_RestartsConsumersOnTheSameConnection()
        {
            var fake = new FakeBroker();
            var (provider, broker, _) = CreateBroker(fake, recoversAutomatically: true);
            await using var _p = provider;
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();

            await ChannelShutdownHandler(fake)(fake.Channel, ChannelClosedByBroker());
            await broker.ConsumerRecovery;

            var consumes = fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicConsumeAsync));
            Assert.Equal(2, consumes.Count);
            Assert.Equal(Queue, consumes[1].Arg<string>("queue"));
            Assert.Single(fake.Connections);
        }

        [Fact]
        public async Task ChannelClosedByBroker_WithCustomConnection_RestartsConsumers()
        {
            var fake = new FakeBroker();
            var custom = new ForwardingConnection(new PersistenceConnection(fake.ConnectionFactory, NullLogger<PersistenceConnection>.Instance, 0, recoversAutomatically: true));
            var (provider, broker, _) = CreateBroker(fake, recoversAutomatically: true, connection: custom);
            await using var _p = provider;
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();

            await ChannelShutdownHandler(fake)(fake.Channel, ChannelClosedByBroker());
            await broker.ConsumerRecovery;

            Assert.Equal(2, fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicConsumeAsync)).Count);
        }

        [Fact]
        public async Task ChannelShutdownDuringConnectionRecovery_DoesNotRestartConsumers()
        {
            var fake = new FakeBroker();
            var custom = new ForwardingConnection(new PersistenceConnection(fake.ConnectionFactory, NullLogger<PersistenceConnection>.Instance, 0, recoversAutomatically: true));
            var (provider, broker, _) = CreateBroker(fake, recoversAutomatically: true, connection: custom);
            await using var _p = provider;
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            fake.Connections[0].IsOpen = false;

            await ChannelShutdownHandler(fake)(fake.Channel, PeerShutdown());
            await broker.ConsumerRecovery;

            Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicConsumeAsync)));
        }

        [Fact]
        public async Task Restart_DetachesHandlersFromTheClosedChannel()
        {
            var fake = new FakeBroker();
            var (provider, broker, _) = CreateBroker(fake, recoversAutomatically: true);
            await using var _p = provider;
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();

            await ChannelShutdownHandler(fake)(fake.Channel, ChannelClosedByBroker());
            await broker.ConsumerRecovery;

            Assert.Single(fake.ChannelRecorder.CallsTo("remove_" + nameof(IChannel.ChannelShutdownAsync)));
            Assert.Single(fake.ChannelRecorder.CallsTo("remove_" + nameof(IChannel.CallbackExceptionAsync)));
        }

        [Fact]
        public async Task RestartedConsumer_StillDispatchesToTheHandler()
        {
            var fake = new FakeBroker();
            var (provider, broker, _) = CreateBroker(fake, recoversAutomatically: false);
            await using var _p = provider;
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();

            await BreakConnectionAsync(fake);
            await broker.ConsumerRecovery;

            var consumer = fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicConsumeAsync))[^1].Arg<IAsyncBasicConsumer>("consumer");
            var body = System.Text.Encoding.UTF8.GetBytes("{\"Customer\":\"restored\"}");
            await consumer.HandleBasicDeliverAsync("tag", 1, false, TestHost.ServiceName, nameof(OrderPlaced), new BasicProperties(), body);

            Assert.Contains(provider.GetRequiredService<HandledOrders>().Items, i => i.Event.Customer == "restored");
            Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicAckAsync)));
        }

        [Fact]
        public async Task ChannelClosedByBroker_ReportedByAnotherChannelObject_RestartsWhenConsumerChannelIsClosed()
        {
            var fake = new FakeBroker();
            var (provider, broker, _) = CreateBroker(fake, recoversAutomatically: true);
            await using var _p = provider;
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            var (innerChannel, _) = RecordingProxy.Create<IChannel>();
            fake.ChannelIsOpen = false;

            await ChannelShutdownHandler(fake)(innerChannel, ChannelClosedByBroker());
            await broker.ConsumerRecovery;

            Assert.Equal(2, fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicConsumeAsync)).Count);
        }

        [Fact]
        public async Task StaleShutdownFromAnotherChannel_WhileConsumerChannelIsOpen_IsIgnored()
        {
            var fake = new FakeBroker();
            var (provider, broker, _) = CreateBroker(fake, recoversAutomatically: true);
            await using var _p = provider;
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            var (otherChannel, _) = RecordingProxy.Create<IChannel>();

            await ChannelShutdownHandler(fake)(otherChannel, ChannelClosedByBroker());
            await broker.ConsumerRecovery;

            Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicConsumeAsync)));
        }

        private sealed class ForwardingConnection : IPersistenceConnection
        {
            private readonly IPersistenceConnection _inner;

            public ForwardingConnection(IPersistenceConnection inner) => _inner = inner;

            public bool IsConnected => _inner.IsConnected;

            public bool TryConnect() => _inner.TryConnect();

            public Task<bool> TryConnectAsync(CancellationToken cancellationToken = default) => _inner.TryConnectAsync(cancellationToken);

            public Task<IChannel> CreateChannelAsync() => _inner.CreateChannelAsync();

            public void Dispose() => _inner.Dispose();
        }
    }
}
