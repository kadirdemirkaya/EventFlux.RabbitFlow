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
using Xunit;

namespace EventFlux.RabbitFlow.Tests
{
    public class ConnectionRecoveryTests
    {
        private const string ConnectionShutdown = nameof(IConnection.ConnectionShutdownAsync);

        private static PersistenceConnection CreateConnection(FakeBroker fake, bool recoversAutomatically)
            => new PersistenceConnection(fake.ConnectionFactory, NullLogger<PersistenceConnection>.Instance, 1, recoversAutomatically);

        private static ShutdownEventArgs PeerShutdown()
            => new ShutdownEventArgs(ShutdownInitiator.Peer, 320, "CONNECTION_FORCED");

        [Fact]
        public void TryConnect_WhenAlreadyConnected_DoesNotOpenSecondConnection()
        {
            var fake = new FakeBroker();
            using var connection = CreateConnection(fake, recoversAutomatically: false);

            Assert.True(connection.TryConnect());
            Assert.True(connection.TryConnect());

            Assert.Single(fake.Connections);
        }

        [Fact]
        public async Task ConcurrentTryConnect_OpensSingleConnection()
        {
            var fake = new FakeBroker();
            using var connection = CreateConnection(fake, recoversAutomatically: false);

            await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(connection.TryConnect)));

            Assert.Single(fake.Connections);
        }

        [Fact]
        public async Task ConnectionBlocked_DoesNotReconnect()
        {
            var fake = new FakeBroker();
            using var connection = CreateConnection(fake, recoversAutomatically: false);
            connection.TryConnect();

            await fake.Connections[0].RaiseAsync(nameof(IConnection.ConnectionBlockedAsync), new ConnectionBlockedEventArgs("low on memory"));

            Assert.Single(fake.Connections);
        }

        [Fact]
        public async Task CallbackException_DoesNotReconnect()
        {
            var fake = new FakeBroker();
            using var connection = CreateConnection(fake, recoversAutomatically: false);
            connection.TryConnect();

            await fake.Connections[0].RaiseAsync(nameof(IConnection.CallbackExceptionAsync),
                new CallbackExceptionEventArgs(new Dictionary<string, object?>(), new InvalidOperationException("handler failed")));

            Assert.Single(fake.Connections);
        }

        [Fact]
        public async Task ApplicationInitiatedShutdown_DoesNotReconnect()
        {
            var fake = new FakeBroker();
            using var connection = CreateConnection(fake, recoversAutomatically: false);
            connection.TryConnect();
            fake.Connections[0].IsOpen = false;

            await fake.Connections[0].RaiseAsync(ConnectionShutdown, new ShutdownEventArgs(ShutdownInitiator.Application, 200, "Goodbye"));

            Assert.Single(fake.Connections);
        }

        [Fact]
        public async Task PeerShutdown_WithAutomaticRecovery_LeavesRecoveryToTheClient()
        {
            var fake = new FakeBroker();
            using var connection = CreateConnection(fake, recoversAutomatically: true);
            connection.TryConnect();
            fake.Connections[0].IsOpen = false;

            await fake.Connections[0].RaiseAsync(ConnectionShutdown, PeerShutdown());

            Assert.Single(fake.Connections);
        }

        [Fact]
        public void TryConnect_WhileRecovering_DoesNotReplaceTheRecoveringConnection()
        {
            var fake = new FakeBroker();
            using var connection = CreateConnection(fake, recoversAutomatically: true);
            connection.TryConnect();
            fake.Connections[0].IsOpen = false;

            Assert.False(connection.TryConnect());

            Assert.Single(fake.Connections);
            Assert.Empty(fake.Connections[0].Recorder.CallsTo(nameof(IDisposable.Dispose)));
        }

        [Fact]
        public async Task PeerShutdown_WithoutAutomaticRecovery_ReplacesAndReleasesTheOldConnection()
        {
            var fake = new FakeBroker();
            using var connection = CreateConnection(fake, recoversAutomatically: false);
            connection.TryConnect();
            var old = fake.Connections[0];
            old.IsOpen = false;

            await old.RaiseAsync(ConnectionShutdown, PeerShutdown());
            await connection.Reconnection;

            Assert.Equal(2, fake.Connections.Count);
            Assert.True(connection.IsConnected);
            Assert.Single(old.Recorder.CallsTo(nameof(IDisposable.Dispose)));
            Assert.Equal(0, old.HandlerCount(ConnectionShutdown));
            Assert.Equal(1, fake.Connections[1].HandlerCount(ConnectionShutdown));
        }

        [Fact]
        public async Task PeerShutdown_WithoutAutomaticRecovery_DoesNotDisposeConnectionInsideItsOwnShutdownCallback()
        {
            var fake = new FakeBroker();
            using var connection = CreateConnection(fake, recoversAutomatically: false);
            connection.TryConnect();
            var old = fake.Connections[0];
            old.IsOpen = false;

            await old.RaiseAsync(ConnectionShutdown, PeerShutdown());
            await connection.Reconnection;

            Assert.False(old.DisposedInsideOwnCallback);
            Assert.Single(old.Recorder.CallsTo(nameof(IDisposable.Dispose)));
        }

        [Fact]
        public void Dispose_DetachesConnectionEvents()
        {
            var fake = new FakeBroker();
            var connection = CreateConnection(fake, recoversAutomatically: false);
            connection.TryConnect();

            connection.Dispose();

            Assert.Equal(0, fake.Connections[0].HandlerCount(ConnectionShutdown));
            Assert.Equal(0, fake.Connections[0].HandlerCount(nameof(IConnection.ConnectionBlockedAsync)));
            Assert.Equal(0, fake.Connections[0].HandlerCount(nameof(IConnection.CallbackExceptionAsync)));
        }

        [Fact]
        public void PublicConstructor_TreatsConnectionFactoryRecoverySettingAsSource()
        {
            var logger = NullLogger<PersistenceConnection>.Instance;

            Assert.True(new PersistenceConnection(new ConnectionFactory(), logger).RecoversAutomatically);
            Assert.False(new PersistenceConnection(new ConnectionFactory { AutomaticRecoveryEnabled = false }, logger).RecoversAutomatically);
            Assert.False(new PersistenceConnection(new FakeBroker().ConnectionFactory, logger).RecoversAutomatically);
        }

        private static (ServiceProvider Provider, EventBusRabbitMQ Broker) CreateBroker(FakeBroker fake, bool recoversAutomatically)
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton<HandledOrders>();
            services.AddEventFluxRabbitFlow(typeof(OrderPlaced).Assembly, fake.ConnectionFactory, TestHost.ServiceName);
            var provider = services.BuildServiceProvider();

            var broker = new EventBusRabbitMQ(
                CreateConnection(fake, recoversAutomatically),
                provider.GetRequiredService<ILogger<IEventBroker>>(),
                provider.GetRequiredService<IServiceScopeFactory>(),
                new EventBusSubscriptionsManager(),
                provider.GetRequiredService<IEventFluxContextAccessor>(),
                TestHost.ServiceName,
                new RabbitFlowOptions { RetryCount = 1 });

            return (provider, broker);
        }

        [Fact]
        public async Task ConsumerChannelShutdown_WithoutAutomaticRecovery_RestartsConsumersOnNewConnection()
        {
            var fake = new FakeBroker();
            var (provider, broker) = CreateBroker(fake, recoversAutomatically: false);
            await using var _ = provider;
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            var shutdownHandler = (AsyncEventHandler<ShutdownEventArgs>)fake.ChannelRecorder.CallsTo("add_" + nameof(IChannel.ChannelShutdownAsync)).Single().Args[0]!;

            fake.Connections[0].IsOpen = false;
            await fake.Connections[0].RaiseAsync(ConnectionShutdown, PeerShutdown());
            await shutdownHandler(fake.Channel, PeerShutdown());
            await broker.ConsumerRecovery;

            var consumes = fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicConsumeAsync));
            Assert.Equal(2, consumes.Count);
            Assert.Equal($"{TestHost.ServiceName}_{nameof(OrderPlaced)}", consumes[1].Arg<string>("queue"));
            Assert.Equal(2, fake.Connections[1].Recorder.CallsTo(nameof(IConnection.CreateChannelAsync)).Count);
        }

        [Fact]
        public async Task ConsumerChannelShutdown_WithAutomaticRecovery_DoesNotStartDuplicateConsumers()
        {
            var fake = new FakeBroker();
            var (provider, broker) = CreateBroker(fake, recoversAutomatically: true);
            await using var _ = provider;
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            var shutdownHandler = (AsyncEventHandler<ShutdownEventArgs>)fake.ChannelRecorder.CallsTo("add_" + nameof(IChannel.ChannelShutdownAsync)).Single().Args[0]!;
            fake.Connections[0].IsOpen = false;

            await shutdownHandler(fake.Channel, PeerShutdown());
            await broker.ConsumerRecovery;

            Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicConsumeAsync)));
        }

        [Fact]
        public async Task PublishAsync_WhileConnectionIsRecovering_RetriesUntilItIsBack()
        {
            var fake = new FakeBroker();
            var (provider, broker) = CreateBroker(fake, recoversAutomatically: true);
            await using var _ = provider;
            await broker.PublishAsync(new OrderPlaced());
            fake.Connections[0].IsOpen = false;

            var publish = broker.PublishAsync(new OrderPlaced());
            await Task.Delay(200);
            fake.Connections[0].IsOpen = true;
            await publish;

            Assert.Equal(2, fake.Published.Count);
            Assert.Single(fake.Connections);
        }
    }
}
