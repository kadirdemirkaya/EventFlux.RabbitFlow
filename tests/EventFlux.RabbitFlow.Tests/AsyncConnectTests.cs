using EventFlux.RabbitFlow.Tests.Events;
using EventFlux.RabbitFlow.Tests.Fakes;
using EventFlux.RabbitMQ;
using EventFlux.RabbitMQ.Abstractions;
using EventFlux.RabbitMQ.Context;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System.Diagnostics;
using Xunit;

namespace EventFlux.RabbitFlow.Tests
{
    public class AsyncConnectTests
    {
        private static PersistenceConnection CreateConnection(FakeBroker fake, bool recoversAutomatically = false)
            => new PersistenceConnection(fake.ConnectionFactory, NullLogger<PersistenceConnection>.Instance, 1, recoversAutomatically);

        private static BrokerUnreachableException Unreachable()
            => new BrokerUnreachableException(new IOException("connection refused"));

        [Fact]
        public async Task TryConnectAsync_DoesNotBlockTheCallerWhileConnecting()
        {
            var fake = new FakeBroker { ConnectGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) };
            using var connection = CreateConnection(fake);
            _ = Task.Delay(500).ContinueWith(_ => fake.ConnectGate.TrySetResult());

            var connecting = connection.TryConnectAsync();

            Assert.False(connecting.IsCompleted);
            Assert.True(await connecting);
            Assert.True(connection.IsConnected);
            Assert.Single(fake.Connections);
        }

        [Fact]
        public async Task TryConnectAsync_WhenAlreadyConnected_CompletesSynchronously()
        {
            var fake = new FakeBroker();
            using var connection = CreateConnection(fake);
            await connection.TryConnectAsync();

            var again = connection.TryConnectAsync();

            Assert.True(again.IsCompletedSuccessfully);
            Assert.True(await again);
            Assert.Single(fake.Connections);
        }

        [Fact]
        public async Task ConcurrentTryConnectAsync_WhileConnecting_OpensSingleConnection()
        {
            var fake = new FakeBroker { ConnectGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) };
            using var connection = CreateConnection(fake);

            var callers = Enumerable.Range(0, 8).Select(_ => connection.TryConnectAsync()).ToList();
            fake.ConnectGate.SetResult();
            var results = await Task.WhenAll(callers);

            Assert.All(results, Assert.True);
            Assert.Single(fake.Connections);
        }

        [Fact]
        public async Task SyncAndAsyncConnect_ShareOneConnection()
        {
            var fake = new FakeBroker { ConnectGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) };
            using var connection = CreateConnection(fake);

            var asyncCaller = connection.TryConnectAsync();
            var syncCaller = Task.Run(connection.TryConnect);
            fake.ConnectGate.SetResult();

            Assert.True(await asyncCaller);
            Assert.True(await syncCaller);
            Assert.Single(fake.Connections);
        }

        [Fact]
        public async Task TryConnectAsync_RetriesUnreachableBroker()
        {
            var fake = new FakeBroker();
            fake.ConnectFailures.Enqueue(Unreachable());
            using var connection = CreateConnection(fake);

            Assert.True(await connection.TryConnectAsync());

            Assert.Single(fake.Connections);
        }

        [Fact]
        public async Task TryConnectAsync_WhenRetriesAreExhausted_ThrowsTheOriginalException()
        {
            var fake = new FakeBroker();
            fake.ConnectFailures.Enqueue(Unreachable());
            fake.ConnectFailures.Enqueue(Unreachable());
            using var connection = CreateConnection(fake);

            await Assert.ThrowsAsync<BrokerUnreachableException>(() => connection.TryConnectAsync());

            Assert.False(connection.IsConnected);
        }

        [Fact]
        public async Task TryConnectAsync_CancelledDuringRetryDelay_StopsPromptly()
        {
            var fake = new FakeBroker();
            fake.ConnectFailures.Enqueue(Unreachable());
            using var connection = CreateConnection(fake);
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
            var watch = Stopwatch.StartNew();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => connection.TryConnectAsync(cts.Token));

            Assert.True(watch.Elapsed < TimeSpan.FromSeconds(1.5), $"took {watch.Elapsed}");
            Assert.Empty(fake.Connections);
        }

        [Fact]
        public async Task TryConnectAsync_CancelledWhileWaitingForAnotherCaller_DoesNotAffectThatCaller()
        {
            var fake = new FakeBroker { ConnectGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) };
            using var connection = CreateConnection(fake);
            using var cts = new CancellationTokenSource();

            var first = connection.TryConnectAsync();
            var second = connection.TryConnectAsync(cts.Token);
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => second);
            fake.ConnectGate.SetResult();
            Assert.True(await first);
            Assert.Single(fake.Connections);
        }

        [Fact]
        public async Task Dispose_DuringRetryDelay_StopsReconnectingAndOpensNothing()
        {
            var fake = new FakeBroker();
            fake.ConnectFailures.Enqueue(Unreachable());
            var connection = CreateConnection(fake);
            var watch = Stopwatch.StartNew();

            var connecting = connection.TryConnectAsync();
            await Task.Delay(100);
            connection.Dispose();

            Assert.False(await connecting);
            Assert.True(watch.Elapsed < TimeSpan.FromSeconds(1.5), $"took {watch.Elapsed}");
            Assert.Empty(fake.Connections);
            Assert.False(connection.IsConnected);
        }

        [Fact]
        public async Task Dispose_WhileConnecting_ReturnsFalse()
        {
            var fake = new FakeBroker { ConnectGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) };
            var connection = CreateConnection(fake);

            var connecting = connection.TryConnectAsync();
            connection.Dispose();

            Assert.False(await connecting);
            Assert.Empty(fake.Connections);
        }

        [Fact]
        public async Task TryConnectAsync_AfterDispose_ReturnsFalse()
        {
            var fake = new FakeBroker();
            var connection = CreateConnection(fake);
            connection.Dispose();

            Assert.False(await connection.TryConnectAsync());
            Assert.False(connection.TryConnect());
            Assert.Empty(fake.Connections);
        }

        [Fact]
        public async Task TryConnectAsync_WhileRecovering_DoesNotReplaceTheRecoveringConnection()
        {
            var fake = new FakeBroker();
            using var connection = CreateConnection(fake, recoversAutomatically: true);
            await connection.TryConnectAsync();
            fake.Connections[0].IsOpen = false;

            Assert.False(await connection.TryConnectAsync());

            Assert.Single(fake.Connections);
            Assert.Empty(fake.Connections[0].Recorder.CallsTo(nameof(IDisposable.Dispose)));
        }

        [Fact]
        public async Task DefaultTryConnectAsync_ForwardsToTryConnect()
        {
            IPersistenceConnection legacy = new SyncOnlyConnection();

            Assert.True(await legacy.TryConnectAsync());
            Assert.Equal(1, ((SyncOnlyConnection)legacy).TryConnectCalls);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => legacy.TryConnectAsync(new CancellationToken(canceled: true)));
            Assert.Equal(1, ((SyncOnlyConnection)legacy).TryConnectCalls);
        }

        [Fact]
        public async Task Broker_PublishAndSubscribe_ConnectWithoutTheBlockingOverload()
        {
            var fake = new FakeBroker();
            var connection = new AsyncOnlyConnection(CreateConnection(fake));
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton<HandledOrders>();
            services.AddEventFluxRabbitFlow(typeof(OrderPlaced).Assembly, fake.ConnectionFactory, TestHost.ServiceName);
            await using var provider = services.BuildServiceProvider();
            var broker = new EventBusRabbitMQ(
                connection,
                provider.GetRequiredService<ILogger<IEventBroker>>(),
                provider.GetRequiredService<IServiceScopeFactory>(),
                new EventBusSubscriptionsManager(),
                provider.GetRequiredService<IEventFluxContextAccessor>(),
                TestHost.ServiceName,
                new RabbitFlowOptions { RetryCount = 0 });

            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            await broker.PublishAsync(new OrderPlaced());

            Assert.Equal(0, connection.TryConnectCalls);
            Assert.True(connection.TryConnectAsyncCalls > 0);
            Assert.Single(fake.Published);
            Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicConsumeAsync)));
        }

        [Fact]
        public async Task PublishAsync_Cancelled_StopsWaitingForTheConnection()
        {
            var fake = new FakeBroker { ConnectGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) };
            await using var provider = TestHost.Build(fake);
            var broker = provider.GetRequiredService<IEventBroker>();
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => broker.PublishAsync(new OrderPlaced(), cts.Token));

            Assert.Empty(fake.Published);
        }

        private sealed class SyncOnlyConnection : IPersistenceConnection
        {
            public int TryConnectCalls { get; private set; }

            public bool IsConnected => TryConnectCalls > 0;

            public bool TryConnect()
            {
                TryConnectCalls++;
                return true;
            }

            public Task<IChannel> CreateChannelAsync() => throw new NotSupportedException();

            public void Dispose()
            {
            }
        }

        private sealed class AsyncOnlyConnection : IPersistenceConnection
        {
            private readonly PersistenceConnection _inner;

            public AsyncOnlyConnection(PersistenceConnection inner) => _inner = inner;

            public int TryConnectCalls { get; private set; }

            public int TryConnectAsyncCalls { get; private set; }

            public bool IsConnected => _inner.IsConnected;

            public bool TryConnect()
            {
                TryConnectCalls++;
                throw new NotSupportedException("the blocking overload was used");
            }

            public Task<bool> TryConnectAsync(CancellationToken cancellationToken = default)
            {
                TryConnectAsyncCalls++;
                return _inner.TryConnectAsync(cancellationToken);
            }

            public Task<IChannel> CreateChannelAsync() => _inner.CreateChannelAsync();

            public void Dispose() => _inner.Dispose();
        }
    }
}
