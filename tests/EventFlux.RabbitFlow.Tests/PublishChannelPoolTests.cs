using EventFlux.RabbitFlow.Tests.Events;
using EventFlux.RabbitFlow.Tests.Fakes;
using EventFlux.RabbitMQ;
using EventFlux.RabbitMQ.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Xunit;

namespace EventFlux.RabbitFlow.Tests
{
    public class PublishChannelPoolTests
    {
        private static (ServiceProvider Provider, EventBusRabbitMQ Broker, FakeBroker Fake) Create(Action<RabbitFlowOptions>? configure = null)
        {
            var fake = new FakeBroker { DistinctChannels = true };
            var provider = TestHost.Build(fake, o =>
            {
                o.RetryCount = 0;
                configure?.Invoke(o);
            });
            return (provider, (EventBusRabbitMQ)provider.GetRequiredService<IEventBroker>(), fake);
        }

        private static int Publishes(FakeChannel channel) => channel.Recorder.CallsTo(nameof(IChannel.BasicPublishAsync)).Count;

        [Fact]
        public async Task SequentialPublishes_ReuseOneChannel()
        {
            var (provider, broker, fake) = Create();
            await using var _ = provider;

            for (var i = 0; i < 5; i++) await broker.PublishAsync(new OrderPlaced());

            var channel = Assert.Single(fake.CreatedChannels);
            Assert.Equal(5, Publishes(channel));
            Assert.Equal(5, channel.Recorder.CallsTo(nameof(IChannel.ExchangeDeclareAsync)).Count);
            Assert.Equal(0, channel.Disposals);
        }

        [Fact]
        public async Task ConcurrentPublishes_NeverShareAChannelAndAreReused()
        {
            var (provider, broker, fake) = Create();
            await using var _ = provider;
            fake.OnPublish = _ => Task.Delay(50);

            await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => broker.PublishAsync(new OrderPlaced())));
            var created = fake.CreatedChannels.Count;
            await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => broker.PublishAsync(new OrderPlaced())));

            Assert.All(fake.CreatedChannels, c => Assert.False(c.UsedConcurrently));
            Assert.InRange(created, 2, 8);
            Assert.Equal(created, fake.CreatedChannels.Count);
            Assert.Equal(16, fake.CreatedChannels.Sum(Publishes));
        }

        [Fact]
        public async Task IdleChannels_AreBounded()
        {
            var (provider, broker, fake) = Create();
            await using var _ = provider;
            broker.MaxIdlePublishChannels = 2;
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            fake.OnPublish = _ => gate.Task;

            var publishes = Enumerable.Range(0, 5).Select(_ => broker.PublishAsync(new OrderPlaced())).ToList();
            while (fake.CreatedChannels.Count < 5) await Task.Delay(10);
            gate.SetResult();
            await Task.WhenAll(publishes);

            Assert.Equal(3, fake.CreatedChannels.Count(c => c.Disposals > 0));
            Assert.Equal(2, fake.CreatedChannels.Count(c => c.Disposals == 0));
        }

        [Fact]
        public async Task ClosedPooledChannel_IsDiscardedAndReplaced()
        {
            var (provider, broker, fake) = Create();
            await using var _ = provider;
            await broker.PublishAsync(new OrderPlaced());
            var first = Assert.Single(fake.CreatedChannels);
            first.IsOpen = false;

            await broker.PublishAsync(new OrderPlaced());

            Assert.Equal(2, fake.CreatedChannels.Count);
            Assert.True(first.Disposals > 0);
            Assert.Equal(1, Publishes(fake.CreatedChannels.Last()));
        }

        [Fact]
        public async Task FailedPublish_DisposesTheChannelInsteadOfReusingIt()
        {
            var (provider, broker, fake) = Create();
            await using var _ = provider;
            var fail = true;
            fake.OnPublish = _ => fail ? Task.FromException(new OperationInterruptedException(null)) : Task.CompletedTask;

            await Assert.ThrowsAnyAsync<Exception>(() => broker.PublishAsync(new OrderPlaced()));
            fail = false;
            await broker.PublishAsync(new OrderPlaced());

            Assert.Equal(2, fake.CreatedChannels.Count);
            Assert.True(fake.CreatedChannels.First().Disposals > 0);
            Assert.Equal(0, fake.CreatedChannels.Last().Disposals);
        }

        [Fact]
        public async Task CancelledPublish_DisposesTheChannel()
        {
            var (provider, broker, fake) = Create();
            await using var _ = provider;
            using var cts = new CancellationTokenSource();
            fake.OnPublish = _ => Task.Delay(Timeout.Infinite, cts.Token);

            var publish = broker.PublishAsync(new OrderPlaced(), cts.Token);
            while (fake.CreatedChannels.IsEmpty) await Task.Delay(10);
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => publish);
            Assert.True(Assert.Single(fake.CreatedChannels).Disposals > 0);
        }

        [Fact]
        public async Task UnroutablePublishWithConfirms_KeepsTheChannel()
        {
            var (provider, broker, fake) = Create(o => o.PublisherConfirms = true);
            await using var _ = provider;
            fake.OnPublish = _ => Task.FromException(new PublishException(1, isReturn: true));

            await broker.PublishAsync(new OrderPlaced());
            await broker.PublishAsync(new OrderPlaced());

            var channel = Assert.Single(fake.CreatedChannels);
            Assert.Equal(0, channel.Disposals);
            var options = fake.Connections[0].Recorder.CallsTo(nameof(IConnection.CreateChannelAsync)).Single().Args[0] as CreateChannelOptions;
            Assert.True(options!.PublisherConfirmationTrackingEnabled);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task DisposingTheBroker_DisposesPooledChannels(bool async)
        {
            var (provider, broker, fake) = Create();
            await using var _ = provider;
            fake.OnPublish = _ => Task.Delay(20);
            await Task.WhenAll(Enumerable.Range(0, 3).Select(_ => broker.PublishAsync(new OrderPlaced())));

            if (async) await broker.DisposeAsync();
            else broker.Dispose();

            Assert.All(fake.CreatedChannels, c => Assert.True(c.Disposals > 0));
        }

        [Fact]
        public async Task PublishAfterDispose_DoesNotPoolTheChannel()
        {
            var (provider, broker, fake) = Create();
            await using var _ = provider;
            await broker.DisposeAsync();

            await broker.PublishAsync(new OrderPlaced());

            Assert.True(Assert.Single(fake.CreatedChannels).Disposals > 0);
        }
    }
}
