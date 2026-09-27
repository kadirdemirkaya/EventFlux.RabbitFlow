using EventFlux.RabbitFlow.Tests.Events;
using EventFlux.RabbitFlow.Tests.Fakes;
using EventFlux.RabbitMQ;
using EventFlux.RabbitMQ.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using Xunit;

namespace EventFlux.RabbitFlow.Tests
{
    public class UnsubscribeTests
    {
        private static readonly string Queue = $"{TestHost.ServiceName}_{nameof(OrderPlaced)}";

        private static FakeBroker NewFake(Func<string, object?[], object?>? extra = null)
        {
            var fake = new FakeBroker();
            fake.ChannelRecorder.Responder = (method, args) =>
                method.Name == nameof(IChannel.BasicConsumeAsync) ? Task.FromResult("consumer-1") : extra?.Invoke(method.Name, args);
            return fake;
        }

        private static async Task<(ServiceProvider Provider, EventBusRabbitMQ Broker, FakeBroker Fake)> Subscribed(Action<RabbitFlowOptions>? configure = null, params string[] extraExchanges)
        {
            var fake = NewFake();
            var provider = TestHost.Build(fake, configure);
            var broker = (EventBusRabbitMQ)provider.GetRequiredService<IEventBroker>();
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            foreach (var exchange in extraExchanges) await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>(exchange);
            fake.ChannelRecorder.Calls.Clear();
            return (provider, broker, fake);
        }

        private static async Task UnsubscribeAsync(EventBusRabbitMQ broker)
        {
            broker.Unsubscribe<OrderPlaced, OrderPlacedHandler>();
            await broker.SubscriptionRemoval;
        }

        [Fact]
        public async Task ByDefault_UnbindsAndCancelsButKeepsTheQueue()
        {
            var (provider, broker, fake) = await Subscribed(null, "billing");
            await using var _ = provider;

            await UnsubscribeAsync(broker);

            var unbinds = fake.ChannelRecorder.CallsTo(nameof(IChannel.QueueUnbindAsync));
            Assert.Equal(new[] { "billing", TestHost.ServiceName }, unbinds.Select(c => c.Arg<string>("exchange")).OrderBy(x => x));
            Assert.All(unbinds, c => Assert.Equal(Queue, c.Arg<string>("queue")));
            Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicCancelAsync)));
            Assert.Empty(fake.ChannelRecorder.CallsTo(nameof(IChannel.QueueDeleteAsync)));
        }

        [Fact]
        public async Task DeleteQueueOnUnsubscribe_DeletesTheQueueAsBefore()
        {
            var (provider, broker, fake) = await Subscribed(o => o.DeleteQueueOnUnsubscribe = true);
            await using var _ = provider;

            await UnsubscribeAsync(broker);

            Assert.Equal(Queue, Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.QueueDeleteAsync))).Arg<string>("queue"));
            Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicCancelAsync)));
        }

        [Fact]
        public async Task DeleteQueueOnUnsubscribe_WithRedeliveryDelay_AlsoDeletesTheRetryQueueButNeverTheDeadLetterQueue()
        {
            var (provider, broker, fake) = await Subscribed(o =>
            {
                o.DeleteQueueOnUnsubscribe = true;
                o.MaxDeliveryAttempts = 3;
                o.RedeliveryDelay = TimeSpan.FromSeconds(1);
            });
            await using var _ = provider;

            await UnsubscribeAsync(broker);

            var deleted = fake.ChannelRecorder.CallsTo(nameof(IChannel.QueueDeleteAsync)).Select(c => c.Arg<string>("queue")).ToList();
            Assert.Equal(new[] { Queue, $"{Queue}_retry" }, deleted);
        }

        [Fact]
        public async Task RemovingOneOfTwoHandlers_TouchesNothingOnTheBroker()
        {
            var fake = NewFake();
            await using var provider = TestHost.Build(fake);
            var broker = (EventBusRabbitMQ)provider.GetRequiredService<IEventBroker>();
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            await broker.SubscribeAsync<OrderPlaced, SecondOrderPlacedHandler>();
            fake.ChannelRecorder.Calls.Clear();

            await UnsubscribeAsync(broker);

            Assert.Empty(fake.ChannelRecorder.CallsTo(nameof(IChannel.QueueUnbindAsync)));
            Assert.Empty(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicCancelAsync)));
        }

        [Fact]
        public async Task SubscribingAgainAfterUnsubscribe_RebindsAndConsumesTheKeptQueue()
        {
            var (provider, broker, fake) = await Subscribed();
            await using var _ = provider;
            await UnsubscribeAsync(broker);
            fake.ChannelRecorder.Calls.Clear();

            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();

            Assert.Equal(Queue, Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.QueueBindAsync))).Arg<string>("queue"));
            Assert.Equal(Queue, Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicConsumeAsync))).Arg<string>("queue"));
        }

        [Fact]
        public async Task RemovalQueuedBehindAnotherSubscribe_SkipsWhenTheEventWasSubscribedAgain()
        {
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var fake = NewFake((name, args) => name == nameof(IChannel.QueueBindAsync) && (string)args[1]! == "hold" ? gate.Task : null);
            await using var provider = TestHost.Build(fake);
            var broker = (EventBusRabbitMQ)provider.GetRequiredService<IEventBroker>();
            var manager = provider.GetRequiredService<IEventBusSubscriptionsManager>();
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();

            var holding = broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>("hold");
            broker.Unsubscribe<OrderPlaced, OrderPlacedHandler>();
            manager.AddSubscription<OrderPlaced, OrderPlacedHandler>();
            gate.SetResult();
            await holding;
            await broker.SubscriptionRemoval;

            Assert.Empty(fake.ChannelRecorder.CallsTo(nameof(IChannel.QueueUnbindAsync)));
            Assert.Empty(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicCancelAsync)));
        }

        public class SecondOrderPlacedHandler : EventFlux.Abstractions.IEventHandler<OrderPlaced>
        {
            public Task Handle(OrderPlaced @event, CancellationToken cancellationToken) => Task.CompletedTask;
        }
    }
}
