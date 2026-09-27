using EventFlux.RabbitFlow.Tests.Events;
using EventFlux.RabbitFlow.Tests.Fakes;
using EventFlux.RabbitMQ;
using EventFlux.RabbitMQ.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using Xunit;

namespace EventFlux.RabbitFlow.Tests
{
    public class GracefulShutdownTests
    {
        private static async Task<(ServiceProvider Provider, EventBusRabbitMQ Broker, FakeBroker Fake, HandledOrders Handled)> Subscribed(Action<RabbitFlowOptions>? configure = null)
        {
            var fake = new FakeBroker();
            fake.ChannelRecorder.Responder = (method, _) =>
                method.Name == nameof(IChannel.BasicConsumeAsync) ? Task.FromResult("consumer-1") : null;
            var provider = TestHost.Build(fake, configure);
            var broker = (EventBusRabbitMQ)provider.GetRequiredService<IEventBroker>();
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            fake.ChannelRecorder.Calls.Clear();
            return (provider, broker, fake, provider.GetRequiredService<HandledOrders>());
        }

        private static BasicDeliverEventArgs Delivery(ulong tag)
            => new BasicDeliverEventArgs("consumer-1", tag, false, TestHost.ServiceName, nameof(OrderPlaced),
                new BasicProperties(), Encoding.UTF8.GetBytes("{\"Customer\":\"ada\"}"));

        private static TaskCompletionSource BlockHandler(HandledOrders handled, TaskCompletionSource? started = null)
        {
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            handled.BeforeHandle = () =>
            {
                started?.TrySetResult();
                return gate.Task;
            };
            return gate;
        }

        private static List<string> Names(FakeBroker fake) => fake.ChannelRecorder.Calls.Select(c => c.Method.Name).ToList();

        [Fact]
        public async Task StopConsuming_WaitsUntilTheRunningMessageIsAcknowledged()
        {
            var (provider, broker, fake, handled) = await Subscribed();
            await using var _ = provider;
            var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var gate = BlockHandler(handled, started);

            var delivery = broker.HandleDeliveryAsync(fake.Channel, Delivery(1));
            await started.Task;
            var stop = broker.StopConsumingAsync();
            await Task.Delay(100);

            Assert.False(stop.IsCompleted);
            Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicCancelAsync)));
            gate.SetResult();
            await stop;
            await delivery;
            Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicAckAsync)));
        }

        [Fact]
        public async Task StopConsuming_GivesUpWaitingWhenTheTokenIsCancelled()
        {
            var (provider, broker, fake, handled) = await Subscribed();
            await using var _ = provider;
            var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var gate = BlockHandler(handled, started);
            var delivery = broker.HandleDeliveryAsync(fake.Channel, Delivery(1));
            await started.Task;
            using var cts = new CancellationTokenSource();

            var stop = broker.StopConsumingAsync(cts.Token);
            await Task.Delay(50);
            cts.Cancel();

            await stop.WaitAsync(TimeSpan.FromSeconds(5));
            gate.SetResult();
            await delivery;
        }

        [Fact]
        public async Task DeliveriesAfterStop_AreRequeuedWithoutRunningTheHandler()
        {
            var (provider, broker, fake, handled) = await Subscribed();
            await using var _ = provider;
            await broker.StopConsumingAsync();

            await broker.HandleDeliveryAsync(fake.Channel, Delivery(5));

            Assert.Empty(handled.Items);
            var nack = Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicNackAsync)));
            Assert.Equal(5ul, nack.Arg<ulong>("deliveryTag"));
            Assert.True(nack.Arg<bool>("requeue"));
        }

        [Fact]
        public async Task SubscribingAfterStop_AcceptsDeliveriesAgain()
        {
            var (provider, broker, fake, handled) = await Subscribed();
            await using var _ = provider;
            await broker.StopConsumingAsync();

            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            await broker.HandleDeliveryAsync(fake.Channel, Delivery(6));

            Assert.Single(handled.Items);
            Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicAckAsync)));
        }

        [Fact]
        public async Task StopConsumingWithNothingInFlight_ReturnsAtOnce()
        {
            var (provider, broker, _, _) = await Subscribed();
            await using var _p = provider;

            await broker.StopConsumingAsync().WaitAsync(TimeSpan.FromSeconds(1));
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Dispose_AcknowledgesTheRunningMessageBeforeClosingTheConsumerChannel(bool async)
        {
            var (provider, broker, fake, handled) = await Subscribed();
            await using var _ = provider;
            var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var gate = BlockHandler(handled, started);
            var delivery = broker.HandleDeliveryAsync(fake.Channel, Delivery(1));
            await started.Task;

            var dispose = async ? broker.DisposeAsync().AsTask() : Task.Run(broker.Dispose);
            await Task.Delay(100);
            Assert.False(dispose.IsCompleted);
            gate.SetResult();
            await dispose;
            await delivery;

            var names = Names(fake);
            var ack = names.IndexOf(nameof(IChannel.BasicAckAsync));
            var close = names.FindIndex(n => n is nameof(IAsyncDisposable.DisposeAsync) or nameof(IDisposable.Dispose));
            Assert.True(ack >= 0 && close > ack, string.Join(",", names));
            Assert.True(names.IndexOf(nameof(IChannel.BasicCancelAsync)) < ack, string.Join(",", names));
        }

        [Fact]
        public async Task Dispose_WaitsNoLongerThanShutdownTimeout()
        {
            var (provider, broker, fake, handled) = await Subscribed(o => o.ShutdownTimeout = TimeSpan.FromMilliseconds(200));
            await using var _ = provider;
            var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var gate = BlockHandler(handled, started);
            var delivery = broker.HandleDeliveryAsync(fake.Channel, Delivery(1));
            await started.Task;

            await broker.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));

            gate.SetResult();
            await delivery;
        }

        [Fact]
        public async Task StoppedBroker_DoesNotResumeConsumingAfterAChannelClose()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake);
            var broker = (EventBusRabbitMQ)provider.GetRequiredService<IEventBroker>();
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            var shutdownHandler = (AsyncEventHandler<ShutdownEventArgs>)fake.ChannelRecorder.CallsTo("add_" + nameof(IChannel.ChannelShutdownAsync)).Single().Args[0]!;
            await broker.StopConsumingAsync();
            fake.ChannelRecorder.Calls.Clear();

            await shutdownHandler(fake.Channel, new ShutdownEventArgs(ShutdownInitiator.Peer, 406, "PRECONDITION_FAILED"));
            await broker.ConsumerRecovery;

            Assert.Empty(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicConsumeAsync)));
        }

        [Fact]
        public void NegativeShutdownTimeout_FailsWhenTheBrokerIsCreated()
        {
            using var provider = TestHost.Build(new FakeBroker(), o => o.ShutdownTimeout = TimeSpan.FromSeconds(-1));

            Assert.Throws<ArgumentOutOfRangeException>(() => provider.GetRequiredService<IEventBroker>());
        }
    }
}
