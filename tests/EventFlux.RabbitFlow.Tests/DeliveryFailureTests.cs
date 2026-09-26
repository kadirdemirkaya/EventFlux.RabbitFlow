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
    public class DeliveryFailureTests
    {
        private const ulong DeliveryTag = 7;

        private static async Task<(ServiceProvider Provider, EventBusRabbitMQ Broker, FakeBroker Fake)> CreateSubscribedBroker(Action<RabbitFlowOptions>? configure = null)
        {
            var fake = new FakeBroker();
            var provider = TestHost.Build(fake, configure);
            var broker = (EventBusRabbitMQ)provider.GetRequiredService<IEventBroker>();
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            fake.ChannelRecorder.Calls.Clear();
            return (provider, broker, fake);
        }

        private static BasicDeliverEventArgs Delivery(string body, CancellationToken cancellationToken = default)
            => new BasicDeliverEventArgs(
                consumerTag: "consumer",
                deliveryTag: DeliveryTag,
                redelivered: false,
                exchange: TestHost.ServiceName,
                routingKey: nameof(OrderPlaced),
                properties: new BasicProperties { Headers = new Dictionary<string, object?> { ["CorrelationId"] = "c-1" } },
                body: Encoding.UTF8.GetBytes(body),
                cancellationToken: cancellationToken);

        [Fact]
        public async Task SuccessfulHandler_AcksMessage()
        {
            var (provider, broker, fake) = await CreateSubscribedBroker();
            await using var _ = provider;

            await broker.HandleDeliveryAsync(fake.Channel, Delivery("{\"Customer\":\"ada\"}"));

            var ack = Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicAckAsync)));
            Assert.Equal(DeliveryTag, ack.Arg<ulong>("deliveryTag"));
            Assert.Empty(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicNackAsync)));
        }

        [Fact]
        public async Task FailingHandler_IsRequeuedAndNotAckedByDefault()
        {
            var (provider, broker, fake) = await CreateSubscribedBroker();
            await using var _ = provider;

            await broker.HandleDeliveryAsync(fake.Channel, Delivery("{\"ShouldFail\":true}"));

            Assert.Empty(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicAckAsync)));
            var nack = Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicNackAsync)));
            Assert.Equal(DeliveryTag, nack.Arg<ulong>("deliveryTag"));
            Assert.True(nack.Arg<bool>("requeue"));
        }

        [Fact]
        public async Task UndeserializableMessage_IsRequeuedAndNotAcked()
        {
            var (provider, broker, fake) = await CreateSubscribedBroker();
            await using var _ = provider;

            await broker.HandleDeliveryAsync(fake.Channel, Delivery("not json"));

            Assert.Empty(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicAckAsync)));
            Assert.True(Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicNackAsync))).Arg<bool>("requeue"));
        }

        [Fact]
        public async Task FailingHandler_WithDeadLetter_MovesMessageToDeadLetterExchange()
        {
            var (provider, broker, fake) = await CreateSubscribedBroker(o => o.OnFailure = RabbitFlowFailureMode.DeadLetter);
            await using var _ = provider;
            const string body = "{\"ShouldFail\":true}";

            await broker.HandleDeliveryAsync(fake.Channel, Delivery(body));

            var published = Assert.Single(fake.Published);
            Assert.Equal($"{TestHost.ServiceName}_dead_letter", published.Arg<string>("exchange"));
            Assert.Equal(nameof(OrderPlaced), published.Arg<string>("routingKey"));
            Assert.Equal(body, Encoding.UTF8.GetString(published.Arg<ReadOnlyMemory<byte>>("body").Span));

            var headers = published.Arg<BasicProperties>("basicProperties").Headers!;
            Assert.Equal("c-1", headers["CorrelationId"]);
            Assert.Equal(typeof(InvalidOperationException).FullName, headers["x-exception-type"]);
            Assert.Equal(nameof(OrderPlaced), headers["x-original-routing-key"]);

            Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicAckAsync)));
            Assert.Empty(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicNackAsync)));
        }

        [Fact]
        public async Task FailingHandler_WithDeadLetter_RequeuesWhenDeadLetterPublishFails()
        {
            var fake = new FakeBroker();
            var publishFails = false;
            fake.ChannelRecorder.Responder = (method, _) =>
                publishFails && method.Name == nameof(IChannel.BasicPublishAsync)
                    ? ValueTask.FromException(new InvalidOperationException("broker unavailable"))
                    : null;

            await using var provider = TestHost.Build(fake, o => o.OnFailure = RabbitFlowFailureMode.DeadLetter);
            var broker = (EventBusRabbitMQ)provider.GetRequiredService<IEventBroker>();
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            fake.ChannelRecorder.Calls.Clear();
            publishFails = true;

            await broker.HandleDeliveryAsync(fake.Channel, Delivery("{\"ShouldFail\":true}"));

            Assert.Empty(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicAckAsync)));
            Assert.True(Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicNackAsync))).Arg<bool>("requeue"));
        }

        [Fact]
        public async Task DeadLetterMode_DeclaresDeadLetterQueueOnSubscribe()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake, o =>
            {
                o.OnFailure = RabbitFlowFailureMode.DeadLetter;
                o.DeadLetterExchange = "orders_failed";
            });

            await provider.GetRequiredService<IEventBroker>().SubscribeAsync<OrderPlaced, OrderPlacedHandler>();

            var queues = fake.ChannelRecorder.CallsTo(nameof(IChannel.QueueDeclareAsync)).Select(c => c.Arg<string>("queue")).ToList();
            Assert.Contains($"{TestHost.ServiceName}_{nameof(OrderPlaced)}_dead_letter", queues);

            var bindings = fake.ChannelRecorder.CallsTo(nameof(IChannel.QueueBindAsync)).Select(c => c.Arg<string>("exchange")).ToList();
            Assert.Contains("orders_failed", bindings);
        }

        [Fact]
        public async Task CancelledDelivery_IsRequeuedEvenInDeadLetterMode()
        {
            var (provider, broker, fake) = await CreateSubscribedBroker(o => o.OnFailure = RabbitFlowFailureMode.DeadLetter);
            await using var _ = provider;
            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();

            await broker.HandleDeliveryAsync(fake.Channel, Delivery("{}", cancellation.Token));

            Assert.Empty(fake.Published);
            Assert.Empty(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicAckAsync)));
            Assert.True(Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicNackAsync))).Arg<bool>("requeue"));
        }

        [Fact]
        public async Task MessageWithoutSubscription_IsAcked()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake);
            var broker = (EventBusRabbitMQ)provider.GetRequiredService<IEventBroker>();

            await broker.HandleDeliveryAsync(fake.Channel, Delivery("{}"));

            Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicAckAsync)));
        }

        [Fact]
        public async Task FakeExceptionPayload_IsProcessedLikeAnyOtherMessage()
        {
            var (provider, broker, fake) = await CreateSubscribedBroker();
            await using var _ = provider;

            await broker.HandleDeliveryAsync(fake.Channel, Delivery("{\"Customer\":\"throw-fake-exception\"}"));

            Assert.Equal("throw-fake-exception", Assert.Single(provider.GetRequiredService<HandledOrders>().Items).Event.Customer);
            Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicAckAsync)));
        }
    }
}
