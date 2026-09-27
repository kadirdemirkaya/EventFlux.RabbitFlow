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
    public class RetryLimitTests
    {
        private const ulong DeliveryTag = 11;
        private static readonly string Queue = $"{TestHost.ServiceName}_{nameof(OrderPlaced)}";
        private static readonly string DeadLetterExchange = $"{TestHost.ServiceName}_dead_letter";

        private static async Task<(ServiceProvider Provider, EventBusRabbitMQ Broker, FakeBroker Fake)> CreateSubscribedBroker(Action<RabbitFlowOptions> configure, bool clearCalls = true)
        {
            var fake = new FakeBroker();
            var provider = TestHost.Build(fake, configure);
            var broker = (EventBusRabbitMQ)provider.GetRequiredService<IEventBroker>();
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            if (clearCalls) fake.ChannelRecorder.Calls.Clear();
            return (provider, broker, fake);
        }

        private static BasicDeliverEventArgs FirstDelivery(string body)
            => Delivery(body, TestHost.ServiceName, nameof(OrderPlaced), new Dictionary<string, object?> { ["CorrelationId"] = "c-1" });

        private static BasicDeliverEventArgs Delivery(string body, string exchange, string routingKey, IDictionary<string, object?> headers)
            => new BasicDeliverEventArgs(
                consumerTag: "consumer",
                deliveryTag: DeliveryTag,
                redelivered: false,
                exchange: exchange,
                routingKey: routingKey,
                properties: new BasicProperties { Headers = headers },
                body: Encoding.UTF8.GetBytes(body),
                cancellationToken: default);

        private static BasicDeliverEventArgs RetriedDelivery(string body, object retryCount)
            => Delivery(body, string.Empty, Queue, new Dictionary<string, object?>
            {
                ["CorrelationId"] = "c-1",
                ["x-retry-count"] = retryCount,
                ["x-original-exchange"] = Encoding.UTF8.GetBytes(TestHost.ServiceName),
                ["x-original-routing-key"] = Encoding.UTF8.GetBytes(nameof(OrderPlaced))
            });

        private static IDictionary<string, object?> HeadersOf(Invocation publish)
            => publish.Arg<BasicProperties>("basicProperties").Headers!;

        private static string Text(object? value) => value is byte[] bytes ? Encoding.UTF8.GetString(bytes) : value?.ToString() ?? string.Empty;

        [Fact]
        public async Task FirstFailure_RepublishesToItsOwnQueueWithRetryCountAndAcks()
        {
            var (provider, broker, fake) = await CreateSubscribedBroker(o => o.MaxDeliveryAttempts = 3);
            await using var _ = provider;

            await broker.HandleDeliveryAsync(fake.Channel, FirstDelivery("{\"ShouldFail\":true}"));

            var publish = Assert.Single(fake.Published);
            Assert.Equal(string.Empty, publish.Arg<string>("exchange"));
            Assert.Equal(Queue, publish.Arg<string>("routingKey"));
            Assert.Equal("{\"ShouldFail\":true}", Encoding.UTF8.GetString(publish.Arg<ReadOnlyMemory<byte>>("body").Span));
            var properties = publish.Arg<BasicProperties>("basicProperties");
            Assert.Equal(DeliveryModes.Persistent, properties.DeliveryMode);
            Assert.Null(properties.Expiration);
            Assert.Equal(1, properties.Headers!["x-retry-count"]);
            Assert.Equal("c-1", properties.Headers["CorrelationId"]);
            Assert.Equal(TestHost.ServiceName, properties.Headers["x-original-exchange"]);
            Assert.Equal(nameof(OrderPlaced), properties.Headers["x-original-routing-key"]);
            Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicAckAsync)));
            Assert.Empty(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicNackAsync)));
        }

        [Fact]
        public async Task RetriedFailure_IncrementsCountAndKeepsTheOriginalRoute()
        {
            var (provider, broker, fake) = await CreateSubscribedBroker(o => o.MaxDeliveryAttempts = 3);
            await using var _ = provider;

            await broker.HandleDeliveryAsync(fake.Channel, RetriedDelivery("{\"ShouldFail\":true}", 1));

            var headers = HeadersOf(Assert.Single(fake.Published));
            Assert.Equal(2, headers["x-retry-count"]);
            Assert.Equal(TestHost.ServiceName, Text(headers["x-original-exchange"]));
            Assert.Equal(nameof(OrderPlaced), Text(headers["x-original-routing-key"]));
        }

        [Fact]
        public async Task LastAttempt_MovesTheMessageToTheDeadLetterQueue()
        {
            var (provider, broker, fake) = await CreateSubscribedBroker(o => o.MaxDeliveryAttempts = 3);
            await using var _ = provider;

            await broker.HandleDeliveryAsync(fake.Channel, RetriedDelivery("{\"ShouldFail\":true}", 2));

            var publish = Assert.Single(fake.Published);
            Assert.Equal(DeadLetterExchange, publish.Arg<string>("exchange"));
            Assert.Equal(nameof(OrderPlaced), publish.Arg<string>("routingKey"));
            var headers = HeadersOf(publish);
            Assert.Equal(typeof(InvalidOperationException).FullName, headers["x-exception-type"]);
            Assert.Equal(2, headers["x-retry-count"]);
            Assert.Equal(TestHost.ServiceName, Text(headers["x-original-exchange"]));
            Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicAckAsync)));
        }

        [Fact]
        public async Task SingleAttempt_DeadLettersOnFirstFailureEvenInRequeueMode()
        {
            var (provider, broker, fake) = await CreateSubscribedBroker(o => o.MaxDeliveryAttempts = 1);
            await using var _ = provider;

            await broker.HandleDeliveryAsync(fake.Channel, FirstDelivery("{\"ShouldFail\":true}"));

            Assert.Equal(DeadLetterExchange, Assert.Single(fake.Published).Arg<string>("exchange"));
            Assert.Empty(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicNackAsync)));
        }

        [Fact]
        public async Task RetriedMessage_IsDispatchedToTheEventHandler()
        {
            var (provider, broker, fake) = await CreateSubscribedBroker(o => o.MaxDeliveryAttempts = 3);
            await using var _ = provider;

            await broker.HandleDeliveryAsync(fake.Channel, RetriedDelivery("{\"Customer\":\"second-try\"}", 1));

            var handled = Assert.Single(provider.GetRequiredService<HandledOrders>().Items);
            Assert.Equal("second-try", handled.Event.Customer);
            Assert.Equal("c-1", handled.Context!.Items["CorrelationId"]);
            Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicAckAsync)));
            Assert.Empty(fake.Published);
        }

        [Fact]
        public async Task RetryPublishFailure_RequeuesTheMessage()
        {
            var (provider, broker, fake) = await CreateSubscribedBroker(o => o.MaxDeliveryAttempts = 3);
            await using var _ = provider;
            fake.ChannelRecorder.Responder = (method, _) => method.Name == nameof(IChannel.BasicPublishAsync)
                ? ValueTask.FromException(new InvalidOperationException("broker unavailable"))
                : null;

            await broker.HandleDeliveryAsync(fake.Channel, FirstDelivery("{\"ShouldFail\":true}"));

            Assert.Empty(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicAckAsync)));
            Assert.True(Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicNackAsync))).Arg<bool>("requeue"));
        }

        [Fact]
        public async Task RedeliveryDelay_PublishesToTheRetryQueueWithExpiration()
        {
            var (provider, broker, fake) = await CreateSubscribedBroker(o =>
            {
                o.MaxDeliveryAttempts = 3;
                o.RedeliveryDelay = TimeSpan.FromSeconds(5);
            });
            await using var _ = provider;

            await broker.HandleDeliveryAsync(fake.Channel, FirstDelivery("{\"ShouldFail\":true}"));

            var publish = Assert.Single(fake.Published);
            Assert.Equal(string.Empty, publish.Arg<string>("exchange"));
            Assert.Equal($"{Queue}_retry", publish.Arg<string>("routingKey"));
            Assert.Equal("5000", publish.Arg<BasicProperties>("basicProperties").Expiration);
        }

        [Fact]
        public async Task RedeliveryDelay_DeclaresRetryQueueThatReturnsToTheEventQueue()
        {
            var (provider, _, fake) = await CreateSubscribedBroker(o =>
            {
                o.MaxDeliveryAttempts = 3;
                o.RedeliveryDelay = TimeSpan.FromSeconds(5);
            }, clearCalls: false);
            await using var _p = provider;

            var retryQueue = Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.QueueDeclareAsync)), c => c.Arg<string>("queue") == $"{Queue}_retry");
            Assert.True(retryQueue.Arg<bool>("durable"));
            var arguments = retryQueue.Arg<IDictionary<string, object?>>("arguments");
            Assert.Equal(string.Empty, arguments["x-dead-letter-exchange"]);
            Assert.Equal(Queue, arguments["x-dead-letter-routing-key"]);
            var eventQueue = Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.QueueDeclareAsync)), c => c.Arg<string>("queue") == Queue);
            Assert.Null(eventQueue.Arg<IDictionary<string, object?>?>("arguments"));
        }

        [Fact]
        public async Task LimitInRequeueMode_DeclaresDeadLetterTopology()
        {
            var (provider, _, fake) = await CreateSubscribedBroker(o => o.MaxDeliveryAttempts = 3, clearCalls: false);
            await using var _p = provider;

            var queues = fake.ChannelRecorder.CallsTo(nameof(IChannel.QueueDeclareAsync)).Select(c => c.Arg<string>("queue")).ToList();
            Assert.Contains($"{Queue}_dead_letter", queues);
            Assert.DoesNotContain($"{Queue}_retry", queues);
        }

        [Fact]
        public async Task WithoutLimit_DelayIsIgnoredAndNoExtraQueuesAreDeclared()
        {
            var (provider, broker, fake) = await CreateSubscribedBroker(o => o.RedeliveryDelay = TimeSpan.FromSeconds(5), clearCalls: false);
            await using var _ = provider;

            Assert.Equal(new[] { Queue }, fake.ChannelRecorder.CallsTo(nameof(IChannel.QueueDeclareAsync)).Select(c => c.Arg<string>("queue")));
            fake.ChannelRecorder.Calls.Clear();

            await broker.HandleDeliveryAsync(fake.Channel, FirstDelivery("{\"ShouldFail\":true}"));

            Assert.Empty(fake.Published);
            Assert.True(Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicNackAsync))).Arg<bool>("requeue"));
        }

        [Fact]
        public async Task Retry_DropsBrokerDeathHeaders()
        {
            var (provider, broker, fake) = await CreateSubscribedBroker(o => o.MaxDeliveryAttempts = 5);
            await using var _ = provider;
            var delivery = RetriedDelivery("{\"ShouldFail\":true}", 1);
            delivery.BasicProperties.Headers!["x-death"] = new List<object?>();
            delivery.BasicProperties.Headers["x-first-death-queue"] = Encoding.UTF8.GetBytes($"{Queue}_retry");

            await broker.HandleDeliveryAsync(fake.Channel, delivery);

            var headers = HeadersOf(Assert.Single(fake.Published));
            Assert.False(headers.ContainsKey("x-death"));
            Assert.False(headers.ContainsKey("x-first-death-queue"));
        }

        [Fact]
        public async Task DeadLetterModeWithoutLimit_OverwritesOriginalRouteHeadersAsBefore()
        {
            var (provider, broker, fake) = await CreateSubscribedBroker(o => o.OnFailure = RabbitFlowFailureMode.DeadLetter);
            await using var _ = provider;
            var delivery = Delivery("{\"ShouldFail\":true}", TestHost.ServiceName, nameof(OrderPlaced),
                new Dictionary<string, object?> { ["x-original-exchange"] = "somewhere-else" });

            await broker.HandleDeliveryAsync(fake.Channel, delivery);

            Assert.Equal(TestHost.ServiceName, HeadersOf(Assert.Single(fake.Published))["x-original-exchange"]);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        public void InvalidMaxDeliveryAttempts_FailsWhenTheBrokerIsCreated(int attempts)
        {
            using var provider = TestHost.Build(new FakeBroker(), o => o.MaxDeliveryAttempts = attempts);

            Assert.Throws<ArgumentOutOfRangeException>(() => provider.GetRequiredService<IEventBroker>());
        }

        [Fact]
        public void NegativeRedeliveryDelay_FailsWhenTheBrokerIsCreated()
        {
            using var provider = TestHost.Build(new FakeBroker(), o =>
            {
                o.MaxDeliveryAttempts = 3;
                o.RedeliveryDelay = TimeSpan.FromSeconds(-1);
            });

            Assert.Throws<ArgumentOutOfRangeException>(() => provider.GetRequiredService<IEventBroker>());
        }

        [Fact]
        public void RetryCountHeader_IsReadFromEveryWireType()
        {
            Assert.Equal(0, EventBusRabbitMQ.GetRetryCount(null));
            Assert.Equal(0, EventBusRabbitMQ.GetRetryCount(new Dictionary<string, object?>()));
            Assert.Equal(3, EventBusRabbitMQ.GetRetryCount(new Dictionary<string, object?> { ["x-retry-count"] = 3 }));
            Assert.Equal(3, EventBusRabbitMQ.GetRetryCount(new Dictionary<string, object?> { ["x-retry-count"] = 3L }));
            Assert.Equal(3, EventBusRabbitMQ.GetRetryCount(new Dictionary<string, object?> { ["x-retry-count"] = (short)3 }));
            Assert.Equal(3, EventBusRabbitMQ.GetRetryCount(new Dictionary<string, object?> { ["x-retry-count"] = (byte)3 }));
            Assert.Equal(3, EventBusRabbitMQ.GetRetryCount(new Dictionary<string, object?> { ["x-retry-count"] = Encoding.UTF8.GetBytes("3") }));
            Assert.Equal(3, EventBusRabbitMQ.GetRetryCount(new Dictionary<string, object?> { ["x-retry-count"] = "3" }));
            Assert.Equal(0, EventBusRabbitMQ.GetRetryCount(new Dictionary<string, object?> { ["x-retry-count"] = "abc" }));
            Assert.Equal(0, EventBusRabbitMQ.GetRetryCount(new Dictionary<string, object?> { ["x-retry-count"] = -5 }));
        }
    }
}
