using EventFlux.RabbitFlow.Tests.Events;
using EventFlux.RabbitFlow.Tests.Fakes;
using EventFlux.RabbitMQ;
using EventFlux.RabbitMQ.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System.Text;
using Xunit;

namespace EventFlux.RabbitFlow.Tests
{
    public class PublisherConfirmsTests
    {
        private static IReadOnlyList<CreateChannelOptions?> ChannelOptions(FakeBroker fake)
            => fake.Connections.SelectMany(c => c.Recorder.CallsTo(nameof(IConnection.CreateChannelAsync)))
                .Select(c => (CreateChannelOptions?)c.Args[0])
                .ToList();

        private static void FailPublishes(FakeBroker fake, Func<Exception?> failure)
            => fake.ChannelRecorder.Responder = (method, _) =>
                method.Name == nameof(IChannel.BasicPublishAsync) && failure() is { } ex ? ValueTask.FromException(ex) : null;

        private static BasicDeliverEventArgs FailingDelivery(IDictionary<string, object?>? headers = null)
            => new BasicDeliverEventArgs("consumer", 3, false, TestHost.ServiceName, nameof(OrderPlaced),
                new BasicProperties { Headers = headers ?? new Dictionary<string, object?>() },
                Encoding.UTF8.GetBytes("{\"ShouldFail\":true}"));

        [Fact]
        public async Task ByDefault_ChannelsAreCreatedWithoutOptions()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake);
            var broker = provider.GetRequiredService<IEventBroker>();

            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            await broker.PublishAsync(new OrderPlaced());

            Assert.All(ChannelOptions(fake), Assert.Null);
        }

        [Fact]
        public async Task Enabled_PublishAndConsumerChannelsTrackConfirmations()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake, o => o.PublisherConfirms = true);
            var broker = provider.GetRequiredService<IEventBroker>();

            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            await broker.PublishAsync(new OrderPlaced());

            var options = ChannelOptions(fake);
            var confirming = options.Where(o => o != null).ToList();
            Assert.Equal(2, confirming.Count);
            Assert.All(confirming, o =>
            {
                Assert.True(o!.PublisherConfirmationsEnabled);
                Assert.True(o.PublisherConfirmationTrackingEnabled);
                Assert.Equal((ushort)1, o.ConsumerDispatchConcurrency);
            });
            Assert.Single(options, o => o == null);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task DeadLetterOrLimit_ConsumerChannelConfirmsEvenWhenPublisherConfirmsIsOff(bool deadLetterMode)
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake, o =>
            {
                if (deadLetterMode) o.OnFailure = RabbitFlowFailureMode.DeadLetter;
                else o.MaxDeliveryAttempts = 3;
            });
            var broker = provider.GetRequiredService<IEventBroker>();

            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            await broker.PublishAsync(new OrderPlaced());

            var options = ChannelOptions(fake);
            var confirming = Assert.Single(options, o => o != null);
            Assert.True(confirming!.PublisherConfirmationTrackingEnabled);
            var consumerChannelIndex = fake.Connections[0].Recorder.Calls
                .Where(c => c.Method.Name == nameof(IConnection.CreateChannelAsync))
                .Select((c, i) => (c, i))
                .Single(x => x.c.Args[0] != null).i;
            Assert.Equal(1, consumerChannelIndex);
        }

        [Fact]
        public async Task Enabled_BrokerNack_IsRetriedThenThrown()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake, o =>
            {
                o.PublisherConfirms = true;
                o.RetryCount = 1;
            });
            var broker = provider.GetRequiredService<IEventBroker>();
            FailPublishes(fake, () => new PublishException(1, isReturn: false));

            var ex = await Assert.ThrowsAsync<PublishException>(() => broker.PublishAsync(new OrderPlaced()));

            Assert.False(ex.IsReturn);
            Assert.Equal(2, fake.Published.Count);
        }

        [Fact]
        public async Task Enabled_NackThenAck_Succeeds()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake, o =>
            {
                o.PublisherConfirms = true;
                o.RetryCount = 1;
            });
            var broker = provider.GetRequiredService<IEventBroker>();
            var calls = 0;
            FailPublishes(fake, () => ++calls == 1 ? new PublishException(1, isReturn: false) : null);

            await broker.PublishAsync(new OrderPlaced());

            Assert.Equal(2, fake.Published.Count);
        }

        [Fact]
        public async Task Enabled_UnroutableEvent_IsNotAnErrorAndIsNotRetried()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake, o => o.PublisherConfirms = true);
            var broker = provider.GetRequiredService<IEventBroker>();
            FailPublishes(fake, () => new PublishException(1, isReturn: true));

            await broker.PublishAsync(new OrderPlaced());

            Assert.Single(fake.Published);
        }

        [Fact]
        public async Task Enabled_DeadLetterPublishNotRoutable_RequeuesTheMessage()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake, o =>
            {
                o.PublisherConfirms = true;
                o.OnFailure = RabbitFlowFailureMode.DeadLetter;
            });
            var broker = (EventBusRabbitMQ)provider.GetRequiredService<IEventBroker>();
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            fake.ChannelRecorder.Calls.Clear();
            FailPublishes(fake, () => new PublishException(1, isReturn: true));

            await broker.HandleDeliveryAsync(fake.Channel, FailingDelivery());

            Assert.Empty(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicAckAsync)));
            Assert.True(Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicNackAsync))).Arg<bool>("requeue"));
        }

        [Fact]
        public async Task Enabled_RetryPublishNacked_RequeuesTheMessage()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake, o =>
            {
                o.PublisherConfirms = true;
                o.MaxDeliveryAttempts = 3;
            });
            var broker = (EventBusRabbitMQ)provider.GetRequiredService<IEventBroker>();
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            fake.ChannelRecorder.Calls.Clear();
            FailPublishes(fake, () => new PublishException(1, isReturn: false));

            await broker.HandleDeliveryAsync(fake.Channel, FailingDelivery());

            Assert.Empty(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicAckAsync)));
            Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicNackAsync)));
        }

        [Fact]
        public async Task SequenceHeader_IsNotPassedToTheHandlerContext()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake);
            var broker = (EventBusRabbitMQ)provider.GetRequiredService<IEventBroker>();
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            var delivery = new BasicDeliverEventArgs("consumer", 3, false, TestHost.ServiceName, nameof(OrderPlaced),
                new BasicProperties { Headers = new Dictionary<string, object?> { ["x-dotnet-pub-seq-no"] = Encoding.UTF8.GetBytes("12"), ["CorrelationId"] = "c-9" } },
                Encoding.UTF8.GetBytes("{}"));

            await broker.HandleDeliveryAsync(fake.Channel, delivery);

            var context = Assert.Single(provider.GetRequiredService<HandledOrders>().Items).Context!;
            Assert.Equal("c-9", context.Items["CorrelationId"]);
            Assert.False(context.Items.ContainsKey("x-dotnet-pub-seq-no"));
        }

        [Fact]
        public async Task SequenceHeader_IsNotCopiedIntoRetries()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake, o => o.MaxDeliveryAttempts = 3);
            var broker = (EventBusRabbitMQ)provider.GetRequiredService<IEventBroker>();
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            fake.ChannelRecorder.Calls.Clear();

            await broker.HandleDeliveryAsync(fake.Channel, FailingDelivery(new Dictionary<string, object?> { ["x-dotnet-pub-seq-no"] = Encoding.UTF8.GetBytes("12") }));

            var headers = Assert.Single(fake.Published).Arg<BasicProperties>("basicProperties").Headers!;
            Assert.False(headers.ContainsKey("x-dotnet-pub-seq-no"));
        }

        [Fact]
        public async Task CustomConnection_DefaultOverloadFallsBackToThePlainChannel()
        {
            IPersistenceConnection custom = new PlainConnection();

            await custom.CreateChannelAsync(new CreateChannelOptions(true, true), CancellationToken.None);

            Assert.Equal(1, ((PlainConnection)custom).PlainCalls);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => custom.CreateChannelAsync(null, new CancellationToken(canceled: true)));
        }

        private sealed class PlainConnection : IPersistenceConnection
        {
            public int PlainCalls { get; private set; }

            public bool IsConnected => true;

            public bool TryConnect() => true;

            public Task<IChannel> CreateChannelAsync()
            {
                PlainCalls++;
                return Task.FromResult(RecordingProxy.Create<IChannel>().Instance);
            }

            public void Dispose()
            {
            }
        }
    }
}
