using EventFlux.RabbitFlow.Tests.Events;
using EventFlux.RabbitFlow.Tests.Fakes;
using EventFlux.RabbitMQ;
using EventFlux.RabbitMQ.Abstractions;
using EventFlux.RabbitMQ.Context;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using Xunit;

namespace EventFlux.RabbitFlow.Tests
{
    public class MessageFormatTests
    {
        private const string LegacyPayload = "{\"OrderId\":\"3f2c7a51-9d4e-4b8a-a6f1-0c2d9e8b7a61\",\"Amount\":42.5,\"Customer\":\"ada\",\"ShouldFail\":false}";

        private static OrderPlaced LegacyEvent() => new OrderPlaced
        {
            OrderId = Guid.Parse("3f2c7a51-9d4e-4b8a-a6f1-0c2d9e8b7a61"),
            Amount = 42.5m,
            Customer = "ada"
        };

        [Fact]
        public async Task PublishAsync_WritesSameBodyRoutingKeyAndExchangeAsPreviousRelease()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake);

            await provider.GetRequiredService<IEventBroker>().PublishAsync(LegacyEvent());

            var published = Assert.Single(fake.Published);
            Assert.Equal(TestHost.ServiceName, published.Arg<string>("exchange"));
            Assert.Equal(nameof(OrderPlaced), published.Arg<string>("routingKey"));
            Assert.True(published.Arg<bool>("mandatory"));
            Assert.Equal(LegacyPayload, Encoding.UTF8.GetString(published.Arg<ReadOnlyMemory<byte>>("body").Span));

            var properties = published.Arg<BasicProperties>("basicProperties");
            Assert.Equal(DeliveryModes.Persistent, properties.DeliveryMode);
            Assert.Null(properties.Headers);
        }

        [Fact]
        public async Task PublishAsync_WithExchangeAndToken_UsesGivenExchange()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake);

            await provider.GetRequiredService<IEventBroker>().PublishAsync(LegacyEvent(), "eventbus_flow_2", CancellationToken.None);

            Assert.Equal("eventbus_flow_2", Assert.Single(fake.Published).Arg<string>("exchange"));
        }

        [Fact]
        public async Task PublishAsync_CopiesContextItemsToHeaders()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake);
            var contextAccessor = provider.GetRequiredService<IEventFluxContextAccessor>();
            contextAccessor.Context = new EventFluxContext();
            contextAccessor.Context.Items["UserId"] = "12345";

            await provider.GetRequiredService<IEventBroker>().PublishAsync(LegacyEvent());

            var headers = Assert.Single(fake.Published).Arg<BasicProperties>("basicProperties").Headers!;
            Assert.Equal("12345", headers["UserId"]);
        }

        [Fact]
        public async Task PublishAsync_HonoursCancellation()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake);
            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => provider.GetRequiredService<IEventBroker>().PublishAsync(LegacyEvent(), cancellation.Token));

            Assert.Empty(fake.Published);
        }

        [Fact]
        public async Task PayloadFromPreviousRelease_DeserializesToSameEvent()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake);
            var broker = (EventBusRabbitMQ)provider.GetRequiredService<IEventBroker>();
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();

            await broker.HandleDeliveryAsync(fake.Channel, new BasicDeliverEventArgs(
                "consumer", 1, false, TestHost.ServiceName, nameof(OrderPlaced), new BasicProperties(), Encoding.UTF8.GetBytes(LegacyPayload)));

            var handled = Assert.Single(provider.GetRequiredService<HandledOrders>().Items).Event;
            Assert.Equal(LegacyEvent().OrderId, handled.OrderId);
            Assert.Equal(42.5m, handled.Amount);
            Assert.Equal("ada", handled.Customer);
        }

        [Fact]
        public void ExtensionsAbstractions_MatchTargetFrameworkMajor()
        {
            var expectedMajor = Environment.Version.Major;
            var references = typeof(EventBusRabbitMQ).Assembly.GetReferencedAssemblies()
                .Where(a => a.Name!.StartsWith("Microsoft.Extensions.", StringComparison.Ordinal))
                .ToList();

            Assert.NotEmpty(references);
            Assert.All(references, reference => Assert.Equal(expectedMajor, reference.Version!.Major));
        }
    }
}
