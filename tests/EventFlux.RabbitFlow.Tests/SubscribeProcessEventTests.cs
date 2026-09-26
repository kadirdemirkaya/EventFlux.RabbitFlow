using EventFlux.RabbitFlow.Tests.Events;
using EventFlux.RabbitFlow.Tests.Fakes;
using EventFlux.RabbitMQ;
using EventFlux.RabbitMQ.Abstractions;
using EventFlux.RabbitMQ.Context;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace EventFlux.RabbitFlow.Tests
{
    public class SubscribeProcessEventTests
    {
        private const string LegacyPayload = "{\"OrderId\":\"3f2c7a51-9d4e-4b8a-a6f1-0c2d9e8b7a61\",\"Amount\":42.5,\"Customer\":\"ada\",\"ShouldFail\":false}";

        private sealed class TestEventProcessor : SubscribeProcessEvent
        {
            public TestEventProcessor(IServiceProvider serviceProvider)
                : base(serviceProvider.GetRequiredService<ILogger<IEventBroker>>(),
                    serviceProvider.GetRequiredService<IEventBusSubscriptionsManager>(),
                    serviceProvider.GetRequiredService<IServiceScopeFactory>(),
                    "tests",
                    serviceProvider.GetRequiredService<IEventFluxContextAccessor>())
            {
            }

            public Task Process(string eventName, string message)
                => ProcessEvent(eventName, message);

            public Task Process(string eventName, string message, IDictionary<string, object?>? headers, CancellationToken cancellationToken)
                => ProcessEvent(eventName, message, headers, cancellationToken);
        }

        private static (ServiceProvider Provider, TestEventProcessor Processor) CreateProcessor()
        {
            var provider = TestHost.Build(new FakeBroker());
            provider.GetRequiredService<IEventBusSubscriptionsManager>().AddSubscription<OrderPlaced, OrderPlacedHandler>();
            return (provider, new TestEventProcessor(provider));
        }

        [Fact]
        public async Task ProcessEvent_DispatchesThroughEventFluxEventBusToHandler()
        {
            var (provider, processor) = CreateProcessor();
            await using var _ = provider;

            await processor.Process(nameof(OrderPlaced), LegacyPayload);

            var handled = Assert.Single(provider.GetRequiredService<HandledOrders>().Items);
            Assert.Equal(Guid.Parse("3f2c7a51-9d4e-4b8a-a6f1-0c2d9e8b7a61"), handled.Event.OrderId);
            Assert.Equal(42.5m, handled.Event.Amount);
            Assert.Equal("ada", handled.Event.Customer);
        }

        [Fact]
        public async Task ProcessEvent_PassesCancellationTokenToHandler()
        {
            var (provider, processor) = CreateProcessor();
            await using var _ = provider;
            using var cancellation = new CancellationTokenSource();

            await processor.Process(nameof(OrderPlaced), LegacyPayload, null, cancellation.Token);

            var handled = Assert.Single(provider.GetRequiredService<HandledOrders>().Items);
            Assert.Equal(cancellation.Token, handled.Token);
        }

        [Fact]
        public async Task ProcessEvent_ExposesHeadersThroughContextAccessor()
        {
            var (provider, processor) = CreateProcessor();
            await using var _ = provider;
            var headers = new Dictionary<string, object?>
            {
                ["UserId"] = System.Text.Encoding.UTF8.GetBytes("12345"),
                ["Attempt"] = 2
            };

            await processor.Process(nameof(OrderPlaced), LegacyPayload, headers, CancellationToken.None);

            var context = Assert.Single(provider.GetRequiredService<HandledOrders>().Items).Context;
            Assert.NotNull(context);
            Assert.Equal("12345", context!.Items["UserId"]);
            Assert.Equal(2, context.Items["Attempt"]);
        }

        [Fact]
        public async Task ProcessEvent_PropagatesHandlerException()
        {
            var (provider, processor) = CreateProcessor();
            await using var _ = provider;

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => processor.Process(nameof(OrderPlaced), "{\"ShouldFail\":true}"));
        }

        [Fact]
        public async Task ProcessEvent_IgnoresEventWithoutSubscription()
        {
            var (provider, processor) = CreateProcessor();
            await using var _ = provider;

            await processor.Process("UnknownEvent", "{}");

            Assert.Empty(provider.GetRequiredService<HandledOrders>().Items);
        }
    }
}
