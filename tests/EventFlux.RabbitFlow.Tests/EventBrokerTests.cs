using EventFlux.Abstractions;
using EventFlux.RabbitFlow.Tests.Events;
using EventFlux.RabbitFlow.Tests.Fakes;
using EventFlux.RabbitMQ;
using EventFlux.RabbitMQ.Abstractions;
using EventFlux.RabbitMQ.Context;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using Xunit;

namespace EventFlux.RabbitFlow.Tests
{
    public class EventBrokerTests
    {
        [Fact]
        public async Task AddEventFluxRabbitFlow_RegistersAlongsideEventFluxAndValidates()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake);

            await using var scope = provider.CreateAsyncScope();
            Assert.NotNull(provider.GetRequiredService<IEventBroker>());
            Assert.NotNull(scope.ServiceProvider.GetRequiredService<IEventBus>());
            Assert.NotNull(provider.GetRequiredService<IEventFluxContextAccessor>());
        }

        [Fact]
        public void AddEventFluxRabbitFlow_RegistersBrokerInfrastructureAsSingletons()
        {
            var services = new ServiceCollection();
            services.AddEventFluxRabbitFlow(typeof(OrderPlaced).Assembly, new FakeBroker().ConnectionFactory, TestHost.ServiceName);

            Assert.Equal(ServiceLifetime.Singleton, services.Single(d => d.ServiceType == typeof(IEventBroker)).Lifetime);
            Assert.Equal(ServiceLifetime.Singleton, services.Single(d => d.ServiceType == typeof(IPersistenceConnection)).Lifetime);
            Assert.Equal(ServiceLifetime.Singleton, services.Single(d => d.ServiceType == typeof(IEventBusSubscriptionsManager)).Lifetime);
            Assert.DoesNotContain(services, d => d.ServiceType == typeof(IHostedService));
        }

        [Fact]
        public async Task Subscription_OutlivesTheScopeThatCreatedIt()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake);

            IEventBroker scopedBroker;
            await using (var scope = provider.CreateAsyncScope())
            {
                scopedBroker = scope.ServiceProvider.GetRequiredService<IEventBroker>();
                await scopedBroker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            }

            await using (var otherScope = provider.CreateAsyncScope())
            {
                Assert.Same(scopedBroker, otherScope.ServiceProvider.GetRequiredService<IEventBroker>());
            }

            Assert.Empty(fake.ChannelRecorder.CallsTo(nameof(IChannel.Dispose)));
            Assert.Empty(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicCancelAsync)));
            Assert.True(provider.GetRequiredService<IEventBusSubscriptionsManager>().HasSubscriptionsForEvent<OrderPlaced>());

            await ((EventBusRabbitMQ)scopedBroker).HandleDeliveryAsync(fake.Channel, new BasicDeliverEventArgs(
                "consumer", 1, false, TestHost.ServiceName, nameof(OrderPlaced), new BasicProperties(), Encoding.UTF8.GetBytes("{}")));

            Assert.Single(provider.GetRequiredService<HandledOrders>().Items);
        }

        [Fact]
        public async Task SubscribeAsync_DeclaresQueueAndStartsConsumerWithManualAck()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake);

            await provider.GetRequiredService<IEventBroker>().SubscribeAsync<OrderPlaced, OrderPlacedHandler>();

            var queue = Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.QueueDeclareAsync)));
            Assert.Equal($"{TestHost.ServiceName}_{nameof(OrderPlaced)}", queue.Arg<string>("queue"));
            Assert.True(queue.Arg<bool>("durable"));

            var binding = Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.QueueBindAsync)));
            Assert.Equal(TestHost.ServiceName, binding.Arg<string>("exchange"));
            Assert.Equal(nameof(OrderPlaced), binding.Arg<string>("routingKey"));

            var consume = Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicConsumeAsync)));
            Assert.False(consume.Arg<bool>("autoAck"));
        }

        [Fact]
        public async Task SubscribeAsync_Twice_DoesNotThrowOrStartSecondConsumer()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake);
            var broker = provider.GetRequiredService<IEventBroker>();

            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();

            Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicConsumeAsync)));
        }

        [Fact]
        public async Task SubscribeAsync_OnSecondExchange_BindsExistingQueue()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake);
            var broker = provider.GetRequiredService<IEventBroker>();

            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>();
            await broker.SubscribeAsync<OrderPlaced, OrderPlacedHandler>("other_exchange");

            var exchanges = fake.ChannelRecorder.CallsTo(nameof(IChannel.QueueBindAsync)).Select(c => c.Arg<string>("exchange"));
            Assert.Equal(new[] { TestHost.ServiceName, "other_exchange" }, exchanges);
            Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicConsumeAsync)));
        }

        [Fact]
        public async Task PrefetchCount_AppliesQosToConsumerChannel()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake, o => o.PrefetchCount = 16);

            await provider.GetRequiredService<IEventBroker>().SubscribeAsync<OrderPlaced, OrderPlacedHandler>();

            Assert.Equal((ushort)16, Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicQosAsync))).Arg<ushort>("prefetchCount"));
        }

        [Fact]
        public async Task ResolvingBroker_DoesNotOpenConsumerChannelUntilSubscribe()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake);

            provider.GetRequiredService<IEventBroker>();

            Assert.Empty(fake.ConnectionRecorder.CallsTo(nameof(IConnection.CreateChannelAsync)));
        }

        [Fact]
        public async Task AutoSubscribe_SubscribesHandlersOnStartAndCancelsConsumersOnStop()
        {
            var fake = new FakeBroker();
            fake.ChannelRecorder.Responder = (method, _) =>
                method.Name == nameof(IChannel.BasicConsumeAsync) ? Task.FromResult("consumer-1") : null;
            await using var provider = TestHost.Build(fake, o => o.AutoSubscribe = true);

            var hostedService = Assert.Single(provider.GetServices<IHostedService>());
            await hostedService.StartAsync(CancellationToken.None);

            Assert.True(provider.GetRequiredService<IEventBusSubscriptionsManager>().HasSubscriptionsForEvent<OrderPlaced>());
            Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicConsumeAsync)));

            await hostedService.StopAsync(CancellationToken.None);

            Assert.Equal("consumer-1", Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicCancelAsync))).Arg<string>("consumerTag"));
        }

        [Fact]
        public async Task UseEventFluxRabbitFlow_SubscribesEveryHandlerInAssembly()
        {
            var fake = new FakeBroker();
            await using var provider = TestHost.Build(fake);

            provider.UseEventFluxRabbitFlow(typeof(OrderPlaced).Assembly);

            Assert.True(provider.GetRequiredService<IEventBusSubscriptionsManager>().HasSubscriptionsForEvent<OrderPlaced>());
            Assert.Single(fake.ChannelRecorder.CallsTo(nameof(IChannel.BasicConsumeAsync)));
        }
    }
}
