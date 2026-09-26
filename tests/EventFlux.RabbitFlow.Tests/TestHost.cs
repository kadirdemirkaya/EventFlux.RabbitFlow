using EventFlux.RabbitFlow.Tests.Events;
using EventFlux.RabbitFlow.Tests.Fakes;
using EventFlux.RabbitMQ;
using Microsoft.Extensions.DependencyInjection;

namespace EventFlux.RabbitFlow.Tests
{
    internal static class TestHost
    {
        public const string ServiceName = "orders_service";

        public static ServiceProvider Build(FakeBroker broker, Action<RabbitFlowOptions>? configure = null)
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton<HandledOrders>();
            services.AddEventFluxRabbitFlow(typeof(OrderPlaced).Assembly, broker.ConnectionFactory, ServiceName, configure ?? (_ => { }));

            return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        }
    }
}
