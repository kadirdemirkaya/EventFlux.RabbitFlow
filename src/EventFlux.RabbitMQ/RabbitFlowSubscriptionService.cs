using EventFlux.RabbitMQ.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Reflection;

namespace EventFlux.RabbitMQ
{
    internal sealed class RabbitFlowSubscriptionService : IHostedService
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly Assembly _assembly;

        public RabbitFlowSubscriptionService(IServiceProvider serviceProvider, Assembly assembly)
        {
            _serviceProvider = serviceProvider;
            _assembly = assembly;
        }

        public Task StartAsync(CancellationToken cancellationToken)
            => _serviceProvider.UseEventFluxRabbitFlowAsync(_assembly, cancellationToken);

        public Task StopAsync(CancellationToken cancellationToken)
            => _serviceProvider.GetRequiredService<IEventBroker>() is EventBusRabbitMQ eventBroker
                ? eventBroker.StopConsumingAsync(cancellationToken)
                : Task.CompletedTask;
    }
}
