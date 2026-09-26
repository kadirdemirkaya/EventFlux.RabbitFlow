using EventFlux.Abstractions;
using EventFlux.RabbitMQ.Abstractions;
using EventFlux.RabbitMQ.Context;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using System.Reflection;

namespace EventFlux.RabbitMQ
{
    public static class RabbitFlowExtensions
    {
        public static IServiceCollection AddEventFluxRabbitFlow(this IServiceCollection services, Assembly assembly, IConnectionFactory connectionFactory, string serviceName)
            => services.AddEventFluxRabbitFlow(assembly, connectionFactory, serviceName, _ => { });

        public static IServiceCollection AddEventFluxRabbitFlow(this IServiceCollection services, Assembly assembly, IConnectionFactory connectionFactory, string serviceName, Action<RabbitFlowOptions> configure)
        {
            ArgumentNullException.ThrowIfNull(services);
            ArgumentNullException.ThrowIfNull(assembly);
            ArgumentNullException.ThrowIfNull(connectionFactory);
            ArgumentNullException.ThrowIfNull(serviceName);
            ArgumentNullException.ThrowIfNull(configure);

            var options = new RabbitFlowOptions();
            configure(options);

            services.TryAddSingleton<IEventFluxContextAccessor, EventFluxContextAccessor>();
            services.AddEventBus(assembly);

            services.TryAddSingleton<IEventBusSubscriptionsManager, EventBusSubscriptionsManager>();

            services.TryAddSingleton<IPersistenceConnection>(sp =>
            {
                var logger = sp.GetRequiredService<ILogger<PersistenceConnection>>();

                return new PersistenceConnection(connectionFactory, logger, options.RetryCount);
            });

            services.TryAddSingleton<IEventBroker>(sp => new EventBusRabbitMQ(
                sp.GetRequiredService<IPersistenceConnection>(),
                sp.GetRequiredService<ILogger<IEventBroker>>(),
                sp.GetRequiredService<IServiceScopeFactory>(),
                sp.GetRequiredService<IEventBusSubscriptionsManager>(),
                sp.GetRequiredService<IEventFluxContextAccessor>(),
                serviceName,
                options));

            foreach (var (_, handlerType) in FindEventHandlers(assembly))
            {
                services.TryAddTransient(handlerType);
            }

            if (options.AutoSubscribe)
            {
                services.AddSingleton<IHostedService>(sp => new RabbitFlowSubscriptionService(sp, assembly));
            }

            return services;
        }

        public static IServiceProvider UseEventFluxRabbitFlow(this IServiceProvider serviceProvider, Assembly assembly)
        {
            serviceProvider.UseEventFluxRabbitFlowAsync(assembly).GetAwaiter().GetResult();
            return serviceProvider;
        }

        public static async Task UseEventFluxRabbitFlowAsync(this IServiceProvider serviceProvider, Assembly assembly, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(serviceProvider);
            ArgumentNullException.ThrowIfNull(assembly);

            var eventBroker = serviceProvider.GetRequiredService<IEventBroker>();
            var subscribeMethod = typeof(IEventBroker).GetMethod(nameof(IEventBroker.SubscribeAsync))!;

            foreach (var (eventType, handlerType) in FindEventHandlers(assembly))
            {
                cancellationToken.ThrowIfCancellationRequested();

                var task = (Task)subscribeMethod.MakeGenericMethod(eventType, handlerType).Invoke(eventBroker, new object?[] { null })!;
                await task.ConfigureAwait(false);
            }
        }

        internal static IEnumerable<(Type EventType, Type HandlerType)> FindEventHandlers(Assembly assembly)
            => assembly.GetTypes()
                .Where(t => t.IsClass && !t.IsAbstract && !t.ContainsGenericParameters)
                .SelectMany(t => t.GetInterfaces()
                    .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEventHandler<>))
                    .Select(i => (i.GetGenericArguments()[0], t)));
    }
}
