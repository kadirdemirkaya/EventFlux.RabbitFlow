using EventFlux.Abstractions;
using EventFlux.Extensions;
using EventFlux.RabbitMQ.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using System.Reflection;

namespace EventFlux.RabbitMQ
{
    public static class RabbitFlowExtensions
    {
        public static IServiceCollection AddEventFluxRabbitFlow(this IServiceCollection services, Assembly assembly, IConnectionFactory connectionFactory, string serviceName)
        {
            // 1
            services.AddEventBus(assembly);

            // 2
            services.AddScoped<IEventBusSubscriptionsManager, EventBusSubscriptionsManager>();

            // 3
            services.AddScoped<IPersistenceConnection>(sp =>
            {
                var logger = sp.GetRequiredService<ILogger<PersistenceConnection>>();

                return new PersistenceConnection(connectionFactory, logger, 5);
            });

            // 4
            services.AddScoped<IEventBroker, EventBusRabbitMQ>(sp =>
            {
                var rabbitMQPersistentConnection = sp.GetRequiredService<IPersistenceConnection>();
                var iLifetimeScope = sp.GetRequiredService<IServiceScopeFactory>();
                var logger = sp.GetRequiredService<ILogger<IEventBroker>>();

                var serviceBus = sp.GetRequiredService<IEventBus>(); // !!!!!

                var eventBusSubcriptionsManager = sp.GetRequiredService<IEventBusSubscriptionsManager>();

                return new EventBusRabbitMQ(rabbitMQPersistentConnection, logger, iLifetimeScope, eventBusSubcriptionsManager, serviceBus, serviceName, 5);
            });

            // 5 (handler inject and subscribe)
            var eventHandlerTypes = assembly.GetTypes()
                .Where(t => t.GetInterfaces().Any(i =>
                    i.IsGenericType &&
                    i.GetGenericTypeDefinition() == typeof(IEventHandler<>)))
                .ToList();

            foreach (var handlerType in eventHandlerTypes)
            {
                var eventType = handlerType.GetInterfaces()
                    .First(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEventHandler<>))
                    .GetGenericArguments()[0];

                services.AddTransient(handlerType);
            }

            return services;
        }

        private static void UseEventFluxRabbitFlow(this IServiceProvider serviceProvider, Assembly assembly)
        {
            using (var scope = serviceProvider.CreateScope())
            {
                var eventBus = scope.ServiceProvider.GetRequiredService<IEventBroker>();

                var eventHandlerTypes = assembly.GetTypes()
                    .Where(t => t.GetInterfaces().Any(i =>
                        i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEventHandler<>)))
                    .ToList();

                foreach (var handlerType in eventHandlerTypes)
                {
                    var eventType = handlerType.GetInterfaces()
                        .First(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEventHandler<>))
                        .GetGenericArguments()[0];

                    var subscribeMethod = eventBus.GetType().GetMethod("SubscribeAsync")!
                        .MakeGenericMethod(eventType, handlerType);

                    // invoke async subscribe method (no parameters) and wait for completion
                    var result = subscribeMethod.Invoke(eventBus, new object[] { });
                    if (result is System.Threading.Tasks.Task task)
                    {
                        task.GetAwaiter().GetResult();
                    }
                }
            }
        }
    }

}
