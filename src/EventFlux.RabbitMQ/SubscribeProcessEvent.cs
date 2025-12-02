using EventFlux.Abstractions;
using EventFlux.RabbitMQ.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace EventFlux.RabbitMQ
{
    public abstract class SubscribeProcessEvent
    {
        protected readonly ILogger<IEventBroker> _logger;
        protected readonly IEventBusSubscriptionsManager _subsManager;
        private readonly string _appName;
        private readonly IServiceScopeFactory _serviceScopeFactory;
        private readonly IEventBus _eventBus;

        protected SubscribeProcessEvent(ILogger<IEventBroker> logger,
            IEventBusSubscriptionsManager subsManager,
            IServiceScopeFactory serviceScopeFactory,
            string appName,
            IEventBus eventBus)
        {
            _logger = logger;
            _subsManager = subsManager;
            _serviceScopeFactory = serviceScopeFactory;
            _appName = appName;
            _eventBus = eventBus;
        }

        protected virtual async Task ProcessEvent(string eventName, string message)
        {
            if (_subsManager.HasSubscriptionsForEvent(eventName))
            {
                using var scope = _serviceScopeFactory.CreateScope();

                var eventType = _subsManager.GetEventTypeByName(eventName);
                var integrationEvent = (IEventRequest)JsonConvert.DeserializeObject(message, eventType);

                var eventPublisher = scope.ServiceProvider.GetRequiredService<IEventBus>();
                await eventPublisher.PublishAsync(integrationEvent);

                _logger.LogInformation("EventFlux ile {EventName} işlendi", eventName);
            }
            else
            {
                _logger.LogWarning($"No subscription for {_appName} event: {eventName}");
            }
        }
    }
}
