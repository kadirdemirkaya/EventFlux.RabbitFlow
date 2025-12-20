using EventFlux.Abstractions;
using EventFlux.RabbitMQ.Abstractions;
using EventFlux.RabbitMQ.Context;
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
        protected readonly IEventFluxContextAccessor _contextAccessor;

        protected SubscribeProcessEvent(ILogger<IEventBroker> logger,
            IEventBusSubscriptionsManager subsManager,
            IServiceScopeFactory serviceScopeFactory,
            string appName,
            IEventBus eventBus,
            IEventFluxContextAccessor contextAccessor)
        {
            _logger = logger;
            _subsManager = subsManager;
            _serviceScopeFactory = serviceScopeFactory;
            _appName = appName;
            _eventBus = eventBus;
            _contextAccessor = contextAccessor;
        }

        protected virtual async Task ProcessEvent(string eventName, string message, IDictionary<string, object?> headers = null)
        {
            if (_subsManager.HasSubscriptionsForEvent(eventName))
            {
                using var scope = _serviceScopeFactory.CreateScope();

                if (headers != null && headers.Count > 0)
                {
                    var context = new EventFluxContext();
                    foreach (var header in headers)
                    {
                        if (header.Value is byte[] bytes)
                        {
                            context.Items[header.Key] = System.Text.Encoding.UTF8.GetString(bytes);
                        }
                        else
                        {
                            context.Items[header.Key] = header.Value;
                        }
                    }
                    _contextAccessor.Context = context;
                }

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
