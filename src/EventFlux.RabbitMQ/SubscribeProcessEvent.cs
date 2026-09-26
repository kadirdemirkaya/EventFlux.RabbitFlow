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
        protected readonly IEventFluxContextAccessor _contextAccessor;

        protected SubscribeProcessEvent(ILogger<IEventBroker> logger,
            IEventBusSubscriptionsManager subsManager,
            IServiceScopeFactory serviceScopeFactory,
            string appName,
            IEventBus eventBus,
            IEventFluxContextAccessor contextAccessor)
            : this(logger, subsManager, serviceScopeFactory, appName, contextAccessor)
        {
        }

        protected SubscribeProcessEvent(ILogger<IEventBroker> logger,
            IEventBusSubscriptionsManager subsManager,
            IServiceScopeFactory serviceScopeFactory,
            string appName,
            IEventFluxContextAccessor contextAccessor)
        {
            _logger = logger;
            _subsManager = subsManager;
            _serviceScopeFactory = serviceScopeFactory;
            _appName = appName;
            _contextAccessor = contextAccessor;
        }

        protected virtual Task ProcessEvent(string eventName, string message, IDictionary<string, object?>? headers = null)
            => ProcessEvent(eventName, message, headers, CancellationToken.None);

        protected virtual async Task ProcessEvent(string eventName, string message, IDictionary<string, object?>? headers, CancellationToken cancellationToken)
        {
            if (!_subsManager.HasSubscriptionsForEvent(eventName))
            {
                _logger.LogWarning("No subscription for {AppName} event: {EventName}", _appName, eventName);
                return;
            }

            var eventType = _subsManager.GetEventTypeByName(eventName)
                ?? throw new InvalidOperationException($"No event type is registered for '{eventName}'.");

            var integrationEvent = JsonConvert.DeserializeObject(message, eventType) as IEventRequest
                ?? throw new InvalidOperationException($"Message could not be deserialized to '{eventType.FullName}'.");

            var scope = _serviceScopeFactory.CreateAsyncScope();
            await using var scopeDisposal = scope.ConfigureAwait(false);

            if (headers != null && headers.Count > 0)
            {
                var context = new EventFluxContext();
                foreach (var header in headers)
                {
                    context.Items[header.Key] = header.Value is byte[] bytes
                        ? System.Text.Encoding.UTF8.GetString(bytes)
                        : header.Value!;
                }
                _contextAccessor.Context = context;
            }

            var eventBus = scope.ServiceProvider.GetRequiredService<IEventBus>();
            await eventBus.PublishAsync(integrationEvent, cancellationToken).ConfigureAwait(false);

            _logger.LogInformation("Processed {EventName} with EventFlux", eventName);
        }
    }
}
