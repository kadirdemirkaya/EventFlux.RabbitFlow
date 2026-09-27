using EventFlux.Abstractions;
using EventFlux.RabbitMQ.Abstractions;

namespace EventFlux.RabbitMQ
{
    public class EventBusSubscriptionsManager : IEventBusSubscriptionsManager
    {
        private readonly object _sync = new object();
        private readonly Dictionary<string, List<SubscriptionInfo>> _handlers;
        private readonly Dictionary<string, List<Type>> _eventTypes;

        public event EventHandler<string> OnEventRemoved;

        public EventBusSubscriptionsManager()
        {
            _handlers = new Dictionary<string, List<SubscriptionInfo>>();
            _eventTypes = new Dictionary<string, List<Type>>();
        }

        public bool IsEmpty
        {
            get
            {
                lock (_sync)
                {
                    return _handlers.Count == 0;
                }
            }
        }

        public void Clear()
        {
            lock (_sync)
            {
                _handlers.Clear();
                _eventTypes.Clear();
            }
        }

        public void AddSubscription<T, TH>()
            where T : IEventRequest
            where TH : IEventHandler<T>
        {
            var eventName = GetEventKey<T>();
            var handlerType = typeof(TH);

            lock (_sync)
            {
                if (_handlers.TryGetValue(eventName, out var handlers) && handlers.Any(s => s.HandlerType == handlerType))
                {
                    throw new ArgumentException($"Handler Type {handlerType.Name} already registered for '{eventName}'", nameof(TH));
                }

                if (handlers == null)
                {
                    handlers = new List<SubscriptionInfo>();
                    _handlers[eventName] = handlers;
                }

                handlers.Add(SubscriptionInfo.Typed(handlerType));

                if (!_eventTypes.TryGetValue(eventName, out var types))
                {
                    types = new List<Type>();
                    _eventTypes[eventName] = types;
                }

                if (!types.Contains(typeof(T)))
                {
                    types.Add(typeof(T));
                }
            }
        }

        public void RemoveSubscription<T, TH>()
            where T : IEventRequest
            where TH : IEventHandler<T>
        {
            var eventName = GetEventKey<T>();
            var handlerType = typeof(TH);
            var eventRemoved = false;

            lock (_sync)
            {
                if (!_handlers.TryGetValue(eventName, out var handlers)) return;

                var subscription = handlers.SingleOrDefault(s => s.HandlerType == handlerType);
                if (subscription == null) return;

                handlers.Remove(subscription);

                if (handlers.Count == 0)
                {
                    _handlers.Remove(eventName);
                    _eventTypes.Remove(eventName);
                    eventRemoved = true;
                }
            }

            if (eventRemoved)
            {
                OnEventRemoved?.Invoke(this, eventName);
            }
        }

        public IEnumerable<SubscriptionInfo> GetHandlersForEvent<T>() where T : IEventRequest
            => GetHandlersForEvent(GetEventKey<T>());

        public IEnumerable<SubscriptionInfo> GetHandlersForEvent(string eventName)
        {
            lock (_sync)
            {
                return _handlers.TryGetValue(eventName, out var handlers)
                    ? handlers.ToArray()
                    : Array.Empty<SubscriptionInfo>();
            }
        }

        public bool HasSubscriptionsForEvent<T>() where T : IEventRequest
            => HasSubscriptionsForEvent(GetEventKey<T>());

        public bool HasSubscriptionsForEvent(string eventName)
        {
            lock (_sync)
            {
                return _handlers.ContainsKey(eventName);
            }
        }

        public Type GetEventTypeByName(string eventName)
        {
            lock (_sync)
            {
                if (!_eventTypes.TryGetValue(eventName, out var types)) return null;

                if (types.Count > 1)
                {
                    throw new InvalidOperationException($"Event name '{eventName}' is used by more than one event type: {string.Join(", ", types.Select(t => t.FullName))}");
                }

                return types[0];
            }
        }

        public string GetEventKey<T>()
        {
            return typeof(T).Name;
        }
    }
}
