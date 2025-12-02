using EventFlux.Abstractions;
using EventFlux.RabbitMQ.Abstractions;
using System.Collections.Concurrent;

namespace EventFlux.RabbitMQ
{
    public partial class EventBusSubscriptionsManager : IEventBusSubscriptionsManager
    {
        private readonly ConcurrentDictionary<string, List<SubscriptionInfo>> _handlers;
        private readonly List<Type> _eventTypes;

        public event EventHandler<string> OnEventRemoved;

        public EventBusSubscriptionsManager()
        {
            _handlers = new ConcurrentDictionary<string, List<SubscriptionInfo>>();
            _eventTypes = new List<Type>();
        }

        public bool IsEmpty => !_handlers.Keys.Any();
        public void Clear() => _handlers.Clear();


        public void AddSubscription<T, TH>()
            where T : IEventRequest
            where TH : IEventHandler<T>
        {
            var eventName = GetEventKey<T>();

            AddSubscription(typeof(TH), eventName);

            if (!_eventTypes.Contains(typeof(T)))
            {
                _eventTypes.Add(typeof(T));
            }
        }

        private void AddSubscription(Type handlerType, string eventName)
        {
            var handlersList = _handlers.GetOrAdd(eventName, _ => new List<SubscriptionInfo>());

            lock (handlersList)
            {
                if (handlersList.Any(s => s.HandlerType == handlerType))
                    throw new ArgumentException($"Handler Type {handlerType.Name} already registered for '{eventName}'", nameof(handlerType));

                handlersList.Add(SubscriptionInfo.Typed(handlerType));
            }
        }

        public void RemoveSubscription<T, TH>()
            where T : IEventRequest
            where TH : IEventHandler<T>
        {
            var handlerToRemove = FindSubscriptionToRemove<T, TH>();
            var eventName = GetEventKey<T>();
            RemoveHandler(eventName, handlerToRemove);
        }

        private void RemoveHandler(string eventName, SubscriptionInfo subsToRemove)
        {
            if (subsToRemove != null)
            {
                if (_handlers.TryGetValue(eventName, out var handlersList))
                {
                    lock (handlersList)
                    {
                        handlersList.Remove(subsToRemove);

                        if (!handlersList.Any())
                        {
                            _handlers.TryRemove(eventName, out _);

                            var eventType = _eventTypes.SingleOrDefault(e => e.Name == eventName);
                            if (eventType != null)
                            {
                                lock (_eventTypes)
                                {
                                    _eventTypes.Remove(eventType);
                                }
                            }

                            RaiseOnEventRemoved(eventName);
                        }
                    }
                }
            }
        }

        public IEnumerable<SubscriptionInfo> GetHandlersForEvent<T>() where T : IEventRequest
        {
            var key = GetEventKey<T>();
            return GetHandlersForEvent(key);
        }
        public IEnumerable<SubscriptionInfo> GetHandlersForEvent(string eventName) => _handlers[eventName];

        private void RaiseOnEventRemoved(string eventName)
        {
            var handler = OnEventRemoved;
            handler?.Invoke(this, eventName);
        }
        private SubscriptionInfo FindSubscriptionToRemove<T, TH>()
            where T : IEventRequest
            where TH : IEventHandler<T>
        {
            var eventName = GetEventKey<T>();
            return FindSubscriptionToRemove(eventName, typeof(TH));
        }

        private SubscriptionInfo FindSubscriptionToRemove(string eventName, Type handlerType)
        {
            if (!HasSubscriptionsForEvent(eventName))
            {
                return null;
            }

            return _handlers[eventName].SingleOrDefault(s => s.HandlerType == handlerType);

        }

        public bool HasSubscriptionsForEvent<T>() where T : IEventRequest
        {
            var key = GetEventKey<T>();
            return HasSubscriptionsForEvent(key);
        }
        public bool HasSubscriptionsForEvent(string eventName) => _handlers.ContainsKey(eventName);

        public Type GetEventTypeByName(string eventName) => _eventTypes.SingleOrDefault(t => t.Name == eventName);

        public string GetEventKey<T>()
        {
            return typeof(T).Name;
        }
    }
}
