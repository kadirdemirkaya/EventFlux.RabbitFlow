using EventFlux.Abstractions;

namespace EventFlux.RabbitMQ.Abstractions
{
    public interface IEventBusSubscriptionsManager
    {
        bool IsEmpty { get; }
        event EventHandler<string> OnEventRemoved;

        void AddSubscription<T, TH>()
           where T : IEventRequest
           where TH : IEventHandler<T>;

        void RemoveSubscription<T, TH>()
           where T : IEventRequest
           where TH : IEventHandler<T>;

        bool HasSubscriptionsForEvent<T>() where T : IEventRequest;
        bool HasSubscriptionsForEvent(string eventName);
        Type GetEventTypeByName(string eventName);
        void Clear();
        IEnumerable<SubscriptionInfo> GetHandlersForEvent<T>() where T : IEventRequest;
        IEnumerable<SubscriptionInfo> GetHandlersForEvent(string eventName);
        string GetEventKey<T>();
    }
}
