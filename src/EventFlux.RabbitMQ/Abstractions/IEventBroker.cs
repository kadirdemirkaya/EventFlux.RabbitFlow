using EventFlux.Abstractions;

namespace EventFlux.RabbitMQ.Abstractions
{
    public interface IEventBroker
    {
        Task PublishAsync(IEventRequest @event);
        Task PublishAsync(IEventRequest @event, string exchangeName);

        Task SubscribeAsync<T, TH>(string exchangeName = null)
            where T : IEventRequest
            where TH : IEventHandler<T>;

        void Unsubscribe<T, TH>()
            where TH : IEventHandler<T>
            where T : IEventRequest;
    }
}
