using RabbitMQ.Client;

namespace EventFlux.RabbitMQ.Abstractions
{
    public interface IPersistenceConnection : IDisposable
    {
        bool IsConnected { get; }

        bool TryConnect();

        Task<IChannel> CreateChannelAsync();
    }
}
