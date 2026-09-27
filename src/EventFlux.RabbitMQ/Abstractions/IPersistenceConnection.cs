using RabbitMQ.Client;

namespace EventFlux.RabbitMQ.Abstractions
{
    public interface IPersistenceConnection : IDisposable
    {
        bool IsConnected { get; }

        bool TryConnect();

        /// <summary>Opens the connection without blocking the calling thread. Returns <see langword="true"/> when a connection is open.</summary>
        /// <remarks>The default implementation calls <see cref="TryConnect"/>.</remarks>
        Task<bool> TryConnectAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(TryConnect());
        }

        Task<IChannel> CreateChannelAsync();
    }
}
