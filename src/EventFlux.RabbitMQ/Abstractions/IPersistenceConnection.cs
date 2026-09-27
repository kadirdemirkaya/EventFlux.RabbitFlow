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

        /// <summary>Creates a channel with the given options, for example with publisher confirmations enabled.</summary>
        /// <remarks>The default implementation ignores <paramref name="options"/> and calls <see cref="CreateChannelAsync()"/>.</remarks>
        Task<IChannel> CreateChannelAsync(CreateChannelOptions? options, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return CreateChannelAsync();
        }
    }
}
