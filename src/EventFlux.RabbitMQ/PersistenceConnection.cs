using EventFlux.RabbitMQ.Abstractions;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System.Net.Sockets;

namespace EventFlux.RabbitMQ
{
    public class PersistenceConnection : IPersistenceConnection
    {
        private readonly IConnectionFactory _connectionFactory;
        private readonly ILogger<PersistenceConnection> _logger;
        private readonly int _retryCount;
        IConnection? _connection;
        bool _disposed;

        object @lock = new object();

        public PersistenceConnection(IConnectionFactory connectionFactory, ILogger<PersistenceConnection> logger, int retryCount = 5)
            : this(connectionFactory, logger, retryCount, connectionFactory is ConnectionFactory { AutomaticRecoveryEnabled: true })
        {
        }

        internal PersistenceConnection(IConnectionFactory connectionFactory, ILogger<PersistenceConnection> logger, int retryCount, bool recoversAutomatically)
        {
            _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _retryCount = retryCount;
            RecoversAutomatically = recoversAutomatically;
        }

        internal bool RecoversAutomatically { get; }

        internal Task Reconnection { get; private set; } = Task.CompletedTask;

        public bool IsConnected
        {
            get
            {
                return _connection != null && _connection.IsOpen && !_disposed;
            }
        }

        public async Task<IChannel> CreateChannelAsync()
        {
            if (!IsConnected)
            {
                throw new InvalidOperationException("No RabbitMQ connections are available to perform this action");
            }

            return await _connection!.CreateChannelAsync().ConfigureAwait(false);
        }

        public void Dispose()
        {
            if (_disposed) return;

            _disposed = true;

            if (_connection == null) return;

            try
            {
                Detach(_connection);
                _connection.Dispose();
            }
            catch (IOException ex)
            {
                _logger.LogCritical(ex.ToString());
            }
        }

        public bool TryConnect()
        {
            lock (@lock)
            {
                if (IsConnected) return true;
                if (_disposed) return false;

                if (_connection != null && RecoversAutomatically)
                {
                    _logger.LogWarning("RabbitMQ connection is down, waiting for automatic recovery");
                    return false;
                }

                _logger.LogInformation("RabbitMQ Client is trying to connect");

                var policy = RetryPolicy.Handle<SocketException>()
                    .Or<BrokerUnreachableException>()
                    .WaitAndRetry(_retryCount, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (ex, time) =>
                    {
                        _logger.LogWarning(ex, "RabbitMQ Client could not connect after {TimeOut}s ({ExceptionMessage})", $"{time.TotalSeconds:n1}", ex.Message);
                    }
                );

                Release(_connection);
                _connection = null;

                policy.Execute(() =>
                {
                    _connection = _connectionFactory.CreateConnectionAsync().GetAwaiter().GetResult();
                });

                if (IsConnected)
                {
                    _connection!.ConnectionShutdownAsync += OnConnectionShutdown;
                    _connection.CallbackExceptionAsync += OnCallbackException;
                    _connection.ConnectionBlockedAsync += OnConnectionBlocked;

                    return true;
                }
                else
                {
                    _logger.LogCritical("RabbitMQ connections could not be created and opened");

                    return false;
                }
            }
        }

        private void Release(IConnection? connection)
        {
            if (connection == null) return;

            Detach(connection);

            try
            {
                connection.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Could not dispose the previous RabbitMQ connection");
            }
        }

        private void Detach(IConnection connection)
        {
            connection.ConnectionShutdownAsync -= OnConnectionShutdown;
            connection.CallbackExceptionAsync -= OnCallbackException;
            connection.ConnectionBlockedAsync -= OnConnectionBlocked;
        }

        private Task OnConnectionBlocked(object sender, ConnectionBlockedEventArgs e)
        {
            if (_disposed) return Task.CompletedTask;

            _logger.LogWarning("RabbitMQ connection is blocked by the broker ({Reason})", e.Reason);

            return Task.CompletedTask;
        }

        private Task OnCallbackException(object sender, CallbackExceptionEventArgs e)
        {
            if (_disposed) return Task.CompletedTask;

            _logger.LogWarning(e.Exception, "RabbitMQ connection callback threw an exception");

            return Task.CompletedTask;
        }

        private Task OnConnectionShutdown(object sender, ShutdownEventArgs reason)
        {
            if (_disposed || reason.Initiator == ShutdownInitiator.Application) return Task.CompletedTask;

            if (RecoversAutomatically)
            {
                _logger.LogWarning("RabbitMQ connection was shut down ({ReplyText}), waiting for automatic recovery", reason.ReplyText);
                return Task.CompletedTask;
            }

            _logger.LogWarning("RabbitMQ connection was shut down ({ReplyText}). Trying to reconnect...", reason.ReplyText);

            Reconnection = Task.Run(TryConnect);

            return Task.CompletedTask;
        }

    }
}
