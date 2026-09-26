using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace EventFlux.RabbitFlow.Tests.Fakes
{
    public class FakeBroker
    {
        private readonly List<FakeConnection> _connections = new();
        private FakeConnection _nextConnection;

        public FakeBroker()
        {
            _nextConnection = new FakeConnection(this);

            (Channel, ChannelRecorder) = RecordingProxy.Create<IChannel>();

            (ConnectionFactory, _) = RecordingProxy.Create<IConnectionFactory>((method, _) =>
                method.Name == nameof(IConnectionFactory.CreateConnectionAsync) ? Task.FromResult(OpenConnection()) : null);
        }

        public IConnectionFactory ConnectionFactory { get; }

        public IReadOnlyList<FakeConnection> Connections => _connections;

        public IConnection Connection => Current.Connection;

        public RecordingProxy ConnectionRecorder => Current.Recorder;

        private FakeConnection Current => _connections.Count > 0 ? _connections[^1] : _nextConnection;

        public IChannel Channel { get; }

        public RecordingProxy ChannelRecorder { get; }

        public IReadOnlyList<Invocation> Published => ChannelRecorder.CallsTo(nameof(IChannel.BasicPublishAsync));

        private IConnection OpenConnection()
        {
            var connection = _nextConnection;
            _connections.Add(connection);
            _nextConnection = new FakeConnection(this);
            return connection.Connection;
        }
    }

    public class FakeConnection
    {
        [ThreadStatic]
        private static FakeConnection? _raising;

        public FakeConnection(FakeBroker broker)
        {
            (Connection, Recorder) = RecordingProxy.Create<IConnection>((method, _) => method.Name switch
            {
                nameof(IConnection.CreateChannelAsync) => Task.FromResult(broker.Channel),
                "get_" + nameof(IConnection.IsOpen) => IsOpen,
                nameof(IDisposable.Dispose) => MarkDisposed(),
                _ => null
            });
        }

        public IConnection Connection { get; }

        public RecordingProxy Recorder { get; }

        public bool IsOpen { get; set; } = true;

        public bool DisposedInsideOwnCallback { get; private set; }

        public int HandlerCount(string eventName)
            => Recorder.CallsTo("add_" + eventName).Count - Recorder.CallsTo("remove_" + eventName).Count;

        public Task RaiseAsync<TArgs>(string eventName, TArgs args) where TArgs : AsyncEventArgs
        {
            var handlers = Recorder.CallsTo("add_" + eventName)
                .Select(c => (AsyncEventHandler<TArgs>)c.Args[0]!)
                .Except(Recorder.CallsTo("remove_" + eventName).Select(c => (AsyncEventHandler<TArgs>)c.Args[0]!))
                .ToList();

            _raising = this;
            try
            {
                return Task.WhenAll(handlers.Select(handler => handler(Connection, args)));
            }
            finally
            {
                _raising = null;
            }
        }

        private object? MarkDisposed()
        {
            if (ReferenceEquals(_raising, this)) DisposedInsideOwnCallback = true;
            return null;
        }
    }
}
