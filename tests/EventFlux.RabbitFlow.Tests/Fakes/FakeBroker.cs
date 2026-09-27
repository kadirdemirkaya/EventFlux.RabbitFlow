using System.Collections.Concurrent;
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

            (Channel, ChannelRecorder) = RecordingProxy.Create<IChannel>((method, _) =>
                method.Name == "get_" + nameof(IChannel.IsOpen) ? ChannelIsOpen : null);

            (ConnectionFactory, _) = RecordingProxy.Create<IConnectionFactory>((method, args) =>
                method.Name == nameof(IConnectionFactory.CreateConnectionAsync) ? ConnectAsync((CancellationToken)args[^1]!) : null);
        }

        public IConnectionFactory ConnectionFactory { get; }

        public TaskCompletionSource? ConnectGate { get; set; }

        public ConcurrentQueue<Exception> ConnectFailures { get; } = new();

        public IReadOnlyList<FakeConnection> Connections => _connections;

        public IConnection Connection => Current.Connection;

        public RecordingProxy ConnectionRecorder => Current.Recorder;

        private FakeConnection Current => _connections.Count > 0 ? _connections[^1] : _nextConnection;

        public IChannel Channel { get; }

        public bool ChannelIsOpen { get; set; } = true;

        public bool DistinctChannels { get; set; }

        public Func<FakeChannel, Task>? OnPublish { get; set; }

        public ConcurrentQueue<FakeChannel> CreatedChannels { get; } = new();

        internal IChannel NextChannel()
        {
            if (!DistinctChannels) return Channel;

            var channel = new FakeChannel(this);
            CreatedChannels.Enqueue(channel);
            return channel.Channel;
        }

        public RecordingProxy ChannelRecorder { get; }

        public IReadOnlyList<Invocation> Published => ChannelRecorder.CallsTo(nameof(IChannel.BasicPublishAsync));

        private async Task<IConnection> ConnectAsync(CancellationToken cancellationToken)
        {
            if (ConnectGate is { } gate)
            {
                await gate.Task.WaitAsync(cancellationToken);
            }

            if (ConnectFailures.TryDequeue(out var failure))
            {
                throw failure;
            }

            return OpenConnection();
        }

        private IConnection OpenConnection()
        {
            var connection = _nextConnection;
            _connections.Add(connection);
            _nextConnection = new FakeConnection(this);
            return connection.Connection;
        }
    }

    public class FakeChannel
    {
        private int _inUse;

        public FakeChannel(FakeBroker broker)
        {
            (Channel, Recorder) = RecordingProxy.Create<IChannel>((method, _) => method.Name switch
            {
                "get_" + nameof(IChannel.IsOpen) => IsOpen,
                nameof(IChannel.BasicPublishAsync) => new ValueTask(PublishAsync(broker)),
                _ => null
            });
        }

        public IChannel Channel { get; }

        public RecordingProxy Recorder { get; }

        public bool IsOpen { get; set; } = true;

        public bool UsedConcurrently { get; private set; }

        public int Disposals => Recorder.CallsTo(nameof(IAsyncDisposable.DisposeAsync)).Count + Recorder.CallsTo(nameof(IDisposable.Dispose)).Count;

        private async Task PublishAsync(FakeBroker broker)
        {
            if (Interlocked.Increment(ref _inUse) > 1) UsedConcurrently = true;
            try
            {
                if (broker.OnPublish is { } onPublish) await onPublish(this);
            }
            finally
            {
                Interlocked.Decrement(ref _inUse);
            }
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
                nameof(IConnection.CreateChannelAsync) => Task.FromResult(broker.NextChannel()),
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
