using RabbitMQ.Client;

namespace EventFlux.RabbitFlow.Tests.Fakes
{
    public class FakeBroker
    {
        public FakeBroker()
        {
            (Channel, ChannelRecorder) = RecordingProxy.Create<IChannel>();

            (Connection, ConnectionRecorder) = RecordingProxy.Create<IConnection>((method, _) =>
                method.Name == nameof(IConnection.CreateChannelAsync) ? Task.FromResult(Channel) : null);

            (ConnectionFactory, _) = RecordingProxy.Create<IConnectionFactory>((method, _) =>
                method.Name == nameof(IConnectionFactory.CreateConnectionAsync) ? Task.FromResult(Connection) : null);
        }

        public IConnectionFactory ConnectionFactory { get; }

        public IConnection Connection { get; }

        public RecordingProxy ConnectionRecorder { get; }

        public IChannel Channel { get; }

        public RecordingProxy ChannelRecorder { get; }

        public IReadOnlyList<Invocation> Published => ChannelRecorder.CallsTo(nameof(IChannel.BasicPublishAsync));
    }
}
