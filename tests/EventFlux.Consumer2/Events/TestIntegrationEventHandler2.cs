using EventFlux.Abstractions;

namespace EventFlux.Consumer2.Events
{
    public class TestIntegrationEventHandler2 : IEventHandler<TestIntegrationEvent>
    {
        public async Task Handle(TestIntegrationEvent @event, CancellationToken cancellationToken)
        {
            Console.WriteLine("Consume data: " + @event.TestName);
        }
    }
}
