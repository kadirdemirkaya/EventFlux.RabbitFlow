using EventFlux.Abstractions;

namespace EventFlux.Consumer.Events
{
    public class TestIntegrationEventHandler2 : IEventHandler<TestIntegrationEvent>
    {
        public async Task Handle(TestIntegrationEvent @event)
        {
            Console.WriteLine("Consume data: " + @event.TestName);
        }
    }
}
