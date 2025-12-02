
using EventFlux.Abstractions;

namespace EventFlux.Consumer2.Events
{
    public class TestIntegrationEventHandler : IEventHandler<TestIntegrationEvent>
    {
        public async Task Handle(TestIntegrationEvent @event)
        {
            Console.WriteLine("Consume data: " + @event.TestName);
        }
    }
}
