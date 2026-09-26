using EventFlux.Abstractions;
using EventFlux.RabbitMQ.Context;

namespace EventFlux.Consumer.Events
{
    public class TestIntegrationEventHandler : IEventHandler<TestIntegrationEvent>
    {
        private readonly IEventFluxContextAccessor _contextAccessor;

        public TestIntegrationEventHandler(IEventFluxContextAccessor contextAccessor)
        {
            _contextAccessor = contextAccessor;
        }

        public async Task Handle(TestIntegrationEvent @event, CancellationToken cancellationToken)
        {
            Console.WriteLine("Consume data: " + @event.TestName);

            if (_contextAccessor.Context != null)
            {
                foreach (var item in _contextAccessor.Context.Items)
                {
                    Console.WriteLine($"Context Item - Key: {item.Key}, Value: {item.Value}");
                }
            }
            else
            {
                Console.WriteLine("Context is null");
            }
        }
    }
}
