using EventFlux.Abstractions;

namespace EventFlux.Publisher.Events
{
    public class ExampleEventRequestHandler : IEventHandler<ExampleEventRequest>
    {
        public async Task Handle(ExampleEventRequest @event, CancellationToken cancellationToken)
        {
            Console.WriteLine("Consume data: " + @event.Str);
        }
    }
}
