using EventFlux.Abstractions;

namespace EventFlux.Publisher.Events
{
    public class ExampleEventRequest : IEventRequest
    {
        public string Str { get; set; }
    }
}
