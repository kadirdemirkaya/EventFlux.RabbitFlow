using EventFlux.Abstractions;

namespace EventFlux.Consumer.Events
{
    public class ExampleEventRequest : IEventRequest
    {
        public string Str { get; set; }

        public ExampleEventRequest(string str)
        {
            Str = str;
        }
    }
}
