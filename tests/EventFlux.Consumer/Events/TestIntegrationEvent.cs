using EventFlux.Abstractions;

namespace EventFlux.Consumer.Events
{
    public class TestIntegrationEvent : IEventRequest
    {
        public string TestName { get; set; }
        public TestIntegrationEvent(string testName)
        {
            TestName = testName;
        }
    }
}
