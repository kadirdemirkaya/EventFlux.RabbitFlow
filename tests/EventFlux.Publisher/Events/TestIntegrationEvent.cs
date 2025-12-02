using EventFlux.Abstractions;

namespace EventFlux.Publisher.Events
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
