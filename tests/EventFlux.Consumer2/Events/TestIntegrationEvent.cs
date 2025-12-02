using EventFlux.Abstractions;

namespace EventFlux.Consumer2.Events
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
