using EventFlux.Publisher.Events;
using EventFlux.RabbitMQ.Abstractions;
using Microsoft.AspNetCore.Mvc;

namespace EventFlux.Publisher.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PublishController(IEventBroker _eventBus) : ControllerBase
    {
        [HttpPost("eventbus_flow")]
        public async Task<IActionResult> eventbus_flow()
        {
            TestIntegrationEvent @event = new TestIntegrationEvent("aasdnasfnfavsfh");
            await _eventBus.PublishAsync(@event);

            return Ok();
        }

        [HttpPost("eventbus_flow_2")]
        public async Task<IActionResult> eventbus_flow_2()
        {
            TestIntegrationEvent @event = new TestIntegrationEvent("aasdnasfnfavsfh");
            await _eventBus.PublishAsync(@event, "eventbus_flow_2");

            return Ok();
        }
    }
}
