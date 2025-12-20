using EventFlux.Publisher.Events;
using EventFlux.RabbitMQ.Abstractions;
using EventFlux.RabbitMQ.Context;
using Microsoft.AspNetCore.Mvc;

namespace EventFlux.Publisher.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PublishController(IEventBroker _eventBus, IEventFluxContextAccessor _contextAccessor) : ControllerBase
    {
        [HttpPost("eventbus_flow")]
        public async Task<IActionResult> eventbus_flow()
        {
            // Set context
            _contextAccessor.Context = new EventFluxContext();
            _contextAccessor.Context.Items["UserId"] = "12345";
            _contextAccessor.Context.Items["CorrelationId"] = Guid.NewGuid().ToString();

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
