
namespace EventFlux.RabbitMQ.Context
{
    public interface IEventFluxContextAccessor
    {
        EventFluxContext Context { get; set; }
    }
}
