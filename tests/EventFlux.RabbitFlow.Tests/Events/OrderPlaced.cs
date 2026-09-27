using EventFlux.Abstractions;
using EventFlux.RabbitMQ.Context;
using System.Collections.Concurrent;

namespace EventFlux.RabbitFlow.Tests.Events
{
    public class OrderPlaced : IEventRequest
    {
        public Guid OrderId { get; set; }

        public decimal Amount { get; set; }

        public string Customer { get; set; } = string.Empty;

        public bool ShouldFail { get; set; }
    }

    public class HandledOrders
    {
        public ConcurrentQueue<(OrderPlaced Event, CancellationToken Token, EventFluxContext? Context)> Items { get; } = new();

        public Func<Task>? BeforeHandle { get; set; }
    }

    public class OrderPlacedHandler : IEventHandler<OrderPlaced>
    {
        private readonly HandledOrders _handledOrders;
        private readonly IEventFluxContextAccessor _contextAccessor;

        public OrderPlacedHandler(HandledOrders handledOrders, IEventFluxContextAccessor contextAccessor)
        {
            _handledOrders = handledOrders;
            _contextAccessor = contextAccessor;
        }

        public async Task Handle(OrderPlaced @event, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (_handledOrders.BeforeHandle is { } beforeHandle)
            {
                await beforeHandle();
            }

            if (@event.ShouldFail)
            {
                throw new InvalidOperationException("Order handler failed");
            }

            _handledOrders.Items.Enqueue((@event, cancellationToken, _contextAccessor.Context));
        }
    }
}
