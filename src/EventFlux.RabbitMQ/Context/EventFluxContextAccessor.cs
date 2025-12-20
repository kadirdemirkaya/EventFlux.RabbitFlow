
using System.Threading;

namespace EventFlux.RabbitMQ.Context
{
    public class EventFluxContextAccessor : IEventFluxContextAccessor
    {
        private static AsyncLocal<EventFluxContextHolder> _currentContext = new AsyncLocal<EventFluxContextHolder>();

        public EventFluxContext Context
        {
            get
            {
                return _currentContext.Value?.Context;
            }
            set
            {
                var holder = _currentContext.Value;
                if (holder != null)
                {
                    holder.Context = null;
                }

                if (value != null)
                {
                    _currentContext.Value = new EventFluxContextHolder { Context = value };
                }
            }
        }

        private class EventFluxContextHolder
        {
            public EventFluxContext Context;
        }
    }
}
