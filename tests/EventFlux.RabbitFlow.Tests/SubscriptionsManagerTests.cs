using EventFlux.Abstractions;
using EventFlux.RabbitMQ;
using Xunit;

namespace EventFlux.RabbitFlow.Tests
{
    public class SubscriptionsManagerTests
    {
        public class StressEvent : IEventRequest { }

        public abstract class H1 : IEventHandler<StressEvent> { public Task Handle(StressEvent e, CancellationToken ct) => Task.CompletedTask; }
        public abstract class H2 : IEventHandler<StressEvent> { public Task Handle(StressEvent e, CancellationToken ct) => Task.CompletedTask; }
        public abstract class H3 : IEventHandler<StressEvent> { public Task Handle(StressEvent e, CancellationToken ct) => Task.CompletedTask; }
        public abstract class H4 : IEventHandler<StressEvent> { public Task Handle(StressEvent e, CancellationToken ct) => Task.CompletedTask; }
        public abstract class H5 : IEventHandler<StressEvent> { public Task Handle(StressEvent e, CancellationToken ct) => Task.CompletedTask; }
        public abstract class H6 : IEventHandler<StressEvent> { public Task Handle(StressEvent e, CancellationToken ct) => Task.CompletedTask; }
        public abstract class H7 : IEventHandler<StressEvent> { public Task Handle(StressEvent e, CancellationToken ct) => Task.CompletedTask; }
        public abstract class H8 : IEventHandler<StressEvent> { public Task Handle(StressEvent e, CancellationToken ct) => Task.CompletedTask; }

        private static readonly Action<EventBusSubscriptionsManager>[] AddEach =
        {
            m => m.AddSubscription<StressEvent, H1>(), m => m.AddSubscription<StressEvent, H2>(),
            m => m.AddSubscription<StressEvent, H3>(), m => m.AddSubscription<StressEvent, H4>(),
            m => m.AddSubscription<StressEvent, H5>(), m => m.AddSubscription<StressEvent, H6>(),
            m => m.AddSubscription<StressEvent, H7>(), m => m.AddSubscription<StressEvent, H8>()
        };

        private static void RunTogether(int threads, Action<int> body)
        {
            using var barrier = new Barrier(threads);
            var workers = Enumerable.Range(0, threads).Select(i => new Thread(() =>
            {
                barrier.SignalAndWait();
                body(i);
            })).ToList();
            workers.ForEach(t => t.Start());
            workers.ForEach(t => t.Join());
        }

        [Fact]
        public void ConcurrentSubscriptionsToOneEvent_RegisterTheEventTypeOnce()
        {
            for (var round = 0; round < 300; round++)
            {
                var manager = new EventBusSubscriptionsManager();

                RunTogether(AddEach.Length, i => AddEach[i](manager));

                Assert.Equal(typeof(StressEvent), manager.GetEventTypeByName(nameof(StressEvent)));
                Assert.Equal(AddEach.Length, manager.GetHandlersForEvent(nameof(StressEvent)).Count());
            }
        }

        [Fact]
        public void EnumeratingHandlers_WhileSubscriptionsChange_DoesNotThrow()
        {
            var manager = new EventBusSubscriptionsManager();
            manager.AddSubscription<StressEvent, H1>();
            var errors = new List<Exception>();

            RunTogether(2, i =>
            {
                for (var n = 0; n < 20000; n++)
                {
                    try
                    {
                        if (i == 0)
                        {
                            manager.AddSubscription<StressEvent, H2>();
                            manager.RemoveSubscription<StressEvent, H2>();
                        }
                        else
                        {
                            foreach (var _ in manager.GetHandlersForEvent(nameof(StressEvent))) { }
                            manager.GetEventTypeByName(nameof(StressEvent));
                        }
                    }
                    catch (Exception ex)
                    {
                        lock (errors) errors.Add(ex);
                        return;
                    }
                }
            });

            Assert.Empty(errors);
        }

        [Fact]
        public void AddingWhileTheLastHandlerIsRemoved_NeverLosesTheNewSubscription()
        {
            for (var round = 0; round < 2000; round++)
            {
                var manager = new EventBusSubscriptionsManager();
                manager.AddSubscription<StressEvent, H1>();

                RunTogether(2, i =>
                {
                    if (i == 0) manager.RemoveSubscription<StressEvent, H1>();
                    else manager.AddSubscription<StressEvent, H2>();
                });

                Assert.True(manager.HasSubscriptionsForEvent(nameof(StressEvent)), $"round {round}");
                Assert.Equal(typeof(H2), Assert.Single(manager.GetHandlersForEvent(nameof(StressEvent))).HandlerType);
                Assert.Equal(typeof(StressEvent), manager.GetEventTypeByName(nameof(StressEvent)));
            }
        }

        [Fact]
        public void GetHandlersForEvent_ReturnsASnapshot()
        {
            var manager = new EventBusSubscriptionsManager();
            manager.AddSubscription<StressEvent, H1>();

            var handlers = manager.GetHandlersForEvent(nameof(StressEvent));
            manager.AddSubscription<StressEvent, H2>();

            Assert.Single(handlers);
            Assert.Equal(2, manager.GetHandlersForEvent<StressEvent>().Count());
        }

        [Fact]
        public void GetHandlersForEvent_UnknownEvent_ReturnsEmpty()
        {
            var manager = new EventBusSubscriptionsManager();

            Assert.Empty(manager.GetHandlersForEvent("Nope"));
            Assert.Null(manager.GetEventTypeByName("Nope"));
            Assert.False(manager.HasSubscriptionsForEvent("Nope"));
            Assert.True(manager.IsEmpty);
        }

        [Fact]
        public void DuplicateHandler_StillThrows()
        {
            var manager = new EventBusSubscriptionsManager();
            manager.AddSubscription<StressEvent, H1>();

            Assert.Throws<ArgumentException>(() => manager.AddSubscription<StressEvent, H1>());
            Assert.Single(manager.GetHandlersForEvent<StressEvent>());
        }

        [Fact]
        public void SameEventNameFromAnotherNamespace_SubscribesButCannotBeResolved()
        {
            var manager = new EventBusSubscriptionsManager();
            manager.AddSubscription<StressEvent, H1>();

            manager.AddSubscription<Other.StressEvent, Other.OtherHandler>();

            var ex = Assert.Throws<InvalidOperationException>(() => manager.GetEventTypeByName(nameof(StressEvent)));
            Assert.Contains(typeof(StressEvent).FullName!, ex.Message);
            Assert.Contains(typeof(Other.StressEvent).FullName!, ex.Message);
            Assert.Equal(2, manager.GetHandlersForEvent(nameof(StressEvent)).Count());
        }

        [Fact]
        public void RemovingTheLastHandler_RaisesOnEventRemovedOnceAndForgetsTheType()
        {
            var manager = new EventBusSubscriptionsManager();
            manager.AddSubscription<StressEvent, H1>();
            manager.AddSubscription<StressEvent, H2>();
            var removed = new List<string>();
            manager.OnEventRemoved += (_, name) => removed.Add(name);

            manager.RemoveSubscription<StressEvent, H1>();
            Assert.Empty(removed);
            manager.RemoveSubscription<StressEvent, H2>();
            manager.RemoveSubscription<StressEvent, H2>();

            Assert.Equal(new[] { nameof(StressEvent) }, removed);
            Assert.Null(manager.GetEventTypeByName(nameof(StressEvent)));
            Assert.True(manager.IsEmpty);
        }

        [Fact]
        public void OnEventRemoved_HandlerCanUseTheManagerFromAnotherThread()
        {
            var manager = new EventBusSubscriptionsManager();
            manager.AddSubscription<StressEvent, H1>();
            var handled = false;
            manager.OnEventRemoved += (_, _) =>
            {
                handled = Task.Run(() => manager.HasSubscriptionsForEvent(nameof(StressEvent))).Wait(TimeSpan.FromSeconds(5));
            };

            manager.RemoveSubscription<StressEvent, H1>();

            Assert.True(handled);
        }

        [Fact]
        public void Clear_ForgetsHandlersAndEventTypes()
        {
            var manager = new EventBusSubscriptionsManager();
            manager.AddSubscription<StressEvent, H1>();

            manager.Clear();

            Assert.True(manager.IsEmpty);
            Assert.Null(manager.GetEventTypeByName(nameof(StressEvent)));
        }
    }
}

namespace EventFlux.RabbitFlow.Tests.Other
{
    public class StressEvent : IEventRequest { }

    public abstract class OtherHandler : IEventHandler<StressEvent> { public Task Handle(StressEvent e, CancellationToken ct) => Task.CompletedTask; }
}
