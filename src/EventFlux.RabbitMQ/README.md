# EventFlux.RabbitFlow

EventFlux.RabbitFlow carries EventFlux events across services over RabbitMQ: publish an `IEventRequest` in one service and its `IEventHandler<T>` runs in another.

```
dotnet add package EventFlux.RabbitFlow --version 1.1.0
```

```csharp
builder.Services.AddEventFluxRabbitFlow(
    typeof(Program).Assembly,
    new ConnectionFactory { Uri = new Uri("amqp://guest:guest@localhost:5672") },
    "orders_service",
    options => options.AutoSubscribe = true);

await eventBroker.PublishAsync(new OrderPlaced { OrderId = orderId }, cancellationToken);
```

## Features

- Publish and subscribe to EventFlux events through RabbitMQ
- Handlers are plain EventFlux `IEventHandler<T>` classes, discovered by assembly scanning
- Subscriptions live for the whole lifetime of the host
- A failing handler never loses its message: it is requeued or moved to a dead-letter queue
- Context propagation through message headers
- Cancellation support from publish to handler

## Requirements

- .NET 8.0, 9.0 or 10.0
- EventFlux 2.1.0 or later
- A RabbitMQ broker reachable from your application

## Installation

```
dotnet add package EventFlux.RabbitFlow --version 1.1.0
```

or in your `.csproj`:

```xml
<PackageReference Include="EventFlux.RabbitFlow" Version="1.1.0" />
```

## Quick Start

1. Define an event

Create an event that implements `IEventRequest`:

```csharp
public class ExampleEventRequest : IEventRequest
{
    public string Str { get; set; }
}
```

2. Create an event handler

Implement `IEventHandler<T>`. The cancellation token is the delivery's token, so it is cancelled when the consumer shuts down:

```csharp
public class ExampleEventRequestHandler : IEventHandler<ExampleEventRequest>
{
    public Task Handle(ExampleEventRequest @event, CancellationToken cancellationToken)
    {
        Console.WriteLine("Consume data: " + @event.Str);
        return Task.CompletedTask;
    }
}
```

3. Configure Dependency Injection

`AddEventFluxRabbitFlow` registers EventFlux, scans the assembly for handlers and configures the broker. The third argument is the service name, which is also the default exchange:

```csharp
builder.Services.AddEventFluxRabbitFlow(
    typeof(Program).Assembly,
    new ConnectionFactory { Uri = new Uri("amqp://guest:guest@localhost:5672") },
    "publisher_queue");
```

4. Subscribe to events

Either let the host subscribe every handler in the scanned assembly when it starts:

```csharp
builder.Services.AddEventFluxRabbitFlow(assembly, connectionFactory, "publisher_queue",
    options => options.AutoSubscribe = true);
```

or subscribe explicitly after the app is built:

```csharp
var app = builder.Build();

var eventBroker = app.Services.GetRequiredService<IEventBroker>();
await eventBroker.SubscribeAsync<ExampleEventRequest, ExampleEventRequestHandler>();
```

`app.Services.UseEventFluxRabbitFlow(typeof(Program).Assembly)` subscribes every handler of an assembly in one call.

`IEventBroker` is a singleton, so a subscription stays active until the host stops, no matter which scope created it. Subscribing the same handler twice is a no-op.

5. Publish an event

```csharp
[HttpPost]
public async Task<IActionResult> Post(CancellationToken cancellationToken)
{
    var @event = new ExampleEventRequest { Str = "Hello World" };
    await _eventBroker.PublishAsync(@event, cancellationToken);
    return Ok();
}
```

6. Publish an event with context

```csharp
[HttpPost]
public async Task<IActionResult> Post()
{
    _contextAccessor.Context = new EventFluxContext();
    _contextAccessor.Context.Items["UserId"] = "12345";
    _contextAccessor.Context.Items["CorrelationId"] = Guid.NewGuid().ToString();

    await _eventBroker.PublishAsync(new TestIntegrationEvent("aasdnasfnfavsfh"));

    return Ok();
}
```

7. Consume event with context

Inject `IEventFluxContextAccessor` into your event handler to read the context sent with the message:

```csharp
public class ExampleEventRequestHandler : IEventHandler<ExampleEventRequest>
{
    private readonly IEventFluxContextAccessor _contextAccessor;

    public ExampleEventRequestHandler(IEventFluxContextAccessor contextAccessor)
    {
        _contextAccessor = contextAccessor;
    }

    public Task Handle(ExampleEventRequest @event, CancellationToken cancellationToken)
    {
        if (_contextAccessor.Context?.Items.TryGetValue("UserId", out var userId) == true)
        {
            Console.WriteLine($"UserId: {userId}");
        }

        return Task.CompletedTask;
    }
}
```

## Failure handling

A message is acknowledged only after its handlers complete. When a handler throws, `RabbitFlowOptions.OnFailure` decides what happens:

| `OnFailure` | Behaviour |
|---|---|
| `Requeue` (default) | The message is rejected and put back on its queue. |
| `DeadLetter` | The message is published to the dead-letter exchange and then acknowledged. If that publish fails, the message is requeued. |

```csharp
builder.Services.AddEventFluxRabbitFlow(assembly, connectionFactory, "orders_service", options =>
{
    options.OnFailure = RabbitFlowFailureMode.DeadLetter;
    options.DeadLetterExchange = "orders_failed";
});
```

In dead-letter mode every event queue `{serviceName}_{EventName}` gets a durable `{serviceName}_{EventName}_dead_letter` queue bound to the dead-letter exchange (default `{serviceName}_dead_letter`) with the event name as routing key. Dead-lettered messages keep their body and headers and add `x-exception-type`, `x-exception-message`, `x-original-exchange` and `x-original-routing-key`.

A delivery cancelled by consumer shutdown is always requeued. A message for an event with no subscription is acknowledged and logged.

## Configuration

| Option | Default | Description |
|---|---|---|
| `RetryCount` | `5` | Connection and publish retry attempts, with exponential back-off. |
| `OnFailure` | `Requeue` | What happens to a message whose handler throws. |
| `DeadLetterExchange` | `{serviceName}_dead_letter` | Exchange used when `OnFailure` is `DeadLetter`. |
| `PrefetchCount` | `0` (no limit) | Maximum unacknowledged messages per consumer channel. |
| `AutoSubscribe` | `false` | Subscribes every handler in the scanned assembly when the host starts, and stops consuming when it stops. |

Connection settings come from the `IConnectionFactory` passed to `AddEventFluxRabbitFlow`.

## Exchange Support

The service name passed to `AddEventFluxRabbitFlow` is the default exchange. Pass an exchange name to publish or subscribe on a different one.

```csharp
await _eventBroker.PublishAsync(@event);
await _eventBroker.PublishAsync(@event, "eventbus_flow_2");

await _eventBroker.SubscribeAsync<ExampleEventRequest, ExampleEventRequestHandler>();
await _eventBroker.SubscribeAsync<ExampleEventRequest, ExampleEventRequestHandler>("eventbus_flow_2");
```

## Message format

Messages are JSON bodies with the event type name as routing key, persisted on a direct exchange named after the service. Each subscribing service consumes from a durable queue named `{serviceName}_{EventName}`. The format is the same as in 1.0.x, so services on 1.0.x and 1.1.0 can exchange messages.

## Upgrading from 1.0.x

- Reference EventFlux 2.1.0 or later and target .NET 8.0, 9.0 or 10.0.
- Change handlers to `Handle(TEvent @event, CancellationToken cancellationToken)`.
- Resolve `IEventBroker` from `app.Services` (or use `AutoSubscribe`) instead of building a second service provider with `builder.Services.BuildServiceProvider()`.
- A failing handler no longer acknowledges its message. Make handlers idempotent, or use `OnFailure = DeadLetter` to stop a failing message from being retried.

## Project layout

- `EventBusRabbitMQ.cs` — publish/subscribe orchestration and delivery handling
- `EventBusSubscriptionsManager.cs` — in-memory mapping between events and handlers
- `PersistenceConnection.cs` — RabbitMQ connection lifecycle
- `RabbitFlowExtensions.cs` — DI registration and subscription helpers
- `RabbitFlowOptions.cs` — configuration options
