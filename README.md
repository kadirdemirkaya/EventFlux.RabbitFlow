# EventFlux.RabbitFlow

EventFlux.RabbitFlow is a lightweight event-driven library that integrates with RabbitMQ, providing an efficient way to handle event-based messaging in .NET applications.

## Features

- Seamless integration with RabbitMQ
- Supports event-driven CQRS patterns
- Easy event publishing and subscription
- Automatic dependency injection support
- Simple conventions for event handlers

## Requirements

- .NET 6.0 or later (library targets modern .NET; check project file for exact target)
- RabbitMQ broker reachable from your application

## Installation

Install the package from NuGet:

```
Install-Package EventFlux.RabbitFlow
```

or with the .NET CLI:

```
dotnet add package EventFlux.RabbitFlow
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

Implement the `IEventHandler<T>` interface to handle the event:

```csharp
public class ExampleEventRequestHandler : IEventHandler<ExampleEventRequest>
{
	public async Task Handle(ExampleEventRequest @event)
	{
		Console.WriteLine("Consume data: " + @event.Str);
		await Task.CompletedTask;
	}
}
```

3. Configure Dependency Injection

Register the library and RabbitMQ connection in `Program.cs`. The library exposes an extension method `AddEventFluxRabbitFlow` that scans assemblies for handlers and configures the required services. Example:

```csharp
builder.Services.AddEventFluxRabbitFlow(
	typeof(Program).Assembly,
	new ConnectionFactory() { Uri = new Uri("amqp://guest:guest@localhost:5672") },
	"publisher_queue"
);
```

4. Subscribe to events

Resolve the `IEventBroker` and subscribe to events (typically done as part of your application startup):

```csharp
var _sp = builder.Services.BuildServiceProvider();
var _eventBus = _sp.GetRequiredService<EventFlux.RabbitFlow.Abstractions.IEventBroker>();
await _eventBus.SubscribeAsync<ExampleEventRequest, ExampleEventRequestHandler>();
```

5. Publish an event

In your API controller or service, publish an event:

```csharp
[HttpPost]
public async Task<IActionResult> Post()
{
	ExampleEventRequest @event = new ExampleEventRequest { Str = "Hello World" };
	await _eventBus.PublishAsync(@event);
	return Ok();
}
```

## Configuration

- Connection settings are provided via `RabbitMQ.Client.ConnectionFactory` when calling `AddEventFluxRabbitFlow`.
- You can customize queue names, routing, and other behavior by extending the provided abstractions such as `IPersistenceConnection` and `IEventBusSubscriptionsManager`.

## Exchange Support

The library uses a default exchange name derived from the configured service name. You can also publish and subscribe to events using a different exchange by passing an exchange name to the provided overloads.

- Default exchange: when you register the library with a `serviceName` (the third parameter in `AddEventFluxRabbitFlow`), that name is used as the default exchange.
- Custom exchange: call the overloads that accept an `exchangeName` to publish or subscribe on a different exchange.

Examples:

Publish to a custom exchange:

```csharp
// publish to the default exchange
await _eventBus.PublishAsync(@event);

// publish to a custom exchange named "eventbus_flow_2"
await _eventBus.PublishAsync(@event, "eventbus_flow_2");
```

Subscribe to events on a custom exchange:

```csharp
// subscribe using default exchange
await _eventBus.SubscribeAsync<ExampleEventRequest, ExampleEventRequestHandler>();

// subscribe to the same event on a specific exchange
await _eventBus.SubscribeAsync<ExampleEventRequest, ExampleEventRequestHandler>("eventbus_flow_2");
```

## Project layout

- `EventBusRabbitMQ.cs` — core RabbitMQ integration and publish/subscribe orchestration
- `EventBusSubscriptionsManager.cs` — manages in-memory mapping between events and handlers
- `PersistenceConnection.cs` — manages RabbitMQ connection lifecycle
- `RabbitFlowExtensions.cs` — DI registration and extension methods


