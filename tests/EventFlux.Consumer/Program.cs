using EventFlux.Consumer.Events;
using EventFlux.RabbitMQ;
using EventFlux.RabbitMQ.Abstractions;
using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();

builder.Services.AddEventFluxRabbitFlow(typeof(Program).Assembly, new ConnectionFactory()
{
    Uri = new Uri("amqp://guest:guest@localhost:5672")
}, "eventbus_flow");

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

var _sp = builder.Services.BuildServiceProvider();
var _eventBus = _sp.GetRequiredService<IEventBroker>();
await _eventBus.SubscribeAsync<TestIntegrationEvent, TestIntegrationEventHandler>();

app.UseHttpsRedirection();

app.Run();