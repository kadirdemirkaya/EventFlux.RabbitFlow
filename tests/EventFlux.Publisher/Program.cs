using EventFlux.Publisher.Events;
using EventFlux.RabbitMQ;
using EventFlux.RabbitMQ.Abstractions;
using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddControllers();

builder.Services.AddEventFluxRabbitFlow(
    typeof(Program).Assembly,
    new ConnectionFactory()
    {
        Uri = new Uri("amqp://guest:guest@localhost:5672")
    },
    "eventbus_flow"
);

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

var eventBroker = app.Services.GetRequiredService<IEventBroker>();
await eventBroker.SubscribeAsync<ExampleEventRequest, ExampleEventRequestHandler>();

app.MapControllers();
app.UseHttpsRedirection();
app.Run();
