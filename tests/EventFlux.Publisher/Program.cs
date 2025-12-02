using EventFlux.Publisher.Events;
using EventFlux.RabbitMQ;
using EventFlux.RabbitMQ.Abstractions;
using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);

// ❌ KALDIR: builder.Services.AddOpenApi();

// ✔ Swashbuckle
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

var sp = builder.Services.BuildServiceProvider();
var eventBus = sp.GetRequiredService<IEventBroker>();
await eventBus.SubscribeAsync<ExampleEventRequest, ExampleEventRequestHandler>();

app.MapControllers();
app.UseHttpsRedirection();
app.Run();
