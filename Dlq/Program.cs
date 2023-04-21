using Cocona;
using Dlq;
using Dlq.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NLog;
using NLog.Extensions.Logging;

var builder = CoconaApp.CreateBuilder();

builder.Services.AddSingleton<IAppConfiguration>(_ => AppConfiguration.Instance);
builder.Services.AddSingleton<TransferDeadLetterMessages>();
builder.Services.AddLogging(loggingBuilder => loggingBuilder.AddNLog());
builder.Services.AddTransient<ILogger>(_ => LogManager.GetCurrentClassLogger());

var app = builder.Build();

app.AddSubCommand("queue", x =>
    {
        x.AddCommand("transfer", async (string queue, [FromService]TransferDeadLetterMessages transfer) =>
        {
            await transfer.ProcessDeadLetterMessagesAsync(queue);
        });
    })
    .WithDescription("Queue process");

app.AddSubCommand("topic", x =>
    {
        x.AddCommand("transfer", async (string topic, string subscription, [FromService]TransferDeadLetterMessages transfer) =>
        {
            await transfer.ProcessDeadLetterMessagesAsync(topic, subscription);
        });
        x.AddCommand("purge", async (string topic, string subscription, [FromService]TransferDeadLetterMessages transfer) =>
        {
            await transfer.PurgeDeadLetterMessagesAsync(topic, subscription);
        });
    })
    .WithDescription("Topic process");

app.AddCommand("info", ([FromService]ILogger logger) =>
{
   logger.Info("hello from the app");
});

await app.RunAsync();