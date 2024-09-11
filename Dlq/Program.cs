using Cocona;
using Dlq;
using Dlq.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NLog;
using NLog.Extensions.Logging;

var builder = CoconaApp.CreateBuilder();

builder.Services.AddSingleton<IAppConfiguration>(_ => AppConfiguration.Instance);
builder.Services.AddSingleton<TransferDeadLetterMessages>();
builder.Services.AddSingleton<ServiceBusClientProxy>();
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
        x.AddCommand("transfer", async (string topic, string subscription, [FromService] TransferDeadLetterMessages transfer) =>
        {
            await transfer.ProcessDeadLetterMessagesAsync(topic, subscription);
        });
        x.AddCommand("purge", async (string topic, string subscription, [FromService] TransferDeadLetterMessages transfer) =>
        {
            await transfer.PurgeDeadLetterMessagesAsync(topic, subscription);
        });
        x.AddCommand("transfer-batch", async (
            string[] source,
            string[] destination,
            [FromService] ServiceBusClientProxy clientProxy,
            [FromService] ILogger logger,
            [FromService] TransferDeadLetterMessages transfer) =>
        {
            await foreach (var topic in clientProxy.GetTopics(source))
            {
                logger.Info($"processing topic with name {topic}");
                await foreach (var subscription in clientProxy.GetSubscriptions(topic, destination))
                {
                    if (subscription.DeadLettersCount > 0)
                    {
                        logger.Info($"starting to tranfser {subscription.Name} with {subscription.DeadLettersCount} messages");
                        await transfer.ProcessDeadLetterMessagesAsync(topic, subscription.Name);
                    }
                }

                logger.Info("--------------------------------------------------------------------------------------");
            }
        });
        x.AddCommand("list", async (
            string[] source,
            string[] destination,
            [Option('d')] bool deadLetters,
            [FromService] ServiceBusClientProxy clientProxy,
            [FromService] ILogger logger,
            [FromService] TransferDeadLetterMessages transfer) =>
        {
            await foreach (var topic in clientProxy.GetTopics(source))
            {
                logger.Info($"processing topic with name {topic}");
                await foreach (var subscription in clientProxy.GetSubscriptions(topic, destination))
                {
                    if (subscription.DeadLettersCount > 0)
                    {
                        logger.Info($"got subscription {topic} -> {subscription.Name} ({subscription.ActiveCount}, {subscription.DeadLettersCount})");
                    }
                    else if (!deadLetters)
                    {
                        logger.Info($"got subscription {topic} -> {subscription.Name} ({subscription.ActiveCount}, {subscription.DeadLettersCount})");
                    }
                }

                logger.Info("--------------------------------------------------------------------------------------");
            }
        });
    })
    .WithDescription("Topic process");

app.AddCommand("info", ([FromService]ILogger logger) =>
{
   logger.Info("hello from the app");
});

await app.RunAsync();