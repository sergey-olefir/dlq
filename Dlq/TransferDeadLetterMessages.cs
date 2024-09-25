using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Dlq.Configuration;
using NLog;

namespace Dlq
{
    internal class TransferDeadLetterMessages
    {
        private readonly ILogger _logger;
        private readonly IAppConfiguration _configuration;
        private readonly ServiceBusClient _client;

        public TransferDeadLetterMessages(ILogger logger, IAppConfiguration configuration)
        {
            this._logger = logger;
            this._configuration = configuration;
            this._client = new ServiceBusClient(configuration.ServiceBusConnectionString);
        }

        public async Task PurgeDeadLetterMessagesAsync(string topicName, string subscriberName)
        {
            await this.ProcessTopicAsync(topicName, subscriberName, async (_, receiver) =>
            {
                var wait = new TimeSpan(0, 0, 10);
                var now = DateTime.UtcNow;

                this._logger.Info("starting to purge");
                this._logger.Info($"fetching messages ({wait.TotalSeconds} seconds retrieval timeout)");

                var dlqMessages = new List<ServiceBusReceivedMessage>(this._configuration.BatchSize);
                int counter = 0;
                do
                {
                    dlqMessages.Clear();
                    IReadOnlyList<ServiceBusReceivedMessage> receivedMessages;
                    do
                    {
                        receivedMessages = await receiver.ReceiveMessagesAsync(this._configuration.BatchSize, wait);
                        dlqMessages.AddRange(receivedMessages.Where(x => x.EnqueuedTime < now).Where(x => x.EnqueuedTime < new DateTime(2023, 07, 25, 09, 56, 00, DateTimeKind.Utc)).ToArray());
                        counter += receivedMessages.Count;
                        this._logger.Info($"progessed {counter} messages");
                    } while (receivedMessages.Count != 0 && dlqMessages.Count < this._configuration.BatchSize);

                    this._logger.Info($"dl-count: {dlqMessages.Count}");
                    this._logger.Info($"dl-messages-sent: {dlqMessages.Count}");

                    await Task.WhenAll(dlqMessages.Select(x => this.CompleteMessageAsync(x, receiver)));

                    this._logger.Info("--------------------------------------------------------------------------------------");
                } while (dlqMessages.Count > 0);

                this._logger.Info("finished");
            });
        }

        public async Task ProcessDeadLetterMessagesAsync(string queueName, string subscriberName)
        {
            await this.ProcessTopicAsync(queueName, subscriberName, async (sender, receiver) =>
            {
                var wait = new TimeSpan(0, 0, 10);
                var now = DateTime.UtcNow;

                this._logger.Info("starting to transfer");
                this._logger.Info($"fetching messages ({wait.TotalSeconds} seconds retrieval timeout)");

                var dlqMessages = new List<ServiceBusReceivedMessage>(this._configuration.BatchSize);
                do
                {
                    dlqMessages.Clear();
                    IReadOnlyList<ServiceBusReceivedMessage> receivedMessages;
                    do
                    {
                        receivedMessages = await receiver.ReceiveMessagesAsync(this._configuration.BatchSize, wait);
                        dlqMessages.AddRange(receivedMessages.Where(x => x.EnqueuedTime < now).ToArray());
                    } while (receivedMessages.Count != 0 && dlqMessages.Count < this._configuration.BatchSize);

                    this._logger.Info($"dl-count: {dlqMessages.Count}");

                    this._logger.Info($"dl-attempting to send batch messages: {dlqMessages.Count}");
                    var messages = dlqMessages.Select(x => new ServiceBusMessage(x)).ToArray();
                    await sender.SendMessagesAsync(messages);
                    this._logger.Info($"dl-messages-sent: {dlqMessages.Count}");

                    await Task.WhenAll(dlqMessages.Select(x => this.CompleteMessageAsync(x, receiver)));

                    this._logger.Info("--------------------------------------------------------------------------------------");
                } while (dlqMessages.Count > 0);

                this._logger.Info("finished");
            });
        }

        public async Task ProcessDeadLetterMessagesAsync(string queueName)
        {
            await this.ProcessQueueAsync(queueName, async (sender, receiver) =>
            {
                var blockingCollection = new BlockingCollection<List<ServiceBusReceivedMessage>>(boundedCapacity: 20);
                var wait = new TimeSpan(0, 0, 30);
                var now = DateTime.UtcNow;

                async Task Produce()
                {
                    this._logger.Info("starting to transfer");
                    this._logger.Info($"fetching messages ({wait.TotalSeconds} seconds retrieval timeout)");

                    var dlqMessages = new List<ServiceBusReceivedMessage>(this._configuration.BatchSize);
                    do
                    {
                        dlqMessages.Clear();
                        IReadOnlyList<ServiceBusReceivedMessage> receivedMessages;
                        do
                        {
                            receivedMessages = await receiver.ReceiveMessagesAsync(this._configuration.BatchSize, wait);
                            dlqMessages.AddRange(receivedMessages.Where(x => x.EnqueuedTime < now).ToArray());
                        } while (receivedMessages.Count != 0 && dlqMessages.Count < this._configuration.BatchSize);

                        blockingCollection.Add(dlqMessages.ToList());

                        this._logger.Info($"dl-count-produced: {dlqMessages.Count}");
                    } while (dlqMessages.Count > 0);

                    blockingCollection.CompleteAdding();
                    this._logger.Info("finished");
                }

                async Task Consume()
                {
                    foreach (var dlqMessages in blockingCollection.GetConsumingEnumerable())
                    {
                        this._logger.Info($"dl-count-consumed: {dlqMessages.Count}");

                        this._logger.Info($"dl-attempting to send batch messages: {dlqMessages.Count}");
                        var messages = dlqMessages.Select(x => new ServiceBusMessage(x)).ToArray();
                        await sender.SendMessagesAsync(messages);
                        this._logger.Info($"dl-messages-sent: {dlqMessages.Count}");

                        await Task.WhenAll(dlqMessages.Select(x => this.CompleteMessageAsync(x, receiver)));

                        this._logger.Info("--------------------------------------------------------------------------------------");
                    }
                }

                var producerTask = Task.Run(Produce);

                var tasks = Enumerable.Range(0, 5).Select(_ => Task.Run(Consume)).Append(producerTask).ToArray();
                await Task.WhenAll(tasks);
            });
        }

        private async Task CompleteMessageAsync(ServiceBusReceivedMessage dlqMessage, ServiceBusReceiver dlqReceiver)
        {
            this._logger.Trace($"start completing message {dlqMessage.MessageId}");
            this._logger.Trace($"message-{dlqMessage.MessageId}-body: {dlqMessage.Body}");
            this._logger.Trace($"message-{dlqMessage.MessageId}-reason: {dlqMessage.DeadLetterReason}");
            this._logger.Trace($"message-{dlqMessage.MessageId}-error-description: {dlqMessage.DeadLetterErrorDescription}");

            await dlqReceiver.CompleteMessageAsync(dlqMessage);

            this._logger.Trace($"finished completing message {dlqMessage.MessageId}");
        }

        private async Task ProcessTopicAsync(string topicName, string subscriberName, Func<ServiceBusSender, ServiceBusReceiver, Task> action)
        {
            ServiceBusSender? sender = this._client.CreateSender(topicName);

            try
            {
                ServiceBusReceiver? dlqReceiver = this._client.CreateReceiver(topicName, subscriberName, new ServiceBusReceiverOptions
                {
                    SubQueue = SubQueue.DeadLetter,
                    ReceiveMode = ServiceBusReceiveMode.PeekLock
                });

                this._logger.Info($"topic: {topicName} -> subscriber: {subscriberName}");
                await action(sender, dlqReceiver);
                await dlqReceiver.CloseAsync();
            }
            catch (ServiceBusException ex)
            {
                if (ex.Reason == ServiceBusFailureReason.MessagingEntityNotFound)
                {
                    this._logger.Error(ex, $"Topic:Subscriber '{topicName}:{subscriberName}' not found. Check that the name provided is correct.");
                }
                else
                {
                    throw;
                }
            }
            finally
            {
                await sender.CloseAsync();
                await this._client.DisposeAsync();
            }
        }

        private async Task ProcessQueueAsync(string queueName, Func<ServiceBusSender, ServiceBusReceiver, Task> action)
        {
            ServiceBusSender? sender = this._client.CreateSender(queueName);

            try
            {
                ServiceBusReceiver? dlqReceiver = this._client.CreateReceiver(queueName, new ServiceBusReceiverOptions
                {
                    SubQueue = SubQueue.DeadLetter,
                    ReceiveMode = ServiceBusReceiveMode.PeekLock
                });

                this._logger.Info($"queue: {queueName}");
                await action(sender, dlqReceiver);
                await dlqReceiver.CloseAsync();
            }
            catch (ServiceBusException ex)
            {
                if (ex.Reason == ServiceBusFailureReason.MessagingEntityNotFound)
                {
                    this._logger.Error(ex, $"Queue:Subscriber '{queueName}' not found. Check that the name provided is correct.");
                }
                else
                {
                    throw;
                }
            }
            finally
            {
                await sender.CloseAsync();
                await this._client.DisposeAsync();
            }
        }
    }
}