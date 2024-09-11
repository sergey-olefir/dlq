using System;
using System.Collections.Generic;
using System.Linq;
using Azure.Messaging.ServiceBus.Administration;
using Dlq.Configuration;
using NLog;

namespace Dlq
{
    internal interface IServiceBusClientProxy
    {
        IAsyncEnumerable<string> GetTopics(params string[] patters);

        IAsyncEnumerable<SubscriptionMetadata> GetSubscriptions(string topic, params string[] patters);
    }

    internal class ServiceBusClientProxy : IServiceBusClientProxy
    {
        private readonly ILogger _logger;
        private readonly ServiceBusAdministrationClient _client;

        public ServiceBusClientProxy(ILogger logger, IAppConfiguration configuration)
        {
            this._logger = logger;
            this._client = new ServiceBusAdministrationClient(configuration.ServiceBusConnectionString);
        }

        public async IAsyncEnumerable<string> GetTopics(params string[] patters)
        {
            this._logger.Debug("starting to get topics from service bus");
            var topics = this._client.GetTopicsAsync();
            await foreach (var topic in topics)
            {
                if (patters.Any(x => topic.Name.Contains(x, StringComparison.InvariantCultureIgnoreCase)))
                    yield return topic.Name;
            }

            this._logger.Debug("topics have been retrieved");
        }

        public async IAsyncEnumerable<SubscriptionMetadata> GetSubscriptions(string topic, params string[] patters)
        {
            this._logger.Debug($"starting to get subscriptions from {topic} topic");
            var subscriptions = this._client.GetSubscriptionsAsync(topic);
            await foreach (var subscription in subscriptions)
            {
                if (patters.Any(x => subscription.SubscriptionName.Contains(x, StringComparison.InvariantCultureIgnoreCase)))
                {
                    var properties = await this._client.GetSubscriptionRuntimePropertiesAsync(topic, subscription.SubscriptionName);
                    yield return new SubscriptionMetadata
                    {
                        Name = properties.Value.SubscriptionName,
                        ActiveCount = properties.Value.ActiveMessageCount,
                        DeadLettersCount = properties.Value.DeadLetterMessageCount
                    };
                }
            }

            this._logger.Debug("subscriptions have been retrieved");
        }
    }
}