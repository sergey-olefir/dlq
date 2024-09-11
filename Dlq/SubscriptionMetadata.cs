namespace Dlq
{
    public class SubscriptionMetadata
    {
        public string Name { get; init; } = null!;

        public long DeadLettersCount { get; init; }

        public long ActiveCount { get; init; }
    }
}