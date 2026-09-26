namespace EventFlux.RabbitMQ
{
    public enum RabbitFlowFailureMode
    {
        /// <summary>Rejects the message and puts it back on its queue.</summary>
        Requeue,

        /// <summary>Moves the message to the dead-letter queue of its event.</summary>
        DeadLetter
    }

    public class RabbitFlowOptions
    {
        /// <summary>Connection and publish retry attempts.</summary>
        public int RetryCount { get; set; } = 5;

        /// <summary>What happens to a message whose handler throws. Defaults to <see cref="RabbitFlowFailureMode.Requeue"/>.</summary>
        public RabbitFlowFailureMode OnFailure { get; set; } = RabbitFlowFailureMode.Requeue;

        /// <summary>Dead-letter exchange name. Defaults to "{serviceName}_dead_letter".</summary>
        public string? DeadLetterExchange { get; set; }

        /// <summary>Maximum unacknowledged messages per consumer channel. 0 means no limit.</summary>
        public ushort PrefetchCount { get; set; }

        /// <summary>Subscribes every handler in the scanned assembly when the host starts.</summary>
        public bool AutoSubscribe { get; set; }

        internal string GetDeadLetterExchange(string serviceName)
            => string.IsNullOrWhiteSpace(DeadLetterExchange) ? $"{serviceName}_dead_letter" : DeadLetterExchange;

        internal static string GetDeadLetterQueue(string queueName) => $"{queueName}_dead_letter";
    }
}
