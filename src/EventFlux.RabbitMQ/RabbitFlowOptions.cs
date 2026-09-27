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

        /// <summary>
        /// Maximum number of times a message is handled before it is moved to the dead-letter queue, counting the first
        /// attempt. <see langword="null"/> (the default) keeps the <see cref="OnFailure"/> behaviour without a limit.
        /// </summary>
        /// <remarks>
        /// A failed message is sent back to the end of its queue with an <c>x-retry-count</c> header and the original is
        /// acknowledged. Every consumer of the queue must run a version that understands these messages.
        /// </remarks>
        public int? MaxDeliveryAttempts { get; set; }

        /// <summary>
        /// Time a failed message waits before it is handled again. Applies when <see cref="MaxDeliveryAttempts"/> is set.
        /// Defaults to <see cref="TimeSpan.Zero"/>. A positive delay declares a <c>{queue}_retry</c> queue.
        /// </summary>
        public TimeSpan RedeliveryDelay { get; set; } = TimeSpan.Zero;

        /// <summary>
        /// Waits for the broker to confirm every publish, including retry and dead-letter publishes, and fails the publish
        /// when the broker rejects it. Defaults to <see langword="false"/>.
        /// </summary>
        /// <remarks>
        /// Retry and dead-letter publishes are always confirmed when <see cref="OnFailure"/> is
        /// <see cref="RabbitFlowFailureMode.DeadLetter"/> or <see cref="MaxDeliveryAttempts"/> is set, whatever this value.
        /// A published event that no queue is bound to is logged as a warning and does not fail. Retry and dead-letter
        /// publishes that cannot be routed fail, and the message is requeued. Requires the built-in connection; a custom
        /// <see cref="Abstractions.IPersistenceConnection"/> must implement the <c>CreateChannelAsync</c> overload that takes options.
        /// </remarks>
        public bool PublisherConfirms { get; set; }

        /// <summary>
        /// Deletes the event queue (and its retry queue) when the last handler of an event is unsubscribed, dropping any
        /// messages still in it. Defaults to <see langword="false"/>: the queue is unbound and its consumer cancelled, and
        /// waiting messages are kept for the next subscription. The dead-letter queue is never deleted.
        /// </summary>
        public bool DeleteQueueOnUnsubscribe { get; set; }

        /// <summary>
        /// How long disposing the broker waits for messages that are being handled to be acknowledged. Defaults to 30 seconds.
        /// With <see cref="AutoSubscribe"/> the host's shutdown timeout applies first.
        /// </summary>
        public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(30);

        internal bool LimitsDeliveries => MaxDeliveryAttempts.HasValue;

        internal bool UsesDeadLetter => OnFailure == RabbitFlowFailureMode.DeadLetter || LimitsDeliveries;

        internal bool DelaysRedelivery => LimitsDeliveries && RedeliveryDelay > TimeSpan.Zero;

        internal void Validate()
        {
            if (MaxDeliveryAttempts is < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(MaxDeliveryAttempts), MaxDeliveryAttempts, "MaxDeliveryAttempts must be at least 1.");
            }

            if (ShutdownTimeout < TimeSpan.Zero && ShutdownTimeout != Timeout.InfiniteTimeSpan)
            {
                throw new ArgumentOutOfRangeException(nameof(ShutdownTimeout), ShutdownTimeout, "ShutdownTimeout must not be negative.");
            }

            if (RedeliveryDelay < TimeSpan.Zero || RedeliveryDelay.TotalMilliseconds > uint.MaxValue)
            {
                throw new ArgumentOutOfRangeException(nameof(RedeliveryDelay), RedeliveryDelay, "RedeliveryDelay must be between zero and 49 days.");
            }
        }

        internal string GetDeadLetterExchange(string serviceName)
            => string.IsNullOrWhiteSpace(DeadLetterExchange) ? $"{serviceName}_dead_letter" : DeadLetterExchange;

        internal static string GetDeadLetterQueue(string queueName) => $"{queueName}_dead_letter";

        internal static string GetRetryQueue(string queueName) => $"{queueName}_retry";
    }
}
