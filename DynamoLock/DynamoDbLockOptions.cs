using System;

namespace DynamoLock
{
    public class DynamoDbLockOptions
    {
        public string TableName { get; set; }
        public string NodeId { get; set; } = Guid.NewGuid().ToString();
        public TimeSpan LeaseTime { get; set; } = TimeSpan.FromSeconds(30);
        public TimeSpan JitterTolerance { get; set; } = TimeSpan.FromSeconds(1);
        public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(15);
        public LockTableBillingMode TableBillingMode { get; set; } = LockTableBillingMode.PayPerRequest;

        public void Validate()
        {
            if (string.IsNullOrEmpty(TableName))
            {
                throw new InvalidOperationException($"{nameof(TableName)} must be non-empty");
            }

            if (string.IsNullOrEmpty(NodeId))
            {
                throw new InvalidOperationException($"{nameof(NodeId)} must be non-empty");
            }

            if (LeaseTime <= TimeSpan.Zero)
            {
                throw new InvalidOperationException($"{nameof(LeaseTime)} must be positive, found {LeaseTime}");
            }

            if (HeartbeatInterval <= TimeSpan.Zero || HeartbeatInterval >= LeaseTime)
            {
                throw new InvalidOperationException($"{nameof(HeartbeatInterval)} must be positive and smaller than {nameof(LeaseTime)} ({LeaseTime}), found {HeartbeatInterval}");
            }

            if (JitterTolerance <= TimeSpan.Zero)
            {
                throw new InvalidOperationException($"{nameof(JitterTolerance)} must be positive, found {JitterTolerance}");
            }
        }
    }

    public enum LockTableBillingMode
    {
        PayPerRequest,
        MinimalProvisionedThroughput,
    }
}
