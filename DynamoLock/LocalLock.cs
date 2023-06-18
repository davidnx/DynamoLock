using System;

namespace DynamoLock
{
    public class LocalLock
    {
        public LocalLock(string lockId, Action onLost)
        {
            if (string.IsNullOrEmpty(lockId))
            {
                throw new ArgumentException($"'{nameof(lockId)}' cannot be null or empty.", nameof(lockId));
            }

            LockId = lockId;
            OnLost = onLost ?? throw new ArgumentNullException(nameof(onLost));
        }

        public string LockId { get; }
        public Action OnLost { get; }
    }
}
