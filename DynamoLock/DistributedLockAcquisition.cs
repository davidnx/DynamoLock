using System;
using System.Threading;
using System.Threading.Tasks;

namespace DynamoLock
{
    /// <remarks>
    /// This only needs to be disposed if someone is using <see cref="CancellationToken.WaitHandle"/>
    /// on the provided <see cref="LockLost"/>.
    /// In all other cases, it is okay to not call dispose.
    /// </remarks>
    public struct DistributedLockAcquisition : IDisposable
    {
        private readonly Func<CancellationToken, Task> _releaseAsync;
        private readonly CancellationTokenSource _cts;

        private DistributedLockAcquisition(bool acquired, Func<CancellationToken, Task> releaseAsync, CancellationTokenSource cts)
        {
            Acquired = acquired;
            _releaseAsync = releaseAsync;
            _cts = cts;
        }

        public bool Acquired { get; }
        public CancellationToken LockLost => Acquired ? _cts.Token : throw CreateNotAcquiredException();

        public static DistributedLockAcquisition CreateAcquired(CancellationTokenSource cts, Func<CancellationToken, Task> releaseAsync)
        {
            _ = cts ?? throw new ArgumentNullException(nameof(cts));
            _ = releaseAsync ?? throw new ArgumentNullException(nameof(releaseAsync));

            return new DistributedLockAcquisition(true, releaseAsync, cts);
        }

        public static DistributedLockAcquisition CreateLost()
        {
            return new DistributedLockAcquisition(false, null, null);
        }

        public Task ReleaseLockAsync(CancellationToken cancellation)
        {
            if (!Acquired)
            {
                throw CreateNotAcquiredException();
            }

            return _releaseAsync(cancellation);
        }

        public void Dispose()
        {
            _cts?.Dispose();
        }

        private static Exception CreateNotAcquiredException()
        {
            return new InvalidOperationException("Invalid call, lock was not acquired");
        }
    }
}
