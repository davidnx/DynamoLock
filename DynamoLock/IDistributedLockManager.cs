using System.Threading;
using System.Threading.Tasks;

namespace DynamoLock
{
    public interface IDistributedLockManager
    {
        Task<DistributedLockAcquisition> AcquireLockAsync(string Id, CancellationToken cancellation);

        Task ExecuteAsync(CancellationToken cancellation);
    }
}
