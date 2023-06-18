using System.Threading;
using System.Threading.Tasks;

namespace DynamoLock
{
    public interface ILockTableProvisioner
    {
        Task ProvisionAsync(CancellationToken cancellation);
    }
}