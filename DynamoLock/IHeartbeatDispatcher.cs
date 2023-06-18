using System.Collections.Generic;
using System;
using System.Threading.Tasks;
using System.Threading;

namespace DynamoLock
{
    public interface IHeartbeatDispatcher
    {
        Task ExecuteAsync(Func<ICollection<LocalLock>> getSnapshotFunc, CancellationToken cancellation);
    }
}