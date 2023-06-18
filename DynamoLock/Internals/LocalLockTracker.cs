using System.Collections.Generic;
using System.Linq;

namespace DynamoLock.Internals
{
    internal class LocalLockTracker
    {
        private readonly Dictionary<string, LocalLock> _localLocks = new Dictionary<string, LocalLock>();
        private readonly object _syncRoot = new object();

        public void Add(LocalLock item)
        {
            lock (_syncRoot)
            {
                _localLocks[item.LockId] = item;
            }
        }

        public bool Remove(string id)
        {
            lock (_syncRoot)
            {
                if (_localLocks.ContainsKey(id))
                {
                    _localLocks.Remove(id);
                    return true;
                }

                return false;
            }
        }

        public void Clear()
        {
            lock (_syncRoot)
            {
                _localLocks.Clear();
            }
        }

        public ICollection<LocalLock> GetSnapshot()
        {
            lock (_syncRoot)
            {
                return _localLocks.Values.ToArray();
            }
        }
    }
}
