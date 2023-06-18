using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;

namespace DynamoLock.Internals
{
    internal static class LockItemHelper
    {
        internal static Dictionary<string, AttributeValue> CreateLockItem(DateTimeOffset now, string lockId, DynamoDbLockOptions options)
        {
            return new Dictionary<string, AttributeValue>
            {
                { "id", new AttributeValue(lockId) },
                { "lock_owner", new AttributeValue(options.NodeId) },
                {
                    "expires", new AttributeValue()
                    {
                        N = (now + options.LeaseTime).ToUnixTimeSeconds().ToString()
                    }
                },
                {
                    "purge_time", new AttributeValue()
                    {
                        N = (now + TimeSpan.FromTicks(options.LeaseTime.Ticks * 10)).ToUnixTimeSeconds().ToString()
                    }
                }
            };
        }
    }
}
