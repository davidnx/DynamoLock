using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoLock.Internals;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DynamoLock
{
    public class DynamoDbLockManager : IDistributedLockManager
    {
        private readonly IAmazonDynamoDB _client;
        private readonly DynamoDbLockOptions _options;

        private readonly ILockTableProvisioner _provisioner;
        private readonly IHeartbeatDispatcher _heartbeatDispatcher;
        private readonly LocalLockTracker _lockTracker;

        private int _running;

        public DynamoDbLockManager(
            IAmazonDynamoDB client,
            IOptions<DynamoDbLockOptions> options,
            ILoggerFactory loggerFactory)
            : this(
                  client,
                  options,
                  new LockTableProvisioner(client, options, loggerFactory.CreateLogger<LockTableProvisioner>()),
                  new HeartbeatDispatcher(client, options, loggerFactory.CreateLogger<HeartbeatDispatcher>()))
        {
        }

        /// <summary>
        /// This is intended for unit tests.
        /// </summary>
        public DynamoDbLockManager(
            IAmazonDynamoDB client,
            IOptions<DynamoDbLockOptions> options,
            ILockTableProvisioner provisioner,
            IHeartbeatDispatcher heartbeatDispatcher)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
            _options.Validate();

            _provisioner = provisioner ?? throw new ArgumentNullException(nameof(provisioner));
            _heartbeatDispatcher = heartbeatDispatcher ?? throw new ArgumentNullException(nameof(heartbeatDispatcher));
            _lockTracker = new LocalLockTracker();
        }

        public async Task<DistributedLockAcquisition> AcquireLockAsync(string lockId, CancellationToken cancellation)
        {
            EnsureRunning();

            try
            {
                var now = DateTimeOffset.UtcNow;
                var req = new PutItemRequest()
                {
                    TableName = _options.TableName,
                    Item = LockItemHelper.CreateLockItem(now, lockId, _options),
                    ConditionExpression = "attribute_not_exists(id) OR (expires < :expired)",
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                    {
                        { ":expired", new AttributeValue()
                            {
                                N = (now + _options.JitterTolerance).ToUnixTimeSeconds().ToString(),
                            }
                        }
                    }
                };
                var response = await _client.PutItemAsync(req, cancellation);

                if (response.HttpStatusCode != HttpStatusCode.OK)
                {
                    throw new InvalidOperationException($"Unexpected status code from DynamoDB: {(int)response.HttpStatusCode}");
                }

                var lockCts = new CancellationTokenSource();
                var lockItem = new LocalLock(lockId, () => lockCts.Cancel());
                _lockTracker.Add(lockItem);
                return DistributedLockAcquisition.CreateAcquired(lockCts, releaseCancellation => ReleaseLockAsync(lockId, releaseCancellation));
            }
            catch (ConditionalCheckFailedException)
            {
                return DistributedLockAcquisition.CreateLost();
            }
        }

        private async Task ReleaseLockAsync(string lockId, CancellationToken cancellation)
        {
            EnsureRunning();

            if (!_lockTracker.Remove(lockId))
            {
                return;
            }

            try
            {
                var req = new DeleteItemRequest()
                {
                    TableName = _options.TableName,
                    Key = new Dictionary<string, AttributeValue>
                    {
                        { "id", new AttributeValue(lockId) }
                    },
                    ConditionExpression = "lock_owner = :node_id",
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                    {
                        { ":node_id", new AttributeValue(_options.NodeId) }
                    }

                };
                await _client.DeleteItemAsync(req, cancellation);
            }
            catch (ConditionalCheckFailedException)
            {
            }
        }

        public async Task ExecuteAsync(CancellationToken cancellation)
        {
            if (Interlocked.CompareExchange(ref _running, 1, 0) != 0)
            {
                throw new InvalidOperationException($"{nameof(DynamoDbLockManager)} is already running");
            }

            try
            {
                // Init...
                await _provisioner.ProvisionAsync(cancellation);

                // Run heartbeat task in the background
                await _heartbeatDispatcher.ExecuteAsync(() => _lockTracker.GetSnapshot(), cancellation);
            }
            catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
            {
                return;
            }
            finally
            {
                try
                {
                    foreach (var lockItem in _lockTracker.GetSnapshot())
                    {
                        try
                        {
                            lockItem.OnLost();
                        }
                        catch { }
                    }

                    _lockTracker.Clear();
                }
                catch { }

                Volatile.Write(ref _running, 0);
            }
        }

        private void EnsureRunning()
        {
            if (Volatile.Read(ref _running) == 0)
            {
                throw new InvalidOperationException($"{nameof(DynamoDbLockManager)} isn't started");
            }
        }
    }
}
