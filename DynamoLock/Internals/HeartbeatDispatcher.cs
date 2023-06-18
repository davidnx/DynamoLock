using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DynamoLock.Internals
{
    internal class HeartbeatDispatcher : IHeartbeatDispatcher
    {
        private readonly ILogger _logger;
        private readonly IAmazonDynamoDB _client;
        private readonly DynamoDbLockOptions _options;

        public HeartbeatDispatcher(
            IAmazonDynamoDB dynamoClient,
            IOptions<DynamoDbLockOptions> options,
            ILogger<HeartbeatDispatcher> logger)
        {
            _client = dynamoClient ?? throw new ArgumentNullException(nameof(dynamoClient));
            _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task ExecuteAsync(Func<ICollection<LocalLock>> getSnapshotFunc, CancellationToken cancellation)
        {
            while (!cancellation.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(_options.HeartbeatInterval, cancellation);

                    foreach (var lockItem in getSnapshotFunc())
                    {
                        var now = DateTimeOffset.UtcNow;
                        var req = new PutItemRequest
                        {
                            TableName = _options.TableName,
                            Item = LockItemHelper.CreateLockItem(now, lockItem.LockId, _options),
                            ConditionExpression = "lock_owner = :node_id",
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                            {
                                { ":node_id", new AttributeValue(_options.NodeId) }
                            }
                        };

                        try
                        {
                            await _client.PutItemAsync(req, cancellation);
                        }
                        catch (ConditionalCheckFailedException)
                        {
                            _logger.LogWarning($"Lock not owned anymore when sending heartbeat for {lockItem.LockId}");
                            lockItem.OnLost?.Invoke();
                        }
                    }
                }
                catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
                {
                    // Graceful shutdown...
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Unhandled exception while sending heartbeat");
                }
            }
        }
    }
}
