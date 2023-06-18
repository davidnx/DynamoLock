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
    internal class LockTableProvisioner : ILockTableProvisioner
    {
        private readonly IAmazonDynamoDB _client;
        private readonly DynamoDbLockOptions _options;
        private readonly ILogger _logger;

        public LockTableProvisioner(
            IAmazonDynamoDB dynamoClient,
            IOptions<DynamoDbLockOptions> options,
            ILogger<LockTableProvisioner> logger)
        {
            _client = dynamoClient ?? throw new ArgumentNullException(nameof(dynamoClient));
            _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task ProvisionAsync(CancellationToken cancellation)
        {
            try
            {
                try
                {
                    var table = await _client.DescribeTableAsync(_options.TableName, cancellation);
                    if (table.Table.TableStatus == TableStatus.ACTIVE)
                    {
                        _logger.LogInformation($"Lock table {_options.TableName} already exists, all good");
                        return;
                    }
                    else if (table.Table.TableStatus == TableStatus.CREATING)
                    {
                        await WaitForTableActivation(cancellation);
                    }

                }
                catch (ResourceNotFoundException)
                {
                    await CreateTable(cancellation);
                }

                var ttlConfig = await _client.DescribeTimeToLiveAsync(_options.TableName, cancellation);
                if (ttlConfig.TimeToLiveDescription == null ||
                    ttlConfig.TimeToLiveDescription.TimeToLiveStatus == TimeToLiveStatus.DISABLED)
                {
                    await SetPurgeTTL(cancellation);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error creating lock table {_options.TableName}", ex);
            }
        }

        private async Task CreateTable(CancellationToken cancellation)
        {
            _logger.LogInformation($"Creating lock table {_options.TableName}");
            var createRequest = new CreateTableRequest(_options.TableName, new List<KeySchemaElement>()
            {
                new KeySchemaElement("id", KeyType.HASH)
            })
            {
                AttributeDefinitions = new List<AttributeDefinition>()
                {
                    new AttributeDefinition("id", ScalarAttributeType.S)
                },
            };

            if (_options.TableBillingMode == LockTableBillingMode.PayPerRequest)
            {
                createRequest.BillingMode = BillingMode.PAY_PER_REQUEST;
            }
            else if (_options.TableBillingMode == LockTableBillingMode.MinimalProvisionedThroughput)
            {
                createRequest.ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 1,
                    WriteCapacityUnits = 1,
                };
            }
            else
            {
                throw new InvalidOperationException($"Unknown {nameof(_options.TableBillingMode)}: {_options.TableBillingMode}");
            }

            await _client.CreateTableAsync(createRequest, cancellation);
            await WaitForTableActivation(cancellation);
        }

        private async Task WaitForTableActivation(CancellationToken cancellation)
        {
            _logger.LogInformation($"Waiting for Active state for lock table {_options.TableName}");

            const int NumAttempts = 20;
            for (int i = 0; i < NumAttempts; i++)
            {
                try
                {
                    await Task.Delay(1000, cancellation);
                    var poll = await _client.DescribeTableAsync(_options.TableName, cancellation);
                    _logger.LogInformation($"Status for lock table {_options.TableName}: {poll.Table.TableStatus}");
                    if (poll.Table.TableStatus == TableStatus.ACTIVE)
                    {
                        _logger.LogInformation($"Table {_options.TableName} is now ready");
                        return;
                    }
                }
                catch (ResourceNotFoundException)
                {
                    _logger.LogInformation($"Lock table {_options.TableName} doesn't exist yet...");
                }
            }

            throw new InvalidOperationException($"Creation of lock table {_options.TableName} still not complete after {NumAttempts} polling attempts");
        }

        private async Task SetPurgeTTL(CancellationToken cancellation)
        {
            _logger.LogInformation($"Setting up TTL for table {_options.TableName}");
            var request = new UpdateTimeToLiveRequest()
            {
                TableName = _options.TableName,
                TimeToLiveSpecification = new TimeToLiveSpecification()
                {
                    AttributeName = "purge_time",
                    Enabled = true
                }
            };

            await _client.UpdateTimeToLiveAsync(request);
        }
    }
}
