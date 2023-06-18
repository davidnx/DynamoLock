using System;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace DynamoLock.Tests
{
    [Collection("DynamoDb collection")]
    public class DynamoDbLockerManagerTests
    {
        private readonly DynamoDbDockerSetup _dockerSetup;
        private readonly IDistributedLockManager _subject;
        private readonly TimeSpan _heartbeat = TimeSpan.FromSeconds(2);
        private readonly TimeSpan _leaseTime = TimeSpan.FromSeconds(3);

        public DynamoDbLockerManagerTests(DynamoDbDockerSetup dockerSetup)
        {
            _dockerSetup = dockerSetup;
            var cfg = new AmazonDynamoDBConfig { ServiceURL = DynamoDbDockerSetup.ConnectionString };
            var client = new AmazonDynamoDBClient(DynamoDbDockerSetup.Credentials, cfg);

            var options = Options.Create(new DynamoDbLockOptions
            {
                TableName = "lock-tests",
                LeaseTime = _leaseTime,
                HeartbeatInterval = _heartbeat,
            });
            _subject = new DynamoDbLockManager(client, options, new NullLoggerFactory());
        }

        [Fact]
        public async void should_lock_resource()
        {
            var lockId = Guid.NewGuid().ToString();

            using var cts = new CancellationTokenSource();
            var task = _subject.ExecuteAsync(cts.Token);

            var first = await _subject.AcquireLockAsync(lockId, CancellationToken.None);
            var second = await _subject.AcquireLockAsync(lockId, CancellationToken.None);

            cts.Cancel();
            await task;

            Assert.True(first.Acquired);
            Assert.False(second.Acquired);
        }

        [Fact]
        public async void should_release_lock()
        {
            var lockId = Guid.NewGuid().ToString();

            using var cts = new CancellationTokenSource();
            var task = _subject.ExecuteAsync(cts.Token);

            var first = await _subject.AcquireLockAsync(lockId, CancellationToken.None);
            await first.ReleaseLockAsync(CancellationToken.None);
            var second = await _subject.AcquireLockAsync(lockId, CancellationToken.None);

            cts.Cancel();
            await task;

            Assert.True(first.Acquired);
            Assert.True(second.Acquired);
        }

        [Fact]
        public async void should_renew_lock_when_heartbeat_active()
        {
            var lockId = Guid.NewGuid().ToString();

            using var cts = new CancellationTokenSource();
            var task = _subject.ExecuteAsync(cts.Token);

            var first = await _subject.AcquireLockAsync(lockId, CancellationToken.None);
            await Task.Delay(_leaseTime + TimeSpan.FromSeconds(2));
            var second = await _subject.AcquireLockAsync(lockId, CancellationToken.None);

            cts.Cancel();
            await task;

            Assert.True(first.Acquired);
            Assert.False(second.Acquired);
        }

        [Fact]
        public async void should_expire_lock_when_heartbeat_inactive()
        {
            var lockId = Guid.NewGuid().ToString();

            using var cts1 = new CancellationTokenSource();
            var task = _subject.ExecuteAsync(cts1.Token);

            var first = await _subject.AcquireLockAsync(lockId, CancellationToken.None);
            cts1.Cancel();
            await task;

            await Task.Delay(_leaseTime + TimeSpan.FromSeconds(2));

            using var cts2 = new CancellationTokenSource();
            task = _subject.ExecuteAsync(cts2.Token);

            var second = await _subject.AcquireLockAsync(lockId, CancellationToken.None);

            cts2.Cancel();
            await task;

            Assert.True(first.Acquired);
            Assert.True(second.Acquired);
        }
    }
}
