using System;
using System.Threading;
using StackExchange.Redis;
using System.Threading.Tasks;

namespace ScratchPatch.Caching.InProcess
{
    public class RedisDistributedLock : IDisposable
    {
        private readonly IDatabase _redisDb;
        private readonly string _lockKey;
        private readonly string _lockChannel;
        private bool _lockTaken;
        private readonly string _lockValue = new Guid().ToString();
        private readonly ManualResetEventSlim _gate = new ManualResetEventSlim();
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();

        public RedisDistributedLock(string lockKey, ConnectionMultiplexer redisConnection)
        {
            _redisDb = redisConnection.GetDatabase();
            _lockKey = lockKey;
            _lockChannel = $"{lockKey}:lock-channel";
            
            new TaskFactory().StartNew(async () =>
            {
                var subscriber = _redisDb.Multiplexer.GetSubscriber();
                var channel = await subscriber.SubscribeAsync(_lockChannel);
                await channel.ReadAsync(_cancellationTokenSource.Token);
                _gate.Set();
            });
        }

        public async Task<IDisposable> LockAsync(TimeSpan expiryTime)
        {
            if (await _redisDb.LockTakeAsync(_lockKey, _lockValue, expiryTime))
            {
                _lockTaken = true; 
                _gate.Reset();
                return this;
            }

            if (!_gate.WaitHandle.WaitOne(expiryTime))
            {
                _cancellationTokenSource.Cancel();
            }
            return this;
        }

        public void Dispose()
        {
            if (!_lockTaken) 
                return; 
            _redisDb.LockRelease(_lockKey, _lockValue);
            _redisDb.Publish(_lockChannel, "released");
            _lockTaken = false;
        }
    }
}

