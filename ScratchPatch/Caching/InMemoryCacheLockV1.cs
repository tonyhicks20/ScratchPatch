using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace ScratchPatch.Caching.InProcess
{
	public class InMemoryCacheLockV1 : ICacheLock
	{

		private static readonly ConcurrentDictionary<string, LockTracker> Locks = new ConcurrentDictionary<string, LockTracker>();
		

		#region Public Methods

		public void Lock(string key, string subKey, Action insideLock)
		{
			throw new NotImplementedException();
		}

		public async Task LockAsync(string key, string subKey, Func<Task> insideLock)
		{
			string lockKey = GetLockKey(key, subKey);
			LockTracker lockTracker = Locks.GetOrAdd(lockKey + "0", k => new LockTracker());
			Interlocked.Increment(ref lockTracker.WaitingThreads);

			try
			{
				await lockTracker.Lock.WaitAsync();
				await insideLock();
			}
			finally
			{
				Locks.TryRemove(lockKey + Interlocked.Decrement(ref lockTracker.WaitingThreads), out LockTracker _);
				lockTracker.Lock.Release();
			}
		}

		#endregion Public Methods

		#region Private Methods

		private string GetLockKey(string key, string subKey)
		{
			return $"{key}~{subKey}";
		}

		#endregion Private Methods

		#region Private Classes

		private class LockTracker
		{
			#region Public Fields

			public int WaitingThreads;

			#endregion Public Fields

			#region Public Properties

			public SemaphoreSlim Lock { get; } = new SemaphoreSlim(1, 1);

			#endregion Public Properties
		}

		#endregion Private Classes
	}
}