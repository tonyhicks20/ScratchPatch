using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace ScratchPatch.Caching.InProcess
{
	public class InMemoryCacheLockV2 : ICacheLock
	{

		private static readonly ConcurrentDictionary<string, SemaphoreSlim> Locks = new ConcurrentDictionary<string, SemaphoreSlim>();
		

		#region Public Methods

		public void Lock(string key, string subKey, Action insideLock)
		{
			throw new NotImplementedException();
		}

		public async Task LockAsync(string key, string subKey, Func<Task> insideLock)
		{
			string lockKey = GetLockKey(key, subKey);
			var @lock = Locks.GetOrAdd(lockKey, k => new SemaphoreSlim(1));
			
			try
			{
				await @lock.WaitAsync();
				await insideLock();
			}
			finally
			{
				if (@lock.CurrentCount == 1)
				{
					Locks.TryRemove(lockKey, out SemaphoreSlim _);	
				}
				@lock.Release();
			}
		}

		#endregion Public Methods

		#region Private Methods

		private string GetLockKey(string key, string subKey)
		{
			return $"{key}~{subKey}";
		}

		#endregion Private Methods
		
	}
}