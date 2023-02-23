using System;
using System.Collections.Concurrent;
using System.Runtime.InteropServices.ComTypes;
using System.Threading;
using System.Threading.Tasks;

namespace ScratchPatch.Caching.InProcess
{
	public class InMemoryAsyncLock
	{
		private static readonly ConcurrentDictionary<string, LockRelease> Locks = 
			new ConcurrentDictionary<string, LockRelease>();

		public async Task<IDisposable> LockAsync(string key)
		{
			string lockKey = key;
			var lockRelease = Locks.GetOrAdd(lockKey, k => new LockRelease());

			try
			{
				await lockRelease.Lock.WaitAsync();
				return lockRelease;
			}
			finally
			{
				if (lockRelease.Lock.CurrentCount == 1)
				{
					Locks.TryRemove(lockKey, out LockRelease _);
				}
			}
		}
		
		internal class LockRelease: IDisposable
		{
			public SemaphoreSlim Lock { get; } = new SemaphoreSlim(1, 1);

			public void Dispose()
			{
				Lock.Release();
				Lock?.Dispose();
			}
		}
	}
}