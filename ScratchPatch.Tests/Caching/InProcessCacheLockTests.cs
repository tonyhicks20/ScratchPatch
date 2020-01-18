using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ScratchPatch.Caching.InProcess;

namespace ScratchPatch.Caching.Tests
{
	[TestClass]
	public class InProcessCacheLockTests
	{
		/// <summary>
		/// Ensures that no two threads access the resource at the same time
		/// </summary>
		[TestMethod]
		public void NoConcurrencyAllowed()
		{
			InProcessCacheLock lck = new InProcessCacheLock();
			int checker = 0;

			Dictionary<int, int> threads = new Dictionary<int, int>();

			Enumerable.Range(1, 100)
				.AsParallel()
				.ForAll
				(
				   async obj =>
				   {
					   int threadId = Thread.CurrentThread.ManagedThreadId;
					   if (!threads.TryGetValue(threadId, out int someVariable))
						   threads.Add(threadId, threadId);

					   await lck.LockAsync
					   (
						   "MainKey",
						   "SubKey",
						   async () =>
						   {
							   Assert.IsTrue(checker == 0);
							   checker += 1;

							   await Task.Delay(100);

							   checker -= 1;
						   }
						);
				   }
				);
		}
	}
}