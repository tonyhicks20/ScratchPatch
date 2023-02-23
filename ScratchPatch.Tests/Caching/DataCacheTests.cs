using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ScratchPatch.Caching.InProcess;
using ScratchPatch.Caching.TimeoutSettings;

namespace ScratchPatch.Caching.Tests
{
	[TestClass]
	public class DataCacheTests
	{
#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
#pragma warning disable S1854 // Dead stores should be removed

		#region Private Fields

		private readonly ReadThroughCache _Cache;

		/// <summary>
		/// Everytime there is a cache miss, this value is incremented
		/// </summary>
		private int _CacheMisses;

		private readonly int _CacheValue = 123;
		private readonly Func<Task<int>> _GetNewItemToCache;
		private readonly string _MainKey, _SubKey;

		#endregion Private Fields

		#region Public Constructors

		public DataCacheTests()
		{
			_Cache = new ReadThroughCache
				(
					new InProcessCacheStore(
						new ServiceCollection()
					   .AddMemoryCache()
					   .BuildServiceProvider()
					   .GetService<IMemoryCache>()
					),
					new InMemoryCacheLockV1(),
					new FiveMinuteTimePolicy()
				);
			_MainKey = "MainKey";
			_SubKey = "SubKey";

			_GetNewItemToCache = async () =>
			{
				_CacheMisses += 1;
				return _CacheValue;
			};
		}

		#endregion Public Constructors

		#region Public Methods

		[TestCleanup]
		public void CleanUp()
		{
			_Cache.RemoveAsync(_MainKey, _SubKey);
			_CacheMisses = 0;
		}

		[TestMethod]
		public void EnsureCachePopulates()
		{
			int valReturned = _Cache.GetAsync(_MainKey, _SubKey, _GetNewItemToCache).Result;
			Assert.AreEqual(_CacheValue, valReturned);
			Assert.AreEqual(1, _CacheMisses);
		}

		[TestMethod]
		public void EnsureCachePopulatesOnce()
		{
			EnsureCachePopulates();
			int valReturned = _Cache.GetAsync(_MainKey, _SubKey, _GetNewItemToCache).Result;
			Assert.AreEqual(_CacheValue, valReturned);
			Assert.AreEqual(1, _CacheMisses, "The cache has been missed not as often as expected");
		}

		[TestMethod]
		public void EnsureCacheBreakerWorks()
		{
			int valReturned = _Cache.GetAsync(_MainKey, _SubKey, _GetNewItemToCache, i => i == 123).Result;
			Assert.AreEqual(valReturned, _CacheValue);
			Assert.AreEqual(1, _CacheMisses);
			valReturned = _Cache.GetAsync(_MainKey, _SubKey, _GetNewItemToCache, i => i == 123).Result;
			Assert.AreEqual(2, _CacheMisses);
			valReturned = _Cache.GetAsync(_MainKey, _SubKey, _GetNewItemToCache, i => i == 123).Result;
			Assert.AreEqual(3, _CacheMisses);
		}

		[TestMethod]
		public void EnsureTimeOutOverrideWorks()
		{
			int valReturned = _Cache.GetAsync(_MainKey, _SubKey, _GetNewItemToCache, getCacheableTime: i => TimeSpan.FromMilliseconds(100)).Result;
			Assert.AreEqual(valReturned, _CacheValue);
			Assert.AreEqual(1, _CacheMisses);
			valReturned = _Cache.GetAsync(_MainKey, _SubKey, _GetNewItemToCache, getCacheableTime: i => TimeSpan.FromMilliseconds(100)).Result;
			Assert.AreEqual(1, _CacheMisses);

			//Ensure cache time expired
			Thread.Sleep(100);
			valReturned = _Cache.GetAsync(_MainKey, _SubKey, _GetNewItemToCache, getCacheableTime: i => TimeSpan.FromMilliseconds(100)).Result;
			Assert.AreEqual(2, _CacheMisses);
		}

		[TestMethod]
		public void EnsureNoNullsCached()
		{
			Func<Task<object>> getNewItemToCache = async () =>
			{
				_CacheMisses += 1;
				return null;
			};
			object valReturned = _Cache.GetAsync(_MainKey, _SubKey, getNewItemToCache).Result;
			Assert.AreEqual(1, _CacheMisses);
			valReturned = _Cache.GetAsync(_MainKey, _SubKey, getNewItemToCache).Result;
			Assert.AreEqual(2, _CacheMisses);
			valReturned = _Cache.GetAsync(_MainKey, _SubKey, getNewItemToCache).Result;
			Assert.AreEqual(3, _CacheMisses);
		}

		[TestMethod]
		public void EnsureNoEmptyListCached()
		{
			Func<Task<List<object>>> getNewItemToCache = async () =>
			{
				_CacheMisses += 1;
				return new List<object>();
			};

			var valReturned = _Cache.GetAsync(_MainKey, _SubKey, getNewItemToCache).Result;
			Assert.AreEqual(1, _CacheMisses);
			valReturned = _Cache.GetAsync(_MainKey, _SubKey, getNewItemToCache).Result;
			Assert.AreEqual(2, _CacheMisses);

			valReturned = _Cache.GetAsync(_MainKey, _SubKey, getNewItemToCache).Result;

			Assert.AreEqual(3, _CacheMisses);
		}

		[TestMethod]
		public void NestedCacheItems()
		{
			var sb1 = _Cache.GetAsync("Item1", "subKey1", async () =>
			{
				await _Cache.GetAsync("Item1", "subKey2", async () => new StringBuilder("There"));
				return new StringBuilder("Hello");
			}).Result;

			var sb2 = _Cache.GetAsync("Item1", "subKey2", async () => new StringBuilder("You")).Result;

			Assert.AreEqual("Hello There", sb1 + " " + sb2);
		}

		[TestMethod]
		public void NestedCacheItemsDeadLock()
		{
			var task = _Cache.GetAsync("Item1", "subKey1", async () =>
			{
				//Recursive lock, deadlock!
				await _Cache.GetAsync("Item1", "subKey1", async () => new StringBuilder("There"));
				return new StringBuilder("Hello");
			});

			Thread.Sleep(5000);
			Assert.IsFalse(task.IsCompleted);
		}

		#endregion Public Methods

#pragma warning restore S1854 // Dead stores should be removed
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
	}
}