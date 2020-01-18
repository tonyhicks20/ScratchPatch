using System;
using System.Threading;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ScratchPatch.Caching.InProcess;

namespace ScratchPatch.Caching.Tests
{
	[TestClass]
	public class InProcessCacheStoreTests
	{
		#region Private Fields

		private readonly InProcessCacheStore cacheStore;

		#endregion Private Fields

		public InProcessCacheStoreTests()
		{
			cacheStore = new InProcessCacheStore
						(
							new ServiceCollection()
						   .AddMemoryCache()
						   .BuildServiceProvider()
						   .GetService<IMemoryCache>()
						);
		}

		#region Public Methods

		[TestCleanup]
		public void CleanUp()
		{
			removeItemFromCache();
		}

		[TestMethod]
		public void CacheEntryExpires()
		{
			KeyInserted();
			//Ensure timeout works
			Thread.Sleep(100);
			KeyNotFound();
		}

		[TestMethod]
		public void KeyRemoved()
		{
			KeyInserted();
			removeItemFromCache();
			KeyNotFound();
		}

		[TestMethod]
		public void KeyInserted()
		{
			var cacheVal = "SomeValue";
			insertValToCache(cacheVal, TimeSpan.FromMilliseconds(100));
			var stringVal = cacheStore.GetAsync<string>("MainKey", "SubKey").Result;
			Assert.AreEqual(stringVal, cacheVal);
		}

		[TestMethod]
		public void KeyNotFound()
		{
			var stringVal = getValFromCache();
			Assert.IsNull(stringVal);
		}

		#endregion Public Methods

		#region Private Methods

		private string getValFromCache()
		{
			return cacheStore.GetAsync<string>("MainKey", "SubKey").Result;
		}

		private void insertValToCache(string value, TimeSpan cacheTime)
		{
			cacheStore.InsertAsync("MainKey", "SubKey", value, cacheTime).Wait();
		}

		private void removeItemFromCache()
		{
			cacheStore.RemoveAsync("MainKey", "SubKey").Wait();
		}

		#endregion Private Methods
	}
}