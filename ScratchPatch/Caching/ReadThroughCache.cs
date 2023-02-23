using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ScratchPatch.Caching
{
	/// <summary>
	/// The default implementation of IDataCache
	/// Generally this will be used, and its dependencies replaced to do what is required
	/// </summary>
	public class ReadThroughCache : IReadThroughCache
	{
		#region Private Fields

		private readonly ICacheStore _CacheStore;
		private readonly ICacheTimePolicy _CacheTimePolicy;
		private readonly ICacheLock _Locker;

		#endregion Private Fields

		#region Public Constructors

		public ReadThroughCache(ICacheStore cacheStore, ICacheLock locker, ICacheTimePolicy cacheTimePolicy)
		{
			_CacheStore = cacheStore;
			_CacheTimePolicy = cacheTimePolicy;
			_Locker = locker;
		}

		#endregion Public Constructors

		#region Public Methods

		public async Task<T> GetAsync<T>(string mainKey, string subKey, Func<Task<T>> getNewItemToCache, Func<T, bool> cacheBreaker = null, Func<T, TimeSpan> getCacheableTime = null)
		{
			var cacheTimeCalculator = getCacheableTime ?? _CacheTimePolicy.GetTimeoutCalculator<T>(mainKey, subKey);
			var cacheItem = await GetFromCacheStoreAsync(mainKey, subKey, getNewItemToCache, cacheTimeCalculator);

			T data = cacheItem.Value;

			if (cacheBreaker != null && !cacheItem.IsNew && cacheBreaker(data))
			{
				await RemoveAsync(mainKey, subKey);
				data = (await GetFromCacheStoreAsync(mainKey, subKey, getNewItemToCache, cacheTimeCalculator)).Value;
			}

			return data;
		}

		public async Task RemoveAsync(string mainKey, string subKey = null)
		{
			await _CacheStore.RemoveAsync(mainKey, subKey);
		}

		#endregion Public Methods

		#region Private Methods

		private async Task<CacheItem<T>> GetFromCacheStoreAsync<T>(string key, string subKey, Func<Task<T>> getNewItemToCache, Func<T, TimeSpan> getCacheableTime)
		{
			CacheItem<T> result = new CacheItem<T>();
			T data = await _CacheStore.GetAsync<T>(key, subKey);
			if (data == null || data.Equals(default(T)))
			{
				await _Locker.LockAsync(key, subKey, async () =>
				{
					data = await _CacheStore.GetAsync<T>(key, subKey);
					if (data == null || data.Equals(default(T)))
					{
						result.IsNew = true;
						data = await getNewItemToCache();
						if (IsCacheable(data))
							await _CacheStore.InsertAsync(key, subKey, data, getCacheableTime(data));
					}
				});
			}
			result.Value = data;
			return result;
		}

		private bool IsCacheable<T>(T value)
		{
			if (value == null || value.Equals(default(T)))
			{
				return false;
			}

			if (value is IEnumerable && !(value is IDictionary) && !(value is string))
			{
				var collection = value as ICollection;
				if (collection?.Count == 0)
				{
					return false;
				}
				if (!(value is ICollection) && !((IEnumerable<object>)value).Any())
				{
					return false;
				}
			}
			return true;
		}

		#endregion Private Methods

		#region Private Classes

		private class CacheItem<T>
		{
			#region Public Properties

			public bool IsNew { get; set; }
			public T Value { get; set; }

			#endregion Public Properties
		}

		#endregion Private Classes
	}
}