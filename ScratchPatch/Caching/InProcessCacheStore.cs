using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;

namespace ScratchPatch.Caching.InProcess
{
	public class InProcessCacheStore : ICacheStore
	{
		#region Private Fields

		private readonly IMemoryCache _MemoryCacheInstance;

		#endregion Private Fields

		public InProcessCacheStore(IMemoryCache memoryCache)
		{
			_MemoryCacheInstance = memoryCache;
		}

		#region Public Methods

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously

		// None of these methods are actually asynchronous since they don't need to wait for any I/O
		// bound operation to complete
		public async Task<T> GetAsync<T>(string key, string subKey)
		{
			if (_MemoryCacheInstance.TryGetValue(CreateKey(key, subKey), out T data))
				return data;
			return default;
		}

		public async Task InsertAsync<T>(string key, string subKey, T data, TimeSpan timeSpan)
		{
			var cacheKey = CreateKey(key, subKey);

			if (!string.IsNullOrWhiteSpace(cacheKey))
			{
				_MemoryCacheInstance.Remove(cacheKey);
				_MemoryCacheInstance.GetOrCreate(cacheKey, cacheEntry =>
				{
					cacheEntry.AbsoluteExpiration = DateTimeOffset.Now.Add(timeSpan);
					return data;
				});
			}
		}

		public async Task RemoveAsync(string mainKey, string subKey)
		{
			_MemoryCacheInstance.Remove(CreateKey(mainKey, subKey));
		}

#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously

		#endregion Public Methods

		#region Private Methods

		private string CreateKey(string key, string subKey)
		{
			string createdKey = key;
			if (!string.IsNullOrEmpty(subKey))
				createdKey = string.Format("{0}-{1}", key, subKey);
			return createdKey;
		}

		#endregion Private Methods
	}
}