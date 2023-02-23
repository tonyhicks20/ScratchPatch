using System;
using System.Threading.Tasks;

namespace ScratchPatch.Caching
{
	public interface IReadThroughCache
	{
		/// <summary>
		/// Gets an item from the cache
		/// </summary>
		/// <typeparam name="T">The type of the item</typeparam>
		/// <param name="mainKey">The main key of the item - The part that won't change given different variations</param>
		/// <param name="subKey">The subkey of the item - This will change based on parameters etc.</param>
		/// <param name="getNewItemToCache">A function to call when nothing is found in the cache</param>
		/// <param name="cacheBreaker">A function to call if something is found in the cache. Will remove the item if this returns true</param>
		/// <param name="getCacheableTime">A function to call to calculate the time to cache an item. Will only be called if a new item is being cached</param>
		/// <returns></returns>
		Task<T> GetAsync<T>(string mainKey, string subKey, Func<Task<T>> getNewItemToCache, Func<T, bool> cacheBreaker = null, Func<T, TimeSpan> getCacheableTime = null);

		Task RemoveAsync(string mainKey, string subKey = null);
	}
}