using System;
using System.Threading.Tasks;

namespace ScratchPatch.Caching
{
	public interface ICacheStore
	{
		Task<T> GetAsync<T>(string key, string subKey);

		Task InsertAsync<T>(string key, string subKey, T data, TimeSpan timeSpan);

		Task RemoveAsync(string mainKey, string subKey);
	}
}