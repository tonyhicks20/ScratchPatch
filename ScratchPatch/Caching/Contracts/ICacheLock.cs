using System;
using System.Threading.Tasks;

namespace ScratchPatch.Caching
{
	public interface ICacheLock
	{
		Task LockAsync(string key, string subKey, Func<Task> insideLock);

		void Lock(string key, string subKey, Action insideLock);
	}
}