using System;

namespace ScratchPatch.Caching
{
	public interface ICacheTimePolicy
	{
		Func<T, TimeSpan> GetTimeoutCalculator<T>(string mainKey, string subKey);
	}
}