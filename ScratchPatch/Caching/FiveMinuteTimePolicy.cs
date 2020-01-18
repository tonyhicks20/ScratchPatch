using System;

namespace ScratchPatch.Caching.TimeoutSettings
{
	public class FiveMinuteTimePolicy : ICacheTimePolicy
	{
		public Func<T, TimeSpan> GetTimeoutCalculator<T>(string mainKey, string subKey)
		{
			return item => new TimeSpan(0, 5, 0);
		}
	}
}