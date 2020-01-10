using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ScratchPatch.ActionQueue
{
    public class ActionQueue
    {
        #region Private Fields

        private ConcurrentDictionary<string, ConcurrentDictionary<Type, ItemContainer>> _Batches = new ConcurrentDictionary<string, ConcurrentDictionary<Type, ItemContainer>>();

        #endregion Private Fields

        #region Private Methods

        private ItemContainer GetContainer<T>(string cacheKey)
        {
            if (!_Batches.TryGetValue(cacheKey, out ConcurrentDictionary<Type, ItemContainer> typeBatch))
                return null;
            if (!typeBatch.TryGetValue(typeof(T), out ItemContainer batch))
                return null;
            return batch;
        }

        #endregion Private Methods

        #region Public Methods

        /// <summary>
        /// NOT THREAD SAFE!!! - Do not call re-entrantly on the same type + key!
        /// </summary>
        public async Task ActionItems<T>(Func<IEnumerable<T>, Task> bulkAction, string cacheKey = "")
        {
            var container = (ItemContainer<T>)GetContainer<T>(cacheKey);
            if (container != null)
                await container.ActionQueue(bulkAction);
        }

        public void PrepareForAction<T>(string queueKey = "")
        {
            ItemContainer container = GetContainer<T>(queueKey);
            if (container != null)
                container.PrepareForExecution();
        }

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj"></param>
        /// <param name="queueKey"></param>
        /// <param name="mapIfExistingFound">>Map(ExistingItem, NewItem)</param>
        /// <returns></returns>
        public virtual T AddItem<T>(T obj, string queueKey = "", Action<T, T> mapIfExistingFound = null)
        {
            var typeBatch = _Batches.GetOrAdd(queueKey, x => new ConcurrentDictionary<Type, ItemContainer>());
            ItemContainer itemBatch = typeBatch.GetOrAdd(typeof(T), x => new ItemContainer<T>());
            ((ItemContainer<T>)itemBatch).AddItem(obj, mapIfExistingFound);
            return obj;
        }

        #endregion Public Methods
    }
}