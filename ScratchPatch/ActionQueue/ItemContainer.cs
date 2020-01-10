using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ScratchPatch.ActionQueue
{
    internal abstract class ItemContainer
    {
        #region Public Methods

        public abstract bool HasItems();

        public abstract void PrepareForExecution();

        #endregion Public Methods
    }

    internal class ItemContainer<T> : ItemContainer
    {
        #region Private Fields

        private ConcurrentDictionary<string, T> _ActionItems;
        private ConcurrentDictionary<string, T> _Items = new ConcurrentDictionary<string, T>();

        #endregion Private Fields

        #region Public Methods

        public async Task ActionQueue(Func<IEnumerable<T>, Task> bulkAction)
        {
            if (bulkAction == null)
                throw new ArgumentNullException("An action for the queue must be given (Provide a legitimate value for queueAction)");

            if (_ActionItems == null)
                throw new NotSupportedException("You must manually call PrepareForExecution before executing queue items");

            IEnumerable<T> items = _ActionItems.Values;

            if (items.Any())
                await bulkAction(items);

            _ActionItems = null;
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="item"></param>
        /// <param name="mapIfExists">Map(ExistingItem, NewItem)</param>
        public void AddItem(T item, Action<T, T> mapIfExists = null)
        {
            bool isNew = false;
            var existing = _Items.GetOrAdd(item.GetHashCode().ToString(), x =>
            {
                isNew = true;
                return item;
            });

            if (!isNew && mapIfExists != null)
                mapIfExists(existing, item);
        }

        public override bool HasItems()
        {
            return !(_Items == null || _Items.IsEmpty);
        }

        public override void PrepareForExecution()
        {
            _ActionItems = _Items;
            _Items = new ConcurrentDictionary<string, T>();
        }

        #endregion Public Methods
    }
}