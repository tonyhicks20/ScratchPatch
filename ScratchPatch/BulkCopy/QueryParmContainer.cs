using System;
using System.Collections.Generic;
using System.Text;

namespace ScratchPatch.BulkCopy
{
    public class QueryParmContainer<T>
    {
        #region Public Constructors

        public QueryParmContainer(params T[] dataModels)
        {
            DataModels = dataModels;
        }

        public QueryParmContainer(IEnumerable<T> dataModels)
        {
            DataModels = dataModels;
        }

        #endregion Public Constructors

        #region Public Properties

        public int BatchSize { get; private set; } = 1000;

        public IEnumerable<T> DataModels { get; private set; }
        public IEnumerable<string> PrimaryKeyColumns { get; private set; }
        public IEnumerable<string> PropertiesForJoin { get; private set; }
        public IEnumerable<string> PropertiesForUpdate { get; private set; }

        #endregion Public Properties

        #region Public Methods

        public QueryParmContainer<T> SetPrimaryKeyColumns(params string[] propNames)
        {
            PrimaryKeyColumns = propNames;
            return this;
        }

        public QueryParmContainer<T> SetBatchSize(int batchSize)
        {
            BatchSize = batchSize;
            return this;
        }

        public QueryParmContainer<T> SetPropertiesForJoin(params string[] propNames)
        {
            PropertiesForJoin = propNames;
            return this;
        }

        public QueryParmContainer<T> SetPropertiesForUpdate(params string[] propNames)
        {
            PropertiesForUpdate = propNames;
            return this;
        }

        #endregion Public Methods
    }

}
