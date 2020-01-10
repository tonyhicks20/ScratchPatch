using ScratchPatch.Extensions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading.Tasks;

namespace ScratchPatch.BulkCopy
{
    public class MsSqlBulkCopy
    {
        #region Private Fields

        private readonly string _connectionString;

        #endregion Private Fields

        #region Public Constructors

        public MsSqlBulkCopy(string connectionString)
        {
            _connectionString = connectionString;
        }

        #endregion Public Constructors

        #region Public Methods

        public void BulkInsert<T>(QueryParmContainer<T> query) where T : class
        {
            runAsyncMethod(() => BulkInsertAsync(query));
        }

        public async Task BulkInsertAsync<T>(QueryParmContainer<T> query) where T : class
        {
            validateQueryParms(query,
                nameof(QueryParmContainer<T>.DataModels)
                );

            await getConnectionAsync<T>(async connection =>
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        await bulkCopyInsertAsync(query.DataModels, query.BatchSize, connection, transaction).ConfigureAwait(false);
                        transaction.Commit();
                    }
                    catch (Exception)
                    {
                        transaction.Rollback();
                        throw;
                    }
                }
                return null;
            }).ConfigureAwait(false);
        }

        public void BulkUpdate<T>(QueryParmContainer<T> query) where T : class
        {
            runAsyncMethod(() => BulkUpdateAsync(query));
        }

        public async Task BulkUpdateAsync<T>(QueryParmContainer<T> query) where T : class
        {
            validateQueryParms(query,
                nameof(QueryParmContainer<T>.DataModels),
                nameof(QueryParmContainer<T>.PropertiesForJoin)
                );

            await getConnectionAsync<T>(async connection =>
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        await bulkCopyUpdateAsync(query.DataModels, query.BatchSize, query.PropertiesForJoin, query.PropertiesForUpdate, connection, transaction).ConfigureAwait(false);
                        transaction.Commit();
                    }
                    catch (Exception)
                    {
                        transaction.Rollback();
                        throw;
                    }
                }
                return null;
            }).ConfigureAwait(false);
        }

        public void BulkUpsert<T>(QueryParmContainer<T> query) where T : class
        {
            runAsyncMethod(() => BulkUpsertAsync(query));
        }

        public async Task BulkUpsertAsync<T>(QueryParmContainer<T> query) where T : class
        {
            validateQueryParms(query,
                nameof(QueryParmContainer<T>.DataModels),
                nameof(QueryParmContainer<T>.PrimaryKeyColumns),
                nameof(QueryParmContainer<T>.PropertiesForJoin)
                );

            await getConnectionAsync<T>(async connection =>
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        await bulkCopyMergeAsync(
                            query.DataModels,
                            query.PropertiesForUpdate,
                            query.PrimaryKeyColumns,
                            query.PropertiesForJoin,
                            connection,
                            transaction,
                            query.BatchSize).ConfigureAwait(false);
                        transaction.Commit();
                    }
                    catch (Exception)
                    {
                        transaction.Rollback();
                        throw;
                    }
                }
                return null;
            }).ConfigureAwait(false);
        }

        #endregion Public Methods

        #region Private Methods

        private async Task bulkCopyInsertAsync<T>(IEnumerable<T> items, int batchSize, SqlConnection connection, SqlTransaction transaction) where T : class
        {
            var itemsArray = items as T[] ?? items.ToArray();
            if (itemsArray == null || itemsArray.Length == 0 || batchSize <= 0)
                return;

            DataTable toInsert = itemsArray.ToDataTable();
            var schema = "dbo";

            using (var bulkCopy = getSqlBulkCopy(connection, transaction, toInsert))
            {
                bulkCopy.BatchSize = batchSize;
                bulkCopy.DestinationTableName = $"[{schema}].[{toInsert.TableName}]";
                await bulkCopy.WriteToServerAsync(toInsert).ConfigureAwait(false);
            }
        }

        private async Task bulkCopyMergeAsync<T>(IEnumerable<T> items, IEnumerable<string> propertiesToUpdate, IEnumerable<string> pkColumnNames, IEnumerable<string> joinNames, SqlConnection connection, SqlTransaction transaction, int batchSize) where T : class
        {
            if (items.IsNullOrEmpty() || batchSize <= 0)
                return;

            DataTable dt = items.ToDataTable();
            string tableName = typeof(T).Name;
            string schemaName = "dbo";

            string[] propertyNames = dt.Columns.OfType<DataColumn>().Select(dc => dc.ColumnName).ToArray();
            string selectColumns = string.Join(",", propertyNames);
            string insertColumns = string.Join(",", propertyNames.Select(propName => $"SOURCE.{propName}"));

            if (!propertiesToUpdate.IsNullOrEmpty())
                propertyNames = propertiesToUpdate.ToArray();

            string updateColumns = string.Join(",", propertyNames.Select(propName => $"TARGET.{propName} = SOURCE.{propName}"));
            string joinColumns = string.Join(" AND ", joinNames.Select(joinName => $"TARGET.{joinName} = SOURCE.{joinName}"));
            string whereMustInsert = string.Join(" AND ", pkColumnNames.Select(colName => $"TARGET.{colName} IS NULL"));

            string tempTableName = "#tmpMerge";

            using (SqlCommand command = new SqlCommand($"SELECT {selectColumns} INTO {tempTableName} FROM {schemaName}.{tableName} WHERE 1 = 0;", connection, transaction))
            {
                //Creating temp table on database
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                //Bulk insert into temp table
                using (SqlBulkCopy bulkCopy = getSqlBulkCopy(connection, transaction, dt))
                {
                    bulkCopy.BatchSize = batchSize;
                    bulkCopy.DestinationTableName = tempTableName;
                    await bulkCopy.WriteToServerAsync(dt).ConfigureAwait(false);
                    bulkCopy.Close();
                }

                // Updating destination table, and dropping temp table
                command.CommandText = $"UPDATE TARGET SET {updateColumns} FROM {schemaName}.{tableName} TARGET INNER JOIN {tempTableName} SOURCE ON {joinColumns};";
                int updatedItems = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                if (updatedItems != dt.Rows.Count)
                {
                    command.CommandText = $"INSERT INTO [{schemaName}].{tableName} ({selectColumns}) SELECT {insertColumns} FROM {tempTableName} SOURCE LEFT JOIN [{schemaName}].{tableName} TARGET (NOLOCK) ON {joinColumns} WHERE {whereMustInsert}";
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
        }

        private async Task bulkCopyUpdateAsync<T>(IEnumerable<T> items, int batchSize, IEnumerable<string> joinNames, IEnumerable<string> updateNames, SqlConnection connection, SqlTransaction transaction) where T : class
        {
            if (items.IsNullOrEmpty() || batchSize <= 0)
                return;

            DataTable dt = items.ToDataTable();
            string tableName = typeof(T).Name;
            string schemaName = "dbo";

            var propertyNames = dt.Columns.OfType<DataColumn>().Select(dc => dc.ColumnName);
            string selectColumns = string.Join(",", propertyNames);

            StringBuilder sb = new StringBuilder();
            (updateNames ?? propertyNames).ForEach(propName => sb.Append($",T.{propName} = Temp.{propName}"));
            sb.Remove(0, 1);
            string updateColumns = sb.ToString();

            sb.Clear();
            joinNames.ForEach(joinName => sb.Append($" AND Temp.{joinName} = T.{joinName}"));
            sb.Remove(0, 4);
            string joinColumns = sb.ToString();

            using (SqlCommand command = new SqlCommand("", connection, transaction))
            {
                //Creating temp table on database
                command.CommandText = $"SELECT {selectColumns} INTO #tmpUpdate FROM {schemaName}.{tableName} WHERE 1 = 0;";
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                //Bulk insert into temp table
                using (SqlBulkCopy bulkCopy = getSqlBulkCopy(connection, transaction, dt))
                {
                    bulkCopy.BatchSize = batchSize;
                    bulkCopy.DestinationTableName = "#tmpUpdate";
                    await bulkCopy.WriteToServerAsync(dt).ConfigureAwait(false);
                    bulkCopy.Close();
                }

                // Updating destination table, and dropping temp table
                command.CommandText = $"UPDATE T SET {updateColumns} FROM {schemaName}.{tableName} T INNER JOIN #tmpUpdate Temp ON {joinColumns}; DROP TABLE #tmpUpdate;";
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        private async Task<T> getConnectionAsync<T>(Func<SqlConnection, Task<T>> action)
        {
            using (var conn = new SqlConnection(_connectionString))
            {
                await conn.OpenAsync().ConfigureAwait(false);
                return await action(conn).ConfigureAwait(false);
            }
        }

        private SqlBulkCopy getSqlBulkCopy(SqlConnection connection, SqlTransaction transaction, DataTable dt)
        {
            var sqlBC = new SqlBulkCopy(connection, SqlBulkCopyOptions.FireTriggers | SqlBulkCopyOptions.CheckConstraints, transaction);
            foreach (DataColumn dc in dt.Columns)
            {
                sqlBC.ColumnMappings.Add(dc.ColumnName, dc.ColumnName);
            }
            return sqlBC;
        }

        private void runAsyncMethod(Func<Task> action)
        {
            try
            {
                Task.Run(async () => await action()).GetAwaiter().GetResult();
            }
            catch (AggregateException ex) when (ex.InnerExceptions.Count == 1)
            {
                ExceptionDispatchInfo.Capture(ex.Flatten().InnerExceptions.First()).Throw();
            }
        }

        private void validateQueryParms<T>(QueryParmContainer<T> query, params string[] propertiesToValidate)
        {
            StringBuilder errorMessage = new StringBuilder();

            foreach (string property in propertiesToValidate)
            {
                switch (property)
                {
                    case nameof(QueryParmContainer<T>.DataModels):
                        if (query.DataModels.IsNullOrEmpty())
                            errorMessage.AppendLine("DataModels must be given");
                        break;

                    case nameof(QueryParmContainer<T>.PropertiesForUpdate):
                        if (query.PropertiesForUpdate.IsNullOrEmpty())
                            errorMessage.AppendLine("PropertiesForUpdate must be specified");
                        break;

                    case nameof(QueryParmContainer<T>.PrimaryKeyColumns):
                        if (query.PrimaryKeyColumns.IsNullOrEmpty())
                            errorMessage.AppendLine("PrimaryKeyColumns must be specified");
                        break;

                    case nameof(QueryParmContainer<T>.PropertiesForJoin):
                        if (query.PropertiesForJoin.IsNullOrEmpty())
                            errorMessage.AppendLine("PropertiesForJoin must be specified");
                        break;
                }
            }

            if (query.PropertiesForUpdate.IsNullOrEmpty())
                errorMessage.AppendLine("PropertiesForUpdate must be specified");

            if (errorMessage.Length > 0)
                throw new NotSupportedException(errorMessage.ToString());
        }

        #endregion Private Methods
    }
}