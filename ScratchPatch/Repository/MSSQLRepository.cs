using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using ScratchPatch.BulkCopy;
using ScratchPatch.Extensions;

namespace ScratchPatch.Repository
{
	public class MsSqlRepository : IMsSqlRepository
	{
		#region Private Fields

		private readonly string _connectionString;

		#endregion Private Fields

		#region Public Constructors

		public MsSqlRepository(string connectionString)
		{
			if (string.IsNullOrWhiteSpace(connectionString))
				throw new NotSupportedException("You must specify a connection string");
			_connectionString = connectionString;
		}

		#endregion Public Constructors

		#region Public Methods

		public void BulkInsert<T>(IEnumerable<T> items, int batchSize = 1000) where T : class
		{
			runAsyncMethod(() => BulkInsertAsync(items, batchSize));
		}

		public async Task BulkInsertAsync<T>(IEnumerable<T> items, int batchSize = 1000) where T : class
		{
			await getConnectionAsync<T>(async connection =>
			{
				using (var transaction = connection.BeginTransaction())
				{
					try
					{
						await bulkCopyInsertAsync(items, batchSize, connection, transaction).ConfigureAwait(false);
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
				nameof(QueryParmContainer<T>.PropertiesForUpdate),
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

		public void BulkUpdate<T>(IEnumerable<T> items, IEnumerable<string> joinNames, int batchSize = 1000) where T : class
		{
			runAsyncMethod(() => BulkUpdateAsync(items, joinNames, batchSize));
		}

		public async Task BulkUpdateAsync<T>(IEnumerable<T> items, IEnumerable<string> joinNames, int batchSize = 1000) where T : class
		{
			await getConnectionAsync<T>(async connection =>
			{
				using (var transaction = connection.BeginTransaction())
				{
					try
					{
						await bulkCopyUpdateAsync(items, batchSize, joinNames, connection, transaction).ConfigureAwait(false);
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

		public int Count<T>(object parms, string columnName = "*") => runAsyncMethod(() => CountAsync<T>(parms, columnName));

		public async Task<int> CountAsync<T>(object parms, string columnName = "*")
		{
			string query = $"SELECT COUNT({columnName}) FROM {getFullTableName<T>() + getWhereClause(parms)}";
			return (int)await ExecuteNonQueryAsync(query, parms).ConfigureAwait(false);
		}


		public int Delete<T>(T item)
		{
			return runAsyncMethod(() => DeleteAsync(item));
		}

		public int Delete<T>(object parms) => runAsyncMethod(() => DeleteAsync<T>(parms));

		public async Task<int> DeleteAsync<T>(T item) => await getConnectionAsync(c => c.ExecuteAsync($"DELETE FROM {getFullTableName<T>() + getWhereClause(item)}", item)).ConfigureAwait(false);

		public async Task<int> DeleteAsync<T>(object parms) => await getConnectionAsync(c => c.ExecuteAsync($"DELETE FROM {getFullTableName<T>() + getWhereClause(parms)}", parms)).ConfigureAwait(false);

		public int Execute(string query, object parms) => runAsyncMethod(() => ExecuteAsync(query, parms));

		public async Task<int> ExecuteAsync(string query, object parms) => await getConnectionAsync(c => c.ExecuteAsync(query, parms)).ConfigureAwait(false);

		public object ExecuteNonQuery(string query, object parms) => runAsyncMethod(() => ExecuteNonQueryAsync(query, parms));

		public async Task<object> ExecuteNonQueryAsync(string query, object parms) => await getConnectionAsync(c => c.ExecuteScalarAsync(query, parms)).ConfigureAwait(false);

		public IEnumerable<T> ExecuteQuery<T>(string query, object parms) => runAsyncMethod(() => ExecuteQueryAsync<T>(query, parms));

		public async Task<IEnumerable<T>> ExecuteQueryAsync<T>(string query, object parms) => await getConnectionAsync(c => c.QueryAsync<T>(query, parms)).ConfigureAwait(false);

		public IEnumerable<T> Find<T>(object parms) => runAsyncMethod(() => FindAsync<T>(parms));

		public async Task<IEnumerable<T>> FindAsync<T>() => await getConnectionAsync(c => c.QueryAsync<T>(getSelectClause<T>())).ConfigureAwait(false);

		public async Task<IEnumerable<T>> FindAsync<T>(object parms) => await getConnectionAsync(c => c.QueryAsync<T>(getSelectClause<T>() + getWhereClause(parms), parms)).ConfigureAwait(false);

		public SqlConnection GetConnection()
		{
			return runAsyncMethod(GetConnectionAsync);
		}

		public async Task<SqlConnection> GetConnectionAsync()
		{
			var conn = new SqlConnection(_connectionString);
			await conn.OpenAsync().ConfigureAwait(false);
			return conn;
		}

		public IEnumerable<T> GetStoredProcResults<T>(string spName, object parms)
		{
			return runAsyncMethod(() => GetStoredProcResultsAsync<T>(spName, parms));
		}

		public async Task<IEnumerable<T>> GetStoredProcResultsAsync<T>(string spName, object parms)
		{
			return await getConnectionAsync(c => c.QueryAsync<T>(spName, parms, commandType: CommandType.StoredProcedure)).ConfigureAwait(false);
		}


		public int Insert<T>(T item)
		{
			return runAsyncMethod(() => InsertAsync(item));
		}

		public async Task<int> InsertAsync<T>(T item, params string[] columnNamesToIgnore)
		{
			var columnNames = getColumnNames<T>(columnNamesToIgnore);
			string sql = $"INSERT INTO {getFullTableName<T>()} ({string.Join(",", columnNames)}) SELECT {string.Join(",", columnNames.Select(s => "@" + s))}";
			return await getConnectionAsync(c => c.ExecuteAsync(sql, item)).ConfigureAwait(false);
		}

		public object InsertGetIdentity<T>(T item, IEnumerable<string> ignoreColumnNames = null)
		{
			return runAsyncMethod(() => InsertGetIdentityAsync(item, ignoreColumnNames));
		}

		public async Task<object> InsertGetIdentityAsync<T>(T item, IEnumerable<string> ignoreColumnNames = null)
		{
			var columnNames = getColumnNames<T>(ignoreColumnNames);
			string sql = $"INSERT INTO {getFullTableName<T>()} ({string.Join(",", columnNames)}) SELECT {string.Join(",", columnNames.Select(s => "@" + s))}; SELECT @@IDENTITY;";
			return await getConnectionAsync(c => c.ExecuteScalarAsync(sql, item)).ConfigureAwait(false);
		}

		public IEnumerable<T> StoredProcedure<T>(string storedProc, object parms) => runAsyncMethod(() => StoredProcedureAsync<T>(storedProc, parms));

		public async Task<IEnumerable<T>> StoredProcedureAsync<T>(string storedProc, object parms) => await getConnectionAsync(c => c.QueryAsync<T>(storedProc, parms, commandType: CommandType.StoredProcedure)).ConfigureAwait(false);

		public int Update<T>(T item, object whereClauseParms, object ignoreProperties = null) => runAsyncMethod(() => UpdateAsync(item, whereClauseParms, ignoreProperties));

		public async Task<int> UpdateAsync<T>(T item, object whereClauseParms, object ignoreProperties = null)
		{
			Func<string, string> parmsStringConverter = name => name + "Parms";

			IEnumerable<string> ignoreColumnNames = null;

			if (ignoreProperties == null)
			{
				ignoreColumnNames = whereClauseParms.GetType().GetProperties().Select(itm => itm.Name);
			}
			else
			{
				ignoreColumnNames = ignoreProperties.GetType().GetProperties().Select(itm => itm.Name);
			}

			string sql = $"UPDATE {getFullTableName<T>()} SET {getColumnNamesWithParameters(item, kvDelimiter: ", ", ignoreColumnNames: ignoreColumnNames)} {getWhereClause(whereClauseParms, parmsStringConverter)}";
			DynamicParameters dynamicParameters = new DynamicParameters();
			addDynamicParameters(dynamicParameters, item);
			addDynamicParameters(dynamicParameters, whereClauseParms, parmsStringConverter);
			return await getConnectionAsync(c => c.ExecuteAsync(sql, dynamicParameters)).ConfigureAwait(false);
		}

		public int Upsert<T>(T item, object whereClauseParms) => runAsyncMethod(() => UpsertAsync(item, whereClauseParms));

		public async Task<int> UpsertAsync<T>(T item, object whereClauseParms)
		{
			var columnNames = getColumnNames<T>();
			string sql = $@"
IF NOT EXISTS (SELECT * FROM {getFullTableName<T>()}{getWhereClause(whereClauseParms)})
    INSERT INTO {getFullTableName<T>()} ({string.Join(",", columnNames)}) Values ({string.Join(",", columnNames.Select(s => "@" + s))})
ELSE
    UPDATE {getFullTableName<T>()} SET {getColumnNamesWithParameters(item, kvDelimiter: ", ", ignoreColumnNames: getColumnNames(whereClauseParms))}{getWhereClause(whereClauseParms)}
";
			return await getConnectionAsync(c => c.ExecuteAsync(sql, item)).ConfigureAwait(false);
		}

		#endregion Public Methods

		#region Private Methods

		private void addDynamicParameters(DynamicParameters dynamicParameters, object parms, Func<string, string> nameConverter = null)
		{
			if (dynamicParameters == null)
				throw new ArgumentNullException("dynamicParameters", "The argument 'dynamicParameters' must be given");
			if (parms == null)
				throw new ArgumentNullException("parms", "The argument 'parms' must be given");

			var propertyValues = parms.GetType().GetProperties()
			.Where(p => p.CanRead)
			.Select(p => new
			{
				Name = nameConverter == null ? p.Name : nameConverter(p.Name),
				Value = p.GetValue(parms)
			});

			foreach (var propVal in propertyValues)
				dynamicParameters.Add(propVal.Name, propVal.Value);
		}

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

		private async Task bulkCopyUpdateAsync<T>(IEnumerable<T> items, int batchSize, IEnumerable<string> joinNames, SqlConnection connection, SqlTransaction transaction) where T : class
		{
			if (items.IsNullOrEmpty() || batchSize <= 0)
				return;

			DataTable dt = items.ToDataTable();
			string tableName = typeof(T).Name;
			string schemaName = "dbo";

			string[] propertyNames = dt.Columns.OfType<DataColumn>().Select(dc => dc.ColumnName).ToArray();
			string selectColumns = string.Join(",", propertyNames);

			StringBuilder sb = new StringBuilder();
			propertyNames.ForEach(propName => sb.Append($",T.{propName} = Temp.{propName}"));
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
			using (var conn = await GetConnectionAsync().ConfigureAwait(false))
			{
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

		private T runAsyncMethod<T>(Func<Task<T>> func)
		{
			try
			{
				return Task.Run(async () => await func()).GetAwaiter().GetResult();
			}
			catch (AggregateException ex) when (ex.InnerExceptions.Count == 1)
			{
				ExceptionDispatchInfo.Capture(ex.Flatten().InnerExceptions.First()).Throw();
			}

			return default;
		}

		#endregion Private Methods

		#region Sql String Builders

		private IEnumerable<string> getColumnNames(Type type, IEnumerable<string> ignoreColumnNames = null)
		{
			if (ignoreColumnNames == null || !ignoreColumnNames.Any())
				return type.GetProperties().Select(p => p.Name).OrderBy(x => x);
			return type.GetProperties().Select(p => p.Name).Except(ignoreColumnNames).OrderBy(x => x);
		}

		private IEnumerable<string> getColumnNames<T>(IEnumerable<string> ignoreColumnNames = null)
		{
			return getColumnNames(typeof(T), ignoreColumnNames);
		}

		private IEnumerable<string> getColumnNames(object parms, IEnumerable<string> ignoreColumnNames = null)
		{
			return getColumnNames(parms.GetType(), ignoreColumnNames);
		}

		private string getColumnNamesWithParameters(object parms, Func<string, string> variableNameConverter = null, string kvDelimiter = " AND ", IEnumerable<string> ignoreColumnNames = null)
		{
			var columnNames = getColumnNames(parms, ignoreColumnNames);
			return string.Join(kvDelimiter, columnNames.Select(p => p + " = @" + (variableNameConverter == null ? p : variableNameConverter(p))));
		}

		private string getFullTableName<T>()
		{
			//Will return the schema as well as the name of the table.
			//Table name can also be overridden with an attribute
			var schemaName = "dbo";
			//SchemaAttribute schema = (SchemaAttribute)typeof(T).GetCustomAttributes(typeof(SchemaAttribute), false).FirstOrDefault();
			//if (schema != null)
			//	schemaName = schema.SchemaName;
			return $"[{schemaName}].[{typeof(T).Name}]";
		}

		private string getSelectClause<T>()
		{
			return $"SELECT * FROM {getFullTableName<T>()}";
		}

		private string getWhereClause(object parms, Func<string, string> variableNameConverter = null)
		{
			return " WHERE " + getColumnNamesWithParameters(parms, variableNameConverter);
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

		#endregion Sql String Builders
	}
}