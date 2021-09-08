using ScratchPatch.BulkCopy;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace ScratchPatch.Repository
{
	public interface IMsSqlRepository
	{
		void BulkInsert<T>(IEnumerable<T> items, int batchSize = 1000) where T : class;

		Task BulkInsertAsync<T>(IEnumerable<T> items, int batchSize = 1000) where T : class;

		void BulkUpdate<T>(IEnumerable<T> items, IEnumerable<string> joinNames, int batchSize = 1000) where T : class;

		Task BulkUpdateAsync<T>(IEnumerable<T> items, IEnumerable<string> joinNames, int batchSize = 1000) where T : class;

		void BulkUpsert<T>(QueryParmContainer<T> query) where T : class;

		Task BulkUpsertAsync<T>(QueryParmContainer<T> query) where T : class;

		int Count<T>(object parms, string columnName = "*");

		Task<int> CountAsync<T>(object parms, string columnName = "*");

		int Delete<T>(T item);

		int Delete<T>(object parms);

		Task<int> DeleteAsync<T>(T item);

		Task<int> DeleteAsync<T>(object parms);

		int Execute(string query, object parms);

		Task<int> ExecuteAsync(string query, object parms);

		object ExecuteNonQuery(string query, object parms);

		Task<object> ExecuteNonQueryAsync(string query, object parms);

		IEnumerable<T> ExecuteQuery<T>(string query, object parms);

		Task<IEnumerable<T>> ExecuteQueryAsync<T>(string query, object parms);

		IEnumerable<T> Find<T>(object parms);

		Task<IEnumerable<T>> FindAsync<T>();

		SqlConnection GetConnection();

		Task<SqlConnection> GetConnectionAsync();

		IEnumerable<T> GetStoredProcResults<T>(string spName, object parms);

		Task<IEnumerable<T>> GetStoredProcResultsAsync<T>(string spName, object parms);

		int Insert<T>(T item);

		Task<int> InsertAsync<T>(T item, params string[] columnNamesToIgnore);

		object InsertGetIdentity<T>(T item, IEnumerable<string> ignoreColumnNames = null);

		Task<object> InsertGetIdentityAsync<T>(T item, IEnumerable<string> ignoreColumnNames = null);

		IEnumerable<T> StoredProcedure<T>(string storedProc, object parms);

		Task<IEnumerable<T>> StoredProcedureAsync<T>(string storedProc, object parms);

		int Update<T>(T item, object whereClauseParms, object ignoreProperties = null);

		Task<int> UpdateAsync<T>(T item, object whereClauseParms, object ignoreProperties = null);

		int Upsert<T>(T item, object whereClauseParms);

		Task<int> UpsertAsync<T>(T item, object whereClauseParms);
	}
}