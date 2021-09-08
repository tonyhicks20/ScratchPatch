using System;
using System.Transactions;

namespace ScratchPatch.Repository
{
	public class UnitOfWork : IDisposable
	{
		#region Private Fields

		private readonly TransactionScope transaction;

		#endregion Private Fields

		#region Public Constructors

        public UnitOfWork()
		{
			transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
		}

        public UnitOfWork(IsolationLevel isolationLevel)
        {
			transaction = new TransactionScope(
                TransactionScopeOption.RequiresNew,
                new TransactionOptions { IsolationLevel = isolationLevel },
                TransactionScopeAsyncFlowOption.Enabled);
        }

		#endregion Public Constructors

		#region Private Destructors

		~UnitOfWork()
		{
			Dispose(false);
		}

		#endregion Private Destructors

		#region Public Methods

		public void Complete()
		{
			transaction.Complete();
		}

		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		#endregion Public Methods

		#region Protected Methods

		protected virtual void Dispose(bool disposing)
		{
			transaction.Dispose();
		}

		#endregion Protected Methods
	}
}