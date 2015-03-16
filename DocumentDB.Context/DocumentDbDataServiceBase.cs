using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DataServiceProvider;

namespace DocumentDB.Context
{
    public abstract class DocumentDbDataServiceBase<T,Q> : DSPDataService<T, Q, DSPUpdateProvider>
        where T : DSPContext
        where Q : DSPResourceQueryProvider
    {
        protected string connectionString;
        protected DocumentDbConfiguration dbConfiguration;
        protected static Action<string> ResetDataContext;
        protected static T context;
        protected DocumentDbMetadata dbMetadata;

        /// <summary>Constructor</summary>
        public DocumentDbDataServiceBase(string connectionString, DocumentDbConfiguration dbConfiguration)
        {
            this.connectionString = connectionString;
            this.dbConfiguration = dbConfiguration;
            this.createUpdateProvider = () => new DocumentDbDSPUpdateProvider(this.connectionString, this.CurrentDataSource, this.dbMetadata);

            ResetDataContext = x =>
            {
                this.dbMetadata = new DocumentDbMetadata(x, this.dbConfiguration == null ? null : this.dbConfiguration.MetadataBuildStrategy);
                DocumentDbDataServiceBase<T,Q>.context = this.CreateContext(x);
            };

            ResetDataContext(connectionString);
        }

        public static IDisposable RestoreDataContext(string connectionString)
        {
            return new DocumentDbDataServiceBase<T,Q>.RestoreDataContextDisposable(connectionString);
        }

        public abstract T CreateContext(string connectionString);

        private class RestoreDataContextDisposable : IDisposable
        {
            private readonly string connectionString;

            public RestoreDataContextDisposable(string connectionString)
            {
                this.connectionString = connectionString;
            }

            public void Dispose()
            {
                ResetDataContext(this.connectionString);
            }
        }

        protected override T CreateDataSource()
        {
            return context;
        }

        protected override DSPMetadata CreateDSPMetadata()
        {
            lock(this)
            {
                if (this.metadata == null)
                {
                    this.metadata = this.dbMetadata.CreateDSPMetadata();
                }
            }
            return metadata;
        }

        public static void ResetDSPMetadata()
        {
            DocumentDbMetadata.ResetDSPMetadata();
        }
    }
}
