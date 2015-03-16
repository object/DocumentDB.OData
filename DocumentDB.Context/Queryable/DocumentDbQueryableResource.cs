using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using DataServiceProvider;

namespace DocumentDB.Context.Queryable
{
    public class DocumentDbQueryableResource : IQueryable<DSPResource>
    {
        private DocumentDbMetadata _documentDbMetadata;
        private DocumentDbQueryProvider provider;
        private Expression expression;

        public DocumentDbQueryableResource(DocumentDbMetadata dbMetadata, string connectionString, string collectionName, Type collectionType)
        {
            this._documentDbMetadata = dbMetadata;
            this.provider = new DocumentDbQueryProvider(dbMetadata, connectionString, collectionName, collectionType);
            this.expression = (new DSPResource[0]).AsQueryable().Expression;
        }

        public DocumentDbQueryableResource(DocumentDbQueryProvider provider, Expression expression)
        {
            this.provider = provider;
            this.expression = expression;
        }

        public IEnumerator<DSPResource> GetEnumerator()
        {
            return this.provider.ExecuteQuery<DSPResource>(this.expression);
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return this.provider.ExecuteQuery<DSPResource>(this.expression);
        }

        public Type ElementType
        {
            get { return typeof(DSPResource); }
        }

        public Expression Expression
        {
            get { return this.expression; }
        }

        public IQueryProvider Provider
        {
            get { return this.provider; }
        }
    }
}
