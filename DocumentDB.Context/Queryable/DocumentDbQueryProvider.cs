using System;
using System.Collections.Generic;
using System.Data.Services.Providers;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using DataServiceProvider;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace DocumentDB.Context.Queryable
{
    public class DocumentDbQueryProvider : IQueryProvider
    {
        private readonly DocumentDbContext dbContext;
        private readonly DocumentDbMetadata dbMetadata;
        private string connectionString;
        private readonly string collectionName;
        private readonly Type collectionType;

        public DocumentDbQueryProvider(DocumentDbMetadata dbMetadata, string connectionString, string collectionName, Type collectionType)
        {
            this.dbContext = new DocumentDbContext(connectionString);
            this.dbMetadata = dbMetadata;
            this.connectionString = connectionString;
            this.collectionName = collectionName;
            this.collectionType = collectionType;
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            if (ExpressionUtils.IsExpressionLinqSelect(expression))
            {
                return CreateProjectionQuery<TElement>(expression);
            }
            else
            {
                return new DocumentDbQueryableResource(this, expression) as IQueryable<TElement>;
            }
        }

        public IQueryable CreateQuery(Expression expression)
        {
            return new DocumentDbQueryableResource(this, expression);
        }

        public TResult Execute<TResult>(Expression expression)
        {
            if (expression == null)
            {
                throw new ArgumentNullException("expression");
            }
            if (!typeof(TResult).IsAssignableFrom(expression.Type))
            {
                throw new ArgumentException("Argument expression is not valid.");
            }
            return (TResult)Execute(expression);
        }

        public object Execute(Expression expression)
        {
            return ExecuteNonQuery(expression);
        }

        public IEnumerator<TElement> ExecuteQuery<TElement>(Expression expression)
        {
            DocumentCollection documentCollection;
            Expression mongoExpression;
            MethodInfo method;

            PrepareExecution(expression, "GetEnumerableCollection", out documentCollection, out mongoExpression, out method);

            var resourceEnumerable = method.Invoke(this, new object[] { documentCollection, mongoExpression }) as IEnumerable<DSPResource>;
            return resourceEnumerable.GetEnumerator() as IEnumerator<TElement>;
        }

        public object ExecuteNonQuery(Expression expression)
        {
            DocumentCollection documentCollection;
            Expression mongoExpression;
            MethodInfo method;

            PrepareExecution(expression, "GetExecutionResult", out documentCollection, out mongoExpression, out method);

            return method.Invoke(this, new object[] { documentCollection, mongoExpression });
        }

        private void PrepareExecution(Expression expression, string methodName, out DocumentCollection documentCollection, out Expression mongoExpression, out MethodInfo method)
        {
            documentCollection = this.dbContext.Database.GetCollection(collectionName);
            mongoExpression = new QueryExpressionVisitor(documentCollection, this.dbMetadata, collectionType).Visit(expression);

            var genericMethod = this.GetType().GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Instance);
            method = genericMethod.MakeGenericMethod(collectionType);
        }

        private IEnumerable<DSPResource> GetEnumerableCollection<TSource>(DocumentCollection documentCollection, Expression expression)
        {
            var enumerator = dbContext.Client.CreateDocumentQuery<TSource>(documentCollection.DocumentsLink)
                .Provider.CreateQuery<TSource>(expression).GetEnumerator();
            return GetEnumerable(enumerator);
        }

        private object GetExecutionResult<TSource>(DocumentCollection documentCollection, Expression expression)
        {
            return dbContext.Client.CreateDocumentQuery<TSource>(documentCollection.DocumentsLink)
                .Provider.Execute(expression);
        }

        private IEnumerable<DSPResource> GetEnumerable<TSource>(IEnumerator<TSource> enumerator)
        {
            while (enumerator.MoveNext())
            {
                yield return CreateDSPResource(enumerator.Current, this.collectionName);
            }
            yield break;
        }

        private DSPResource CreateDSPResource<TSource>(TSource document, string resourceName)
        {
            var typedDocument = document;
            var text = JsonConvert.SerializeObject(typedDocument);
            var resource = DocumentDbDSPConverter.CreateDSPResource(JObject.Parse(text), this.dbMetadata, resourceName);

            if (this.dbMetadata.Configuration.UpdateDynamically)
            {
                UpdateMetadataFromResourceSet(resourceName, JObject.Parse(text));
            }

            return resource;
        }

        private void UpdateMetadataFromResourceSet(string resourceName, JToken typedDocument)
        {
            var resourceType = dbMetadata.ResolveResourceType(resourceName);
            var collection = dbContext.Database.GetCollection(resourceName);
            //var query = Query.EQ(DocumentDbMetadata.ProviderObjectIdName, ObjectId.Parse(typedDocument.GetValue(DocumentDbMetadata.ProviderObjectIdName).ToString()));
            //var document = collection.FindOne(query);
            //foreach (var element in document.Elements)
            //{
            //    dbMetadata.RegisterResourceProperty(this.dbContext, resourceType, element);
            //}
        }

        private IQueryable<TElement> CreateProjectionQuery<TElement>(Expression expression)
        {
            var callExpression = expression as MethodCallExpression;

            MethodInfo methodInfo = typeof(DocumentDbQueryProvider)
                .GetMethod("ProcessProjection", BindingFlags.Instance | BindingFlags.NonPublic)
                .MakeGenericMethod(typeof(DSPResource), typeof(TElement));

            return
                (IQueryable<TElement>)methodInfo.Invoke(this,
                    new object[]
                        {
                            callExpression.Arguments[0],
                            ExpressionUtils.RemoveQuotes(callExpression.Arguments[1])
                        });
        }

        private IQueryable<TResultElement> ProcessProjection<TSourceElement, TResultElement>(Expression source, LambdaExpression lambda)
        {
            var dataSourceQuery = this.CreateQuery<TSourceElement>(source);
            var dataSourceQueryResults = dataSourceQuery.AsEnumerable();
            var newLambda = new ProjectionExpressionVisitor().Visit(lambda) as LambdaExpression;
            var projectionFunc = (Func<TSourceElement, TResultElement>)newLambda.Compile();

            var r = dataSourceQueryResults.FirstOrDefault();
            var u = projectionFunc(r);
            var q = dataSourceQueryResults.Select(sourceItem => projectionFunc(sourceItem));
            var z = q.FirstOrDefault();

            return dataSourceQueryResults.Select(sourceItem => projectionFunc(sourceItem)).AsQueryable();
        }
    }
}
