using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DataServiceProvider;
using Microsoft.Azure.Documents;

namespace DocumentDB.Context.Queryable
{
    public abstract class DocumentDbQueryableDataService : DocumentDbDataServiceBase<DSPQueryableContext, DocumentDbDSPResourceQueryProvider>
    {
        public DocumentDbQueryableDataService(string connectionString, DocumentDbConfiguration dbConfiguration = null)
            : base(connectionString, dbConfiguration)
        {
            this.createResourceQueryProvider = () => new DocumentDbDSPResourceQueryProvider();
        }

        public override DSPQueryableContext CreateContext(string connectionString)
        {
            Func<string, IQueryable> queryProviders = x => GetQueryableCollection(connectionString, x,
                this.dbMetadata.ProviderTypes, this.dbMetadata.GeneratedTypes);
            var dspContext = new DSPQueryableContext(this.Metadata, queryProviders);
            return dspContext;
        }

        private IQueryable GetQueryableCollection(string connectionString, string collectionName,
            Dictionary<string, Type> providerTypes, Dictionary<string, Type> generatedTypes)
        {
            var collectionType = CreateDynamicTypeForCollection(collectionName, providerTypes, generatedTypes);

            //var conventionPack = new ConventionPack();
            //conventionPack.Add(new NamedIdMemberConvention(DocumentDbMetadata.MappedObjectIdName));
            //conventionPack.Add(new IgnoreExtraElementsConvention(true));
            //ConventionRegistry.Register(collectionName, conventionPack, t => t == collectionType);

            return InterceptingProvider.Intercept(
                new DocumentDbQueryableResource(this.dbMetadata, connectionString, collectionName, collectionType),
                new ResultExpressionVisitor());
        }

        private Type CreateDynamicTypeForCollection(string collectionName, Dictionary<string, Type> providerTypes, Dictionary<string, Type> generatedTypes)
        {
            Func<string, bool> criteria = x =>
                                          x.StartsWith(collectionName + ".") ||
                                          DocumentDbMetadata.UseGlobalComplexTypeNames &&
                                          x.StartsWith(collectionName + DocumentDbMetadata.WordSeparator);

            return CreateDynamicTypes(criteria, providerTypes, generatedTypes);
        }

        private Type CreateDynamicTypes(Func<string, bool> criteria, Dictionary<string, Type> providerTypes, Dictionary<string, Type> generatedTypes)
        {
            var fieldTypes = providerTypes.Where(x => criteria(x.Key));

            var fields = fieldTypes.ToDictionary(
                x => x.Key.Split('.').Last(),
                x => GetDynamicTypeForProviderType(x.Key, x.Value, providerTypes, generatedTypes));

            return DocumentTypeBuilder.CompileDocumentType(typeof(object), fields);
        }

        private Type GetDynamicTypeForProviderType(string typeName, Type providerType,
            Dictionary<string, Type> providerTypes, Dictionary<string, Type> generatedTypes)
        {
            if (DocumentDbMetadata.CreateDynamicTypesForComplexTypes && providerType == typeof(Document))
            {
                Type dynamicType;
                if (generatedTypes.ContainsKey(typeName))
                {
                    dynamicType = generatedTypes[typeName];
                }
                else
                {
                    var typeNameWords = typeName.Split('.');
                    Func<string, bool> criteria = x => x.StartsWith(string.Join(DocumentDbMetadata.WordSeparator, typeNameWords) + ".");

                    dynamicType = CreateDynamicTypes(criteria, providerTypes, generatedTypes);
                    generatedTypes.Add(typeName, dynamicType);
                }
                return dynamicType;
            }
            else
            {
                return providerType;
            }
        }
    }
}
