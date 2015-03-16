using System;
using System.Collections.Generic;
using System.Data.Services.Providers;
using System.Linq;
using System.Text;
using DataServiceProvider;
using MongoDB.Bson;

namespace DocumentDB.Context.InMemory
{
    public abstract class DocumentDbInMemoryDataService : DocumentDbDataServiceBase<DSPInMemoryContext, DSPResourceQueryProvider>
    {
        /// <summary>Constructor</summary>
        public DocumentDbInMemoryDataService(string connectionString, DocumentDbConfiguration dbConfiguration = null)
            : base(connectionString, dbConfiguration)
        {
        }

        public override DSPInMemoryContext CreateContext(string connectionString)
        {
            var dspContext = new DSPInMemoryContext();
            using (DocumentDbContext dbContext = new DocumentDbContext(connectionString))
            {
                PopulateData(dspContext, dbContext);
            }

            return dspContext;
        }

        private void PopulateData(DSPInMemoryContext dspContext, DocumentDbContext dbContext)
        {
            foreach (var resourceSet in this.Metadata.ResourceSets)
            {
                var storage = dspContext.GetResourceSetStorage(resourceSet.Name);
                var collection = dbContext.Database.GetCollection(resourceSet.Name);
                foreach (var document in collection.FindAll())
                {
                    var resource = DocumentDbDSPConverter.CreateDSPResource(document, this.dbMetadata, resourceSet.Name);
                    storage.Add(resource);

                    if (this.dbMetadata.Configuration.UpdateDynamically)
                    {
                        UpdateMetadataFromResourceSet(dbContext, resourceSet, document);
                    }
                }
            }
        }

        private void UpdateMetadataFromResourceSet(DocumentDbContext dbContext, ResourceSet resourceSet, BsonDocument document)
        {
            var resourceType = dbMetadata.ResolveResourceType(resourceSet.Name);
            foreach (var element in document.Elements)
            {
                dbMetadata.RegisterResourceProperty(dbContext, resourceType, element);
            }
        }
    }
}
