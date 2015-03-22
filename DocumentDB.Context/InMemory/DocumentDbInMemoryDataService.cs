using System;
using System.Collections.Generic;
using System.Data.Services.Providers;
using System.Linq;
using System.Text;
using DataServiceProvider;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json.Linq;

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
                var query = dbContext.Client.CreateDocumentQuery<JObject>(collection.DocumentsLink);
                foreach (var document in query)
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

        private void UpdateMetadataFromResourceSet(DocumentDbContext dbContext, ResourceSet resourceSet, JToken document)
        {
            var resourceType = dbMetadata.ResolveResourceType(resourceSet.Name);
            foreach (var element in document)
            {
                dbMetadata.RegisterResourceProperty(dbContext, resourceType, element);
            }
        }
    }
}
