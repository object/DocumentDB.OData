using System;
using System.Linq;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace DocumentDB.Context.Tests
{
    public class ClientDatabase
    {
        private readonly DocumentClient _documentClient;
        private readonly Database _database;

        public ClientDatabase(string endpointUrl, string authorizationKey, string databaseName, bool clearDatabase = false)
        {
            _documentClient = new DocumentClient(new Uri(endpointUrl), authorizationKey);

            var database = _documentClient.CreateDatabaseQuery().Where(x => x.Id == databaseName).AsEnumerable().FirstOrDefault();
            if (clearDatabase && database != null)
            {
                _documentClient.DeleteDatabaseAsync(database.SelfLink).Wait();
                database = null;
            }
            _database = database ?? _documentClient.CreateDatabaseAsync(new Database { Id = databaseName }).Result.Resource;
        }

        public ClientCollection GetCollection(string collectionName)
        {
            var documentCollection = _documentClient
                .CreateDocumentCollectionQuery(_database.SelfLink)
                .Where(c => c.Id == collectionName)
                .AsEnumerable()
                .FirstOrDefault();

            if (documentCollection == null)
            {
                documentCollection = new DocumentCollection {Id = collectionName};
                documentCollection.IndexingPolicy.IncludedPaths.Add(new IndexingPath
                {
                    IndexType = IndexType.Hash,
                    Path = "/",
                });
                if (collectionName == "Products")
                {
                    documentCollection.IndexingPolicy.IncludedPaths.Add(new IndexingPath
                    {
                        IndexType = IndexType.Range,
                        Path = @"/""ProductID""/?",
                    });
                    documentCollection.IndexingPolicy.IncludedPaths.Add(new IndexingPath
                    {
                        IndexType = IndexType.Range,
                        Path = @"/""Rating""/?",
                    });
                }
                
                documentCollection = _documentClient.CreateDocumentCollectionAsync(
                    _database.CollectionsLink, documentCollection).Result;
            }

            return new ClientCollection(_documentClient, documentCollection);
        }
    }
}
