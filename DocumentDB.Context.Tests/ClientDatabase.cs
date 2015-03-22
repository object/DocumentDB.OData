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
            var documentCollection = _documentClient.CreateDocumentCollectionAsync(
                _database.CollectionsLink,
                new DocumentCollection
                {
                    Id = collectionName
                }).Result;

            return new ClientCollection(_documentClient, documentCollection);
        }
    }
}