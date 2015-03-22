using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace DocumentDB.Context
{
    public class DocumentDatabase
    {
        private readonly DocumentClient _documentClient;
        private readonly Database _database;

        public DocumentDatabase(DocumentClient documentClient, string databaseName)
        {
            _documentClient = documentClient;
            _database = _documentClient.CreateDatabaseQuery()
                .Where(x => x.Id == databaseName)
                .AsEnumerable().Single();
        }

        public DocumentCollection GetCollection(string collectionName)
        {
            var documentCollection = _documentClient.CreateDocumentCollectionAsync(
                _database.CollectionsLink,
                new DocumentCollection
                {
                    Id = collectionName
                }).Result;

            return documentCollection;
        }
    }
}
