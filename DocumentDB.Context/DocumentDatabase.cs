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

        public IEnumerable<DocumentCollection> GetCollections()
        {
            var documentCollections = _documentClient
                .CreateDocumentCollectionQuery(_database.SelfLink)
                .AsEnumerable();
            return documentCollections;
        }

        public DocumentCollection GetCollection(string collectionName)
        {
            var documentCollection = _documentClient
                .CreateDocumentCollectionQuery(_database.SelfLink)
                .Where(c => c.Id == collectionName)
                .AsEnumerable()
                .FirstOrDefault();

            if (documentCollection == null)
            {
                documentCollection = new DocumentCollection { Id = collectionName };
                documentCollection.IndexingPolicy.IncludedPaths.Add(new IndexingPath
                {
                    IndexType = IndexType.Hash,
                    Path = "/",
                });

                documentCollection = _documentClient.CreateDocumentCollectionAsync(
                    _database.CollectionsLink, documentCollection).Result;
            }

            return documentCollection;
        }
    }
}
