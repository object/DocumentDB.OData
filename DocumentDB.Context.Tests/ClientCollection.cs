using System.Linq;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace DocumentDB.Context.Tests
{
    public class ClientCollection
    {
        private readonly DocumentClient _documentClient;
        private readonly DocumentCollection _documentCollection;

        public ClientCollection(DocumentClient documentClient, DocumentCollection documentCollection)
        {
            _documentClient = documentClient;
            _documentCollection = documentCollection;
        }

        public void Insert(object document)
        {
            _documentClient.CreateDocumentAsync(_documentCollection.DocumentsLink, document);
        }

        public IOrderedQueryable<Document> AsQueryable()
        {
            return _documentClient.CreateDocumentQuery(_documentCollection.DocumentsLink);
        }

        public IOrderedQueryable<T> AsQueryable<T>()
        {
            return _documentClient.CreateDocumentQuery<T>(_documentCollection.DocumentsLink);
        }
    }
}