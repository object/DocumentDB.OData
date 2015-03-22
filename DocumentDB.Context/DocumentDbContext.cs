using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Azure.Documents.Client;

namespace DocumentDB.Context
{
    public partial class DocumentDbContext : IDisposable
    {
        protected string _connectionString;
        protected string _databaseName;
        protected DocumentClient _documentClient;
        protected DocumentDatabase _database;

        public DocumentDbContext(string connectionString)
        {
            _connectionString = connectionString;
            _documentClient = GetClient(connectionString);
            _database = new DocumentDatabase(_documentClient, _databaseName);
        }

        public DocumentClient Client
        {
            get { return _documentClient; }
        }

        public DocumentDatabase Database
        {
            get { return _database; }
        }

        public void Dispose()
        {
            // TODO
        }

        public void SaveChanges()
        {
        }

        private DocumentClient GetClient(string connectionString)
        {
            var endpointUrl = string.Empty;
            var authorizationKey = string.Empty;
            var databaseName = string.Empty;
            foreach (var item in connectionString.Split(';'))
            {
                var key = item.Substring(0, item.IndexOf('='));
                var value = item.Substring(key.Length + 1);
                switch (key)
                {
                    case "EndpointUrl":
                        endpointUrl = value;
                        break;
                    case "AuthorizationKey":
                        authorizationKey = value;
                        break;
                    case "Database":
                        databaseName = value;
                        break;
                }
            }

            _databaseName = databaseName;
            return new DocumentClient(new Uri(endpointUrl), authorizationKey);
        }
    }
}
