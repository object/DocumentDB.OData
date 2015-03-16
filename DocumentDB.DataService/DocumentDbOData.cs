using System.Configuration;
using System.Data.Services;
using System.Data.Services.Common;
using DocumentDB.Context;
using DocumentDB.Context.Queryable;

namespace DocumentDB.DataService
{
    public class DocumentDbOData : DocumentDbQueryableDataService
    {
        public DocumentDbOData()
            : base(Utils.BuildConnectionString(), ConfigurationManager.GetSection(DocumentDbConfiguration.SectionName) as DocumentDbConfiguration)
        {
        }

        public static void InitializeService(DataServiceConfiguration config)
        {
            config.SetEntitySetAccessRule("*", EntitySetRights.AllRead);
            config.SetServiceOperationAccessRule("*", ServiceOperationRights.All);
            config.DataServiceBehavior.MaxProtocolVersion = DataServiceProtocolVersion.V3;
            config.DataServiceBehavior.AcceptCountRequests = true;
            config.DataServiceBehavior.AcceptProjectionRequests = true;
            config.UseVerboseErrors = true;
        }
    }
}
