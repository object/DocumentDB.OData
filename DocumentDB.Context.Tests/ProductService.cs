using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Services;
using System.Data.Services.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using DataServiceProvider;
using DocumentDB.Context.InMemory;
using DocumentDB.Context.Queryable;

namespace DocumentDB.Context.Tests
{
    public class ProductInMemoryService : DocumentDbInMemoryDataService
    {
        public ProductInMemoryService() 
            : base (ConfigurationManager.ConnectionStrings["MongoDB"].ConnectionString, TestService.Configuration)
        {
        }

        public static void InitializeService(DataServiceConfiguration config)
        {
            config.SetEntitySetAccessRule("*", EntitySetRights.All);
            config.DataServiceBehavior.MaxProtocolVersion = System.Data.Services.Common.DataServiceProtocolVersion.V3;
            config.DataServiceBehavior.AcceptCountRequests = true;
            config.DataServiceBehavior.AcceptProjectionRequests = true;
            config.UseVerboseErrors = true;
        }
    }

    public class ProductQueryableService : DocumentDbQueryableDataService
    {
        public ProductQueryableService()
            : base(ConfigurationManager.ConnectionStrings["MongoDB"].ConnectionString, TestService.Configuration)
        {
        }

        public static void InitializeService(DataServiceConfiguration config)
        {
            config.SetEntitySetAccessRule("*", EntitySetRights.All);
            config.DataServiceBehavior.MaxProtocolVersion = DataServiceProtocolVersion.V3;
            config.DataServiceBehavior.AcceptCountRequests = true;
            config.DataServiceBehavior.AcceptProjectionRequests = true;
            config.UseVerboseErrors = true;
        }
    }

    public class ProductQueryableServiceWithQueryInterceptor : ProductQueryableService
    {
        public static Expression<Func<DSPResource, bool>> ProductQueryInterceptor = null;

        [QueryInterceptor("Products")]
        public Expression<Func<DSPResource, bool>> OnQueryProducts()
        {
            return ProductQueryInterceptor ?? (x => true);
        }
    }
}
