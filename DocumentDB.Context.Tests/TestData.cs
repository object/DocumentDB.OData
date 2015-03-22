using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Xml.Linq;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;

namespace DocumentDB.Context.Tests
{
    public static class TestData
    {
        public static void PopulateWithCategoriesAndProducts(bool clearDatabase = true)
        {
            var database = GetDatabase(clearDatabase);

            var categories = database.GetCollection("Categories");
            var products = database.GetCollection("Products");

            var categoryFood = new ClientCategory
                                   {
                                       Name = "Food",
                                       Products = null,
                                   };
            var categoryBeverages = new ClientCategory
                                        {
                                            Name = "Beverages",
                                            Products = null,
                                        };
            var categoryElectronics = new ClientCategory
                                          {
                                              Name = "Electronics",
                                              Products = null,
                                          };

            categories.Insert(categoryFood);
            categories.Insert(categoryBeverages);
            categories.Insert(categoryElectronics);

            products.Insert(
                new ClientProduct
                    {
                        ID = 1,
                        Name = "Bread",
                        Description = "Whole grain bread",
                        ReleaseDate = new DateTime(1992, 1, 1),
                        DiscontinueDate = null,
                        Rating = 4,
                        Quantity = new Quantity
                            {
                                Value = (double)12, 
                                Units = "pieces",
                            },
                        Supplier = new Supplier
                            {
                                Name = "City Bakery",
                                Addresses = new[]
                                    {
                                        new Address { Type = AddressType.Postal, Lines = new[] {"P.O.Box 89", "123456 City"} },
                                        new Address { Type = AddressType.Street, Lines = new[] {"Long Street 100", "654321 City"} },
                                    },
                            },
                        Category = categoryFood,
                    });
            products.Insert(
                new ClientProduct
                    {
                        ID = 2,
                        Name = "Milk",
                        Description = "Low fat milk",
                        ReleaseDate = new DateTime(1995, 10, 21),
                        DiscontinueDate = null,
                        Rating = 3,
                        Quantity = new Quantity
                            {
                                Value = (double)4,
                                Units = "liters",
                            },
                        Supplier = new Supplier
                            {
                                Name = "Green Farm",
                                Addresses = new[]
                                    {
                                        new Address { Type = AddressType.Street, Lines = new[] {"P.O.Box 123", "321321 Green Village"} },
                                    },
                            },
                        Category = categoryBeverages,
                    });
            products.Insert(
                new ClientProduct
                    {
                        ID = 3,
                        Name = "Wine",
                        Description = "Red wine, year 2003",
                        ReleaseDate = new DateTime(2003, 11, 24),
                        DiscontinueDate = new DateTime(2008, 3, 1),
                        Rating = 5,
                        Quantity = new Quantity
                            {
                                Value = (double)7,
                                Units = "bottles",
                            },
                        Category = categoryBeverages,
                    });
        }

        public static void PopulateWithClrTypes(bool clearDatabase = true)
        {
            var database = GetDatabase(clearDatabase);

            var clrTypes = database.GetCollection("ClrTypes");
            clrTypes.Insert(
                new ClrType
                {
                    BinaryValue = new[] { (byte)1 },
                    BoolValue = true,
                    NullableBoolValue = true,
                    DateTimeValue = new DateTime(2012, 1, 1),
                    NullableDateTimeValue = new DateTime(2012, 1, 1),
                    TimeSpanValue = new TimeSpan(1, 2, 3),
                    NullableTimeSpanValue = new TimeSpan(1, 2, 3),
                    GuidValue = Guid.Empty,
                    NullableGuidValue = Guid.Empty,
                    ByteValue = (byte)1,
                    NullableByteValue = (byte)1,
                    SByteValue = (sbyte)2,
                    NullableSByteValue = (sbyte)2,
                    Int16Value = 3,
                    NullableInt16Value = 3,
                    UInt16Value = 4,
                    NullableUInt16Value = 4,
                    Int32Value = 5,
                    NullableInt32Value = 5,
                    UInt32Value = 6,
                    NullableUInt32Value = 6,
                    Int64Value = 7,
                    NullableInt64Value = 7,
                    UInt64Value = 8,
                    NullableUInt64Value = 8,
                    SingleValue = 9,
                    NullableSingleValue = 9,
                    DoubleValue = 10,
                    NullableDoubleValue = 10,
                    DecimalValue = 11,
                    NullableDecimalValue = 11,
                    StringValue = "abc",
                    BsonIdValue = new BsonObjectId(new byte[] {1,2,3,4,5,6,7,8,9,10,11,12}),
                });
        }

        public static void PopulateWithVariableTypes(bool clearDatabase = true)
        {
            var database = GetDatabase(clearDatabase);

            var variableTypes = database.GetCollection("VariableTypes");
            variableTypes.Insert(new TypeWithOneField { StringValue = "1" });
            variableTypes.Insert(new TypeWithTwoFields { StringValue = "2", IntValue = 2 });
            variableTypes.Insert(new TypeWithThreeFields { StringValue = "3", IntValue = 3, DecimalValue = 3m });
        }

        public static void PopulateWithDocumentIdTypes(bool clearDatabase = true)
        {
            var database = GetDatabase(clearDatabase);

            var typesWithoutExplicitId = database.GetCollection("TypeWithoutExplicitId");
            typesWithoutExplicitId.Insert(new TypeWithoutExplicitId { Name = "A" });
            typesWithoutExplicitId.Insert(new TypeWithoutExplicitId { Name = "B" });
            typesWithoutExplicitId.Insert(new TypeWithoutExplicitId { Name = "C" });

            var typeWithIntId = database.GetCollection("TypeWithIntId");
            typeWithIntId.Insert(new TypeWithIntId { Id = 1, Name = "A" });
            typeWithIntId.Insert(new TypeWithIntId { Id = 2, Name = "B" });
            typeWithIntId.Insert(new TypeWithIntId { Id = 3, Name = "C" });

            var typeWithStringId = database.GetCollection("TypeWithStringId");
            typeWithStringId.Insert(new TypeWithStringId { Id = "1", Name = "A" });
            typeWithStringId.Insert(new TypeWithStringId { Id = "2", Name = "B" });
            typeWithStringId.Insert(new TypeWithStringId { Id = "3", Name = "C" });

            var typeWithGuidId = database.GetCollection("TypeWithGuidId");
            typeWithGuidId.Insert(new TypeWithGuidId { Id = Guid.NewGuid(), Name = "A" });
            typeWithGuidId.Insert(new TypeWithGuidId { Id = Guid.NewGuid(), Name = "B" });
            typeWithGuidId.Insert(new TypeWithGuidId { Id = Guid.NewGuid(), Name = "C" });
        }

        public static void PopulateWithJsonSamples(bool clearDatabase = true)
        {
            var database = GetDatabase(clearDatabase);

            var jsonSamples = new[]
                {
                    "Colors", 
                    "Facebook", 
                    "Flickr", 
                    //"GoogleMaps", 
                    //"iPhone", 
                    //"Twitter", 
                    //"YouTube", 
                    //"Nested", 
                    //"ArrayOfNested", 
                    //"ArrayInArray", 
                    //"EmptyArray", 
                    //"NullArray",
                    //"UnresolvedArray",
                    //"UnresolvedProperty",
                    //"EmptyProperty",
                };

            foreach (var collectionName in jsonSamples)
            {
                var jsonCollection = GetResourceAsString(collectionName + ".json").Split(new string[] { "---" }, StringSplitOptions.RemoveEmptyEntries);
                foreach (var json in jsonCollection)
                {
                    var doc = JObject.Parse(json);
                    var collection = database.GetCollection(collectionName);
                    collection.Insert(doc);
                }
            }
        }

        public static void Clean()
        {
            GetDatabase(true);
        }

        public static ClientDatabase CreateDatabase()
        {
            return GetDatabase(true);
        }

        public static ClientDatabase OpenDatabase()
        {
            return GetDatabase(false);
        }

        private static ClientDatabase GetDatabase(bool clear)
        {
            var endpointUrl = string.Empty;
            var authorizationKey = string.Empty;
            var databaseName = string.Empty;
            var connectionString = ConfigurationManager.ConnectionStrings["DocumentDB"].ConnectionString;
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

            return new ClientDatabase(endpointUrl, authorizationKey, databaseName, clear);
        }

        private static string GetResourceAsString(string resourceName)
        {
            var assembly = Assembly.GetExecutingAssembly();
            var completeResourceName = assembly.GetManifestResourceNames().Single(o => o.EndsWith("." + resourceName));
            using (var resourceStream = assembly.GetManifestResourceStream(completeResourceName))
            {
                TextReader reader = new StreamReader(resourceStream);
                var result = reader.ReadToEnd();
                return result;
            }
        }
    }
}
