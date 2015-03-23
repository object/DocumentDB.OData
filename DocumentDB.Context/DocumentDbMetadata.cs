using System;
using System.Collections.Generic;
using System.Data.Services.Providers;
using System.Linq;
using System.Text;
using DataServiceProvider;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;

namespace DocumentDB.Context
{
    class CollectionProperty
    {
        public ResourceType CollectionType { get; set; }
        public string PropertyName { get; set; }

        public override bool Equals(object obj)
        {
            if (obj is CollectionProperty)
            {
                var prop = obj as CollectionProperty;
                return this.CollectionType == prop.CollectionType && this.PropertyName == prop.PropertyName;
            }
            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return this.CollectionType.GetHashCode() ^ this.PropertyName.GetHashCode();
        }
    }

    public class DocumentDbMetadata
    {
        public static readonly string ProviderObjectIdName = "id";
        public static readonly string MappedObjectIdName = "id";
        public static readonly Type ProviderObjectIdType = typeof(BsonObjectId);
        public static readonly Type MappedObjectIdType = typeof(string);
        public static readonly string ContainerName = "DocumentDbContext";
        public static readonly string RootNamespace = "DocumentDB";
        public static readonly bool UseGlobalComplexTypeNames = false;
        internal static bool CreateDynamicTypesForComplexTypes = true;
        internal static readonly string WordSeparator = "__";
        internal static readonly string PrefixForInvalidLeadingChar = "x";

        private readonly string connectionString;
        private readonly List<CollectionProperty> unresolvedProperties = new List<CollectionProperty>();
        private static readonly Dictionary<string, DocumentDbMetadataCache> MetadataCache = new Dictionary<string, DocumentDbMetadataCache>();
        private readonly DocumentDbMetadataCache instanceMetadataCache;

        public DocumentDbConfiguration.Metadata Configuration { get; private set; }
        internal Dictionary<string, Type> ProviderTypes { get { return this.instanceMetadataCache.ProviderTypes; } }
        internal Dictionary<string, Type> GeneratedTypes { get { return this.instanceMetadataCache.GeneratedTypes; } }

        public DocumentDbMetadata(string connectionString, DocumentDbConfiguration.Metadata metadata = null)
        {
            this.connectionString = connectionString;
            this.Configuration = metadata ?? DocumentDbConfiguration.Metadata.Default;

            lock (MetadataCache)
            {
                this.instanceMetadataCache = GetOrCreateMetadataCache();
            }

            using (var context = new DocumentDbContext(connectionString))
            {
                PopulateMetadata(context);
            }
        }

        public DSPMetadata CreateDSPMetadata()
        {
            return this.instanceMetadataCache.CloneDSPMetadata();
        }

        public static void ResetDSPMetadata()
        {
            MetadataCache.Clear();
        }

        private DocumentDbMetadataCache GetOrCreateMetadataCache()
        {
            DocumentDbMetadataCache metadataCache;
            MetadataCache.TryGetValue(this.connectionString, out metadataCache);
            if (metadataCache == null)
            {
                metadataCache = new DocumentDbMetadataCache(ContainerName, RootNamespace);
                MetadataCache.Add(this.connectionString, metadataCache);
            }
            return metadataCache;
        }

        public ResourceType ResolveResourceType(string resourceName, string ownerPrefix = null)
        {
            ResourceType resourceType;
            var qualifiedResourceName = string.IsNullOrEmpty(ownerPrefix) ? resourceName : DocumentDbMetadata.GetQualifiedTypeName(ownerPrefix, resourceName);
            this.instanceMetadataCache.TryResolveResourceType(GetQualifiedPropertyName(DocumentDbMetadata.RootNamespace, qualifiedResourceName), out resourceType);
            return resourceType;
        }

        public ResourceProperty ResolveResourceProperty(ResourceType resourceType, JProperty element)
        {
            var propertyName = DocumentDbMetadata.GetResourcePropertyName(element, resourceType.ResourceTypeKind);
            return ResolveResourceProperty(resourceType, propertyName);
        }

        public ResourceProperty ResolveResourceProperty(ResourceType resourceType, string propertyName)
        {
            return resourceType.Properties.SingleOrDefault(x => x.Name == propertyName);
        }

        private IEnumerable<string> GetCollectionNames(DocumentDbContext context)
        {
            return context.Database.GetCollections()
                .Select(x => x.Id)
                .Where(x => !x.StartsWith("system."));
        }

        private void PopulateMetadata(DocumentDbContext context)
        {
            lock (this.instanceMetadataCache)
            {
                foreach (var collectionName in GetCollectionNames(context))
                {
                    var resourceSet = this.instanceMetadataCache.ResolveResourceSet(collectionName);

                    if (this.Configuration.PrefetchRows == 0)
                    {
                        if (resourceSet == null)
                        {
                            AddResourceSet(context, collectionName);
                        }
                    }
                    else
                    {
                        PopulateMetadataFromCollection(context, collectionName, resourceSet);
                    }
                }

                foreach (var prop in this.unresolvedProperties)
                {
                    var providerType = typeof (string);
                    var propertyName = NormalizeResourcePropertyName(prop.PropertyName);
                    this.instanceMetadataCache.AddPrimitiveProperty(prop.CollectionType, propertyName, providerType);
                    this.instanceMetadataCache.ProviderTypes.Add(
                        GetQualifiedPropertyName(prop.CollectionType.Name, propertyName), providerType);
                }
            }
        }

        private void PopulateMetadataFromCollection(DocumentDbContext context, string collectionName, ResourceSet resourceSet)
        {
            var collection = context.Database.GetCollection(collectionName);
            //const string naturalSort = "$natural";
            //var sortOrder = this.Configuration.FetchPosition == DocumentDbConfiguration.FetchPosition.End
            //                    ? SortBy.Descending(naturalSort)
            //                    : SortBy.Ascending(naturalSort);
            //var documents = collection.FindAll().SetSortOrder(sortOrder);

            var documents = context.Client.CreateDocumentQuery<JObject>(collection.DocumentsLink);

            int rowCount = 0;
            foreach (var document in documents)
            {
                if (resourceSet == null)
                {
                    resourceSet = AddResourceSet(context, collectionName, document);
                }
                else
                {
                    UpdateResourceSet(context, resourceSet, document);
                }

                ++rowCount;
                if (this.Configuration.PrefetchRows >= 0 && rowCount >= this.Configuration.PrefetchRows)
                    break;
            }
        }

        public ResourceSet ResolveResourceSet(string resourceName)
        {
            return this.instanceMetadataCache.ResolveResourceSet(resourceName);
        }

        private ResourceSet AddResourceSet(DocumentDbContext context, string collectionName, JToken document = null)
        {
            AddDocumentType(context, collectionName, document, ResourceTypeKind.EntityType);
            return this.instanceMetadataCache.ResolveResourceSet(collectionName);
        }

        private void UpdateResourceSet(DocumentDbContext context, ResourceSet resourceSet, JToken document)
        {
            foreach (var element in document)
            {
                RegisterDocumentProperty(context, resourceSet.ResourceType, element as JProperty);
            }
        }

        private ResourceType AddDocumentType(DocumentDbContext context, string collectionName, JToken document, ResourceTypeKind resourceTypeKind)
        {
            var collectionType = resourceTypeKind == ResourceTypeKind.EntityType
                                     ? this.instanceMetadataCache.AddEntityType(collectionName)
                                     : this.instanceMetadataCache.AddComplexType(collectionName);

            bool hasObjectId = false;
            if (document != null)
            {
                foreach (var element in document)
                {
                    var property = element as JProperty;
                    RegisterResourceProperty(context, collectionType, property);
                    if (IsObjectId(property))
                        hasObjectId = true;
                }
            }

            if (!hasObjectId)
            {
                if (resourceTypeKind == ResourceTypeKind.EntityType)
                {
                    this.instanceMetadataCache.AddKeyProperty(collectionType, MappedObjectIdName, MappedObjectIdType);
                }
                AddProviderType(collectionName, ProviderObjectIdName, new JProperty(ProviderObjectIdName, string.Empty), true); // TODO
            }

            if (resourceTypeKind == ResourceTypeKind.EntityType)
                this.instanceMetadataCache.AddResourceSet(collectionName, collectionType);

            return collectionType;
        }

        internal void RegisterResourceProperty(DocumentDbContext context, ResourceType resourceType, JProperty element)
        {
            var collectionProperty = new CollectionProperty { CollectionType = resourceType, PropertyName = element.Name };
            var resourceProperty = ResolveResourceProperty(resourceType, element);
            if (resourceProperty == null)
            {
                lock (unresolvedProperties)
                {
                    var unresolvedEarlier = unresolvedProperties.Contains(collectionProperty);
                    var resolvedNow = ResolveProviderType(element, IsObjectId(element)) != null;

                    if (!unresolvedEarlier && !resolvedNow)
                        this.unresolvedProperties.Add(collectionProperty);
                    else if (unresolvedEarlier && resolvedNow)
                        this.unresolvedProperties.Remove(collectionProperty);

                    if (resolvedNow)
                    {
                        AddResourceProperty(context, resourceType.Name, resourceType, element, resourceType.ResourceTypeKind == ResourceTypeKind.EntityType);
                    }
                }
            }
        }

        private void AddResourceProperty(DocumentDbContext context, string collectionName, ResourceType collectionType,
            JProperty element, bool treatObjectIdAsKey = false)
        {
            var elementType = GetElementType(element, treatObjectIdAsKey);
            var propertyName = GetResourcePropertyName(element, collectionType.ResourceTypeKind);
            var propertyValue = element.Value;

            if (string.IsNullOrEmpty(propertyName))
                return;

            var isKey = false;
            if (IsObjectId(element))
            {
                if (treatObjectIdAsKey)
                    this.instanceMetadataCache.AddKeyProperty(collectionType, propertyName, elementType);
                else
                    this.instanceMetadataCache.AddPrimitiveProperty(collectionType, propertyName, elementType);
                isKey = true;
            }
            else if (elementType == typeof(JObject) || elementType == typeof(JProperty))
            {
                AddDocumentProperty(context, collectionName, collectionType, propertyName, element);
            }
            else if (elementType == typeof(JArray))
            {
                RegisterArrayProperty(context, collectionType, element);
            }
            else
            {
                this.instanceMetadataCache.AddPrimitiveProperty(collectionType, propertyName, elementType);
            }

            if (!string.IsNullOrEmpty(propertyName))
            {
                AddProviderType(collectionName, IsObjectId(element) ? ProviderObjectIdName : propertyName, element, isKey);
            }
        }

        private void RegisterDocumentProperties(DocumentDbContext context, ResourceType collectionType, JProperty element)
        {
            var resourceName = GetResourcePropertyName(element, ResourceTypeKind.EntityType);
            var resourceType = ResolveResourceType(resourceName, collectionType.Name);
            if (resourceType == null)
            {
                AddDocumentProperty(context, collectionType.Name, collectionType, resourceName, element, true);
            }
            else
            {
                foreach (var documentElement in element.Value)
                {
                    RegisterDocumentProperty(context, resourceType, documentElement as JProperty);
                }
            }
        }

        private void RegisterDocumentProperty(DocumentDbContext context, ResourceType resourceType, JProperty element)
        {
            var resourceProperty = ResolveResourceProperty(resourceType, element);
            if (resourceProperty == null)
            {
                RegisterResourceProperty(context, resourceType, element);
            }
            else if ((resourceProperty.Kind & ResourcePropertyKind.ComplexType) != 0 && element.Value.Type != JTokenType.Null)
            {
                RegisterDocumentProperties(context, resourceType, element);
            }
            else if ((resourceProperty.Kind & ResourcePropertyKind.Collection) != 0 && element.Value.Type != JTokenType.Null)
            {
                RegisterArrayProperty(context, resourceType, element);
            }
        }

        private void AddDocumentProperty(DocumentDbContext context, string collectionName, ResourceType collectionType, string propertyName, JProperty element, bool isCollection = false)
        {
            ResourceType resourceType = null;
            var resourceSet = this.instanceMetadataCache.ResolveResourceSet(collectionName);
            if (resourceSet != null)
            {
                resourceType = resourceSet.ResourceType;
            }
            else
            {
                resourceType = AddDocumentType(context, GetQualifiedTypeName(collectionName, propertyName),
                                                element.Value, ResourceTypeKind.ComplexType);
            }
            if (isCollection && ResolveResourceProperty(collectionType, propertyName) == null)
                this.instanceMetadataCache.AddCollectionProperty(collectionType, propertyName, resourceType);
            else
                this.instanceMetadataCache.AddComplexProperty(collectionType, propertyName, resourceType);
        }

        private void RegisterArrayProperty(DocumentDbContext context, ResourceType collectionType, JProperty element)
        {
            var propertyName = GetResourcePropertyName(element, ResourceTypeKind.EntityType);
            foreach (var arrayValue in element.Value)
            {
                if (arrayValue.Type == JTokenType.Null)
                    continue;

                if (arrayValue.Type == JTokenType.Object)
                {
                    RegisterDocumentProperties(context, collectionType, new JProperty((element as JProperty).Name, arrayValue));
                }
                else if (ResolveResourceProperty(collectionType, propertyName) == null)
                {
                    // OData protocol doesn't support collections of collections
                    if (arrayValue.Type != JTokenType.Array)
                    {
                        var mappedType = MapToDotNetType(arrayValue);
                        this.instanceMetadataCache.AddCollectionProperty(collectionType, propertyName, mappedType);
                    }
                }
            }
        }

        private void AddProviderType(string collectionName, string elementName, JProperty element, bool isKey = false)
        {
            var providerType = ResolveProviderType(element, isKey);
            var qualifiedName = GetQualifiedPropertyName(collectionName, elementName);
            if (providerType != null && !this.instanceMetadataCache.ProviderTypes.ContainsKey(qualifiedName))
            {
                this.instanceMetadataCache.ProviderTypes.Add(qualifiedName, providerType);
            }
        }

        private static Type ResolveProviderType(JProperty element, bool isKey)
        {
            if (element.Value.GetType() == typeof(JArray) || element.Value.GetType() == typeof(JObject))
            {
                return element.Value.GetType();
            }
            else if (MapToDotNetType(element.Value) != null)
            {
                return GetRawValueType(element.Value, isKey);
            }
            else
            {
                return null;
            }
        }

        private static bool IsObjectId(JProperty element)
        {
            return string.Compare(element.Name, DocumentDbMetadata.ProviderObjectIdName, StringComparison.InvariantCultureIgnoreCase) == 0;
        }

        private static Type GetElementType(JProperty element, bool treatObjectIdAsKey)
        {
            if (IsObjectId(element))
            {
                if (treatObjectIdAsKey)
                    return MappedObjectIdType;
                else
                    return GetRawValueType(element.Value, treatObjectIdAsKey);
            }
            else if (element.Value.Type == JTokenType.Array || element.Value.Type == JTokenType.Object)
            {
                return element.Value.GetType();
            }
            else if (MapToDotNetType(element.Value) != null)
            {
                return GetRawValueType(element.Value);
            }
            else
            {
                switch (element.Type)
                {
                    //case BsonType.Null:
                    //    return typeof(object);
                    //case BsonType.Binary:
                    //    return typeof(byte[]);
                    default:
                        return typeof(string);
                }
            }
        }

        private static Type GetRawValueType(JToken elementValue, bool isKey = false)
        {
            var elementType = MapToDotNetType(elementValue);
            if (!isKey && elementType.IsValueType)
            {
                elementType = typeof(Nullable<>).MakeGenericType(elementType);
            }
            return elementType;
        }

        internal static Type MapToDotNetType(JToken value)
        {
            switch (value.Type)
            {
                case JTokenType.String:
                    return typeof(string);
                case JTokenType.Boolean:
                    return typeof(bool);
                case JTokenType.Bytes:
                    return typeof(byte[]);
                case JTokenType.Date:
                    return typeof(DateTime);
                case JTokenType.Float:
                    return typeof(double);
                case JTokenType.Guid:
                    return typeof(Guid);
                case JTokenType.Integer:
                    return typeof(int);
                case JTokenType.TimeSpan:
                    return typeof(TimeSpan);
                default:
                    return null;
            }
        }

        internal static string GetQualifiedTypeName(string collectionName, string resourceName)
        {
            return UseGlobalComplexTypeNames ? resourceName : string.Join(WordSeparator, collectionName, resourceName);
        }

        internal static string GetQualifiedTypePrefix(string ownerName)
        {
            return UseGlobalComplexTypeNames ? string.Empty : ownerName;
        }

        internal static string GetQualifiedPropertyName(string typeName, string propertyName)
        {
            return string.Join(".", typeName, propertyName);
        }

        private static string GetResourcePropertyName(JProperty element, ResourceTypeKind resourceTypeKind)
        {
            return IsObjectId(element) && resourceTypeKind != ResourceTypeKind.ComplexType ?
                DocumentDbMetadata.MappedObjectIdName :
                NormalizeResourcePropertyName(element.Name);
        }

        private static string NormalizeResourcePropertyName(string propertyName)
        {
            propertyName = propertyName.Trim();
            return propertyName.StartsWith("_") ? PrefixForInvalidLeadingChar + propertyName : propertyName;
        }
    }
}
