﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Services.Providers;
using System.Linq;
using System.Text;
using DataServiceProvider;
using Microsoft.Azure.Documents;
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
        public static readonly string ProviderObjectIdName = "_id";
        public static readonly string MappedObjectIdName = "db_id";
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

        public ResourceProperty ResolveResourceProperty(ResourceType resourceType, JToken element)
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
            //return context.Database.GetCollectionNames().Where(x => !x.StartsWith("system."));
            return null;
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
                RegisterDocumentProperty(context, resourceSet.ResourceType, element);
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
                    RegisterResourceProperty(context, collectionType, element);
                    if (IsObjectId(element))
                        hasObjectId = true;
                }
            }

            if (!hasObjectId)
            {
                if (resourceTypeKind == ResourceTypeKind.EntityType)
                {
                    this.instanceMetadataCache.AddKeyProperty(collectionType, MappedObjectIdName, MappedObjectIdType);
                }
                //AddProviderType(collectionName, ProviderObjectIdName, BsonObjectId.Empty, true);
            }

            if (resourceTypeKind == ResourceTypeKind.EntityType)
                this.instanceMetadataCache.AddResourceSet(collectionName, collectionType);

            return collectionType;
        }

        internal void RegisterResourceProperty(DocumentDbContext context, ResourceType resourceType, JToken element)
        {
            var collectionProperty = new CollectionProperty { CollectionType = resourceType, PropertyName = (element as JProperty).Name };
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
            JToken element, bool treatObjectIdAsKey = false)
        {
            var elementType = GetElementType(element, treatObjectIdAsKey);
            var propertyName = GetResourcePropertyName(element, collectionType.ResourceTypeKind);
            var propertyValue = element;

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
            else if (elementType == typeof(Document))
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
                AddProviderType(collectionName, IsObjectId(element) ? ProviderObjectIdName : propertyName, propertyValue, isKey);
            }
        }

        private void RegisterDocumentProperties(DocumentDbContext context, ResourceType collectionType, JToken element)
        {
            var resourceName = GetResourcePropertyName(element, ResourceTypeKind.EntityType);
            var resourceType = ResolveResourceType(resourceName, collectionType.Name);
            if (resourceType == null)
            {
                AddDocumentProperty(context, collectionType.Name, collectionType, resourceName, element, true);
            }
            else
            {
                foreach (var documentElement in element)
                {
                    RegisterDocumentProperty(context, resourceType, documentElement);
                }
            }
        }

        private void RegisterDocumentProperty(DocumentDbContext context, ResourceType resourceType, JToken element)
        {
            var resourceProperty = ResolveResourceProperty(resourceType, element);
            if (resourceProperty == null)
            {
                RegisterResourceProperty(context, resourceType, element);
            }
            else if ((resourceProperty.Kind & ResourcePropertyKind.ComplexType) != 0 && element != null) // BsonNull
            {
                RegisterDocumentProperties(context, resourceType, element);
            }
            else if ((resourceProperty.Kind & ResourcePropertyKind.Collection) != 0 && element != null) // BsonNull
            {
                RegisterArrayProperty(context, resourceType, element);
            }
        }

        private void AddDocumentProperty(DocumentDbContext context, string collectionName, ResourceType collectionType, string propertyName, JToken element, bool isCollection = false)
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
                                                    element, ResourceTypeKind.ComplexType);
            }
            if (isCollection && ResolveResourceProperty(collectionType, propertyName) == null)
                this.instanceMetadataCache.AddCollectionProperty(collectionType, propertyName, resourceType);
            else
                this.instanceMetadataCache.AddComplexProperty(collectionType, propertyName, resourceType);
        }

        private void RegisterArrayProperty(DocumentDbContext context, ResourceType collectionType, JToken element)
        {
            var propertyName = GetResourcePropertyName(element, ResourceTypeKind.EntityType);
            var jsonArray = element.ToArray();
            if (jsonArray != null)
            {
                foreach (var arrayValue in jsonArray)
                {
                    if (arrayValue == null) // TODO: BsonNull
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
                            //var mappedType = BsonTypeMapper.MapToDotNetValue(arrayValue).GetType();
                            //this.instanceMetadataCache.AddCollectionProperty(collectionType, propertyName, mappedType);
                        }
                    }
                }
            }
        }

        private void AddProviderType(string collectionName, string elementName, JToken elementValue, bool isKey = false)
        {
            Type providerType = ResolveProviderType(elementValue, isKey);
            var qualifiedName = GetQualifiedPropertyName(collectionName, elementName);
            if (providerType != null && !this.instanceMetadataCache.ProviderTypes.ContainsKey(qualifiedName))
            {
                this.instanceMetadataCache.ProviderTypes.Add(qualifiedName, providerType);
            }
        }

        private static Type ResolveProviderType(JToken elementValue, bool isKey)
        {
            if (elementValue.GetType() == typeof(JArray) || elementValue.GetType() == typeof(Document))
            {
                return elementValue.GetType();
            }
            //else if (BsonTypeMapper.MapToDotNetValue(elementValue) != null)
            //{
            //    return GetRawValueType(elementValue, isKey);
            //}
            else
            {
                return null;
            }
        }

        private static bool IsObjectId(JToken element)
        {
            return (element as JProperty).Name == DocumentDbMetadata.ProviderObjectIdName;
        }

        private static Type GetElementType(JToken element, bool treatObjectIdAsKey)
        {
            if (IsObjectId(element))
            {
                if (element.GetType() == ProviderObjectIdType && treatObjectIdAsKey)
                    return MappedObjectIdType;
                else
                    return GetRawValueType(element, treatObjectIdAsKey);
            }
            else if (element.GetType() == typeof (BsonObjectId))
            {
                return MappedObjectIdType;
            }
            else if (element.GetType() == typeof(JArray) || element.GetType() == typeof(Document))
            {
                return element.GetType();
            }
            //else if (BsonTypeMapper.MapToDotNetValue(element.Value) != null)
            //{
            //    return GetRawValueType(element);
            //}
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
            Type elementType;
            switch (elementValue.Type)
            {
                case JTokenType.Date:
                    elementType = typeof (DateTime);
                    break;
                default:
                    // TODO
                    elementType = typeof (object);
                    //elementType = BsonTypeMapper.MapToDotNetValue(elementValue).GetType();
                    break;
            }

            if (!isKey && elementType.IsValueType)
            {
                elementType = typeof(Nullable<>).MakeGenericType(elementType);
            }
            return elementType;
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

        private static string GetResourcePropertyName(JToken element, ResourceTypeKind resourceTypeKind)
        {
            return IsObjectId(element) && resourceTypeKind != ResourceTypeKind.ComplexType ?
                DocumentDbMetadata.MappedObjectIdName :
                NormalizeResourcePropertyName((element as JProperty).Name);
        }

        private static string NormalizeResourcePropertyName(string propertyName)
        {
            propertyName = propertyName.Trim();
            return propertyName.StartsWith("_") ? PrefixForInvalidLeadingChar + propertyName : propertyName;
        }
    }
}
