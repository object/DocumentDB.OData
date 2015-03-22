using System;
using System.Collections.Generic;
using System.Data.Services.Providers;
using System.Linq;
using System.Text;
using DataServiceProvider;
using Microsoft.Azure.Documents;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace DocumentDB.Context
{
    internal static class DocumentDbDSPConverter
    {
        private static DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static DSPResource CreateDSPResource(JToken document, DocumentDbMetadata dbMetadata, string resourceName, string ownerPrefix = null)
        {
            var resourceType = dbMetadata.ResolveResourceType(resourceName, ownerPrefix);
            if (resourceType == null)
                throw new ArgumentException(string.Format("Unable to resolve resource type {0}", resourceName), "resourceName");
            var resource = new DSPResource(resourceType);

            foreach (var element in document)
            {
                var resourceProperty = dbMetadata.ResolveResourceProperty(resourceType, element);
                if (resourceProperty == null)
                    continue;

                object propertyValue = ConvertJsonValue(element, resourceType, resourceProperty, resourceProperty.Name, dbMetadata);
                resource.SetValue(resourceProperty.Name, propertyValue);
            }
            AssignNullCollections(resource, resourceType);

            return resource;
        }

        public static JObject CreateJsonDocument(DSPResource resource, DocumentDbMetadata dbMetadata, string resourceName)
        {
            var document = new JObject();
            var resourceSet = dbMetadata.ResolveResourceSet(resourceName);
            if (resourceSet != null)
            {
                foreach (var property in resourceSet.ResourceType.Properties)
                {
                    var propertyValue = resource.GetValue(property.Name);
                    if (propertyValue != null)
                    {
                        var text = JsonConvert.SerializeObject(propertyValue);
                        document.Add(property.Name, JObject.Parse(text));
                    }
                }
            }
            return document;
        }

        private static object ConvertJsonValue(JToken jsonValue, ResourceType resourceType, ResourceProperty resourceProperty, string propertyName, DocumentDbMetadata dbMetadata)
        {
            if (jsonValue == null)
                return null;

            object propertyValue = null;
            bool convertValue;

            if (jsonValue.GetType() == typeof(Document))
            {
                var document = jsonValue;
                //if (IsCsharpNullDocument(document))
                //{
                //    convertValue = false;
                //}
                //else
                //{
                    propertyValue = CreateDSPResource(document, dbMetadata, propertyName,
                        DocumentDbMetadata.GetQualifiedTypePrefix(resourceType.Name));
                    convertValue = true;
                //}
            }
            else if (jsonValue.GetType() == typeof(JArray))
            {
                var jsonArray = jsonValue.ToArray();
                if (jsonArray != null && jsonArray.Any())
                    propertyValue = ConvertJsonArray(JArray.FromObject(jsonArray), resourceType, propertyName, dbMetadata);
                convertValue = false;
            }
            else if (jsonValue == null && resourceProperty.Kind == ResourcePropertyKind.Collection) // BsonNull
            {
                propertyValue = ConvertJsonArray(new JArray(0), resourceType, propertyName, dbMetadata);
                convertValue = false;
            }
            else
            {
                propertyValue = ConvertRawValue(jsonValue);
                convertValue = true;
            }

            if (propertyValue != null && convertValue)
            {
                var propertyType = resourceProperty.ResourceType.InstanceType;
                Type underlyingNonNullableType = Nullable.GetUnderlyingType(resourceProperty.ResourceType.InstanceType);
                if (underlyingNonNullableType != null)
                {
                    propertyType = underlyingNonNullableType;
                }
                propertyValue = Convert.ChangeType(propertyValue, propertyType);
            }

            return propertyValue;
        }

        private static object ConvertJsonArray(JArray jsonArray, ResourceType resourceType, string propertyName, DocumentDbMetadata dbMetadata)
        {
            if (jsonArray == null || jsonArray.Count == 0)
            {
                return new object[0];
            }

            bool isDocument = false;
            int nonNullItemCount = 0;
            for (int index = 0; index < jsonArray.Count; index++)
            {
                if (jsonArray[index] != null) // TODO BsonNull
                {
                    if (jsonArray[index].GetType() == typeof(Document))
                        isDocument = true;
                    ++nonNullItemCount;
                }
            }
            object[] propertyValue = isDocument ? new DSPResource[nonNullItemCount] : new object[nonNullItemCount];
            int valueIndex = 0;
            for (int index = 0; index < jsonArray.Count; index++)
            {
                if (jsonArray[index] != null) // TODO BsonNull
                {
                    if (isDocument)
                    {
                        propertyValue[valueIndex++] = CreateDSPResource(jsonArray[index], dbMetadata,
                                                                     propertyName,
                                                                     DocumentDbMetadata.GetQualifiedTypePrefix(resourceType.Name));
                    }
                    else
                    {
                        propertyValue[valueIndex++] = ConvertRawValue(jsonArray[index]);
                    }
                }
            }
            return propertyValue;
        }

        private static object ConvertRawValue(JToken jsonValue)
        {
            if (jsonValue == null)
                return null;

            return null;
            //if (BsonTypeMapper.MapToDotNetValue(jsonValue) != null)
            //{
            //    if (jsonValue.IsObjectId)
            //    {
            //        return jsonValue.ToString();
            //    }
            //    else if (jsonValue.IsGuid)
            //    {
            //        return jsonValue.AsGuid;
            //    }
            //    else
            //    {
            //        //switch (jsonValue.BsonType)
            //        //{
            //        //    case BsonType.DateTime:
            //        //        return UnixEpoch + TimeSpan.FromMilliseconds(jsonValue.AsBsonDateTime.MillisecondsSinceEpoch);
            //        //    default:
            //        //        return BsonTypeMapper.MapToDotNetValue(jsonValue);
            //        //}
            //    }
            //}
            //else
            //{
            //    //switch (jsonValue.BsonType)
            //    //{
            //    //    case BsonType.Binary:
            //    //        return jsonValue.AsBsonBinaryData.Bytes;
            //    //    default:
            //    //        return BsonTypeMapper.MapToDotNetValue(jsonValue);
            //    //}
            //}
        }

        private static void AssignNullCollections(DSPResource resource, ResourceType resourceType)
        {
            foreach (var resourceProperty in resourceType.Properties)
            {
                var propertyValue = resource.GetValue(resourceProperty.Name);
                if (resourceProperty.Kind == ResourcePropertyKind.Collection)
                {
                    if (propertyValue == null)
                    {
                        resource.SetValue(resourceProperty.Name, new object[0]);
                    }
                }
                else if (propertyValue is DSPResource)
                {
                    AssignNullCollections(propertyValue as DSPResource, resourceProperty.ResourceType);
                }
            }
        }

        //private static bool IsCsharpNullDocument(JToken document)
        //{
        //    if (document.Count() == 1)
        //    {
        //        var element = document.First();
        //        return element.Name == "_csharpnull" && element.Value<bool>();
        //    }
        //    return false;
        //}
    }
}
