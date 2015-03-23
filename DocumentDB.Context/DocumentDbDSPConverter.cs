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
        public static DSPResource CreateDSPResource(JToken document, DocumentDbMetadata dbMetadata, string resourceName, string ownerPrefix = null)
        {
            var resourceType = dbMetadata.ResolveResourceType(resourceName, ownerPrefix);
            if (resourceType == null)
                throw new ArgumentException(string.Format("Unable to resolve resource type {0}", resourceName), "resourceName");
            var resource = new DSPResource(resourceType);

            foreach (var element in document)
            {
                var resourceProperty = dbMetadata.ResolveResourceProperty(resourceType, element as JProperty);
                if (resourceProperty == null)
                    continue;

                object propertyValue = ConvertJsonValue(element as JProperty, resourceType, resourceProperty, resourceProperty.Name, dbMetadata);
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

        private static object ConvertJsonValue(JProperty element, ResourceType resourceType, ResourceProperty resourceProperty, string propertyName, DocumentDbMetadata dbMetadata)
        {
            if (element == null)
                return null;

            object propertyValue = null;
            bool convertValue;

            if (element.Value.Type == JTokenType.Object)
            {
                var document = element.Value;
                propertyValue = CreateDSPResource(document, dbMetadata, propertyName,
                    DocumentDbMetadata.GetQualifiedTypePrefix(resourceType.Name));
                convertValue = true;
            }
            else if (element.Value.Type == JTokenType.Array)
            {
                var jsonArray = element.Value.ToArray();
                if (jsonArray.Any())
                    propertyValue = ConvertJsonArray(JArray.FromObject(jsonArray), resourceType, propertyName, dbMetadata);
                convertValue = false;
            }
            else if (element.Value.Type == JTokenType.Null && resourceProperty.Kind == ResourcePropertyKind.Collection)
            {
                propertyValue = ConvertJsonArray(new JArray(0), resourceType, propertyName, dbMetadata);
                convertValue = false;
            }
            else
            {
                propertyValue = ConvertRawValue(element.Value as JValue);
                convertValue = true;
            }

            if (propertyValue != null && convertValue)
            {
                var propertyType = resourceProperty.ResourceType.InstanceType;
                var underlyingNonNullableType = Nullable.GetUnderlyingType(resourceProperty.ResourceType.InstanceType);
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
                if (jsonArray[index].Type != JTokenType.Null)
                {
                    if (jsonArray[index].Type == JTokenType.Object)
                        isDocument = true;
                    ++nonNullItemCount;
                }
            }
            object[] propertyValue = isDocument ? new DSPResource[nonNullItemCount] : new object[nonNullItemCount];
            int valueIndex = 0;
            for (int index = 0; index < jsonArray.Count; index++)
            {
                if (jsonArray[index].Type != JTokenType.Null)
                {
                    if (isDocument)
                    {
                        propertyValue[valueIndex++] = CreateDSPResource(jsonArray[index], dbMetadata,
                                                                     propertyName,
                                                                     DocumentDbMetadata.GetQualifiedTypePrefix(resourceType.Name));
                    }
                    else
                    {
                        propertyValue[valueIndex++] = ConvertRawValue(jsonArray[index] as JValue);
                    }
                }
            }
            return propertyValue;
        }

        private static object ConvertRawValue(JValue elementValue)
        {
            if (elementValue == null)
                return null;

            if (DocumentDbMetadata.MapToDotNetType(elementValue) != null)
            {
                return elementValue.Value;
            }
            else
            {
                switch (elementValue.Type)
                {
                    case JTokenType.Null:
                    default:
                        return null;
                }
            }
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
    }
}
