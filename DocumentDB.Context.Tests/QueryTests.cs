using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Simple.OData.Client;

namespace DocumentDB.Context.Tests
{
    public abstract class QueryTests<T> : TestBase<T>
    {
        protected override void PopulateTestData()
        {
            TestData.PopulateWithCategoriesAndProducts();
        }

        [SetUp]
        public override void SetUp()
        {
            TestService.Configuration = new DocumentDbConfiguration { MetadataBuildStrategy = new DocumentDbConfiguration.Metadata { PrefetchRows = -1, UpdateDynamically = false } };
            base.SetUp();
        }

        [Test]
        public void ValidateMetadata()
        {
            base.RequestAndValidateMetadata();
        }

        [Test]
        public void SchemaTables()
        {
            var schema = base.GetSchema();
            var tableNames = schema.Tables.Select(x => x.ActualName).ToList();
            Assert.Contains("Products", tableNames);
            Assert.Contains("Categories", tableNames);
        }

        [Test]
        public void SchemaColumnNullability()
        {
            var schema = base.GetSchema();
            base.ValidateColumnNullability(schema);
        }

        [Test]
        public void SchemaColumnNames()
        {
            var schema = base.GetSchema();
            base.ValidatePropertyNames(schema);
        }

        [Test]
        public void AllEntitiesVerifyResultCount()
        {
            var result = ctx.Products.All().ToList();
            Assert.AreEqual(3, result.Count, "The service returned unexpected number of results.");
        }

        [Test]
        public void AllEntitiesTakeOneVerifyResultCount()
        {
            if (this.GetType() == typeof(QueryableServiceQueryTests))
                Assert.Ignore("Take is not supported");

            var result = ctx.Products.All().Take(1).ToList();
            Assert.AreEqual(1, result.Count, "The service returned unexpected number of results.");
        }

        [Test]
        public void AllEntitiesSkipOneVerifyResultCount()
        {
            if (this.GetType() == typeof(QueryableServiceQueryTests))
                Assert.Ignore("Skip is not supported");

            var result = ctx.Products.All().Skip(1).ToList();
            Assert.AreEqual(2, result.Count, "The service returned unexpected number of results.");
        }

        [Test]
        public void AllEntitiesCountVerifyResult()
        {
            if (this.GetType() == typeof(QueryableServiceQueryTests))
                Assert.Ignore("Count is not supported");

            var result = ctx.Products.All().Count();
            Assert.AreEqual(3, result, "The count is not correctly computed.");
        }

        [Test]
        public void AllEntitiesVerifyProductID()
        {
            var result = ctx.Products.All().ToList();
            Assert.AreEqual(2, result[2].ProductID, "The ID is not correctly filled.");
        }

        [Test]
        public void AllEntitiesVerifyProductName()
        {
            var result = ctx.Products.All().ToList();
            Assert.AreEqual("Wine", result[1].Name, "The Product Name is not correctly filled.");
        }

        [Test]
        public void AllEntitiesVerifySupplierName()
        {
            var result = ctx.Products.All().ToList();
            Assert.AreEqual("City Bakery", result[0].Supplier.Name, "The Supplier Name is not correctly filled.");
        }

        [Test]
        public void AllEntitiesVerifyReleaseDate()
        {
            var result = ctx.Products.All().ToList();
            Assert.AreEqual(new DateTime(1992, 1, 1), result[0].ReleaseDate, "The ReleaseDate is not correctly filled.");
        }

        [Test]
        public void AllEntitiesVerifyNullDiscontinueDate()
        {
            var result = ctx.Products.All().ToList();
            Assert.Null(result[0].DiscontinueDate, "The DiscontinueDate must be null.");
        }

        [Test]
        public void AllEntitiesVerifyNonNullDiscontinueDate()
        {
            var result = ctx.Products.All().ToList();
            Assert.NotNull(result[1].DiscontinueDate, "The DiscontinueDate must not be null.");
        }

        [Test]
        public void AllEntitiesOrderby()
        {
            if (this.GetType() == typeof(QueryableServiceQueryTests))
                Assert.Ignore("OrderBy is not supported");

            var result = ctx.Products.All().OrderBy(ctx.Products.Name).ToList();
            for (int i = 0; i < 2; i++)
            {
                Assert.Greater(result[i + 1].Name, result[i].Name, "Names are not in correct order.");
            }
        }

        [Test]
        public void AllEntitiesOrderbyDescending()
        {
            if (this.GetType() == typeof(QueryableServiceQueryTests))
                Assert.Ignore("OrderBy is not supported");

            var result = ctx.Products.All().OrderByDescending(ctx.Products.Name).ToList();
            for (int i = 0; i < 2; i++)
            {
                Assert.Less(result[i + 1].Name, result[i].Name, "Names are not in correct order.");
            }
        }

        [Test]
        public void AllEntitiesOrderbyTakeOneVerifyResultCount()
        {
            if (this.GetType() == typeof(QueryableServiceQueryTests))
                Assert.Ignore("OrderBy and Take are not supported");

            var result = ctx.Products.All().OrderBy(ctx.Products.Name).Take(1).ToList();
            Assert.AreEqual(1, result.Count, "The service returned unexpected number of results.");
        }

        [Test]
        public void AllEntitiesVerifyQuantityValue()
        {
            var result = ctx.Products.All().ToList();
            Assert.AreEqual(12, result[0].Quantity.Value, "Unexpected quantity value.");
        }

        [Test]
        public void FilterEqualProductID()
        {
            var product = ctx.Products.Find(ctx.Products.ProductID == 1);
            Assert.AreEqual(1, product.ProductID);
        }

        [Test]
        public void FilterEqualName()
        {
            var result = ctx.Products.FindAll(ctx.Products.Name == "Bread").ToList();
            Assert.AreEqual(1, result.Count);
        }

        [Test]
        public void FilterEqualNameCountVerifyResult()
        {
            if (this.GetType() == typeof(QueryableServiceQueryTests))
                Assert.Ignore("Count is not supported");

            var result = ctx.Products.FindAll(ctx.Products.Name == "Bread").Count();
            Assert.AreEqual(1, result, "The count is not correctly computed.");
        }

        [Test]
        public void FilterEqualProductIDAndEqualName()
        {
            var result = ctx.Products.FindAll(ctx.Products.ProductID == 1 && ctx.Products.Name == "Bread").ToList();
            Assert.AreEqual(1, result.Count);
        }

        [Test]
        public void FilterGreaterProductID()
        {
            var result = ctx.Products.FindAll(ctx.Products.ProductID > 0).ToList();
            Assert.AreEqual(3, result.Count);
        }

        [Test]
        public void FilterNameContainsEqualsTrue()
        {
            if (this.GetType() == typeof(QueryableServiceQueryTests))
                Assert.Ignore("String.Contains is not supported");

            var result = ctx.Products.FindAll(ctx.Products.Name.Contains("i") == true).ToList();
            Assert.AreEqual(2, result.Count);
        }

        [Test]
        public void FilterNameContainsEqualsFalse()
        {
            if (this.GetType() == typeof(QueryableServiceQueryTests))
                Assert.Ignore("String.Contains is not supported");

            var result = ctx.Products.FindAll(ctx.Products.Name.Contains("i") == false).ToList();
            Assert.AreEqual(1, result.Count);
        }

        [Test]
        public void FilterGreaterRating()
        {
            var result = ctx.Products.FindAll(ctx.Products.Rating > 3).ToList();
            Assert.AreEqual(2, result.Count);
        }

        [Test]
        public void FilterNameLength()
        {
            if (this.GetType() == typeof(QueryableServiceQueryTests))
                Assert.Ignore("String.Length is not supported");

            var result = ctx.Products.FindAll(ctx.Products.Name.Length() == 4).ToList();
            Assert.AreEqual(2, result.Count);
        }

        [Test]
        public void FilterGreaterProductIDAndNameLength()
        {
            if (this.GetType() == typeof(QueryableServiceQueryTests))
                Assert.Ignore("String.Length is not supported");

            var result = ctx.Products.FindAll(ctx.Products.ProductID > 0 && ctx.Products.Name.Length() == 4).ToList();
            Assert.AreEqual(2, result.Count);
        }

        [Test]
        public void FilterNameLengthOrderByCountVerifyResult()
        {
            if (this.GetType() == typeof(QueryableServiceQueryTests))
                Assert.Ignore("OrderBy is not supported");

            var result = ctx.Products.FindAll(ctx.Products.Name.Length() == 4).OrderBy(ctx.Products.Rating).Count();
            Assert.AreEqual(2, result, "The count is not correctly computed.");
        }

        [Test]
        public void FilterEqualQuantityValue()
        {
            var result = ctx.Products.Find(ctx.Products.Quantity.Value == 7);
            Assert.AreEqual("Wine", result.Name);
        }

        [Test]
        public void FilterEqualQuantityUnits()
        {
            var result = ctx.Products.Find(ctx.Products.Quantity.Units == "liters");
            Assert.AreEqual("Milk", result.Name);
        }

        [Test]
        public void FilterEqualObjectID()
        {
            var product = ctx.Products.Find(ctx.Products.id != null);
            var id = product.id;
            product = ctx.Products.Find(ctx.Products.id == id);
            Assert.AreEqual(id, product.id);
        }

        [Test]
        public void ProjectionVerifyExcluded()
        {
            var product = ctx.Products.All().Select(ctx.Products.id).First();
            var id = product.id;
            try
            {
                var name = product.Name;
            }
            catch (Microsoft.CSharp.RuntimeBinder.RuntimeBinderException)
            {
            }
        }

        [Test]
        public void ProjectionVerifyID()
        {
            var products = ctx.Products.FindAll(ctx.Products.id != null).Select(ctx.Products.id).ToList();
            foreach (var product in products)
            {
                Assert.NotNull(product.id, "The ID is not correctly filled.");
            }
        }

        [Test]
        public void ProjectionVerifyName()
        {
            var products = ctx.Products.All().Select(ctx.Products.Name).ToList();
            foreach (var product in products)
            {
                Assert.IsNotNull(product.Name, "The Name is not correctly filled.");
            }
        }

        [Test]
        public void ProjectionVerifyQuantity()
        {
            var products = ctx.Products.All().Select(ctx.Products.Quantity).ToList();
            foreach (var product in products)
            {
                Assert.Greater(product.Quantity.Value, 0, "The Quantity is not correctly filled.");
            }
        }

        [Test]
        public void ProjectionVerifyNameDescriptionRating()
        {
            var products = ctx.Products.All().Select(ctx.Products.Name, ctx.Products.Description, ctx.Products.Rating).ToList();
            foreach (var product in products)
            {
                Assert.IsNotNull(product.Name, "The Name is not correctly filled.");
                Assert.IsNotNull(product.Description, "The Description is not correctly filled.");
                Assert.Greater(product.Rating, 0, "The Rating is not correctly filled.");
            }
        }
    }

    [TestFixture]
    public class InMemoryServiceQueryTests : QueryTests<ProductInMemoryService>
    {
    }

    [TestFixture]
    public class QueryableServiceQueryTests : QueryTests<ProductQueryableService>
    {
        [Test]
        public void FilterEqualQuantityValue_NoDynamicTypes()
        {
            try
            {
                DocumentDbMetadata.CreateDynamicTypesForComplexTypes = false;
                Assert.Throws<WebRequestException>(() => { var x = ctx.Products.Find(ctx.Products.Quantity.Value == 7); });
            }
            finally 
            {
                DocumentDbMetadata.CreateDynamicTypesForComplexTypes = true;
            }
        }

        [Test]
        public void FilterEqualQuantityUnits_NoDynamicTypes()
        {
            try
            {
                DocumentDbMetadata.CreateDynamicTypesForComplexTypes = false;
                Assert.Throws<WebRequestException>(() => { var x = ctx.Products.Find(ctx.Products.Quantity.Units == "liters"); });
            }
            finally
            {
                DocumentDbMetadata.CreateDynamicTypesForComplexTypes = true;
            }
        }
    }

    [TestFixture]
    public class QueryableServiceInterceptorTests : TestBase<ProductQueryableServiceWithQueryInterceptor>
    {
        protected override void PopulateTestData()
        {
            TestData.PopulateWithCategoriesAndProducts();
        }

        [SetUp]
        public override void SetUp()
        {
            TestService.Configuration = new DocumentDbConfiguration { MetadataBuildStrategy = new DocumentDbConfiguration.Metadata { PrefetchRows = -1, UpdateDynamically = false } };
            ProductQueryableServiceWithQueryInterceptor.ProductQueryInterceptor = null;
            base.SetUp();
        }

        [Test]
        public void AllEntitiesCountWithQueryInterceptorVerifyResult()
        {
            ProductQueryableServiceWithQueryInterceptor.ProductQueryInterceptor = (x => x.GetValue("Name").ToString() == "Wine");
            var result = ctx.Products.All().Count();
            ProductQueryableServiceWithQueryInterceptor.ProductQueryInterceptor = null;
            Assert.AreEqual(1, result, "The count is not correctly computed.");
        }

        [Test]
        public void AllEntitiesCountWithVBEqualInterceptorVerifyResult()
        {
            ProductQueryableServiceWithQueryInterceptor.ProductQueryInterceptor = (x => (bool)(ExtensionMethods.CompareObjectEqual(x.GetValue("Name").ToString(), "Wine", false)));
            var result = ctx.Products.All().Count();
            ProductQueryableServiceWithQueryInterceptor.ProductQueryInterceptor = null;
            Assert.AreEqual(1, result, "The count is not correctly computed.");
        }

        [Test]
        public void AllEntitiesCountWithVBNotEqualInterceptorVerifyResult()
        {
            ProductQueryableServiceWithQueryInterceptor.ProductQueryInterceptor = (x => (bool)(ExtensionMethods.CompareObjectNotEqual(x.GetValue("Name").ToString(), "Wine", false)));
            var result = ctx.Products.All().Count();
            ProductQueryableServiceWithQueryInterceptor.ProductQueryInterceptor = null;
            Assert.AreEqual(2, result, "The count is not correctly computed.");
        }

        [Test]
        public void AllEntitiesCountWithVBConditionalEqualInterceptorVerifyResult()
        {
            ProductQueryableServiceWithQueryInterceptor.ProductQueryInterceptor = (x => (ExtensionMethods.ConditionalCompareObjectEqual(x.GetValue("Name").ToString(), "Wine", false)));
            var result = ctx.Products.All().Count();
            ProductQueryableServiceWithQueryInterceptor.ProductQueryInterceptor = null;
            Assert.AreEqual(1, result, "The count is not correctly computed.");
        }

        [Test]
        public void AllEntitiesCountWithVBConditionalNotEqualInterceptorVerifyResult()
        {
            ProductQueryableServiceWithQueryInterceptor.ProductQueryInterceptor = (x => (ExtensionMethods.ConditionalCompareObjectNotEqual(x.GetValue("Name").ToString(), "Wine", false)));
            var result = ctx.Products.All().Count();
            ProductQueryableServiceWithQueryInterceptor.ProductQueryInterceptor = null;
            Assert.AreEqual(2, result, "The count is not correctly computed.");
        }
    }
}
