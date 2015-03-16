using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Text;
using NUnit.Framework;
using Simple.Data;
using Simple.Data.OData;

namespace DocumentDB.Context.Tests
{
    public abstract class DocumentIdTests<T> : TestBase<T>
    {
        protected override void PopulateTestData()
        {
            TestData.PopulateWithDocumentIdTypes();
        }

        [Test]
        public void ValidateMetadata()
        {
            base.RequestAndValidateMetadata();
        }

        [Test]
        public void AllTypesWithoutExplicitIdVerifyResultCountAndId()
        {
            var result = ctx.TypeWithoutExplicitId.All().ToList();
            Assert.AreEqual(3, result.Count, "The service returned unexpected number of results.");
            Assert.IsNotNull(result[0].db_id);
        }

        [Test]
        public void AllTypesWithDocumentIdVerifyResultCountAndId()
        {
            var result = ctx.TypeWithDocumentId.All().ToList();
            Assert.AreEqual(3, result.Count, "The service returned unexpected number of results.");
            Assert.IsNotNull(result[0].db_id);
        }

        [Test]
        public void AllTypesWithIntIdVerifyResultCountAndId()
        {
            var result = ctx.TypeWithIntId.All().ToList();
            Assert.AreEqual(3, result.Count, "The service returned unexpected number of results.");
            Assert.AreEqual(1, result[0].db_id);
        }

        [Test]
        public void AllTypesWithStringIdVerifyResultCountAndId()
        {
            var result = ctx.TypeWithStringId.All().ToList();
            Assert.AreEqual(3, result.Count, "The service returned unexpected number of results.");
            Assert.AreEqual("1", result[0].db_id);
        }

        [Test]
        public void AllTypesWithGuidIdVerifyResultCountAndId()
        {
            var result = ctx.TypeWithGuidId.All().ToList();
            Assert.AreEqual(3, result.Count, "The service returned unexpected number of results.");
            Assert.AreNotEqual(Guid.Empty.ToString(), result[0].db_id);
        }
    }

    [TestFixture]
    public class InMemoryServiceDocumentIdTests : DocumentIdTests<ProductInMemoryService>
    {
    }

    [TestFixture]
    public class QueryableServiceDocumentIdTests : DocumentIdTests<ProductQueryableService>
    {
    }
}
