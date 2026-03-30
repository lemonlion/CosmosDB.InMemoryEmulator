using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class PatchItemTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private async Task<TestDocument> CreateTestItem(string id = "1", string pk = "pk1")
    {
        var item = new TestDocument
        {
            Id = id,
            PartitionKey = pk,
            Name = "Original",
            Value = 10,
            IsActive = true,
            Tags = ["tag1", "tag2"],
            Nested = new NestedObject { Description = "Nested", Score = 5.0 }
        };
        await _container.CreateItemAsync(item, new PartitionKey(pk));
        return item;
    }

    [Fact]
    public async Task PatchItemAsync_SetOperation_UpdatesProperty()
    {
        await CreateTestItem();
        var patchOperations = new[] { PatchOperation.Set("/name", "Patched") };

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), patchOperations);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Name.Should().Be("Patched");
    }

    [Fact]
    public async Task PatchItemAsync_ReplaceOperation_UpdatesProperty()
    {
        await CreateTestItem();
        var patchOperations = new[] { PatchOperation.Replace("/value", 99) };

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), patchOperations);

        response.Resource.Value.Should().Be(99);
    }

    [Fact]
    public async Task PatchItemAsync_AddOperation_AddsProperty()
    {
        await CreateTestItem();
        var patchOperations = new[] { PatchOperation.Add("/newField", "newValue") };

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), patchOperations);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task PatchItemAsync_RemoveOperation_RemovesProperty()
    {
        await CreateTestItem();
        var patchOperations = new[] { PatchOperation.Remove("/name") };

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), patchOperations);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Name.Should().BeNull();
    }

    [Fact]
    public async Task PatchItemAsync_IncrementOperation_IncrementsValue()
    {
        await CreateTestItem();
        var patchOperations = new[] { PatchOperation.Increment("/value", 5) };

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), patchOperations);

        response.Resource.Value.Should().Be(15);
    }

    [Fact]
    public async Task PatchItemAsync_NonExistentItem_ThrowsNotFound()
    {
        var patchOperations = new[] { PatchOperation.Set("/name", "Patched") };

        var act = () => _container.PatchItemAsync<TestDocument>("nonexistent", new PartitionKey("pk1"), patchOperations);

        var exception = await act.Should().ThrowAsync<CosmosException>();
        exception.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task PatchItemAsync_WithIfMatchCurrentETag_Succeeds()
    {
        await CreateTestItem();
        var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        var patchOperations = new[] { PatchOperation.Set("/name", "Patched") };

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), patchOperations,
            new PatchItemRequestOptions { IfMatchEtag = readResponse.ETag });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task PatchItemAsync_WithIfMatchStaleETag_ThrowsPreconditionFailed()
    {
        await CreateTestItem();
        var patchOperations = new[] { PatchOperation.Set("/name", "Patched") };

        var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), patchOperations,
            new PatchItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

        var exception = await act.Should().ThrowAsync<CosmosException>();
        exception.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task PatchItemAsync_UpdatesETag()
    {
        await CreateTestItem();
        var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        var originalEtag = readResponse.ETag;

        var patchOperations = new[] { PatchOperation.Set("/name", "Patched") };
        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), patchOperations);

        response.ETag.Should().NotBe(originalEtag);
    }

    [Fact]
    public async Task PatchItemAsync_MultipleOperations_AllApplied()
    {
        await CreateTestItem();
        var patchOperations = new List<PatchOperation>
        {
            PatchOperation.Set("/name", "MultiPatched"),
            PatchOperation.Replace("/value", 42),
            PatchOperation.Set("/isActive", false)
        };

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), patchOperations);

        response.Resource.Name.Should().Be("MultiPatched");
        response.Resource.Value.Should().Be(42);
        response.Resource.IsActive.Should().BeFalse();
    }

    [Fact]
    public async Task PatchItemAsync_NestedProperty_UpdatesCorrectly()
    {
        await CreateTestItem();
        var patchOperations = new[] { PatchOperation.Set("/nested/description", "Updated Nested") };

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), patchOperations);

        response.Resource.Nested!.Description.Should().Be("Updated Nested");
    }
}
