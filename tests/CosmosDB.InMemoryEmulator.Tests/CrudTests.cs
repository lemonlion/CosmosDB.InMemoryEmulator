using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class CreateItemTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task CreateItemAsync_WithValidItem_ReturnsCreatedResponse()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

        var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Resource.Should().NotBeNull();
        response.Resource.Id.Should().Be("1");
        response.Resource.Name.Should().Be("Test");
    }

    [Fact]
    public async Task CreateItemAsync_WithValidItem_SetsETag()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

        var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        response.ETag.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task CreateItemAsync_DuplicateId_ThrowsConflict()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var act = () => _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var exception = await act.Should().ThrowAsync<CosmosException>();
        exception.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task CreateItemAsync_SameIdDifferentPartitionKey_BothSucceed()
    {
        var item1 = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "First" };
        var item2 = new TestDocument { Id = "1", PartitionKey = "pk2", Name = "Second" };

        await _container.CreateItemAsync(item1, new PartitionKey("pk1"));
        var response = await _container.CreateItemAsync(item2, new PartitionKey("pk2"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task CreateItemAsync_WithoutId_ThrowsInvalidOperation()
    {
        var item = new { notAnId = "test" };

        var act = () => _container.CreateItemAsync(item, new PartitionKey("pk1"));

        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task CreateItemAsync_WithNullPartitionKey_ExtractsFromPartitionKeyPath()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

        var response = await _container.CreateItemAsync(item);

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        readResponse.Resource.Name.Should().Be("Test");
    }

    [Fact]
    public async Task CreateItemAsync_ItemIsRetrievable()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 42 };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        readResponse.Resource.Name.Should().Be("Test");
        readResponse.Resource.Value.Should().Be(42);
    }
}

public class ReadItemTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task ReadItemAsync_ExistingItem_ReturnsOk()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var response = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Name.Should().Be("Test");
    }

    [Fact]
    public async Task ReadItemAsync_NonExistentItem_ThrowsNotFound()
    {
        var act = () => _container.ReadItemAsync<TestDocument>("nonexistent", new PartitionKey("pk1"));

        var exception = await act.Should().ThrowAsync<CosmosException>();
        exception.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task ReadItemAsync_WrongPartitionKey_ThrowsNotFound()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("wrong-pk"));

        var exception = await act.Should().ThrowAsync<CosmosException>();
        exception.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task ReadItemAsync_ReturnsETag()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var response = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        response.ETag.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task ReadItemAsync_WithIfNoneMatchCurrentETag_ThrowsNotModified()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        var createResponse = await _container.CreateItemAsync(item, new PartitionKey("pk1"));
        var etag = createResponse.ETag;

        var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfNoneMatchEtag = etag });

        var exception = await act.Should().ThrowAsync<CosmosException>();
        exception.Which.StatusCode.Should().Be(HttpStatusCode.NotModified);
    }

    [Fact]
    public async Task ReadItemAsync_WithIfNoneMatchStaleETag_ReturnsOk()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var response = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfNoneMatchEtag = "\"stale-etag\"" });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}

public class UpsertItemTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task UpsertItemAsync_NewItem_ReturnsCreated()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

        var response = await _container.UpsertItemAsync(item, new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Resource.Name.Should().Be("Test");
    }

    [Fact]
    public async Task UpsertItemAsync_ExistingItem_ReturnsOkWithUpdatedData()
    {
        var original = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
        await _container.CreateItemAsync(original, new PartitionKey("pk1"));

        var updated = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" };
        var response = await _container.UpsertItemAsync(updated, new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        readResponse.Resource.Name.Should().Be("Updated");
    }

    [Fact]
    public async Task UpsertItemAsync_WithIfMatchCurrentETag_Succeeds()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        var createResponse = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var updated = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" };
        var response = await _container.UpsertItemAsync(updated, new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = createResponse.ETag });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task UpsertItemAsync_WithIfMatchStaleETag_ThrowsPreconditionFailed()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var updated = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" };
        var act = () => _container.UpsertItemAsync(updated, new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

        var exception = await act.Should().ThrowAsync<CosmosException>();
        exception.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task UpsertItemAsync_UpdatesETag()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        var createResponse = await _container.CreateItemAsync(item, new PartitionKey("pk1"));
        var originalEtag = createResponse.ETag;

        var updated = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" };
        var upsertResponse = await _container.UpsertItemAsync(updated, new PartitionKey("pk1"));

        upsertResponse.ETag.Should().NotBe(originalEtag);
    }
}

public class ReplaceItemTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task ReplaceItemAsync_ExistingItem_ReturnsOk()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var replacement = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" };
        var response = await _container.ReplaceItemAsync(replacement, "1", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Name.Should().Be("Replaced");
    }

    [Fact]
    public async Task ReplaceItemAsync_NonExistentItem_ThrowsNotFound()
    {
        var replacement = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" };

        var act = () => _container.ReplaceItemAsync(replacement, "1", new PartitionKey("pk1"));

        var exception = await act.Should().ThrowAsync<CosmosException>();
        exception.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task ReplaceItemAsync_WithIfMatchCurrentETag_Succeeds()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
        var createResponse = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var replacement = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" };
        var response = await _container.ReplaceItemAsync(replacement, "1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = createResponse.ETag });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task ReplaceItemAsync_WithIfMatchStaleETag_ThrowsPreconditionFailed()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var replacement = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" };
        var act = () => _container.ReplaceItemAsync(replacement, "1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

        var exception = await act.Should().ThrowAsync<CosmosException>();
        exception.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task ReplaceItemAsync_DataIsUpdated()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original", Value = 1 };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var replacement = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced", Value = 99 };
        await _container.ReplaceItemAsync(replacement, "1", new PartitionKey("pk1"));

        var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        readResponse.Resource.Name.Should().Be("Replaced");
        readResponse.Resource.Value.Should().Be(99);
    }
}

public class DeleteItemTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task DeleteItemAsync_ExistingItem_ReturnsNoContent()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var response = await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task DeleteItemAsync_NonExistentItem_ThrowsNotFound()
    {
        var act = () => _container.DeleteItemAsync<TestDocument>("nonexistent", new PartitionKey("pk1"));

        var exception = await act.Should().ThrowAsync<CosmosException>();
        exception.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task DeleteItemAsync_ItemNoLongerReadable()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));
        await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        var exception = await act.Should().ThrowAsync<CosmosException>();
        exception.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task DeleteItemAsync_WithIfMatchCurrentETag_Succeeds()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        var createResponse = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var response = await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = createResponse.ETag });

        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task DeleteItemAsync_WithIfMatchStaleETag_ThrowsPreconditionFailed()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var act = () => _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

        var exception = await act.Should().ThrowAsync<CosmosException>();
        exception.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task DeleteItemAsync_OnlyAffectsSpecifiedPartitionKey()
    {
        var item1 = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Keep" };
        var item2 = new TestDocument { Id = "1", PartitionKey = "pk2", Name = "Delete" };
        await _container.CreateItemAsync(item1, new PartitionKey("pk1"));
        await _container.CreateItemAsync(item2, new PartitionKey("pk2"));

        await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk2"));

        var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        readResponse.Resource.Name.Should().Be("Keep");
    }

    [Fact]
    public async Task DeleteItemAsync_CanRecreateAfterDelete()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));
        await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        var newItem = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Recreated" };
        var response = await _container.CreateItemAsync(newItem, new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        readResponse.Resource.Name.Should().Be("Recreated");
    }
}
