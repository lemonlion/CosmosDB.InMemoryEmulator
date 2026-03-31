using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using System.Net;
using System.Text;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;


public class ETagGapTests2
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task IfMatch_WithWildcard_Star_AlwaysSucceeds()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var response = await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
            new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "*" });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task IfNoneMatch_WithWildcard_Star_Returns304WhenExists()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfNoneMatchEtag = "*" });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotModified);
    }
}


public class ETagGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task ETag_ChangesOnEveryWrite()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "First" };
        var create = await _container.CreateItemAsync(item, new PartitionKey("pk1"));
        var firstEtag = create.ETag;

        var upsert = await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Second" },
            new PartitionKey("pk1"));
        var secondEtag = upsert.ETag;

        firstEtag.Should().NotBe(secondEtag);
    }

    [Fact]
    public async Task ETag_ConsistentAcrossMultipleReads()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var read1 = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        var read2 = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        read1.ETag.Should().Be(read2.ETag);
    }

    [Fact]
    public async Task ConcurrentUpsert_IfMatch_SecondWriteFails()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
        var create = await _container.CreateItemAsync(item, new PartitionKey("pk1"));
        var etag = create.ETag;

        await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "First Writer" },
            new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = etag });

        var act = () => _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Second Writer" },
            new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = etag });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }
}


public class ETagGapTests3
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task IfMatch_OnCreate_IsIgnored()
    {
        // Create doesn't have a prior version, so IfMatch should be irrelevant
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

        var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "\"nonexistent\"" });

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task IfMatch_OnPatch_WithCorrectETag_Succeeds()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 10 };
        var create = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Patched")],
            new PatchItemRequestOptions { IfMatchEtag = create.ETag });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Name.Should().Be("Patched");
    }

    [Fact]
    public async Task IfMatch_OnPatch_WithStaleETag_Fails412()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 10 };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Patched")],
            new PatchItemRequestOptions { IfMatchEtag = "\"stale\"" });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }
}
