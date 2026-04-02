using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using System.Net;
using System.Text;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;


public class ResponseMetadataGapTests4
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Response_ActivityId_NotNull()
    {
        var response = await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        response.ActivityId.Should().NotBeNullOrEmpty();
        Guid.TryParse(response.ActivityId, out _).Should().BeTrue();
    }

    [Fact]
    public async Task Response_Headers_ContainStandardCosmosHeaders()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        var response = await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

        response.Headers.ETag.Should().NotBeNullOrEmpty();
        response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
        response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
    }
}


public class ResponseMetadataGapTests3
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Response_Diagnostics_NotNull()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        response.Diagnostics.Should().NotBeNull();
    }

    [Fact]
    public async Task StreamResponse_Headers_ContainETag_AfterWrite()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        var response = await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

        response.Headers.ETag.Should().NotBeNullOrEmpty();
    }
}


public class ResponseMetadataGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Response_RequestCharge_PositiveOnWrite()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        response.RequestCharge.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task Response_ETag_SetOnAllWriteOperations()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

        var create = await _container.CreateItemAsync(item, new PartitionKey("pk1"));
        create.ETag.Should().NotBeNullOrEmpty();

        var upsert = await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Upserted" },
            new PartitionKey("pk1"));
        upsert.ETag.Should().NotBeNullOrEmpty();

        var replace = await _container.ReplaceItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" },
            "1", new PartitionKey("pk1"));
        replace.ETag.Should().NotBeNullOrEmpty();

        var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.ETag.Should().NotBeNullOrEmpty();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Typed Response Status Codes
// ═══════════════════════════════════════════════════════════════════════════

public class TypedResponseStatusCodeTests
{
    private readonly InMemoryContainer _container = new("test", "/partitionKey");

    [Fact]
    public async Task CreateItemAsync_Returns_Created()
    {
        var response = await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task ReadItemAsync_Returns_OK()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        var response = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task UpsertItemAsync_NewItem_Returns_Created()
    {
        var response = await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task UpsertItemAsync_ExistingItem_Returns_OK()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        var response = await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Updated" },
            new PartitionKey("pk"));
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task ReplaceItemAsync_Returns_OK()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        var response = await _container.ReplaceItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Replaced" },
            "1", new PartitionKey("pk"));
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task DeleteItemAsync_Returns_NoContent()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        var response = await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk"));
        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task PatchItemAsync_Returns_OK()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk"),
            [PatchOperation.Set("/name", "Patched")]);
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Typed Response Metadata
// ═══════════════════════════════════════════════════════════════════════════

public class TypedResponseMetadataTests
{
    private readonly InMemoryContainer _container = new("test", "/partitionKey");

    [Fact]
    public async Task CreateItemAsync_HasAllMetadata()
    {
        var response = await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        response.RequestCharge.Should().BeGreaterThan(0);
        response.ActivityId.Should().NotBeNullOrEmpty();
        response.Diagnostics.Should().NotBeNull();
        response.ETag.Should().NotBeNullOrEmpty();
        response.Headers.Should().NotBeNull();
    }

    [Fact]
    public async Task ReadItemAsync_HasAllMetadata()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        var response = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));

        response.RequestCharge.Should().BeGreaterThan(0);
        response.ActivityId.Should().NotBeNullOrEmpty();
        response.Diagnostics.Should().NotBeNull();
        response.ETag.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task UpsertItemAsync_HasAllMetadata()
    {
        var response = await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        response.RequestCharge.Should().BeGreaterThan(0);
        response.ActivityId.Should().NotBeNullOrEmpty();
        response.Diagnostics.Should().NotBeNull();
        response.ETag.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task ReplaceItemAsync_HasAllMetadata()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        var response = await _container.ReplaceItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "B" },
            "1", new PartitionKey("pk"));

        response.RequestCharge.Should().BeGreaterThan(0);
        response.ActivityId.Should().NotBeNullOrEmpty();
        response.Diagnostics.Should().NotBeNull();
        response.ETag.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task DeleteItemAsync_HasMetadata()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        var response = await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk"));

        response.RequestCharge.Should().BeGreaterThan(0);
        response.ActivityId.Should().NotBeNullOrEmpty();
        response.Diagnostics.Should().NotBeNull();
    }

    [Fact]
    public async Task PatchItemAsync_HasAllMetadata()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk"),
            [PatchOperation.Set("/name", "Patched")]);

        response.RequestCharge.Should().BeGreaterThan(0);
        response.ActivityId.Should().NotBeNullOrEmpty();
        response.Diagnostics.Should().NotBeNull();
        response.ETag.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task ActivityId_IsUnique_PerOperation()
    {
        var r1 = await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));
        var r2 = await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk", Name = "B" },
            new PartitionKey("pk"));

        r1.ActivityId.Should().NotBe(r2.ActivityId);
    }

    [Fact]
    public async Task ETag_IsQuotedString()
    {
        var response = await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        response.ETag.Should().StartWith("\"").And.EndWith("\"");
    }

    [Fact]
    public async Task Diagnostics_GetClientElapsedTime_ReturnsTimeSpan()
    {
        var response = await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        response.Diagnostics.GetClientElapsedTime().Should().Be(TimeSpan.Zero);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Stream Response Status Codes
// ═══════════════════════════════════════════════════════════════════════════

public class StreamResponseStatusCodeTests
{
    private readonly InMemoryContainer _container = new("test", "/partitionKey");

    private static MemoryStream MakeStream(string json) =>
        new(Encoding.UTF8.GetBytes(json));

    [Fact]
    public async Task CreateItemStreamAsync_Returns_Created()
    {
        var response = await _container.CreateItemStreamAsync(
            MakeStream("""{"id":"1","partitionKey":"pk"}"""),
            new PartitionKey("pk"));
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task ReadItemStreamAsync_Returns_OK()
    {
        await _container.CreateItemStreamAsync(
            MakeStream("""{"id":"1","partitionKey":"pk"}"""),
            new PartitionKey("pk"));

        var response = await _container.ReadItemStreamAsync("1", new PartitionKey("pk"));
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task UpsertItemStreamAsync_NewItem_Returns_Created()
    {
        var response = await _container.UpsertItemStreamAsync(
            MakeStream("""{"id":"1","partitionKey":"pk"}"""),
            new PartitionKey("pk"));
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task UpsertItemStreamAsync_ExistingItem_Returns_OK()
    {
        await _container.CreateItemStreamAsync(
            MakeStream("""{"id":"1","partitionKey":"pk"}"""),
            new PartitionKey("pk"));

        var response = await _container.UpsertItemStreamAsync(
            MakeStream("""{"id":"1","partitionKey":"pk","name":"Updated"}"""),
            new PartitionKey("pk"));
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task ReplaceItemStreamAsync_Returns_OK()
    {
        await _container.CreateItemStreamAsync(
            MakeStream("""{"id":"1","partitionKey":"pk"}"""),
            new PartitionKey("pk"));

        var response = await _container.ReplaceItemStreamAsync(
            MakeStream("""{"id":"1","partitionKey":"pk","name":"Replaced"}"""),
            "1", new PartitionKey("pk"));
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task DeleteItemStreamAsync_Returns_NoContent()
    {
        await _container.CreateItemStreamAsync(
            MakeStream("""{"id":"1","partitionKey":"pk"}"""),
            new PartitionKey("pk"));

        var response = await _container.DeleteItemStreamAsync("1", new PartitionKey("pk"));
        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task PatchItemStreamAsync_Returns_OK()
    {
        await _container.CreateItemStreamAsync(
            MakeStream("""{"id":"1","partitionKey":"pk","name":"A"}"""),
            new PartitionKey("pk"));

        var response = await _container.PatchItemStreamAsync("1", new PartitionKey("pk"),
            [PatchOperation.Set("/name", "Patched")]);
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Stream Response Headers
// ═══════════════════════════════════════════════════════════════════════════

public class StreamResponseHeaderTests
{
    private readonly InMemoryContainer _container = new("test", "/partitionKey");

    private static MemoryStream MakeStream(string json) =>
        new(Encoding.UTF8.GetBytes(json));

    [Fact]
    public async Task StreamCreate_HasActivityId_RequestCharge_ETag()
    {
        var response = await _container.CreateItemStreamAsync(
            MakeStream("""{"id":"1","partitionKey":"pk"}"""),
            new PartitionKey("pk"));

        response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
        response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
        response.Headers.ETag.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task StreamRead_HasActivityId_RequestCharge_ETag()
    {
        await _container.CreateItemStreamAsync(
            MakeStream("""{"id":"1","partitionKey":"pk"}"""),
            new PartitionKey("pk"));

        var response = await _container.ReadItemStreamAsync("1", new PartitionKey("pk"));

        response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
        response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
        response.Headers.ETag.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task StreamUpsert_HasActivityId_RequestCharge_ETag()
    {
        var response = await _container.UpsertItemStreamAsync(
            MakeStream("""{"id":"1","partitionKey":"pk"}"""),
            new PartitionKey("pk"));

        response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
        response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
        response.Headers.ETag.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task StreamReplace_HasActivityId_RequestCharge_ETag()
    {
        await _container.CreateItemStreamAsync(
            MakeStream("""{"id":"1","partitionKey":"pk"}"""),
            new PartitionKey("pk"));

        var response = await _container.ReplaceItemStreamAsync(
            MakeStream("""{"id":"1","partitionKey":"pk","name":"B"}"""),
            "1", new PartitionKey("pk"));

        response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
        response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
        response.Headers.ETag.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task StreamDelete_HasActivityId_RequestCharge()
    {
        await _container.CreateItemStreamAsync(
            MakeStream("""{"id":"1","partitionKey":"pk"}"""),
            new PartitionKey("pk"));

        var response = await _container.DeleteItemStreamAsync("1", new PartitionKey("pk"));

        response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
        response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Error Response Status Codes
// ═══════════════════════════════════════════════════════════════════════════

public class ErrorResponseStatusCodeTests
{
    private readonly InMemoryContainer _container = new("test", "/partitionKey");

    [Fact]
    public async Task CreateItemAsync_Duplicate_Throws_Conflict_409()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        var act = () => _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Dup" },
            new PartitionKey("pk"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task ReadItemAsync_NotFound_Throws_404()
    {
        var act = () => _container.ReadItemAsync<TestDocument>("nope", new PartitionKey("pk"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task ReplaceItemAsync_NotFound_Throws_404()
    {
        var act = () => _container.ReplaceItemAsync(
            new TestDocument { Id = "nope", PartitionKey = "pk", Name = "X" },
            "nope", new PartitionKey("pk"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task DeleteItemAsync_NotFound_Throws_404()
    {
        var act = () => _container.DeleteItemAsync<TestDocument>("nope", new PartitionKey("pk"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task PatchItemAsync_NotFound_Throws_404()
    {
        var act = () => _container.PatchItemAsync<TestDocument>("nope", new PartitionKey("pk"),
            [PatchOperation.Set("/name", "X")]);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task ReplaceItemAsync_StaleETag_Throws_PreconditionFailed_412()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        var act = () => _container.ReplaceItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "B" },
            "1", new PartitionKey("pk"),
            new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task PatchItemAsync_FilterPredicate_Throws_PreconditionFailed_412()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A", IsActive = false },
            new PartitionKey("pk"));

        var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk"),
            [PatchOperation.Set("/name", "X")],
            new PatchItemRequestOptions { FilterPredicate = "FROM c WHERE c.isActive = true" });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Feed Response Metadata
// ═══════════════════════════════════════════════════════════════════════════

public class FeedResponseMetadataTests
{
    [Fact]
    public async Task FeedResponse_HasRequestCharge()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
        var page = await iterator.ReadNextAsync();

        page.RequestCharge.Should().BeGreaterThanOrEqualTo(0);
    }

    [Fact]
    public async Task FeedResponse_HasDiagnostics()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
        var page = await iterator.ReadNextAsync();

        page.Diagnostics.Should().NotBeNull();
    }

    [Fact]
    public async Task FeedResponse_HasStatusCode_OK()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
        var page = await iterator.ReadNextAsync();

        page.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task FeedResponse_ActivityId_IsValidGuid()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
        var page = await iterator.ReadNextAsync();

        Guid.TryParse(page.ActivityId, out _).Should().BeTrue();
    }

    [Fact]
    public async Task FeedResponse_ActivityId_EmulatorBehavior_IsValidGuid()
    {
        // Previously divergent: ActivityId was empty string. Now returns a valid GUID.
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
        var page = await iterator.ReadNextAsync();

        Guid.TryParse(page.ActivityId, out _).Should().BeTrue("emulator now returns a valid GUID for FeedResponse.ActivityId");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Batch Response Metadata
// ═══════════════════════════════════════════════════════════════════════════

public class BatchResponseMetadataTests
{
    [Fact]
    public async Task BatchResponse_HasStatusCode_OnSuccess()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var batch = container.CreateTransactionalBatch(new PartitionKey("pk"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" });

        using var response = await batch.ExecuteAsync();
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task BatchResponse_HasRequestCharge()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var batch = container.CreateTransactionalBatch(new PartitionKey("pk"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" });

        using var response = await batch.ExecuteAsync();
        response.RequestCharge.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task BatchResponse_Count_MatchesOperations()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var batch = container.CreateTransactionalBatch(new PartitionKey("pk"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" });
        batch.CreateItem(new TestDocument { Id = "2", PartitionKey = "pk", Name = "B" });

        using var response = await batch.ExecuteAsync();
        response.Count.Should().Be(2);
    }

    [Fact]
    public async Task BatchResponse_IsSuccessStatusCode_TrueOnSuccess()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var batch = container.CreateTransactionalBatch(new PartitionKey("pk"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" });

        using var response = await batch.ExecuteAsync();
        response.IsSuccessStatusCode.Should().BeTrue();
    }

    [Fact]
    public async Task BatchResponse_PerOperation_HasStatusCode()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var batch = container.CreateTransactionalBatch(new PartitionKey("pk"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" });
        batch.CreateItem(new TestDocument { Id = "2", PartitionKey = "pk", Name = "B" });

        using var response = await batch.ExecuteAsync();
        response[0].StatusCode.Should().Be(HttpStatusCode.Created);
        response[1].StatusCode.Should().Be(HttpStatusCode.Created);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Database/Container Response Metadata
// ═══════════════════════════════════════════════════════════════════════════

public class DatabaseContainerResponseMetadataTests
{
    [Fact]
    public async Task CreateDatabaseAsync_Returns_Created()
    {
        var client = new InMemoryCosmosClient();
        var response = await client.CreateDatabaseAsync("testdb");
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task CreateContainerAsync_Returns_Created()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("testdb")).Database;

        var response = await db.CreateContainerAsync("mycontainer", "/pk");
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task CreateContainerAsync_HasResource()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("testdb")).Database;

        var response = await db.CreateContainerAsync("mycontainer", "/pk");
        response.Resource.Should().NotBeNull();
        response.Resource.Id.Should().Be("mycontainer");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Divergent Behavior Documentation
// ═══════════════════════════════════════════════════════════════════════════

public class ResponseMetadataDivergentBehaviorTests
{
    [Fact(Skip = "Real Cosmos DB: writes ~6-10 RU, reads ~1 RU, queries vary by complexity. " +
        "The emulator always returns a synthetic 1.0 RU charge.")]
    public void RequestCharge_ShouldVaryByOperationType()
    {
        // Placeholder — real Cosmos varies RU by operation
    }

    [Fact]
    public async Task RequestCharge_EmulatorBehavior_AlwaysSynthetic()
    {
        // ── Divergent behavior documentation ──
        // Real Cosmos: RU charge varies (writes ~6-10, reads ~1, queries vary).
        // Emulator: Always returns 1.0 RU.
        var container = new InMemoryContainer("test", "/partitionKey");

        var createResp = await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));
        var readResp = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));

        createResp.RequestCharge.Should().Be(readResp.RequestCharge,
            "emulator returns the same synthetic RU for all operations");
    }

    [Fact(Skip = "Real Cosmos DB: Diagnostics contain query plan, execution timing, retries, " +
        "contacted regions, and partitions. The emulator returns a stub with TimeSpan.Zero.")]
    public void Diagnostics_ShouldContainQueryPlanAndTimings()
    {
        // Placeholder
    }

    [Fact]
    public async Task Diagnostics_EmulatorBehavior_StubReturnsZeroElapsed()
    {
        // ── Divergent behavior documentation ──
        // Real Cosmos: Rich diagnostics with timing, retries, regions.
        // Emulator: Stub returns TimeSpan.Zero.
        var container = new InMemoryContainer("test", "/partitionKey");
        var response = await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        response.Diagnostics.GetClientElapsedTime().Should().Be(TimeSpan.Zero);
    }

    [Fact(Skip = "Real Cosmos DB: Session tokens are cumulative LSN-based (e.g. '0:0#12345'). " +
        "The emulator generates a random GUID per response with no cumulative state.")]
    public void SessionToken_ShouldBeCumulative()
    {
        // Placeholder
    }

    [Fact]
    public async Task SessionToken_EmulatorBehavior_FixedPerResponse()
    {
        // Emulator returns a fixed session token on all stream responses.
        // Real Cosmos DB uses LSN-based cumulative tokens.
        var container = new InMemoryContainer("test", "/partitionKey");

        var json = """{"id":"1","partitionKey":"pk"}""";
        var r1 = await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk"));

        json = """{"id":"2","partitionKey":"pk"}""";
        var r2 = await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk"));

        var token1 = r1.Headers["x-ms-session-token"];
        var token2 = r2.Headers["x-ms-session-token"];

        token1.Should().NotBeNullOrEmpty();
        token2.Should().NotBeNullOrEmpty();
    }
}
