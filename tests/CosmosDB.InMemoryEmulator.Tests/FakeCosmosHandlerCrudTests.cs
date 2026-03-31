using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// TDD tests for CRUD route handling in <see cref="FakeCosmosHandler"/>.
/// These tests exercise the real Cosmos SDK HTTP pipeline end-to-end:
/// SDK → HTTP request → FakeCosmosHandler → InMemoryContainer → HTTP response → SDK.
/// </summary>
public class FakeCosmosHandlerCrudTests : IDisposable
{
    private readonly InMemoryContainer _inMemoryContainer;
    private readonly FakeCosmosHandler _handler;
    private readonly CosmosClient _client;
    private readonly Container _container;

    public FakeCosmosHandlerCrudTests()
    {
        _inMemoryContainer = new InMemoryContainer("test-crud", "/partitionKey");
        _handler = new FakeCosmosHandler(_inMemoryContainer);
        _client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                RequestTimeout = TimeSpan.FromSeconds(10),
                HttpClientFactory = () => new HttpClient(_handler) { Timeout = TimeSpan.FromSeconds(10) }
            });
        _container = _client.GetContainer("db", "test-crud");
    }

    public void Dispose()
    {
        _client.Dispose();
        _handler.Dispose();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  1A. Create Item
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Handler_CreateItem_ReturnsCreated()
    {
        var doc = new TestDocument { Id = "c1", PartitionKey = "pk1", Name = "Alice", Value = 10 };

        var response = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Resource.Should().NotBeNull();
        response.Resource.Id.Should().Be("c1");
        response.Resource.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task Handler_CreateItem_DuplicateId_ReturnsConflict()
    {
        var doc = new TestDocument { Id = "c2", PartitionKey = "pk1", Name = "Alice" };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var act = () => _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task Handler_CreateItem_ThenQuery_ReturnsItem()
    {
        var doc = new TestDocument { Id = "c3", PartitionKey = "pk1", Name = "Bob", Value = 20 };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var results = new List<TestDocument>();
        var iterator = _container.GetItemQueryIterator<TestDocument>("SELECT * FROM c WHERE c.name = 'Bob'");
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle().Which.Id.Should().Be("c3");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  1B. Read Item
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Handler_ReadItem_ReturnsDocument()
    {
        var doc = new TestDocument { Id = "r1", PartitionKey = "pk1", Name = "Alice", Value = 10 };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var response = await _container.ReadItemAsync<TestDocument>("r1", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Name.Should().Be("Alice");
        response.Resource.Value.Should().Be(10);
    }

    [Fact]
    public async Task Handler_ReadItem_NotFound_Throws404()
    {
        var act = () => _container.ReadItemAsync<TestDocument>("nonexistent", new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Handler_ReadItem_WithPartitionKey_ReturnsCorrectItem()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "r3", PartitionKey = "pkA", Name = "FromA" },
            new PartitionKey("pkA"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "r3", PartitionKey = "pkB", Name = "FromB" },
            new PartitionKey("pkB"));

        var responseA = await _container.ReadItemAsync<TestDocument>("r3", new PartitionKey("pkA"));
        var responseB = await _container.ReadItemAsync<TestDocument>("r3", new PartitionKey("pkB"));

        responseA.Resource.Name.Should().Be("FromA");
        responseB.Resource.Name.Should().Be("FromB");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  1C. Upsert Item
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Handler_UpsertItem_NewItem_ReturnsCreated()
    {
        var doc = new TestDocument { Id = "u1", PartitionKey = "pk1", Name = "Alice" };

        var response = await _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Resource.Id.Should().Be("u1");
    }

    [Fact]
    public async Task Handler_UpsertItem_ExistingItem_ReturnsOk()
    {
        var doc = new TestDocument { Id = "u2", PartitionKey = "pk1", Name = "Alice", Value = 10 };
        await _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

        doc.Name = "Updated";
        doc.Value = 99;
        var response = await _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Name.Should().Be("Updated");
        response.Resource.Value.Should().Be(99);
    }

    [Fact]
    public async Task Handler_UpsertItem_ThenQuery_ReturnsUpdatedItem()
    {
        var doc = new TestDocument { Id = "u3", PartitionKey = "pk1", Name = "Original" };
        await _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

        doc.Name = "Modified";
        await _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

        var results = new List<TestDocument>();
        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c WHERE c.id = 'u3'");
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle().Which.Name.Should().Be("Modified");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  1D. Replace Item
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Handler_ReplaceItem_ReturnsOk()
    {
        var doc = new TestDocument { Id = "rp1", PartitionKey = "pk1", Name = "Before", Value = 1 };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        doc.Name = "After";
        doc.Value = 2;
        var response = await _container.ReplaceItemAsync(doc, "rp1", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Name.Should().Be("After");
        response.Resource.Value.Should().Be(2);
    }

    [Fact]
    public async Task Handler_ReplaceItem_NotFound_Throws404()
    {
        var doc = new TestDocument { Id = "rp2", PartitionKey = "pk1", Name = "Ghost" };

        var act = () => _container.ReplaceItemAsync(doc, "rp2", new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Handler_ReplaceItem_WithETag_StaleETag_ThrowsPreconditionFailed()
    {
        var doc = new TestDocument { Id = "rp3", PartitionKey = "pk1", Name = "Original" };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        // Modify to change the ETag
        doc.Name = "Modified";
        await _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

        // Try to replace with a stale ETag
        doc.Name = "StaleReplace";
        var act = () => _container.ReplaceItemAsync(doc, "rp3", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  1E. Delete Item
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Handler_DeleteItem_ReturnsNoContent()
    {
        var doc = new TestDocument { Id = "d1", PartitionKey = "pk1", Name = "ToDelete" };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var response = await _container.DeleteItemAsync<TestDocument>("d1", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task Handler_DeleteItem_NotFound_Throws404()
    {
        var act = () => _container.DeleteItemAsync<TestDocument>("nonexistent", new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Handler_DeleteItem_ThenRead_Throws404()
    {
        var doc = new TestDocument { Id = "d3", PartitionKey = "pk1", Name = "Ephemeral" };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));
        await _container.DeleteItemAsync<TestDocument>("d3", new PartitionKey("pk1"));

        var act = () => _container.ReadItemAsync<TestDocument>("d3", new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  1F. Patch Item
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Handler_PatchItem_SetOperation_ReturnsOk()
    {
        var doc = new TestDocument { Id = "p1", PartitionKey = "pk1", Name = "Before", Value = 1 };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var response = await _container.PatchItemAsync<TestDocument>("p1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "After")]);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Name.Should().Be("After");
        response.Resource.Value.Should().Be(1); // unchanged
    }

    [Fact]
    public async Task Handler_PatchItem_MultipleOperations_AllApplied()
    {
        var doc = new TestDocument { Id = "p2", PartitionKey = "pk1", Name = "Start", Value = 10 };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var response = await _container.PatchItemAsync<TestDocument>("p2", new PartitionKey("pk1"),
        [
            PatchOperation.Set("/name", "Patched"),
            PatchOperation.Increment("/value", 5)
        ]);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Name.Should().Be("Patched");
        response.Resource.Value.Should().Be(15);
    }

    [Fact]
    public async Task Handler_PatchItem_NotFound_Throws404()
    {
        var act = () => _container.PatchItemAsync<TestDocument>("nonexistent", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Ghost")]);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Handler_PatchItem_WithFilterPredicate_MatchingCondition_Succeeds()
    {
        var doc = new TestDocument { Id = "p4", PartitionKey = "pk1", Name = "Conditional", Value = 100 };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var response = await _container.PatchItemAsync<TestDocument>("p4", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Updated")],
            new PatchItemRequestOptions { FilterPredicate = "FROM c WHERE c.value = 100" });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Name.Should().Be("Updated");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  1G. Integration / Multi-Container
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Handler_CrudThenLinqQuery_RoundTrip()
    {
        // Create items via CRUD
        await _container.CreateItemAsync(
            new TestDocument { Id = "lq1", PartitionKey = "pk1", Name = "Active", Value = 10 },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "lq2", PartitionKey = "pk1", Name = "Inactive", Value = 5 },
            new PartitionKey("pk1"));

        // Query via LINQ .ToFeedIterator() — the whole point of this feature
        var results = new List<TestDocument>();
        var iterator = _container.GetItemLinqQueryable<TestDocument>()
            .Where(d => d.Value > 7)
            .ToFeedIterator();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle().Which.Name.Should().Be("Active");
    }

    [Fact]
    public async Task Handler_MultiContainer_CrudIsolated()
    {
        var containerA = new InMemoryContainer("containerA", "/partitionKey");
        var containerB = new InMemoryContainer("containerB", "/partitionKey");

        using var handlerA = new FakeCosmosHandler(containerA);
        using var handlerB = new FakeCosmosHandler(containerB);

        var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
        {
            ["containerA"] = handlerA,
            ["containerB"] = handlerB,
        });

        using var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                HttpClientFactory = () => new HttpClient(router) { Timeout = TimeSpan.FromSeconds(10) }
            });

        var cA = client.GetContainer("db", "containerA");
        var cB = client.GetContainer("db", "containerB");

        // Create in A
        await cA.CreateItemAsync(
            new TestDocument { Id = "iso1", PartitionKey = "pk1", Name = "InA" },
            new PartitionKey("pk1"));

        // Read from A succeeds
        var readA = await cA.ReadItemAsync<TestDocument>("iso1", new PartitionKey("pk1"));
        readA.Resource.Name.Should().Be("InA");

        // Read from B fails — item doesn't exist there
        var act = () => cB.ReadItemAsync<TestDocument>("iso1", new PartitionKey("pk1"));
        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Handler_RequestLog_RecordsCrudOperations()
    {
        var doc = new TestDocument { Id = "log1", PartitionKey = "pk1", Name = "Logged" };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));
        await _container.ReadItemAsync<TestDocument>("log1", new PartitionKey("pk1"));
        await _container.DeleteItemAsync<TestDocument>("log1", new PartitionKey("pk1"));

        // POST for create, GET for read, DELETE for delete
        _handler.RequestLog.Should().Contain(e => e.StartsWith("POST") && e.Contains("/docs"));
        _handler.RequestLog.Should().Contain(e => e.StartsWith("GET") && e.Contains("/docs/"));
        _handler.RequestLog.Should().Contain(e => e.StartsWith("DELETE") && e.Contains("/docs/"));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  1H. SDK Compatibility
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Handler_VerifySdkCompatibility_IncludesCrudCheck()
    {
        var act = FakeCosmosHandler.VerifySdkCompatibilityAsync;

        await act.Should().NotThrowAsync();
    }
}
