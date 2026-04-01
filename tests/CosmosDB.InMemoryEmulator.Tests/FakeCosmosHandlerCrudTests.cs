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

    // ═══════════════════════════════════════════════════════════════════════════
    //  1I. Edge Cases
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Handler_FaultInjection_ThrottlesCreateRequest()
    {
        _handler.FaultInjector = _ =>
            new HttpResponseMessage((HttpStatusCode)429)
            {
                Headers = { RetryAfter = new System.Net.Http.Headers.RetryConditionHeaderValue(TimeSpan.FromMilliseconds(1)) }
            };
        _handler.FaultInjectorIncludesMetadata = false;

        var doc = new TestDocument { Id = "fi1", PartitionKey = "pk1", Name = "Throttled" };
        var act = () => _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be((HttpStatusCode)429);
    }

    [Fact]
    public async Task Handler_PatchItem_WithFilterPredicate_NonMatchingCondition_ThrowsPreconditionFailed()
    {
        var doc = new TestDocument { Id = "p5", PartitionKey = "pk1", Name = "Conditional", Value = 50 };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var act = () => _container.PatchItemAsync<TestDocument>("p5", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Updated")],
            new PatchItemRequestOptions { FilterPredicate = "FROM c WHERE c.value > 100" });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task Handler_ReadItem_WithUrlEncodedId_Succeeds()
    {
        var id = "doc with spaces";
        var doc = new TestDocument { Id = id, PartitionKey = "pk1", Name = "Encoded" };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var response = await _container.ReadItemAsync<TestDocument>(id, new PartitionKey("pk1"));

        response.Resource.Name.Should().Be("Encoded");
        response.Resource.Id.Should().Be(id);
    }

    [Fact]
    public async Task Handler_Crud_WithCompositePartitionKey_RoundTrip()
    {
        var compositeContainer = new InMemoryContainer("composite-test", ["/tenantId", "/userId"]);
        using var compositeHandler = new FakeCosmosHandler(compositeContainer);
        using var compositeClient = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                HttpClientFactory = () => new HttpClient(compositeHandler) { Timeout = TimeSpan.FromSeconds(10) }
            });
        var container = compositeClient.GetContainer("db", "composite-test");

        var pk = new PartitionKeyBuilder().Add("t1").Add("u1").Build();
        var doc = new { id = "cpk1", tenantId = "t1", userId = "u1", name = "Composite" };
        var createResponse = await container.CreateItemAsync(doc, pk);
        createResponse.StatusCode.Should().Be(HttpStatusCode.Created);

        var readResponse = await container.ReadItemAsync<dynamic>("cpk1", pk);
        ((string)readResponse.Resource.name).Should().Be("Composite");

        await container.DeleteItemAsync<dynamic>("cpk1", pk);
        var act = () => container.ReadItemAsync<dynamic>("cpk1", pk);
        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  A1. Special Character IDs
    // ═══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("a.b.c")]
    [InlineData("hello world")]
    [InlineData("日本語")]
    [InlineData("a+b")]
    public async Task Handler_CreateItem_WithSpecialCharactersInId_Succeeds(string id)
    {
        var doc = new TestDocument { Id = id, PartitionKey = "pk1", Name = "Special" };
        var createResponse = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));
        createResponse.StatusCode.Should().Be(HttpStatusCode.Created);

        var readResponse = await _container.ReadItemAsync<TestDocument>(id, new PartitionKey("pk1"));
        readResponse.Resource.Id.Should().Be(id);
        readResponse.Resource.Name.Should().Be("Special");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  A2. Replace with matching ETag
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Handler_ReplaceItem_WithMatchingETag_Succeeds()
    {
        var doc = new TestDocument { Id = "etag-rp1", PartitionKey = "pk1", Name = "Original" };
        var createResponse = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));
        var etag = createResponse.ETag;

        doc.Name = "Updated";
        var replaceResponse = await _container.ReplaceItemAsync(doc, "etag-rp1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = etag });

        replaceResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        replaceResponse.Resource.Name.Should().Be("Updated");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  A3/A4. Delete with ETag
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Handler_DeleteItem_WithStaleETag_ThrowsPreconditionFailed()
    {
        var doc = new TestDocument { Id = "etag-d1", PartitionKey = "pk1", Name = "Original" };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        // Modify to change the ETag
        doc.Name = "Modified";
        await _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

        var act = () => _container.DeleteItemAsync<TestDocument>("etag-d1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task Handler_DeleteItem_WithMatchingETag_Succeeds()
    {
        var doc = new TestDocument { Id = "etag-d2", PartitionKey = "pk1", Name = "ToDelete" };
        var createResponse = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));
        var etag = createResponse.ETag;

        var deleteResponse = await _container.DeleteItemAsync<TestDocument>("etag-d2", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = etag });

        deleteResponse.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  A5/A6. Patch Remove + Add operations
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Handler_PatchItem_RemoveOperation_Succeeds()
    {
        var doc = new TestDocument { Id = "prm1", PartitionKey = "pk1", Name = "HasName", Value = 42 };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var response = await _container.PatchItemAsync<TestDocument>("prm1", new PartitionKey("pk1"),
            [PatchOperation.Remove("/name")]);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Name.Should().BeNull();
        response.Resource.Value.Should().Be(42); // unchanged
    }

    [Fact]
    public async Task Handler_PatchItem_AddOperation_Succeeds()
    {
        var doc = new TestDocument { Id = "padd1", PartitionKey = "pk1", Name = "Before" };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var response = await _container.PatchItemAsync<TestDocument>("padd1", new PartitionKey("pk1"),
            [PatchOperation.Add("/tags", new[] { "alpha", "beta" })]);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Tags.Should().Contain("alpha");
        response.Resource.Tags.Should().Contain("beta");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  A7-A10. ETag in CRUD responses
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Handler_CreateItem_ReturnsETagInResponse()
    {
        var doc = new TestDocument { Id = "etag-c1", PartitionKey = "pk1", Name = "Alice" };

        var response = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        response.ETag.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task Handler_UpsertItem_ReturnsETagInResponse()
    {
        var doc = new TestDocument { Id = "etag-u1", PartitionKey = "pk1", Name = "Alice" };

        var response = await _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

        response.ETag.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task Handler_ReplaceItem_ReturnsUpdatedETag()
    {
        var doc = new TestDocument { Id = "etag-rp2", PartitionKey = "pk1", Name = "Before" };
        var createResponse = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));
        var originalETag = createResponse.ETag;

        doc.Name = "After";
        var replaceResponse = await _container.ReplaceItemAsync(doc, "etag-rp2", new PartitionKey("pk1"));

        replaceResponse.ETag.Should().NotBeNullOrEmpty();
        replaceResponse.ETag.Should().NotBe(originalETag);
    }

    [Fact]
    public async Task Handler_ReadItem_ReturnsETagInResponse()
    {
        var doc = new TestDocument { Id = "etag-r1", PartitionKey = "pk1", Name = "Alice" };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var response = await _container.ReadItemAsync<TestDocument>("etag-r1", new PartitionKey("pk1"));

        response.ETag.Should().NotBeNullOrEmpty();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  B3. PartitionKey.None round-trip
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Handler_ReadItem_WithPartitionKeyNone_Succeeds()
    {
        var pkNoneContainer = new InMemoryContainer("pknone-test", "/partitionKey");
        using var pkNoneHandler = new FakeCosmosHandler(pkNoneContainer);
        using var pkNoneClient = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                HttpClientFactory = () => new HttpClient(pkNoneHandler) { Timeout = TimeSpan.FromSeconds(10) }
            });
        var container = pkNoneClient.GetContainer("db", "pknone-test");

        var doc = new { id = "none1", name = "NoPK" };
        await container.CreateItemAsync(doc, PartitionKey.None);

        var response = await container.ReadItemAsync<dynamic>("none1", PartitionKey.None);
        ((string)response.Resource.name).Should().Be("NoPK");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  D1-D3. Response headers on CRUD operations
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Handler_CrudResponse_ContainsRequestCharge()
    {
        var doc = new TestDocument { Id = "hdr1", PartitionKey = "pk1", Name = "Alice" };

        var response = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        response.RequestCharge.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task Handler_CrudResponse_ContainsActivityId()
    {
        var doc = new TestDocument { Id = "hdr2", PartitionKey = "pk1", Name = "Alice" };

        var response = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        response.ActivityId.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task Handler_CrudResponse_ContainsSessionToken()
    {
        var doc = new TestDocument { Id = "hdr3", PartitionKey = "pk1", Name = "Alice" };

        var response = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        response.Headers["x-ms-session-token"].Should().NotBeNullOrEmpty();
    }
}
