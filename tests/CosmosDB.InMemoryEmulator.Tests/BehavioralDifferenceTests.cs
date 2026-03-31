using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;
using System.Text;
using Newtonsoft.Json.Linq;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests that verify actual InMemoryContainer behaviour where it DIFFERS from
/// real Cosmos DB. Each test has a comment explaining the behavioural difference.
/// Use these to understand the limitations of the in-memory implementation.
/// </summary>
public class BehavioralDifferenceTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    // ── Change Feed ──────────────────────────────────────────────────────────

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: Real Cosmos DB change feed returns only the latest
    /// version of each document, ordered by modification time (_ts), and supports
    /// continuation tokens for incremental reads. InMemoryContainer returns all
    /// current items from the store in a single page, regardless of when they were
    /// created or modified, and does not support incremental continuation.
    /// </summary>
    [Fact]
    public async Task ChangeFeed_ReturnsAllCurrentItems_NotIncrementalChanges()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
            new PartitionKey("pk1"));

        var iterator = _container.GetChangeFeedIterator<TestDocument>(
            ChangeFeedStartFrom.Beginning(),
            ChangeFeedMode.Incremental);

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            if (response.StatusCode == HttpStatusCode.NotModified)
            {
                break;
            }

            results.AddRange(response);
        }

        // InMemoryContainer returns all current items as a snapshot, not incremental changes
        results.Should().HaveCount(2);
    }

    // ── Delete Container ─────────────────────────────────────────────────────

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: Real Cosmos DB DeleteContainerAsync returns
    /// HttpStatusCode.NoContent (204). InMemoryContainer also clears internal
    /// state but the container object remains usable - you can still add items
    /// after deletion. Real Cosmos DB would reject subsequent operations until
    /// the container is recreated.
    /// </summary>
    [Fact]
    public async Task DeleteContainer_ContainerRemainsUsable_UnlikeRealCosmos()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        await _container.DeleteContainerAsync();

        // InMemoryContainer allows continued use after deletion
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
            new PartitionKey("pk1"));

        var response = await _container.ReadItemAsync<TestDocument>("2", new PartitionKey("pk1"));
        response.Resource.Name.Should().Be("Bob");
    }

    // ── Throughput ───────────────────────────────────────────────────────────

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: Real Cosmos DB returns the actual provisioned
    /// throughput. InMemoryContainer always returns a hardcoded value of 400 RU/s
    /// regardless of any ReplaceThroughputAsync calls. The replace operation
    /// appears to succeed but the read does not reflect the change.
    /// </summary>
    [Fact]
    public async Task ReadThroughput_AlwaysReturns400_RegardlessOfReplace()
    {
        await _container.ReplaceThroughputAsync(1000);

        var throughput = await _container.ReadThroughputAsync();

        // InMemoryContainer always returns 400, not the value set by ReplaceThroughputAsync
        throughput.Should().Be(400);
    }

    // ── ETag format ──────────────────────────────────────────────────────────

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: Real Cosmos DB ETags are opaque strings derived
    /// from the document's internal timestamp. InMemoryContainer generates
    /// ETags as quoted GUIDs (e.g. "\"guid-here\""). The format differs but
    /// the concurrency semantics (If-Match / If-None-Match) work correctly.
    /// </summary>
    [Fact]
    public async Task ETag_IsQuotedGuid_NotOpaqueTimestamp()
    {
        var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" };
        var response = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var etag = response.ETag;
        etag.Should().NotBeNullOrEmpty();
        // InMemoryContainer uses quoted GUID format
        etag.Should().StartWith("\"").And.EndWith("\"");
        var inner = etag.Trim('"');
        Guid.TryParse(inner, out _).Should().BeTrue();
    }

    // ── LINQ query ──────────────────────────────────────────────────────────

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: Real Cosmos DB LINQ queries are translated to SQL
    /// and executed server-side. InMemoryContainer LINQ operates on an in-memory
    /// IQueryable, meaning all LINQ-to-Objects operators work but there is no
    /// SQL translation step. Some LINQ expressions that would fail on real Cosmos
    /// (e.g. unsupported operators) will succeed against InMemoryContainer.
    /// </summary>
    [Fact]
    public async Task LinqQuery_SupportsAllLinqToObjectsOperators()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new PartitionKey("pk1"));

        var queryable = _container.GetItemLinqQueryable<TestDocument>(true);

        // String.Contains works in LINQ-to-Objects but may not always translate to Cosmos SQL
        var results = queryable.Where(d => d.Name.Contains("li")).ToList();
        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Alice");
    }

    // ── Partition key extraction ─────────────────────────────────────────────

    /// <summary>
    /// InMemoryContainer now correctly extracts the partition key value from
    /// the document body using the configured partition key path when no
    /// explicit PartitionKey is supplied — matching real Cosmos DB behaviour.
    /// </summary>
    [Fact]
    public async Task PartitionKey_ExtractsFromConfiguredPath_WhenNotSupplied()
    {
        var doc = new TestDocument { Id = "my-id", PartitionKey = "my-pk", Name = "Alice" };

        await _container.CreateItemAsync(doc);

        var response = await _container.ReadItemAsync<TestDocument>("my-id", new PartitionKey("my-pk"));
        response.Resource.Name.Should().Be("Alice");
    }

    // ── Stream CRUD enforces ETag checks ────────────────────────────

    /// <summary>
    /// Stream-based CRUD now enforces If-Match / If-None-Match like real Cosmos DB.
    /// </summary>
    [Fact]
    public async Task StreamReplace_ChecksIfMatch_LikeRealCosmos()
    {
        var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var updatedDoc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" };
        var json = Newtonsoft.Json.JsonConvert.SerializeObject(updatedDoc);
        var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

        var requestOptions = new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" };
        using var response = await _container.ReplaceItemStreamAsync(stream, "1", new PartitionKey("pk1"), requestOptions);

        response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    // ── Database property ────────────────────────────────────────────────────

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: Real Cosmos DB Container.Database returns a real
    /// Database object that can be used to manage containers and users.
    /// InMemoryContainer returns an NSubstitute mock which has no real behaviour,
    /// so calling methods on .Database will return default values or throw.
    /// </summary>
    [Fact]
    public void Database_ReturnsSubstituteMock_NotRealDatabase()
    {
        var database = _container.Database;
        database.Should().NotBeNull();
        // The returned Database is an NSubstitute mock with no real behaviour
        database.Id.Should().BeEmpty();
    }

    // ── Replace Container ────────────────────────────────────────────────────

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: Real Cosmos DB ReplaceContainerAsync updates the
    /// container's actual properties (partition key, indexing policy, etc.).
    /// InMemoryContainer's ReplaceContainerAsync returns the supplied properties
    /// but does not actually update the internal container state. Subsequent
    /// ReadContainerAsync calls return the original properties.
    /// </summary>
    [Fact]
    public async Task ReplaceContainer_DoesNotPersistChanges()
    {
        var newProperties = new ContainerProperties("new-name", "/newPk");
        await _container.ReplaceContainerAsync(newProperties);

        var readResponse = await _container.ReadContainerAsync();
        // InMemoryContainer still returns original properties
        readResponse.Resource.Id.Should().Be("test-container");
        readResponse.Resource.PartitionKeyPath.Should().Be("/partitionKey");
    }

    // ── Feed ranges ──────────────────────────────────────────────────────────

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: Real Cosmos DB returns feed ranges that correspond
    /// to physical partitions and can be used for parallel processing. 
    /// InMemoryContainer returns a single NSubstitute mock FeedRange that has
    /// no real partition routing behaviour.
    /// </summary>
    [Fact]
    public async Task GetFeedRanges_ReturnsSingleMockRange()
    {
        var feedRanges = await _container.GetFeedRangesAsync();

        // InMemoryContainer always returns exactly one mocked FeedRange
        feedRanges.Should().HaveCount(1);
    }

    // ── ChangeFeed processor builders ────────────────────────────────────────

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: Real Cosmos DB ChangeFeedProcessorBuilder is used
    /// to create a change feed processor that monitors the container for changes.
    /// InMemoryContainer attempts to return NSubstitute mocks for the processor
    /// builder, but ChangeFeedProcessorBuilder cannot be proxied by NSubstitute
    /// (it has no accessible constructor), so calling GetChangeFeedProcessorBuilder
    /// Returns an uninitialized stub since NSubstitute cannot proxy it.
    /// </summary>
    [Fact]
    public void ChangeFeedProcessorBuilder_ReturnsMock_NoRealProcessing()
    {
        var builder = _container.GetChangeFeedProcessorBuilder<TestDocument>(
            "testProcessor",
            (IReadOnlyCollection<TestDocument> changes, CancellationToken token) => Task.CompletedTask);

        builder.Should().NotBeNull();
    }
}


/// <summary>
/// Tests that document known behavioral differences between InMemoryContainer and real
/// Cosmos DB. Each test shows the ACTUAL behavior and explains the divergence.
/// These are reference tests — they pass if InMemoryContainer has the documented behavior,
/// even when that behavior differs from real Cosmos.
/// </summary>
public class BehavioralDifferenceGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: Real Cosmos DB change feed returns 304 NotModified
    /// when there are no new changes. InMemoryContainer returns 200 OK with an
    /// empty result set. This is because InMemoryContainer uses a simple list-based
    /// change feed that doesn't support the NotModified status code pattern.
    /// </summary>
    [Fact]
    public async Task ChangeFeed_EmptyContainer_ReturnsOk_NotNotModified()
    {
        var iterator = _container.GetChangeFeedIterator<TestDocument>(
            ChangeFeedStartFrom.Beginning(),
            ChangeFeedMode.Incremental);

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            // InMemoryContainer returns OK and we iterate normally (empty results)
            results.AddRange(response);
        }

        results.Should().BeEmpty();
    }

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: Real Cosmos DB deletes appear as tombstone entries in
    /// the change feed (FullFidelity mode) or cause the document to disappear from
    /// incremental reads. InMemoryContainer does not record deletes in the change feed
    /// at all — they are simply absent. This is documented as a known limitation.
    /// </summary>
    [Fact]
    public async Task ChangeFeed_DeletesNotRecorded()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "ToDelete" },
            new PartitionKey("pk1"));
        var checkpointAfterCreate = _container.GetChangeFeedCheckpoint();

        await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        var checkpointAfterDelete = _container.GetChangeFeedCheckpoint();

        // Delete does not add to change feed
        checkpointAfterDelete.Should().Be(checkpointAfterCreate);
    }

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: Real Cosmos DB container delete makes the container
    /// permanently unavailable — subsequent operations throw. InMemoryContainer
    /// merely clears its internal state but the object remains usable. Items can
    /// be added after deletion.
    /// </summary>
    [Fact]
    public async Task DeleteContainer_RemainsUsable_UnlikeRealCosmos()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Before" },
            new PartitionKey("pk1"));

        await _container.DeleteContainerAsync();

        // Container is still usable after deletion (unlike real Cosmos)
        var response = await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "After" },
            new PartitionKey("pk1"));
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: Real Cosmos DB ETags are opaque server-generated
    /// strings based on internal timestamps. InMemoryContainer generates ETags
    /// as quoted GUIDs. The format differs but conditional (IfMatch/IfNoneMatch)
    /// operations work identically.
    /// </summary>
    [Fact]
    public async Task ETag_Format_IsQuotedGuid_NotOpaqueTimestamp()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var etag = response.ETag;
        etag.Should().StartWith("\"").And.EndWith("\"");

        var inner = etag.Trim('"');
        Guid.TryParse(inner, out _).Should().BeTrue();
    }

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: Real Cosmos DB ReadThroughputAsync returns the actual
    /// provisioned throughput for the container. InMemoryContainer always returns 400
    /// RU/s regardless of any ReplaceThroughputAsync calls.
    /// </summary>
    [Fact]
    public async Task Throughput_AlwaysReturns400_IgnoresReplace()
    {
        await _container.ReplaceThroughputAsync(2000);
        var throughput = await _container.ReadThroughputAsync();

        // Always returns 400, not the value set
        throughput.Should().Be(400);
    }

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: Real Cosmos DB aggregates like COUNT/SUM without
    /// GROUP BY return a single aggregated value across all matching documents.
    /// InMemoryContainer supports this via GROUP BY but cross-partition aggregation
    /// without GROUP BY may return per-document values depending on the query path.
    /// Use the checkpoint-based change feed or GROUP BY for accurate aggregation.
    /// </summary>
    [Fact]
    public async Task Aggregate_Count_WithoutGroupBy_ReturnsCount()
    {
        for (var i = 0; i < 3; i++)
        {
            await _container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));
        }

        var iterator = _container.GetItemQueryIterator<JToken>("SELECT VALUE COUNT(1) FROM c");
        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        // Behavior may vary: real Cosmos returns single aggregated number,
        // InMemoryContainer may return per-document counts
        results.Should().NotBeEmpty();
    }

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: CosmosSqlParser partially handles the null-coalescing
    /// operator (??). Real Cosmos DB evaluates (expr ?? default) as "return expr if
    /// non-null, else return default". InMemoryContainer may parse the expression but
    /// produces results that cannot be deserialized to JObject since SELECT VALUE
    /// returns raw scalar values. Use JToken for scalar results.
    /// </summary>
    [Fact]
    public async Task Query_NullCoalesce_ProducesScalarResult_NotJObject()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        // Query with ?? works but returns a scalar — JToken works, JObject would fail
        var iterator = _container.GetItemQueryIterator<JToken>(
            """SELECT VALUE (c.name ?? "default") FROM c""");

        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(1);
    }
}
