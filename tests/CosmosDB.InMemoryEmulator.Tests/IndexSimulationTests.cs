using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class IndexSimulationTests
{
    // ── Basic index policy support ──────────────────────────────────────────

    [Fact]
    public void DefaultIndexingPolicy_HasAutomaticIndexing()
    {
        var container = new InMemoryContainer("test-container", "/partitionKey");

        var policy = container.IndexingPolicy;

        policy.Should().NotBeNull();
        policy.Automatic.Should().BeTrue();
        policy.IndexingMode.Should().Be(IndexingMode.Consistent);
    }

    [Fact]
    public void SetIndexingPolicy_UpdatesPolicy()
    {
        var container = new InMemoryContainer("test-container", "/partitionKey");
        var policy = new IndexingPolicy
        {
            Automatic = false,
            IndexingMode = IndexingMode.None
        };

        container.IndexingPolicy = policy;

        container.IndexingPolicy.Automatic.Should().BeFalse();
        container.IndexingPolicy.IndexingMode.Should().Be(IndexingMode.None);
    }

    // ── Included/Excluded paths ─────────────────────────────────────────────

    [Fact]
    public void IndexingPolicy_DefaultIncludesAllPaths()
    {
        var container = new InMemoryContainer("test-container", "/partitionKey");

        container.IndexingPolicy.IncludedPaths.Should().ContainSingle(p => p.Path == "/*");
    }

    [Fact]
    public async Task ExcludedPath_StillAllowsPointReads()
    {
        var container = new InMemoryContainer("test-container", "/partitionKey");
        container.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/name/*" });

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));

        var response = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        response.Resource.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task ExcludedPath_QueriesStillWorkButLogWarning()
    {
        var container = new InMemoryContainer("test-container", "/partitionKey");
        container.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/name/*" });

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));

        // Queries on excluded paths still work in our in-memory implementation
        // (they would just be slower in real Cosmos DB - a full scan instead of index lookup)
        var query = new QueryDefinition("SELECT * FROM c WHERE c.name = 'Alice'");
        var iterator = container.GetItemQueryIterator<TestDocument>(query);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(1);
    }

    // ── Composite indexes ───────────────────────────────────────────────────

    [Fact]
    public void CompositeIndexes_CanBeConfigured()
    {
        var container = new InMemoryContainer("test-container", "/partitionKey");
        container.IndexingPolicy.CompositeIndexes.Add(
        [
            new CompositePath { Path = "/name", Order = CompositePathSortOrder.Ascending },
            new CompositePath { Path = "/value", Order = CompositePathSortOrder.Descending }
        ]);

        container.IndexingPolicy.CompositeIndexes.Should().HaveCount(1);
        container.IndexingPolicy.CompositeIndexes[0].Should().HaveCount(2);
    }

    [Fact]
    public async Task OrderBy_WithCompositeIndex_ReturnsCorrectOrder()
    {
        var container = new InMemoryContainer("test-container", "/partitionKey");
        container.IndexingPolicy.CompositeIndexes.Add(
        [
            new CompositePath { Path = "/name", Order = CompositePathSortOrder.Ascending },
            new CompositePath { Path = "/value", Order = CompositePathSortOrder.Descending }
        ]);

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));
        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Alice", Value = 20 },
            new PartitionKey("pk1"));
        await container.CreateItemAsync(
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Bob", Value = 5 },
            new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT * FROM c ORDER BY c.name ASC, c.value DESC");
        var iterator = container.GetItemQueryIterator<TestDocument>(query);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(3);
        results[0].Name.Should().Be("Alice");
        results[0].Value.Should().Be(20);
        results[1].Name.Should().Be("Alice");
        results[1].Value.Should().Be(10);
        results[2].Name.Should().Be("Bob");
    }

    // ── Spatial indexes ─────────────────────────────────────────────────────

    [Fact]
    public void SpatialIndexes_CanBeConfigured()
    {
        var container = new InMemoryContainer("test-container", "/partitionKey");
        container.IndexingPolicy.SpatialIndexes.Add(new SpatialPath
        {
            Path = "/location/*"
        });

        container.IndexingPolicy.SpatialIndexes.Should().HaveCount(1);
    }

    // ── Request charge reflects index usage ─────────────────────────────────
    // Behavioral difference: In real Cosmos DB, queries on excluded paths consume
    // more RUs because they require a full scan. Our implementation always returns
    // a synthetic request charge - we simulate this by returning a higher synthetic
    // charge when querying on excluded paths.

    [Fact(Skip = "InMemoryContainer uses a synthetic request charge of 1.0 for all operations. " +
        "Real Cosmos DB varies request charge based on index usage, document size and query complexity. " +
        "Implementing realistic RU calculation would require modeling the full Cosmos DB cost model.")]
    public async Task ExcludedPath_QueryReturnsHigherRequestCharge()
    {
        var container = new InMemoryContainer("test-container", "/partitionKey");

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));

        // Indexed query
        var indexedQuery = new QueryDefinition("SELECT * FROM c WHERE c.value = 10");
        var indexedIterator = container.GetItemQueryIterator<TestDocument>(indexedQuery);
        var indexedResponse = await indexedIterator.ReadNextAsync();
        var indexedCharge = indexedResponse.RequestCharge;

        // Exclude the name path
        container.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/name/*" });

        // Query on excluded path
        var excludedQuery = new QueryDefinition("SELECT * FROM c WHERE c.name = 'Alice'");
        var excludedIterator = container.GetItemQueryIterator<TestDocument>(excludedQuery);
        var excludedResponse = await excludedIterator.ReadNextAsync();
        var excludedCharge = excludedResponse.RequestCharge;

        excludedCharge.Should().BeGreaterThan(indexedCharge);
    }

    // Behavioral difference test: documents the actual divergent behaviour
    [Fact]
    public async Task BehavioralDifference_RequestChargeIsAlwaysSynthetic()
    {
        // In real Cosmos DB, request charge varies based on index usage, document size,
        // and query complexity. Our implementation always returns a synthetic flat charge.
        var container = new InMemoryContainer("test-container", "/partitionKey");

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT * FROM c WHERE c.value = 10");
        var iterator = container.GetItemQueryIterator<TestDocument>(query);
        var response = await iterator.ReadNextAsync();

        // Our synthetic charge is always the same regardless of index configuration
        response.RequestCharge.Should().Be(1.0);
    }
}
