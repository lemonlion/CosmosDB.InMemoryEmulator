using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Integration tests for GitHub issue #6: hierarchical partition key prefix scoping
/// not respected in queries via FakeCosmosHandler + CosmosClient path.
/// Parity-validated: runs against both in-memory and real emulator.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class FakeCosmosHandlerHierarchicalPkPrefixTests(EmulatorSession session) : IAsyncLifetime
{
    private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
    private Container _container = null!;

    public class HierarchicalDocument
    {
        [JsonProperty("id")] public string Id { get; set; } = "";
        [JsonProperty("tenantId")] public string TenantId { get; set; } = "";
        [JsonProperty("category")] public string Category { get; set; } = "";
        [JsonProperty("region")] public string Region { get; set; } = "";
        [JsonProperty("data")] public string Data { get; set; } = "";
    }

    public async ValueTask InitializeAsync()
    {
        _container = await _fixture.CreateContainerAsync(
            "test-hier-prefix",
            new[] { "/tenantId", "/category", "/region" });
    }

    public async ValueTask DisposeAsync()
    {
        await _fixture.DisposeAsync();
    }

    private static async Task<List<T>> DrainQuery<T>(Container container, QueryDefinition query, QueryRequestOptions? options = null)
    {
        var iterator = container.GetItemQueryIterator<T>(query, requestOptions: options);
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        return results;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Issue #6 reproduction: prefix PK scoping
    // ═══════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Issue #6 reproduction: querying with a 2-level prefix key (tenantId + category)
    /// should only return documents matching that prefix.
    /// </summary>
    [Fact]
    public async Task Query_WithTwoLevelPrefixKey_ScopesToMatchingDocumentsOnly()
    {
        var tenantId = "tenant-001";
        var region = "eu-west";

        await _container.CreateItemAsync(
            new HierarchicalDocument { Id = "id-1", TenantId = tenantId, Category = "CategoryA", Region = region, Data = "data A" },
            new PartitionKeyBuilder().Add(tenantId).Add("CategoryA").Add(region).Build());

        await _container.CreateItemAsync(
            new HierarchicalDocument { Id = "id-2", TenantId = tenantId, Category = "CategoryB", Region = region, Data = "data B" },
            new PartitionKeyBuilder().Add(tenantId).Add("CategoryB").Add(region).Build());

        // Query scoped to 2-level prefix (tenantId + "CategoryA")
        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add(tenantId).Add("CategoryA").Build()
        };

        var results = await DrainQuery<HierarchicalDocument>(
            _container, new QueryDefinition("SELECT * FROM c"), requestOptions);

        results.Should().HaveCount(1);
        results[0].Category.Should().Be("CategoryA");
    }

    /// <summary>
    /// Issue #6 reproduction: querying with a 1-level prefix key (tenantId only)
    /// should only return documents matching that tenant.
    /// </summary>
    [Fact]
    public async Task Query_WithOneLevelPrefixKey_ScopesToMatchingTenantOnly()
    {
        var region = "eu-west";

        await _container.CreateItemAsync(
            new HierarchicalDocument { Id = "id-a", TenantId = "tenant-A", Category = "CategoryA", Region = region, Data = "data A" },
            new PartitionKeyBuilder().Add("tenant-A").Add("CategoryA").Add(region).Build());

        await _container.CreateItemAsync(
            new HierarchicalDocument { Id = "id-b", TenantId = "tenant-B", Category = "CategoryA", Region = region, Data = "data B" },
            new PartitionKeyBuilder().Add("tenant-B").Add("CategoryA").Add(region).Build());

        // Query scoped to 1-level prefix (tenant-A only)
        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("tenant-A").Build()
        };

        var results = await DrainQuery<HierarchicalDocument>(
            _container, new QueryDefinition("SELECT * FROM c"), requestOptions);

        results.Should().HaveCount(1);
        results[0].TenantId.Should().Be("tenant-A");
    }

    /// <summary>
    /// Full partition key query (all 3 levels) should return only the exact match.
    /// </summary>
    [Fact]
    public async Task Query_WithFullPartitionKey_ReturnsExactMatch()
    {
        await _container.CreateItemAsync(
            new HierarchicalDocument { Id = "id-1", TenantId = "t1", Category = "CatA", Region = "eu", Data = "match" },
            new PartitionKeyBuilder().Add("t1").Add("CatA").Add("eu").Build());

        await _container.CreateItemAsync(
            new HierarchicalDocument { Id = "id-2", TenantId = "t1", Category = "CatA", Region = "us", Data = "nomatch" },
            new PartitionKeyBuilder().Add("t1").Add("CatA").Add("us").Build());

        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("t1").Add("CatA").Add("eu").Build()
        };

        var results = await DrainQuery<HierarchicalDocument>(
            _container, new QueryDefinition("SELECT * FROM c"), requestOptions);

        results.Should().HaveCount(1);
        results[0].Data.Should().Be("match");
    }

    /// <summary>
    /// Prefix PK with no matching documents should return empty results.
    /// </summary>
    [Fact]
    public async Task Query_WithPrefixKey_NoMatches_ReturnsEmpty()
    {
        await _container.CreateItemAsync(
            new HierarchicalDocument { Id = "id-1", TenantId = "t1", Category = "CatA", Region = "eu", Data = "data" },
            new PartitionKeyBuilder().Add("t1").Add("CatA").Add("eu").Build());

        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("nonexistent-tenant").Build()
        };

        var results = await DrainQuery<HierarchicalDocument>(
            _container, new QueryDefinition("SELECT * FROM c"), requestOptions);

        results.Should().BeEmpty();
    }

    /// <summary>
    /// 1-level prefix with multiple documents sharing that prefix should return all of them.
    /// </summary>
    [Fact]
    public async Task Query_WithOneLevelPrefix_ReturnsAllMatchingDocuments()
    {
        await _container.CreateItemAsync(
            new HierarchicalDocument { Id = "id-1", TenantId = "t1", Category = "CatA", Region = "eu", Data = "d1" },
            new PartitionKeyBuilder().Add("t1").Add("CatA").Add("eu").Build());

        await _container.CreateItemAsync(
            new HierarchicalDocument { Id = "id-2", TenantId = "t1", Category = "CatB", Region = "us", Data = "d2" },
            new PartitionKeyBuilder().Add("t1").Add("CatB").Add("us").Build());

        await _container.CreateItemAsync(
            new HierarchicalDocument { Id = "id-3", TenantId = "t2", Category = "CatA", Region = "eu", Data = "d3" },
            new PartitionKeyBuilder().Add("t2").Add("CatA").Add("eu").Build());

        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("t1").Build()
        };

        var results = await DrainQuery<HierarchicalDocument>(
            _container, new QueryDefinition("SELECT * FROM c"), requestOptions);

        results.Should().HaveCount(2);
        results.Select(r => r.TenantId).Should().AllBe("t1");
    }
}
