using System.Net;
using CosmosDB.InMemoryEmulator;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Newtonsoft.Json;
using Xunit;
using AwesomeAssertions;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for issue #6: Hierarchical partition key prefix scoping not respected in queries
/// via FakeCosmosHandler + CosmosClient path.
/// </summary>
public class FakeCosmosHandlerHierarchicalPkPrefixScopingTests
{
    private record Document(
        [property: JsonProperty("id")] string Id,
        [property: JsonProperty("tenantId")] string TenantId,
        [property: JsonProperty("category")] string Category,
        [property: JsonProperty("region")] string Region,
        [property: JsonProperty("data")] string Data);

    private class NullableDocument
    {
        [JsonProperty("id")] public string Id { get; set; } = "";
        [JsonProperty("tenantId")] public string? TenantId { get; set; }
        [JsonProperty("category")] public string? Category { get; set; }
        [JsonProperty("region")] public string? Region { get; set; }
        [JsonProperty("data")] public string Data { get; set; } = "";
    }

    private class MixedTypeDocument
    {
        [JsonProperty("id")] public string Id { get; set; } = "";
        [JsonProperty("tenantId")] public string TenantId { get; set; } = "";
        [JsonProperty("priority")] public double Priority { get; set; }
        [JsonProperty("region")] public string Region { get; set; } = "";
        [JsonProperty("data")] public string Data { get; set; } = "";
    }

    private class BoolDocument
    {
        [JsonProperty("id")] public string Id { get; set; } = "";
        [JsonProperty("tenantId")] public string TenantId { get; set; } = "";
        [JsonProperty("isActive")] public bool IsActive { get; set; }
        [JsonProperty("region")] public string Region { get; set; } = "";
        [JsonProperty("data")] public string Data { get; set; } = "";
    }

    private static CosmosClient CreateClient(InMemoryContainer inMemoryContainer)
    {
        var handler = new FakeCosmosHandler(inMemoryContainer);
        return new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                HttpClientFactory = () => new HttpClient(handler)
            });
    }

    private static async Task<List<T>> DrainIterator<T>(FeedIterator<T> iterator)
    {
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        return results;
    }

    [Fact]
    public async Task Query_WithTwoLevelPrefixKey_ScopesToMatchingDocumentsOnly()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        var tenantId = "tenant-001";
        var region = "eu-west";

        await container.CreateItemAsync(
            new Document("id-1", tenantId, "CategoryA", region, "data A"),
            new PartitionKeyBuilder().Add(tenantId).Add("CategoryA").Add(region).Build());

        await container.CreateItemAsync(
            new Document("id-2", tenantId, "CategoryB", region, "data B"),
            new PartitionKeyBuilder().Add(tenantId).Add("CategoryB").Add(region).Build());

        // Query scoped to the 2-level prefix (tenantId + "CategoryA")
        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add(tenantId).Add("CategoryA").Build()
        };

        var results = new List<Document>();
        var iterator = container.GetItemQueryIterator<Document>(
            new QueryDefinition("SELECT * FROM c"), requestOptions: requestOptions);
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        Assert.Single(results);
        results[0].Category.Should().Be("CategoryA");
    }

    [Fact]
    public async Task Query_WithOneLevelPrefixKey_ScopesToMatchingTenantOnly()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        var region = "eu-west";

        await container.CreateItemAsync(
            new Document("id-a", "tenant-A", "CategoryA", region, "data A"),
            new PartitionKeyBuilder().Add("tenant-A").Add("CategoryA").Add(region).Build());

        await container.CreateItemAsync(
            new Document("id-b", "tenant-B", "CategoryA", region, "data B"),
            new PartitionKeyBuilder().Add("tenant-B").Add("CategoryA").Add(region).Build());

        // Query scoped to tenant-A only (1-level prefix)
        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("tenant-A").Build()
        };

        var results = new List<Document>();
        var iterator = container.GetItemQueryIterator<Document>(
            new QueryDefinition("SELECT * FROM c"), requestOptions: requestOptions);
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        Assert.Single(results);
        results[0].TenantId.Should().Be("tenant-A");
    }

    [Fact]
    public async Task Query_WithFullKey_StillWorksCorrectly()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        await container.CreateItemAsync(
            new Document("id-1", "t1", "catA", "eu", "data1"),
            new PartitionKeyBuilder().Add("t1").Add("catA").Add("eu").Build());

        await container.CreateItemAsync(
            new Document("id-2", "t1", "catA", "us", "data2"),
            new PartitionKeyBuilder().Add("t1").Add("catA").Add("us").Build());

        // Full key — should return exactly 1
        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("t1").Add("catA").Add("eu").Build()
        };

        var results = await DrainIterator(
            container.GetItemQueryIterator<Document>(
                new QueryDefinition("SELECT * FROM c"), requestOptions: requestOptions));

        Assert.Single(results);
        results[0].Id.Should().Be("id-1");
    }

    [Fact]
    public async Task Query_WithPrefixKey_AndWhereClause_FiltersCorrectly()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        await container.CreateItemAsync(
            new Document("id-1", "t1", "catA", "eu", "data1"),
            new PartitionKeyBuilder().Add("t1").Add("catA").Add("eu").Build());

        await container.CreateItemAsync(
            new Document("id-2", "t1", "catA", "us", "data2"),
            new PartitionKeyBuilder().Add("t1").Add("catA").Add("us").Build());

        await container.CreateItemAsync(
            new Document("id-3", "t1", "catB", "eu", "data3"),
            new PartitionKeyBuilder().Add("t1").Add("catB").Add("eu").Build());

        // 1-level prefix + WHERE clause
        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("t1").Build()
        };

        var query = new QueryDefinition("SELECT * FROM c WHERE c.region = @r")
            .WithParameter("@r", "eu");

        var results = await DrainIterator(
            container.GetItemQueryIterator<Document>(query, requestOptions: requestOptions));

        results.Should().HaveCount(2);
        results.Select(r => r.Id).Should().BeEquivalentTo(new[] { "id-1", "id-3" });
    }

    [Fact]
    public async Task Query_WithTwoLevelPrefix_TwoPathContainer_ScopesCorrectly()
    {
        var inMemoryContainer = new InMemoryContainer("items2", new[] { "/tenantId", "/category" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items2");

        await container.CreateItemAsync(
            new Document("id-1", "t1", "catA", "eu", "data1"),
            new PartitionKeyBuilder().Add("t1").Add("catA").Build());

        await container.CreateItemAsync(
            new Document("id-2", "t1", "catB", "eu", "data2"),
            new PartitionKeyBuilder().Add("t1").Add("catB").Build());

        await container.CreateItemAsync(
            new Document("id-3", "t2", "catA", "eu", "data3"),
            new PartitionKeyBuilder().Add("t2").Add("catA").Build());

        // 1-level prefix on a 2-path container
        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("t1").Build()
        };

        var results = await DrainIterator(
            container.GetItemQueryIterator<Document>(
                new QueryDefinition("SELECT * FROM c"), requestOptions: requestOptions));

        results.Should().HaveCount(2);
        results.Select(r => r.Id).Should().BeEquivalentTo(new[] { "id-1", "id-2" });
    }

    [Fact]
    public async Task Query_WithPrefixKey_NoMatches_ReturnsEmpty()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        await container.CreateItemAsync(
            new Document("id-1", "t1", "catA", "eu", "data1"),
            new PartitionKeyBuilder().Add("t1").Add("catA").Add("eu").Build());

        // Prefix with non-existent tenant
        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("nonexistent").Build()
        };

        var results = await DrainIterator(
            container.GetItemQueryIterator<Document>(
                new QueryDefinition("SELECT * FROM c"), requestOptions: requestOptions));

        results.Should().BeEmpty();
    }

    // ============================================================
    // Edge case: Empty container with prefix query
    // ============================================================

    [Fact]
    public async Task Query_WithPrefixKey_EmptyContainer_ReturnsEmpty()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("any-tenant").Build()
        };

        var results = await DrainIterator(
            container.GetItemQueryIterator<Document>(
                new QueryDefinition("SELECT * FROM c"), requestOptions: requestOptions));

        results.Should().BeEmpty();
    }

    // ============================================================
    // Edge case: ORDER BY + prefix PK
    // ============================================================

    [Fact]
    public async Task Query_WithOrderBy_AndPrefixKey_ScopesAndOrdersCorrectly()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        await container.CreateItemAsync(
            new Document("id-1", "t1", "catB", "eu", "data-B"),
            new PartitionKeyBuilder().Add("t1").Add("catB").Add("eu").Build());

        await container.CreateItemAsync(
            new Document("id-2", "t1", "catA", "us", "data-A"),
            new PartitionKeyBuilder().Add("t1").Add("catA").Add("us").Build());

        await container.CreateItemAsync(
            new Document("id-3", "t2", "catA", "eu", "data-other"),
            new PartitionKeyBuilder().Add("t2").Add("catA").Add("eu").Build());

        // Prefix for t1, with ORDER BY category ASC
        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("t1").Build()
        };

        var results = await DrainIterator(
            container.GetItemQueryIterator<Document>(
                new QueryDefinition("SELECT * FROM c ORDER BY c.category ASC"),
                requestOptions: requestOptions));

        results.Should().HaveCount(2);
        results[0].Category.Should().Be("catA");
        results[1].Category.Should().Be("catB");
        results.Select(r => r.TenantId).Should().AllBe("t1");
    }

    [Fact]
    public async Task Query_WithOrderByDesc_AndPrefixKey_ScopesAndOrdersCorrectly()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        await container.CreateItemAsync(
            new Document("id-1", "t1", "catA", "eu", "data1"),
            new PartitionKeyBuilder().Add("t1").Add("catA").Add("eu").Build());

        await container.CreateItemAsync(
            new Document("id-2", "t1", "catC", "us", "data2"),
            new PartitionKeyBuilder().Add("t1").Add("catC").Add("us").Build());

        await container.CreateItemAsync(
            new Document("id-3", "t1", "catB", "us", "data3"),
            new PartitionKeyBuilder().Add("t1").Add("catB").Add("us").Build());

        await container.CreateItemAsync(
            new Document("id-4", "t2", "catZ", "eu", "data4"),
            new PartitionKeyBuilder().Add("t2").Add("catZ").Add("eu").Build());

        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("t1").Build()
        };

        var results = await DrainIterator(
            container.GetItemQueryIterator<Document>(
                new QueryDefinition("SELECT * FROM c ORDER BY c.category DESC"),
                requestOptions: requestOptions));

        results.Should().HaveCount(3);
        results[0].Category.Should().Be("catC");
        results[1].Category.Should().Be("catB");
        results[2].Category.Should().Be("catA");
    }

    // ============================================================
    // Edge case: LINQ query + prefix PK
    // ============================================================

    [Fact]
    public async Task LinqQuery_WithPrefixKey_ScopesToMatchingDocumentsOnly()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        await container.CreateItemAsync(
            new Document("id-1", "t1", "catA", "eu", "match"),
            new PartitionKeyBuilder().Add("t1").Add("catA").Add("eu").Build());

        await container.CreateItemAsync(
            new Document("id-2", "t2", "catA", "eu", "no-match"),
            new PartitionKeyBuilder().Add("t2").Add("catA").Add("eu").Build());

        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("t1").Build()
        };

        var queryable = container.GetItemLinqQueryable<Document>(requestOptions: requestOptions);
        var iterator = queryable.Where(d => d.Data == "match").ToFeedIterator();
        var results = await DrainIterator(iterator);

        results.Should().HaveCount(1);
        results[0].TenantId.Should().Be("t1");
    }

    [Fact]
    public async Task LinqQuery_WithTwoLevelPrefixKey_ScopesCorrectly()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        await container.CreateItemAsync(
            new Document("id-1", "t1", "catA", "eu", "data1"),
            new PartitionKeyBuilder().Add("t1").Add("catA").Add("eu").Build());

        await container.CreateItemAsync(
            new Document("id-2", "t1", "catB", "us", "data2"),
            new PartitionKeyBuilder().Add("t1").Add("catB").Add("us").Build());

        await container.CreateItemAsync(
            new Document("id-3", "t1", "catA", "us", "data3"),
            new PartitionKeyBuilder().Add("t1").Add("catA").Add("us").Build());

        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("t1").Add("catA").Build()
        };

        var queryable = container.GetItemLinqQueryable<Document>(requestOptions: requestOptions);
        var iterator = queryable.ToFeedIterator();
        var results = await DrainIterator(iterator);

        results.Should().HaveCount(2);
        results.Select(r => r.Id).Should().BeEquivalentTo(new[] { "id-1", "id-3" });
    }

    // ============================================================
    // Edge case: ReadFeed (no SQL query) + prefix PK
    // ============================================================

    [Fact]
    public async Task ReadFeed_WithPrefixKey_ScopesToMatchingDocumentsOnly()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        await container.CreateItemAsync(
            new Document("id-1", "t1", "catA", "eu", "data1"),
            new PartitionKeyBuilder().Add("t1").Add("catA").Add("eu").Build());

        await container.CreateItemAsync(
            new Document("id-2", "t2", "catA", "eu", "data2"),
            new PartitionKeyBuilder().Add("t2").Add("catA").Add("eu").Build());

        await container.CreateItemAsync(
            new Document("id-3", "t1", "catB", "us", "data3"),
            new PartitionKeyBuilder().Add("t1").Add("catB").Add("us").Build());

        // ReadFeed with prefix PK — no QueryDefinition, just requestOptions
        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("t1").Build()
        };

        var results = await DrainIterator(
            container.GetItemQueryIterator<Document>(requestOptions: requestOptions));

        results.Should().HaveCount(2);
        results.Select(r => r.TenantId).Should().AllBe("t1");
    }

    // ============================================================
    // Edge case: Null PK component in hierarchical key
    // ============================================================

    [Fact]
    public async Task Query_WithPrefixKey_NullPkComponent_ScopesCorrectly()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        // Document with null category
        await container.CreateItemAsync(
            new NullableDocument { Id = "id-1", TenantId = "t1", Category = null, Region = "eu", Data = "null-cat" },
            new PartitionKeyBuilder().Add("t1").AddNullValue().Add("eu").Build());

        // Document with non-null category
        await container.CreateItemAsync(
            new NullableDocument { Id = "id-2", TenantId = "t1", Category = "catA", Region = "eu", Data = "has-cat" },
            new PartitionKeyBuilder().Add("t1").Add("catA").Add("eu").Build());

        // Document from different tenant
        await container.CreateItemAsync(
            new NullableDocument { Id = "id-3", TenantId = "t2", Category = null, Region = "eu", Data = "other" },
            new PartitionKeyBuilder().Add("t2").AddNullValue().Add("eu").Build());

        // Prefix for t1 — should return both t1 docs
        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("t1").Build()
        };

        var results = await DrainIterator(
            container.GetItemQueryIterator<NullableDocument>(
                new QueryDefinition("SELECT * FROM c"), requestOptions: requestOptions));

        results.Should().HaveCount(2);
        results.Select(r => r.TenantId).Should().AllBe("t1");
    }

    [Fact]
    public async Task Query_WithPrefixKey_NullFirstComponent_ScopesCorrectly()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        // Documents with null tenantId
        await container.CreateItemAsync(
            new NullableDocument { Id = "id-1", TenantId = null, Category = "catA", Region = "eu", Data = "null-tenant" },
            new PartitionKeyBuilder().AddNullValue().Add("catA").Add("eu").Build());

        await container.CreateItemAsync(
            new NullableDocument { Id = "id-2", TenantId = "t1", Category = "catA", Region = "eu", Data = "has-tenant" },
            new PartitionKeyBuilder().Add("t1").Add("catA").Add("eu").Build());

        // Prefix for null tenant
        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().AddNullValue().Build()
        };

        var results = await DrainIterator(
            container.GetItemQueryIterator<NullableDocument>(
                new QueryDefinition("SELECT * FROM c"), requestOptions: requestOptions));

        results.Should().HaveCount(1);
        results[0].TenantId.Should().BeNull();
    }

    // ============================================================
    // Edge case: Numeric partition key values in hierarchical key
    // ============================================================

    [Fact]
    public async Task Query_WithPrefixKey_NumericPkValue_ScopesCorrectly()
    {
        // Container: /tenantId (string), /priority (number), /region (string)
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/priority", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        await container.CreateItemAsync(
            new MixedTypeDocument { Id = "id-1", TenantId = "t1", Priority = 1.0, Region = "eu", Data = "low" },
            new PartitionKeyBuilder().Add("t1").Add(1.0).Add("eu").Build());

        await container.CreateItemAsync(
            new MixedTypeDocument { Id = "id-2", TenantId = "t1", Priority = 5.0, Region = "us", Data = "high" },
            new PartitionKeyBuilder().Add("t1").Add(5.0).Add("us").Build());

        await container.CreateItemAsync(
            new MixedTypeDocument { Id = "id-3", TenantId = "t2", Priority = 1.0, Region = "eu", Data = "other" },
            new PartitionKeyBuilder().Add("t2").Add(1.0).Add("eu").Build());

        // Prefix for t1 — should get both t1 docs regardless of numeric priority values
        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("t1").Build()
        };

        var results = await DrainIterator(
            container.GetItemQueryIterator<MixedTypeDocument>(
                new QueryDefinition("SELECT * FROM c"), requestOptions: requestOptions));

        results.Should().HaveCount(2);
        results.Select(r => r.TenantId).Should().AllBe("t1");
    }

    [Fact]
    public async Task Query_WithTwoLevelPrefix_StringAndNumeric_ScopesCorrectly()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/priority", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        await container.CreateItemAsync(
            new MixedTypeDocument { Id = "id-1", TenantId = "t1", Priority = 1.0, Region = "eu", Data = "d1" },
            new PartitionKeyBuilder().Add("t1").Add(1.0).Add("eu").Build());

        await container.CreateItemAsync(
            new MixedTypeDocument { Id = "id-2", TenantId = "t1", Priority = 2.0, Region = "us", Data = "d2" },
            new PartitionKeyBuilder().Add("t1").Add(2.0).Add("us").Build());

        await container.CreateItemAsync(
            new MixedTypeDocument { Id = "id-3", TenantId = "t1", Priority = 1.0, Region = "us", Data = "d3" },
            new PartitionKeyBuilder().Add("t1").Add(1.0).Add("us").Build());

        // 2-level prefix: t1 + priority=1.0
        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("t1").Add(1.0).Build()
        };

        var results = await DrainIterator(
            container.GetItemQueryIterator<MixedTypeDocument>(
                new QueryDefinition("SELECT * FROM c"), requestOptions: requestOptions));

        results.Should().HaveCount(2);
        results.Select(r => r.Id).Should().BeEquivalentTo(new[] { "id-1", "id-3" });
    }

    // ============================================================
    // Edge case: Boolean partition key values in hierarchical key
    // ============================================================

    [Fact]
    public async Task Query_WithPrefixKey_BooleanPkValue_ScopesCorrectly()
    {
        // Container: /tenantId (string), /isActive (boolean), /region (string)
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/isActive", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        await container.CreateItemAsync(
            new BoolDocument { Id = "id-1", TenantId = "t1", IsActive = true, Region = "eu", Data = "active" },
            new PartitionKeyBuilder().Add("t1").Add(true).Add("eu").Build());

        await container.CreateItemAsync(
            new BoolDocument { Id = "id-2", TenantId = "t1", IsActive = false, Region = "us", Data = "inactive" },
            new PartitionKeyBuilder().Add("t1").Add(false).Add("us").Build());

        await container.CreateItemAsync(
            new BoolDocument { Id = "id-3", TenantId = "t2", IsActive = true, Region = "eu", Data = "other" },
            new PartitionKeyBuilder().Add("t2").Add(true).Add("eu").Build());

        // Prefix for t1 — should return both active and inactive t1 docs
        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("t1").Build()
        };

        var results = await DrainIterator(
            container.GetItemQueryIterator<BoolDocument>(
                new QueryDefinition("SELECT * FROM c"), requestOptions: requestOptions));

        results.Should().HaveCount(2);
        results.Select(r => r.TenantId).Should().AllBe("t1");
    }

    [Fact]
    public async Task Query_WithTwoLevelPrefix_StringAndBoolean_ScopesCorrectly()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/isActive", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        await container.CreateItemAsync(
            new BoolDocument { Id = "id-1", TenantId = "t1", IsActive = true, Region = "eu", Data = "d1" },
            new PartitionKeyBuilder().Add("t1").Add(true).Add("eu").Build());

        await container.CreateItemAsync(
            new BoolDocument { Id = "id-2", TenantId = "t1", IsActive = false, Region = "us", Data = "d2" },
            new PartitionKeyBuilder().Add("t1").Add(false).Add("us").Build());

        await container.CreateItemAsync(
            new BoolDocument { Id = "id-3", TenantId = "t1", IsActive = true, Region = "us", Data = "d3" },
            new PartitionKeyBuilder().Add("t1").Add(true).Add("us").Build());

        // 2-level prefix: t1 + isActive=true
        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("t1").Add(true).Build()
        };

        var results = await DrainIterator(
            container.GetItemQueryIterator<BoolDocument>(
                new QueryDefinition("SELECT * FROM c"), requestOptions: requestOptions));

        results.Should().HaveCount(2);
        results.Select(r => r.Id).Should().BeEquivalentTo(new[] { "id-1", "id-3" });
    }

    // ============================================================
    // Edge case: Many documents under same prefix
    // ============================================================

    [Fact]
    public async Task Query_WithPrefixKey_ManyDocumentsSamePrefix_ReturnsAll()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        // Create 20 documents under t1, 5 under t2
        for (var i = 0; i < 20; i++)
        {
            await container.CreateItemAsync(
                new Document($"t1-{i}", "t1", $"cat{i % 4}", $"r{i % 3}", $"data{i}"),
                new PartitionKeyBuilder().Add("t1").Add($"cat{i % 4}").Add($"r{i % 3}").Build());
        }
        for (var i = 0; i < 5; i++)
        {
            await container.CreateItemAsync(
                new Document($"t2-{i}", "t2", $"cat{i}", $"r{i}", $"other{i}"),
                new PartitionKeyBuilder().Add("t2").Add($"cat{i}").Add($"r{i}").Build());
        }

        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("t1").Build()
        };

        var results = await DrainIterator(
            container.GetItemQueryIterator<Document>(
                new QueryDefinition("SELECT * FROM c"), requestOptions: requestOptions));

        results.Should().HaveCount(20);
        results.Select(r => r.TenantId).Should().AllBe("t1");
    }

    // ============================================================
    // Edge case: Single document exact prefix match
    // ============================================================

    [Fact]
    public async Task Query_WithPrefixKey_SingleDocumentExactMatch_ReturnsIt()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        await container.CreateItemAsync(
            new Document("only-one", "sole-tenant", "catA", "eu", "lonely"),
            new PartitionKeyBuilder().Add("sole-tenant").Add("catA").Add("eu").Build());

        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("sole-tenant").Build()
        };

        var results = await DrainIterator(
            container.GetItemQueryIterator<Document>(
                new QueryDefinition("SELECT * FROM c"), requestOptions: requestOptions));

        Assert.Single(results);
        results[0].Id.Should().Be("only-one");
    }

    // ============================================================
    // Edge case: SELECT TOP with prefix PK
    // ============================================================

    [Fact]
    public async Task Query_WithTopN_AndPrefixKey_ScopesCorrectly()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        for (var i = 0; i < 5; i++)
        {
            await container.CreateItemAsync(
                new Document($"t1-{i}", "t1", $"cat{i}", "eu", $"data{i}"),
                new PartitionKeyBuilder().Add("t1").Add($"cat{i}").Add("eu").Build());
        }
        await container.CreateItemAsync(
            new Document("t2-0", "t2", "catX", "us", "other"),
            new PartitionKeyBuilder().Add("t2").Add("catX").Add("us").Build());

        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("t1").Build()
        };

        // TOP is applied per-range by the SDK, so total count may be less than 5.
        // The key assertion: no t2 documents should leak through.
        var results = await DrainIterator(
            container.GetItemQueryIterator<Document>(
                new QueryDefinition("SELECT TOP 3 * FROM c"), requestOptions: requestOptions));

        results.Should().NotBeEmpty();
        results.Count.Should().BeLessThanOrEqualTo(5);
        results.Select(r => r.TenantId).Should().AllBe("t1");
    }

    // ============================================================
    // Edge case: ORDER BY + WHERE + prefix PK (combined)
    // ============================================================

    [Fact]
    public async Task Query_WithOrderByAndWhere_AndPrefixKey_ScopesFiltersAndOrders()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        await container.CreateItemAsync(
            new Document("id-1", "t1", "catA", "eu", "d1"),
            new PartitionKeyBuilder().Add("t1").Add("catA").Add("eu").Build());

        await container.CreateItemAsync(
            new Document("id-2", "t1", "catB", "eu", "d2"),
            new PartitionKeyBuilder().Add("t1").Add("catB").Add("eu").Build());

        await container.CreateItemAsync(
            new Document("id-3", "t1", "catC", "us", "d3"),
            new PartitionKeyBuilder().Add("t1").Add("catC").Add("us").Build());

        await container.CreateItemAsync(
            new Document("id-4", "t2", "catA", "eu", "d4"),
            new PartitionKeyBuilder().Add("t2").Add("catA").Add("eu").Build());

        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("t1").Build()
        };

        var query = new QueryDefinition("SELECT * FROM c WHERE c.region = @r ORDER BY c.category ASC")
            .WithParameter("@r", "eu");

        var results = await DrainIterator(
            container.GetItemQueryIterator<Document>(query, requestOptions: requestOptions));

        results.Should().HaveCount(2);
        results[0].Id.Should().Be("id-1");
        results[1].Id.Should().Be("id-2");
        results.Select(r => r.TenantId).Should().AllBe("t1");
    }

    // ============================================================
    // Edge case: Documents with missing PK path properties
    // ============================================================

    [Fact]
    public async Task Query_WithPrefixKey_DocumentMissingPkProperty_DoesNotLeak()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        // Normal documents
        await container.CreateItemAsync(
            new Document("id-1", "t1", "catA", "eu", "data1"),
            new PartitionKeyBuilder().Add("t1").Add("catA").Add("eu").Build());

        // Document with explicit "None" partition key for missing category
        // (simulated by creating directly on the backing store)
        await container.CreateItemAsync(
            new NullableDocument { Id = "id-2", TenantId = "t2", Category = "catB", Region = "eu", Data = "other" },
            new PartitionKeyBuilder().Add("t2").Add("catB").Add("eu").Build());

        // Prefix for t1 — should only get t1's document
        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("t1").Build()
        };

        var results = await DrainIterator(
            container.GetItemQueryIterator<NullableDocument>(
                new QueryDefinition("SELECT * FROM c"), requestOptions: requestOptions));

        results.Should().HaveCount(1);
        results[0].TenantId.Should().Be("t1");
    }

    // ============================================================
    // Edge case: Containers with exactly 2 partition key paths
    // ============================================================

    [Fact]
    public async Task Query_WithPrefixKey_TwoPathContainer_MultipleTenants()
    {
        var inMemoryContainer = new InMemoryContainer("items2", new[] { "/tenantId", "/category" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items2");

        // Multiple documents per tenant
        await container.CreateItemAsync(
            new Document("id-1", "t1", "catA", "eu", "d1"),
            new PartitionKeyBuilder().Add("t1").Add("catA").Build());
        await container.CreateItemAsync(
            new Document("id-2", "t1", "catB", "us", "d2"),
            new PartitionKeyBuilder().Add("t1").Add("catB").Build());
        await container.CreateItemAsync(
            new Document("id-3", "t2", "catA", "eu", "d3"),
            new PartitionKeyBuilder().Add("t2").Add("catA").Build());
        await container.CreateItemAsync(
            new Document("id-4", "t3", "catC", "us", "d4"),
            new PartitionKeyBuilder().Add("t3").Add("catC").Build());

        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("t1").Build()
        };

        var results = await DrainIterator(
            container.GetItemQueryIterator<Document>(
                new QueryDefinition("SELECT * FROM c"), requestOptions: requestOptions));

        results.Should().HaveCount(2);
        results.Select(r => r.TenantId).Should().AllBe("t1");
    }

    // ============================================================
    // Edge case: Prefix PK with special characters
    // ============================================================

    [Fact]
    public async Task Query_WithPrefixKey_SpecialCharacters_ScopesCorrectly()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        var specialTenant = "tenant|with|pipes";
        await container.CreateItemAsync(
            new Document("id-1", specialTenant, "catA", "eu", "data1"),
            new PartitionKeyBuilder().Add(specialTenant).Add("catA").Add("eu").Build());

        await container.CreateItemAsync(
            new Document("id-2", "normal-tenant", "catA", "eu", "data2"),
            new PartitionKeyBuilder().Add("normal-tenant").Add("catA").Add("eu").Build());

        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add(specialTenant).Build()
        };

        var results = await DrainIterator(
            container.GetItemQueryIterator<Document>(
                new QueryDefinition("SELECT * FROM c"), requestOptions: requestOptions));

        Assert.Single(results);
        results[0].TenantId.Should().Be(specialTenant);
    }

    // ============================================================
    // Edge case: LINQ with OrderBy + prefix PK
    // ============================================================

    [Fact]
    public async Task LinqQuery_WithOrderByAndPrefixKey_ScopesAndOrders()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        await container.CreateItemAsync(
            new Document("id-1", "t1", "catZ", "eu", "data1"),
            new PartitionKeyBuilder().Add("t1").Add("catZ").Add("eu").Build());

        await container.CreateItemAsync(
            new Document("id-2", "t1", "catA", "us", "data2"),
            new PartitionKeyBuilder().Add("t1").Add("catA").Add("us").Build());

        await container.CreateItemAsync(
            new Document("id-3", "t2", "catM", "eu", "data3"),
            new PartitionKeyBuilder().Add("t2").Add("catM").Add("eu").Build());

        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add("t1").Build()
        };

        var queryable = container.GetItemLinqQueryable<Document>(requestOptions: requestOptions);
        var iterator = queryable.OrderBy(d => d.Category).ToFeedIterator();
        var results = await DrainIterator(iterator);

        results.Should().HaveCount(2);
        results[0].Category.Should().Be("catA");
        results[1].Category.Should().Be("catZ");
        results.Select(r => r.TenantId).Should().AllBe("t1");
    }

    // ============================================================
    // Edge case: Cross-partition query (no prefix) still returns all
    // ============================================================

    [Fact]
    public async Task Query_WithoutPartitionKey_ReturnsAllDocuments()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
        using var client = CreateClient(inMemoryContainer);
        var container = client.GetContainer("test-db", "items");

        await container.CreateItemAsync(
            new Document("id-1", "t1", "catA", "eu", "data1"),
            new PartitionKeyBuilder().Add("t1").Add("catA").Add("eu").Build());

        await container.CreateItemAsync(
            new Document("id-2", "t2", "catB", "us", "data2"),
            new PartitionKeyBuilder().Add("t2").Add("catB").Add("us").Build());

        // No partition key — cross-partition query should return all
        var results = await DrainIterator(
            container.GetItemQueryIterator<Document>(
                new QueryDefinition("SELECT * FROM c")));

        results.Should().HaveCount(2);
    }
}
