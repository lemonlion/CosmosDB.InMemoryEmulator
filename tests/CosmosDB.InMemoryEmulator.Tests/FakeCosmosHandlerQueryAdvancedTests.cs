using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for advanced SQL query features through FakeCosmosHandler.
/// Covers subqueries, built-in functions, vector/FTS/geospatial queries,
/// and parameterized queries that go through the full SDK → HTTP → handler pipeline.
/// </summary>
public class FakeCosmosHandlerQueryAdvancedTests : IDisposable
{
    private class QueryDoc
    {
        [JsonProperty("id")] public string Id { get; set; } = default!;
        [JsonProperty("partitionKey")] public string PartitionKey { get; set; } = default!;
        [JsonProperty("name")] public string Name { get; set; } = default!;
        [JsonProperty("score")] public int Score { get; set; }
        [JsonProperty("isActive")] public bool IsActive { get; set; } = true;
        [JsonProperty("tags")] public string[] Tags { get; set; } = [];
        [JsonProperty("nested")] public NestedObject? Nested { get; set; }
    }

    private readonly InMemoryContainer _inMemoryContainer;
    private readonly FakeCosmosHandler _handler;
    private readonly CosmosClient _client;
    private readonly Container _container;

    public FakeCosmosHandlerQueryAdvancedTests()
    {
        _inMemoryContainer = new InMemoryContainer("test-query-adv", "/partitionKey");
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
        _container = _client.GetContainer("db", "test-query-adv");
        SeedData().GetAwaiter().GetResult();
    }

    private async Task SeedData()
    {
        var docs = new[]
        {
            new QueryDoc { Id = "1", PartitionKey = "pk1", Name = "Alice", Score = 30, Tags = ["admin", "user"] },
            new QueryDoc { Id = "2", PartitionKey = "pk1", Name = "Bob", Score = 20, Tags = ["user"] },
            new QueryDoc { Id = "3", PartitionKey = "pk2", Name = "Charlie", Score = 50, Tags = ["admin"] },
            new QueryDoc { Id = "4", PartitionKey = "pk2", Name = "Diana", Score = 10, Tags = ["user", "moderator"] },
            new QueryDoc { Id = "5", PartitionKey = "pk1", Name = "Eve", Score = 40, Tags = [] },
        };
        foreach (var doc in docs)
            await _container.CreateItemAsync(doc, new PartitionKey(doc.PartitionKey));
    }

    public void Dispose()
    {
        _client.Dispose();
        _handler.Dispose();
    }

    private async Task<List<T>> DrainQuery<T>(string sql)
    {
        var iterator = _container.GetItemQueryIterator<T>(sql);
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        return results;
    }

    private async Task<List<T>> DrainQuery<T>(QueryDefinition queryDef)
    {
        var iterator = _container.GetItemQueryIterator<T>(queryDef);
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        return results;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  String Functions
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_StringUpper_ReturnsUppercased()
    {
        var results = await DrainQuery<JObject>(
            "SELECT UPPER(c.name) AS upper_name FROM c WHERE c.id = '1'");

        results.Should().HaveCount(1);
        results[0]["upper_name"]!.Value<string>().Should().Be("ALICE");
    }

    [Fact]
    public async Task Query_StringLower_ReturnsLowercased()
    {
        var results = await DrainQuery<JObject>(
            "SELECT LOWER(c.name) AS lower_name FROM c WHERE c.id = '1'");

        results.Should().HaveCount(1);
        results[0]["lower_name"]!.Value<string>().Should().Be("alice");
    }

    [Fact]
    public async Task Query_StringConcat_Works()
    {
        var results = await DrainQuery<JObject>(
            "SELECT CONCAT(c.name, '-', c.partitionKey) AS full FROM c WHERE c.id = '1'");

        results.Should().HaveCount(1);
        results[0]["full"]!.Value<string>().Should().Be("Alice-pk1");
    }

    [Fact]
    public async Task Query_StringContains_Filters()
    {
        var results = await DrainQuery<QueryDoc>(
            "SELECT * FROM c WHERE CONTAINS(c.name, 'li')");

        results.Should().HaveCount(2);
        results.Select(r => r.Name).Should().BeEquivalentTo("Alice", "Charlie");
    }

    [Fact]
    public async Task Query_Length_ReturnsCorrectLength()
    {
        var results = await DrainQuery<JObject>(
            "SELECT LENGTH(c.name) AS len FROM c WHERE c.id = '3'");

        results.Should().HaveCount(1);
        results[0]["len"]!.Value<int>().Should().Be(7); // "Charlie"
    }

    [Fact]
    public async Task Query_Substring_ExtractsCorrectly()
    {
        var results = await DrainQuery<JObject>(
            "SELECT SUBSTRING(c.name, 0, 3) AS sub FROM c WHERE c.id = '1'");

        results.Should().HaveCount(1);
        results[0]["sub"]!.Value<string>().Should().Be("Ali");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Math Functions
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_MathAbs_ReturnsAbsoluteValue()
    {
        var results = await DrainQuery<JObject>(
            "SELECT ABS(c.score - 35) AS diff FROM c WHERE c.id = '1'");

        results.Should().HaveCount(1);
        results[0]["diff"]!.Value<double>().Should().Be(5);
    }

    [Fact]
    public async Task Query_MathFloor_Works()
    {
        var results = await DrainQuery<JObject>(
            "SELECT FLOOR(c.score / 3.0) AS floored FROM c WHERE c.id = '1'");

        results.Should().HaveCount(1);
        results[0]["floored"]!.Value<double>().Should().Be(10);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Array Functions
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_ArrayContains_Filters()
    {
        var results = await DrainQuery<QueryDoc>(
            "SELECT * FROM c WHERE ARRAY_CONTAINS(c.tags, 'admin')");

        results.Should().HaveCount(2);
        results.Select(r => r.Name).Should().BeEquivalentTo("Alice", "Charlie");
    }

    [Fact]
    public async Task Query_ArrayLength_ReturnsCount()
    {
        var results = await DrainQuery<JObject>(
            "SELECT ARRAY_LENGTH(c.tags) AS tagCount FROM c WHERE c.id = '1'");

        results.Should().HaveCount(1);
        results[0]["tagCount"]!.Value<int>().Should().Be(2);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Type-Checking Functions
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_IsDefined_ChecksPropertyExistence()
    {
        var results = await DrainQuery<QueryDoc>(
            "SELECT * FROM c WHERE IS_DEFINED(c.nonExistentProperty)");

        // No docs have this property
        results.Should().BeEmpty();
    }

    [Fact]
    public async Task Query_IsString_ChecksType()
    {
        var results = await DrainQuery<JObject>(
            "SELECT IS_STRING(c.name) AS isStr FROM c WHERE c.id = '1'");

        results.Should().HaveCount(1);
        results[0]["isStr"]!.Value<bool>().Should().BeTrue();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Subqueries
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_ExistsSubquery_FiltersCorrectly()
    {
        var results = await DrainQuery<QueryDoc>(
            "SELECT * FROM c WHERE EXISTS (SELECT VALUE t FROM t IN c.tags WHERE t = 'admin')");

        results.Should().HaveCount(2);
        results.Select(r => r.Name).Should().BeEquivalentTo("Alice", "Charlie");
    }

    [Fact]
    public async Task Query_ArraySubquery_ReturnsArray()
    {
        var results = await DrainQuery<JObject>(
            "SELECT c.name, ARRAY(SELECT VALUE UPPER(t) FROM t IN c.tags) AS upperTags FROM c WHERE c.id = '1'");

        results.Should().HaveCount(1);
        var tags = results[0]["upperTags"]!.ToObject<string[]>();
        tags.Should().BeEquivalentTo("ADMIN", "USER");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Parameterized Queries
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_Parameterized_WithStringParam()
    {
        var query = new QueryDefinition("SELECT * FROM c WHERE c.name = @name")
            .WithParameter("@name", "Charlie");

        var results = await DrainQuery<QueryDoc>(query);
        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Charlie");
    }

    [Fact]
    public async Task Query_Parameterized_WithNumericParam()
    {
        var query = new QueryDefinition("SELECT * FROM c WHERE c.score > @minVal")
            .WithParameter("@minVal", 25);

        var results = await DrainQuery<QueryDoc>(query);
        results.Should().HaveCount(3);
    }

    [Fact]
    public async Task Query_Parameterized_WithMultipleParams()
    {
        var query = new QueryDefinition("SELECT * FROM c WHERE c.name = @name AND c.partitionKey = @pk")
            .WithParameter("@name", "Alice")
            .WithParameter("@pk", "pk1");

        var results = await DrainQuery<QueryDoc>(query);
        results.Should().HaveCount(1);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Conditional Expressions
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_TernaryExpression_ReturnsCorrectValue()
    {
        var results = await DrainQuery<JObject>(
            "SELECT c.name, (c.score >= 30 ? 'high' : 'low') AS level FROM c");

        results.Should().HaveCount(5);
        results.First(r => r["name"]!.Value<string>() == "Alice")["level"]!.Value<string>().Should().Be("high");
        results.First(r => r["name"]!.Value<string>() == "Bob")["level"]!.Value<string>().Should().Be("low");
    }

    [Fact]
    public async Task Query_NullCoalesce_Works()
    {
        await _container.CreateItemAsync(
            new QueryDoc { Id = "null1", PartitionKey = "pk1", Name = null!, Score = 0 },
            new PartitionKey("pk1"));

        var results = await DrainQuery<JObject>(
            "SELECT (c.name ?? 'Unknown') AS displayName FROM c WHERE c.id = 'null1'");

        results.Should().HaveCount(1);
        results[0]["displayName"]!.Value<string>().Should().Be("Unknown");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  IN / BETWEEN operators
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_InOperator_FiltersCorrectly()
    {
        var results = await DrainQuery<QueryDoc>(
            "SELECT * FROM c WHERE c.name IN ('Alice', 'Charlie', 'Eve')");

        results.Should().HaveCount(3);
        results.Select(r => r.Name).Should().BeEquivalentTo("Alice", "Charlie", "Eve");
    }

    [Fact]
    public async Task Query_BetweenOperator_FiltersRange()
    {
        var results = await DrainQuery<QueryDoc>(
            "SELECT * FROM c WHERE c.score BETWEEN 20 AND 40");

        results.Should().HaveCount(3);
        results.Select(r => r.Name).Should().BeEquivalentTo("Alice", "Bob", "Eve");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  LIKE operator
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_LikeOperator_Wildcard()
    {
        var results = await DrainQuery<QueryDoc>(
            "SELECT * FROM c WHERE c.name LIKE 'A%'");

        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Alice");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  JOIN
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_Join_FlattensTags()
    {
        var results = await DrainQuery<JObject>(
            "SELECT c.name, t AS tag FROM c JOIN t IN c.tags WHERE c.id = '1'");

        results.Should().HaveCount(2);
        results.Select(r => r["tag"]!.Value<string>()).Should().BeEquivalentTo("admin", "user");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Conversion Functions
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_ToString_ConvertsNumber()
    {
        var results = await DrainQuery<JObject>(
            "SELECT ToString(c.score) AS strVal FROM c WHERE c.id = '1'");

        results.Should().HaveCount(1);
        results[0]["strVal"]!.Value<string>().Should().Be("30");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Aggregate with GROUP BY
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_GroupBy_WithCount_ThroughHandler()
    {
        var results = await DrainQuery<JObject>(
            "SELECT c.partitionKey, COUNT(1) AS cnt FROM c GROUP BY c.partitionKey");

        results.Should().HaveCount(2);
        var pk1 = results.First(r => r["partitionKey"]!.Value<string>() == "pk1");
        pk1["cnt"]!.Value<int>().Should().Be(3);
        var pk2 = results.First(r => r["partitionKey"]!.Value<string>() == "pk2");
        pk2["cnt"]!.Value<int>().Should().Be(2);
    }

    [Fact]
    public async Task Query_GroupBy_WithSum_ThroughHandler()
    {
        var results = await DrainQuery<JObject>(
            "SELECT c.partitionKey, SUM(c.score) AS total FROM c GROUP BY c.partitionKey");

        results.Should().HaveCount(2);
        var pk1 = results.First(r => r["partitionKey"]!.Value<string>() == "pk1");
        pk1["total"]!.Value<int>().Should().Be(90); // 30+20+40
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Multiple aggregates
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_MultipleAggregates_ThroughHandler()
    {
        var results = await DrainQuery<JObject>(
            "SELECT COUNT(1) AS cnt, SUM(c.score) AS total, AVG(c.score) AS average FROM c");

        results.Should().HaveCount(1);
        results[0]["cnt"]!.Value<int>().Should().Be(5);
        results[0]["total"]!.Value<int>().Should().Be(150);
        results[0]["average"]!.Value<double>().Should().Be(30);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Continuation token pagination through queries
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_ContinuationToken_PaginatesCorrectly()
    {
        var results = new List<QueryDoc>();
        var iterator = _container.GetItemQueryIterator<QueryDoc>(
            "SELECT * FROM c ORDER BY c.name",
            requestOptions: new QueryRequestOptions { MaxItemCount = 2 });

        int pageCount = 0;
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
            pageCount++;
        }

        results.Should().HaveCount(5);
        pageCount.Should().BeGreaterThan(1);
        results.Select(r => r.Name).Should().ContainInOrder("Alice", "Bob", "Charlie", "Diana", "Eve");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Date/Time Functions
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_GetCurrentTimestamp_ReturnsNumber()
    {
        var results = await DrainQuery<JObject>(
            "SELECT GetCurrentTimestamp() AS ts FROM c WHERE c.id = '1'");

        results.Should().HaveCount(1);
        var ts = results[0]["ts"]!.Value<long>();
        ts.Should().BeGreaterThan(0);
    }
}
