using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for the gateway query plan endpoint in <see cref="FakeCosmosHandler"/>.
/// On non-Windows platforms the Cosmos SDK uses this endpoint instead of the native
/// ServiceInterop DLL to determine how to build the query execution pipeline
/// (ORDER BY merge sort, aggregate accumulation, DISTINCT deduplication, etc.).
/// These tests verify that the query plan metadata is accurate for each query pattern.
/// </summary>
public class QueryPlanTests : IDisposable
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");
    private readonly FakeCosmosHandler _handler;
    private readonly HttpClient _httpClient;

    public QueryPlanTests()
    {
        _handler = new FakeCosmosHandler(_container);
        _httpClient = new HttpClient(_handler)
        {
            BaseAddress = new Uri("https://localhost:9999/")
        };
    }

    public void Dispose()
    {
        _httpClient.Dispose();
        _handler.Dispose();
    }

    private async Task<JObject> GetQueryPlanAsync(string sql)
    {
        var body = new JObject { ["query"] = sql }.ToString();
        var request = new HttpRequestMessage(HttpMethod.Post,
            "dbs/fakeDb/colls/test-container/docs")
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json")
        };
        request.Headers.Add("x-ms-cosmos-is-query-plan-request", "True");
        request.Headers.Add("x-ms-documentdb-query-enablecrosspartition", "True");

        var response = await _httpClient.SendAsync(request);
        var json = await response.Content.ReadAsStringAsync();
        return JObject.Parse(json);
    }

    [Fact]
    public async Task QueryPlan_SimpleSelect_HasNoSpecialFlags()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c");
        var info = plan["queryInfo"]!;

        info["distinctType"]!.ToString().Should().Be("None");
        info["orderBy"]!.Should().BeOfType<JArray>().Which.Should().BeEmpty();
        info["aggregates"]!.Should().BeOfType<JArray>().Which.Should().BeEmpty();
        info["groupByExpressions"]!.Should().BeOfType<JArray>().Which.Should().BeEmpty();
        ((bool)info["hasSelectValue"]!).Should().BeFalse();
        info["top"]!.Type.Should().Be(JTokenType.Null);
        info["offset"]!.Type.Should().Be(JTokenType.Null);
        info["limit"]!.Type.Should().Be(JTokenType.Null);
    }

    [Fact]
    public async Task QueryPlan_OrderByAscending_SetsOrderByMetadata()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c ORDER BY c.name ASC");
        var info = plan["queryInfo"]!;

        var orderBy = (JArray)info["orderBy"]!;
        orderBy.Should().HaveCount(1);
        orderBy[0]!.ToString().Should().Be("Ascending");

        var expressions = (JArray)info["orderByExpressions"]!;
        expressions.Should().HaveCount(1);
        expressions[0]!.ToString().Should().Be("c.name");

        ((bool)info["hasNonStreamingOrderBy"]!).Should().BeTrue();
    }

    [Fact]
    public async Task QueryPlan_OrderByDescending_SetsDescendingFlag()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c ORDER BY c.value DESC");
        var info = plan["queryInfo"]!;

        var orderBy = (JArray)info["orderBy"]!;
        orderBy[0]!.ToString().Should().Be("Descending");
    }

    [Fact]
    public async Task QueryPlan_MultipleOrderBy_SetsAllFields()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c ORDER BY c.name ASC, c.value DESC");
        var info = plan["queryInfo"]!;

        var orderBy = (JArray)info["orderBy"]!;
        orderBy.Should().HaveCount(2);
        orderBy[0]!.ToString().Should().Be("Ascending");
        orderBy[1]!.ToString().Should().Be("Descending");

        var expressions = (JArray)info["orderByExpressions"]!;
        expressions.Should().HaveCount(2);
    }

    [Fact]
    public async Task QueryPlan_Top_SetsTopField()
    {
        var plan = await GetQueryPlanAsync("SELECT TOP 10 * FROM c");
        var info = plan["queryInfo"]!;

        ((int)info["top"]!).Should().Be(10);
    }

    [Fact]
    public async Task QueryPlan_OffsetLimit_SetsBothFields()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c OFFSET 5 LIMIT 10");
        var info = plan["queryInfo"]!;

        ((int)info["offset"]!).Should().Be(5);
        ((int)info["limit"]!).Should().Be(10);
    }

    [Fact]
    public async Task QueryPlan_Distinct_SetsDistinctTypeUnordered()
    {
        var plan = await GetQueryPlanAsync("SELECT DISTINCT c.name FROM c");
        var info = plan["queryInfo"]!;

        info["distinctType"]!.ToString().Should().Be("Unordered");
    }

    [Fact]
    public async Task QueryPlan_DistinctWithOrderBy_SetsDistinctTypeOrdered()
    {
        var plan = await GetQueryPlanAsync("SELECT DISTINCT c.name FROM c ORDER BY c.name");
        var info = plan["queryInfo"]!;

        info["distinctType"]!.ToString().Should().Be("Ordered");
    }

    [Fact]
    public async Task QueryPlan_CountAggregate_DetectsCount()
    {
        var plan = await GetQueryPlanAsync("SELECT COUNT(1) FROM c");
        var info = plan["queryInfo"]!;

        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().Contain(t => t.ToString() == "Count");
    }

    [Fact]
    public async Task QueryPlan_SumAggregate_DetectsSum()
    {
        var plan = await GetQueryPlanAsync("SELECT SUM(c.value) FROM c");
        var info = plan["queryInfo"]!;

        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().Contain(t => t.ToString() == "Sum");
    }

    [Fact]
    public async Task QueryPlan_MinMaxAvg_DetectsAll()
    {
        var plan = await GetQueryPlanAsync(
            "SELECT MIN(c.value) AS minVal, MAX(c.value) AS maxVal, AVG(c.value) AS avgVal FROM c");
        var info = plan["queryInfo"]!;

        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Select(t => t.ToString()).Should().Contain("Min");
        aggregates.Select(t => t.ToString()).Should().Contain("Max");
        aggregates.Select(t => t.ToString()).Should().Contain("Average");

        var aliasMap = (JObject)info["groupByAliasToAggregateType"]!;
        aliasMap["minVal"]!.ToString().Should().Be("Min");
        aliasMap["maxVal"]!.ToString().Should().Be("Max");
        aliasMap["avgVal"]!.ToString().Should().Be("Average");
    }

    [Fact]
    public async Task QueryPlan_GroupBy_SetsGroupByExpressions()
    {
        var plan = await GetQueryPlanAsync(
            "SELECT c.status, COUNT(1) AS cnt FROM c GROUP BY c.status");
        var info = plan["queryInfo"]!;

        var groupBy = (JArray)info["groupByExpressions"]!;
        groupBy.Should().HaveCount(1);
        groupBy[0]!.ToString().Should().Be("c.status");
    }

    [Fact]
    public async Task QueryPlan_SelectValue_SetsHasSelectValue()
    {
        var plan = await GetQueryPlanAsync("SELECT VALUE c.name FROM c");
        var info = plan["queryInfo"]!;

        ((bool)info["hasSelectValue"]!).Should().BeTrue();
    }

    [Fact]
    public async Task QueryPlan_WhereClause_DoesNotAffectFlags()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c WHERE c.value > 10");
        var info = plan["queryInfo"]!;

        info["distinctType"]!.ToString().Should().Be("None");
        info["orderBy"]!.Should().BeOfType<JArray>().Which.Should().BeEmpty();
        info["aggregates"]!.Should().BeOfType<JArray>().Which.Should().BeEmpty();
    }

    [Fact]
    public async Task QueryPlan_QueryRanges_CoversFullRange()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c");

        var ranges = (JArray)plan["queryRanges"]!;
        ranges.Should().HaveCount(1);
        ranges[0]!["min"]!.ToString().Should().Be("");
        ranges[0]!["max"]!.ToString().Should().Be("FF");
        ((bool)ranges[0]!["isMinInclusive"]!).Should().BeTrue();
        ((bool)ranges[0]!["isMaxInclusive"]!).Should().BeFalse();
    }

    [Fact]
    public async Task QueryPlan_ComplexQuery_SetsAllRelevantFlags()
    {
        var plan = await GetQueryPlanAsync(
            "SELECT DISTINCT TOP 5 c.category, SUM(c.value) AS total " +
            "FROM c WHERE c.isActive = true " +
            "GROUP BY c.category " +
            "ORDER BY c.category ASC");
        var info = plan["queryInfo"]!;

        info["distinctType"]!.ToString().Should().Be("Ordered");
        ((int)info["top"]!).Should().Be(5);

        var orderBy = (JArray)info["orderBy"]!;
        orderBy.Should().HaveCount(1);
        orderBy[0]!.ToString().Should().Be("Ascending");

        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().Contain(t => t.ToString() == "Sum");

        var groupBy = (JArray)info["groupByExpressions"]!;
        groupBy.Should().HaveCount(1);

        var aliasMap = (JObject)info["groupByAliasToAggregateType"]!;
        aliasMap["total"]!.ToString().Should().Be("Sum");
    }

    [Fact]
    public async Task QueryPlan_UnparsableQuery_StillReturnsValidPlan()
    {
        // Even if the parser can't handle the query, the plan should be valid
        // with sensible defaults so the SDK doesn't crash.
        var plan = await GetQueryPlanAsync("SELECT ??? FROM c");
        var info = plan["queryInfo"]!;

        info["distinctType"]!.ToString().Should().Be("None");
        info["rewrittenQuery"]!.ToString().Should().NotBeNullOrEmpty();
        plan["queryRanges"]!.Should().BeOfType<JArray>().Which.Should().NotBeEmpty();
    }
}
