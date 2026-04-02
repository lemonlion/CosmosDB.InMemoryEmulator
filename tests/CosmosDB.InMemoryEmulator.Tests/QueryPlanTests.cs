using System.Net;
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

    private async Task<(JObject Plan, HttpResponseMessage Response)> GetQueryPlanWithResponseAsync(string sql)
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
        return (JObject.Parse(json), response);
    }

    private async Task<JObject> GetQueryPlanAsync(string sql)
    {
        var (plan, _) = await GetQueryPlanWithResponseAsync(sql);
        return plan;
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

    // ──────────────────────────────────────────────────────────────
    // A. ORDER BY edge cases
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task QueryPlan_OrderByWithoutDirection_DefaultsToAscending()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c ORDER BY c.name");
        var info = plan["queryInfo"]!;

        var orderBy = (JArray)info["orderBy"]!;
        orderBy.Should().HaveCount(1);
        orderBy[0]!.ToString().Should().Be("Ascending");

        var expressions = (JArray)info["orderByExpressions"]!;
        expressions.Should().HaveCount(1);
        expressions[0]!.ToString().Should().Be("c.name");
    }

    [Fact]
    public async Task QueryPlan_OrderByNestedProperty_SetsExpression()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c ORDER BY c.address.city ASC");
        var info = plan["queryInfo"]!;

        var expressions = (JArray)info["orderByExpressions"]!;
        expressions.Should().HaveCount(1);
        expressions[0]!.ToString().Should().Be("c.address.city");

        ((bool)info["hasNonStreamingOrderBy"]!).Should().BeTrue();
    }

    [Fact]
    public async Task QueryPlan_OrderByFunctionExpression_SetsExpression()
    {
        // When ORDER BY uses a function expression, the parser creates OrderByField
        // with Field=null and Expression=FunctionCallExpression.
        // The query plan should use ExprToString(field.Expression) as the fallback.
        var plan = await GetQueryPlanAsync("SELECT * FROM c ORDER BY LOWER(c.name) ASC");
        var info = plan["queryInfo"]!;

        var expressions = (JArray)info["orderByExpressions"]!;
        expressions.Should().HaveCount(1);
        // Should not be null/empty — should contain the stringified expression
        expressions[0]!.Type.Should().NotBe(JTokenType.Null);
        expressions[0]!.ToString().Should().NotBeNullOrEmpty();

        var orderBy = (JArray)info["orderBy"]!;
        orderBy.Should().HaveCount(1);
        orderBy[0]!.ToString().Should().Be("Ascending");
    }

    [Fact]
    public async Task QueryPlan_NoOrderBy_HasNonStreamingOrderByIsFalse()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c WHERE c.value > 10");
        var info = plan["queryInfo"]!;

        ((bool)info["hasNonStreamingOrderBy"]!).Should().BeFalse();
    }

    [Fact]
    public async Task QueryPlan_OrderBy_RewrittenQueryHasCorrectStructure()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c ORDER BY c.name ASC");
        var info = plan["queryInfo"]!;

        var rewritten = info["rewrittenQuery"]!.ToString();
        // The SDK expects: SELECT c._rid, [{"item": c.name}] AS orderByItems, c AS payload FROM c ORDER BY c.name ASC
        rewritten.Should().Contain("_rid");
        rewritten.Should().Contain("orderByItems");
        rewritten.Should().Contain("payload");
        rewritten.Should().Contain("ORDER BY");
        rewritten.Should().Contain("c.name");
    }

    [Fact]
    public async Task QueryPlan_MultipleOrderBy_RewrittenQueryHasAllFields()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c ORDER BY c.name ASC, c.age DESC");
        var info = plan["queryInfo"]!;

        var rewritten = info["rewrittenQuery"]!.ToString();
        rewritten.Should().Contain("c.name");
        rewritten.Should().Contain("c.age");
        rewritten.Should().Contain("orderByItems");
        rewritten.Should().Contain("ASC");
        rewritten.Should().Contain("DESC");
    }

    [Fact]
    public async Task QueryPlan_OrderByWithWhere_RewrittenQueryIncludesWhere()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c WHERE c.active = true ORDER BY c.name ASC");
        var info = plan["queryInfo"]!;

        var rewritten = info["rewrittenQuery"]!.ToString();
        rewritten.Should().Contain("WHERE");
        rewritten.Should().Contain("ORDER BY");
        rewritten.Should().Contain("c.name");
    }

    // ──────────────────────────────────────────────────────────────
    // B. DISTINCT edge cases
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task QueryPlan_DistinctValue_SetsBothFlags()
    {
        var plan = await GetQueryPlanAsync("SELECT DISTINCT VALUE c.name FROM c");
        var info = plan["queryInfo"]!;

        info["distinctType"]!.ToString().Should().Be("Unordered");
        ((bool)info["hasSelectValue"]!).Should().BeTrue();
    }

    // ──────────────────────────────────────────────────────────────
    // C. Aggregate edge cases
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task QueryPlan_AggregateInArithmeticExpression_DetectsAggregate()
    {
        // Bug test: SUM(c.a) * 2 AS total — the aggregate is inside a BinaryExpression.
        // DetectAggregates should still find it and map alias "total" → "Sum".
        var plan = await GetQueryPlanAsync(
            "SELECT c.category, SUM(c.value) * 2 AS total FROM c GROUP BY c.category");
        var info = plan["queryInfo"]!;

        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().Contain(t => t.ToString() == "Sum");

        var aliasMap = (JObject)info["groupByAliasToAggregateType"]!;
        aliasMap["total"]!.ToString().Should().Be("Sum");
    }

    [Fact]
    public async Task QueryPlan_DuplicateAggregateType_DeduplicatesInArray()
    {
        var plan = await GetQueryPlanAsync(
            "SELECT SUM(c.price) AS sumPrice, SUM(c.quantity) AS sumQty FROM c");
        var info = plan["queryInfo"]!;

        var aggregates = (JArray)info["aggregates"]!;
        // "Sum" should appear only once in the aggregates array
        aggregates.Count(t => t.ToString() == "Sum").Should().Be(1);

        // But both aliases should be mapped
        var aliasMap = (JObject)info["groupByAliasToAggregateType"]!;
        aliasMap["sumPrice"]!.ToString().Should().Be("Sum");
        aliasMap["sumQty"]!.ToString().Should().Be("Sum");
    }

    [Fact]
    public async Task QueryPlan_CountWithoutAlias_StillDetectsAggregate()
    {
        var plan = await GetQueryPlanAsync("SELECT COUNT(1) FROM c");
        var info = plan["queryInfo"]!;

        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().Contain(t => t.ToString() == "Count");
    }

    [Fact]
    public async Task QueryPlan_AggregateFunctionCaseInsensitive_Detected()
    {
        var plan = await GetQueryPlanAsync("SELECT count(1) AS cnt FROM c");
        var info = plan["queryInfo"]!;

        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().Contain(t => t.ToString() == "Count");
    }

    [Fact]
    public async Task QueryPlan_NonAggregateFunction_NotInAggregates()
    {
        var plan = await GetQueryPlanAsync("SELECT UPPER(c.name) AS upperName FROM c");
        var info = plan["queryInfo"]!;

        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().BeEmpty();
    }

    // ──────────────────────────────────────────────────────────────
    // D. GROUP BY edge cases
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task QueryPlan_MultipleGroupByFields_SetsAll()
    {
        var plan = await GetQueryPlanAsync(
            "SELECT c.status, c.region, COUNT(1) AS cnt FROM c GROUP BY c.status, c.region");
        var info = plan["queryInfo"]!;

        var groupBy = (JArray)info["groupByExpressions"]!;
        groupBy.Should().HaveCount(2);
        groupBy.Select(t => t.ToString()).Should().Contain("c.status");
        groupBy.Select(t => t.ToString()).Should().Contain("c.region");
    }

    // ──────────────────────────────────────────────────────────────
    // E. SELECT VALUE + aggregate combination
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task QueryPlan_SelectValueWithAggregate_SetsBothFlags()
    {
        var plan = await GetQueryPlanAsync("SELECT VALUE COUNT(1) FROM c");
        var info = plan["queryInfo"]!;

        ((bool)info["hasSelectValue"]!).Should().BeTrue();

        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().Contain(t => t.ToString() == "Count");
    }

    // ──────────────────────────────────────────────────────────────
    // F. Rewritten query edge cases
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task QueryPlan_NonOrderByQuery_RewrittenQueryIsOriginalSql()
    {
        const string sql = "SELECT * FROM c WHERE c.value > 10";
        var plan = await GetQueryPlanAsync(sql);
        var info = plan["queryInfo"]!;

        info["rewrittenQuery"]!.ToString().Should().Be(sql);
    }

    // ──────────────────────────────────────────────────────────────
    // G. Response structure
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task QueryPlan_ResponseVersion_IsTwo()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c");

        ((int)plan["partitionedQueryExecutionInfoVersion"]!).Should().Be(2);
    }

    [Fact]
    public async Task QueryPlan_ResponseStatusCode_Is200()
    {
        var (_, response) = await GetQueryPlanWithResponseAsync("SELECT * FROM c");

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task QueryPlan_SimpleSelect_AllDefaultFieldsPresent()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c");
        var info = plan["queryInfo"]!;

        // Verify every expected key exists in queryInfo
        info["distinctType"].Should().NotBeNull();
        info["top"].Should().NotBeNull();       // JToken exists, value is null
        info["offset"].Should().NotBeNull();
        info["limit"].Should().NotBeNull();
        info["orderBy"].Should().NotBeNull();
        info["orderByExpressions"].Should().NotBeNull();
        info["groupByExpressions"].Should().NotBeNull();
        info["groupByAliases"].Should().NotBeNull();
        info["aggregates"].Should().NotBeNull();
        info["groupByAliasToAggregateType"].Should().NotBeNull();
        info["rewrittenQuery"].Should().NotBeNull();
        info["hasSelectValue"].Should().NotBeNull();
        info["hasNonStreamingOrderBy"].Should().NotBeNull();

        // Top-level structure
        plan["partitionedQueryExecutionInfoVersion"].Should().NotBeNull();
        plan["queryRanges"].Should().NotBeNull();
    }

    // ──────────────────────────────────────────────────────────────
    // H. General edge cases
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task QueryPlan_QueryWithParameters_ReturnsValidPlan()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c WHERE c.name = @name");
        var info = plan["queryInfo"]!;

        info["distinctType"]!.ToString().Should().Be("None");
        info["rewrittenQuery"]!.ToString().Should().NotBeNullOrEmpty();
        plan["queryRanges"]!.Should().BeOfType<JArray>().Which.Should().NotBeEmpty();
    }

    [Fact]
    public async Task QueryPlan_TopZero_SetsTopToZero()
    {
        var plan = await GetQueryPlanAsync("SELECT TOP 0 * FROM c");
        var info = plan["queryInfo"]!;

        ((int)info["top"]!).Should().Be(0);
    }

    [Fact]
    public async Task QueryPlan_SelectSpecificFields_NoAggregates()
    {
        var plan = await GetQueryPlanAsync("SELECT c.name, c.age FROM c");
        var info = plan["queryInfo"]!;

        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().BeEmpty();
    }

    [Fact]
    public async Task QueryPlan_JoinQuery_DoesNotAffectOrderByOrAggregates()
    {
        var plan = await GetQueryPlanAsync(
            "SELECT t.name FROM c JOIN t IN c.tags");
        var info = plan["queryInfo"]!;

        info["orderBy"]!.Should().BeOfType<JArray>().Which.Should().BeEmpty();
        info["aggregates"]!.Should().BeOfType<JArray>().Which.Should().BeEmpty();
        info["distinctType"]!.ToString().Should().Be("None");
    }
}
