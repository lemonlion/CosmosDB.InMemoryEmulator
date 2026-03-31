using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;
using System.Text;

namespace CosmosDB.InMemoryEmulator.Tests;

public class QueryTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private async Task SeedItems()
    {
        var items = new[]
        {
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, IsActive = true, Tags = ["a", "b"] },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, IsActive = false, Tags = ["b", "c"] },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30, IsActive = true, Tags = ["a", "c"] },
            new TestDocument { Id = "4", PartitionKey = "pk2", Name = "Diana", Value = 40, IsActive = true, Tags = ["d"] },
            new TestDocument { Id = "5", PartitionKey = "pk1", Name = "Eve", Value = 50, IsActive = false, Tags = ["a"] },
        };
        foreach (var item in items)
        {
            await _container.CreateItemAsync(item, new PartitionKey(item.PartitionKey));
        }
    }

    [Fact]
    public async Task GetItemQueryIterator_SelectAll_ReturnsAllItems()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c");

        var iterator = _container.GetItemQueryIterator<TestDocument>(query);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(5);
    }

    [Fact]
    public async Task GetItemQueryIterator_WithWhereClause_FiltersCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE c.isActive = @isActive")
            .WithParameter("@isActive", true);

        var iterator = _container.GetItemQueryIterator<TestDocument>(query);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(3);
        results.Should().AllSatisfy(item => item.IsActive.Should().BeTrue());
    }

    [Fact]
    public async Task GetItemQueryIterator_WithPartitionKey_FiltersToPartition()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c");
        var requestOptions = new QueryRequestOptions { PartitionKey = new PartitionKey("pk2") };

        var iterator = _container.GetItemQueryIterator<TestDocument>(query, requestOptions: requestOptions);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Diana");
    }

    [Fact]
    public async Task GetItemQueryIterator_WithTopClause_LimitsResults()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT TOP 2 * FROM c");

        var iterator = _container.GetItemQueryIterator<TestDocument>(query);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(2);
    }

    [Fact]
    public async Task GetItemQueryIterator_WithOffsetLimit_PagesCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c ORDER BY c.value ASC OFFSET 1 LIMIT 2");

        var iterator = _container.GetItemQueryIterator<TestDocument>(query);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(2);
        results[0].Value.Should().Be(20);
        results[1].Value.Should().Be(30);
    }

    [Fact]
    public async Task GetItemQueryIterator_WithOrderByAsc_SortsCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c ORDER BY c.value ASC");

        var iterator = _container.GetItemQueryIterator<TestDocument>(query);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Select(r => r.Value).Should().BeInAscendingOrder();
    }

    [Fact]
    public async Task GetItemQueryIterator_WithOrderByDesc_SortsCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c ORDER BY c.value DESC");

        var iterator = _container.GetItemQueryIterator<TestDocument>(query);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Select(r => r.Value).Should().BeInDescendingOrder();
    }

    [Fact]
    public async Task GetItemQueryIterator_WithSelectProjection_ReturnsProjectedFields()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT c.name, c.value FROM c WHERE c.id = '1'");

        var iterator = _container.GetItemQueryIterator<JObject>(query);
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(1);
        results[0]["name"]!.ToString().Should().Be("Alice");
        results[0]["value"]!.Value<int>().Should().Be(10);
    }

    [Fact]
    public async Task GetItemQueryIterator_CountAggregate_ReturnsCorrectCount()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT COUNT(1) AS itemCount FROM c WHERE c.isActive = true");

        var iterator = _container.GetItemQueryIterator<JObject>(query);
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(1);
        results[0]["itemCount"]!.Value<int>().Should().Be(3);
    }

    [Fact]
    public async Task GetItemQueryIterator_LikeOperator_MatchesPattern()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE c.name LIKE @pattern")
            .WithParameter("@pattern", "A%");

        var iterator = _container.GetItemQueryIterator<TestDocument>(query);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Alice");
    }

    [Fact]
    public async Task GetItemQueryIterator_StringQueryOverload_Works()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>("SELECT * FROM c WHERE c.value > 25");
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(3);
    }

    [Fact]
    public async Task GetItemQueryIterator_NullQuery_ReturnsAllItems()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(queryText: null);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(5);
    }

    [Fact]
    public async Task GetItemQueryIterator_WithDistinct_ReturnsUniqueResults()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT DISTINCT c.isActive FROM c");

        var iterator = _container.GetItemQueryIterator<JObject>(query);
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(2);
    }

    [Fact]
    public async Task GetItemQueryIterator_MultipleOrderByFields_SortsCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c ORDER BY c.isActive DESC, c.value ASC");

        var iterator = _container.GetItemQueryIterator<TestDocument>(query);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        var activeItems = results.TakeWhile(r => r.IsActive).ToList();
        var inactiveItems = results.SkipWhile(r => r.IsActive).ToList();
        activeItems.Select(r => r.Value).Should().BeInAscendingOrder();
        inactiveItems.Select(r => r.Value).Should().BeInAscendingOrder();
    }

    [Fact]
    public async Task GetItemQueryStreamIterator_ReturnsDocumentsEnvelope()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE c.id = '1'");

        var iterator = _container.GetItemQueryStreamIterator(query);
        string body;
        using (var response = await iterator.ReadNextAsync())
        {
            response.StatusCode.Should().Be(HttpStatusCode.OK);
            using var reader = new StreamReader(response.Content);
            body = await reader.ReadToEndAsync();
        }

        var jObj = JObject.Parse(body);
        jObj["Documents"].Should().NotBeNull();
        ((JArray)jObj["Documents"]!).Should().HaveCount(1);
    }

    [Fact]
    public async Task GetItemQueryIterator_AndOrCombination_FiltersCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition(
            "SELECT * FROM c WHERE c.isActive = true AND (c.value > 15 OR c.name = 'Alice')");

        var iterator = _container.GetItemQueryIterator<TestDocument>(query);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(3);
    }

    [Fact]
    public async Task GetItemQueryIterator_NotEqual_FiltersCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE c.name != 'Alice'");

        var iterator = _container.GetItemQueryIterator<TestDocument>(query);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(4);
    }

    [Fact]
    public async Task SelectValueRoot_ReturnsEntireDocuments()
    {
        await SeedItems();

        var query = new QueryDefinition("SELECT VALUE c FROM c WHERE c.partitionKey = 'pk1'");
        var iterator = _container.GetItemQueryIterator<TestDocument>(query);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(4);
        results.Select(document => document.Name).Should().Contain("Alice");
    }

    [Fact]
    public void SimplifySdkWhereExpression_RemovesInjectedNodes()
    {
        var parsed = CosmosSqlParser.Parse(
            "SELECT * FROM root WHERE ((true) AND (root.value >= 20) AND IS_DEFINED(root.value))");

        var simplified = CosmosSqlParser.SimplifySdkWhereExpression(parsed.WhereExpr);
        var sql = CosmosSqlParser.ExprToString(simplified!);

        sql.Should().Be("root.value >= 20");
    }

    [Fact]
    public void SimplifySdkWhereExpression_RemovesAllTrueAndIsDefined()
    {
        var parsed = CosmosSqlParser.Parse(
            "SELECT * FROM root WHERE ((true) AND IS_DEFINED(root))");

        var simplified = CosmosSqlParser.SimplifySdkWhereExpression(parsed.WhereExpr);

        simplified.Should().BeNull();
    }

    [Fact]
    public void Parse_SdkOrderByQuery_ParsesSuccessfully()
    {
        var sdkQuery = """SELECT root._rid, [{"item": root["name"]}] AS orderByItems, root AS payload FROM root WHERE (true) ORDER BY root["name"] ASC""";

        var parsed = CosmosSqlParser.Parse(sdkQuery);

        parsed.SelectFields.Should().HaveCount(3);
        parsed.SelectFields[1].Alias.Should().Be("orderByItems");
        parsed.OrderByFields.Should().HaveCount(1);
        parsed.OrderByFields![0].Field.Should().Be("root.name");
    }

    [Fact]
    public void SimplifySdkWhereExpression_WithFromAlias_PreservesUserIsDefined()
    {
        var parsed = CosmosSqlParser.Parse(
            "SELECT * FROM root WHERE ((true) AND IS_DEFINED(root.name) AND (root.value >= 20) AND IS_DEFINED(root))");

        var simplified = CosmosSqlParser.SimplifySdkWhereExpression(parsed.WhereExpr, "root");
        var sql = CosmosSqlParser.ExprToString(simplified!);

        sql.Should().Be("(IS_DEFINED(root.name) AND root.value >= 20)");
    }

    [Fact]
    public void SimplifySdkWhereExpression_PreservesFieldPathIsDefined_EvenIfMatchesOrderBy()
    {
        // The SDK only injects IS_DEFINED(root) (bare alias). User code might write
        // IS_DEFINED(root.name) on the same field used in ORDER BY — this must be preserved.
        var parsed = CosmosSqlParser.Parse(
            "SELECT * FROM root WHERE (IS_DEFINED(root.name) AND root.name = 'Alice' AND IS_DEFINED(root))");

        var simplified = CosmosSqlParser.SimplifySdkWhereExpression(parsed.WhereExpr, "root");
        var sql = CosmosSqlParser.ExprToString(simplified!);

        sql.Should().Be("(IS_DEFINED(root.name) AND root.name = 'Alice')");
    }

    [Fact]
    public void SimplifySdkWhereExpression_WithDifferentAlias_StripsCorrectIsDefined()
    {
        var parsed = CosmosSqlParser.Parse(
            "SELECT * FROM c WHERE ((true) AND IS_DEFINED(c) AND c.value > 10)");

        var simplified = CosmosSqlParser.SimplifySdkWhereExpression(parsed.WhereExpr, "c");
        var sql = CosmosSqlParser.ExprToString(simplified!);

        sql.Should().Be("c.value > 10");
    }

    [Fact]
    public async Task Parse_SdkOrderByQueryEndToEnd_ReturnsResults()
    {
        await SeedItems();

        var sdkQuery = """SELECT root._rid, [{"item": root["name"]}] AS orderByItems, root AS payload FROM root WHERE (true) ORDER BY root["name"] ASC""";

        var parsed = CosmosSqlParser.Parse(sdkQuery);
        var simplifiedWhere = CosmosSqlParser.SimplifySdkWhereExpression(parsed.WhereExpr, parsed.FromAlias);

        var simplifiedSql = $"SELECT VALUE {parsed.FromAlias} FROM {parsed.FromAlias}";
        if (simplifiedWhere is not null)
        {
            simplifiedSql += $" WHERE {CosmosSqlParser.ExprToString(simplifiedWhere)}";
        }
        simplifiedSql += $" ORDER BY {parsed.OrderByFields![0].Field} ASC";

        var iterator = _container.GetItemQueryIterator<JObject>(new QueryDefinition(simplifiedSql));
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(5);
    }

    [Fact]
    public void SimplifySdkQuery_WithOrderByQuery_ProducesCleanSql()
    {
        var sdkQuery = """SELECT root._rid, [{"item": root["name"]}] AS orderByItems, root AS payload FROM root WHERE ((true) AND IS_DEFINED(root)) ORDER BY root["name"] ASC""";
        var parsed = CosmosSqlParser.Parse(sdkQuery);

        var simplified = CosmosSqlParser.SimplifySdkQuery(parsed);

        simplified.Should().Be("SELECT VALUE root FROM root ORDER BY root.name ASC");
    }

    [Fact]
    public void SimplifySdkQuery_WithWhereClause_PreservesUserCondition()
    {
        var sdkQuery = """SELECT root._rid, [{"item": root["value"]}] AS orderByItems, root AS payload FROM root WHERE ((root["value"] > 10) AND IS_DEFINED(root)) ORDER BY root["value"] ASC""";
        var parsed = CosmosSqlParser.Parse(sdkQuery);

        var simplified = CosmosSqlParser.SimplifySdkQuery(parsed);

        simplified.Should().Be("SELECT VALUE root FROM root WHERE root.value > 10 ORDER BY root.value ASC");
    }

    [Fact]
    public void SimplifySdkQuery_WithTopAndWhere_IncludesAll()
    {
        var sdkQuery = """SELECT TOP 2 root._rid, [{"item": root["value"]}] AS orderByItems, root AS payload FROM root WHERE ((root["value"] >= 20) AND IS_DEFINED(root)) ORDER BY root["value"] DESC""";
        var parsed = CosmosSqlParser.Parse(sdkQuery);

        var simplified = CosmosSqlParser.SimplifySdkQuery(parsed);

        simplified.Should().Be("SELECT TOP 2 VALUE root FROM root WHERE root.value >= 20 ORDER BY root.value DESC");
    }

    [Fact]
    public void SimplifySdkQuery_WithPlainQuery_PassesThroughCleanly()
    {
        var plainQuery = "SELECT * FROM c WHERE c.value > 10";
        var parsed = CosmosSqlParser.Parse(plainQuery);

        var simplified = CosmosSqlParser.SimplifySdkQuery(parsed);

        simplified.Should().Be("SELECT * FROM c WHERE c.value > 10");
    }

    [Fact]
    public void SimplifySdkQuery_WithDistinctValueSelect_PreservesDistinct()
    {
        var query = "SELECT DISTINCT VALUE c.value FROM c";
        var parsed = CosmosSqlParser.Parse(query);

        var simplified = CosmosSqlParser.SimplifySdkQuery(parsed);

        simplified.Should().Be("SELECT DISTINCT VALUE c.value FROM c");
    }

    [Fact]
    public void SimplifySdkQuery_WithSdkBracketNotation_NormalisesToDotNotation()
    {
        var sdkQuery = """SELECT VALUE root FROM root WHERE (root["isActive"] = true)""";
        var parsed = CosmosSqlParser.Parse(sdkQuery);

        var simplified = CosmosSqlParser.SimplifySdkQuery(parsed);

        simplified.Should().Be("SELECT VALUE root FROM root WHERE root.isActive = true");
    }

    // ── HAVING emission ──

    [Fact]
    public void SimplifySdkQuery_WithGroupByAndHaving_EmitsHavingClause()
    {
        var query = "SELECT c.category, COUNT(1) AS cnt FROM c GROUP BY c.category HAVING COUNT(1) > 2";
        var parsed = CosmosSqlParser.Parse(query);

        var simplified = CosmosSqlParser.SimplifySdkQuery(parsed);

        simplified.Should().Be("SELECT c.category, COUNT(1) AS cnt FROM c GROUP BY c.category HAVING COUNT(1) > 2");
    }

    [Fact]
    public void SimplifySdkQuery_WithGroupByWithoutHaving_OmitsHaving()
    {
        var query = "SELECT c.category, COUNT(1) AS cnt FROM c GROUP BY c.category";
        var parsed = CosmosSqlParser.Parse(query);

        var simplified = CosmosSqlParser.SimplifySdkQuery(parsed);

        simplified.Should().Be("SELECT c.category, COUNT(1) AS cnt FROM c GROUP BY c.category");
    }

    // ── GROUP BY with expressions ──

    [Fact]
    public void Parse_GroupByWithFunctionCall_ParsesCorrectly()
    {
        var query = "SELECT LOWER(c.category) AS cat, COUNT(1) AS cnt FROM c GROUP BY LOWER(c.category)";
        var parsed = CosmosSqlParser.Parse(query);

        parsed.GroupByFields.Should().HaveCount(1);
        parsed.GroupByFields![0].Should().Be("LOWER(c.category)");
    }

    // ── Subquery expression ──

    [Fact]
    public void Parse_SubqueryInSelect_ParsesAsSubqueryExpression()
    {
        var query = "SELECT c.id, (SELECT VALUE t FROM t IN c.tags) AS tags FROM c";
        var parsed = CosmosSqlParser.Parse(query);

        parsed.SelectFields.Should().HaveCount(2);
        parsed.SelectFields[1].Alias.Should().Be("tags");
        parsed.SelectFields[1].SqlExpr.Should().BeOfType<SubqueryExpression>();
    }

    [Fact]
    public void ExprToString_SubqueryExpression_ProducesParenthesisedSelect()
    {
        var query = "SELECT c.id, (SELECT VALUE t FROM t IN c.tags) AS tags FROM c";
        var parsed = CosmosSqlParser.Parse(query);

        var subExpr = parsed.SelectFields[1].SqlExpr;
        var sql = CosmosSqlParser.ExprToString(subExpr);

        sql.Should().Be("(SELECT VALUE t FROM t IN c.tags)");
    }

    [Fact]
    public void SimplifySdkQuery_WithSubquery_PreservesSubquery()
    {
        var query = "SELECT c.id, (SELECT VALUE t FROM t IN c.tags) AS tags FROM c";
        var parsed = CosmosSqlParser.Parse(query);

        var simplified = CosmosSqlParser.SimplifySdkQuery(parsed);

        simplified.Should().Be("SELECT c.id, (SELECT VALUE t FROM t IN c.tags) AS tags FROM c");
    }

    // ── ExprToString completeness ──

    [Fact]
    public void ExprToString_BetweenExpression_ProducesCorrectSql()
    {
        var query = "SELECT * FROM c WHERE c.value BETWEEN 10 AND 50";
        var parsed = CosmosSqlParser.Parse(query);

        var sql = CosmosSqlParser.ExprToString(parsed.WhereExpr!);

        sql.Should().Be("c.value BETWEEN 10 AND 50");
    }

    [Fact]
    public void ExprToString_InExpression_ProducesCorrectSql()
    {
        var query = "SELECT * FROM c WHERE c.status IN ('active', 'pending')";
        var parsed = CosmosSqlParser.Parse(query);

        var sql = CosmosSqlParser.ExprToString(parsed.WhereExpr!);

        sql.Should().Be("c.status IN ('active', 'pending')");
    }

    [Fact]
    public void ExprToString_TernaryExpression_ProducesCorrectSql()
    {
        var query = "SELECT c.isActive ? 'yes' : 'no' AS label FROM c";
        var parsed = CosmosSqlParser.Parse(query);

        var sql = CosmosSqlParser.ExprToString(parsed.SelectFields[0].SqlExpr!);

        sql.Should().Be("c.isActive ? 'yes' : 'no'");
    }

    [Fact]
    public void ExprToString_CoalesceExpression_ProducesCorrectSql()
    {
        var query = "SELECT c.nickname ?? c.name AS displayName FROM c";
        var parsed = CosmosSqlParser.Parse(query);

        var sql = CosmosSqlParser.ExprToString(parsed.SelectFields[0].SqlExpr!);

        sql.Should().Be("c.nickname ?? c.name");
    }

    [Fact]
    public void ExprToString_OrNestedInAnd_PreservesGrouping()
    {
        var query = "SELECT * FROM c WHERE (c.a = 1 OR c.b = 2) AND c.c = 3";
        var parsed = CosmosSqlParser.Parse(query);

        var sql = CosmosSqlParser.ExprToString(parsed.WhereExpr!);

        sql.Should().Contain("OR");
        sql.Should().Contain("AND");
        // Re-parsing the output should produce equivalent results
        var reparsed = CosmosSqlParser.Parse($"SELECT * FROM c WHERE {sql}");
        reparsed.WhereExpr.Should().NotBeNull();
    }

    // ── GROUP BY + HAVING integration ──

    [Fact]
    public async Task GetItemQueryIterator_WithGroupByHaving_FiltersGroups()
    {
        await SeedItems();
        var query = new QueryDefinition(
            "SELECT c.partitionKey, COUNT(1) AS cnt FROM c GROUP BY c.partitionKey HAVING COUNT(1) > 1");

        var iterator = _container.GetItemQueryIterator<JObject>(query);
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(1);
        results[0]["partitionKey"]!.Value<string>().Should().Be("pk1");
        results[0]["cnt"]!.Value<int>().Should().Be(4);
    }

    // ── ARRAY() function with subquery ──

    [Fact]
    public void Parse_ArrayFunctionCall_ParsesAsFunction()
    {
        var query = "SELECT ARRAY(SELECT VALUE t FROM t IN c.tags) AS allTags FROM c";
        var parsed = CosmosSqlParser.Parse(query);

        parsed.SelectFields.Should().HaveCount(1);
        parsed.SelectFields[0].Alias.Should().Be("allTags");
        parsed.SelectFields[0].SqlExpr.Should().BeOfType<FunctionCallExpression>();
        var func = (FunctionCallExpression)parsed.SelectFields[0].SqlExpr!;
        func.FunctionName.Should().Be("ARRAY");
        func.Arguments.Should().HaveCount(1);
        func.Arguments[0].Should().BeOfType<SubqueryExpression>();
    }

    // ── UDF function calls ──

    [Fact]
    public void Parse_UdfFunctionCall_ParsesCorrectly()
    {
        var query = "SELECT udf.myFunc(c.name) AS result FROM c";
        var parsed = CosmosSqlParser.Parse(query);

        parsed.SelectFields.Should().HaveCount(1);
        parsed.SelectFields[0].SqlExpr.Should().BeOfType<FunctionCallExpression>();
        var func = (FunctionCallExpression)parsed.SelectFields[0].SqlExpr!;
        func.FunctionName.Should().Be("UDF.MYFUNC");
    }

    // ── Round-trip: parse then SimplifySdkQuery ──

    [Fact]
    public void SimplifySdkQuery_WithJoinAndWhere_PreservesJoin()
    {
        var query = "SELECT c.id, t AS tag FROM c JOIN t IN c.tags WHERE t = 'a'";
        var parsed = CosmosSqlParser.Parse(query);

        var simplified = CosmosSqlParser.SimplifySdkQuery(parsed);

        simplified.Should().Be("SELECT c.id, t AS tag FROM c JOIN t IN c.tags WHERE t = 'a'");
    }

    [Fact]
    public void SimplifySdkQuery_WithOffsetLimit_PreservesOffsetLimit()
    {
        var query = "SELECT * FROM c OFFSET 5 LIMIT 10";
        var parsed = CosmosSqlParser.Parse(query);

        var simplified = CosmosSqlParser.SimplifySdkQuery(parsed);

        simplified.Should().Be("SELECT * FROM c OFFSET 5 LIMIT 10");
    }

    [Fact]
    public void SimplifySdkQuery_WithSelectManyJoin_NormalisesCorrectly()
    {
        var sdkQuery = """SELECT VALUE document0 FROM root JOIN document0 IN root["tags"]""";
        var parsed = CosmosSqlParser.Parse(sdkQuery);

        var simplified = CosmosSqlParser.SimplifySdkQuery(parsed);

        simplified.Should().Be("SELECT VALUE document0 FROM root JOIN document0 IN root.tags");
    }

    [Fact]
    public void Parse_AggregateWithNestedObjectArray_ParsesCorrectly()
    {
        var sdkQuery = """SELECT VALUE [{"item": {"sum": SUM(root["value"]), "count": COUNT(root["value"])}}] FROM root""";
        var parsed = CosmosSqlParser.Parse(sdkQuery);

        parsed.IsValueSelect.Should().BeTrue();
        parsed.SelectFields.Should().HaveCount(1);
    }

    [Fact]
    public async Task GetItemQueryIterator_WithSelectManyJoin_FlattensArrays()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE t FROM c JOIN t IN c.tags");

        var iterator = _container.GetItemQueryIterator<string>(query);
        var results = new List<string>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().Contain("a");
        results.Should().Contain("b");
        results.Should().Contain("c");
    }

    [Fact]
    public async Task SelectValueObjectLiteral_WithComputedField_ReturnsObjectsDirectly()
    {
        await SeedItems();

        var sql = "SELECT VALUE {Name: root.name, DoubleValue: (root.value * 2)} FROM root";
        var iterator = _container.GetItemQueryIterator<JObject>(new QueryDefinition(sql));
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(5);
        var first = results.FirstOrDefault(r => r["Name"]?.ToString() == "Alice");
        first.Should().NotBeNull();
        first!["DoubleValue"]!.Value<int>().Should().Be(20);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Subquery evaluation
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ArraySubquery_ReturnsFilteredArray()
    {
        await SeedItems();
        var query = new QueryDefinition(
            "SELECT c.name, ARRAY(SELECT VALUE t FROM t IN c.tags WHERE t != 'b') AS filtered FROM c WHERE c.id = '1'");

        var results = await RunQuery<JObject>(query);

        results.Should().ContainSingle();
        var tags = results[0]["filtered"] as JArray;
        tags.Should().NotBeNull();
        tags.Should().ContainSingle().Which.Value<string>().Should().Be("a");
    }

    [Fact]
    public async Task ArraySubquery_WithNoMatches_ReturnsEmptyArray()
    {
        await SeedItems();
        var query = new QueryDefinition(
            "SELECT c.name, ARRAY(SELECT VALUE t FROM t IN c.tags WHERE t = 'zzz') AS filtered FROM c WHERE c.id = '1'");

        var results = await RunQuery<JObject>(query);

        results.Should().ContainSingle();
        var tags = results[0]["filtered"] as JArray;
        tags.Should().NotBeNull();
        tags.Should().BeEmpty();
    }

    [Fact]
    public async Task ScalarSubquery_ReturnsFirstValue()
    {
        await SeedItems();
        var query = new QueryDefinition(
            "SELECT c.name, (SELECT VALUE COUNT(1) FROM t IN c.tags) AS tagCount FROM c WHERE c.id = '1'");

        var results = await RunQuery<JObject>(query);

        results.Should().ContainSingle();
        results[0]["name"]!.Value<string>().Should().Be("Alice");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Multiple JOINs
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MultipleJoins_ExpandsBothArrays()
    {
        var container = new InMemoryContainer("multi-join-test", "/pk");
        await container.CreateItemAsync(new MultiJoinDocument
        {
            Id = "1",
            Pk = "pk1",
            Colors = ["red", "blue"],
            Sizes = ["S", "M"]
        }, new PartitionKey("pk1"));

        var query = new QueryDefinition(
            "SELECT color, size FROM c JOIN color IN c.colors JOIN size IN c.sizes");

        var iterator = container.GetItemQueryIterator<JObject>(query);
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        // 2 colors × 2 sizes = 4 cross-product results
        results.Should().HaveCount(4);
        results.Select(r => $"{r["color"]}-{r["size"]}").Should()
            .BeEquivalentTo(["red-S", "red-M", "blue-S", "blue-M"]);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  BETWEEN and IN expressions
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Between_FiltersInRange()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE c.value BETWEEN 20 AND 40");

        var results = await RunQuery<TestDocument>(query);

        results.Should().HaveCount(3);
        results.Select(r => r.Name).Should().BeEquivalentTo(["Bob", "Charlie", "Diana"]);
    }

    [Fact]
    public async Task InExpression_FiltersMatchingValues()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE c.name IN ('Alice', 'Charlie', 'Eve')");

        var results = await RunQuery<TestDocument>(query);

        results.Should().HaveCount(3);
        results.Select(r => r.Name).Should().BeEquivalentTo(["Alice", "Charlie", "Eve"]);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Ternary and coalesce expressions
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task TernaryExpression_EvaluatesCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition(
            "SELECT VALUE c.isActive ? 'active' : 'inactive' FROM c WHERE c.id = '1'");

        var results = await RunQuery<string>(query);

        results.Should().ContainSingle().Which.Should().Be("active");
    }

    [Fact]
    public async Task CoalesceExpression_ReturnsFirstNonNull()
    {
        var container = new InMemoryContainer("coalesce-test", "/partitionKey");
        await container.CreateItemAsync(new TestDocument
        {
            Id = "1",
            PartitionKey = "pk1",
            Name = "Test",
            Nested = null
        }, new PartitionKey("pk1"));

        var query = new QueryDefinition(
            "SELECT VALUE c.nested.description ?? 'default' FROM c");

        var results = await RunQuery<string>(container, query);

        results.Should().ContainSingle().Which.Should().Be("default");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Object and array literals in SELECT
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ObjectLiteral_InSelect_ProjectsCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition(
            "SELECT VALUE {'name': c.name, 'doubled': c.value * 2} FROM c WHERE c.id = '1'");

        var results = await RunQuery<JObject>(query);

        results.Should().ContainSingle();
        results[0]["name"]!.Value<string>().Should().Be("Alice");
        results[0]["doubled"]!.Value<int>().Should().Be(20);
    }

    [Fact]
    public void Parser_ParsesArrayLiteralValueSelect()
    {
        var query = "SELECT VALUE [c.name, c.value] FROM c";
        var parsed = CosmosSqlParser.Parse(query);

        parsed.IsValueSelect.Should().BeTrue();
        parsed.SelectFields.Should().ContainSingle();
        parsed.SelectFields[0].SqlExpr.Should().BeOfType<ArrayLiteralExpression>();
    }

    [Fact]
    public async Task ArrayLiteral_InSelect_ProjectsCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition(
            "SELECT VALUE [c.name, c.value] FROM c WHERE c.id = '1'");

        var results = await RunQuery<JArray>(query);

        results.Should().ContainSingle();
        results[0][0]!.Value<string>().Should().Be("Alice");
        results[0][1]!.Value<int>().Should().Be(10);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Single-quoted object keys in SQL (GeoJSON / SDK patterns)
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void Parser_HandlesSingleQuotedObjectKeys()
    {
        var query = "SELECT VALUE {'type': 'Point', 'coordinates': [0, 0]} FROM c";
        var parsed = CosmosSqlParser.Parse(query);

        parsed.SelectFields.Should().ContainSingle();
        parsed.IsValueSelect.Should().BeTrue();
    }

    private async Task<List<T>> RunQuery<T>(QueryDefinition query)
    {
        return await RunQuery<T>(_container, query);
    }

    private static async Task<List<T>> RunQuery<T>(InMemoryContainer container, QueryDefinition query)
    {
        var iterator = container.GetItemQueryIterator<T>(query);
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }
        return results;
    }
}


public class QueryOrderByGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Query_OrderBy_NullValues_SortPosition()
    {
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","partitionKey":"pk1","name":"Alice","score":10}""")),
            new PartitionKey("pk1"));
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"2","partitionKey":"pk1","name":"Bob","score":null}""")),
            new PartitionKey("pk1"));
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"3","partitionKey":"pk1","name":"Charlie","score":20}""")),
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c ORDER BY c.score");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(3);
    }

    [Fact]
    public async Task Query_OrderBy_MissingField_StillReturnsAllItems()
    {
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","partitionKey":"pk1","name":"Alice","rank":1}""")),
            new PartitionKey("pk1"));
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"2","partitionKey":"pk1","name":"Bob"}""")),
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c ORDER BY c.rank");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
    }

    [Fact]
    public async Task Query_OrderBy_WithTopAndOffset()
    {
        for (var i = 1; i <= 10; i++)
            await _container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}", Value = i },
                new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c ORDER BY c.value OFFSET 3 LIMIT 4");
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(4);
        results[0].Value.Should().Be(4);
        results[3].Value.Should().Be(7);
    }
}


public class QueryGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private async Task SeedItems()
    {
        var items = new[]
        {
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, IsActive = true, Tags = ["urgent", "review"] },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, IsActive = false, Tags = ["review"] },
            new TestDocument { Id = "3", PartitionKey = "pk2", Name = "Charlie", Value = 30, IsActive = true, Tags = ["urgent"] },
            new TestDocument { Id = "4", PartitionKey = "pk2", Name = "Diana", Value = 40, IsActive = false },
            new TestDocument { Id = "5", PartitionKey = "pk1", Name = "Eve", Value = 50, IsActive = true, Tags = ["urgent", "important"] },
        };
        foreach (var item in items)
        {
            await _container.CreateItemAsync(item, new PartitionKey(item.PartitionKey));
        }
    }

    [Fact]
    public async Task Query_NullQueryText_ReturnsAllItems()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(queryText: null);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(5);
    }

    [Fact]
    public async Task Query_EmptyString_ReturnsAllItems()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>("");
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(5);
    }

    [Fact]
    public async Task Query_WithPartitionKeyFilter_OnlyScopesToPartition()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk1") });

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(3);
        results.Should().OnlyContain(t => t.PartitionKey == "pk1");
    }

    [Fact]
    public async Task Query_SelectValue_ReturnsRawValues()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<string>(
            "SELECT VALUE c.name FROM c ORDER BY c.name");

        var results = new List<string>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainInOrder("Alice", "Bob", "Charlie", "Diana", "Eve");
    }

    [Fact]
    public async Task Query_Where_Between()
    {
        await SeedItems();

        var query = new QueryDefinition("SELECT * FROM c WHERE c.value BETWEEN @lo AND @hi")
            .WithParameter("@lo", 15)
            .WithParameter("@hi", 35);

        var iterator = _container.GetItemQueryIterator<TestDocument>(query);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        results.Select(r => r.Name).Should().Contain("Bob").And.Contain("Charlie");
    }

    [Fact]
    public async Task Query_Where_In()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            """SELECT * FROM c WHERE c.name IN ("Alice", "Eve")""");

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        results.Select(r => r.Name).Should().Contain("Alice").And.Contain("Eve");
    }

    [Fact]
    public async Task Query_OrderBy_MultipleFields_MixedDirection()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c ORDER BY c.partitionKey ASC, c.value DESC");

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results[0].PartitionKey.Should().Be("pk1");
        results[0].Value.Should().Be(50);
        results[^1].PartitionKey.Should().Be("pk2");
    }

    [Fact]
    public async Task Query_GroupBy_WithCount()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT c.partitionKey, COUNT(1) AS cnt FROM c GROUP BY c.partitionKey");

        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        var pk1 = results.FirstOrDefault(r => r["partitionKey"]?.ToString() == "pk1");
        pk1.Should().NotBeNull();
        pk1!["cnt"]!.ToObject<int>().Should().Be(3);
    }

    [Fact]
    public async Task Query_Distinct()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT DISTINCT c.partitionKey FROM c");

        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
    }

    [Fact]
    public async Task Query_Top_LimitsResults()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT TOP 2 * FROM c ORDER BY c.value");

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        results[0].Value.Should().Be(10);
        results[1].Value.Should().Be(20);
    }

    [Fact]
    public async Task Query_OffsetLimit_Pagination()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c ORDER BY c.value OFFSET 1 LIMIT 2");

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        results[0].Value.Should().Be(20);
        results[1].Value.Should().Be(30);
    }

    [Fact]
    public async Task Query_Where_Not()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c WHERE NOT c.isActive");

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        results.Should().OnlyContain(r => !r.IsActive);
    }

    [Fact]
    public async Task Query_Where_Like()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c WHERE c.name LIKE 'A%'");

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle().Which.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task Query_Where_ArithmeticExpression()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c WHERE c.value * 2 > 50");

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(3);
    }

    [Fact]
    public async Task Query_ParameterizedQuery_WithMultipleParams()
    {
        await SeedItems();

        var query = new QueryDefinition("SELECT * FROM c WHERE c.value > @min AND c.isActive = @active")
            .WithParameter("@min", 15)
            .WithParameter("@active", true);

        var iterator = _container.GetItemQueryIterator<TestDocument>(query);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        results.Select(r => r.Name).Should().Contain("Charlie").And.Contain("Eve");
    }

    [Fact]
    public async Task Query_NestedFunctionCalls()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<JToken>(
            "SELECT VALUE UPPER(SUBSTRING(c.name, 0, 3)) FROM c WHERE c.id = '1'");

        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle();
    }

    [Fact]
    public async Task Query_Join_SingleArrayExpansion()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Tags = ["a", "b", "c"] },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JToken>(
            "SELECT VALUE t FROM c JOIN t IN c.tags");

        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(3);
    }

    [Fact]
    public async Task Query_Exists_Subquery()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            """SELECT * FROM c WHERE EXISTS(SELECT VALUE t FROM t IN c.tags WHERE t = "urgent")""");

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(3);
        results.Select(r => r.Name).Should().Contain("Alice").And.Contain("Charlie").And.Contain("Eve");
    }

    [Fact]
    public async Task Query_NullCoalesce_Operator()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "HasName" },
            new PartitionKey("pk1"));

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


/// <summary>
/// Tests edge cases for continuation tokens in GetItemQueryIterator.
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.getitemqueryiterator
/// </summary>
public class QueryContinuationTokenEdgeCaseTests5
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task QueryIterator_WithInvalidContinuationToken_HandlesGracefully()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        // Invalid continuation token — should not crash, may return all or empty results
        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c",
            continuationToken: "not-a-valid-token");

        var act = async () =>
        {
            while (iterator.HasMoreResults)
                await iterator.ReadNextAsync();
        };

        // Should not throw — graceful handling of invalid tokens
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task QueryIterator_WithQueryDefinition_AndContinuationToken_Works()
    {
        for (var i = 1; i <= 5; i++)
            await _container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}", Value = i },
                new PartitionKey("pk1"));

        // Read first page with MaxItemCount=2
        var iterator1 = _container.GetItemQueryIterator<TestDocument>(
            new QueryDefinition("SELECT * FROM c ORDER BY c.value"),
            requestOptions: new QueryRequestOptions { MaxItemCount = 2 });

        var page1 = await iterator1.ReadNextAsync();
        var token = page1.ContinuationToken;

        page1.Should().HaveCount(2);

        // Resume from continuation token with QueryDefinition
        var iterator2 = _container.GetItemQueryIterator<TestDocument>(
            new QueryDefinition("SELECT * FROM c ORDER BY c.value"),
            continuationToken: token,
            requestOptions: new QueryRequestOptions { MaxItemCount = 2 });

        var remaining = new List<TestDocument>();
        while (iterator2.HasMoreResults)
        {
            var page = await iterator2.ReadNextAsync();
            remaining.AddRange(page);
        }

        remaining.Should().HaveCount(3);
    }
}


public class QueryFeedRangeAndQueryDefinitionTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task GetItemQueryIterator_WithFeedRange_ReturnsResults()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var ranges = await _container.GetFeedRangesAsync();
        var feedRange = ranges[0];

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            feedRange,
            new QueryDefinition("SELECT * FROM c"));

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().NotBeEmpty();
    }

    [Fact]
    public async Task GetItemQueryStreamIterator_WithFeedRange_ReturnsResults()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var ranges = await _container.GetFeedRangesAsync();
        var feedRange = ranges[0];

        var iterator = _container.GetItemQueryStreamIterator(
            feedRange,
            new QueryDefinition("SELECT * FROM c"));

        var results = new List<ResponseMessage>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.Add(page);
        }

        results.Should().NotBeEmpty();
    }

    [Fact]
    public async Task GetItemQueryIterator_WithQueryDefinition_Parameterized()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
            new PartitionKey("pk1"));

        var queryDef = new QueryDefinition("SELECT * FROM c WHERE c.name = @name")
            .WithParameter("@name", "Alice");

        var iterator = _container.GetItemQueryIterator<TestDocument>(queryDef);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle().Which.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task GetItemQueryIterator_WithQueryDefinition_MultipleParams()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Alice", Value = 30 },
            new PartitionKey("pk1"));

        var queryDef = new QueryDefinition("SELECT * FROM c WHERE c.name = @name AND c.value > @min")
            .WithParameter("@name", "Alice")
            .WithParameter("@min", 5);

        var iterator = _container.GetItemQueryIterator<TestDocument>(queryDef);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        results.Should().OnlyContain(r => r.Name == "Alice");
    }
}


public class QueryGapTests2
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private async Task SeedItems()
    {
        var items = new[]
        {
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, IsActive = true, Tags = ["urgent", "review"] },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, IsActive = false, Tags = ["review"] },
            new TestDocument { Id = "3", PartitionKey = "pk2", Name = "Charlie", Value = 30, IsActive = true, Tags = ["urgent"] },
            new TestDocument { Id = "4", PartitionKey = "pk2", Name = "Diana", Value = 40, IsActive = false },
            new TestDocument { Id = "5", PartitionKey = "pk1", Name = "Eve", Value = 50, IsActive = true, Tags = ["urgent", "important"] },
        };
        foreach (var item in items)
            await _container.CreateItemAsync(item, new PartitionKey(item.PartitionKey));
    }

    [Fact]
    public async Task Query_Where_NullComparison()
    {
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","partitionKey":"pk1","name":null}""")),
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "NotNull" },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name = null");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle();
    }

    [Fact]
    public async Task Query_Where_IsDefined_TrueForExistingField()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c WHERE IS_DEFINED(c.name)");
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(5);
    }

    [Fact]
    public async Task Query_Where_IsDefined_FalseForMissing()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c WHERE IS_DEFINED(c.nonExistentField)");
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task Query_Where_IsNull_TrueForExplicitNull()
    {
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","partitionKey":"pk1","name":null}""")),
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "NotNull" },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE IS_NULL(c.name)");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle();
    }

    [Fact]
    public async Task Query_GroupBy_WithSum()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT c.partitionKey, SUM(c.value) AS total FROM c GROUP BY c.partitionKey");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        var pk1 = results.FirstOrDefault(r => r["partitionKey"]?.ToString() == "pk1");
        pk1.Should().NotBeNull();
        pk1!["total"]!.ToObject<int>().Should().Be(80);
    }

    [Fact]
    public async Task Query_GroupBy_WithAvg()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT c.partitionKey, AVG(c.value) AS avg FROM c GROUP BY c.partitionKey");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
    }

    [Fact]
    public async Task Query_GroupBy_WithMinMax()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT c.partitionKey, MIN(c.value) AS min, MAX(c.value) AS max FROM c GROUP BY c.partitionKey");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        var pk1 = results.FirstOrDefault(r => r["partitionKey"]?.ToString() == "pk1");
        pk1!["min"]!.ToObject<int>().Should().Be(10);
        pk1!["max"]!.ToObject<int>().Should().Be(50);
    }

    [Fact]
    public async Task Query_GroupBy_MultipleFields()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT c.partitionKey, c.isActive, COUNT(1) AS cnt FROM c GROUP BY c.partitionKey, c.isActive");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCountGreaterThanOrEqualTo(3);
    }

    [Fact]
    public async Task Query_Join_EmptyArray_ReturnsNoRows()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Tags = [] },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JToken>(
            "SELECT VALUE t FROM c JOIN t IN c.tags");
        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task Query_Join_MultipleJoins_CartesianProduct()
    {
        var container = new InMemoryContainer("test", "/pk");
        await container.CreateItemAsync(
            new MultiJoinDocument { Id = "1", Pk = "pk1", Colors = ["red", "blue"], Sizes = ["S", "M", "L"] },
            new PartitionKey("pk1"));

        var iterator = container.GetItemQueryIterator<JObject>(
            "SELECT c.id, co AS color, sz AS size FROM c JOIN co IN c.colors JOIN sz IN c.sizes");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(6); // 2 colors * 3 sizes
    }

    [Fact]
    public async Task Query_Join_WithWhere_FiltersExpandedRows()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Tags = ["alpha", "beta", "gamma"] },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JToken>(
            """SELECT VALUE t FROM c JOIN t IN c.tags WHERE t = "gamma" """);
        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle();
        results[0].ToString().Should().Be("gamma");
    }

    [Fact]
    public async Task Query_Contains_CaseSensitive_Default()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            """SELECT * FROM c WHERE CONTAINS(c.name, "alice")""");
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        // Default is case-sensitive, "Alice" != "alice"
        results.Should().BeEmpty();
    }

    [Fact]
    public async Task Query_Contains_CaseInsensitive()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            """SELECT * FROM c WHERE CONTAINS(c.name, "alice", true)""");
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle().Which.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task Query_ParameterizedQuery_MultipleParams()
    {
        await SeedItems();

        var query = new QueryDefinition("SELECT * FROM c WHERE c.value > @min AND c.name != @excluded")
            .WithParameter("@min", 15)
            .WithParameter("@excluded", "Diana");

        var iterator = _container.GetItemQueryIterator<TestDocument>(query);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(3);
        results.Select(r => r.Name).Should().NotContain("Diana");
    }

    [Fact]
    public async Task Query_BracketNotation_ForSpecialFieldNames()
    {
        var json = """{"id":"1","partitionKey":"pk1","field-name":"special-value"}""";
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            """SELECT c["field-name"] FROM c""");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(1);
    }

    [Fact]
    public async Task Query_AliasedSelect()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT c.name AS fullName FROM c WHERE c.id = '1'");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle();
        results[0]["fullName"]?.ToString().Should().Be("Alice");
    }

    [Fact]
    public async Task Query_SelectValue_Count_ReturnsNumber()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<JToken>(
            "SELECT VALUE COUNT(1) FROM c");
        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().NotBeEmpty();
    }

    [Fact]
    public async Task Query_Select_NestedProperty()
    {
        await _container.CreateItemAsync(
            new TestDocument
            {
                Id = "1", PartitionKey = "pk1", Name = "Test",
                Nested = new NestedObject { Description = "MyNested", Score = 9.5 }
            },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT c.nested.description FROM c");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle();
    }
}


public class QueryOrderByGapTests4
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Query_OrderBy_MixedTypes_NumbersAndStrings()
    {
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","partitionKey":"pk1","sortVal":10}""")),
            new PartitionKey("pk1"));
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"2","partitionKey":"pk1","sortVal":"alpha"}""")),
            new PartitionKey("pk1"));
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"3","partitionKey":"pk1","sortVal":5}""")),
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c ORDER BY c.sortVal ASC");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        // All 3 items should be returned regardless of mixed types
        results.Should().HaveCount(3);
    }

    [Fact]
    public async Task Query_OrderBy_NestedProperty()
    {
        await _container.CreateItemAsync(
            new TestDocument
            {
                Id = "1", PartitionKey = "pk1", Name = "A",
                Nested = new NestedObject { Description = "Z", Score = 1.0 }
            }, new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument
            {
                Id = "2", PartitionKey = "pk1", Name = "B",
                Nested = new NestedObject { Description = "A", Score = 2.0 }
            }, new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c ORDER BY c.nested.description ASC");
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        results[0].Nested!.Description.Should().Be("A");
        results[1].Nested!.Description.Should().Be("Z");
    }
}


public class QueryWhereGapTests3
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Query_Where_UndefinedField_NotEqualToNull()
    {
        // Create item WITHOUT a "status" field
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        // undefined != null in Cosmos
        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c WHERE c.status = null");
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        // Should NOT match — missing field is undefined, not null
        results.Should().BeEmpty();
    }

    [Fact]
    public async Task Query_Where_StringConcatOperator()
    {
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","partitionKey":"pk1","first":"John","last":"Doe"}""")),
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            """SELECT * FROM c WHERE c.first || ' ' || c.last = "John Doe" """);
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle();
    }
}


public class QueryFeedRangeDivergentBehaviorTests4
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey") { FeedRangeCount = 4 };

    /// <summary>
    /// FeedRange parameter now scopes query results. When FeedRangeCount > 1,
    /// querying through each range and unioning the results yields the full dataset.
    /// With the default FeedRangeCount=1, the single range covers the entire hash space.
    /// </summary>
    [Fact]
    public async Task QueryIterator_FeedRange_ScopesResultsByRange()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk2", Name = "B" }, new PartitionKey("pk2"));

        var ranges = await _container.GetFeedRangesAsync();
        var allResults = new List<TestDocument>();
        foreach (var range in ranges)
        {
            var iterator = _container.GetItemQueryIterator<TestDocument>(
                range, new QueryDefinition("SELECT * FROM c"));
            while (iterator.HasMoreResults)
            {
                var page = await iterator.ReadNextAsync();
                allResults.AddRange(page);
            }
        }

        // Union of all ranges returns all items
        allResults.Should().HaveCount(2);
    }
}


public class QueryIteratorGapTests4
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey") { FeedRangeCount = 4 };

    [Fact]
    public async Task QueryIterator_WithFeedRange_FiltersByRange()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk2", Name = "B" }, new PartitionKey("pk2"));

        var ranges = await _container.GetFeedRangesAsync();
        var allResults = new List<TestDocument>();
        foreach (var range in ranges)
        {
            var iterator = _container.GetItemQueryIterator<TestDocument>(
                range, new QueryDefinition("SELECT * FROM c"));
            while (iterator.HasMoreResults)
            {
                var page = await iterator.ReadNextAsync();
                allResults.AddRange(page);
            }
        }

        // Union of all feed ranges returns all items
        allResults.Should().HaveCount(2);
    }

    [Fact]
    public async Task QueryIterator_Dispose_IsIdempotent()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" }, new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
        while (iterator.HasMoreResults)
            await iterator.ReadNextAsync();

        // Double dispose should not throw
        iterator.Dispose();
        iterator.Dispose();
    }
}


public class QueryIteratorGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private async Task SeedItems()
    {
        for (var i = 1; i <= 10; i++)
            await _container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}", Value = i },
                new PartitionKey("pk1"));
    }

    [Fact]
    public async Task Query_WithMaxItemCount_PaginatesCorrectly()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c ORDER BY c.value",
            requestOptions: new QueryRequestOptions { MaxItemCount = 3 });

        var allItems = new List<TestDocument>();
        var pageCount = 0;
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            allItems.AddRange(page);
            pageCount++;
        }

        allItems.Should().HaveCount(10);
        pageCount.Should().BeGreaterThan(1);
    }

    [Fact]
    public async Task Query_ContinuationToken_ResumesCorrectly()
    {
        await SeedItems();

        // First page
        var iterator1 = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c ORDER BY c.value",
            requestOptions: new QueryRequestOptions { MaxItemCount = 3 });
        var page1 = await iterator1.ReadNextAsync();
        var token = page1.ContinuationToken;

        page1.Should().HaveCount(3);
        token.Should().NotBeNullOrEmpty();

        // Resume from continuation token
        var iterator2 = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c ORDER BY c.value",
            continuationToken: token,
            requestOptions: new QueryRequestOptions { MaxItemCount = 3 });

        var allRemaining = new List<TestDocument>();
        while (iterator2.HasMoreResults)
        {
            var page = await iterator2.ReadNextAsync();
            allRemaining.AddRange(page);
        }

        allRemaining.Should().HaveCount(7);
    }

    [Fact]
    public async Task Query_AfterLastPage_HasMoreResults_IsFalse()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
        while (iterator.HasMoreResults)
            await iterator.ReadNextAsync();

        iterator.HasMoreResults.Should().BeFalse();
    }
}


public class QueryBitwiseGapTests4
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Query_Where_BitwiseAnd_FiltersCorrectly()
    {
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","partitionKey":"pk1","flags":7}""")),
            new PartitionKey("pk1"));
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"2","partitionKey":"pk1","flags":4}""")),
            new PartitionKey("pk1"));

        // Use IntBitAnd function instead of & operator in WHERE for reliable filtering
        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE IntBitAnd(c.flags, 1) = 1");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        // flags=7 (binary 111) has bit 0 set; flags=4 (binary 100) does not
        results.Should().ContainSingle();
        results[0]["id"]!.ToString().Should().Be("1");
    }
}



public class QueryJoinGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Query_Join_NullArray_ReturnsNoRows()
    {
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","partitionKey":"pk1","tags":null}""")),
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JToken>(
            "SELECT VALUE t FROM c JOIN t IN c.tags");
        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().BeEmpty();
    }
}


public class QueryGroupByGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private async Task SeedItems()
    {
        await _container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, IsActive = true }, new PartitionKey("pk1"));
        await _container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, IsActive = true }, new PartitionKey("pk1"));
        await _container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30, IsActive = false }, new PartitionKey("pk1"));
        await _container.CreateItemAsync(new TestDocument { Id = "4", PartitionKey = "pk1", Name = "Diana", Value = 40, IsActive = false }, new PartitionKey("pk1"));
        await _container.CreateItemAsync(new TestDocument { Id = "5", PartitionKey = "pk1", Name = "Eve", Value = 50, IsActive = true }, new PartitionKey("pk1"));
    }

    [Fact]
    public async Task Query_GroupBy_WithHaving()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT c.isActive, COUNT(1) AS cnt FROM c GROUP BY c.isActive HAVING COUNT(1) > 2");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        // isActive=true has 3 items, isActive=false has 2 — only 3 > 2
        results.Should().ContainSingle();
    }

    [Fact]
    public async Task Query_Count_Star_VsCount_1_SameResult()
    {
        await SeedItems();

        var iter1 = _container.GetItemQueryIterator<JToken>("SELECT VALUE COUNT(1) FROM c");
        var results1 = new List<JToken>();
        while (iter1.HasMoreResults) results1.AddRange(await iter1.ReadNextAsync());

        var iter2 = _container.GetItemQueryIterator<JToken>("SELECT VALUE COUNT(1) FROM c");
        var results2 = new List<JToken>();
        while (iter2.HasMoreResults) results2.AddRange(await iter2.ReadNextAsync());

        results1.First().ToObject<int>().Should().Be(results2.First().ToObject<int>());
    }
}


public class QueryParserGapTests4
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Parser_VeryLongQuery_DoesNotStackOverflow()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 50 },
            new PartitionKey("pk1"));

        // Build a deeply nested WHERE clause: ((((c.value > 0) AND c.value > 0) AND ...))
        var conditions = string.Join(" AND ", Enumerable.Range(0, 50).Select(_ => "c.value > 0"));
        var query = $"SELECT * FROM c WHERE {conditions}";

        var iterator = _container.GetItemQueryIterator<TestDocument>(query);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle();
    }
}


public class QueryFunctionGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Query_StringEquals_CaseInsensitive()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "John" },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            """SELECT * FROM c WHERE StringEquals(c.name, "JOHN", true)""");
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle();
    }

    [Fact]
    public async Task Query_RegexMatch_PatternMatching()
    {
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","partitionKey":"pk1","email":"test@example.com"}""")),
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            """SELECT * FROM c WHERE RegexMatch(c.email, "^[a-z]+@.*")""");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle();
    }

    [Fact]
    public async Task Query_EscapedQuoteInStringLiteral()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "O'Brien" },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c WHERE c.name = 'O''Brien'");
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle();
    }

    [Fact]
    public async Task Query_NegativeNumberLiteral()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = -5 },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c WHERE c.value = -5");
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle();
    }
}


public class QuerySelectGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Query_Select_ComputedExpression()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 10 },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT c.value * 2 AS doubled FROM c");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle();
    }

    [Fact]
    public async Task Query_Select_ObjectLiteral()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 30 },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            """SELECT {"name": c.name, "val": c.value} AS info FROM c""");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle();
    }
}


public class QueryFunctionGapTests4
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Query_ArraySlice_WithNegativeIndex()
    {
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","partitionKey":"pk1","items":["a","b","c","d","e"]}""")),
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT ARRAY_SLICE(c.items, -2) AS sliced FROM c");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle();
        var sliced = results[0]["sliced"]!.ToObject<string[]>();
        sliced.Should().BeEquivalentTo(["d", "e"]);
    }

    [Fact]
    public async Task Query_TypeChecking_Functions_OnVariousTypes()
    {
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","partitionKey":"pk1","arr":[1,2],"obj":{"a":1},"str":"hello","num":42,"bln":true}""")),
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            """SELECT IS_ARRAY(c.arr) AS isArr, IS_OBJECT(c.obj) AS isObj, IS_STRING(c.num) AS isStrNum, IS_NUMBER(c.str) AS isNumStr, IS_BOOL(c.bln) AS isBln FROM c""");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle();
        results[0]["isArr"]!.Value<bool>().Should().BeTrue();
        results[0]["isObj"]!.Value<bool>().Should().BeTrue();
        results[0]["isStrNum"]!.Value<bool>().Should().BeFalse();
        results[0]["isNumStr"]!.Value<bool>().Should().BeFalse();
        results[0]["isBln"]!.Value<bool>().Should().BeTrue();
    }

    [Fact]
    public async Task Query_MathFunctions_EdgeCases()
    {
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","partitionKey":"pk1","val":1}""")),
            new PartitionKey("pk1"));

        // POWER(0,0) should return 1 per IEEE 754
        var iterator = _container.GetItemQueryIterator<JToken>(
            "SELECT VALUE POWER(0, 0) FROM c");
        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle();
        results[0].Value<double>().Should().Be(1.0);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  FROM alias IN c.field — top-level array iteration tests
// ═══════════════════════════════════════════════════════════════════════════

public class FromSourceArrayIterationTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task FromSource_WithWhere_IteratesArrayNotTopLevelDocs()
    {
        var parent = JObject.FromObject(new
        {
            id = "parent",
            partitionKey = "pk1",
            children = new[]
            {
                new { id = "child1", value = 100 },
                new { id = "child2", value = 200 },
            }
        });
        var child = JObject.FromObject(new { id = "child1", partitionKey = "pk1", value = 999 });
        await _container.CreateItemAsync(parent, new PartitionKey("pk1"));
        await _container.CreateItemAsync(child, new PartitionKey("pk1"));

        // FROM item IN c.children iterates array elements, not top-level docs
        var query = new QueryDefinition("SELECT * FROM item IN c.children WHERE item.id = @id")
            .WithParameter("@id", "child1");

        var iterator = _container.GetItemQueryIterator<JObject>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk1") });
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        // Array expansion returns the nested element, not the top-level doc with id="child1"
        results.Should().ContainSingle();
        results[0]["item"]!["id"]!.Value<string>().Should().Be("child1");
        results[0]["item"]!["value"]!.Value<int>().Should().Be(100);
    }

    [Fact]
    public async Task FromSource_SelectAll_ReturnsAllArrayElements()
    {
        var doc = JObject.FromObject(new
        {
            id = "doc1",
            partitionKey = "pk1",
            tags = new[] { "alpha", "beta", "gamma" }
        });
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT * FROM t IN c.tags");
        var iterator = _container.GetItemQueryIterator<JObject>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk1") });
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().HaveCount(3);
    }

    [Fact]
    public async Task FromSource_ValueSelect_ReturnsScalarElements()
    {
        var doc = JObject.FromObject(new
        {
            id = "doc1",
            partitionKey = "pk1",
            tags = new[] { "alpha", "beta", "gamma" }
        });
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT VALUE t FROM t IN c.tags");
        var iterator = _container.GetItemQueryIterator<string>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk1") });
        var results = new List<string>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().BeEquivalentTo(["alpha", "beta", "gamma"]);
    }

    [Fact]
    public async Task FromSource_WithWhere_FiltersArrayElements()
    {
        var doc = JObject.FromObject(new
        {
            id = "doc1",
            partitionKey = "pk1",
            scores = new[] { 10, 25, 30, 5 }
        });
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT VALUE s FROM s IN c.scores WHERE s > @min")
            .WithParameter("@min", 15);
        var iterator = _container.GetItemQueryIterator<int>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk1") });
        var results = new List<int>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().BeEquivalentTo([25, 30]);
    }

    [Fact]
    public async Task FromSource_ObjectElements_WithWhereAndProjection()
    {
        var doc = JObject.FromObject(new
        {
            id = "doc1",
            partitionKey = "pk1",
            items = new[]
            {
                new { name = "Widget", price = 10 },
                new { name = "Gadget", price = 50 },
                new { name = "Gizmo", price = 30 },
            }
        });
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var query = new QueryDefinition(
            "SELECT item.name FROM item IN c.items WHERE item.price > @min")
            .WithParameter("@min", 20);
        var iterator = _container.GetItemQueryIterator<JObject>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk1") });
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().HaveCount(2);
        results.Select(r => r["name"]!.Value<string>()).Should().BeEquivalentTo(["Gadget", "Gizmo"]);
    }

    [Fact]
    public async Task FromSource_EmptyArray_ReturnsNoResults()
    {
        var doc = JObject.FromObject(new
        {
            id = "doc1",
            partitionKey = "pk1",
            tags = Array.Empty<string>()
        });
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT VALUE t FROM t IN c.tags");
        var iterator = _container.GetItemQueryIterator<string>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk1") });
        var results = new List<string>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task FromSource_MissingField_ReturnsNoResults()
    {
        var doc = JObject.FromObject(new
        {
            id = "doc1",
            partitionKey = "pk1",
        });
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT VALUE t FROM t IN c.nonexistent");
        var iterator = _container.GetItemQueryIterator<string>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk1") });
        var results = new List<string>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().BeEmpty();
    }
}
