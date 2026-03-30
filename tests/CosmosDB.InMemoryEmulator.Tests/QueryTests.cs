using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

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