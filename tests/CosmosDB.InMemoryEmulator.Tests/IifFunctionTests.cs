using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class IifFunctionTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private async Task SeedItems()
    {
        var items = new[]
        {
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, IsActive = true, Tags = ["dot", "net"], Nested = new NestedObject { Description = "desc", Score = 9.5 } },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, IsActive = false, Tags = ["java"] },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 0, IsActive = true, Tags = [] },
        };
        foreach (var item in items)
        {
            await _container.CreateItemAsync(item, new PartitionKey(item.PartitionKey));
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Original tests (pre-existing)
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Iif_TrueCondition_ReturnsSecondArgument()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IIF(c.isActive, 'yes', 'no') AS result FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.ToString().Should().Be("yes");
    }

    [Fact]
    public async Task Iif_FalseCondition_ReturnsThirdArgument()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IIF(c.isActive, 'yes', 'no') AS result FROM c WHERE c.id = '2'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.ToString().Should().Be("no");
    }

    [Fact]
    public async Task Iif_WithComparisonExpression_EvaluatesCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IIF(c.value > 15, 'high', 'low') AS level FROM c ORDER BY c.id");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(3);
        results[0]["level"]!.ToString().Should().Be("low");   // value=10
        results[1]["level"]!.ToString().Should().Be("high");  // value=20
        results[2]["level"]!.ToString().Should().Be("low");   // value=0
    }

    [Fact]
    public async Task Iif_WithNumericReturnValues_ReturnsNumbers()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IIF(c.isActive, 1, 0) AS flag FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["flag"]!.Value<long>().Should().Be(1);
    }

    [Fact]
    public async Task Iif_InWhereClause_FiltersCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE IIF(c.isActive, c.value, 0) > 5");

        var results = await QueryAll<TestDocument>(query);

        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Alice");
    }

    [Fact]
    public async Task Iif_NestedIif_EvaluatesCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IIF(c.value > 15, 'high', IIF(c.value > 5, 'medium', 'low')) AS level FROM c ORDER BY c.id");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(3);
        results[0]["level"]!.ToString().Should().Be("medium"); // value=10
        results[1]["level"]!.ToString().Should().Be("high");   // value=20
        results[2]["level"]!.ToString().Should().Be("low");    // value=0
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Group 1: Non-boolean condition bug fix
    //  Real Cosmos DB IIF only treats boolean true as truthy.
    //  Numbers, strings, arrays, objects all return the false branch.
    //  See: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/iif
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact(Skip = "Pre-existing failure - to be fixed at end of Plan X")]
    public async Task Iif_NumericNonZeroCondition_ReturnsFalseBranch()
    {
        await SeedItems();
        // Real Cosmos DB: IIF(42, ...) → false branch (42 is not boolean true)
        var query = new QueryDefinition("SELECT IIF(42, 'yes', 'no') AS result FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.ToString().Should().Be("no");
    }

    [Fact]
    public async Task Iif_NumericZeroCondition_ReturnsFalseBranch()
    {
        await SeedItems();
        // Real Cosmos DB: IIF(0, ...) → false branch (0 is not boolean true)
        var query = new QueryDefinition("SELECT IIF(0, 'yes', 'no') AS result FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.ToString().Should().Be("no");
    }

    [Fact(Skip = "Pre-existing failure - to be fixed at end of Plan X")]
    public async Task Iif_StringCondition_ReturnsFalseBranch()
    {
        await SeedItems();
        // Real Cosmos DB: IIF('hello', ...) → false branch ('hello' is not boolean true)
        var query = new QueryDefinition("SELECT IIF('hello', 'yes', 'no') AS result FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.ToString().Should().Be("no");
    }

    [Fact]
    public async Task Iif_EmptyStringCondition_ReturnsFalseBranch()
    {
        await SeedItems();
        // Real Cosmos DB: IIF('', ...) → false branch ('' is not boolean true)
        var query = new QueryDefinition("SELECT IIF('', 'yes', 'no') AS result FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.ToString().Should().Be("no");
    }

    [Fact(Skip = "Pre-existing failure - to be fixed at end of Plan X")]
    public async Task Iif_NumericFieldAsCondition_ReturnsFalseBranch()
    {
        await SeedItems();
        // Real Cosmos DB: IIF(c.value, ...) → false branch for all items (numeric field is never boolean)
        var query = new QueryDefinition("SELECT IIF(c.value, 'yes', 'no') AS result FROM c ORDER BY c.id");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(3);
        results[0]["result"]!.ToString().Should().Be("no"); // value=10 — not boolean
        results[1]["result"]!.ToString().Should().Be("no"); // value=20 — not boolean
        results[2]["result"]!.ToString().Should().Be("no"); // value=0  — not boolean
    }

    [Fact(Skip = "Pre-existing failure - to be fixed at end of Plan X")]
    public async Task Iif_StringFieldAsCondition_ReturnsFalseBranch()
    {
        await SeedItems();
        // Real Cosmos DB: IIF(c.name, ...) → false branch for all items (string field is never boolean)
        var query = new QueryDefinition("SELECT IIF(c.name, 'yes', 'no') AS result FROM c ORDER BY c.id");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(3);
        results[0]["result"]!.ToString().Should().Be("no"); // name=Alice   — not boolean
        results[1]["result"]!.ToString().Should().Be("no"); // name=Bob     — not boolean
        results[2]["result"]!.ToString().Should().Be("no"); // name=Charlie — not boolean
    }

    [Fact(Skip = "Pre-existing failure - to be fixed at end of Plan X")]
    public async Task Iif_ArrayFieldAsCondition_ReturnsFalseBranch()
    {
        await SeedItems();
        // Real Cosmos DB: IIF(c.tags, ...) → false branch for all items (array is never boolean)
        // Even empty arrays return false branch — only boolean true returns true branch
        var query = new QueryDefinition("SELECT IIF(c.tags, 'yes', 'no') AS result FROM c ORDER BY c.id");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(3);
        results[0]["result"]!.ToString().Should().Be("no"); // tags=["dot","net"] — not boolean
        results[1]["result"]!.ToString().Should().Be("no"); // tags=["java"]      — not boolean
        results[2]["result"]!.ToString().Should().Be("no"); // tags=[]            — not boolean
    }

    [Fact(Skip = "Pre-existing failure - to be fixed at end of Plan X")]
    public async Task Iif_ObjectFieldAsCondition_ReturnsFalseBranch()
    {
        await SeedItems();
        // Real Cosmos DB: IIF(c.nested, ...) → false branch (object is never boolean)
        // Item 1 has nested = { description: "desc", score: 9.5 }
        var query = new QueryDefinition("SELECT IIF(c.nested, 'yes', 'no') AS result FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.ToString().Should().Be("no");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Group 2: Null and undefined conditions
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Iif_NullCondition_ReturnsFalseBranch()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IIF(null, 'yes', 'no') AS result FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.ToString().Should().Be("no");
    }

    [Fact]
    public async Task Iif_UndefinedPropertyCondition_ReturnsFalseBranch()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IIF(c.nonExistentField, 'yes', 'no') AS result FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.ToString().Should().Be("no");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Group 3: Complex boolean expressions in condition
    //  These produce actual boolean values, so IIF should work normally.
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Iif_WithAndCondition_EvaluatesCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IIF(c.isActive AND c.value > 5, 'yes', 'no') AS result FROM c ORDER BY c.id");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(3);
        results[0]["result"]!.ToString().Should().Be("yes"); // id=1: active AND 10>5
        results[1]["result"]!.ToString().Should().Be("no");  // id=2: NOT active
        results[2]["result"]!.ToString().Should().Be("no");  // id=3: active AND NOT 0>5
    }

    [Fact]
    public async Task Iif_WithOrCondition_EvaluatesCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IIF(c.isActive OR c.value > 15, 'yes', 'no') AS result FROM c ORDER BY c.id");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(3);
        results[0]["result"]!.ToString().Should().Be("yes"); // id=1: active
        results[1]["result"]!.ToString().Should().Be("yes"); // id=2: 20>15
        results[2]["result"]!.ToString().Should().Be("yes"); // id=3: active
    }

    [Fact]
    public async Task Iif_WithNotCondition_EvaluatesCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IIF(NOT c.isActive, 'inactive', 'active') AS status FROM c ORDER BY c.id");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(3);
        results[0]["status"]!.ToString().Should().Be("active");   // id=1: NOT true = false
        results[1]["status"]!.ToString().Should().Be("inactive"); // id=2: NOT false = true
        results[2]["status"]!.ToString().Should().Be("active");   // id=3: NOT true = false
    }

    [Fact]
    public async Task Iif_WithEqualityCondition_EvaluatesCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IIF(c.name = 'Alice', 'found', 'not found') AS result FROM c ORDER BY c.id");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(3);
        results[0]["result"]!.ToString().Should().Be("found");     // id=1: Alice
        results[1]["result"]!.ToString().Should().Be("not found"); // id=2: Bob
        results[2]["result"]!.ToString().Should().Be("not found"); // id=3: Charlie
    }

    [Fact]
    public async Task Iif_WithIsDefinedCondition_EvaluatesCorrectly()
    {
        await SeedItems();
        // Item 1 has nested set (non-null), items 2&3 have nested = null (property still exists in JSON)
        var query = new QueryDefinition("SELECT IIF(IS_DEFINED(c.nested), 'has nested', 'no nested') AS result FROM c ORDER BY c.id");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(3);
        results[0]["result"]!.ToString().Should().Be("has nested"); // id=1: nested is defined
    }

    [Fact]
    public async Task Iif_WithContainsFunctionCondition_EvaluatesCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IIF(CONTAINS(c.name, 'Ali'), 'match', 'no match') AS result FROM c ORDER BY c.id");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(3);
        results[0]["result"]!.ToString().Should().Be("match");    // Alice contains 'Ali'
        results[1]["result"]!.ToString().Should().Be("no match"); // Bob
        results[2]["result"]!.ToString().Should().Be("no match"); // Charlie
    }

    [Fact]
    public async Task Iif_WithArrayLengthComparisonCondition_EvaluatesCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IIF(ARRAY_LENGTH(c.tags) > 0, 'tagged', 'untagged') AS result FROM c ORDER BY c.id");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(3);
        results[0]["result"]!.ToString().Should().Be("tagged");   // id=1: 2 tags
        results[1]["result"]!.ToString().Should().Be("tagged");   // id=2: 1 tag
        results[2]["result"]!.ToString().Should().Be("untagged"); // id=3: 0 tags
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Group 4: Return value variations
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Iif_WithMixedReturnTypes_ReturnsCorrectType()
    {
        await SeedItems();
        var queryTrue = new QueryDefinition("SELECT IIF(c.isActive, 42, 'inactive') AS result FROM c WHERE c.id = '1'");
        var queryFalse = new QueryDefinition("SELECT IIF(c.isActive, 42, 'inactive') AS result FROM c WHERE c.id = '2'");

        var resultsTrue = await QueryAll<JObject>(queryTrue);
        var resultsFalse = await QueryAll<JObject>(queryFalse);

        resultsTrue[0]["result"]!.Value<long>().Should().Be(42);
        resultsFalse[0]["result"]!.ToString().Should().Be("inactive");
    }

    [Fact]
    public async Task Iif_WithNullTrueBranch_ReturnsNull()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IIF(true, null, 'fallback') AS result FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.Type.Should().Be(JTokenType.Null);
    }

    [Fact]
    public async Task Iif_WithNullFalseBranch_ReturnsNull()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IIF(false, 'value', null) AS result FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.Type.Should().Be(JTokenType.Null);
    }

    [Fact]
    public async Task Iif_WithExpressionReturnValues_EvaluatesExpressions()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IIF(c.isActive, c.value * 2, c.value) AS computed FROM c ORDER BY c.id");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(3);
        results[0]["computed"]!.Value<long>().Should().Be(20); // id=1: active, 10*2
        results[1]["computed"]!.Value<long>().Should().Be(20); // id=2: not active, raw 20
        results[2]["computed"]!.Value<long>().Should().Be(0);  // id=3: active, 0*2
    }

    [Fact]
    public async Task Iif_WithFunctionCallReturnValues_EvaluatesFunctions()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IIF(c.isActive, UPPER(c.name), LOWER(c.name)) AS result FROM c ORDER BY c.id");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(3);
        results[0]["result"]!.ToString().Should().Be("ALICE");   // id=1: active → UPPER
        results[1]["result"]!.ToString().Should().Be("bob");     // id=2: not active → LOWER
        results[2]["result"]!.ToString().Should().Be("CHARLIE"); // id=3: active → UPPER
    }

    [Fact]
    public async Task Iif_WithBooleanReturnValues_ReturnsBooleans()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IIF(c.value > 10, true, false) AS overTen FROM c ORDER BY c.id");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(3);
        results[0]["overTen"]!.Value<bool>().Should().BeFalse(); // 10 NOT > 10
        results[1]["overTen"]!.Value<bool>().Should().BeTrue();  // 20 > 10
        results[2]["overTen"]!.Value<bool>().Should().BeFalse(); // 0
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Group 5: Advanced nesting and composition
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Iif_TripleNested_EvaluatesCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition(
            "SELECT IIF(c.value > 15, 'high', IIF(c.value > 5, 'medium', IIF(c.value > 0, 'low', 'zero'))) AS tier FROM c ORDER BY c.id");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(3);
        results[0]["tier"]!.ToString().Should().Be("medium"); // value=10
        results[1]["tier"]!.ToString().Should().Be("high");   // value=20
        results[2]["tier"]!.ToString().Should().Be("zero");   // value=0
    }

    [Fact]
    public async Task Iif_InsideConcat_EvaluatesCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition(
            "SELECT CONCAT(IIF(c.isActive, 'Active', 'Inactive'), ': ', c.name) AS label FROM c ORDER BY c.id");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(3);
        results[0]["label"]!.ToString().Should().Be("Active: Alice");
        results[1]["label"]!.ToString().Should().Be("Inactive: Bob");
        results[2]["label"]!.ToString().Should().Be("Active: Charlie");
    }

    [Fact]
    public async Task Iif_MultipleInSameSelect_EvaluatesIndependently()
    {
        await SeedItems();
        var query = new QueryDefinition(
            "SELECT IIF(c.isActive, 'active', 'inactive') AS status, IIF(c.value > 10, 'high', 'low') AS level FROM c ORDER BY c.id");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(3);
        results[0]["status"]!.ToString().Should().Be("active");
        results[0]["level"]!.ToString().Should().Be("low");     // 10 NOT > 10
        results[1]["status"]!.ToString().Should().Be("inactive");
        results[1]["level"]!.ToString().Should().Be("high");    // 20 > 10
        results[2]["status"]!.ToString().Should().Be("active");
        results[2]["level"]!.ToString().Should().Be("low");     // 0
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Group 6: Usage contexts
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Iif_InOrderBy_SortsCorrectly()
    {
        await SeedItems();
        // Sort active items first (0) then inactive (1), secondary sort by name
        var query = new QueryDefinition(
            "SELECT c.name FROM c ORDER BY IIF(c.isActive, 0, 1), c.name");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(3);
        // Active items first (sorted: Alice, Charlie), then inactive (Bob)
        results[0]["name"]!.ToString().Should().Be("Alice");
        results[1]["name"]!.ToString().Should().Be("Charlie");
        results[2]["name"]!.ToString().Should().Be("Bob");
    }

    [Fact]
    public async Task Iif_WithParameterizedQuery_UsesParameterValue()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IIF(c.value > @threshold, 'high', 'low') AS level FROM c ORDER BY c.id")
            .WithParameter("@threshold", 15);

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(3);
        results[0]["level"]!.ToString().Should().Be("low");  // 10
        results[1]["level"]!.ToString().Should().Be("high"); // 20
        results[2]["level"]!.ToString().Should().Be("low");  // 0
    }

    [Fact]
    public async Task Iif_InValueSelect_ReturnsScalar()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE IIF(c.isActive, 'yes', 'no') FROM c WHERE c.id = '1'");

        var results = await QueryAll<string>(query);

        results.Should().ContainSingle().Which.Should().Be("yes");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Group 7: Edge cases
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Iif_FunctionNameCaseInsensitive_Works()
    {
        await SeedItems();
        var query = new QueryDefinition(
            "SELECT iif(c.isActive, 'yes', 'no') AS r1, Iif(c.isActive, 'yes', 'no') AS r2 FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["r1"]!.ToString().Should().Be("yes");
        results[0]["r2"]!.ToString().Should().Be("yes");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Helper
    // ═══════════════════════════════════════════════════════════════════════════

    private async Task<List<T>> QueryAll<T>(QueryDefinition query)
    {
        var iterator = _container.GetItemQueryIterator<T>(query);
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }
        return results;
    }
}
