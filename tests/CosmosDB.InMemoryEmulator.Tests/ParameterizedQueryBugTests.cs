using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for parameterized query bugs:
/// 1. IS_NULL(@param) returns false when parameter value is null
/// 2. SUM(IIF(field = @param, 1, 0)) returns 0 with parameterized queries
/// </summary>
public class ParameterizedQueryBugTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private async Task SeedItems()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", partitionKey = "pk1", status = "Completed", amount = 50 }),
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", partitionKey = "pk1", status = "Completed", amount = 30 }),
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "3", partitionKey = "pk1", status = "Failed", amount = 20 }),
            new PartitionKey("pk1"));
    }

    // ── Bug 1: IS_NULL(@param) with null parameter ──────────────────────────

    [Fact]
    public async Task IsNull_WithParameterizedNull_ReturnsTrue()
    {
        await SeedItems();

        var query = new QueryDefinition(
            "SELECT IS_NULL(@status) AS isNullResult FROM c WHERE c.partitionKey = @pk")
            .WithParameter("@pk", "pk1")
            .WithParameter("@status", null);

        var results = await DrainQuery<JObject>(query);

        results.Should().HaveCount(3);
        results[0]["isNullResult"]!.Value<bool>().Should().BeTrue();
    }

    [Fact]
    public async Task IsNull_OptionalFilterPattern_WithNullParam_ReturnsAllItems()
    {
        await SeedItems();

        // Common Cosmos pattern: (IS_NULL(@filter) OR c.field = @filter)
        // When @filter is null, IS_NULL(@filter) should be true, so all items match
        var query = new QueryDefinition(
            "SELECT * FROM c WHERE c.partitionKey = @pk AND (IS_NULL(@status) OR c.status = @status)")
            .WithParameter("@pk", "pk1")
            .WithParameter("@status", null);

        var results = await DrainQuery<JObject>(query);

        results.Should().HaveCount(3);
    }

    [Fact]
    public async Task IsNull_OptionalFilterPattern_WithNonNullParam_FiltersCorrectly()
    {
        await SeedItems();

        var query = new QueryDefinition(
            "SELECT * FROM c WHERE c.partitionKey = @pk AND (IS_NULL(@status) OR c.status = @status)")
            .WithParameter("@pk", "pk1")
            .WithParameter("@status", "Completed");

        var results = await DrainQuery<JObject>(query);

        results.Should().HaveCount(2);
    }

    // ── Bug 2: SUM(IIF(field = @param, 1, 0)) ──────────────────────────────

    [Fact]
    public async Task SumIif_WithParameterizedValue_ReturnsCorrectCount()
    {
        await SeedItems();

        var query = new QueryDefinition(
            "SELECT SUM(IIF(c.status = @completedStatus, 1, 0)) AS filesCompleted FROM c WHERE c.partitionKey = @pk")
            .WithParameter("@pk", "pk1")
            .WithParameter("@completedStatus", "Completed");

        var results = await DrainQuery<JObject>(query);

        results.Should().ContainSingle();
        results[0]["filesCompleted"]!.Value<int>().Should().Be(2);
    }

    [Fact]
    public async Task SumIif_FullAggregation_WithParameters_ReturnsCorrectCounts()
    {
        await SeedItems();

        var query = new QueryDefinition(@"
            SELECT
                COUNT(1) AS totalFiles,
                SUM(c.amount) AS totalAmount,
                SUM(IIF(c.status = @completedStatus, 1, 0)) AS filesCompleted,
                SUM(IIF(c.status = @failedStatus, 1, 0)) AS filesFailed
            FROM c
            WHERE c.partitionKey = @pk")
            .WithParameter("@pk", "pk1")
            .WithParameter("@completedStatus", "Completed")
            .WithParameter("@failedStatus", "Failed");

        var results = await DrainQuery<JObject>(query);

        results.Should().ContainSingle();
        var result = results[0];
        result["totalFiles"]!.Value<int>().Should().Be(3);
        result["totalAmount"]!.Value<int>().Should().Be(100);
        result["filesCompleted"]!.Value<int>().Should().Be(2);
        result["filesFailed"]!.Value<int>().Should().Be(1);
    }

    private async Task<List<T>> DrainQuery<T>(QueryDefinition query)
    {
        var iterator = _container.GetItemQueryIterator<T>(query);
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        return results;
    }
}

/// <summary>
/// Extended coverage for parameterized queries in type-checking functions,
/// aggregate paths, GROUP BY HAVING, and edge cases around parameter resolution.
/// </summary>
public class ParameterizedQueryExtendedTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    private async Task SeedItems()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", status = "Active", category = "A", price = 10.0, score = 5 }),
            new PartitionKey("a"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", status = "Active", category = "A", price = 20.0, score = 8 }),
            new PartitionKey("a"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "3", pk = "a", status = "Inactive", category = "B", price = 30.0, score = 3 }),
            new PartitionKey("a"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "4", pk = "a", status = "Active", category = "B", price = 40.0 }),
            new PartitionKey("a"));
    }

    // ── IS_DEFINED with @param ──────────────────────────────────────────────

    [Fact]
    public async Task IsDefined_WithParameterizedNonNull_ReturnsTrue()
    {
        await SeedItems();

        var query = new QueryDefinition(
            "SELECT IS_DEFINED(@val) AS result FROM c WHERE c.pk = @pk")
            .WithParameter("@pk", "a")
            .WithParameter("@val", "something");

        var results = await DrainQuery<JObject>(query);
        results.Should().HaveCount(4);
        results[0]["result"]!.Value<bool>().Should().BeTrue();
    }

    [Fact]
    public async Task IsDefined_WithParameterizedNull_ReturnsTrue()
    {
        // IS_DEFINED(null) returns true in real Cosmos (null is defined, just null)
        await SeedItems();

        var query = new QueryDefinition(
            "SELECT IS_DEFINED(@val) AS result FROM c WHERE c.pk = @pk")
            .WithParameter("@pk", "a")
            .WithParameter("@val", null);

        var results = await DrainQuery<JObject>(query);
        results.Should().HaveCount(4);
        // null is a defined value — IS_DEFINED(null) = true
        results[0]["result"]!.Value<bool>().Should().BeTrue();
    }

    // ── IS_NULL variants ────────────────────────────────────────────────────

    [Fact]
    public async Task IsNull_WithParameterizedString_ReturnsFalse()
    {
        await SeedItems();

        var query = new QueryDefinition(
            "SELECT IS_NULL(@val) AS result FROM c WHERE c.pk = @pk")
            .WithParameter("@pk", "a")
            .WithParameter("@val", "notNull");

        var results = await DrainQuery<JObject>(query);
        results[0]["result"]!.Value<bool>().Should().BeFalse();
    }

    [Fact]
    public async Task IsNull_WithParameterizedInteger_ReturnsFalse()
    {
        await SeedItems();

        var query = new QueryDefinition(
            "SELECT IS_NULL(@val) AS result FROM c WHERE c.pk = @pk")
            .WithParameter("@pk", "a")
            .WithParameter("@val", 42);

        var results = await DrainQuery<JObject>(query);
        results[0]["result"]!.Value<bool>().Should().BeFalse();
    }

    // ── SUM with parameterized arithmetic ───────────────────────────────────

    [Fact]
    public async Task Sum_WithParameterizedMultiplier_Works()
    {
        await SeedItems();

        var query = new QueryDefinition(
            "SELECT SUM(c.price * @multiplier) AS total FROM c WHERE c.pk = @pk")
            .WithParameter("@pk", "a")
            .WithParameter("@multiplier", 2);

        var results = await DrainQuery<JObject>(query);
        results.Should().ContainSingle();
        results[0]["total"]!.Value<double>().Should().Be(200.0);
    }

    [Fact]
    public async Task Avg_WithParameterizedExpression_Works()
    {
        await SeedItems();

        var query = new QueryDefinition(
            "SELECT AVG(c.price + @bonus) AS avgPrice FROM c WHERE c.pk = @pk")
            .WithParameter("@pk", "a")
            .WithParameter("@bonus", 10);

        var results = await DrainQuery<JObject>(query);
        results.Should().ContainSingle();
        // Prices: 10, 20, 30, 40. With +10 bonus: 20, 30, 40, 50. Avg = 35
        results[0]["avgPrice"]!.Value<double>().Should().Be(35.0);
    }

    // ── Multiple IIF aggregates in one query ────────────────────────────────

    [Fact]
    public async Task SumIif_MultipleDifferentParams_AllResolve()
    {
        await SeedItems();

        var query = new QueryDefinition(@"
            SELECT
                SUM(IIF(c.status = @active, 1, 0)) AS activeCount,
                SUM(IIF(c.status = @inactive, 1, 0)) AS inactiveCount,
                SUM(IIF(c.status = @unknown, 1, 0)) AS unknownCount
            FROM c WHERE c.pk = @pk")
            .WithParameter("@pk", "a")
            .WithParameter("@active", "Active")
            .WithParameter("@inactive", "Inactive")
            .WithParameter("@unknown", "Unknown");

        var results = await DrainQuery<JObject>(query);
        results.Should().ContainSingle();
        results[0]["activeCount"]!.Value<int>().Should().Be(3);
        results[0]["inactiveCount"]!.Value<int>().Should().Be(1);
        results[0]["unknownCount"]!.Value<int>().Should().Be(0);
    }

    // ── SUM(IIF) with conditional amount ────────────────────────────────────

    [Fact]
    public async Task SumIif_ConditionalAmount_WithParameter()
    {
        await SeedItems();

        var query = new QueryDefinition(
            "SELECT SUM(IIF(c.status = @status, c.price, 0)) AS totalActive FROM c WHERE c.pk = @pk")
            .WithParameter("@pk", "a")
            .WithParameter("@status", "Active");

        var results = await DrainQuery<JObject>(query);
        results.Should().ContainSingle();
        // Active items: price 10 + 20 + 40 = 70
        results[0]["totalActive"]!.Value<double>().Should().Be(70.0);
    }

    // ── GROUP BY with parameterized aggregates ──────────────────────────────

    [Fact]
    public async Task GroupBy_SumIif_WithParameter()
    {
        await SeedItems();

        var query = new QueryDefinition(@"
            SELECT c.category,
                   SUM(IIF(c.status = @activeStatus, 1, 0)) AS activeCount
            FROM c
            WHERE c.pk = @pk
            GROUP BY c.category")
            .WithParameter("@pk", "a")
            .WithParameter("@activeStatus", "Active");

        var results = await DrainQuery<JObject>(query);
        results.Should().HaveCount(2);

        var catA = results.First(r => r["category"]!.Value<string>() == "A");
        var catB = results.First(r => r["category"]!.Value<string>() == "B");

        catA["activeCount"]!.Value<int>().Should().Be(2);
        catB["activeCount"]!.Value<int>().Should().Be(1);
    }

    [Fact]
    public async Task GroupBy_Sum_WithParameterizedMultiplier()
    {
        await SeedItems();

        var query = new QueryDefinition(@"
            SELECT c.category, SUM(c.price * @factor) AS adjustedTotal
            FROM c WHERE c.pk = @pk
            GROUP BY c.category")
            .WithParameter("@pk", "a")
            .WithParameter("@factor", 1.1);

        var results = await DrainQuery<JObject>(query);
        results.Should().HaveCount(2);

        var catA = results.First(r => r["category"]!.Value<string>() == "A");
        // Cat A prices: 10, 20 → * 1.1 → 11 + 22 = 33
        catA["adjustedTotal"]!.Value<double>().Should().BeApproximately(33.0, 0.001);
    }

    // ── GROUP BY HAVING with parameterized threshold ────────────────────────

    [Fact]
    public async Task GroupBy_Having_WithParameterizedThreshold()
    {
        await SeedItems();

        var query = new QueryDefinition(@"
            SELECT c.category, SUM(c.price) AS total
            FROM c WHERE c.pk = @pk
            GROUP BY c.category
            HAVING SUM(c.price) > @threshold")
            .WithParameter("@pk", "a")
            .WithParameter("@threshold", 50);

        var results = await DrainQuery<JObject>(query);
        // Cat A: 10+20=30 (below 50), Cat B: 30+40=70 (above 50)
        results.Should().ContainSingle();
        results[0]["category"]!.Value<string>().Should().Be("B");
        results[0]["total"]!.Value<double>().Should().Be(70.0);
    }

    // ── IS_NULL in WHERE combined with other conditions ──────────────────────

    [Fact]
    public async Task IsNull_InWhereWithAnd_WithNullParam()
    {
        await SeedItems();

        // Items with id=4 has no "score" property
        var query = new QueryDefinition(
            "SELECT c.id FROM c WHERE c.pk = @pk AND (IS_NULL(@filter) OR c.category = @filter)")
            .WithParameter("@pk", "a")
            .WithParameter("@filter", null);

        var results = await DrainQuery<JObject>(query);
        results.Should().HaveCount(4); // All items returned when filter is null
    }

    [Fact]
    public async Task IsNull_InWhereWithAnd_WithNonNullParam()
    {
        await SeedItems();

        var query = new QueryDefinition(
            "SELECT c.id FROM c WHERE c.pk = @pk AND (IS_NULL(@filter) OR c.category = @filter)")
            .WithParameter("@pk", "a")
            .WithParameter("@filter", "A");

        var results = await DrainQuery<JObject>(query);
        results.Should().HaveCount(2); // Only category A items
    }

    // ── MIN/MAX with parameterized expressions ──────────────────────────────

    [Fact]
    public async Task Min_WithParameterizedExpression_Works()
    {
        await SeedItems();

        var query = new QueryDefinition(
            "SELECT MIN(c.price + @offset) AS minAdjusted FROM c WHERE c.pk = @pk")
            .WithParameter("@pk", "a")
            .WithParameter("@offset", 5);

        var results = await DrainQuery<JObject>(query);
        results.Should().ContainSingle();
        // Min price is 10, +5 = 15
        results[0]["minAdjusted"]!.Value<double>().Should().Be(15.0);
    }

    [Fact]
    public async Task Max_WithParameterizedExpression_Works()
    {
        await SeedItems();

        var query = new QueryDefinition(
            "SELECT MAX(c.price * @factor) AS maxAdjusted FROM c WHERE c.pk = @pk")
            .WithParameter("@pk", "a")
            .WithParameter("@factor", 0.5);

        var results = await DrainQuery<JObject>(query);
        results.Should().ContainSingle();
        // Max price is 40, *0.5 = 20
        results[0]["maxAdjusted"]!.Value<double>().Should().Be(20.0);
    }

    // ── Nested IIF with multiple param references ───────────────────────────

    [Fact]
    public async Task Iif_NestedWithMultipleParams_InSelect()
    {
        await SeedItems();

        var query = new QueryDefinition(@"
            SELECT c.id,
                   IIF(c.status = @active, @activeLabel, @inactiveLabel) AS label
            FROM c WHERE c.pk = @pk ORDER BY c.id")
            .WithParameter("@pk", "a")
            .WithParameter("@active", "Active")
            .WithParameter("@activeLabel", "ON")
            .WithParameter("@inactiveLabel", "OFF");

        var results = await DrainQuery<JObject>(query);
        results.Should().HaveCount(4);
        results[0]["label"]!.Value<string>().Should().Be("ON");  // id=1, Active
        results[2]["label"]!.Value<string>().Should().Be("OFF"); // id=3, Inactive
    }

    // ── Combined IS_NULL + SUM(IIF) pattern (production use case) ───────────

    [Fact]
    public async Task ProductionPattern_OptionalFilterWithConditionalAggregation()
    {
        await SeedItems();

        // Real-world pattern: optional status filter + conditional counting
        var query = new QueryDefinition(@"
            SELECT
                COUNT(1) AS total,
                SUM(IIF(c.status = @targetStatus, 1, 0)) AS targetCount
            FROM c
            WHERE c.pk = @pk AND (IS_NULL(@categoryFilter) OR c.category = @categoryFilter)")
            .WithParameter("@pk", "a")
            .WithParameter("@targetStatus", "Active")
            .WithParameter("@categoryFilter", null); // No filter — all items

        var results = await DrainQuery<JObject>(query);
        results.Should().ContainSingle();
        results[0]["total"]!.Value<int>().Should().Be(4);
        results[0]["targetCount"]!.Value<int>().Should().Be(3);
    }

    [Fact]
    public async Task ProductionPattern_WithCategoryFilter()
    {
        await SeedItems();

        var query = new QueryDefinition(@"
            SELECT
                COUNT(1) AS total,
                SUM(IIF(c.status = @targetStatus, 1, 0)) AS targetCount
            FROM c
            WHERE c.pk = @pk AND (IS_NULL(@categoryFilter) OR c.category = @categoryFilter)")
            .WithParameter("@pk", "a")
            .WithParameter("@targetStatus", "Active")
            .WithParameter("@categoryFilter", "B"); // Filter to category B only

        var results = await DrainQuery<JObject>(query);
        results.Should().ContainSingle();
        results[0]["total"]!.Value<int>().Should().Be(2); // 2 items in cat B
        results[0]["targetCount"]!.Value<int>().Should().Be(1); // 1 active in cat B
    }

    // ── IS_STRING/IS_NUMBER/IS_BOOL with parameters ─────────────────────────

    [Fact]
    public async Task IsString_WithParameterizedValue_Works()
    {
        await SeedItems();

        var query = new QueryDefinition(
            "SELECT VALUE IS_STRING(@val) FROM c WHERE c.id = '1' AND c.pk = @pk")
            .WithParameter("@pk", "a")
            .WithParameter("@val", "hello");

        var results = await DrainQuery<bool>(query);
        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task IsNumber_WithParameterizedValue_Works()
    {
        await SeedItems();

        var query = new QueryDefinition(
            "SELECT VALUE IS_NUMBER(@val) FROM c WHERE c.id = '1' AND c.pk = @pk")
            .WithParameter("@pk", "a")
            .WithParameter("@val", 42);

        var results = await DrainQuery<bool>(query);
        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task IsBool_WithParameterizedValue_Works()
    {
        await SeedItems();

        var query = new QueryDefinition(
            "SELECT VALUE IS_BOOL(@val) FROM c WHERE c.id = '1' AND c.pk = @pk")
            .WithParameter("@pk", "a")
            .WithParameter("@val", true);

        var results = await DrainQuery<bool>(query);
        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    private async Task<List<T>> DrainQuery<T>(QueryDefinition query)
    {
        var iterator = _container.GetItemQueryIterator<T>(query);
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        return results;
    }
}
