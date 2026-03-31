using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════
//  Gap Fix Tests — TDD tests for deep gap analysis fixes
//  Each test class corresponds to a gap ID from gap-fix-tdd-plan.md
// ═══════════════════════════════════════════════════════════════════════════

// ─── C5: ObjectToArray should return {k, v} not {Name, Value} ───────────

public class ObjectToArray_KV_Tests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task ObjectToArray_ReturnsKAndVKeys()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", props = new { name = "Alice", age = 30 } }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JArray>(
            "SELECT VALUE ObjectToArray(c.props) FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<JArray>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        var arr = results.Should().ContainSingle().Subject;
        arr.Count.Should().Be(2);
        // Real Cosmos DB uses lowercase "k" and "v"
        arr[0]["k"]!.Value<string>().Should().Be("name");
        arr[0]["v"]!.Value<string>().Should().Be("Alice");
        arr[1]["k"]!.Value<string>().Should().Be("age");
        arr[1]["v"]!.Value<int>().Should().Be(30);
    }
}

// ─── M1: COUNT(c.field) should exclude docs where field is undefined ────

public class CountFieldTests
{
    [Fact]
    public async Task Query_Count_OfField_ExcludesUndefined()
    {
        var container = new InMemoryContainer("test-container", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","optional":"yes"}""")),
            new PartitionKey("a"));
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","pk":"a"}""")),
            new PartitionKey("a"));
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"3","pk":"a","optional":"also"}""")),
            new PartitionKey("a"));

        var iterator = container.GetItemQueryIterator<JObject>(
            "SELECT COUNT(c.optional) AS cnt FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        // Only 2 docs have "optional" field defined
        results.Should().ContainSingle().Subject["cnt"]!.Value<int>().Should().Be(2);
    }

    [Fact]
    public async Task Query_Count_Star_CountsAllRows()
    {
        var container = new InMemoryContainer("test-container", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","optional":"yes"}""")),
            new PartitionKey("a"));
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","pk":"a"}""")),
            new PartitionKey("a"));

        var iterator = container.GetItemQueryIterator<JObject>(
            "SELECT COUNT(1) AS cnt FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle().Subject["cnt"]!.Value<int>().Should().Be(2);
    }
}

// ─── M2: MIN/MAX on strings — should work lexicographically ─────────────

public class MinMaxStringTests
{
    [Fact]
    public async Task Query_Min_OnStrings_ReturnsLexicographicMin()
    {
        var container = new InMemoryContainer("test-container", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","name":"Charlie"}""")),
            new PartitionKey("a"));
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","pk":"a","name":"Alice"}""")),
            new PartitionKey("a"));
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"3","pk":"a","name":"Bob"}""")),
            new PartitionKey("a"));

        var iterator = container.GetItemQueryIterator<JObject>(
            "SELECT MIN(c.name) AS minName FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle().Subject["minName"]!.Value<string>().Should().Be("Alice");
    }

    [Fact]
    public async Task Query_Max_OnStrings_ReturnsLexicographicMax()
    {
        var container = new InMemoryContainer("test-container", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","name":"Charlie"}""")),
            new PartitionKey("a"));
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","pk":"a","name":"Alice"}""")),
            new PartitionKey("a"));
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"3","pk":"a","name":"Bob"}""")),
            new PartitionKey("a"));

        var iterator = container.GetItemQueryIterator<JObject>(
            "SELECT MAX(c.name) AS maxName FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle().Subject["maxName"]!.Value<string>().Should().Be("Charlie");
    }
}

// ─── M3: AVG empty set should return undefined (no value) ───────────────

public class AvgEmptySetTests
{
    [Fact]
    public async Task Query_Avg_EmptySet_ReturnsNoValue()
    {
        var container = new InMemoryContainer("test-container", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","name":"Alice"}""")),
            new PartitionKey("a"));

        // None of the docs have "score" field, so AVG gets empty numeric set
        var iterator = container.GetItemQueryIterator<JObject>(
            "SELECT AVG(c.score) AS avgScore FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        // Real Cosmos returns {} (empty object) when all aggregated values are undefined
        var result = results.Should().ContainSingle().Subject;
        result.ContainsKey("avgScore").Should().BeFalse("AVG of empty set should omit the field entirely");
    }
}

// ─── M4: REGEXMATCH modifiers (m, s, x, combined) ──────────────────────

public class RegexMatchModifierTests
{
    [Fact]
    public async Task RegexMatch_MultilineModifier_MatchesAcrossLines()
    {
        var container = new InMemoryContainer("test-container", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("{\"id\":\"1\",\"pk\":\"a\",\"text\":\"line1\\nline2\"}")),
            new PartitionKey("a"));

        var iterator = container.GetItemQueryIterator<string>(
            "SELECT VALUE c.id FROM c WHERE RegexMatch(c.text, '^line2', 'm')",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<string>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle();
    }

    [Fact]
    public async Task RegexMatch_SinglelineModifier_DotMatchesNewline()
    {
        var container = new InMemoryContainer("test-container", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("{\"id\":\"1\",\"pk\":\"a\",\"text\":\"line1\\nline2\"}")),
            new PartitionKey("a"));

        var iterator = container.GetItemQueryIterator<string>(
            "SELECT VALUE c.id FROM c WHERE RegexMatch(c.text, 'line1.line2', 's')",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<string>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle();
    }

    [Fact]
    public async Task RegexMatch_CombinedModifiers_Work()
    {
        var container = new InMemoryContainer("test-container", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("{\"id\":\"1\",\"pk\":\"a\",\"text\":\"Hello\\nworld\"}")),
            new PartitionKey("a"));

        // 'im' = ignore case + multiline
        var iterator = container.GetItemQueryIterator<string>(
            "SELECT VALUE c.id FROM c WHERE RegexMatch(c.text, '^WORLD', 'im')",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<string>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle();
    }
}

// ─── M5: EXISTS catch-all should return false ───────────────────────────

public class ExistsCatchAllTests
{
    [Fact]
    public async Task Query_Exists_UnparseableSubquery_ReturnsFalse()
    {
        var container = new InMemoryContainer("test-container", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","name":"Alice"}""")),
            new PartitionKey("a"));

        // This subquery is intentionally malformed to trigger the catch block
        var iterator = container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE EXISTS(SELECT %%% INVALID %%%)",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        // Real Cosmos would reject this; catch-all should be false (safe default)
        results.Should().BeEmpty();
    }
}

// ─── M6: ArrayToObject function ─────────────────────────────────────────

public class ArrayToObjectTests
{
    [Fact]
    public async Task ArrayToObject_ConvertsKVArrayToObject()
    {
        var container = new InMemoryContainer("test-container", "/pk");
        var json = """{"id":"1","pk":"a","arr":[{"k":"name","v":"Alice"},{"k":"age","v":30}]}""";
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(json)),
            new PartitionKey("a"));

        var iterator = container.GetItemQueryIterator<JObject>(
            "SELECT VALUE ArrayToObject(c.arr) FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        var obj = results.Should().ContainSingle().Subject;
        obj["name"]!.Value<string>().Should().Be("Alice");
        obj["age"]!.Value<int>().Should().Be(30);
    }

    [Fact]
    public async Task ArrayToObject_WithNonKVArray_ReturnsUndefined()
    {
        var container = new InMemoryContainer("test-container", "/pk");
        var json = """{"id":"1","pk":"a","arr":["hello","world"]}""";
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(json)),
            new PartitionKey("a"));

        // SELECT VALUE means undefined results in empty result set
        var iterator = container.GetItemQueryIterator<JObject>(
            "SELECT VALUE ArrayToObject(c.arr) FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().BeEmpty();
    }
}

// ─── M8: StringTo* functions — invalid input returns undefined ──────────

public class StringToUndefinedTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    private async Task SeedAsync()
    {
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","val":"not-valid"}""")),
            new PartitionKey("a"));
    }

    [Fact]
    public async Task StringToNumber_InvalidInput_ReturnsUndefined()
    {
        await SeedAsync();

        // SELECT VALUE means undefined values are omitted from results
        var iterator = _container.GetItemQueryIterator<JToken>(
            "SELECT VALUE StringToNumber(c.val) FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<JToken>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().BeEmpty("invalid input to StringToNumber should return undefined, omitted by VALUE");
    }

    [Fact]
    public async Task StringToBoolean_InvalidInput_ReturnsUndefined()
    {
        await SeedAsync();

        var iterator = _container.GetItemQueryIterator<JToken>(
            "SELECT VALUE StringToBoolean(c.val) FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<JToken>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().BeEmpty("invalid input to StringToBoolean should return undefined, omitted by VALUE");
    }

    [Fact]
    public async Task StringToArray_InvalidInput_ReturnsUndefined()
    {
        await SeedAsync();

        var iterator = _container.GetItemQueryIterator<JToken>(
            "SELECT VALUE StringToArray(c.val) FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<JToken>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().BeEmpty("invalid input to StringToArray should return undefined, omitted by VALUE");
    }

    [Fact]
    public async Task StringToObject_InvalidInput_ReturnsUndefined()
    {
        await SeedAsync();

        var iterator = _container.GetItemQueryIterator<JToken>(
            "SELECT VALUE StringToObject(c.val) FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<JToken>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().BeEmpty("invalid input to StringToObject should return undefined, omitted by VALUE");
    }

    [Fact]
    public async Task StringToNull_InvalidInput_ReturnsUndefined()
    {
        await SeedAsync();

        var iterator = _container.GetItemQueryIterator<JToken>(
            "SELECT VALUE StringToNull(c.val) FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<JToken>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().BeEmpty("invalid input to StringToNull should return undefined, omitted by VALUE");
    }
}

// ─── M10: GROUP BY without aggregates returns projected fields ──────────

public class GroupByNoAggregateTests
{
    [Fact]
    public async Task Query_GroupBy_WithoutAggregates_ReturnsProjectedFields()
    {
        var container = new InMemoryContainer("test-container", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","city":"London","name":"Alice"}""")),
            new PartitionKey("a"));
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","pk":"a","city":"London","name":"Bob"}""")),
            new PartitionKey("a"));
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"3","pk":"a","city":"Paris","name":"Charlie"}""")),
            new PartitionKey("a"));

        var iterator = container.GetItemQueryIterator<JObject>(
            "SELECT c.city FROM c GROUP BY c.city",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().HaveCount(2);
        // Each result should ONLY have "city" — not the full document
        foreach (var r in results)
        {
            r.Properties().Select(p => p.Name).Should().BeEquivalentTo(new[] { "city" });
        }
        results.Select(r => r["city"]!.Value<string>()).Should().BeEquivalentTo(new[] { "London", "Paris" });
    }
}

// ─── L1: DateTimeBin year/month support ─────────────────────────────────

public class DateTimeBinYearMonthTests
{
    [Fact]
    public async Task DateTimeBin_Year_BinsToYearBoundary()
    {
        var container = new InMemoryContainer("test-container", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","dt":"2023-07-15T10:30:00.0000000Z"}""")),
            new PartitionKey("a"));

        var iterator = container.GetItemQueryIterator<string>(
            "SELECT VALUE DateTimeBin(c.dt, 'year', 1) FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<string>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        // Binning to 1 year => 2023-01-01T00:00:00.0000000Z
        results.Should().ContainSingle().Which.Should().Be("2023-01-01T00:00:00.0000000Z");
    }

    [Fact]
    public async Task DateTimeBin_Month_BinsToMonthBoundary()
    {
        var container = new InMemoryContainer("test-container", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","dt":"2023-07-15T10:30:00.0000000Z"}""")),
            new PartitionKey("a"));

        var iterator = container.GetItemQueryIterator<string>(
            "SELECT VALUE DateTimeBin(c.dt, 'month', 1) FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<string>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        // Binning to 1 month => 2023-07-01T00:00:00.0000000Z
        results.Should().ContainSingle().Which.Should().Be("2023-07-01T00:00:00.0000000Z");
    }

    [Fact]
    public async Task DateTimeBin_Quarter_BinsTo3MonthBoundary()
    {
        var container = new InMemoryContainer("test-container", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","dt":"2023-08-15T10:30:00.0000000Z"}""")),
            new PartitionKey("a"));

        var iterator = container.GetItemQueryIterator<string>(
            "SELECT VALUE DateTimeBin(c.dt, 'month', 3) FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<string>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        // Binning to 3 months from origin 2001-01-01 =>
        // Aug 2023 is in the Q3 2023 bin starting at 2023-07-01
        results.Should().ContainSingle().Which.Should().Be("2023-07-01T00:00:00.0000000Z");
    }
}
