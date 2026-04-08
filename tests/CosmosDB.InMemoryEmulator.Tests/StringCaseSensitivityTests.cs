using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using System.Text;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests verifying that SQL string comparisons are case-sensitive,
/// matching real Azure Cosmos DB behaviour.
/// Real Cosmos DB uses ordinal (case-sensitive) string comparison
/// for =, !=, &lt;, &gt;, &lt;=, &gt;=, IN, NOT IN, and LIKE.
/// </summary>
public class StringCaseSensitivityTests
{
    private readonly InMemoryContainer _container = new("case-test", "/pk");

    private async Task SeedAsync()
    {
        await CreateItem("""{"id":"1","pk":"a","name":"Alice"}""");
        await CreateItem("""{"id":"2","pk":"a","name":"alice"}""");
        await CreateItem("""{"id":"3","pk":"a","name":"BOB"}""");
        await CreateItem("""{"id":"4","pk":"a","name":"bob"}""");
    }

    private async Task CreateItem(string json)
    {
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("a"));
    }

    // ═══════════════════════════════════════════════════════════════════
    //  C1: Equality (=, !=, IN) must be case-sensitive
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_WhereEquals_IsCaseSensitive()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.name = 'Alice'");
        var page = await iter.ReadNextAsync();

        // Only "Alice" (id=1), NOT "alice" (id=2)
        page.Should().HaveCount(1);
        page.First()["id"]!.Value<string>().Should().Be("1");
    }

    [Fact]
    public async Task Query_WhereNotEquals_IsCaseSensitive()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.name != 'Alice'");
        var page = await iter.ReadNextAsync();

        // "alice", "BOB", "bob" — but NOT "Alice"
        page.Should().HaveCount(3);
        page.Select(d => d["id"]!.Value<string>()).Should().NotContain("1");
    }

    [Fact]
    public async Task Query_WhereIn_IsCaseSensitive()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.name IN ('alice', 'bob')");
        var page = await iter.ReadNextAsync();

        // Only lowercase matches: id=2 and id=4
        page.Should().HaveCount(2);
        page.Select(d => d["id"]!.Value<string>()).Should().BeEquivalentTo(["2", "4"]);
    }

    // ═══════════════════════════════════════════════════════════════════
    //  C2: Comparison (<, >, <=, >=) and ORDER BY must be case-sensitive
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_OrderBy_String_IsCaseSensitive()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT c.name FROM c ORDER BY c.name ASC");
        var page = await iter.ReadNextAsync();

        var names = page.Select(d => d["name"]!.Value<string>()).ToList();

        // Ordinal sort: uppercase letters (A=65, B=66) sort before lowercase (a=97, b=98)
        // Expected: "Alice", "BOB", "alice", "bob"
        names.Should().BeEquivalentTo(
            new[] { "Alice", "BOB", "alice", "bob" },
            opts => opts.WithStrictOrdering());
    }

    [Fact]
    public async Task Query_WhereGreaterThan_String_IsCaseSensitive()
    {
        await SeedAsync();

        // In ordinal comparison, 'a' (97) > 'Z' (90), so "alice" > "BOB"
        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.name > 'Z'");
        var page = await iter.ReadNextAsync();

        // "alice" and "bob" are > "Z" in ordinal; "Alice" and "BOB" are < "Z"
        page.Should().HaveCount(2);
        page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["alice", "bob"]);
    }

    // ═══════════════════════════════════════════════════════════════════
    //  C3: LIKE must be case-sensitive
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_Like_IsCaseSensitive()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.name LIKE 'A%'");
        var page = await iter.ReadNextAsync();

        // Only "Alice" starts with uppercase A, not "alice"
        page.Should().HaveCount(1);
        page.First()["id"]!.Value<string>().Should().Be("1");
    }

    [Fact]
    public async Task Query_Like_LowercasePattern_IsCaseSensitive()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.name LIKE 'b%'");
        var page = await iter.ReadNextAsync();

        // Only "bob" starts with lowercase b, not "BOB"
        page.Should().HaveCount(1);
        page.First()["id"]!.Value<string>().Should().Be("4");
    }

    // ═══════════════════════════════════════════════════════════════════
    //  C4: LIKE without ESCAPE must treat regex metacharacters as literals
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_Like_WithSquareBrackets_TreatedAsLiteral()
    {
        var container = new InMemoryContainer("like-test", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","code":"test[1]"}""")), new PartitionKey("a"));
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","pk":"a","code":"testX"}""")), new PartitionKey("a"));

        var iter = container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.code LIKE 'test[1]'");
        var page = await iter.ReadNextAsync();

        // Should match only "test[1]" literally, not treat [1] as regex char class
        page.Should().HaveCount(1);
        page.First()["id"]!.Value<string>().Should().Be("1");
    }

    [Fact]
    public async Task Query_Like_WithDot_TreatedAsLiteral()
    {
        var container = new InMemoryContainer("like-test2", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","code":"foo.bar"}""")), new PartitionKey("a"));
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","pk":"a","code":"fooXbar"}""")), new PartitionKey("a"));

        var iter = container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.code LIKE 'foo.bar'");
        var page = await iter.ReadNextAsync();

        // Should match only "foo.bar" literally, not treat . as regex any-char
        page.Should().HaveCount(1);
        page.First()["id"]!.Value<string>().Should().Be("1");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  C5: Remaining Comparison Operators
// ═══════════════════════════════════════════════════════════════════════════

public class StringComparisonOperatorTests
{
    private readonly InMemoryContainer _container = new("case-cmp", "/pk");

    private async Task SeedAsync()
    {
        foreach (var (id, name) in new[] { ("1", "Alice"), ("2", "alice"), ("3", "BOB"), ("4", "bob") })
        {
            await _container.CreateItemStreamAsync(
                new MemoryStream(Encoding.UTF8.GetBytes(
                    $$$"""{"id":"{{{id}}}","pk":"a","name":"{{{name}}}"}""")),
                new PartitionKey("a"));
        }
    }

    [Fact]
    public async Task Query_WhereLessThan_String_IsCaseSensitive()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.name < 'a'");
        var page = await iter.ReadNextAsync();

        // Ordinal: uppercase chars (A=65, B=66) < 'a' (97)
        page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["Alice", "BOB"]);
    }

    [Fact]
    public async Task Query_WhereLessThanOrEqual_String_IsCaseSensitive()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.name <= 'BOB'");
        var page = await iter.ReadNextAsync();

        // Ordinal: "Alice" <= "BOB" (A<B), "BOB" <= "BOB" (equal)
        page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["Alice", "BOB"]);
    }

    [Fact]
    public async Task Query_WhereGreaterThanOrEqual_String_IsCaseSensitive()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.name >= 'alice'");
        var page = await iter.ReadNextAsync();

        // Ordinal: "alice" >= "alice" (equal), "bob" >= "alice"
        page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["alice", "bob"]);
    }

    [Fact]
    public async Task Query_NotIn_IsCaseSensitive()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.name NOT IN ('Alice', 'BOB')");
        var page = await iter.ReadNextAsync();

        page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["alice", "bob"]);
    }

    [Fact]
    public async Task Query_Between_String_IsCaseSensitive()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.name BETWEEN 'A' AND 'Z'");
        var page = await iter.ReadNextAsync();

        // Ordinal range [A, Z]: "Alice" and "BOB" are in range; "alice" > "Z", "bob" > "Z"
        page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["Alice", "BOB"]);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  C6: ORDER BY DESC
// ═══════════════════════════════════════════════════════════════════════════

public class StringOrderByDescTests
{
    [Fact]
    public async Task Query_OrderByDesc_String_IsCaseSensitive()
    {
        var container = new InMemoryContainer("case-ord", "/pk");
        foreach (var (id, name) in new[] { ("1", "Alice"), ("2", "alice"), ("3", "BOB"), ("4", "bob") })
        {
            await container.CreateItemStreamAsync(
                new MemoryStream(Encoding.UTF8.GetBytes(
                    $$$"""{"id":"{{{id}}}","pk":"a","name":"{{{name}}}"}""")),
                new PartitionKey("a"));
        }

        var iter = container.GetItemQueryIterator<JObject>(
            "SELECT c.name FROM c ORDER BY c.name DESC");
        var page = await iter.ReadNextAsync();

        var names = page.Select(d => d["name"]!.Value<string>()).ToList();
        // Reverse ordinal: "bob", "alice", "BOB", "Alice"
        names.Should().BeEquivalentTo(
            new[] { "bob", "alice", "BOB", "Alice" },
            opts => opts.WithStrictOrdering());
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  C7: LIKE Extended Coverage
// ═══════════════════════════════════════════════════════════════════════════

public class StringLikeExtendedTests
{
    private readonly InMemoryContainer _container = new("case-like", "/pk");

    private async Task SeedAsync()
    {
        foreach (var (id, name) in new[] { ("1", "Alice"), ("2", "alice"), ("3", "BOB"), ("4", "bob") })
        {
            await _container.CreateItemStreamAsync(
                new MemoryStream(Encoding.UTF8.GetBytes(
                    $$$"""{"id":"{{{id}}}","pk":"a","name":"{{{name}}}"}""")),
                new PartitionKey("a"));
        }
    }

    [Fact]
    public async Task Query_Like_ExactMatch_IsCaseSensitive()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.name LIKE 'Alice'");
        var page = await iter.ReadNextAsync();

        page.Should().HaveCount(1);
        page.First()["name"]!.Value<string>().Should().Be("Alice");
    }

    [Fact]
    public async Task Query_Like_UnderscoreWildcard_IsCaseSensitive()
    {
        await SeedAsync();

        // _lice matches any single char + "lice" — both "Alice" and "alice" match
        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.name LIKE '_lice'");
        var page = await iter.ReadNextAsync();

        page.Should().HaveCount(2);
        page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["Alice", "alice"]);
    }

    [Fact]
    public async Task Query_NotLike_IsCaseSensitive()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.name NOT LIKE 'A%'");
        var page = await iter.ReadNextAsync();

        page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["alice", "BOB", "bob"]);
    }

    [Fact]
    public async Task Query_Like_MiddlePercent_IsCaseSensitive()
    {
        await SeedAsync();

        // A%e matches strings starting with 'A' and ending with 'e'
        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.name LIKE 'A%e'");
        var page = await iter.ReadNextAsync();

        page.Should().HaveCount(1);
        page.First()["name"]!.Value<string>().Should().Be("Alice");
    }

    [Fact]
    public async Task Query_Like_SubstringPercent_IsCaseSensitive()
    {
        await SeedAsync();

        // %li% matches any string containing "li"
        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.name LIKE '%li%'");
        var page = await iter.ReadNextAsync();

        page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["Alice", "alice"]);
    }

    [Fact]
    public async Task Query_Like_EmptyPattern_MatchesOnlyEmpty()
    {
        var container = new InMemoryContainer("empty-like", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","name":""}""")),
            new PartitionKey("a"));
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","pk":"a","name":"notempty"}""")),
            new PartitionKey("a"));

        var iter = container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.name LIKE ''");
        var page = await iter.ReadNextAsync();

        page.Should().HaveCount(1);
        page.First()["id"]!.Value<string>().Should().Be("1");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  C8: String Functions Case Sensitivity
// ═══════════════════════════════════════════════════════════════════════════

public class StringFunctionCaseSensitivityTests
{
    private readonly InMemoryContainer _container = new("case-func", "/pk");

    private async Task SeedAsync()
    {
        foreach (var (id, name) in new[] { ("1", "Alice"), ("2", "alice"), ("3", "BOB"), ("4", "bob") })
        {
            await _container.CreateItemStreamAsync(
                new MemoryStream(Encoding.UTF8.GetBytes(
                    $$$"""{"id":"{{{id}}}","pk":"a","name":"{{{name}}}"}""")),
                new PartitionKey("a"));
        }
    }

    [Fact]
    public async Task Query_Contains_Default_IsCaseSensitive()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE CONTAINS(c.name, 'ali')");
        var page = await iter.ReadNextAsync();

        // "ali" is in "alice" (id=2) but not "Alice" (starts with 'A')
        page.Should().HaveCount(1);
        page.First()["id"]!.Value<string>().Should().Be("2");
    }

    [Fact]
    public async Task Query_Contains_ThirdArgTrue_IsCaseInsensitive()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE CONTAINS(c.name, 'ALI', true)");
        var page = await iter.ReadNextAsync();

        page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["Alice", "alice"]);
    }

    [Fact]
    public async Task Query_StartsWith_Default_IsCaseSensitive()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE STARTSWITH(c.name, 'a')");
        var page = await iter.ReadNextAsync();

        page.Should().HaveCount(1);
        page.First()["name"]!.Value<string>().Should().Be("alice");
    }

    [Fact]
    public async Task Query_StartsWith_ThirdArgTrue_IsCaseInsensitive()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE STARTSWITH(c.name, 'a', true)");
        var page = await iter.ReadNextAsync();

        page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["Alice", "alice"]);
    }

    [Fact]
    public async Task Query_EndsWith_Default_IsCaseSensitive()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE ENDSWITH(c.name, 'CE')");
        var page = await iter.ReadNextAsync();

        // No name ends with uppercase "CE" — "Alice" ends with "ce", "alice" ends with "ce"
        page.Should().BeEmpty();
    }

    [Fact]
    public async Task Query_EndsWith_ThirdArgTrue_IsCaseInsensitive()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE ENDSWITH(c.name, 'CE', true)");
        var page = await iter.ReadNextAsync();

        page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["Alice", "alice"]);
    }

    [Fact]
    public async Task Query_StringEquals_Default_IsCaseSensitive()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE StringEquals(c.name, 'alice')");
        var page = await iter.ReadNextAsync();

        page.Should().HaveCount(1);
        page.First()["name"]!.Value<string>().Should().Be("alice");
    }

    [Fact]
    public async Task Query_StringEquals_ThirdArgTrue_IsCaseInsensitive()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE StringEquals(c.name, 'alice', true)");
        var page = await iter.ReadNextAsync();

        page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["Alice", "alice"]);
    }

    [Fact]
    public async Task Query_IndexOf_IsCaseSensitive()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT c.name, INDEX_OF(c.name, 'a') AS idx FROM c ORDER BY c.id");
        var page = await iter.ReadNextAsync();

        var results = page.ToList();
        // "Alice" → INDEX_OF('a') = -1 (capital A, not lowercase a)
        results[0]["idx"]!.Value<int>().Should().Be(-1);
        // "alice" → INDEX_OF('a') = 0
        results[1]["idx"]!.Value<int>().Should().Be(0);
    }

    [Fact]
    public async Task Query_Replace_IsCaseSensitive()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT REPLACE(c.name, 'a', 'X') AS replaced FROM c ORDER BY c.id");
        var page = await iter.ReadNextAsync();

        var results = page.ToList();
        // "Alice" → no 'a' (has 'A') → "Alice"
        results[0]["replaced"]!.Value<string>().Should().Be("Alice");
        // "alice" → has 'a' at 0 → "Xlice"
        results[1]["replaced"]!.Value<string>().Should().Be("Xlice");
    }

    [Fact]
    public async Task Query_RegexMatch_Default_IsCaseSensitive()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE RegexMatch(c.name, '^a')");
        var page = await iter.ReadNextAsync();

        page.Should().HaveCount(1);
        page.First()["name"]!.Value<string>().Should().Be("alice");
    }

    [Fact]
    public async Task Query_RegexMatch_WithIgnoreCaseModifier()
    {
        await SeedAsync();

        var iter = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE RegexMatch(c.name, '^a', 'i')");
        var page = await iter.ReadNextAsync();

        page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["Alice", "alice"]);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  C9: Case Transformations in Comparisons
// ═══════════════════════════════════════════════════════════════════════════

public class CaseTransformComparisonTests
{
    [Fact]
    public async Task Query_Lower_EnablesCaseInsensitiveEquality()
    {
        var container = new InMemoryContainer("lower-test", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","name":"Alice"}""")),
            new PartitionKey("a"));
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","pk":"a","name":"alice"}""")),
            new PartitionKey("a"));
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"3","pk":"a","name":"BOB"}""")),
            new PartitionKey("a"));

        var iter = container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE LOWER(c.name) = 'alice'");
        var page = await iter.ReadNextAsync();

        page.Should().HaveCount(2);
        page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["Alice", "alice"]);
    }

    [Fact]
    public async Task Query_Upper_EnablesCaseInsensitiveEquality()
    {
        var container = new InMemoryContainer("upper-test", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","name":"BOB"}""")),
            new PartitionKey("a"));
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","pk":"a","name":"bob"}""")),
            new PartitionKey("a"));
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"3","pk":"a","name":"Alice"}""")),
            new PartitionKey("a"));

        var iter = container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE UPPER(c.name) = 'BOB'");
        var page = await iter.ReadNextAsync();

        page.Should().HaveCount(2);
        page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["BOB", "bob"]);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  C10: DISTINCT, GROUP BY, MIN/MAX
// ═══════════════════════════════════════════════════════════════════════════

public class StringDistinctGroupByTests
{
    private async Task<InMemoryContainer> CreateSeededContainerAsync()
    {
        var container = new InMemoryContainer("case-dg", "/pk");
        foreach (var (id, name) in new[] { ("1", "Alice"), ("2", "alice"), ("3", "BOB"), ("4", "bob") })
        {
            await container.CreateItemStreamAsync(
                new MemoryStream(Encoding.UTF8.GetBytes(
                    $$$"""{"id":"{{{id}}}","pk":"a","name":"{{{name}}}"}""")),
                new PartitionKey("a"));
        }
        return container;
    }

    [Fact]
    public async Task Query_Distinct_PreservesCaseDifferences()
    {
        var container = await CreateSeededContainerAsync();

        var iter = container.GetItemQueryIterator<JObject>(
            "SELECT DISTINCT c.name FROM c");
        var page = await iter.ReadNextAsync();

        page.Should().HaveCount(4, "Alice, alice, BOB, bob are all distinct values");
    }

    [Fact]
    public async Task Query_GroupBy_TreatsCaseDifferencesAsSeparateGroups()
    {
        var container = await CreateSeededContainerAsync();

        var iter = container.GetItemQueryIterator<JObject>(
            "SELECT c.name, COUNT(1) AS cnt FROM c GROUP BY c.name");
        var page = await iter.ReadNextAsync();

        page.Should().HaveCount(4, "Alice, alice, BOB, bob should be 4 separate groups");
    }

    [Fact]
    public async Task Query_MinMax_String_UsesOrdinalComparison()
    {
        var container = await CreateSeededContainerAsync();

        var iterMin = container.GetItemQueryIterator<JValue>(
            "SELECT VALUE MIN(c.name) FROM c");
        var min = (await iterMin.ReadNextAsync()).First();

        var iterMax = container.GetItemQueryIterator<JValue>(
            "SELECT VALUE MAX(c.name) FROM c");
        var max = (await iterMax.ReadNextAsync()).First();

        // Ordinal: 'A' (65) < 'B' (66) < 'a' (97) < 'b' (98)
        min.Value<string>().Should().Be("Alice");
        max.Value<string>().Should().Be("bob");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  C11: Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class StringCaseEdgeCaseTests
{
    [Fact]
    public async Task Query_ParameterizedQuery_StringIsCaseSensitive()
    {
        var container = new InMemoryContainer("param-test", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","name":"Alice"}""")),
            new PartitionKey("a"));
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","pk":"a","name":"alice"}""")),
            new PartitionKey("a"));

        var query = new QueryDefinition("SELECT * FROM c WHERE c.name = @n")
            .WithParameter("@n", "Alice");
        var iter = container.GetItemQueryIterator<JObject>(query);
        var page = await iter.ReadNextAsync();

        page.Should().HaveCount(1);
        page.First()["id"]!.Value<string>().Should().Be("1");
    }

    [Fact]
    public async Task Query_PropertyNameLookup_IsCaseSensitive()
    {
        var container = new InMemoryContainer("propcase", "/pk");
        // JSON has property "name" (lowercase)
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","name":"Alice"}""")),
            new PartitionKey("a"));

        // Query with "Name" (capital N) — should not match the "name" property
        var iter = container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.Name = 'Alice'");
        var page = await iter.ReadNextAsync();

        page.Should().BeEmpty("JSON property lookup is case-sensitive: 'Name' != 'name'");
    }

    [Fact]
    public async Task Query_UnicodeCase_IsOrdinal()
    {
        var container = new InMemoryContainer("unicode-case", "/pk");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", name = "Über" }),
            new PartitionKey("a"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", name = "über" }),
            new PartitionKey("a"));

        var iter = container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.name = 'Über'");
        var page = await iter.ReadNextAsync();

        // Ordinal: 'Ü' (220) != 'ü' (252) → only exact match
        page.Should().HaveCount(1);
        page.First()["id"]!.Value<string>().Should().Be("1");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 1: StringConcat (||) null handling bug fix
// ═══════════════════════════════════════════════════════════════════════════

public class StringConcatNullTests
{
    [Fact]
    public async Task Query_StringConcat_WithNull_ReturnsNull()
    {
        var container = new InMemoryContainer("concat-null", "/pk");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", name = "hello" }),
            new PartitionKey("a"));

        var iter = container.GetItemQueryIterator<JToken>(
            "SELECT VALUE c.name || null FROM c");
        var page = await iter.ReadNextAsync();
        page.First().Type.Should().Be(JTokenType.Null);
    }

    [Fact]
    public async Task Query_StringConcat_NullOnLeft_ReturnsNull()
    {
        var container = new InMemoryContainer("concat-null", "/pk");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", name = "world" }),
            new PartitionKey("a"));

        var iter = container.GetItemQueryIterator<JToken>(
            "SELECT VALUE null || c.name FROM c");
        var page = await iter.ReadNextAsync();
        page.First().Type.Should().Be(JTokenType.Null);
    }

    [Fact]
    public async Task Query_StringConcat_PreservesCase()
    {
        var container = new InMemoryContainer("concat-case", "/pk");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }),
            new PartitionKey("a"));

        var iter = container.GetItemQueryIterator<JToken>(
            "SELECT VALUE c.name || '-suffix' FROM c");
        var page = await iter.ReadNextAsync();
        page.First().Value<string>().Should().Be("Alice-suffix");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 2: LIKE extended coverage
// ═══════════════════════════════════════════════════════════════════════════

public class StringLikeExtendedDeepDiveTests
{
    [Fact]
    public async Task Query_Like_WithEscape_IsCaseSensitive()
    {
        var container = new InMemoryContainer("like-esc", "/pk");
        await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "A%" }), new PartitionKey("a"));
        await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "a%" }), new PartitionKey("a"));
        await container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "a", name = "ABC" }), new PartitionKey("a"));

        var iter = container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.name LIKE 'A!%' ESCAPE '!'");
        var page = await iter.ReadNextAsync();
        page.Should().HaveCount(1);
        page.First()["name"]!.Value<string>().Should().Be("A%");
    }

    [Fact]
    public async Task Query_NotLike_WithEscape_IsCaseSensitive()
    {
        var container = new InMemoryContainer("like-esc", "/pk");
        await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "A%" }), new PartitionKey("a"));
        await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "a%" }), new PartitionKey("a"));
        await container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "a", name = "ABC" }), new PartitionKey("a"));

        var iter = container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.name NOT LIKE 'A!%' ESCAPE '!'");
        var page = await iter.ReadNextAsync();
        page.Should().HaveCount(2);
    }

    [Theory]
    [InlineData("+")]
    [InlineData("*")]
    [InlineData("^")]
    [InlineData("$")]
    [InlineData("(")]
    [InlineData(")")]
    [InlineData("{")]
    [InlineData("}")]
    [InlineData("?")]
    public async Task Query_Like_AdditionalRegexMetachars_TreatedAsLiteral(string metaChar)
    {
        var container = new InMemoryContainer("like-meta", "/pk");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", name = $"a{metaChar}b" }),
            new PartitionKey("a"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", name = "axb" }),
            new PartitionKey("a"));

        var iter = container.GetItemQueryIterator<JObject>(
            $"SELECT * FROM c WHERE c.name LIKE 'a{metaChar}b'");
        var page = await iter.ReadNextAsync();
        page.Should().HaveCount(1);
        page.First()["id"]!.Value<string>().Should().Be("1");
    }

    [Fact]
    public async Task Query_Like_OnNullField_DoesNotMatch()
    {
        var container = new InMemoryContainer("like-null", "/pk");
        await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

        var iter = container.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name LIKE 'A%'");
        var page = await iter.ReadNextAsync();
        page.Should().BeEmpty();
    }

    [Fact]
    public async Task Query_Like_PercentOnly_MatchesAllNonNull()
    {
        var container = new InMemoryContainer("like-pct", "/pk");
        await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }), new PartitionKey("a"));
        await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "" }), new PartitionKey("a"));
        await container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "a" }), new PartitionKey("a"));

        var iter = container.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name LIKE '%'");
        var page = await iter.ReadNextAsync();
        page.Should().HaveCount(2); // id 1 and 2 (not 3 — null)
    }

    [Fact]
    public async Task Query_Like_UnderscoreOnly_MatchesSingleCharStrings()
    {
        var container = new InMemoryContainer("like-under", "/pk");
        await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", code = "A" }), new PartitionKey("a"));
        await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", code = "AB" }), new PartitionKey("a"));
        await container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "a", code = "" }), new PartitionKey("a"));

        var iter = container.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.code LIKE '_'");
        var page = await iter.ReadNextAsync();
        page.Should().HaveCount(1);
        page.First()["code"]!.Value<string>().Should().Be("A");
    }

    [Fact]
    public async Task Query_Like_UnderscoreWithNewline_MatchesNewline()
    {
        var container = new InMemoryContainer("like-newline", "/pk");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", name = "a\nb" }),
            new PartitionKey("a"));

        var iter = container.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name LIKE 'a_b'");
        var page = await iter.ReadNextAsync();
        page.Should().HaveCount(1);
    }

    [Fact]
    public async Task Query_Like_BackslashInData_TreatedAsLiteral()
    {
        var container = new InMemoryContainer("like-bs", "/pk");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", name = @"a\b" }),
            new PartitionKey("a"));

        var iter = container.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name LIKE 'a\\b'");
        var page = await iter.ReadNextAsync();
        page.Should().HaveCount(1);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 3: String functions — explicit false, null, empty
// ═══════════════════════════════════════════════════════════════════════════

public class StringFunctionEdgeCaseDeepDiveTests
{
    private async Task<InMemoryContainer> SeedAsync()
    {
        var c = new InMemoryContainer("func-edge", "/pk");
        await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }), new PartitionKey("a"));
        await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "alice" }), new PartitionKey("a"));
        await c.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "a", name = "Bob" }), new PartitionKey("a"));
        return c;
    }

    [Fact]
    public async Task Query_Contains_ExplicitFalse_SameAsDefault()
    {
        var c = await SeedAsync();
        var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE CONTAINS(c.name, 'ali', false)");
        var page = await iter.ReadNextAsync();
        page.Should().HaveCount(1);
        page.First()["name"]!.Value<string>().Should().Be("alice");
    }

    [Fact]
    public async Task Query_StartsWith_ExplicitFalse_SameAsDefault()
    {
        var c = await SeedAsync();
        var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE STARTSWITH(c.name, 'a', false)");
        var page = await iter.ReadNextAsync();
        page.Should().HaveCount(1);
        page.First()["name"]!.Value<string>().Should().Be("alice");
    }

    [Fact]
    public async Task Query_EndsWith_ExplicitFalse_SameAsDefault()
    {
        var c = await SeedAsync();
        var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE ENDSWITH(c.name, 'ce', false)");
        var page = await iter.ReadNextAsync();
        page.Should().HaveCount(2); // Both "Alice" and "alice" end with "ce" case-sensitively
    }

    [Fact]
    public async Task Query_StringEquals_ExplicitFalse_SameAsDefault()
    {
        var c = await SeedAsync();
        var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE STRING_EQUALS(c.name, 'alice', false)");
        var page = await iter.ReadNextAsync();
        page.Should().HaveCount(1);
    }

    [Fact]
    public async Task Query_Contains_NullInput_ReturnsNoMatch()
    {
        var c = new InMemoryContainer("null-fn", "/pk");
        await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

        var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE CONTAINS(c.name, 'x')");
        var page = await iter.ReadNextAsync();
        page.Should().BeEmpty();
    }

    [Fact]
    public async Task Query_StartsWith_NullInput_ReturnsNoMatch()
    {
        var c = new InMemoryContainer("null-fn", "/pk");
        await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

        var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE STARTSWITH(c.name, 'x')");
        var page = await iter.ReadNextAsync();
        page.Should().BeEmpty();
    }

    [Fact]
    public async Task Query_EndsWith_NullInput_ReturnsNoMatch()
    {
        var c = new InMemoryContainer("null-fn", "/pk");
        await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

        var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE ENDSWITH(c.name, 'x')");
        var page = await iter.ReadNextAsync();
        page.Should().BeEmpty();
    }

    [Fact]
    public async Task Query_Contains_EmptySearchString_MatchesAll()
    {
        var c = await SeedAsync();
        var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE CONTAINS(c.name, '')");
        var page = await iter.ReadNextAsync();
        page.Should().HaveCount(3);
    }

    [Fact]
    public async Task Query_StartsWith_EmptyPrefix_MatchesAll()
    {
        var c = await SeedAsync();
        var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE STARTSWITH(c.name, '')");
        var page = await iter.ReadNextAsync();
        page.Should().HaveCount(3);
    }

    [Fact]
    public async Task Query_EndsWith_EmptySuffix_MatchesAll()
    {
        var c = await SeedAsync();
        var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE ENDSWITH(c.name, '')");
        var page = await iter.ReadNextAsync();
        page.Should().HaveCount(3);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 4: ARRAY_CONTAINS and CONCAT
// ═══════════════════════════════════════════════════════════════════════════

public class StringArrayConcatTests
{
    [Fact]
    public async Task Query_ArrayContains_StringElement_IsCaseSensitive()
    {
        var c = new InMemoryContainer("arr-case", "/pk");
        await c.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", tags = new[] { "Alice" } }),
            new PartitionKey("a"));

        var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE ARRAY_CONTAINS(c.tags, 'alice')");
        var page = await iter.ReadNextAsync();
        page.Should().BeEmpty(); // case-sensitive: "Alice" ≠ "alice"
    }

    [Fact]
    public async Task Query_ConcatFunction_NullArg_ReturnsUndefined()
    {
        var c = new InMemoryContainer("concat-fn", "/pk");
        await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

        var iter = c.GetItemQueryIterator<JToken>(
            "SELECT VALUE CONCAT('a', null, 'b') FROM c");
        var page = await iter.ReadNextAsync();
        // Cosmos DB: CONCAT with null arg returns undefined
        page.Should().BeEmpty();
    }

    [Fact]
    public async Task Query_ConcatFunction_PreservesCase()
    {
        var c = new InMemoryContainer("concat-fn", "/pk");
        await c.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", first = "Alice", last = "BOB" }),
            new PartitionKey("a"));

        var iter = c.GetItemQueryIterator<JToken>(
            "SELECT VALUE CONCAT(c.first, ' ', c.last) FROM c");
        var page = await iter.ReadNextAsync();
        page.First().Value<string>().Should().Be("Alice BOB");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 5: Null and empty value edge cases
// ═══════════════════════════════════════════════════════════════════════════

public class StringNullEmptyEdgeCaseTests
{
    [Fact]
    public async Task Query_EmptyStringEquality_Works()
    {
        var c = new InMemoryContainer("empty-eq", "/pk");
        await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "" }), new PartitionKey("a"));
        await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "Bob" }), new PartitionKey("a"));

        var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name = ''");
        var page = await iter.ReadNextAsync();
        page.Should().HaveCount(1);
        page.First()["id"]!.Value<string>().Should().Be("1");
    }

    [Fact]
    public async Task Query_Between_WithNullValue_ExcludesNull()
    {
        var c = new InMemoryContainer("between-null", "/pk");
        await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Bob" }), new PartitionKey("a"));
        await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a" }), new PartitionKey("a")); // name is null/undefined

        var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name BETWEEN 'A' AND 'Z'");
        var page = await iter.ReadNextAsync();
        page.Should().HaveCount(1);
        page.First()["id"]!.Value<string>().Should().Be("1");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 6: Composed operations
// ═══════════════════════════════════════════════════════════════════════════

public class StringComposedOperationTests
{
    private async Task<InMemoryContainer> SeedAsync()
    {
        var c = new InMemoryContainer("composed-test", "/pk");
        await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }), new PartitionKey("a"));
        await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "alice" }), new PartitionKey("a"));
        await c.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "a", name = "Bob" }), new PartitionKey("a"));
        await c.CreateItemAsync(JObject.FromObject(new { id = "4", pk = "a", name = "bob" }), new PartitionKey("a"));
        return c;
    }

    [Fact]
    public async Task Query_DistinctLower_CollapsesCaseDifferences()
    {
        var c = await SeedAsync();
        var iter = c.GetItemQueryIterator<JToken>(
            "SELECT DISTINCT VALUE LOWER(c.name) FROM c");
        var page = await iter.ReadNextAsync();
        page.Select(t => t.Value<string>()).Should().BeEquivalentTo("alice", "bob");
    }

    [Fact]
    public async Task Query_GroupByLower_MergesCaseVariants()
    {
        var c = await SeedAsync();
        var iter = c.GetItemQueryIterator<JObject>(
            "SELECT LOWER(c.name) AS lname, COUNT(1) AS cnt FROM c GROUP BY LOWER(c.name)");
        var page = await iter.ReadNextAsync();
        var results = page.ToList();
        results.Should().HaveCount(2);
        results.Should().Contain(r => r["lname"]!.Value<string>() == "alice" && r["cnt"]!.Value<int>() == 2);
        results.Should().Contain(r => r["lname"]!.Value<string>() == "bob" && r["cnt"]!.Value<int>() == 2);
    }

    [Fact]
    public async Task Query_RegexMatch_CombinedIgnoreCaseMultiline()
    {
        var c = new InMemoryContainer("regex-combined", "/pk");
        await c.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", text = "hello\nAlice" }),
            new PartitionKey("a"));

        var iter = c.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE RegexMatch(c.text, '^alice', 'im')");
        var page = await iter.ReadNextAsync();
        page.Should().HaveCount(1); // "Alice" at start of second line, case-insensitive
    }

    [Fact]
    public async Task Query_MultipleOrderBy_StringTiebreaking()
    {
        var c = new InMemoryContainer("multi-order", "/pk");
        await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", last = "Smith", first = "alice" }), new PartitionKey("a"));
        await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", last = "Smith", first = "Alice" }), new PartitionKey("a"));
        await c.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "a", last = "Brown", first = "Bob" }), new PartitionKey("a"));

        var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c ORDER BY c.last ASC, c.first ASC");
        var page = await iter.ReadNextAsync();
        var ids = page.Select(d => d["id"]!.Value<string>()).ToList();
        ids[0].Should().Be("3"); // Brown
        // Smith, Alice vs Smith, alice — ordinal: 'A'(65) < 'a'(97)
        ids[1].Should().Be("2"); // Alice
        ids[2].Should().Be("1"); // alice
    }

    [Fact]
    public async Task Query_CrossPartition_CaseSensitivity_Consistent()
    {
        var c = new InMemoryContainer("cross-pk", "/pk");
        await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }), new PartitionKey("a"));
        await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "b", name = "alice" }), new PartitionKey("b"));

        var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name = 'Alice'");
        var page = await iter.ReadNextAsync();
        page.Should().HaveCount(1);
        page.First()["pk"]!.Value<string>().Should().Be("a");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 7: INDEX_OF null input
// ═══════════════════════════════════════════════════════════════════════════

public class StringIndexOfEdgeCaseTests
{
    [Fact]
    public async Task Query_IndexOf_NullInput_ReturnsNull()
    {
        var c = new InMemoryContainer("indexof-null", "/pk");
        await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

        var iter = c.GetItemQueryIterator<JObject>(
            "SELECT INDEX_OF(c.name, 'a') AS idx FROM c");
        var page = await iter.ReadNextAsync();
        page.First()["idx"]!.Type.Should().Be(JTokenType.Null);
    }

    [Fact]
    public async Task Query_IndexOf_WithStartPosition_IsCaseSensitive()
    {
        var c = new InMemoryContainer("indexof-start", "/pk");
        await c.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", name = "hello hello" }),
            new PartitionKey("a"));

        var iter = c.GetItemQueryIterator<JObject>(
            "SELECT INDEX_OF(c.name, 'hello', 1) AS idx FROM c");
        var page = await iter.ReadNextAsync();
        page.First()["idx"]!.Value<int>().Should().Be(6);
    }
}
