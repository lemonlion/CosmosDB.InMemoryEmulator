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
