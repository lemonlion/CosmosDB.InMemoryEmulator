using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class ExtendedArrayFunctionTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private async Task SeedItems()
    {
        var items = new[]
        {
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, Tags = ["a", "b", "c"] },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, Tags = ["b", "c", "d"] },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30, Tags = ["x", "y", "z"] },
            new TestDocument { Id = "4", PartitionKey = "pk1", Name = "Diana", Value = 40, Tags = [] },
        };
        foreach (var item in items)
        {
            await _container.CreateItemAsync(item, new PartitionKey(item.PartitionKey));
        }
    }

    // ── ARRAY_CONTAINS_ANY ──────────────────────────────────────────────────

    [Fact]
    public async Task ArrayContainsAny_WithMatchingElement_ReturnsTrue()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE ARRAY_CONTAINS_ANY(c.tags, ['a', 'z'])");

        var results = await QueryAll<TestDocument>(query);

        // Item 1 has "a", Item 3 has "z"
        results.Should().HaveCount(2);
        results.Select(r => r.Id).Should().BeEquivalentTo(["1", "3"]);
    }

    [Fact]
    public async Task ArrayContainsAny_WithNoMatchingElement_ReturnsFalse()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE ARRAY_CONTAINS_ANY(c.tags, ['q', 'r'])");

        var results = await QueryAll<TestDocument>(query);

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task ArrayContainsAny_WithEmptySearchArray_ReturnsFalse()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE ARRAY_CONTAINS_ANY(c.tags, [])");

        var results = await QueryAll<TestDocument>(query);

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task ArrayContainsAny_WithEmptySourceArray_ReturnsFalse()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE ARRAY_CONTAINS_ANY(c.tags, ['a']) AND c.id = '4'");

        var results = await QueryAll<TestDocument>(query);

        results.Should().BeEmpty();
    }

    // ── ARRAY_CONTAINS_ALL ──────────────────────────────────────────────────

    [Fact]
    public async Task ArrayContainsAll_WhenAllPresent_ReturnsTrue()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE ARRAY_CONTAINS_ALL(c.tags, ['a', 'b'])");

        var results = await QueryAll<TestDocument>(query);

        // Only Item 1 has both "a" and "b"
        results.Should().HaveCount(1);
        results[0].Id.Should().Be("1");
    }

    [Fact]
    public async Task ArrayContainsAll_WhenSomeMissing_ReturnsFalse()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE ARRAY_CONTAINS_ALL(c.tags, ['a', 'd'])");

        var results = await QueryAll<TestDocument>(query);

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task ArrayContainsAll_WithEmptySearchArray_ReturnsAll()
    {
        await SeedItems();
        // In Cosmos DB, ARRAY_CONTAINS_ALL with an empty search array returns true for all items
        var query = new QueryDefinition("SELECT * FROM c WHERE ARRAY_CONTAINS_ALL(c.tags, [])");

        var results = await QueryAll<TestDocument>(query);

        results.Should().HaveCount(4);
    }

    // ── SetIntersect ────────────────────────────────────────────────────────

    [Fact]
    public async Task SetIntersect_ReturnsCommonElements()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT SetIntersect(c.tags, ['b', 'c', 'z']) AS common FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        var common = (JArray)results[0]["common"]!;
        common.Select(t => t.ToString()).Should().BeEquivalentTo(["b", "c"]);
    }

    [Fact]
    public async Task SetIntersect_WithNoOverlap_ReturnsEmptyArray()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT SetIntersect(c.tags, ['q', 'r']) AS common FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        var common = (JArray)results[0]["common"]!;
        common.Should().BeEmpty();
    }

    [Fact]
    public async Task SetIntersect_WithEmptySourceArray_ReturnsEmptyArray()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT SetIntersect(c.tags, ['a']) AS common FROM c WHERE c.id = '4'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        var common = (JArray)results[0]["common"]!;
        common.Should().BeEmpty();
    }

    // ── SetUnion ────────────────────────────────────────────────────────────

    [Fact]
    public async Task SetUnion_ReturnsCombinedDistinctElements()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT SetUnion(c.tags, ['b', 'd', 'e']) AS combined FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        var combined = (JArray)results[0]["combined"]!;
        combined.Select(t => t.ToString()).Should().BeEquivalentTo(["a", "b", "c", "d", "e"]);
    }

    [Fact]
    public async Task SetUnion_WithEmptySourceArray_ReturnsSecondArray()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT SetUnion(c.tags, ['a', 'b']) AS combined FROM c WHERE c.id = '4'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        var combined = (JArray)results[0]["combined"]!;
        combined.Select(t => t.ToString()).Should().BeEquivalentTo(["a", "b"]);
    }

    [Fact]
    public async Task SetUnion_WithIdenticalArrays_ReturnsOriginal()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT SetUnion(c.tags, c.tags) AS combined FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        var combined = (JArray)results[0]["combined"]!;
        combined.Select(t => t.ToString()).Should().BeEquivalentTo(["a", "b", "c"]);
    }

    // ── VALUE expressions with array functions ──────────────────────────────

    [Fact]
    public async Task ArrayContainsAny_AsValueExpression_ReturnsBoolean()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, ['a']) FROM c WHERE c.id = '1'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task ArrayContainsAll_AsValueExpression_ReturnsBoolean()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, ['a', 'b', 'c']) FROM c WHERE c.id = '1'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

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
