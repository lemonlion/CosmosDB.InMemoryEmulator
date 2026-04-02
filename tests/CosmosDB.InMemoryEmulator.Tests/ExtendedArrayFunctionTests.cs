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

    private async Task SeedTypeDiverseItems()
    {
        await SeedItems();

        // id=5: numeric tags [1, 2, 3]
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "5", partitionKey = "pk1", tags = new[] { 1, 2, 3 } }),
            new PartitionKey("pk1"));

        // id=6: mixed types [1, "a", true, null]
        var mixed = new JObject { ["id"] = "6", ["partitionKey"] = "pk1", ["tags"] = new JArray(1, "a", true, JValue.CreateNull()) };
        await _container.CreateItemAsync(mixed, new PartitionKey("pk1"));

        // id=7: nested array property
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "7", partitionKey = "pk1", nested = new { items = new[] { "p", "q" } } }),
            new PartitionKey("pk1"));

        // id=8: no tags property at all
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "8", partitionKey = "pk1", name = "NoTags" }),
            new PartitionKey("pk1"));

        // id=9: tags is a string scalar, not an array
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "9", partitionKey = "pk1", tags = "not-an-array" }),
            new PartitionKey("pk1"));

        // id=10: tags with duplicates ["a", "a", "b"]
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "10", partitionKey = "pk1", tags = new[] { "a", "a", "b" } }),
            new PartitionKey("pk1"));

        // id=11: two array properties for both-from-document tests
        var twoArrays = new JObject { ["id"] = "11", ["partitionKey"] = "pk1", ["tags"] = new JArray("a", "b"), ["otherTags"] = new JArray("b", "c") };
        await _container.CreateItemAsync(twoArrays, new PartitionKey("pk1"));
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

    [Fact]
    public async Task ArrayContainsAny_WithNumericElements_ReturnsTrue()
    {
        await SeedTypeDiverseItems();
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, [2, 4]) FROM c WHERE c.id = '5'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task ArrayContainsAny_WithMixedTypes_MatchesSameTypeOnly()
    {
        await SeedTypeDiverseItems();
        // Item 6 has [1, "a", true, null] — searching for ["a"] should match the string "a"
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, ['a']) FROM c WHERE c.id = '6'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task ArrayContainsAny_NumberDoesNotMatchString_TypeSensitive()
    {
        await SeedTypeDiverseItems();
        // Item 5 has [1, 2, 3] — searching for string "1" should NOT match number 1
        // Real Cosmos DB is type-sensitive: number ≠ string
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, ['1']) FROM c WHERE c.id = '5'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeFalse();
    }

    [Fact]
    public async Task ArrayContainsAny_BoolDoesNotMatchString_TypeSensitive()
    {
        await SeedTypeDiverseItems();
        // Item 6 has [1, "a", true, null] — searching for string "true" should NOT match boolean true
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, ['true']) FROM c WHERE c.id = '6'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeFalse();
    }

    [Fact]
    public async Task ArrayContainsAny_WithNullElement_MatchesNull()
    {
        await SeedTypeDiverseItems();
        // Item 6 has [1, "a", true, null] — searching for [null] should match
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, [null]) FROM c WHERE c.id = '6'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task ArrayContainsAny_NonExistentProperty_ReturnsFalse()
    {
        await SeedTypeDiverseItems();
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.nonExistent, ['a']) FROM c WHERE c.id = '8'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeFalse();
    }

    [Fact]
    public async Task ArrayContainsAny_NonArrayProperty_ReturnsFalse()
    {
        await SeedTypeDiverseItems();
        // Item 9 has tags = "not-an-array" (string scalar)
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, ['a']) FROM c WHERE c.id = '9'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeFalse();
    }

    [Fact]
    public async Task ArrayContainsAny_InProjection_ReturnsBoolean()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT ARRAY_CONTAINS_ANY(c.tags, ['a']) AS result FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        results[0]["result"]!.Value<bool>().Should().BeTrue();
    }

    [Fact]
    public async Task ArrayContainsAny_WithDuplicatesInSource_StillMatches()
    {
        await SeedTypeDiverseItems();
        // Item 10 has ["a", "a", "b"]
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, ['a']) FROM c WHERE c.id = '10'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task ArrayContainsAny_WithDuplicatesInSearch_StillMatches()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, ['a', 'a']) FROM c WHERE c.id = '1'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task ArrayContainsAny_BothArraysFromDocument_Works()
    {
        await SeedTypeDiverseItems();
        // Item 11 has tags=["a","b"] and otherTags=["b","c"] — overlap is "b"
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, c.otherTags) FROM c WHERE c.id = '11'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task ArrayContainsAny_NestedPropertyPath_Works()
    {
        await SeedTypeDiverseItems();
        // Item 7 has nested.items = ["p", "q"]
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.nested.items, ['p']) FROM c WHERE c.id = '7'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task ArrayContainsAny_SingleElementMatch_ReturnsTrue()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, ['a']) FROM c WHERE c.id = '1'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task ArrayContainsAny_VariadicForm_Works()
    {
        await SeedItems();
        // Real Cosmos DB syntax: ARRAY_CONTAINS_ANY(array, val1, val2, ...)
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, 'a', 'z') FROM c WHERE c.id = '1'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task ArrayContainsAny_VariadicForm_SingleArg_Works()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, 'a') FROM c WHERE c.id = '1'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task ArrayContainsAny_VariadicForm_NoMatch_ReturnsFalse()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, 'q', 'r') FROM c WHERE c.id = '1'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeFalse();
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

    [Fact]
    public async Task ArrayContainsAll_SourceEqualsSearch_ReturnsTrue()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, ['a', 'b', 'c']) FROM c WHERE c.id = '1'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task ArrayContainsAll_SourceIsSupersetOfSearch_ReturnsTrue()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, ['a', 'b']) FROM c WHERE c.id = '1'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task ArrayContainsAll_EmptySourceWithNonEmptySearch_ReturnsFalse()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, ['a']) FROM c WHERE c.id = '4'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeFalse();
    }

    [Fact]
    public async Task ArrayContainsAll_WithNumericElements_ReturnsTrue()
    {
        await SeedTypeDiverseItems();
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, [1, 3]) FROM c WHERE c.id = '5'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task ArrayContainsAll_NumberDoesNotMatchString_TypeSensitive()
    {
        await SeedTypeDiverseItems();
        // Item 5 has [1, 2, 3] — searching for strings "1","2" should NOT match
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, ['1', '2']) FROM c WHERE c.id = '5'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeFalse();
    }

    [Fact]
    public async Task ArrayContainsAll_WithNullElement_MatchesNull()
    {
        await SeedTypeDiverseItems();
        // Item 6 has [1, "a", true, null] — searching for [null] should match
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, [null]) FROM c WHERE c.id = '6'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task ArrayContainsAll_NonExistentProperty_ReturnsFalse()
    {
        await SeedTypeDiverseItems();
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.nonExistent, ['a']) FROM c WHERE c.id = '8'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeFalse();
    }

    [Fact]
    public async Task ArrayContainsAll_NonArrayProperty_ReturnsFalse()
    {
        await SeedTypeDiverseItems();
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, ['a']) FROM c WHERE c.id = '9'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeFalse();
    }

    [Fact]
    public async Task ArrayContainsAll_DuplicatesInSearch_StillWorks()
    {
        await SeedItems();
        // Source has "a", search has "a" twice — should still be true
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, ['a', 'a']) FROM c WHERE c.id = '1'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task ArrayContainsAll_InProjection_ReturnsBoolean()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT ARRAY_CONTAINS_ALL(c.tags, ['a']) AS result FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        results[0]["result"]!.Value<bool>().Should().BeTrue();
    }

    [Fact]
    public async Task ArrayContainsAll_BothEmptyArrays_ReturnsTrue()
    {
        await SeedItems();
        // Empty search against empty source → vacuously true
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, []) FROM c WHERE c.id = '4'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task ArrayContainsAll_VariadicForm_Works()
    {
        await SeedItems();
        // Real Cosmos DB syntax: ARRAY_CONTAINS_ALL(array, val1, val2, ...)
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, 'a', 'b', 'c') FROM c WHERE c.id = '1'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task ArrayContainsAll_VariadicForm_SingleMissing_ReturnsFalse()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, 'a', 'q') FROM c WHERE c.id = '1'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeFalse();
    }

    [Fact]
    public async Task ArrayContainsAll_NestedPropertyPath_Works()
    {
        await SeedTypeDiverseItems();
        // Item 7 has nested.items = ["p", "q"]
        var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.nested.items, ['p', 'q']) FROM c WHERE c.id = '7'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
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

    [Fact]
    public async Task SetIntersect_BothEmpty_ReturnsEmptyArray()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT SetIntersect(c.tags, []) AS common FROM c WHERE c.id = '4'");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        var common = (JArray)results[0]["common"]!;
        common.Should().BeEmpty();
    }

    [Fact]
    public async Task SetIntersect_EmptySecondArray_ReturnsEmptyArray()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT SetIntersect(c.tags, []) AS common FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        var common = (JArray)results[0]["common"]!;
        common.Should().BeEmpty();
    }

    [Fact]
    public async Task SetIntersect_CompleteOverlap_ReturnsSameElements()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT SetIntersect(c.tags, c.tags) AS common FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        var common = (JArray)results[0]["common"]!;
        common.Select(t => t.ToString()).Should().BeEquivalentTo(["a", "b", "c"]);
    }

    [Fact]
    public async Task SetIntersect_DuplicatesInInput_DedupedInResult()
    {
        await SeedTypeDiverseItems();
        // Item 10 has ["a", "a", "b"] — intersect with ["a","b"] should give ["a","b"] (deduped)
        var query = new QueryDefinition("SELECT SetIntersect(c.tags, ['a', 'b']) AS common FROM c WHERE c.id = '10'");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        var common = (JArray)results[0]["common"]!;
        common.Select(t => t.ToString()).Should().Equal("a", "b");
    }

    [Fact]
    public async Task SetIntersect_WithNumericElements_Works()
    {
        await SeedTypeDiverseItems();
        // Item 5 has [1, 2, 3] — intersect with [2, 4] → [2]
        var query = new QueryDefinition("SELECT SetIntersect(c.tags, [2, 4]) AS common FROM c WHERE c.id = '5'");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        var common = (JArray)results[0]["common"]!;
        common.Should().ContainSingle();
        common[0].Value<int>().Should().Be(2);
    }

    [Fact]
    public async Task SetIntersect_TypeSensitive_NumberDoesNotMatchString()
    {
        await SeedTypeDiverseItems();
        // Item 5 has [1, 2, 3] — intersect with ["1","2"] → [] (number ≠ string)
        var query = new QueryDefinition("SELECT SetIntersect(c.tags, ['1', '2']) AS common FROM c WHERE c.id = '5'");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        var common = (JArray)results[0]["common"]!;
        common.Should().BeEmpty();
    }

    [Fact]
    public async Task SetIntersect_WithNullElements_Works()
    {
        await SeedTypeDiverseItems();
        // Item 6 has [1, "a", true, null] — intersect with [null, "a"] → [null is tricky, "a" should match]
        var query = new QueryDefinition("SELECT SetIntersect(c.tags, [null, 'a']) AS common FROM c WHERE c.id = '6'");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        var common = (JArray)results[0]["common"]!;
        common.Should().HaveCount(2);
    }

    [Fact]
    public async Task SetIntersect_NestedInArrayLength_Works()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT ARRAY_LENGTH(SetIntersect(c.tags, ['a', 'b', 'x'])) AS count FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        results[0]["count"]!.Value<long>().Should().Be(2);
    }

    [Fact]
    public async Task SetIntersect_BothFromDocument_Works()
    {
        await SeedTypeDiverseItems();
        // Item 11 has tags=["a","b"] and otherTags=["b","c"]
        var query = new QueryDefinition("SELECT SetIntersect(c.tags, c.otherTags) AS common FROM c WHERE c.id = '11'");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        var common = (JArray)results[0]["common"]!;
        common.Select(t => t.ToString()).Should().Equal("b");
    }

    [Fact]
    public async Task SetIntersect_WithMixedTypes_OnlyMatchesSameType()
    {
        await SeedTypeDiverseItems();
        // Item 6 has [1, "a", true, null] — intersect with [1, "b", true] → [1, true]
        var query = new QueryDefinition("SELECT SetIntersect(c.tags, [1, 'b', true]) AS common FROM c WHERE c.id = '6'");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        var common = (JArray)results[0]["common"]!;
        common.Should().HaveCount(2);
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

    [Fact]
    public async Task SetUnion_BothEmpty_ReturnsEmptyArray()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT SetUnion(c.tags, []) AS combined FROM c WHERE c.id = '4'");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        var combined = (JArray)results[0]["combined"]!;
        combined.Should().BeEmpty();
    }

    [Fact]
    public async Task SetUnion_EmptySecondArray_ReturnsFirstArray()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT SetUnion(c.tags, []) AS combined FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        var combined = (JArray)results[0]["combined"]!;
        combined.Select(t => t.ToString()).Should().BeEquivalentTo(["a", "b", "c"]);
    }

    [Fact]
    public async Task SetUnion_DuplicatesWithinSingleArray_Deduped()
    {
        await SeedTypeDiverseItems();
        // Item 10 has ["a", "a", "b"] — union with ["c"] → ["a", "b", "c"]
        var query = new QueryDefinition("SELECT SetUnion(c.tags, ['c']) AS combined FROM c WHERE c.id = '10'");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        var combined = (JArray)results[0]["combined"]!;
        combined.Select(t => t.ToString()).Should().Equal("a", "b", "c");
    }

    [Fact]
    public async Task SetUnion_WithNumericElements_Works()
    {
        await SeedTypeDiverseItems();
        // Item 5 has [1, 2, 3] — union with [3, 4] → [1, 2, 3, 4]
        var query = new QueryDefinition("SELECT SetUnion(c.tags, [3, 4]) AS combined FROM c WHERE c.id = '5'");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        var combined = (JArray)results[0]["combined"]!;
        combined.Select(t => t.Value<int>()).Should().Equal(1, 2, 3, 4);
    }

    [Fact]
    public async Task SetUnion_TypeSensitive_NumberAndStringBothKept()
    {
        await SeedTypeDiverseItems();
        // Item 5 has [1, 2, 3] — union with ["1","2"] → [1, 2, 3, "1", "2"] (type-sensitive, no dedup)
        var query = new QueryDefinition("SELECT SetUnion(c.tags, ['1', '2']) AS combined FROM c WHERE c.id = '5'");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        var combined = (JArray)results[0]["combined"]!;
        combined.Should().HaveCount(5);
    }

    [Fact]
    public async Task SetUnion_OrderPreserved_FirstThenSecond()
    {
        await SeedItems();
        // Item 1 has ["a","b","c"] — union with ["d","e"] → order should be a,b,c,d,e
        var query = new QueryDefinition("SELECT SetUnion(c.tags, ['d', 'e']) AS combined FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        var combined = (JArray)results[0]["combined"]!;
        combined.Select(t => t.ToString()).Should().Equal("a", "b", "c", "d", "e");
    }

    [Fact]
    public async Task SetUnion_WithNullElements_Works()
    {
        await SeedTypeDiverseItems();
        // Item 6 has [1, "a", true, null] — union with ["b", null] → null deduped
        var query = new QueryDefinition("SELECT SetUnion(c.tags, ['b', null]) AS combined FROM c WHERE c.id = '6'");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        var combined = (JArray)results[0]["combined"]!;
        // Should have [1, "a", true, null, "b"] — 5 elements (null deduped)
        combined.Should().HaveCount(5);
    }

    [Fact]
    public async Task SetUnion_NestedInArrayLength_Works()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT ARRAY_LENGTH(SetUnion(c.tags, ['x', 'y'])) AS count FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        results[0]["count"]!.Value<long>().Should().Be(5);
    }

    [Fact]
    public async Task SetUnion_BothFromDocument_Works()
    {
        await SeedTypeDiverseItems();
        // Item 11 has tags=["a","b"] and otherTags=["b","c"]
        var query = new QueryDefinition("SELECT SetUnion(c.tags, c.otherTags) AS combined FROM c WHERE c.id = '11'");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        var combined = (JArray)results[0]["combined"]!;
        combined.Select(t => t.ToString()).Should().Equal("a", "b", "c");
    }

    [Fact]
    public async Task SetUnion_WithMixedTypes_AllKept()
    {
        await SeedTypeDiverseItems();
        // Item 6 has [1, "a", true, null] — union with [2, "b", false] → all 7 kept
        var query = new QueryDefinition("SELECT SetUnion(c.tags, [2, 'b', false]) AS combined FROM c WHERE c.id = '6'");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        var combined = (JArray)results[0]["combined"]!;
        combined.Should().HaveCount(7);
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

    // ── Cross-cutting / Integration ─────────────────────────────────────────

    [Fact]
    public async Task ExtendedArrayFunctions_CombinedInWhereClause_Works()
    {
        await SeedItems();
        // Item 1 has ["a","b","c"] — matches both conditions
        var query = new QueryDefinition(
            "SELECT * FROM c WHERE ARRAY_CONTAINS_ANY(c.tags, ['a']) AND ARRAY_CONTAINS_ALL(c.tags, ['a', 'b'])");

        var results = await QueryAll<TestDocument>(query);

        results.Should().ContainSingle();
        results[0].Id.Should().Be("1");
    }

    [Fact]
    public async Task SetIntersect_ResultUsedInWhere_WithArrayLength()
    {
        await SeedItems();
        // Items with at least 1 overlap with ["a","b"] — item 1 has a,b; item 2 has b
        var query = new QueryDefinition(
            "SELECT * FROM c WHERE ARRAY_LENGTH(SetIntersect(c.tags, ['a', 'b'])) > 0");

        var results = await QueryAll<TestDocument>(query);

        results.Should().HaveCount(2);
        results.Select(r => r.Id).Should().BeEquivalentTo(["1", "2"]);
    }

    // ── Divergent behavior tests ────────────────────────────────────────────

    // DIVERGENT: SetIntersect returns empty array [] instead of undefined when
    // the source property doesn't exist. Real Cosmos DB would return undefined
    // (omit the property from the result).
    [Fact]
    public async Task SetIntersect_NonExistentProperty_ReturnsUndefined()
    {
        await SeedTypeDiverseItems();
        // Expected real Cosmos behavior: property "common" would not appear in result
        var query = new QueryDefinition("SELECT SetIntersect(c.nonExistent, ['a']) AS common FROM c WHERE c.id = '8'");
        var results = await QueryAll<JObject>(query);
        results.Should().ContainSingle();
        results[0]["common"].Should().BeNull(); // property should be absent
    }

    // Sister test: emulator now matches real Cosmos behavior
    [Fact]
    public async Task SetIntersect_NonExistentProperty_EmulatorAlsoReturnsUndefined()
    {
        // The emulator now correctly returns undefined (property absent) when input
        // array property doesn't exist, matching real Cosmos DB behavior.
        await SeedTypeDiverseItems();
        var query = new QueryDefinition("SELECT SetIntersect(c.nonExistent, ['a']) AS common FROM c WHERE c.id = '8'");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        results[0]["common"].Should().BeNull(); // property absent
    }

    [Fact]
    public async Task SetUnion_NonExistentProperty_ReturnsUndefined()
    {
        await SeedTypeDiverseItems();
        var query = new QueryDefinition("SELECT SetUnion(c.nonExistent, ['a']) AS combined FROM c WHERE c.id = '8'");
        var results = await QueryAll<JObject>(query);
        results.Should().ContainSingle();
        results[0]["combined"].Should().BeNull();
    }

    // Sister test: emulator now matches real Cosmos behavior
    [Fact]
    public async Task SetUnion_NonExistentProperty_EmulatorAlsoReturnsUndefined()
    {
        // The emulator now correctly returns undefined (property absent) when input
        // array property doesn't exist, matching real Cosmos DB behavior.
        await SeedTypeDiverseItems();
        var query = new QueryDefinition("SELECT SetUnion(c.nonExistent, ['a']) AS combined FROM c WHERE c.id = '8'");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        results[0]["combined"].Should().BeNull(); // property absent
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
