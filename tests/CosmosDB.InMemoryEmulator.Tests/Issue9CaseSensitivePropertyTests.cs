using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for Issue #9: Newtonsoft.Json case-insensitive deserialization conflicts
/// with case-sensitive property names. The emulator should preserve exact JSON
/// property casing, matching the real Cosmos DB behaviour.
/// </summary>
public class Issue9CaseSensitivePropertyTests
{
    private class EquinoxStyleEvent
    {
        [JsonProperty("id")] public string Id { get; set; } = "";
        [JsonProperty("p")] public string P { get; set; } = "";
        public string d { get; set; } = "";
        public string D { get; set; } = "";
        public string m { get; set; } = "";
        public string M { get; set; } = "";
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Core issue: case-sensitive property round-trip via JObject
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CaseSensitiveProperties_RoundTripCorrectly_ViaJObject()
    {
        var container = new InMemoryContainer("events", "/p");

        var jObj = new JObject
        {
            ["id"] = "evt-1",
            ["p"] = "stream-1",
            ["d"] = "lowercase-data",
            ["D"] = "uppercase-data",
            ["m"] = "lowercase-meta",
            ["M"] = "uppercase-meta"
        };

        await container.CreateItemAsync(jObj, new PartitionKey("stream-1"));

        var response = await container.ReadItemAsync<JObject>("evt-1", new PartitionKey("stream-1"));
        var result = response.Resource;

        result["d"]!.ToString().Should().Be("lowercase-data");
        result["D"]!.ToString().Should().Be("uppercase-data");
        result["m"]!.ToString().Should().Be("lowercase-meta");
        result["M"]!.ToString().Should().Be("uppercase-meta");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Deserialization to strongly-typed class with case-sensitive properties
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CaseSensitiveProperties_DeserializeCorrectly_ToStrongType()
    {
        var container = new InMemoryContainer("events", "/p");

        var jObj = new JObject
        {
            ["id"] = "evt-1",
            ["p"] = "stream-1",
            ["d"] = "lowercase-data",
            ["D"] = "uppercase-data",
            ["m"] = "lowercase-meta",
            ["M"] = "uppercase-meta"
        };

        await container.CreateItemAsync(jObj, new PartitionKey("stream-1"));

        var response = await container.ReadItemAsync<EquinoxStyleEvent>(
            "evt-1", new PartitionKey("stream-1"));

        var result = response.Resource;
        result.d.Should().Be("lowercase-data");
        result.D.Should().Be("uppercase-data");
        result.m.Should().Be("lowercase-meta");
        result.M.Should().Be("uppercase-meta");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Serialization from anonymous type with case-sensitive properties
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CaseSensitiveProperties_PreservedInSerialization_FromAnonymousType()
    {
        var container = new InMemoryContainer("events", "/p");

        var item = new
        {
            id = "evt-1",
            p = "stream-1",
            d = new byte[] { 1, 2, 3 },
            D = new byte[] { 4, 5, 6 },
            m = "metadata",
            M = "Metadata"
        };

        await container.CreateItemAsync(item, new PartitionKey("stream-1"));

        var response = await container.ReadItemAsync<JObject>("evt-1", new PartitionKey("stream-1"));
        var result = response.Resource;

        // Both d and D should exist as distinct properties
        result["d"].Should().NotBeNull();
        result["D"].Should().NotBeNull();
        result["d"]!.ToString().Should().NotBe(result["D"]!.ToString());
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Upsert preserves case-sensitive properties
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CaseSensitiveProperties_PreservedAfterUpsert()
    {
        var container = new InMemoryContainer("events", "/p");

        var jObj = new JObject
        {
            ["id"] = "evt-1",
            ["p"] = "stream-1",
            ["d"] = "original-lowercase",
            ["D"] = "original-uppercase"
        };

        await container.CreateItemAsync(jObj, new PartitionKey("stream-1"));

        var upsertObj = new JObject
        {
            ["id"] = "evt-1",
            ["p"] = "stream-1",
            ["d"] = "updated-lowercase",
            ["D"] = "updated-uppercase"
        };

        await container.UpsertItemAsync(upsertObj, new PartitionKey("stream-1"));

        var response = await container.ReadItemAsync<JObject>("evt-1", new PartitionKey("stream-1"));
        var result = response.Resource;

        result["d"]!.ToString().Should().Be("updated-lowercase");
        result["D"]!.ToString().Should().Be("updated-uppercase");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Replace preserves case-sensitive properties
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CaseSensitiveProperties_PreservedAfterReplace()
    {
        var container = new InMemoryContainer("events", "/p");

        var jObj = new JObject
        {
            ["id"] = "evt-1",
            ["p"] = "stream-1",
            ["d"] = "original-lowercase",
            ["D"] = "original-uppercase"
        };

        await container.CreateItemAsync(jObj, new PartitionKey("stream-1"));

        var replaceObj = new JObject
        {
            ["id"] = "evt-1",
            ["p"] = "stream-1",
            ["d"] = "replaced-lowercase",
            ["D"] = "replaced-uppercase"
        };

        await container.ReplaceItemAsync(replaceObj, "evt-1", new PartitionKey("stream-1"));

        var response = await container.ReadItemAsync<JObject>("evt-1", new PartitionKey("stream-1"));
        var result = response.Resource;

        result["d"]!.ToString().Should().Be("replaced-lowercase");
        result["D"]!.ToString().Should().Be("replaced-uppercase");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Query returns items with case-sensitive properties intact
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CaseSensitiveProperties_PreservedInQueryResults()
    {
        var container = new InMemoryContainer("events", "/p");

        var jObj = new JObject
        {
            ["id"] = "evt-1",
            ["p"] = "stream-1",
            ["d"] = "lowercase-data",
            ["D"] = "uppercase-data"
        };

        await container.CreateItemAsync(jObj, new PartitionKey("stream-1"));

        var query = new QueryDefinition("SELECT * FROM c WHERE c.id = 'evt-1'");
        var iter = container.GetItemQueryIterator<JObject>(query);
        var results = new List<JObject>();
        while (iter.HasMoreResults)
        {
            var page = await iter.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle();
        var result = results[0];
        result["d"]!.ToString().Should().Be("lowercase-data");
        result["D"]!.ToString().Should().Be("uppercase-data");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Property casing is not modified by CamelCaseNamingStrategy
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task PropertyNames_NotTransformedByCamelCaseStrategy()
    {
        var container = new InMemoryContainer("test", "/pk");

        // A class with PascalCase properties and [JsonProperty] attributes
        // should use the [JsonProperty] names, NOT a camelCase transformation
        var item = new TestDocWithExplicitNames
        {
            Id = "1",
            Pk = "pk1",
            MyProperty = "hello",
            AnotherProp = 42
        };

        await container.CreateItemAsync(item, new PartitionKey("pk1"));

        var response = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
        var result = response.Resource;

        // [JsonProperty] names should be used as-is
        result["id"].Should().NotBeNull();
        result["pk"].Should().NotBeNull();
        result["MyProperty"]!.ToString().Should().Be("hello");
        result["AnotherProp"]!.Value<int>().Should().Be(42);

        // Verify camelCase transformation did NOT occur
        result["myProperty"].Should().BeNull();
        result["anotherProp"].Should().BeNull();
    }

    private class TestDocWithExplicitNames
    {
        [JsonProperty("id")] public string Id { get; set; } = "";
        [JsonProperty("pk")] public string Pk { get; set; } = "";
        public string MyProperty { get; set; } = "";
        public int AnotherProp { get; set; }
    }
}
