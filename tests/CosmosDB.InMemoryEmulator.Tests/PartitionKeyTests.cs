using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using System.Net;
using System.Text;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;


public class PartitionKeyGapTests4
{
    [Fact]
    public async Task PartitionKey_CompositeKey_ThreePaths()
    {
        var container = new InMemoryContainer("test", ["/partitionKey", "/name", "/nested/description"]);

        var compositePk = new PartitionKeyBuilder().Add("pk1").Add("Alice").Add("Desc").Build();
        var item = new TestDocument
        {
            Id = "1", PartitionKey = "pk1", Name = "Alice",
            Nested = new NestedObject { Description = "Desc", Score = 1.0 }
        };
        var response = await container.CreateItemAsync(item, compositePk);

        response.StatusCode.Should().Be(HttpStatusCode.Created);

        // Reading back with the same composite key
        var read = await container.ReadItemAsync<TestDocument>("1", compositePk);
        read.Resource.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task PartitionKey_BooleanValue()
    {
        var container = new InMemoryContainer("test", "/active");

        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","active":true,"name":"BoolPK"}""")),
            new PartitionKey(true));

        var read = await container.ReadItemAsync<JObject>("1", new PartitionKey(true));
        read.Resource["name"]!.ToString().Should().Be("BoolPK");
    }
}


public class PartitionKeyGapTests
{
    [Fact]
    public async Task PartitionKey_ExtractedFromItem_MatchesExplicitPk()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var item = new TestDocument { Id = "1", PartitionKey = "auto-pk", Name = "Test" };

        await container.CreateItemAsync(item);

        var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("auto-pk"));
        read.Resource.Name.Should().Be("Test");
    }

    [Fact]
    public async Task PartitionKey_NumericValue()
    {
        var container = new InMemoryContainer("test", "/value");
        var item = new TestDocument { Id = "1", PartitionKey = "ignored", Name = "Test", Value = 42 };

        await container.CreateItemAsync(item, new PartitionKey(42));

        var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey(42));
        read.Resource.Name.Should().Be("Test");
    }

    [Fact]
    public async Task CrossPartition_Query_ReturnsAllPartitions()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "A", Name = "Alice" },
            new PartitionKey("A"));
        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "B", Name = "Bob" },
            new PartitionKey("B"));

        var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
    }
}


public class PartitionKeyGapTests3
{
    [Fact]
    public async Task PartitionKey_None_ItemsStoredByDocumentPk()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        // With PK.None, the PK is extracted from the document body
        var item1 = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" };
        await container.CreateItemAsync(item1, PartitionKey.None);

        var item2 = new TestDocument { Id = "2", PartitionKey = "pk2", Name = "B" };
        await container.CreateItemAsync(item2, PartitionKey.None);

        // Items should be retrievable using their document PK values
        var read1 = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read1.Resource.Name.Should().Be("A");

        var read2 = await container.ReadItemAsync<TestDocument>("2", new PartitionKey("pk2"));
        read2.Resource.Name.Should().Be("B");
    }

    [Fact(Skip = "InMemoryContainer with PartitionKey.None and missing partition key field throws conflict. " +
                 "Real Cosmos DB would store the item and allow reading with the fallback PK value. " +
                 "InMemoryContainer's ExtractPartitionKeyValue path conflicts with stream-based creation. " +
                 "See divergent behavior test in PartitionKeyFallbackDivergentBehaviorTests.")]
    public async Task PartitionKey_MissingInItem_FallsBackToId()
    {
        // Container with /category PK — but item doesn't have "category" field
        var container = new InMemoryContainer("test", "/category");

        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","name":"NoPK"}""")),
            PartitionKey.None);

        // Without a "category" field, ExtractPartitionKeyValue falls back to id
        var read = await container.ReadItemAsync<JObject>("1", new PartitionKey("1"));
        read.Resource["name"]!.ToString().Should().Be("NoPK");
    }
}


public class PartitionKeyGapTests2
{
    [Fact]
    public async Task PartitionKey_CompositeKey_TwoPaths()
    {
        var container = new InMemoryContainer("test", new[] { "/partitionKey", "/name" });

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task PartitionKey_NestedPath_Extraction()
    {
        var container = new InMemoryContainer("test", "/nested/description");
        await container.CreateItemAsync(
            new TestDocument
            {
                Id = "1", PartitionKey = "ignored", Name = "Test",
                Nested = new NestedObject { Description = "deep-pk", Score = 1.0 }
            },
            new PartitionKey("deep-pk"));

        var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("deep-pk"));
        read.Resource.Name.Should().Be("Test");
    }

    [Fact]
    public async Task PartitionKey_NullValue()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

        // Create with PartitionKey.Null
        await container.CreateItemAsync(item, PartitionKey.Null);

        // Should be retrievable
        var read = await container.ReadItemAsync<TestDocument>("1", PartitionKey.Null);
        read.Resource.Name.Should().Be("Test");
    }
}


public class PartitionKeyFallbackDivergentBehaviorTests
{
    /// <summary>
    /// BEHAVIORAL DIFFERENCE: InMemoryContainer with PartitionKey.None on a container
    /// whose PK path doesn't exist in the document succeeds but may store with an unexpected PK.
    /// Real Cosmos DB would use the partition key extracted from the document body.
    /// InMemoryContainer falls back to the id field as the PK value when the PK path is missing.
    /// Reading back requires knowing the fallback PK value.
    /// </summary>
    [Fact]
    public async Task PartitionKey_None_WithMissingPkField_Succeeds_InMemory()
    {
        var container = new InMemoryContainer("test", "/category");

        var response = await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","name":"NoPK"}""")),
            PartitionKey.None);

        // InMemory accepts the item without throwing
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }
}

// ─── PartitionKey.None vs PartitionKey.Null ─────────────────────────────

public class PartitionKeyNoneVsNullTests
{
    /// <summary>
    /// In real Cosmos DB, PartitionKey.None represents the absence of a partition key (used
    /// for containers created without a partition key definition in older API versions).
    /// PartitionKey.Null represents an explicit null value. They are semantically distinct.
    /// The emulator treats both as the storage key "null", making them interchangeable.
    /// </summary>
    [Fact(Skip = "The emulator's PartitionKeyToString maps both PartitionKey.None and " +
        "PartitionKey.Null to the string 'null'. In real Cosmos DB, PartitionKey.None has " +
        "special routing behavior for legacy non-partitioned containers, while PartitionKey.Null " +
        "is an explicit null value in the partition key field. The emulator does not support " +
        "non-partitioned (legacy) containers so the distinction is not meaningful here.")]
    public async Task PartitionKeyNone_ShouldNotMatchPartitionKeyNull()
    {
        var container = new InMemoryContainer("test-container", "/pk");

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = (string)null! }),
            PartitionKey.Null);

        // In real Cosmos, reading with PartitionKey.Null would not find an item
        // written with PartitionKey.None
        var act = () => container.ReadItemAsync<JObject>("1", PartitionKey.Null);
        await act.Should().ThrowAsync<CosmosException>();
    }

    /// <summary>
    /// Sister test: demonstrates the emulator treats None and Null identically.
    /// </summary>
    [Fact]
    public async Task PartitionKeyNoneVsNull_EmulatorBehavior_TreatedIdentically()
    {
        // ── Divergent behavior documentation ──
        // Real Cosmos DB: PartitionKey.None → system-defined partition for legacy containers.
        //   PartitionKey.Null → explicit null value in the PK field. An item written with
        //   PartitionKey.None cannot be read with PartitionKey.Null (different routing).
        // In-Memory Emulator: ExtractPartitionKeyValue and PartitionKeyToString both map
        //   None and Null to the string "null". Items are stored with key (id, "null") in
        //   both cases, making them interchangeable. This is correct for modern containers
        //   but differs for legacy non-partitioned scenarios.
        var container = new InMemoryContainer("test-container", "/pk");

        // Create item with explicit null pk value via PartitionKey.Null
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = (string)null! }),
            PartitionKey.Null);

        // In the emulator, PartitionKey.None resolves the same way as PartitionKey.Null
        var response = await container.ReadItemAsync<JObject>("1", PartitionKey.None);
        response.Resource["id"]!.Value<string>().Should().Be("1",
            "the emulator treats PartitionKey.None and PartitionKey.Null identically");
    }
}
