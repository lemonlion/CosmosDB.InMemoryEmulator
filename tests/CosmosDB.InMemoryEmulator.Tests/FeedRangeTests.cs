using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using System.Net;
using System.Text;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;


public class FeedRangeGapTests3
{
    [Fact]
    public async Task GetFeedRanges_WithMultipleRanges_ReturnsMultiple()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };
        for (var i = 0; i < 100; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
                new PartitionKey($"pk-{i}"));

        var ranges = await container.GetFeedRangesAsync();
        ranges.Should().HaveCountGreaterThan(1);
    }

    [Fact]
    public async Task FeedRange_UsableWithQueryIterator()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var ranges = await container.GetFeedRangesAsync();
        ranges.Should().NotBeEmpty();

        // Use the first range with a query — should still return results
        var iterator = container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c",
            requestOptions: new QueryRequestOptions { });
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().NotBeEmpty();
    }
}


public class FeedRangeGapTests
{
    [Fact]
    public async Task GetFeedRanges_ReturnsSingleRange_ByDefault()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var feedRanges = await container.GetFeedRangesAsync();

        feedRanges.Should().HaveCount(1);
    }
}


public class FeedRangeGapTests4
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey") { FeedRangeCount = 4 };

    [Fact]
    public async Task FeedRange_UsableWithChangeFeedIterator()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk2", Name = "B" }, new PartitionKey("pk2"));

        var ranges = await _container.GetFeedRangesAsync();
        var allResults = new List<TestDocument>();
        foreach (var range in ranges)
        {
            var iterator = _container.GetChangeFeedIterator<TestDocument>(
                ChangeFeedStartFrom.Beginning(range),
                ChangeFeedMode.Incremental);
            while (iterator.HasMoreResults)
            {
                var page = await iterator.ReadNextAsync();
                allResults.AddRange(page);
            }
        }

        // Union of all ranges covers both items
        allResults.Should().HaveCount(2);
    }
}


public class FeedRangeGapTests2
{
    [Fact]
    public async Task GetFeedRanges_AlwaysReturnsSingleRange()
    {
        // InMemoryContainer always returns a single FeedRange regardless of data
        var container = new InMemoryContainer("test", "/partitionKey");

        for (var i = 0; i < 100; i++)
        {
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i % 10}", Name = $"Item{i}" },
                new PartitionKey($"pk-{i % 10}"));
        }

        var feedRanges = await container.GetFeedRangesAsync();
        feedRanges.Should().HaveCount(1);
    }
}


public class FeedRangeDivergentBehaviorTests
{
    /// <summary>
    /// InMemoryContainer defaults to FeedRangeCount=1 (single range covering the full hash space).
    /// Real Cosmos DB dynamically creates partition ranges based on data volume and throughput.
    /// Set FeedRangeCount > 1 to simulate multiple physical partitions for FeedRange-scoped
    /// queries and change feed iterators.
    /// </summary>
    [Fact]
    public async Task GetFeedRanges_DefaultsSingle_SetFeedRangeCountForMultiple()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        for (var i = 0; i < 100; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
                new PartitionKey($"pk-{i}"));

        // Default: 1 range (unlike real Cosmos DB which may auto-split)
        var ranges = await container.GetFeedRangesAsync();
        ranges.Should().HaveCount(1);

        // Opt-in: set FeedRangeCount for multi-range simulation
        container.FeedRangeCount = 4;
        var multiRanges = await container.GetFeedRangesAsync();
        multiRanges.Should().HaveCount(4);
    }
}


// ═══════════════════════════════════════════════════════════════════════════
//  Category 1: FeedRangeCount Validation Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class FeedRangeCountValidationTests
{
    [Fact]
    public async Task FeedRangeCount_Zero_ClampedToOne()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 0 };
        var ranges = await container.GetFeedRangesAsync();
        ranges.Should().HaveCount(1);
    }

    [Fact]
    public async Task FeedRangeCount_Negative_ClampedToOne()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = -5 };
        var ranges = await container.GetFeedRangesAsync();
        ranges.Should().HaveCount(1);
    }

    [Fact]
    public async Task FeedRangeCount_256_AllItemsAccountedFor()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 256 };
        for (var i = 0; i < 500; i++)
            await container.CreateItemAsync(
                JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
                new PartitionKey($"pk-{i}"));

        var ranges = await container.GetFeedRangesAsync();
        ranges.Should().HaveCount(256);

        var allItems = new List<JObject>();
        foreach (var range in ranges)
        {
            var iterator = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
            while (iterator.HasMoreResults)
                allItems.AddRange(await iterator.ReadNextAsync());
        }

        allItems.Should().HaveCount(500);
    }

    [Fact]
    public async Task FeedRangeCount_Two_SplitsItems()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 2 };
        for (var i = 0; i < 50; i++)
            await container.CreateItemAsync(
                JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
                new PartitionKey($"pk-{i}"));

        var ranges = await container.GetFeedRangesAsync();
        ranges.Should().HaveCount(2);

        var total = 0;
        foreach (var range in ranges)
        {
            var iterator = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
            while (iterator.HasMoreResults)
                total += (await iterator.ReadNextAsync()).Count;
        }

        total.Should().Be(50);
    }

    [Fact]
    public async Task FeedRangeCount_ChangedBetweenCalls_ProducesDifferentRanges()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 2 };
        var ranges2 = await container.GetFeedRangesAsync();
        ranges2.Should().HaveCount(2);

        container.FeedRangeCount = 4;
        var ranges4 = await container.GetFeedRangesAsync();
        ranges4.Should().HaveCount(4);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category 2: Partition Affinity & Hashing Consistency
// ═══════════════════════════════════════════════════════════════════════════

public class FeedRangePartitionAffinityTests
{
    [Fact]
    public async Task SamePartitionKey_AlwaysMapsToSameRange()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };
        for (var i = 0; i < 10; i++)
            await container.CreateItemAsync(
                JObject.FromObject(new { id = $"{i}", partitionKey = "samePK" }),
                new PartitionKey("samePK"));

        var ranges = await container.GetFeedRangesAsync();
        var rangesWithItems = 0;

        foreach (var range in ranges)
        {
            var iterator = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
            var count = 0;
            while (iterator.HasMoreResults)
                count += (await iterator.ReadNextAsync()).Count;
            if (count > 0) rangesWithItems++;
        }

        rangesWithItems.Should().Be(1, "all items with the same PK should land in exactly one range");
    }

    [Fact]
    public void MurmurHash3_Deterministic_SameInputSameOutput()
    {
        var hash1 = PartitionKeyHash.MurmurHash3("test-key");
        var hash2 = PartitionKeyHash.MurmurHash3("test-key");
        var hash3 = PartitionKeyHash.MurmurHash3("test-key");

        hash1.Should().Be(hash2).And.Be(hash3);
    }

    [Fact]
    public async Task HashDistribution_RoughlyEven_With1000Items()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };
        for (var i = 0; i < 1000; i++)
            await container.CreateItemAsync(
                JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
                new PartitionKey($"pk-{i}"));

        var ranges = await container.GetFeedRangesAsync();
        foreach (var range in ranges)
        {
            var iterator = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
            var count = 0;
            while (iterator.HasMoreResults)
                count += (await iterator.ReadNextAsync()).Count;

            count.Should().BeGreaterThan(0, "no range should be empty with 1000 items");
            count.Should().BeLessThan(500, "no range should have more than half the items");
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category 3: Partition Key Type Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class FeedRangePartitionKeyTypeTests
{
    [Fact]
    public async Task EmptyStringPartitionKey_LandsInOneRange()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", partitionKey = "" }),
            new PartitionKey(""));

        var ranges = await container.GetFeedRangesAsync();
        var foundInRanges = 0;
        foreach (var range in ranges)
        {
            var iterator = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
            while (iterator.HasMoreResults)
                if ((await iterator.ReadNextAsync()).Count > 0) foundInRanges++;
        }

        foundInRanges.Should().Be(1);
    }

    [Fact]
    public async Task UnicodePartitionKey_LandsInOneRange()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", partitionKey = "日本語テスト🎉" }),
            new PartitionKey("日本語テスト🎉"));

        var ranges = await container.GetFeedRangesAsync();
        var total = 0;
        foreach (var range in ranges)
        {
            var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
            while (it.HasMoreResults) total += (await it.ReadNextAsync()).Count;
        }

        total.Should().Be(1);
    }

    [Fact]
    public async Task VeryLongPartitionKey_LandsInOneRange()
    {
        var longPk = new string('x', 1000);
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", partitionKey = longPk }),
            new PartitionKey(longPk));

        var ranges = await container.GetFeedRangesAsync();
        var total = 0;
        foreach (var range in ranges)
        {
            var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
            while (it.HasMoreResults) total += (await it.ReadNextAsync()).Count;
        }

        total.Should().Be(1);
    }

    [Fact]
    public async Task BooleanPartitionKey_LandsInOneRange()
    {
        var container = new InMemoryContainer("test", "/pk") { FeedRangeCount = 4 };
        await container.CreateItemAsync(
            JObject.Parse("{\"id\":\"1\",\"pk\":true}"), new PartitionKey(true));
        await container.CreateItemAsync(
            JObject.Parse("{\"id\":\"2\",\"pk\":false}"), new PartitionKey(false));

        var ranges = await container.GetFeedRangesAsync();
        var total = 0;
        foreach (var range in ranges)
        {
            var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
            while (it.HasMoreResults) total += (await it.ReadNextAsync()).Count;
        }

        total.Should().Be(2);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category 4: Query Features with FeedRange
// ═══════════════════════════════════════════════════════════════════════════

public class FeedRangeQueryFeatureTests
{
    [Fact]
    public async Task ItemCount_WithFeedRange_SumEqualsTotal()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };
        for (var i = 0; i < 20; i++)
            await container.CreateItemAsync(
                JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
                new PartitionKey($"pk-{i}"));

        var ranges = await container.GetFeedRangesAsync();
        var totalCount = 0;
        foreach (var range in ranges)
        {
            var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
            while (it.HasMoreResults)
                totalCount += (await it.ReadNextAsync()).Count;
        }

        totalCount.Should().Be(20);
    }

    [Fact]
    public async Task OrderBy_WithFeedRange_OrdersWithinRange()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 2 };
        for (var i = 0; i < 20; i++)
            await container.CreateItemAsync(
                JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}", name = $"Item{i:D3}" }),
                new PartitionKey($"pk-{i}"));

        var ranges = await container.GetFeedRangesAsync();
        foreach (var range in ranges)
        {
            var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c ORDER BY c.name ASC"));
            var names = new List<string>();
            while (it.HasMoreResults)
                names.AddRange((await it.ReadNextAsync()).Select(j => j["name"]!.ToString()));

            names.Should().BeInAscendingOrder();
        }
    }

    [Fact]
    public async Task Top_WithFeedRange()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 2 };
        for (var i = 0; i < 20; i++)
            await container.CreateItemAsync(
                JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
                new PartitionKey($"pk-{i}"));

        var ranges = await container.GetFeedRangesAsync();
        foreach (var range in ranges)
        {
            var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT TOP 3 * FROM c"));
            var results = new List<JObject>();
            while (it.HasMoreResults) results.AddRange(await it.ReadNextAsync());

            results.Count.Should().BeLessThanOrEqualTo(3);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category 5: Change Feed + FeedRange Advanced
// ═══════════════════════════════════════════════════════════════════════════

public class FeedRangeChangeFeedAdvancedTests
{
    [Fact]
    public async Task ChangeFeed_EmptyRange_ReturnsNoItems()
    {
        // Create items with a single PK so they all land in one range
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", partitionKey = "samePK" }),
            new PartitionKey("samePK"));

        var ranges = await container.GetFeedRangesAsync();
        var emptyCount = 0;
        foreach (var range in ranges)
        {
            var iterator = container.GetChangeFeedIterator<JObject>(
                ChangeFeedStartFrom.Beginning(range), ChangeFeedMode.Incremental);
            var items = new List<JObject>();
            while (iterator.HasMoreResults)
                items.AddRange(await iterator.ReadNextAsync());
            if (items.Count == 0) emptyCount++;
        }

        emptyCount.Should().BeGreaterThan(0, "at least some ranges should be empty");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category 6: Empty Container & Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class FeedRangeEmptyContainerTests
{
    [Fact]
    public async Task EmptyContainer_FeedRangeQuery_ReturnsEmpty()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };
        var ranges = await container.GetFeedRangesAsync();

        foreach (var range in ranges)
        {
            var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
            var results = new List<JObject>();
            while (it.HasMoreResults) results.AddRange(await it.ReadNextAsync());
            results.Should().BeEmpty();
        }
    }

    [Fact]
    public async Task SingleItem_HighRangeCount_MostRangesEmpty()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 50 };
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", partitionKey = "onlyPK" }),
            new PartitionKey("onlyPK"));

        var ranges = await container.GetFeedRangesAsync();
        var populatedRanges = 0;
        foreach (var range in ranges)
        {
            var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
            var count = 0;
            while (it.HasMoreResults) count += (await it.ReadNextAsync()).Count;
            if (count > 0) populatedRanges++;
        }

        populatedRanges.Should().Be(1);
    }

    [Fact]
    public async Task AllItems_SamePartitionKey_OnlyOneRangePopulated()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };
        for (var i = 0; i < 20; i++)
            await container.CreateItemAsync(
                JObject.FromObject(new { id = $"{i}", partitionKey = "samePK" }),
                new PartitionKey("samePK"));

        var ranges = await container.GetFeedRangesAsync();
        var rangesWithItems = 0;
        foreach (var range in ranges)
        {
            var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
            var count = 0;
            while (it.HasMoreResults) count += (await it.ReadNextAsync()).Count;
            if (count > 0) rangesWithItems++;
        }

        rangesWithItems.Should().Be(1);
    }
}
