using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 1 — FeedRange Count Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class FeedRangeCountTests
{
    [Fact]
    public async Task FeedRangeCount_DefaultsToOne()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var ranges = await container.GetFeedRangesAsync();

        ranges.Should().HaveCount(1);
    }

    [Fact]
    public async Task FeedRangeCount_ReturnsConfiguredCount()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

        var ranges = await container.GetFeedRangesAsync();

        ranges.Should().HaveCount(4);
    }

    [Fact]
    public async Task FeedRanges_AreRealFeedRangeEpk_NotMocks()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 2 };

        var ranges = await container.GetFeedRangesAsync();

        foreach (var range in ranges)
        {
            var json = range.ToJsonString();
            var obj = JObject.Parse(json);
            obj["Range"].Should().NotBeNull();
            obj["Range"]!["min"].Should().NotBeNull();
            obj["Range"]!["max"].Should().NotBeNull();
        }
    }

    [Fact]
    public async Task FeedRanges_CoverFullRange()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 3 };

        var ranges = await container.GetFeedRangesAsync();

        // First range starts at ""
        var firstJson = JObject.Parse(ranges[0].ToJsonString());
        firstJson["Range"]!["min"]!.ToString().Should().Be("");

        // Last range ends at "FF"
        var lastJson = JObject.Parse(ranges[^1].ToJsonString());
        lastJson["Range"]!["max"]!.ToString().Should().Be("FF");
    }

    [Fact]
    public async Task FeedRanges_AreContiguous()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

        var ranges = await container.GetFeedRangesAsync();

        for (var i = 0; i < ranges.Count - 1; i++)
        {
            var currentMax = JObject.Parse(ranges[i].ToJsonString())["Range"]!["max"]!.ToString();
            var nextMin = JObject.Parse(ranges[i + 1].ToJsonString())["Range"]!["min"]!.ToString();
            currentMax.Should().Be(nextMin, $"range {i} max should equal range {i + 1} min");
        }
    }

    [Fact]
    public async Task FeedRangeCount_SetToOne_ReturnsSingleFullRange()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 1 };

        var ranges = await container.GetFeedRangesAsync();

        ranges.Should().HaveCount(1);
        var json = JObject.Parse(ranges[0].ToJsonString());
        json["Range"]!["min"]!.ToString().Should().Be("");
        json["Range"]!["max"]!.ToString().Should().Be("FF");
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 2 — FeedRange Query Filtering Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class FeedRangeQueryFilteringTests
{
    [Fact]
    public async Task QueryIterator_WithFeedRange_FiltersToMatchingPartitions()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

        // Seed items across different partition keys
        for (var i = 0; i < 20; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
                new PartitionKey($"pk-{i}"));

        var ranges = await container.GetFeedRangesAsync();
        var allResults = new List<TestDocument>();

        foreach (var range in ranges)
        {
            var iterator = container.GetItemQueryIterator<TestDocument>(
                range, new QueryDefinition("SELECT * FROM c"));
            while (iterator.HasMoreResults)
            {
                var page = await iterator.ReadNextAsync();
                allResults.AddRange(page);
            }
        }

        // Union of all ranges should give us all 20 items (no duplicates, no missing)
        allResults.Should().HaveCount(20);
        allResults.Select(r => r.Id).Distinct().Should().HaveCount(20);
    }

    [Fact]
    public async Task QueryIterator_WithFeedRange_ReturnsSubsetNotAll()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

        for (var i = 0; i < 100; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
                new PartitionKey($"pk-{i}"));

        var ranges = await container.GetFeedRangesAsync();
        // At least one range should return fewer than all items
        var anyScopedCorrectly = false;

        foreach (var range in ranges)
        {
            var iterator = container.GetItemQueryIterator<TestDocument>(
                range, new QueryDefinition("SELECT * FROM c"));
            var results = new List<TestDocument>();
            while (iterator.HasMoreResults)
            {
                var page = await iterator.ReadNextAsync();
                results.AddRange(page);
            }

            if (results.Count < 100)
                anyScopedCorrectly = true;
        }

        anyScopedCorrectly.Should().BeTrue("at least one FeedRange should return a subset of items");
    }

    [Fact]
    public async Task QueryStreamIterator_WithFeedRange_FiltersToMatchingPartitions()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

        for (var i = 0; i < 20; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
                new PartitionKey($"pk-{i}"));

        var ranges = await container.GetFeedRangesAsync();
        var allIds = new HashSet<string>();

        foreach (var range in ranges)
        {
            var iterator = container.GetItemQueryStreamIterator(
                range, new QueryDefinition("SELECT * FROM c"));
            while (iterator.HasMoreResults)
            {
                var response = await iterator.ReadNextAsync();
                using var reader = new StreamReader(response.Content);
                var json = await reader.ReadToEndAsync();
                var doc = JObject.Parse(json);
                foreach (var item in doc["Documents"]!)
                    allIds.Add(item["id"]!.ToString());
            }
        }

        allIds.Should().HaveCount(20);
    }

    [Fact]
    public async Task QueryIterator_WithSingleFeedRange_ReturnsAllItems()
    {
        // FeedRangeCount=1 (default) should return all items through the single range
        var container = new InMemoryContainer("test", "/partitionKey");

        for (var i = 0; i < 10; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
                new PartitionKey($"pk-{i}"));

        var ranges = await container.GetFeedRangesAsync();
        ranges.Should().HaveCount(1);

        var iterator = container.GetItemQueryIterator<TestDocument>(
            ranges[0], new QueryDefinition("SELECT * FROM c"));
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(10);
    }

    [Fact]
    public async Task QueryIterator_WithFeedRange_WhereClause_FiltersCorrectly()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 2 };

        for (var i = 0; i < 20; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
                new PartitionKey($"pk-{i}"));

        var ranges = await container.GetFeedRangesAsync();
        var allResults = new List<TestDocument>();

        foreach (var range in ranges)
        {
            var iterator = container.GetItemQueryIterator<TestDocument>(
                range, new QueryDefinition("SELECT * FROM c WHERE c.name != 'NonExistent'"));
            while (iterator.HasMoreResults)
            {
                var page = await iterator.ReadNextAsync();
                allResults.AddRange(page);
            }
        }

        // WHERE clause doesn't filter anything, union of ranges = all items
        allResults.Should().HaveCount(20);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 3 — FeedRange Change Feed Filtering Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class FeedRangeChangeFeedFilteringTests
{
    [Fact]
    public async Task ChangeFeed_Beginning_WithFeedRange_ScopesToRange()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

        for (var i = 0; i < 20; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
                new PartitionKey($"pk-{i}"));

        var ranges = await container.GetFeedRangesAsync();
        var allResults = new List<TestDocument>();

        foreach (var range in ranges)
        {
            var iterator = container.GetChangeFeedIterator<TestDocument>(
                ChangeFeedStartFrom.Beginning(range),
                ChangeFeedMode.Incremental);
            while (iterator.HasMoreResults)
            {
                var page = await iterator.ReadNextAsync();
                allResults.AddRange(page);
            }
        }

        // Union of all ranges should give us all 20 items
        allResults.Should().HaveCount(20);
        allResults.Select(r => r.Id).Distinct().Should().HaveCount(20);
    }

    [Fact]
    public async Task ChangeFeed_WithFeedRange_ReturnsSubsetNotAll()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

        for (var i = 0; i < 100; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
                new PartitionKey($"pk-{i}"));

        var ranges = await container.GetFeedRangesAsync();
        var anyScopedCorrectly = false;

        foreach (var range in ranges)
        {
            var iterator = container.GetChangeFeedIterator<TestDocument>(
                ChangeFeedStartFrom.Beginning(range),
                ChangeFeedMode.Incremental);
            var results = new List<TestDocument>();
            while (iterator.HasMoreResults)
            {
                var page = await iterator.ReadNextAsync();
                results.AddRange(page);
            }

            if (results.Count < 100)
                anyScopedCorrectly = true;
        }

        anyScopedCorrectly.Should().BeTrue("at least one FeedRange should scope to a subset");
    }

    [Fact]
    public async Task ChangeFeedStream_WithFeedRange_ScopesToRange()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

        for (var i = 0; i < 20; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
                new PartitionKey($"pk-{i}"));

        var ranges = await container.GetFeedRangesAsync();
        var allIds = new HashSet<string>();

        foreach (var range in ranges)
        {
            var iterator = container.GetChangeFeedStreamIterator(
                ChangeFeedStartFrom.Beginning(range),
                ChangeFeedMode.Incremental);
            while (iterator.HasMoreResults)
            {
                var response = await iterator.ReadNextAsync();
                using var reader = new StreamReader(response.Content);
                var json = await reader.ReadToEndAsync();
                var doc = JObject.Parse(json);
                if (doc["Documents"] is JArray docs)
                {
                    foreach (var item in docs)
                        allIds.Add(item["id"]!.ToString());
                }
            }
        }

        allIds.Should().HaveCount(20);
    }

    [Fact]
    public async Task ChangeFeed_Now_WithFeedRange_AcceptsRangeParameter()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };
        var ranges = await container.GetFeedRangesAsync();

        // Verify ChangeFeedStartFrom.Now(feedRange) is accepted without error
        foreach (var range in ranges)
        {
            var iterator = container.GetChangeFeedIterator<TestDocument>(
                ChangeFeedStartFrom.Now(range),
                ChangeFeedMode.Incremental);

            // "Now" means changes from this point forward — no items yet, so no results
            iterator.HasMoreResults.Should().BeFalse();
        }
    }

    [Fact]
    public async Task ChangeFeed_WithSingleFeedRange_ReturnsAllItems()
    {
        // FeedRangeCount=1 with Beginning(range[0]) should return everything
        var container = new InMemoryContainer("test", "/partitionKey");

        for (var i = 0; i < 10; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
                new PartitionKey($"pk-{i}"));

        var ranges = await container.GetFeedRangesAsync();
        ranges.Should().HaveCount(1);

        var iterator = container.GetChangeFeedIterator<TestDocument>(
            ChangeFeedStartFrom.Beginning(ranges[0]),
            ChangeFeedMode.Incremental);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(10);
    }
}
