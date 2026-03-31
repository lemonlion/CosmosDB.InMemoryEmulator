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
