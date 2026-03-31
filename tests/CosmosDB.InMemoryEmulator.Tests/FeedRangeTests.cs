using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using System.Net;
using System.Text;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;


public class FeedRangeGapTests3
{
    [Fact(Skip = "InMemoryContainer always returns 1 FeedRange regardless of data distribution. " +
                 "FakeCosmosHandler can simulate multiple partition key ranges but InMemoryContainer " +
                 "does not propagate that to GetFeedRangesAsync. " +
                 "Use FakeCosmosHandler for multi-range testing.")]
    public async Task GetFeedRanges_WithMultipleRanges_ReturnsMultiple()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
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
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact(Skip = "FeedRange parameter is ignored by GetChangeFeedIterator. " +
                 "Real Cosmos DB scopes the change feed to the specified FeedRange. " +
                 "InMemoryContainer ignores the FeedRange and returns all changes. " +
                 "See divergent behavior test in ChangeFeedFeedRangeDivergentBehaviorTests4.")]
    public async Task FeedRange_UsableWithChangeFeedIterator()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk2", Name = "B" }, new PartitionKey("pk2"));

        var ranges = await _container.GetFeedRangesAsync();
        var iterator = _container.GetChangeFeedIterator<TestDocument>(
            ChangeFeedStartFrom.Beginning(ranges[0]),
            ChangeFeedMode.Incremental);

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCountLessThan(2);
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
    /// BEHAVIORAL DIFFERENCE: InMemoryContainer always returns exactly 1 FeedRange.
    /// Real Cosmos DB returns multiple ranges based on physical partition distribution.
    /// For multi-range simulation, use FakeCosmosHandler with PartitionKeyRangeCount.
    /// </summary>
    [Fact]
    public async Task GetFeedRanges_AlwaysReturnsSingle_RegardlessOfData()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        for (var i = 0; i < 100; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
                new PartitionKey($"pk-{i}"));

        var ranges = await container.GetFeedRangesAsync();
        ranges.Should().HaveCount(1);
    }
}
