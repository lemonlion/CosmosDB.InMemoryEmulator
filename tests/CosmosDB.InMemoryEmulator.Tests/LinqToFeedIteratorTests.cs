using AwesomeAssertions;
using CosmosDB.InMemoryEmulator.ProductionExtensions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests that verify LINQ-to-FeedIterator integration with InMemoryContainer,
/// covering both the official <c>CosmosLinqExtensions.ToFeedIterator()</c> and
/// the <c>ToFeedIteratorOverridable()</c> workaround.
/// </summary>
public class LinqToFeedIteratorTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task ToFeedIterator_ThrowsWhenUsedWithInMemoryContainer()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));

        var queryable = _container.GetItemLinqQueryable<TestDocument>()
            .Where(document => document.Name == "Alice");

        var act = () => queryable.ToFeedIterator();

        act.Should().Throw<ArgumentOutOfRangeException>()
            .WithMessage("*ToFeedIterator is only supported on Cosmos LINQ query operations*");
    }

    [Fact]
    public async Task ToFeedIteratorOverridable_WorksWithInMemoryContainer()
    {
        InMemoryFeedIteratorSetup.Register();

        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemLinqQueryable<TestDocument>()
            .Where(document => document.Value > 10)
            .ToFeedIteratorOverridable();

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(2);
        results.Select(document => document.Name).Should().BeEquivalentTo(["Bob", "Charlie"]);
    }

    [Fact]
    public async Task ToFeedIteratorOverridable_WithOrderBy_ReturnsOrderedResults()
    {
        InMemoryFeedIteratorSetup.Register();

        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Charlie", Value = 30 },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemLinqQueryable<TestDocument>()
            .OrderBy(document => document.Name)
            .ToFeedIteratorOverridable();

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Alice", "Bob", "Charlie");
    }

    [Fact]
    public async Task ToFeedIteratorOverridable_WithSelect_ReturnsProjectedResults()
    {
        InMemoryFeedIteratorSetup.Register();

        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 42 },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemLinqQueryable<TestDocument>()
            .Select(document => document.Name)
            .ToFeedIteratorOverridable();

        var results = new List<string>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().ContainSingle().Which.Should().Be("Alice");
    }

    [Fact]
    public async Task ToFeedIteratorOverridable_WithNoMatchingItems_ReturnsEmpty()
    {
        InMemoryFeedIteratorSetup.Register();

        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemLinqQueryable<TestDocument>()
            .Where(document => document.Name == "Nobody")
            .ToFeedIteratorOverridable();

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().BeEmpty();
    }

    [Fact]
    public void Deregister_ClearsFactory()
    {
        InMemoryFeedIteratorSetup.Register();
        CosmosOverridableFeedIteratorExtensions.FeedIteratorFactory.Should().NotBeNull();

        InMemoryFeedIteratorSetup.Deregister();
        CosmosOverridableFeedIteratorExtensions.FeedIteratorFactory.Should().BeNull();
    }

    [Fact]
    public void Deregister_AllowsReRegister()
    {
        InMemoryFeedIteratorSetup.Register();
        InMemoryFeedIteratorSetup.Deregister();
        InMemoryFeedIteratorSetup.Register();

        CosmosOverridableFeedIteratorExtensions.FeedIteratorFactory.Should().NotBeNull();
    }

    [Fact]
    public async Task ToFeedIteratorOverridable_WithRegister_WorksWithFilter()
    {
        InMemoryFeedIteratorSetup.Register();

        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemLinqQueryable<TestDocument>()
            .Where(document => document.Value > 10)
            .ToFeedIteratorOverridable();

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().ContainSingle().Which.Name.Should().Be("Bob");
    }

    [Fact]
    public async Task ToFeedIteratorOverridable_WithRegister_OrderByWorks()
    {
        InMemoryFeedIteratorSetup.Register();

        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Charlie", Value = 30 },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemLinqQueryable<TestDocument>()
            .OrderBy(document => document.Name)
            .ToFeedIteratorOverridable();

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Alice", "Charlie");
    }
}
