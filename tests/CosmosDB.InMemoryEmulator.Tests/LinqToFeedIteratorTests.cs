using AwesomeAssertions;
using CosmosDB.InMemoryEmulator.ProductionExtensions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Xunit;
using System.Net;
using System.Text;
using Newtonsoft.Json.Linq;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests that verify LINQ-to-FeedIterator integration with InMemoryContainer,
/// covering both the official <c>CosmosLinqExtensions.ToFeedIterator()</c> and
/// the <c>ToFeedIteratorOverridable()</c> workaround.
/// </summary>
[Collection("FeedIteratorSetup")]
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


public class LinqGapTests2
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private async Task SeedItems()
    {
        await _container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 30, IsActive = true, Tags = ["urgent"] }, new PartitionKey("pk1"));
        await _container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, IsActive = false }, new PartitionKey("pk1"));
        await _container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk2", Name = "Charlie", Value = 10, IsActive = true, Tags = ["urgent", "important"] }, new PartitionKey("pk2"));
    }

    [Fact]
    public async Task Linq_Where_CompoundConditions()
    {
        await SeedItems();

        var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
        var results = queryable.Where(doc => doc.Value > 15 && doc.IsActive).ToList();

        results.Should().ContainSingle().Which.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task Linq_OrderBy_ThenByDescending()
    {
        await SeedItems();

        var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
        var results = queryable.OrderBy(doc => doc.IsActive).ThenByDescending(doc => doc.Value).ToList();

        // false sorts before true; within active=false, highest value first
        results[0].Name.Should().Be("Bob");
    }

    [Fact]
    public async Task Linq_Skip_Take_Pagination()
    {
        await SeedItems();

        var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
        var results = queryable.OrderBy(doc => doc.Value).Skip(1).Take(1).ToList();

        results.Should().ContainSingle().Which.Value.Should().Be(20);
    }

    [Fact]
    public async Task Linq_Count_Aggregate()
    {
        await SeedItems();

        var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
        var count = queryable.Count();

        count.Should().Be(3);
    }

    [Fact]
    public async Task Linq_FirstOrDefault()
    {
        await SeedItems();

        var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
        var result = queryable.FirstOrDefault(doc => doc.Name == "Charlie");

        result.Should().NotBeNull();
        result!.Value.Should().Be(10);
    }

    [Fact]
    public async Task Linq_Any_ExistenceCheck()
    {
        await SeedItems();

        var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
        var hasUrgent = queryable.Any(doc => doc.Tags.Contains("urgent"));

        hasUrgent.Should().BeTrue();
    }
}


/// <summary>
/// Validates that GetItemLinqQueryable can accept a continuation token and
/// CosmosLinqSerializerOptions without throwing, even though InMemoryContainer
/// ignores both parameters.
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.getitemlinqqueryable
/// </summary>
public class LinqQueryableParameterTests5
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Linq_WithContinuationToken_DoesNotThrow()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        // Should not throw when passing a continuation token
        var queryable = _container.GetItemLinqQueryable<TestDocument>(
            allowSynchronousQueryExecution: true,
            continuationToken: "some-token");

        queryable.ToList().Should().ContainSingle();
    }

    [Fact]
    public async Task Linq_WithLinqSerializerOptions_DoesNotThrow()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var options = new CosmosLinqSerializerOptions
        {
            PropertyNamingPolicy = CosmosPropertyNamingPolicy.CamelCase
        };

        // Should not throw when passing serializer options
        var queryable = _container.GetItemLinqQueryable<TestDocument>(
            allowSynchronousQueryExecution: true,
            linqSerializerOptions: options);

        queryable.ToList().Should().ContainSingle();
    }
}


public class LinqGapTests3
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private async Task SeedItems()
    {
        await _container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, Tags = ["urgent"] }, new PartitionKey("pk1"));
        await _container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 }, new PartitionKey("pk1"));
        await _container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk2", Name = "Charlie", Value = 30, Tags = ["important"] }, new PartitionKey("pk2"));
    }

    [Fact]
    public async Task Linq_WithPartitionKey_FiltersCorrectly()
    {
        await SeedItems();

        var queryable = _container.GetItemLinqQueryable<TestDocument>(true,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk1") });
        var results = queryable.ToList();

        results.Should().HaveCount(2);
        results.Should().OnlyContain(item => item.PartitionKey == "pk1");
    }

    [Fact]
    public async Task Linq_Contains_OnCollection_InStyleQuery()
    {
        await SeedItems();

        var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
        var validNames = new[] { "Alice", "Charlie" };
        var results = queryable.Where(doc => validNames.Contains(doc.Name)).ToList();

        results.Should().HaveCount(2);
    }
}


public class LinqGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Linq_Where_EqualityFilter()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
            new PartitionKey("pk1"));

        var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
        var results = queryable.Where(d => d.Name == "Alice").ToList();

        results.Should().ContainSingle().Which.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task Linq_OrderBy()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Bravo", Value = 2 },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Alpha", Value = 1 },
            new PartitionKey("pk1"));

        var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
        var results = queryable.OrderBy(d => d.Value).ToList();

        results[0].Name.Should().Be("Alpha");
        results[1].Name.Should().Be("Bravo");
    }

    [Fact]
    public async Task Linq_Select_Projection()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 42 },
            new PartitionKey("pk1"));

        var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
        var results = queryable.Select(d => new { d.Name, d.Value }).ToList();

        results.Should().ContainSingle();
        results[0].Name.Should().Be("Alice");
        results[0].Value.Should().Be(42);
    }

    [Fact]
    public async Task Linq_Count()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
            new PartitionKey("pk1"));

        var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
        var count = queryable.Count();

        count.Should().Be(2);
    }
}
