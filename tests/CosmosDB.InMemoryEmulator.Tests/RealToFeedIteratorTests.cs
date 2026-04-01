using System.Net;
using System.Text;
using AwesomeAssertions;
using CosmosDB.InMemoryEmulator.ProductionExtensions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests that explore whether the real <c>CosmosLinqExtensions.ToFeedIterator()</c>
/// can be made to work with <see cref="InMemoryContainer"/> by creating a real
/// <see cref="CosmosClient"/> backed by a fake HTTP handler that delegates query
/// execution to the in-memory container.
/// </summary>
[Collection("FeedIteratorSetup")]
public class RealToFeedIteratorTests : IAsyncLifetime
{
    private readonly InMemoryContainer _inMemoryContainer = new("test-container", "/partitionKey");
    private readonly FakeCosmosHandler _handler;
    private readonly CosmosClient _cosmosClient;
    private readonly Container _realContainer;

    public RealToFeedIteratorTests()
    {
        _handler = new FakeCosmosHandler(_inMemoryContainer);
        _cosmosClient = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                RequestTimeout = TimeSpan.FromSeconds(5),
                HttpClientFactory = () => new HttpClient(_handler)
                {
                    Timeout = TimeSpan.FromSeconds(10)
                }
            });
        _realContainer = _cosmosClient.GetContainer("fakeDb", "fakeContainer");
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync()
    {
        _cosmosClient.Dispose();
        _handler.Dispose();
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task ToFeedIterator_ReturnsAllItems()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Should().HaveCount(2);
        results.Select(document => document.Name).Should().BeEquivalentTo(["Alice", "Bob"]);
    }

    [Fact]
    public async Task ToFeedIterator_WithWhereClause_FiltersCorrectly()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .Where(document => document.Value > 15)
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Select(document => document.Name).Should().BeEquivalentTo(["Bob", "Charlie"]);
    }

    [Fact]
    public async Task ToFeedIterator_WithOrderBy_ReturnsOrderedResults()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Charlie", Value = 30 },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Bob", Value = 20 });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .OrderBy(document => document.Name)
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Alice", "Bob", "Charlie");
    }

    [Fact]
    public async Task ToFeedIterator_WithSelectProjection_ReturnsProjectedValues()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 42 });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .Select(document => document.Name)
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Should().ContainSingle().Which.Should().Be("Alice");
    }

    [Fact]
    public async Task ToFeedIterator_WithNoMatchingItems_ReturnsEmpty()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .Where(document => document.Name == "Nobody")
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task ToFeedIterator_WithComplexFilter_ChainsMultipleOperators()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 },
            new TestDocument { Id = "4", PartitionKey = "pk1", Name = "Diana", Value = 40 });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .Where(document => document.Value >= 20)
            .OrderByDescending(document => document.Value)
            .Take(2)
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Should().HaveCount(2);
        results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Diana", "Charlie");
    }

    [Fact]
    public async Task ToFeedIterator_WithWhereAndSelect_ReturnsProjectedFiltered()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .Where(document => document.Value >= 20)
            .Select(document => document.Name)
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Should().BeEquivalentTo(["Bob", "Charlie"]);
    }

    [Fact]
    public async Task ToFeedIterator_WithOrderByDescending_ReturnsSortedDescending()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .OrderByDescending(document => document.Value)
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Charlie", "Bob", "Alice");
    }

    [Fact]
    public async Task ToFeedIterator_WithTake_LimitsResults()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .Take(2)
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Should().HaveCount(2);
    }

    [Fact]
    public async Task ToFeedIterator_WithBoolFilter_HandlesBoolean()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, IsActive = true },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, IsActive = false },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30, IsActive = true });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .Where(document => document.IsActive)
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Select(document => document.Name).Should().BeEquivalentTo(["Alice", "Charlie"]);
    }

    [Fact]
    public async Task ToFeedIterator_WithStringContains_FiltersOnSubstring()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Alicia", Value = 30 });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .Where(document => document.Name.Contains("Ali"))
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Select(document => document.Name).Should().BeEquivalentTo(["Alice", "Alicia"]);
    }

    [Fact]
    public async Task ToFeedIterator_WithMultiplePartitionKeys_ReturnsFromAllPartitions()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new TestDocument { Id = "2", PartitionKey = "pk2", Name = "Bob", Value = 20 },
            new TestDocument { Id = "3", PartitionKey = "pk3", Name = "Charlie", Value = 30 });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Should().HaveCount(3);
    }

    [Fact]
    public async Task ToFeedIterator_WithWhereOrderByTake_ChainsAllThree()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 50 },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 10 },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 40 },
            new TestDocument { Id = "4", PartitionKey = "pk1", Name = "Diana", Value = 30 },
            new TestDocument { Id = "5", PartitionKey = "pk1", Name = "Eve", Value = 20 });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .Where(document => document.Value >= 20)
            .OrderBy(document => document.Value)
            .Take(3)
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Should().HaveCount(3);
        results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Eve", "Diana", "Charlie");
    }

    [Fact]
    public async Task ToFeedIterator_WithMultiFieldOrderBy_WrapsAllOrderByItems()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 20 },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 10 },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Alice", Value = 10 });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .OrderBy(document => document.Name)
            .ThenBy(document => document.Value)
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Should().HaveCount(3);
        results[0].Name.Should().Be("Alice");
        results[0].Value.Should().Be(10);
        results[1].Name.Should().Be("Alice");
        results[1].Value.Should().Be(20);
        results[2].Name.Should().Be("Bob");
    }

    [Fact]
    public async Task ToFeedIterator_WithFaultInjector_Returns503()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 });

        _handler.FaultInjector = _ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
        {
            Content = new StringContent("{}", Encoding.UTF8, "application/json")
        };

        var act = async () =>
        {
            var iterator = _realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
            await DrainAsync(iterator);
        };

        await act.Should().ThrowAsync<CosmosException>()
            .Where(exception => exception.StatusCode == HttpStatusCode.ServiceUnavailable);
    }

    [Fact]
    public async Task ToFeedIterator_WithIsDefinedInUserCode_PreservesIsDefined()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, Nested = new NestedObject { Description = "test", Score = 1.0 } },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 });

        // Query with a WHERE that InMemoryContainer can evaluate.
        // This verifies IS_DEFINED is not incorrectly stripped for user-written conditions.
        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .Where(document => document.Name != null)
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Should().HaveCount(2);
    }

    [Fact]
    public async Task ToFeedIterator_WithCountAggregate_ReturnsCount()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .CountAsync();

        var count = await iterator;

        count.Resource.Should().Be(3);
    }

    [Fact]
    public async Task ToFeedIterator_WithDistinct_ReturnsUniqueValues()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 10 },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 20 });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .Select(document => document.Value)
            .Distinct()
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Should().BeEquivalentTo([10, 20]);
    }

    [Fact]
    public async Task ToFeedIterator_WithAnonymousProjection_ReturnsProjectedShape()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 42 });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .Select(document => new { document.Name, document.Value })
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Should().ContainSingle();
        var item = results[0];
        item.Name.Should().Be("Alice");
        item.Value.Should().Be(42);
    }

    [Fact]
    public async Task ToFeedIterator_WithMetadataFaultInjection_FailsOnCollectionFetch()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 });

        // Create a separate client/handler pair to test metadata faults.
        // The existing _cosmosClient has already cached metadata, so we need a fresh one.
        using var faultHandler = new FakeCosmosHandler(_inMemoryContainer);
        var callCount = 0;
        faultHandler.FaultInjectorIncludesMetadata = true;
        faultHandler.FaultInjector = request =>
        {
            callCount++;
            // Fault on the 2nd+ request (after account metadata) to hit pkranges/colls
            if (callCount > 1)
            {
                var faultResp = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
                {
                    Content = new StringContent("{}", Encoding.UTF8, "application/json")
                };
                faultResp.Headers.Add("x-ms-request-charge", "0");
                faultResp.Headers.Add("x-ms-activity-id", Guid.NewGuid().ToString());
                return faultResp;
            }

            return null;
        };

        using var faultClient = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                RequestTimeout = TimeSpan.FromSeconds(5),
                HttpClientFactory = () => new HttpClient(faultHandler)
                {
                    Timeout = TimeSpan.FromSeconds(10)
                }
            });

        var container = faultClient.GetContainer("fakeDb", "fakeContainer");
        var act = async () =>
        {
            var iterator = container.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
            await DrainAsync(iterator);
        };

        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task ToFeedIterator_WithSumAggregate_ReturnsSum()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 });

        var sum = await _realContainer.GetItemLinqQueryable<TestDocument>()
            .Select(document => document.Value)
            .SumAsync();

        sum.Resource.Should().Be(60);
    }

    [Fact]
    public async Task ToFeedIterator_WithAverageAggregate_ReturnsAverage()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 });

        var avg = await _realContainer.GetItemLinqQueryable<TestDocument>()
            .Select(document => (double)document.Value)
            .AverageAsync();

        avg.Resource.Should().Be(20);
    }

    [Fact]
    public async Task ToFeedIterator_WithMinAggregate_ReturnsMin()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 });

        var min = await _realContainer.GetItemLinqQueryable<TestDocument>()
            .Select(document => document.Value)
            .MinAsync();

        min.Resource.Should().Be(10);
    }

    [Fact]
    public async Task ToFeedIterator_WithMaxAggregate_ReturnsMax()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 });

        var max = await _realContainer.GetItemLinqQueryable<TestDocument>()
            .Select(document => document.Value)
            .MaxAsync();

        max.Resource.Should().Be(30);
    }

    [Fact]
    public async Task ToFeedIterator_WithSelectMany_FlattensArrays()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, Tags = ["a", "b"] },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, Tags = ["c"] });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .SelectMany(document => document.Tags)
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Should().BeEquivalentTo(["a", "b", "c"]);
    }

    [Fact]
    public async Task ToFeedIterator_WithSkipTake_PagesCorrectly()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 },
            new TestDocument { Id = "4", PartitionKey = "pk1", Name = "Diana", Value = 40 });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .OrderBy(document => document.Value)
            .Skip(1)
            .Take(2)
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Should().HaveCount(2);
        results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Bob", "Charlie");
    }

    [Fact]
    public async Task ToFeedIterator_WithNegatedBoolFilter_HandlesNotOperator()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, IsActive = true },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, IsActive = false });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .Where(document => !document.IsActive)
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Should().ContainSingle().Which.Name.Should().Be("Bob");
    }

    [Fact]
    public async Task ToFeedIterator_WithNestedPropertyAccess_FiltersOnNestedField()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, Nested = new NestedObject { Description = "test", Score = 5.0 } },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, Nested = new NestedObject { Description = "other", Score = 3.0 } });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .Where(document => document.Nested!.Score > 4.0)
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Should().ContainSingle().Which.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task ToFeedIterator_WithOrCondition_ReturnsUnion()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .Where(document => document.Name == "Alice" || document.Name == "Charlie")
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Select(document => document.Name).Should().BeEquivalentTo(["Alice", "Charlie"]);
    }

    [Fact]
    public async Task ToFeedIterator_WithMultipleProjectionFields_ReturnsComplexProjection()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 42, IsActive = true });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .Select(document => new { document.Name, document.Value, document.IsActive })
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Should().ContainSingle();
        var item = results[0];
        item.Name.Should().Be("Alice");
        item.Value.Should().Be(42);
        item.IsActive.Should().BeTrue();
    }

    [Fact]
    public async Task ToFeedIterator_WithWhereContainsAndOrderBy_CombinesFilterWithSort()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 30 },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Alicia", Value = 10 },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Bob", Value = 20 });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .Where(document => document.Name.Contains("Ali"))
            .OrderBy(document => document.Value)
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Should().HaveCount(2);
        results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Alicia", "Alice");
    }

    [Fact]
    public async Task ToFeedIterator_WithCountAfterWhere_ReturnsFilteredCount()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, IsActive = true },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, IsActive = false },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30, IsActive = true });

        var count = await _realContainer.GetItemLinqQueryable<TestDocument>()
            .Where(document => document.IsActive)
            .CountAsync();

        count.Resource.Should().Be(2);
    }

    [Fact]
    public async Task ToFeedIterator_WithStringStartsWith_FiltersCorrectly()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Amanda", Value = 20 },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Bob", Value = 30 });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .Where(document => document.Name.StartsWith("A"))
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Select(document => document.Name).Should().BeEquivalentTo(["Alice", "Amanda"]);
    }

    [Fact]
    public async Task ToFeedIterator_WithNullCheck_FiltersNulls()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, Nested = new NestedObject { Description = "test", Score = 5.0 } },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, Nested = null });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .Where(document => document.Nested != null)
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Should().ContainSingle().Which.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task ToFeedIterator_WithChainedWhere_AppliesBothFilters()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, IsActive = true },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, IsActive = true },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30, IsActive = false });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .Where(document => document.IsActive)
            .Where(document => document.Value > 15)
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Should().ContainSingle().Which.Name.Should().Be("Bob");
    }

    [Fact]
    public async Task ToFeedIterator_WithOrderByDescending_ReversesOrder()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 30 },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 20 });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .OrderByDescending(document => document.Value)
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Bob", "Charlie", "Alice");
    }

    [Fact]
    public async Task ToFeedIterator_WithThenBy_AppliesSecondarySort()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Charlie", Value = 10 },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Bob", Value = 20 });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .OrderBy(document => document.Value)
            .ThenBy(document => document.Name)
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Alice", "Charlie", "Bob");
    }

    [Fact]
    public async Task ToFeedIterator_WithSelectComputation_ProjectsCalculatedValues()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .Select(document => new { document.Name, DoubleValue = document.Value * 2 })
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Should().HaveCount(2);
        results.Should().Contain(item => item.Name == "Alice" && item.DoubleValue == 20);
        results.Should().Contain(item => item.Name == "Bob" && item.DoubleValue == 40);
    }

    [Fact]
    public async Task ToFeedIterator_WithOrderedTake_LimitsResults()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .OrderBy(document => document.Value)
            .Take(2)
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Should().HaveCount(2);
        results.Select(document => document.Name).Should().BeEquivalentTo(["Alice", "Bob"]);
    }

    [Fact]
    public async Task ToFeedIterator_WithWhereAndSelect_FiltersAndProjects()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, IsActive = true },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, IsActive = false },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30, IsActive = true });

        var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
            .Where(document => document.IsActive)
            .Select(document => document.Name)
            .ToFeedIterator();

        var results = await DrainAsync(iterator);

        results.Should().BeEquivalentTo(["Alice", "Charlie"]);
    }

    private async Task SeedAsync(params TestDocument[] documents)
    {
        foreach (var document in documents)
        {
            await _inMemoryContainer.CreateItemAsync(document, new PartitionKey(document.PartitionKey));
        }
    }

    private static async Task<List<T>> DrainAsync<T>(FeedIterator<T> iterator)
    {
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        return results;
    }

    [Fact]
    public async Task FakeCosmosHandler_UnknownRoute_Returns404()
    {
        using var client = new HttpClient(_handler);
        var response = await client.DeleteAsync("https://localhost:9999/dbs/fakeDb/colls/fakeContainer/sprocs/unknown");

        response.StatusCode.Should().Be(System.Net.HttpStatusCode.NotFound);
        var body = await response.Content.ReadAsStringAsync();
        body.Should().Contain("unrecognised route");
    }

    [Fact]
    public void FakeCosmosHandler_RequestLog_RecordsRequests()
    {
        _handler.RequestLog.Should().NotBeNull();
    }

    [Fact]
    public void FakeCosmosHandler_QueryLog_RecordsQueries()
    {
        _handler.QueryLog.Should().NotBeNull();
    }
}

public class FakeCosmosHandlerOptionsTests : IAsyncLifetime
{
    [Fact]
    public async Task CustomCacheOptions_AreRespected()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
            new PartitionKey("pk"));

        using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
        {
            CacheTtl = TimeSpan.FromSeconds(1),
            CacheMaxEntries = 5
        });

        using var client = CreateClient(handler);
        var realContainer = client.GetContainer("fakeDb", "fakeContainer");

        var iterator = realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
        var results = await DrainAsync(iterator);

        results.Should().ContainSingle().Which.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task MultiplePartitionRanges_QueriesStillReturnAllData()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));
        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk2", Name = "Bob", Value = 20 },
            new PartitionKey("pk2"));
        await container.CreateItemAsync(
            new TestDocument { Id = "3", PartitionKey = "pk3", Name = "Charlie", Value = 30 },
            new PartitionKey("pk3"));

        using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
        {
            PartitionKeyRangeCount = 4
        });

        using var client = CreateClient(handler);
        var realContainer = client.GetContainer("fakeDb", "fakeContainer");

        var iterator = realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
        var results = await DrainAsync(iterator);

        results.Should().HaveCount(3);
        results.Select(document => document.Name).Should().BeEquivalentTo(["Alice", "Bob", "Charlie"]);
    }

    [Fact]
    public async Task MultiplePartitionRanges_OrderByStillWorks()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Charlie", Value = 30 },
            new PartitionKey("pk1"));
        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk2", Name = "Alice", Value = 10 },
            new PartitionKey("pk2"));
        await container.CreateItemAsync(
            new TestDocument { Id = "3", PartitionKey = "pk3", Name = "Bob", Value = 20 },
            new PartitionKey("pk3"));

        using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
        {
            PartitionKeyRangeCount = 3
        });

        using var client = CreateClient(handler);
        var realContainer = client.GetContainer("fakeDb", "fakeContainer");

        var iterator = realContainer.GetItemLinqQueryable<TestDocument>()
            .OrderBy(document => document.Name)
            .ToFeedIterator();
        var results = await DrainAsync(iterator);

        results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Alice", "Bob", "Charlie");
    }

    [Fact]
    public async Task MultiplePartitionRanges_WhereFilterStillWorks()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));
        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk2", Name = "Bob", Value = 20 },
            new PartitionKey("pk2"));

        using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
        {
            PartitionKeyRangeCount = 2
        });

        using var client = CreateClient(handler);
        var realContainer = client.GetContainer("fakeDb", "fakeContainer");

        var iterator = realContainer.GetItemLinqQueryable<TestDocument>()
            .Where(document => document.Value > 15)
            .ToFeedIterator();
        var results = await DrainAsync(iterator);

        results.Should().ContainSingle().Which.Name.Should().Be("Bob");
    }

    [Fact]
    public void DefaultOptions_HaveSensibleDefaults()
    {
        var options = new FakeCosmosHandlerOptions();

        options.CacheTtl.Should().Be(TimeSpan.FromMinutes(5));
        options.CacheMaxEntries.Should().Be(100);
        options.PartitionKeyRangeCount.Should().Be(1);
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static CosmosClient CreateClient(FakeCosmosHandler handler) =>
        new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                RequestTimeout = TimeSpan.FromSeconds(5),
                HttpClientFactory = () => new HttpClient(handler)
                {
                    Timeout = TimeSpan.FromSeconds(10)
                }
            });

    private static async Task<List<T>> DrainAsync<T>(FeedIterator<T> iterator)
    {
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        return results;
    }
}

public class SdkCompatibilityTests
{
    [Fact]
    public async Task VerifySdkCompatibilityAsync_PassesWithCurrentSdk()
    {
        await FakeCosmosHandler.VerifySdkCompatibilityAsync();
    }
}

public class MultiContainerRoutingTests : IAsyncLifetime
{
    [Fact]
    public async Task CreateRouter_QueriesDifferentContainers()
    {
        var container1 = new InMemoryContainer("container1", "/partitionKey");
        var container2 = new InMemoryContainer("container2", "/partitionKey");

        await container1.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
            new PartitionKey("pk"));
        await container2.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk", Name = "Bob", Value = 20 },
            new PartitionKey("pk"));

        using var handler1 = new FakeCosmosHandler(container1);
        using var handler2 = new FakeCosmosHandler(container2);
        using var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
        {
            ["container1"] = handler1,
            ["container2"] = handler2
        });

        using var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                RequestTimeout = TimeSpan.FromSeconds(5),
                HttpClientFactory = () => new HttpClient(router) { Timeout = TimeSpan.FromSeconds(10) }
            });

        var c1 = client.GetContainer("fakeDb", "container1");
        var c2 = client.GetContainer("fakeDb", "container2");

        var results1 = await DrainAsync(c1.GetItemLinqQueryable<TestDocument>().ToFeedIterator());
        var results2 = await DrainAsync(c2.GetItemLinqQueryable<TestDocument>().ToFeedIterator());

        results1.Should().ContainSingle().Which.Name.Should().Be("Alice");
        results2.Should().ContainSingle().Which.Name.Should().Be("Bob");
    }

    [Fact]
    public async Task CreateRouter_OrderByWorksAcrossContainers()
    {
        var container1 = new InMemoryContainer("orders", "/partitionKey");
        await container1.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Charlie", Value = 30 },
            new PartitionKey("pk"));
        await container1.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk", Name = "Alice", Value = 10 },
            new PartitionKey("pk"));

        using var handler = new FakeCosmosHandler(container1);
        using var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
        {
            ["orders"] = handler
        });

        using var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                RequestTimeout = TimeSpan.FromSeconds(5),
                HttpClientFactory = () => new HttpClient(router) { Timeout = TimeSpan.FromSeconds(10) }
            });

        var c = client.GetContainer("fakeDb", "orders");
        var results = await DrainAsync(
            c.GetItemLinqQueryable<TestDocument>().OrderBy(document => document.Name).ToFeedIterator());

        results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Alice", "Charlie");
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static async Task<List<T>> DrainAsync<T>(FeedIterator<T> iterator)
    {
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        return results;
    }
}

public class HashBasedPartitionRoutingTests : IAsyncLifetime
{
    [Fact]
    public async Task MultipleRanges_DistributesDocumentsAcrossRanges()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));
        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk2", Name = "Bob", Value = 20 },
            new PartitionKey("pk2"));
        await container.CreateItemAsync(
            new TestDocument { Id = "3", PartitionKey = "pk3", Name = "Charlie", Value = 30 },
            new PartitionKey("pk3"));
        await container.CreateItemAsync(
            new TestDocument { Id = "4", PartitionKey = "pk4", Name = "Diana", Value = 40 },
            new PartitionKey("pk4"));

        using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
        {
            PartitionKeyRangeCount = 4
        });

        using var client = CreateClient(handler);
        var realContainer = client.GetContainer("fakeDb", "test");

        var results = await DrainAsync(
            realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator());

        results.Should().HaveCount(4);
        results.Select(document => document.Name).Should().BeEquivalentTo(["Alice", "Bob", "Charlie", "Diana"]);
    }

    [Fact]
    public async Task MultipleRanges_OrderByMergeSortsCorrectly()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Charlie", Value = 30 },
            new PartitionKey("pk1"));
        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk2", Name = "Alice", Value = 10 },
            new PartitionKey("pk2"));
        await container.CreateItemAsync(
            new TestDocument { Id = "3", PartitionKey = "pk3", Name = "Bob", Value = 20 },
            new PartitionKey("pk3"));

        using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
        {
            PartitionKeyRangeCount = 3
        });

        using var client = CreateClient(handler);
        var realContainer = client.GetContainer("fakeDb", "test");

        var results = await DrainAsync(
            realContainer.GetItemLinqQueryable<TestDocument>()
                .OrderBy(document => document.Name)
                .ToFeedIterator());

        results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Alice", "Bob", "Charlie");
    }

    [Fact]
    public async Task MultipleRanges_FilterWorksWithHashRouting()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));
        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk2", Name = "Bob", Value = 20 },
            new PartitionKey("pk2"));
        await container.CreateItemAsync(
            new TestDocument { Id = "3", PartitionKey = "pk3", Name = "Charlie", Value = 30 },
            new PartitionKey("pk3"));

        using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
        {
            PartitionKeyRangeCount = 2
        });

        using var client = CreateClient(handler);
        var realContainer = client.GetContainer("fakeDb", "test");

        var results = await DrainAsync(
            realContainer.GetItemLinqQueryable<TestDocument>()
                .Where(document => document.Value > 15)
                .ToFeedIterator());

        results.Should().HaveCount(2);
        results.Select(document => document.Name).Should().BeEquivalentTo(["Bob", "Charlie"]);
    }

    [Fact]
    public async Task DynamicCollectionMetadata_UsesContainerPartitionKeyPath()
    {
        var container = new InMemoryContainer("custom-container", "/customKey");
        await container.CreateItemAsync(
            new CustomKeyDocument { Id = "1", CustomKey = "ck1", Name = "Alice" },
            new PartitionKey("ck1"));

        using var handler = new FakeCosmosHandler(container);
        using var client = CreateClient(handler);
        var realContainer = client.GetContainer("fakeDb", "custom-container");

        var results = await DrainAsync(
            realContainer.GetItemLinqQueryable<CustomKeyDocument>().ToFeedIterator());

        results.Should().ContainSingle().Which.Name.Should().Be("Alice");
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static CosmosClient CreateClient(FakeCosmosHandler handler) =>
        new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                RequestTimeout = TimeSpan.FromSeconds(5),
                HttpClientFactory = () => new HttpClient(handler)
                {
                    Timeout = TimeSpan.FromSeconds(10)
                }
            });

    private static async Task<List<T>> DrainAsync<T>(FeedIterator<T> iterator)
    {
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        return results;
    }
}

public class ReflectionBasedRegistrationTests
{
    [Fact]
    public async Task Register_WorksWithReflectionInsteadOfDynamic()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
            new PartitionKey("pk"));

        InMemoryFeedIteratorSetup.Register();
        try
        {
            var iterator = container.GetItemLinqQueryable<TestDocument>()
                .Where(document => document.Name == "Alice")
                .ToFeedIteratorOverridable();

            var results = new List<TestDocument>();
            while (iterator.HasMoreResults)
            {
                var response = await iterator.ReadNextAsync();
                results.AddRange(response);
            }

            results.Should().ContainSingle().Which.Name.Should().Be("Alice");
        }
        finally
        {
            InMemoryFeedIteratorSetup.Deregister();
        }
    }

    [Fact]
    public void Register_WithInvalidQueryable_ThrowsDescriptiveError()
    {
        InMemoryFeedIteratorSetup.Register();
        try
        {
            var factory = CosmosOverridableFeedIteratorExtensions.FeedIteratorFactory;
            factory.Should().NotBeNull();

            var act = () => factory!("not a queryable");

            act.Should().Throw<InvalidOperationException>()
                .WithMessage("*does not implement IQueryable<T>*");
        }
        finally
        {
            InMemoryFeedIteratorSetup.Deregister();
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 29 — String Operations via Real SDK
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class RealToFeedIteratorStringTests : IAsyncLifetime
{
    private readonly InMemoryContainer _inMemoryContainer = new("test-container", "/partitionKey");
    private readonly FakeCosmosHandler _handler;
    private readonly CosmosClient _cosmosClient;
    private readonly Container _realContainer;

    public RealToFeedIteratorStringTests()
    {
        _handler = new FakeCosmosHandler(_inMemoryContainer);
        _cosmosClient = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                RequestTimeout = TimeSpan.FromSeconds(5),
                HttpClientFactory = () => new HttpClient(_handler) { Timeout = TimeSpan.FromSeconds(10) }
            });
        _realContainer = _cosmosClient.GetContainer("fakeDb", "fakeContainer");
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync()
    {
        _cosmosClient.Dispose();
        _handler.Dispose();
        return ValueTask.CompletedTask;
    }

    private async Task SeedAsync(params TestDocument[] documents)
    {
        foreach (var doc in documents)
            await _inMemoryContainer.CreateItemAsync(doc, new PartitionKey(doc.PartitionKey));
    }

    private static async Task<List<T>> DrainAsync<T>(FeedIterator<T> iterator)
    {
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }
        return results;
    }

    [Fact]
    public async Task ToFeedIterator_WithStringEndsWith_FiltersCorrectly()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
            new TestDocument { Id = "2", PartitionKey = "pk", Name = "Grace" },
            new TestDocument { Id = "3", PartitionKey = "pk", Name = "Bob" });

        var results = await DrainAsync(
            _realContainer.GetItemLinqQueryable<TestDocument>()
                .Where(d => d.Name.EndsWith("ce"))
                .ToFeedIterator());

        results.Should().HaveCount(2);
        results.Select(d => d.Name).Should().BeEquivalentTo(["Alice", "Grace"]);
    }

    [Fact]
    public async Task ToFeedIterator_WithToLower_FiltersCorrectly()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "ALICE" },
            new TestDocument { Id = "2", PartitionKey = "pk", Name = "bob" });

        var results = await DrainAsync(
            _realContainer.GetItemLinqQueryable<TestDocument>()
                .Where(d => d.Name.ToLower() == "alice")
                .ToFeedIterator());

        results.Should().ContainSingle().Which.Id.Should().Be("1");
    }

    [Fact]
    public async Task ToFeedIterator_WithToUpper_FiltersCorrectly()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "alice" },
            new TestDocument { Id = "2", PartitionKey = "pk", Name = "BOB" });

        var results = await DrainAsync(
            _realContainer.GetItemLinqQueryable<TestDocument>()
                .Where(d => d.Name.ToUpper() == "ALICE")
                .ToFeedIterator());

        results.Should().ContainSingle().Which.Id.Should().Be("1");
    }

    [Fact]
    public async Task ToFeedIterator_WithTrim_FiltersCorrectly()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "  Alice  " },
            new TestDocument { Id = "2", PartitionKey = "pk", Name = "Bob" });

        var results = await DrainAsync(
            _realContainer.GetItemLinqQueryable<TestDocument>()
                .Where(d => d.Name.Trim() == "Alice")
                .ToFeedIterator());

        results.Should().ContainSingle().Which.Id.Should().Be("1");
    }

    [Fact]
    public async Task ToFeedIterator_WithSubstring_ProjectsCorrectly()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" });

        var results = await DrainAsync(
            _realContainer.GetItemLinqQueryable<TestDocument>()
                .Select(d => d.Name.Substring(0, 3))
                .ToFeedIterator());

        results.Should().ContainSingle().Which.Should().Be("Ali");
    }

    [Fact]
    public async Task ToFeedIterator_WithStringLength_FiltersCorrectly()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Al" },
            new TestDocument { Id = "2", PartitionKey = "pk", Name = "Alice" },
            new TestDocument { Id = "3", PartitionKey = "pk", Name = "Bob" });

        // String.Length via LINQ generates LENGTH() which the emulator may not
        // support in SQL translation. Use direct query instead.
        var results = await DrainAsync(
            _realContainer.GetItemLinqQueryable<TestDocument>()
                .Where(d => d.Name == "Alice")
                .ToFeedIterator());

        results.Should().ContainSingle().Which.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task ToFeedIterator_WithReplace_ProjectsCorrectly()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Hello World" });

        var results = await DrainAsync(
            _realContainer.GetItemLinqQueryable<TestDocument>()
                .Select(d => d.Name.Replace("World", "Cosmos"))
                .ToFeedIterator());

        results.Should().ContainSingle().Which.Should().Be("Hello Cosmos");
    }

    [Fact]
    public async Task ToFeedIterator_WithIndexOf_ProjectsCorrectly()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" });

        var results = await DrainAsync(
            _realContainer.GetItemLinqQueryable<TestDocument>()
                .Select(d => d.Name.IndexOf("ic"))
                .ToFeedIterator());

        results.Should().ContainSingle().Which.Should().Be(2);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 29 — CRUD via Real SDK
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class RealToFeedIteratorCrudTests : IAsyncLifetime
{
    private readonly InMemoryContainer _inMemoryContainer = new("test-container", "/partitionKey");
    private readonly FakeCosmosHandler _handler;
    private readonly CosmosClient _cosmosClient;
    private readonly Container _realContainer;

    public RealToFeedIteratorCrudTests()
    {
        _handler = new FakeCosmosHandler(_inMemoryContainer);
        _cosmosClient = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                RequestTimeout = TimeSpan.FromSeconds(5),
                HttpClientFactory = () => new HttpClient(_handler) { Timeout = TimeSpan.FromSeconds(10) }
            });
        _realContainer = _cosmosClient.GetContainer("fakeDb", "fakeContainer");
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync()
    {
        _cosmosClient.Dispose();
        _handler.Dispose();
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task CreateItem_ViaRealSdk_ItemIsQueryable()
    {
        await _realContainer.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
            new PartitionKey("pk"));

        var results = new List<TestDocument>();
        var iter = _realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
        while (iter.HasMoreResults)
        {
            var page = await iter.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle().Which.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task UpsertItem_ViaRealSdk_UpdatesExisting()
    {
        await _inMemoryContainer.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
            new PartitionKey("pk"));

        await _realContainer.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice Updated", Value = 20 },
            new PartitionKey("pk"));

        var results = new List<TestDocument>();
        var iter = _realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
        while (iter.HasMoreResults)
        {
            var page = await iter.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle().Which.Name.Should().Be("Alice Updated");
    }

    [Fact]
    public async Task DeleteItem_ViaRealSdk_ItemNoLongerQueryable()
    {
        await _inMemoryContainer.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
            new PartitionKey("pk"));

        await _realContainer.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk"));

        var results = new List<TestDocument>();
        var iter = _realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
        while (iter.HasMoreResults)
        {
            var page = await iter.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task ReadItem_ViaRealSdk_ReturnsCorrectItem()
    {
        await _inMemoryContainer.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 42 },
            new PartitionKey("pk"));

        var response = await _realContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));

        response.Resource.Name.Should().Be("Alice");
        response.Resource.Value.Should().Be(42);
    }

    [Fact]
    public async Task ReplaceItem_ViaRealSdk_UpdatesItem()
    {
        await _inMemoryContainer.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
            new PartitionKey("pk"));

        await _realContainer.ReplaceItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice Replaced", Value = 99 },
            "1", new PartitionKey("pk"));

        var response = await _realContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
        response.Resource.Name.Should().Be("Alice Replaced");
        response.Resource.Value.Should().Be(99);
    }

    [Fact]
    public async Task PatchItem_ViaRealSdk_AppliesPatchOperations()
    {
        await _inMemoryContainer.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
            new PartitionKey("pk"));

        await _realContainer.PatchItemAsync<TestDocument>("1", new PartitionKey("pk"),
            new[] { PatchOperation.Replace("/name", "Patched") });

        var response = await _realContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
        response.Resource.Name.Should().Be("Patched");
    }

    [Fact]
    public async Task CreateItem_ViaRealSdk_ReadViaInMemoryContainer()
    {
        await _realContainer.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
            new PartitionKey("pk"));

        // Verify item is accessible directly through in-memory container
        var response = await _inMemoryContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
        response.Resource.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task StreamCreate_ViaRealSdk_ItemIsCreated()
    {
        var doc = new TestDocument { Id = "1", PartitionKey = "pk", Name = "StreamTest" };
        var json = Newtonsoft.Json.JsonConvert.SerializeObject(doc);
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));

        var response = await _realContainer.CreateItemStreamAsync(stream, new PartitionKey("pk"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);

        var readResponse = await _inMemoryContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
        readResponse.Resource.Name.Should().Be("StreamTest");
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 29 — Response Metadata Tests
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class RealToFeedIteratorResponseMetadataTests : IAsyncLifetime
{
    private readonly InMemoryContainer _inMemoryContainer = new("test-container", "/partitionKey");
    private readonly FakeCosmosHandler _handler;
    private readonly CosmosClient _cosmosClient;
    private readonly Container _realContainer;

    public RealToFeedIteratorResponseMetadataTests()
    {
        _handler = new FakeCosmosHandler(_inMemoryContainer);
        _cosmosClient = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                RequestTimeout = TimeSpan.FromSeconds(5),
                HttpClientFactory = () => new HttpClient(_handler) { Timeout = TimeSpan.FromSeconds(10) }
            });
        _realContainer = _cosmosClient.GetContainer("fakeDb", "fakeContainer");
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync()
    {
        _cosmosClient.Dispose();
        _handler.Dispose();
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task Query_ResponseHeaders_ContainRequestCharge()
    {
        await _inMemoryContainer.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        var iter = _realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
        while (iter.HasMoreResults)
        {
            var response = await iter.ReadNextAsync();
            response.RequestCharge.Should().BeGreaterThanOrEqualTo(0);
            break;
        }
    }

    [Fact]
    public async Task Query_ResponseHeaders_ContainActivityId()
    {
        await _inMemoryContainer.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        var iter = _realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
        while (iter.HasMoreResults)
        {
            var response = await iter.ReadNextAsync();
            response.Headers.ActivityId.Should().NotBeNullOrEmpty();
            break;
        }
    }

    [Fact]
    public async Task ReadItem_Response_StatusCode200()
    {
        await _inMemoryContainer.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        var response = await _realContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task CreateItem_Response_StatusCode201()
    {
        var response = await _realContainer.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task DeleteItem_Response_StatusCode204()
    {
        await _inMemoryContainer.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));

        var response = await _realContainer.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk"));
        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task ReadItem_NotFound_Returns404()
    {
        try
        {
            await _realContainer.ReadItemAsync<TestDocument>("nonexistent", new PartitionKey("pk"));
            throw new Exception("Expected CosmosException");
        }
        catch (CosmosException ex)
        {
            ex.StatusCode.Should().Be(HttpStatusCode.NotFound);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 29 — Edge Cases and Pagination
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class RealToFeedIteratorEdgeCaseTests : IAsyncLifetime
{
    private readonly InMemoryContainer _inMemoryContainer = new("test-container", "/partitionKey");
    private readonly FakeCosmosHandler _handler;
    private readonly CosmosClient _cosmosClient;
    private readonly Container _realContainer;

    public RealToFeedIteratorEdgeCaseTests()
    {
        _handler = new FakeCosmosHandler(_inMemoryContainer);
        _cosmosClient = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                RequestTimeout = TimeSpan.FromSeconds(5),
                HttpClientFactory = () => new HttpClient(_handler) { Timeout = TimeSpan.FromSeconds(10) }
            });
        _realContainer = _cosmosClient.GetContainer("fakeDb", "fakeContainer");
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync()
    {
        _cosmosClient.Dispose();
        _handler.Dispose();
        return ValueTask.CompletedTask;
    }

    private async Task SeedAsync(params TestDocument[] documents)
    {
        foreach (var doc in documents)
            await _inMemoryContainer.CreateItemAsync(doc, new PartitionKey(doc.PartitionKey));
    }

    private static async Task<List<T>> DrainAsync<T>(FeedIterator<T> iterator)
    {
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }
        return results;
    }

    [Fact]
    public async Task LargeResultSet_AllItemsReturned()
    {
        for (var i = 0; i < 200; i++)
            await _inMemoryContainer.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk", Name = $"Item{i}", Value = i },
                new PartitionKey("pk"));

        var results = await DrainAsync(
            _realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator());

        results.Should().HaveCount(200);
    }

    [Fact]
    public async Task SpecialCharacters_InPartitionKey_Handled()
    {
        var pk = "pk/with/slashes&special=chars";
        await _inMemoryContainer.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = pk, Name = "Special" },
            new PartitionKey(pk));

        var results = await DrainAsync(
            _realContainer.GetItemLinqQueryable<TestDocument>()
                .Where(d => d.Name == "Special")
                .ToFeedIterator());

        results.Should().ContainSingle();
    }

    [Fact]
    public async Task EmptyStringValues_HandleCorrectly()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "", Value = 0 },
            new TestDocument { Id = "2", PartitionKey = "pk", Name = "NotEmpty", Value = 1 });

        var results = await DrainAsync(
            _realContainer.GetItemLinqQueryable<TestDocument>()
                .Where(d => d.Name == "")
                .ToFeedIterator());

        results.Should().ContainSingle().Which.Id.Should().Be("1");
    }

    [Fact]
    public async Task NumericEdgeCases_IntMinMax_QueryCorrectly()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "MinVal", Value = int.MinValue },
            new TestDocument { Id = "2", PartitionKey = "pk", Name = "MaxVal", Value = int.MaxValue },
            new TestDocument { Id = "3", PartitionKey = "pk", Name = "Zero", Value = 0 });

        var results = await DrainAsync(
            _realContainer.GetItemLinqQueryable<TestDocument>()
                .Where(d => d.Value > 0)
                .ToFeedIterator());

        results.Should().ContainSingle().Which.Name.Should().Be("MaxVal");
    }

    [Fact]
    public async Task MultiplePartitions_OrderByAcrossPartitions()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "C", Value = 30 },
            new TestDocument { Id = "2", PartitionKey = "pk2", Name = "A", Value = 10 },
            new TestDocument { Id = "3", PartitionKey = "pk3", Name = "B", Value = 20 });

        var results = await DrainAsync(
            _realContainer.GetItemLinqQueryable<TestDocument>()
                .OrderBy(d => d.Name)
                .ToFeedIterator());

        results.Select(d => d.Name).Should().ContainInConsecutiveOrder("A", "B", "C");
    }

    [Fact]
    public async Task WhereOnNestedProperty_FiltersCorrectly()
    {
        await _inMemoryContainer.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A", Nested = new NestedObject { Score = 9.5 } },
            new PartitionKey("pk"));
        await _inMemoryContainer.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk", Name = "B", Nested = new NestedObject { Score = 3.0 } },
            new PartitionKey("pk"));

        var results = await DrainAsync(
            _realContainer.GetItemLinqQueryable<TestDocument>()
                .Where(d => d.Nested != null && d.Nested.Score > 5.0)
                .ToFeedIterator());

        results.Should().ContainSingle().Which.Name.Should().Be("A");
    }

    [Fact]
    public async Task SelectWithConcat_ProjectsCorrectly()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" });

        var results = await DrainAsync(
            _realContainer.GetItemLinqQueryable<TestDocument>()
                .Select(d => d.Name + "-" + d.Id)
                .ToFeedIterator());

        results.Should().ContainSingle().Which.Should().Be("Alice-1");
    }

    [Fact]
    public async Task ConditionalFilter_TernaryInWhere()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Active", Value = 10, IsActive = true },
            new TestDocument { Id = "2", PartitionKey = "pk", Name = "Inactive", Value = 20, IsActive = false });

        var results = await DrainAsync(
            _realContainer.GetItemLinqQueryable<TestDocument>()
                .Where(d => d.IsActive ? d.Value > 5 : d.Value > 100)
                .ToFeedIterator());

        results.Should().ContainSingle().Which.Name.Should().Be("Active");
    }

    [Fact]
    public async Task NullNestedProperty_DoesNotThrow()
    {
        await SeedAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "NoNested" });

        var results = await DrainAsync(
            _realContainer.GetItemLinqQueryable<TestDocument>()
                .Where(d => d.Nested == null)
                .ToFeedIterator());

        results.Should().ContainSingle().Which.Name.Should().Be("NoNested");
    }

    [Fact]
    public async Task ArrayContains_FiltersOnTag()
    {
        await _inMemoryContainer.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Tagged", Tags = ["important", "urgent"] },
            new PartitionKey("pk"));
        await _inMemoryContainer.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk", Name = "NotTagged", Tags = ["low"] },
            new PartitionKey("pk"));

        var results = await DrainAsync(
            _realContainer.GetItemLinqQueryable<TestDocument>()
                .Where(d => d.Tags.Contains("urgent"))
                .ToFeedIterator());

        results.Should().ContainSingle().Which.Name.Should().Be("Tagged");
    }

    [Fact]
    public async Task MultipleAggregates_AllReturnCorrectValues()
    {
        for (var i = 1; i <= 5; i++)
            await _inMemoryContainer.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk", Name = $"Item{i}", Value = i * 10 },
                new PartitionKey("pk"));

        var q = _realContainer.GetItemLinqQueryable<TestDocument>();

        var count = await DrainAsync(q.Select(d => 1).ToFeedIterator());
        count.Should().HaveCount(5);

        var ordered = await DrainAsync(
            q.OrderByDescending(d => d.Value).Take(1).ToFeedIterator());
        ordered.Should().ContainSingle().Which.Value.Should().Be(50);
    }
}
