using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class ChangeFeedTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task ChangeFeed_AfterCreate_ContainsCreatedItem()
    {
        var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var iterator = _container.GetChangeFeedIterator<TestDocument>(
            ChangeFeedStartFrom.Beginning(),
            ChangeFeedMode.Incremental);

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            if (response.StatusCode == HttpStatusCode.NotModified)
            {
                break;
            }

            results.AddRange(response);
        }

        results.Should().Contain(item => item.Id == "1");
    }

    [Fact]
    public async Task ChangeFeed_AfterUpsert_ContainsUpdatedItem()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));
        await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alicia" },
            new PartitionKey("pk1"));

        var iterator = _container.GetChangeFeedIterator<TestDocument>(
            ChangeFeedStartFrom.Beginning(),
            ChangeFeedMode.Incremental);

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            if (response.StatusCode == HttpStatusCode.NotModified)
            {
                break;
            }

            results.AddRange(response);
        }

        results.Should().Contain(item => item.Id == "1" && item.Name == "Alicia");
    }

    [Fact]
    public async Task ChangeFeed_MultipleCreates_ContainsAllItems()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
            new PartitionKey("pk1"));

        var iterator = _container.GetChangeFeedIterator<TestDocument>(
            ChangeFeedStartFrom.Beginning(),
            ChangeFeedMode.Incremental);

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            if (response.StatusCode == HttpStatusCode.NotModified)
            {
                break;
            }

            results.AddRange(response);
        }

        results.Should().HaveCountGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task ChangeFeedStream_AfterCreate_ReturnsStream()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var iterator = _container.GetChangeFeedStreamIterator(
            ChangeFeedStartFrom.Beginning(),
            ChangeFeedMode.Incremental);

        if (iterator.HasMoreResults)
        {
            using var response = await iterator.ReadNextAsync();
            if (response.StatusCode != HttpStatusCode.NotModified)
            {
                using var reader = new StreamReader(response.Content);
                var body = await reader.ReadToEndAsync();
                var jObj = JObject.Parse(body);
                jObj["Documents"].Should().NotBeNull();
            }
        }
    }

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: Real Cosmos DB returns NotModified (304) for an empty
    /// change feed. InMemoryContainer returns OK (200) with an empty result set.
    /// </summary>
    [Fact]
    public async Task ChangeFeedIterator_EmptyContainer_ReturnsOkWithEmptyResults()
    {
        var iterator = _container.GetChangeFeedIterator<TestDocument>(
            ChangeFeedStartFrom.Beginning(),
            ChangeFeedMode.Incremental);

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().BeEmpty();
    }

    // ── Incremental change feed (change log) ──

    [Fact]
    public async Task ChangeFeed_FromBeginning_ReturnsAllChangesInOrder()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "First" },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Second" },
            new PartitionKey("pk1"));
        await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "FirstUpdated" },
            new PartitionKey("pk1"));

        var results = await ReadAllChangeFeed<TestDocument>();

        results.Should().HaveCount(2);
        results.Select(r => r.Name).Should().BeEquivalentTo("FirstUpdated", "Second");
    }

    [Fact]
    public async Task ChangeFeed_FromNow_ReturnsOnlySubsequentChanges()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Before" },
            new PartitionKey("pk1"));

        var checkpoint = _container.GetChangeFeedCheckpoint();

        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "After" },
            new PartitionKey("pk1"));

        var results = await ReadChangeFeedFrom<TestDocument>(checkpoint);

        results.Should().ContainSingle().Which.Name.Should().Be("After");
    }

    [Fact]
    public async Task ChangeFeed_Replace_RecordsChange()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));
        await _container.ReplaceItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" },
            "1",
            new PartitionKey("pk1"));

        var results = await ReadAllChangeFeed<TestDocument>();

        results.Should().ContainSingle().Which.Name.Should().Be("Replaced");
    }

    [Fact]
    public async Task ChangeFeed_Patch_RecordsChange()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original", Value = 10 },
            new PartitionKey("pk1"));
        await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            new List<PatchOperation> { PatchOperation.Set("/name", "Patched") });

        var results = await ReadAllChangeFeed<TestDocument>();

        results.Should().ContainSingle().Which.Name.Should().Be("Patched");
    }

    [Fact]
    public async Task ChangeFeed_Delete_DoesNotAppearInFeed()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "ToDelete" },
            new PartitionKey("pk1"));
        await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        var results = await ReadAllChangeFeed<TestDocument>();

        results.Should().ContainSingle().Which.Name.Should().Be("ToDelete");
    }

    [Fact]
    public async Task ChangeFeedProcessor_CanBeBuilt_AndStartedStopped()
    {
        var processor = _container.GetChangeFeedProcessorBuilder<TestDocument>(
                "test-processor",
                (context, changes, token) => Task.CompletedTask)
            .WithInstanceName("instance1")
            .WithInMemoryLeaseContainer()
            .Build();

        await processor.StartAsync();
        await processor.StopAsync();
    }

    [Fact]
    public async Task ChangeFeedProcessor_BuilderMethodsReturnBuilder()
    {
        var builder = _container.GetChangeFeedProcessorBuilder<TestDocument>(
            "test-processor",
            (context, changes, token) => Task.CompletedTask);

        var chained = builder
            .WithInstanceName("instance1")
            .WithInMemoryLeaseContainer()
            .WithPollInterval(TimeSpan.FromMilliseconds(50))
            .WithMaxItems(10);

        chained.Should().NotBeNull();
    }

    [Fact]
    public async Task ChangeFeedProcessor_InvokesHandler_WhenItemsCreated()
    {
        var received = new List<TestDocument>();
        var handlerCalled = new TaskCompletionSource<bool>();

        var processor = _container.GetChangeFeedProcessorBuilder<TestDocument>(
                "test-processor",
                (context, changes, token) =>
                {
                    received.AddRange(changes);
                    handlerCalled.TrySetResult(true);
                    return Task.CompletedTask;
                })
            .WithInstanceName("instance1")
            .WithInMemoryLeaseContainer()
            .Build();

        await processor.StartAsync();

        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Processor-Test" },
            new PartitionKey("pk1"));

        await Task.WhenAny(handlerCalled.Task, Task.Delay(TimeSpan.FromSeconds(5)));

        await processor.StopAsync();

        received.Should().ContainSingle().Which.Name.Should().Be("Processor-Test");
    }

    [Fact]
    public async Task ChangeFeedProcessor_InvokesHandler_MultipleTimesForMultipleChanges()
    {
        var allReceived = new List<TestDocument>();
        var secondBatchReceived = new TaskCompletionSource<bool>();

        var processor = _container.GetChangeFeedProcessorBuilder<TestDocument>(
                "test-processor",
                (context, changes, token) =>
                {
                    allReceived.AddRange(changes);
                    if (allReceived.Count >= 2)
                    {
                        secondBatchReceived.TrySetResult(true);
                    }
                    return Task.CompletedTask;
                })
            .WithInstanceName("instance1")
            .WithInMemoryLeaseContainer()
            .Build();

        await processor.StartAsync();

        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "First" },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Second" },
            new PartitionKey("pk1"));

        await Task.WhenAny(secondBatchReceived.Task, Task.Delay(TimeSpan.FromSeconds(5)));

        await processor.StopAsync();

        allReceived.Should().HaveCountGreaterThanOrEqualTo(2);
        allReceived.Should().Contain(doc => doc.Name == "First");
        allReceived.Should().Contain(doc => doc.Name == "Second");
    }

    [Fact]
    public async Task ChangeFeedProcessor_LegacyChangesHandler_InvokesHandler()
    {
        var received = new List<TestDocument>();
        var handlerCalled = new TaskCompletionSource<bool>();

        var processor = _container.GetChangeFeedProcessorBuilder<TestDocument>(
                "test-processor",
                (IReadOnlyCollection<TestDocument> changes, CancellationToken token) =>
                {
                    received.AddRange(changes);
                    handlerCalled.TrySetResult(true);
                    return Task.CompletedTask;
                })
            .WithInstanceName("instance1")
            .WithInMemoryLeaseContainer()
            .Build();

        await processor.StartAsync();

        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Legacy-Test" },
            new PartitionKey("pk1"));

        await Task.WhenAny(handlerCalled.Task, Task.Delay(TimeSpan.FromSeconds(5)));

        await processor.StopAsync();

        received.Should().ContainSingle().Which.Name.Should().Be("Legacy-Test");
    }

    [Fact]
    public async Task ChangeFeedProcessor_Context_HasLeaseToken()
    {
        ChangeFeedProcessorContext capturedContext = null!;
        var handlerCalled = new TaskCompletionSource<bool>();

        var processor = _container.GetChangeFeedProcessorBuilder<TestDocument>(
                "test-processor",
                (context, changes, token) =>
                {
                    capturedContext = context;
                    handlerCalled.TrySetResult(true);
                    return Task.CompletedTask;
                })
            .WithInstanceName("instance1")
            .WithInMemoryLeaseContainer()
            .Build();

        await processor.StartAsync();

        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Context-Test" },
            new PartitionKey("pk1"));

        await Task.WhenAny(handlerCalled.Task, Task.Delay(TimeSpan.FromSeconds(5)));

        await processor.StopAsync();

        capturedContext.Should().NotBeNull();
        capturedContext.LeaseToken.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task ChangeFeedProcessor_StopAsync_StopsPolling()
    {
        var callCount = 0;

        var processor = _container.GetChangeFeedProcessorBuilder<TestDocument>(
                "test-processor",
                (context, changes, token) =>
                {
                    Interlocked.Increment(ref callCount);
                    return Task.CompletedTask;
                })
            .WithInstanceName("instance1")
            .WithInMemoryLeaseContainer()
            .Build();

        await processor.StartAsync();

        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "StopTest" },
            new PartitionKey("pk1"));

        await Task.Delay(TimeSpan.FromMilliseconds(500));
        await processor.StopAsync();

        var countAfterStop = callCount;
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "AfterStop" },
            new PartitionKey("pk1"));
        await Task.Delay(TimeSpan.FromMilliseconds(300));

        callCount.Should().Be(countAfterStop);
    }

    [Fact]
    public async Task GetFeedRanges_ReturnsSingleRange()
    {
        var ranges = await _container.GetFeedRangesAsync();

        ranges.Should().ContainSingle();
    }

    // ── Helpers ──

    private async Task<List<T>> ReadAllChangeFeed<T>()
    {
        var iterator = _container.GetChangeFeedIterator<T>(
            ChangeFeedStartFrom.Beginning(),
            ChangeFeedMode.Incremental);

        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            if (response.StatusCode == HttpStatusCode.NotModified)
            {
                break;
            }

            results.AddRange(response);
        }

        return results;
    }

    private async Task<List<T>> ReadChangeFeedFrom<T>(long checkpoint)
    {
        var iterator = _container.GetChangeFeedIterator<T>(checkpoint);

        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            if (response.StatusCode == HttpStatusCode.NotModified)
            {
                break;
            }

            results.AddRange(response);
        }

        return results;
    }
}
