using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;
using System.Text;

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
    public async Task ChangeFeed_Incremental_DeletedItem_IsFilteredOut()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "ToDelete" },
            new PartitionKey("pk1"));
        await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        var results = await ReadAllChangeFeed<TestDocument>();

        // Incremental mode filters out items whose last change is a delete
        results.Should().BeEmpty();
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


/// <summary>
/// Validates that GetChangeFeedProcessorBuilder with a ChangeFeedStreamHandler parameter
/// returns a non-null builder but the handler is never invoked (NoOp processor).
/// The basic builder return test is in CosmosClientApiGapTests.cs; this covers the
/// divergent behavior with a skipped ideal test and a sister documenting test.
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.getchangefeedprocessorbuilder
/// </summary>
public class ChangeFeedStreamProcessorDivergentTests5
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task ChangeFeedProcessorBuilder_StreamHandler_ShouldInvokeHandler()
    {
        var handlerCalled = new TaskCompletionSource<bool>();
        Container.ChangeFeedStreamHandler handler =
            (ChangeFeedProcessorContext context, Stream changes, CancellationToken token) =>
            {
                handlerCalled.TrySetResult(true);
                return Task.CompletedTask;
            };

        var processor = _container.GetChangeFeedProcessorBuilder("test-processor", handler)
            .WithInstanceName("instance1")
            .WithInMemoryLeaseContainer()
            .Build();

        await processor.StartAsync();

        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "StreamTest" },
            new PartitionKey("pk1"));

        await Task.WhenAny(handlerCalled.Task, Task.Delay(TimeSpan.FromSeconds(2)));
        await processor.StopAsync();

        handlerCalled.Task.IsCompleted.Should().BeTrue();
    }

}


public class ChangeFeedFeedRangeDivergentBehaviorTests4
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey") { FeedRangeCount = 4 };

    /// <summary>
    /// FeedRange filtering is now supported. When FeedRangeCount > 1,
    /// ChangeFeedStartFrom.Beginning(feedRange) scopes the change feed to the specified range.
    /// Iterating all ranges and unioning results yields the full dataset.
    /// With the default FeedRangeCount=1, the single range covers the entire hash space and
    /// Beginning() without a FeedRange returns all changes.
    /// </summary>
    [Fact]
    public async Task ChangeFeed_FeedRange_ScopesCorrectly()
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

        // Union of all ranges returns all items
        allResults.Should().HaveCount(2);
    }
}


public class ChangeFeedProcessorDivergentBehaviorTests
{
    /// <summary>
    /// BEHAVIORAL DIFFERENCE: The real Cosmos SDK's ChangeFeedProcessorBuilder.WithLeaseContainer()
    /// internally casts the provided Container to ContainerInternal (an internal abstract class that
    /// extends Container). InMemoryContainer only extends the public Container class and cannot be cast
    /// to ContainerInternal, causing an InvalidCastException.
    /// 
    /// This means the ChangeFeedProcessorBuilder flow (WithLeaseContainer + Build + Start/Stop) cannot
    /// be used with InMemoryContainer for lease management. The InMemoryChangeFeedProcessor provides a
    /// separate mechanism for testing change feed processor scenarios without requiring the internal
    /// SDK types.
    /// 
    /// Impact: Tests that build a ChangeFeedProcessor using GetChangeFeedProcessorBuilder and then call
    /// WithLeaseContainer will fail. Use InMemoryChangeFeedProcessor directly for change feed testing.
    /// </summary>
    [Fact]
    public void ChangeFeedProcessorBuilder_WithLeaseContainer_ThrowsInvalidCast()
    {
        var container = new InMemoryContainer("source", "/partitionKey");
        var leaseContainer = new InMemoryContainer("leases", "/id");

        var act = () => container.GetChangeFeedProcessorBuilder(
                "processor",
                (ChangeFeedProcessorContext ctx, IReadOnlyCollection<TestDocument> changes, CancellationToken ct) =>
                    Task.CompletedTask)
            .WithInstanceName("instance")
            .WithLeaseContainer(leaseContainer);

        act.Should().Throw<InvalidCastException>();
    }
}


public class ChangeFeedManualCheckpointDivergentBehaviorTests4
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    /// <summary>
    /// Manual checkpoint processor now works. The handler receives changes and can call
    /// checkpointAsync to save progress. The processor polls the in-memory change feed
    /// every 50ms, same as the automatic checkpoint variant.
    /// </summary>
    [Fact]
    public async Task ManualCheckpoint_ProcessorInvokesHandler()
    {
        var receivedChanges = new List<TestDocument>();
        var checkpointCalled = false;

        var processor = _container.GetChangeFeedProcessorBuilderWithManualCheckpoint<TestDocument>(
            "processor",
            async (context, changes, checkpointAsync, ct) =>
            {
                receivedChanges.AddRange(changes);
                checkpointCalled = true;
                await checkpointAsync();
            })
            .WithInstanceName("instance")
            .WithInMemoryLeaseContainer()
            .Build();

        await processor.StartAsync();

        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "ManualCp" },
            new PartitionKey("pk1"));

        await Task.Delay(500);
        await processor.StopAsync();

        receivedChanges.Should().ContainSingle().Which.Name.Should().Be("ManualCp");
        checkpointCalled.Should().BeTrue();
    }
}


public class ChangeFeedManualCheckpointStreamTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task ManualCheckpoint_StreamHandler_InvokesHandlerAndCheckpoints()
    {
        var handlerCalled = new TaskCompletionSource<bool>();
        var checkpointCalled = false;
        string receivedJson = null!;

        var processor = _container.GetChangeFeedProcessorBuilderWithManualCheckpoint(
            "stream-processor",
            async (context, stream, checkpointAsync, ct) =>
            {
                using var reader = new StreamReader(stream);
                receivedJson = await reader.ReadToEndAsync(ct);
                checkpointCalled = true;
                await checkpointAsync();
                handlerCalled.TrySetResult(true);
            })
            .WithInstanceName("instance")
            .WithInMemoryLeaseContainer()
            .Build();

        await processor.StartAsync();

        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "StreamMC" },
            new PartitionKey("pk1"));

        await Task.WhenAny(handlerCalled.Task, Task.Delay(TimeSpan.FromSeconds(2)));
        await processor.StopAsync();

        handlerCalled.Task.IsCompleted.Should().BeTrue();
        checkpointCalled.Should().BeTrue();
        receivedJson.Should().Contain("StreamMC");
    }

    [Fact]
    public async Task ManualCheckpoint_WithoutCallingCheckpoint_RedeliversChanges()
    {
        var deliveryCount = 0;

        var processor = _container.GetChangeFeedProcessorBuilderWithManualCheckpoint<TestDocument>(
            "processor",
            (context, changes, checkpointAsync, ct) =>
            {
                Interlocked.Increment(ref deliveryCount);
                // Deliberately NOT calling checkpointAsync — changes should be redelivered
                return Task.CompletedTask;
            })
            .WithInstanceName("instance")
            .WithInMemoryLeaseContainer()
            .Build();

        await processor.StartAsync();

        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Redelivery" },
            new PartitionKey("pk1"));

        // Wait for multiple poll cycles — if checkpoint not called, changes are redelivered
        await Task.Delay(300);
        await processor.StopAsync();

        deliveryCount.Should().BeGreaterThan(1,
            "when checkpoint is not called, the same changes should be redelivered on subsequent polls");
    }
}


public class ChangeFeedGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task ChangeFeed_FromCheckpoint_ReturnsOnlyNewChanges()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Before" },
            new PartitionKey("pk1"));

        var checkpoint = _container.GetChangeFeedCheckpoint();

        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "After" },
            new PartitionKey("pk1"));

        var iterator = _container.GetChangeFeedIterator<TestDocument>(checkpoint);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle().Which.Name.Should().Be("After");
    }

    [Fact]
    public async Task ChangeFeed_OrderPreserved_AcrossWrites()
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

        var iterator = _container.GetChangeFeedIterator<TestDocument>(0);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(3);
        results[0].Name.Should().Be("First");
        results[1].Name.Should().Be("Second");
        results[2].Name.Should().Be("FirstUpdated");
    }

    [Fact]
    public async Task ChangeFeed_Delete_AppearsTombstoneViaCheckpoint()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "ToDelete" },
            new PartitionKey("pk1"));
        var checkpointAfterCreate = _container.GetChangeFeedCheckpoint();

        await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        var iterator = _container.GetChangeFeedIterator<JObject>(checkpointAfterCreate);
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle();
        results[0]["id"]!.Value<string>().Should().Be("1");
        results[0]["_deleted"]!.Value<bool>().Should().BeTrue();
    }

    [Fact]
    public async Task ChangeFeed_Patch_RecordsChange()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original", Value = 10 },
            new PartitionKey("pk1"));
        var checkpoint = _container.GetChangeFeedCheckpoint();

        await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Patched")]);

        var iterator = _container.GetChangeFeedIterator<TestDocument>(checkpoint);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle().Which.Name.Should().Be("Patched");
    }

    [Fact]
    public async Task ChangeFeed_StreamIterator_ReturnsJsonEnvelope()
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
                ((JArray)jObj["Documents"]!).Should().HaveCountGreaterThan(0);
            }
        }
    }
}


public class ChangeFeedAdvancedTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public void GetChangeFeedEstimator_ReturnsNonNull()
    {
        var leaseContainer = new InMemoryContainer("leases", "/id");
        var estimator = _container.GetChangeFeedEstimator("estimator", leaseContainer);
        estimator.Should().NotBeNull();
    }

    [Fact]
    public void GetChangeFeedEstimatorBuilder_ReturnsBuilder()
    {
        var leaseContainer = new InMemoryContainer("leases", "/id");
        var builder = _container.GetChangeFeedEstimatorBuilder(
            "estimator",
            (long estimation, CancellationToken ct) => Task.CompletedTask);
        builder.Should().NotBeNull();
    }

    [Fact]
    public async Task GetChangeFeedStreamIterator_FromBeginning_ReturnsStream()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var iterator = _container.GetChangeFeedStreamIterator(
            ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);

        iterator.HasMoreResults.Should().BeTrue();

        var response = await iterator.ReadNextAsync();
        response.StatusCode.Should().NotBe(HttpStatusCode.InternalServerError);
    }

    [Fact(Skip = "InMemoryContainer cannot be cast to ContainerInternal, which is required by " +
                   "ChangeFeedProcessorBuilder.WithLeaseContainer(). The real Cosmos SDK internally casts the " +
                   "lease Container to ContainerInternal (an internal abstract class) to access internal APIs for " +
                   "lease management. InMemoryContainer extends the public Container abstract class but not " +
                   "ContainerInternal, so this cast fails with InvalidCastException. Implementing ContainerInternal " +
                   "would require depending on internal SDK types that are not part of the public API surface.")]
    public async Task ChangeFeedProcessor_StreamHandler_InvokesHandler()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var invoked = false;
        var leaseContainer = new InMemoryContainer("leases", "/id");
        var processor = _container.GetChangeFeedProcessorBuilder(
                "stream-processor",
                (ChangeFeedProcessorContext ctx, Stream changes, CancellationToken ct) =>
                {
                    invoked = true;
                    return Task.CompletedTask;
                })
            .WithInstanceName("instance")
            .WithLeaseContainer(leaseContainer)
            .Build();

        await processor.StartAsync();
        await Task.Delay(500);
        await processor.StopAsync();

        invoked.Should().BeTrue();
    }
}


public class ChangeFeedGapTests3
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task ChangeFeed_IncrementalMode_OnlyLatestVersionPerItem()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "V1" },
            new PartitionKey("pk1"));
        await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "V2" },
            new PartitionKey("pk1"));
        await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "V3" },
            new PartitionKey("pk1"));

        var iterator = _container.GetChangeFeedIterator<TestDocument>(
            ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        // Incremental should only return latest version
        results.Should().ContainSingle().Which.Name.Should().Be("V3");
    }

    [Fact]
    public async Task ChangeFeed_AllVersions_ViaCheckpoint()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "V1" },
            new PartitionKey("pk1"));
        await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "V2" },
            new PartitionKey("pk1"));

        // Checkpoint-based iterator returns all versions (including intermediates)
        var iterator = _container.GetChangeFeedIterator<TestDocument>(0);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        results[0].Name.Should().Be("V1");
        results[1].Name.Should().Be("V2");
    }

    [Fact]
    public async Task ChangeFeed_DeletedItems_InFullFidelity_ShowsTombstone()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));
        var checkpoint = _container.GetChangeFeedCheckpoint();

        await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        var iterator = _container.GetChangeFeedIterator<JObject>(checkpoint);
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        // Checkpoint-based iterator includes delete tombstones
        results.Should().ContainSingle();
        var tombstone = results[0];
        tombstone["id"]!.Value<string>().Should().Be("1");
        tombstone["_deleted"]!.Value<bool>().Should().BeTrue();
    }

    [Fact]
    public async Task ChangeFeed_FromTimestamp_FiltersOlderItems()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Old" },
            new PartitionKey("pk1"));

        await Task.Delay(100);
        var checkpoint = DateTime.UtcNow;

        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "New" },
            new PartitionKey("pk1"));

        var iterator = _container.GetChangeFeedIterator<TestDocument>(
            ChangeFeedStartFrom.Time(checkpoint), ChangeFeedMode.Incremental);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle().Which.Name.Should().Be("New");
    }
}


public class ChangeFeedGapTests4
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task ChangeFeed_PageSizeHint_LimitsResults()
    {
        for (var i = 0; i < 5; i++)
            await _container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));

        var iterator = _container.GetChangeFeedIterator<TestDocument>(
            ChangeFeedStartFrom.Beginning(),
            ChangeFeedMode.Incremental,
            new ChangeFeedRequestOptions { PageSizeHint = 2 });

        var firstPage = await iterator.ReadNextAsync();
        firstPage.Count.Should().BeLessThanOrEqualTo(2);
    }

    [Fact]
    public async Task ChangeFeed_FromFeedRange_ScopesToRange()
    {
        _container.FeedRangeCount = 4;

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

        // Each range returns a subset; union of all ranges returns all items
        allResults.Should().HaveCount(2);

        // At least one range should return fewer than all items (proving scoping works)
        var perRangeCounts = new List<int>();
        foreach (var range in ranges)
        {
            var iterator = _container.GetChangeFeedIterator<TestDocument>(
                ChangeFeedStartFrom.Beginning(range),
                ChangeFeedMode.Incremental);
            var rangeResults = new List<TestDocument>();
            while (iterator.HasMoreResults)
            {
                var page = await iterator.ReadNextAsync();
                rangeResults.AddRange(page);
            }
            perRangeCounts.Add(rangeResults.Count);
        }
        perRangeCounts.Should().Contain(c => c < 2, "at least one range should scope to a subset");
    }

    [Fact]
    public async Task ChangeFeed_ManualCheckpoint_InvokesHandler()
    {
        var invoked = false;
        var builder = _container.GetChangeFeedProcessorBuilderWithManualCheckpoint<TestDocument>(
            "processor",
            async (context, changes, checkpointAsync, ct) =>
            {
                invoked = true;
                await checkpointAsync();
            });

        var processor = builder.WithInstanceName("instance").WithInMemoryLeaseContainer().Build();
        await processor.StartAsync();

        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        await Task.Delay(500);
        await processor.StopAsync();

        invoked.Should().BeTrue();
    }
}





public class ChangeFeedGapTests2
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task ChangeFeed_FromNow_IgnoresPriorChanges()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Before" },
            new PartitionKey("pk1"));

        var iterator = _container.GetChangeFeedIterator<TestDocument>(
            ChangeFeedStartFrom.Now(), ChangeFeedMode.Incremental);

        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "After" },
            new PartitionKey("pk1"));

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().ContainSingle().Which.Name.Should().Be("After");
    }

    [Fact]
    public async Task ChangeFeed_ViaCheckpoint_IncludesUpdatedVersion()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "V1", Value = 1 },
            new PartitionKey("pk1"));

        var checkpoint = _container.GetChangeFeedCheckpoint();

        await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "V2", Value = 2 },
            new PartitionKey("pk1"));
        await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "V3", Value = 3 },
            new PartitionKey("pk1"));

        var iterator = _container.GetChangeFeedIterator<TestDocument>(checkpoint);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        // Incremental mode returns each version
        results.Should().HaveCount(2);
        results[0].Name.Should().Be("V2");
        results[1].Name.Should().Be("V3");
    }

    [Fact]
    public async Task ChangeFeed_UpsertNewAndExisting_BothAppear()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Existing" },
            new PartitionKey("pk1"));

        var checkpoint = _container.GetChangeFeedCheckpoint();

        await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
            new PartitionKey("pk1"));
        await _container.UpsertItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "New" },
            new PartitionKey("pk1"));

        var iterator = _container.GetChangeFeedIterator<TestDocument>(checkpoint);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
    }
}
