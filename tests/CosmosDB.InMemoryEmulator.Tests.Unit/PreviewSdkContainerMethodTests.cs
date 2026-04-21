using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for Container methods that became public abstract in 3.59.0-preview.0:
/// <list type="bullet">
///   <item><see cref="Container.GetChangeFeedProcessorBuilderWithAllVersionsAndDeletes{T}"/></item>
///   <item><see cref="Container.GetPartitionKeyRangesAsync"/></item>
///   <item><see cref="Container.SemanticRerankAsync"/></item>
/// </list>
/// Tests that require types that were internal in 3.58.x are compiled only when targeting
/// 3.59.0 or later (guarded by the <c>COSMOS_SDK_PREVIEW_METHODS</c> symbol).
/// </summary>
public class PreviewSdkContainerMethodTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

#if COSMOS_SDK_PREVIEW_METHODS

    // ── GetChangeFeedProcessorBuilderWithAllVersionsAndDeletes ────────────────

    [Fact]
    public async Task AllVersionsProcessor_Create_ReportsCreateOperationType()
    {
        var received = new List<ChangeFeedItem<TestDocument>>();
        var tcs = new TaskCompletionSource<bool>();

        var processor = _container
            .GetChangeFeedProcessorBuilderWithAllVersionsAndDeletes<TestDocument>(
                "avd-processor",
                (_, changes, _) =>
                {
                    lock (received) received.AddRange(changes);
                    tcs.TrySetResult(true);
                    return Task.CompletedTask;
                })
            .WithInstanceName("i1")
            .WithInMemoryLeaseContainer()
            .Build();

        await processor.StartAsync();

        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        await Task.WhenAny(tcs.Task, Task.Delay(5_000));
        await processor.StopAsync();

        received.Should().ContainSingle();
        received[0].Metadata.OperationType.Should().Be(ChangeFeedOperationType.Create);
        received[0].Current.Should().NotBeNull();
        received[0].Current!.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task AllVersionsProcessor_Upsert_ReportsReplaceOperationType()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "V1" },
            new PartitionKey("pk1"));

        var received = new List<ChangeFeedItem<TestDocument>>();
        var tcs = new TaskCompletionSource<bool>();

        var processor = _container
            .GetChangeFeedProcessorBuilderWithAllVersionsAndDeletes<TestDocument>(
                "avd-processor",
                (_, changes, _) =>
                {
                    lock (received) received.AddRange(changes);
                    tcs.TrySetResult(true);
                    return Task.CompletedTask;
                })
            .WithInstanceName("i1")
            .WithInMemoryLeaseContainer()
            .Build();

        await processor.StartAsync();

        await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "V2" },
            new PartitionKey("pk1"));

        await Task.WhenAny(tcs.Task, Task.Delay(5_000));
        await processor.StopAsync();

        received.Should().ContainSingle();
        received[0].Metadata.OperationType.Should().Be(ChangeFeedOperationType.Replace);
    }

    [Fact]
    public async Task AllVersionsProcessor_Delete_ReportsDeleteOperationType()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var received = new List<ChangeFeedItem<TestDocument>>();
        var tcs = new TaskCompletionSource<bool>();

        var processor = _container
            .GetChangeFeedProcessorBuilderWithAllVersionsAndDeletes<TestDocument>(
                "avd-processor",
                (_, changes, _) =>
                {
                    lock (received) received.AddRange(changes);
                    tcs.TrySetResult(true);
                    return Task.CompletedTask;
                })
            .WithInstanceName("i1")
            .WithInMemoryLeaseContainer()
            .Build();

        await processor.StartAsync();

        await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        await Task.WhenAny(tcs.Task, Task.Delay(5_000));
        await processor.StopAsync();

        received.Should().ContainSingle();
        received[0].Metadata.OperationType.Should().Be(ChangeFeedOperationType.Delete);
    }

    [Fact]
    public async Task AllVersionsProcessor_DeliverAllVersionsWithoutDeduplication()
    {
        var received = new List<ChangeFeedItem<TestDocument>>();
        var targetCount = 3;
        var tcs = new TaskCompletionSource<bool>();

        var processor = _container
            .GetChangeFeedProcessorBuilderWithAllVersionsAndDeletes<TestDocument>(
                "avd-processor",
                (_, changes, _) =>
                {
                    lock (received)
                    {
                        received.AddRange(changes);
                        if (received.Count >= targetCount) tcs.TrySetResult(true);
                    }
                    return Task.CompletedTask;
                })
            .WithInstanceName("i1")
            .WithInMemoryLeaseContainer()
            .Build();

        await processor.StartAsync();

        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "V1" },
            new PartitionKey("pk1"));
        await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "V2" },
            new PartitionKey("pk1"));
        await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        await Task.WhenAny(tcs.Task, Task.Delay(5_000));
        await processor.StopAsync();

        received.Should().HaveCount(3);
        received[0].Metadata.OperationType.Should().Be(ChangeFeedOperationType.Create);
        received[1].Metadata.OperationType.Should().Be(ChangeFeedOperationType.Replace);
        received[2].Metadata.OperationType.Should().Be(ChangeFeedOperationType.Delete);
    }

    [Fact]
    public async Task AllVersionsProcessor_RecreatedItem_ReportsCreateAfterDelete()
    {
        // Create, delete, then re-create the same item id
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "First" },
            new PartitionKey("pk1"));
        await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        var received = new List<ChangeFeedItem<TestDocument>>();
        var tcs = new TaskCompletionSource<bool>();

        var processor = _container
            .GetChangeFeedProcessorBuilderWithAllVersionsAndDeletes<TestDocument>(
                "avd-processor",
                (_, changes, _) =>
                {
                    lock (received)
                    {
                        received.AddRange(changes);
                        if (received.Count >= 1) tcs.TrySetResult(true);
                    }
                    return Task.CompletedTask;
                })
            .WithInstanceName("i1")
            .WithInMemoryLeaseContainer()
            .Build();

        await processor.StartAsync();

        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Recreated" },
            new PartitionKey("pk1"));

        await Task.WhenAny(tcs.Task, Task.Delay(5_000));
        await processor.StopAsync();

        received.Should().ContainSingle();
        received[0].Metadata.OperationType.Should().Be(ChangeFeedOperationType.Create,
            "re-creating an item after a delete should report Create, not Replace");
    }

    // ── GetPartitionKeyRangesAsync ────────────────────────────────────────────

    [Fact]
    public async Task GetPartitionKeyRangesAsync_SingleRange_ReturnsZero()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 1 };
        var feedRanges = await container.GetFeedRangesAsync();

        var rangeIds = (await container.GetPartitionKeyRangesAsync(feedRanges[0])).ToList();

        rangeIds.Should().ContainSingle().Which.Should().Be("0");
    }

    [Fact]
    public async Task GetPartitionKeyRangesAsync_MultipleRanges_ReturnsMatchingIds()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };
        var feedRanges = await container.GetFeedRangesAsync();

        var allRangeIds = new List<string>();
        foreach (var fr in feedRanges)
        {
            var ids = await container.GetPartitionKeyRangesAsync(fr);
            allRangeIds.AddRange(ids);
        }

        // Each feed range should map to exactly one partition key range
        allRangeIds.Should().HaveCount(4);
        allRangeIds.Should().BeEquivalentTo(new[] { "0", "1", "2", "3" });
    }

    // ── SemanticRerankAsync ────────────────────────────────────────────────────

    [Fact]
    public async Task SemanticRerankAsync_ThrowsNotSupportedException()
    {
        var act = async () => await _container.SemanticRerankAsync(
            "my context", new[] { "doc1", "doc2" }, null);

        await act.Should().ThrowAsync<NotSupportedException>()
            .WithMessage("*SemanticRerankAsync*");
    }

#else
    [Fact]
    public void PreviewMethods_NotAvailableIn358_SkipCoverage()
    {
        // The three abstract methods (GetChangeFeedProcessorBuilderWithAllVersionsAndDeletes,
        // GetPartitionKeyRangesAsync, SemanticRerankAsync) were internal in SDK 3.58.x.
        // Full test coverage is enabled by the COSMOS_SDK_PREVIEW_METHODS symbol when building
        // against 3.59.0 or later.
        Assert.True(true, "Placeholder — preview method tests compiled out for 3.58.x.");
    }
#endif
}
