using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for change feed through FakeCosmosHandler.
/// CRUD operations go through the SDK HTTP pipeline, then change feed is verified
/// via backing container's checkpoint-based API and SDK iterators.
/// Note: SDK's Container.GetChangeFeedIterator doesn't work through FakeCosmosHandler
/// because the handler doesn't implement the A-IM change feed HTTP protocol.
/// </summary>
public class FakeCosmosHandlerChangeFeedTests : IDisposable
{
    private readonly InMemoryContainer _inMemoryContainer;
    private readonly FakeCosmosHandler _handler;
    private readonly CosmosClient _client;
    private readonly Container _container;

    public FakeCosmosHandlerChangeFeedTests()
    {
        _inMemoryContainer = new InMemoryContainer("test-changefeed", "/partitionKey");
        _handler = new FakeCosmosHandler(_inMemoryContainer);
        _client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                RequestTimeout = TimeSpan.FromSeconds(10),
                HttpClientFactory = () => new HttpClient(_handler) { Timeout = TimeSpan.FromSeconds(10) }
            });
        _container = _client.GetContainer("db", "test-changefeed");
    }

    public void Dispose()
    {
        _client.Dispose();
        _handler.Dispose();
    }

    private async Task<List<T>> DrainIterator<T>(FeedIterator<T> iterator)
    {
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        return results;
    }

    private async Task<List<T>> DrainChangeFeed<T>(FeedIterator<T> iterator, int maxPages = 10)
    {
        var results = new List<T>();
        int pages = 0;
        while (iterator.HasMoreResults && pages < maxPages)
        {
            try
            {
                var page = await iterator.ReadNextAsync();
                results.AddRange(page);
                pages++;
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotModified)
            {
                break;
            }
        }
        return results;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  CRUD through handler -> change feed via backing container
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ChangeFeed_CreateThroughHandler_VisibleInBackingContainer()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new PartitionKey("pk1"));

        var iterator = _handler.BackingContainer.GetChangeFeedIterator<TestDocument>(0);
        var items = await DrainIterator(iterator);
        items.Should().HaveCount(2);
    }

    [Fact]
    public async Task ChangeFeed_Incremental_TracksNewItemsAfterCheckpoint()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var checkpoint = _handler.BackingContainer.GetChangeFeedCheckpoint();

        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
            new PartitionKey("pk1"));

        var iterator = _handler.BackingContainer.GetChangeFeedIterator<TestDocument>(checkpoint);
        var newItems = await DrainIterator(iterator);
        newItems.Should().HaveCount(1);
        newItems[0].Name.Should().Be("Bob");
    }

    [Fact]
    public async Task ChangeFeed_UpsertThroughHandler_RecordsChange()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Before" },
            new PartitionKey("pk1"));

        var checkpoint = _handler.BackingContainer.GetChangeFeedCheckpoint();

        await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "After" },
            new PartitionKey("pk1"));

        var iterator = _handler.BackingContainer.GetChangeFeedIterator<TestDocument>(checkpoint);
        var changes = await DrainIterator(iterator);
        changes.Should().HaveCount(1);
        changes[0].Name.Should().Be("After");
    }

    [Fact]
    public async Task ChangeFeed_DeleteThroughHandler_AdvancesCheckpoint()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "ToDelete" },
            new PartitionKey("pk1"));

        var checkpoint = _handler.BackingContainer.GetChangeFeedCheckpoint();

        await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        var newCheckpoint = _handler.BackingContainer.GetChangeFeedCheckpoint();
        newCheckpoint.Should().BeGreaterThan(checkpoint);
    }

    [Fact]
    public async Task ChangeFeed_PatchThroughHandler_RecordsUpdatedDocument()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Before", Value = 10 },
            new PartitionKey("pk1"));

        var checkpoint = _handler.BackingContainer.GetChangeFeedCheckpoint();

        await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "After")]);

        var iterator = _handler.BackingContainer.GetChangeFeedIterator<TestDocument>(checkpoint);
        var changes = await DrainIterator(iterator);
        changes.Should().HaveCount(1);
        changes[0].Name.Should().Be("After");
    }

    [Fact]
    public async Task ChangeFeed_ReplaceThroughHandler_RecordsFullDocument()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Before" },
            new PartitionKey("pk1"));

        var checkpoint = _handler.BackingContainer.GetChangeFeedCheckpoint();

        await _container.ReplaceItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "After", Value = 99 },
            "1", new PartitionKey("pk1"));

        var iterator = _handler.BackingContainer.GetChangeFeedIterator<TestDocument>(checkpoint);
        var changes = await DrainIterator(iterator);
        changes.Should().HaveCount(1);
        changes[0].Name.Should().Be("After");
        changes[0].Value.Should().Be(99);
    }

    [Fact]
    public async Task ChangeFeed_BatchThroughHandler_TracksAllOperations()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "b1", PartitionKey = "pk1", Name = "Batch1" });
        batch.CreateItem(new TestDocument { Id = "b2", PartitionKey = "pk1", Name = "Batch2" });
        await batch.ExecuteAsync();

        var iterator = _handler.BackingContainer.GetChangeFeedIterator<TestDocument>(0);
        var changes = await DrainIterator(iterator);
        changes.Should().HaveCount(2);
        changes.Select(c => c.Name).Should().BeEquivalentTo("Batch1", "Batch2");
    }

    [Fact]
    public async Task ChangeFeed_ManyItemsThroughHandler_AllTracked()
    {
        for (int i = 0; i < 20; i++)
        {
            await _container.CreateItemAsync(
                new TestDocument { Id = $"item-{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));
        }

        var iterator = _handler.BackingContainer.GetChangeFeedIterator<TestDocument>(0);
        var allChanges = await DrainIterator(iterator);
        allChanges.Should().HaveCount(20);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Change feed via backing container's SDK iterator
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ChangeFeed_BackingContainerSdkIterator_Beginning_ReturnsAllItems()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk2", Name = "Bob" },
            new PartitionKey("pk2"));

        var iterator = _handler.BackingContainer.GetChangeFeedIterator<TestDocument>(
            ChangeFeedStartFrom.Beginning(),
            ChangeFeedMode.Incremental);

        var results = await DrainChangeFeed(iterator);
        results.Should().HaveCount(2);
        results.Select(r => r.Name).Should().Contain("Alice");
        results.Select(r => r.Name).Should().Contain("Bob");
    }

    [Fact]
    public async Task ChangeFeed_BackingContainerSdkIterator_EmptyContainer_Returns304()
    {
        var iterator = _handler.BackingContainer.GetChangeFeedIterator<TestDocument>(
            ChangeFeedStartFrom.Beginning(),
            ChangeFeedMode.Incremental);

        var results = await DrainChangeFeed(iterator);
        results.Should().BeEmpty();
    }

    [Fact]
    public async Task ChangeFeed_BackingContainerSdkIterator_WithPartitionKey_ScopesToPartition()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk2", Name = "Bob" },
            new PartitionKey("pk2"));

        var iterator = _handler.BackingContainer.GetChangeFeedIterator<TestDocument>(
            ChangeFeedStartFrom.Beginning(FeedRange.FromPartitionKey(new PartitionKey("pk1"))),
            ChangeFeedMode.Incremental);

        var results = await DrainChangeFeed(iterator);
        results.Should().OnlyContain(r => r.PartitionKey == "pk1");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Change feed processor — uses backing container
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ChangeFeed_Processor_ReceivesItemsCreatedThroughHandler()
    {
        var received = new List<TestDocument>();
        var processor = _handler.BackingContainer.GetChangeFeedProcessorBuilder<TestDocument>(
                "test-processor",
                (context, changes, ct) =>
                {
                    received.AddRange(changes);
                    return Task.CompletedTask;
                })
            .WithInMemoryLeaseContainer()
            .WithInstanceName("instance1")
            .WithStartTime(DateTime.MinValue.ToUniversalTime())
            .Build();

        await processor.StartAsync();

        // Create through the handler (HTTP pipeline)
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "ProcessorItem" },
            new PartitionKey("pk1"));

        // Allow processor time to pick up the item
        await Task.Delay(1000);
        await processor.StopAsync();

        received.Should().Contain(r => r.Name == "ProcessorItem");
    }
}
