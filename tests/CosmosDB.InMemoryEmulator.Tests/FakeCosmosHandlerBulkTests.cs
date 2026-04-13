using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for concurrent/bulk operations through FakeCosmosHandler.
/// Exercises concurrent SDK operations through the handler pipeline.
/// </summary>
public class FakeCosmosHandlerBulkTests : IDisposable
{
    private readonly InMemoryContainer _inMemoryContainer;
    private readonly FakeCosmosHandler _handler;
    private readonly CosmosClient _client;
    private readonly Container _container;

    public FakeCosmosHandlerBulkTests()
    {
        _inMemoryContainer = new InMemoryContainer("test-bulk", "/partitionKey");
        _handler = new FakeCosmosHandler(_inMemoryContainer);
        _client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                RequestTimeout = TimeSpan.FromSeconds(30),
                HttpClientFactory = () => new HttpClient(_handler) { Timeout = TimeSpan.FromSeconds(30) }
            });
        _container = _client.GetContainer("db", "test-bulk");
    }

    public void Dispose()
    {
        _client.Dispose();
        _handler.Dispose();
    }

    [Fact]
    public async Task Bulk_ConcurrentCreates_AllSucceed()
    {
        var tasks = Enumerable.Range(0, 50).Select(i =>
            _container.CreateItemAsync(
                new TestDocument { Id = $"bulk-{i}", PartitionKey = "pk1", Name = $"Item{i}", Value = i },
                new PartitionKey("pk1")));

        var results = await Task.WhenAll(tasks);

        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);

        var iterator = _container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
        var items = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            items.AddRange(page);
        }
        items.Should().HaveCount(50);
    }

    [Fact]
    public async Task Bulk_ConcurrentUpserts_AllSucceed()
    {
        // Create initial items
        for (int i = 0; i < 10; i++)
        {
            await _container.CreateItemAsync(
                new TestDocument { Id = $"upsert-{i}", PartitionKey = "pk1", Name = $"Original{i}", Value = i },
                new PartitionKey("pk1"));
        }

        // Concurrent upserts to update them all
        var tasks = Enumerable.Range(0, 10).Select(i =>
            _container.UpsertItemAsync(
                new TestDocument { Id = $"upsert-{i}", PartitionKey = "pk1", Name = $"Updated{i}", Value = i * 10 },
                new PartitionKey("pk1")));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
    }

    [Fact]
    public async Task Bulk_MixedCreateAndUpsert_DifferentPartitions_AllSucceed()
    {
        // Seed items in different partitions
        await _container.CreateItemAsync(
            new TestDocument { Id = "u1", PartitionKey = "pk-u", Name = "Original", Value = 1 },
            new PartitionKey("pk-u"));

        // Concurrent creates across different partitions + upsert in separate partition
        var tasks = new List<Task<ItemResponse<TestDocument>>>
        {
            _container.CreateItemAsync(
                new TestDocument { Id = "new1", PartitionKey = "pk-a", Name = "New1", Value = 100 },
                new PartitionKey("pk-a")),
            _container.CreateItemAsync(
                new TestDocument { Id = "new2", PartitionKey = "pk-b", Name = "New2", Value = 200 },
                new PartitionKey("pk-b")),
            _container.UpsertItemAsync(
                new TestDocument { Id = "u1", PartitionKey = "pk-u", Name = "Updated", Value = 999 },
                new PartitionKey("pk-u")),
        };

        await Task.WhenAll(tasks);

        var read = await _container.ReadItemAsync<TestDocument>("new1", new PartitionKey("pk-a"));
        read.Resource.Name.Should().Be("New1");

        var readUpdated = await _container.ReadItemAsync<TestDocument>("u1", new PartitionKey("pk-u"));
        readUpdated.Resource.Name.Should().Be("Updated");
    }

    [Fact]
    public async Task Bulk_ConcurrentCreates_DifferentPartitions_AllSucceed()
    {
        var tasks = Enumerable.Range(0, 30).Select(i =>
            _container.CreateItemAsync(
                new TestDocument { Id = $"pk-{i}", PartitionKey = $"pk-{i % 5}", Name = $"Item{i}" },
                new PartitionKey($"pk-{i % 5}")));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
    }
}
