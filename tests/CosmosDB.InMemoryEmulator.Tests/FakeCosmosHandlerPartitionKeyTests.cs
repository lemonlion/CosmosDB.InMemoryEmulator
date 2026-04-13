using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for DeleteAllItemsByPartitionKey and other partition key edge cases
/// through FakeCosmosHandler.
/// </summary>
public class FakeCosmosHandlerPartitionKeyTests : IDisposable
{
    private readonly InMemoryContainer _inMemoryContainer;
    private readonly FakeCosmosHandler _handler;
    private readonly CosmosClient _client;
    private readonly Container _container;

    public FakeCosmosHandlerPartitionKeyTests()
    {
        _inMemoryContainer = new InMemoryContainer("test-pk", "/partitionKey");
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
        _container = _client.GetContainer("db", "test-pk");
    }

    public void Dispose()
    {
        _client.Dispose();
        _handler.Dispose();
    }

    private async Task<List<T>> DrainQuery<T>(string sql)
    {
        var iterator = _container.GetItemQueryIterator<T>(sql);
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        return results;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  DeleteAllItemsByPartitionKeyStreamAsync
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task DeleteAllByPK_RemovesAllItemsInPartition()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "B" }, new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "3", PartitionKey = "pk2", Name = "C" }, new PartitionKey("pk2"));

        var response = await _handler.BackingContainer.DeleteAllItemsByPartitionKeyStreamAsync(
            new PartitionKey("pk1"));
        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var remaining = await DrainQuery<TestDocument>("SELECT * FROM c");
        remaining.Should().HaveCount(1);
        remaining[0].Name.Should().Be("C");
    }

    [Fact]
    public async Task DeleteAllByPK_EmptyPartition_Succeeds()
    {
        var response = await _handler.BackingContainer.DeleteAllItemsByPartitionKeyStreamAsync(
            new PartitionKey("nonexistent"));
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task DeleteAllByPK_AdvancesChangeFeed()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

        var checkpoint = _handler.BackingContainer.GetChangeFeedCheckpoint();

        await _handler.BackingContainer.DeleteAllItemsByPartitionKeyStreamAsync(
            new PartitionKey("pk1"));

        var newCheckpoint = _handler.BackingContainer.GetChangeFeedCheckpoint();
        newCheckpoint.Should().BeGreaterThan(checkpoint);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Cross-partition queries through handler
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CrossPartitionQuery_ReturnsItemsFromAllPartitions()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk2", Name = "B" }, new PartitionKey("pk2"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "3", PartitionKey = "pk3", Name = "C" }, new PartitionKey("pk3"));

        var results = await DrainQuery<TestDocument>("SELECT * FROM c ORDER BY c.name");
        results.Should().HaveCount(3);
        results.Select(r => r.Name).Should().ContainInOrder("A", "B", "C");
    }

    [Fact]
    public async Task PartitionKeyNone_CrudRoundTrip()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "none-1", PartitionKey = null!, Name = "NoPartition" },
            PartitionKey.None);

        var read = await _container.ReadItemAsync<TestDocument>("none-1", PartitionKey.None);
        read.Resource.Name.Should().Be("NoPartition");
    }
}
