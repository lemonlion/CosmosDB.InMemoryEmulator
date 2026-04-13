using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for TransactionalBatch operations through FakeCosmosHandler.
/// Validates the HybridRow binary batch protocol handling.
/// </summary>
public class FakeCosmosHandlerBatchTests : IDisposable
{
    private readonly InMemoryContainer _inMemoryContainer;
    private readonly FakeCosmosHandler _handler;
    private readonly CosmosClient _client;
    private readonly Container _container;

    public FakeCosmosHandlerBatchTests()
    {
        _inMemoryContainer = new InMemoryContainer("test-batch", "/partitionKey");
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
        _container = _client.GetContainer("db", "test-batch");
    }

    public void Dispose()
    {
        _client.Dispose();
        _handler.Dispose();
    }

    [Fact]
    public async Task Batch_CreateTwoItems_ReturnsOk()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "item-1", PartitionKey = "pk1", Name = "Alice", Value = 1 });
        batch.CreateItem(new TestDocument { Id = "item-2", PartitionKey = "pk1", Name = "Bob", Value = 2 });

        var response = await batch.ExecuteAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.IsSuccessStatusCode.Should().BeTrue();
        response.Count.Should().Be(2);
        response[0].StatusCode.Should().Be(HttpStatusCode.Created);
        response[1].StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task Batch_CreateItems_PersistsToBackingContainer()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "p1", PartitionKey = "pk1", Name = "Persisted", Value = 42 });

        var response = await batch.ExecuteAsync();
        response.IsSuccessStatusCode.Should().BeTrue();

        // Verify item exists via direct point read through the SDK
        var readResponse = await _container.ReadItemAsync<TestDocument>("p1", new PartitionKey("pk1"));
        readResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        readResponse.Resource.Name.Should().Be("Persisted");
    }

    [Fact]
    public async Task Batch_UpsertItem_ReturnsOk()
    {
        // Create initial item
        await _container.CreateItemAsync(new TestDocument { Id = "u1", PartitionKey = "pk1", Name = "Original", Value = 1 }, new PartitionKey("pk1"));

        // Upsert via batch
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.UpsertItem(new TestDocument { Id = "u1", PartitionKey = "pk1", Name = "Updated", Value = 2 });

        var response = await batch.ExecuteAsync();
        response.IsSuccessStatusCode.Should().BeTrue();

        var readResponse = await _container.ReadItemAsync<TestDocument>("u1", new PartitionKey("pk1"));
        readResponse.Resource.Name.Should().Be("Updated");
    }

    [Fact]
    public async Task Batch_ReadItem_ReturnsOk()
    {
        await _container.CreateItemAsync(new TestDocument { Id = "r1", PartitionKey = "pk1", Name = "Readable", Value = 1 }, new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.ReadItem("r1");

        var response = await batch.ExecuteAsync();
        response.IsSuccessStatusCode.Should().BeTrue();
        response[0].StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Batch_DeleteItem_ReturnsOk()
    {
        await _container.CreateItemAsync(new TestDocument { Id = "d1", PartitionKey = "pk1", Name = "Deletable", Value = 1 }, new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.DeleteItem("d1");

        var response = await batch.ExecuteAsync();
        response.IsSuccessStatusCode.Should().BeTrue();

        // Verify item is gone
        try
        {
            await _container.ReadItemAsync<TestDocument>("d1", new PartitionKey("pk1"));
            throw new Exception("Should have thrown CosmosException with NotFound");
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            // Expected
        }
    }

    [Fact]
    public async Task Batch_ReplaceItem_ReturnsOk()
    {
        await _container.CreateItemAsync(new TestDocument { Id = "rp1", PartitionKey = "pk1", Name = "Original", Value = 1 }, new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.ReplaceItem("rp1", new TestDocument { Id = "rp1", PartitionKey = "pk1", Name = "Replaced", Value = 99 });

        var response = await batch.ExecuteAsync();
        response.IsSuccessStatusCode.Should().BeTrue();

        var readResponse = await _container.ReadItemAsync<TestDocument>("rp1", new PartitionKey("pk1"));
        readResponse.Resource.Name.Should().Be("Replaced");
    }

    [Fact]
    public async Task Batch_MixedOperations_ReturnsOk()
    {
        // Pre-create items for replace and delete
        await _container.CreateItemAsync(new TestDocument { Id = "mx-replace", PartitionKey = "pk1", Name = "ToReplace", Value = 1 }, new PartitionKey("pk1"));
        await _container.CreateItemAsync(new TestDocument { Id = "mx-delete", PartitionKey = "pk1", Name = "ToDelete", Value = 2 }, new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "mx-new", PartitionKey = "pk1", Name = "New", Value = 10 });
        batch.ReplaceItem("mx-replace", new TestDocument { Id = "mx-replace", PartitionKey = "pk1", Name = "Replaced", Value = 11 });
        batch.DeleteItem("mx-delete");
        batch.ReadItem("mx-replace");

        var response = await batch.ExecuteAsync();
        response.IsSuccessStatusCode.Should().BeTrue();
        response.Count.Should().Be(4);
        response[0].StatusCode.Should().Be(HttpStatusCode.Created);     // Create
        response[1].StatusCode.Should().Be(HttpStatusCode.OK);          // Replace
        response[2].StatusCode.Should().Be(HttpStatusCode.NoContent);   // Delete
        response[3].StatusCode.Should().Be(HttpStatusCode.OK);          // Read
    }

    [Fact]
    public async Task Batch_DuplicateId_RollsBackOnFailure()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "dup-1", PartitionKey = "pk1", Name = "First", Value = 1 });
        batch.CreateItem(new TestDocument { Id = "dup-1", PartitionKey = "pk1", Name = "Duplicate", Value = 2 });

        var response = await batch.ExecuteAsync();
        response.IsSuccessStatusCode.Should().BeFalse();

        // First item should have been rolled back
        try
        {
            await _container.ReadItemAsync<TestDocument>("dup-1", new PartitionKey("pk1"));
            throw new Exception("Should have thrown CosmosException with NotFound");
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            // Expected - rollback
        }
    }
}
