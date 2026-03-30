using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class TransactionalBatchTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Batch_CreateMultipleItems_AllSucceed()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });
        batch.CreateItem(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" });

        using var response = await batch.ExecuteAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Count.Should().Be(2);
        response[0].StatusCode.Should().Be(HttpStatusCode.Created);
        response[1].StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task Batch_ReadItem_ExecutesSuccessfully()
    {
        var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.ReadItem("1");

        using var response = await batch.ExecuteAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Count.Should().Be(1);
        response[0].StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task Batch_UpsertItem_CreatesNew()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.UpsertItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

        using var response = await batch.ExecuteAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        readResponse.Resource.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task Batch_UpsertItem_UpdatesExisting()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.UpsertItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alicia" });

        using var response = await batch.ExecuteAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        readResponse.Resource.Name.Should().Be("Alicia");
    }

    [Fact]
    public async Task Batch_ReplaceItem_UpdatesExisting()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.ReplaceItem("1", new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" });

        using var response = await batch.ExecuteAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        readResponse.Resource.Name.Should().Be("Updated");
    }

    [Fact]
    public async Task Batch_DeleteItem_RemovesItem()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.DeleteItem("1");

        using var response = await batch.ExecuteAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Batch_PatchItem_AppliesOperations()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.PatchItem("1", new List<PatchOperation>
        {
            PatchOperation.Set("/name", "Patched"),
            PatchOperation.Increment("/value", 5)
        });

        using var response = await batch.ExecuteAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        readResponse.Resource.Name.Should().Be("Patched");
        readResponse.Resource.Value.Should().Be(15);
    }

    [Fact]
    public async Task Batch_MixedOperations_AllExecuteInSequence()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "existing", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "new1", PartitionKey = "pk1", Name = "New" });
        batch.ReplaceItem("existing", new TestDocument { Id = "existing", PartitionKey = "pk1", Name = "Replaced" });
        batch.ReadItem("existing");

        using var response = await batch.ExecuteAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Count.Should().Be(3);
    }

    [Fact]
    public async Task Batch_CreateItemStream_Works()
    {
        var doc = new TestDocument { Id = "stream1", PartitionKey = "pk1", Name = "Stream" };
        var json = JsonConvert.SerializeObject(doc);
        var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItemStream(stream);

        using var response = await batch.ExecuteAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var readResponse = await _container.ReadItemAsync<TestDocument>("stream1", new PartitionKey("pk1"));
        readResponse.Resource.Name.Should().Be("Stream");
    }

    [Fact]
    public async Task Batch_FluentApi_ChainsOperations()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"))
            .CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" })
            .CreateItem(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "B" });

        using var response = await batch.ExecuteAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Count.Should().Be(2);
    }

    // ── Atomicity / rollback tests ──

    [Fact]
    public async Task Batch_FailingOperation_RollsBackPreviousOperations()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Duplicate" });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeFalse();

        var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task Batch_FailingOperation_MarksEarlierOpsAsFailedDependency()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Duplicate" });

        using var response = await batch.ExecuteAsync();

        response[0].StatusCode.Should().Be(HttpStatusCode.FailedDependency);
        response[1].StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task Batch_ExceedingMaxOperations_ThrowsBadRequest()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        for (var i = 0; i < 101; i++)
        {
            batch.CreateItem(new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" });
        }

        var act = () => batch.ExecuteAsync();

        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task Batch_DeleteNonExistentItem_FailsAndRollsBack()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });
        batch.DeleteItem("nonexistent");

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeFalse();

        var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task Batch_UpsertItemStream_Works()
    {
        var doc = new TestDocument { Id = "stream1", PartitionKey = "pk1", Name = "UpsertStream" };
        var json = JsonConvert.SerializeObject(doc);
        var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.UpsertItemStream(stream);

        using var response = await batch.ExecuteAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var readResponse = await _container.ReadItemAsync<TestDocument>("stream1", new PartitionKey("pk1"));
        readResponse.Resource.Name.Should().Be("UpsertStream");
    }

    [Fact]
    public async Task Batch_ReplaceItemStream_Works()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var updated = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "StreamReplaced" };
        var json = JsonConvert.SerializeObject(updated);
        var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.ReplaceItemStream("1", stream);

        using var response = await batch.ExecuteAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        readResponse.Resource.Name.Should().Be("StreamReplaced");
    }
}
