using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Xunit;
using System.Text;
using Newtonsoft.Json.Linq;

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
        response[0].StatusCode.Should().Be(HttpStatusCode.OK);
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


public class TransactionalBatchGapTests2
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Batch_FailedOp_PriorOps_MarkedFailedDependency()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "first", PartitionKey = "pk1", Name = "First" });
        batch.ReadItem("nonexistent");

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeFalse();
        // First op should be marked FailedDependency after rollback
        response[0].StatusCode.Should().Be(HttpStatusCode.FailedDependency);
    }

    [Fact]
    public async Task Batch_ReplaceInBatch_NonExistent_Fails()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.ReplaceItem("nonexistent", new TestDocument { Id = "nonexistent", PartitionKey = "pk1", Name = "Bad" });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeFalse();
    }

    [Fact]
    public async Task Batch_DeleteInBatch_NonExistent_Fails()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.DeleteItem("nonexistent");

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeFalse();
    }

    [Fact]
    public async Task Batch_PatchInBatch_AppliesChanges()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original", Value = 10 },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.PatchItem("1", [PatchOperation.Set("/name", "Patched")]);

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();

        var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Patched");
    }

    [Fact]
    public async Task Batch_WithIfMatch_StaleETag_FailsBatch()
    {
        var create = await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.ReplaceItem("1",
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
            new TransactionalBatchItemRequestOptions { IfMatchEtag = "\"stale\"" });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeFalse();
        response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task Batch_EmptyBatch_Returns200()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}


public class TransactionalBatchGapTests3
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Batch_SubsequentOpsAfterFailure_Also424()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "first", PartitionKey = "pk1", Name = "A" });
        batch.ReadItem("nonexistent"); // This will fail (404)
        batch.CreateItem(new TestDocument { Id = "third", PartitionKey = "pk1", Name = "C" });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeFalse();
        // All ops marked as FailedDependency after rollback
        response[0].StatusCode.Should().Be(HttpStatusCode.FailedDependency);
        response[2].StatusCode.Should().Be(HttpStatusCode.FailedDependency);
    }

    [Fact]
    public async Task Batch_ReadResult_ContainsDocumentData()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.ReadItem("1");

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();
        var result = response.GetOperationResultAtIndex<TestDocument>(0);
        result.Resource.Should().NotBeNull();
        result.Resource.Name.Should().Be("Alice");
    }
}


public class TransactionalBatchEdgeCaseTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Batch_EmptyBatch_ExecuteAsync_Succeeds()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));

        using var response = await batch.ExecuteAsync();

        // An empty batch should succeed with no operations
        response.IsSuccessStatusCode.Should().BeTrue();
    }

    [Fact]
    public async Task Batch_PatchItem_WithIfMatch_Succeeds()
    {
        var createResponse = await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 10 },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.PatchItem("1", [PatchOperation.Set("/name", "Patched")],
            new TransactionalBatchPatchItemRequestOptions { IfMatchEtag = createResponse.ETag });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();
    }
}


public class TransactionalBatchGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Batch_FailingOperation_RollsBackPrevious()
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
    public async Task Batch_CreateDuplicate_InBatch_Fails409()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "dup", PartitionKey = "pk1", Name = "First" });
        batch.CreateItem(new TestDocument { Id = "dup", PartitionKey = "pk1", Name = "Second" });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeFalse();
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task Batch_Over100Operations_Throws()
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
    public async Task Batch_UpsertInBatch_CreatesOrReplaces()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "existing", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.UpsertItem(new TestDocument { Id = "existing", PartitionKey = "pk1", Name = "Updated" });
        batch.UpsertItem(new TestDocument { Id = "new-item", PartitionKey = "pk1", Name = "NewItem" });

        using var response = await batch.ExecuteAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var read1 = await _container.ReadItemAsync<TestDocument>("existing", new PartitionKey("pk1"));
        read1.Resource.Name.Should().Be("Updated");

        var read2 = await _container.ReadItemAsync<TestDocument>("new-item", new PartitionKey("pk1"));
        read2.Resource.Name.Should().Be("NewItem");
    }

    [Fact]
    public async Task Batch_Rollback_RestoresExactSnapshot()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "pre-existing", PartitionKey = "pk1", Name = "PreExisting", Value = 42 },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "will-rollback", PartitionKey = "pk1", Name = "New" });
        batch.ReplaceItem("nonexistent", new TestDocument { Id = "nonexistent", PartitionKey = "pk1", Name = "Bad" });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeFalse();

        var read = await _container.ReadItemAsync<TestDocument>("pre-existing", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("PreExisting");
        read.Resource.Value.Should().Be(42);

        var act = () => _container.ReadItemAsync<TestDocument>("will-rollback", new PartitionKey("pk1"));
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task Batch_OperationOrder_Matters()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Created" });
        batch.ReplaceItem("1", new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();

        var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Replaced");
    }
}


public class TransactionalBatchGapTests4
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Batch_SingleOp_BehavesLikeRegularOp()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Single" });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();
        response.Count.Should().Be(1);
        response[0].StatusCode.Should().Be(HttpStatusCode.Created);

        var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Single");
    }

    [Fact]
    public async Task Batch_WithRequestOptions_ExecuteAsync()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "WithOptions" });

        using var response = await batch.ExecuteAsync(new TransactionalBatchRequestOptions());

        response.IsSuccessStatusCode.Should().BeTrue();
        response.Count.Should().Be(1);

        var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("WithOptions");
    }

    [Fact]
    public async Task Batch_MaxDocumentSizePerBatch_Enforced()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        // Add many items that together exceed 2MB
        for (var i = 0; i < 10; i++)
        {
            var largeValue = new string('x', 300 * 1024); // 300KB each = 3MB total
            batch.CreateItem(new { id = $"{i}", partitionKey = "pk1", data = largeValue });
        }

        using var response = await batch.ExecuteAsync();
        response.IsSuccessStatusCode.Should().BeFalse();
    }
}


