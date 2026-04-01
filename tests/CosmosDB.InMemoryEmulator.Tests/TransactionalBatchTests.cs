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


// ── Category 1: Status Code Correctness ──

public class TransactionalBatchStatusCodeTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Batch_CreateItem_Returns201Created()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();
        response[0].StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task Batch_ReadItem_Returns200OK()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.ReadItem("1");

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();
        response[0].StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Batch_ReplaceItem_Returns200OK()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.ReplaceItem("1", new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();
        response[0].StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Batch_DeleteItem_Returns204NoContent()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.DeleteItem("1");

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();
        response[0].StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task Batch_UpsertItem_NewItem_Returns201Created()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.UpsertItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();
        response[0].StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task Batch_UpsertItem_ExistingItem_Returns200OK()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.UpsertItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();
        response[0].StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Batch_PatchItem_Returns200OK()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.PatchItem("1", [PatchOperation.Set("/name", "Patched")]);

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();
        response[0].StatusCode.Should().Be(HttpStatusCode.OK);
    }
}


// ── Category 2: Rollback Integrity ──

public class TransactionalBatchRollbackIntegrityTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Batch_FailedRollback_RestoresTimestamps()
    {
        // Pre-existing item count for reference
        _container.DefaultTimeToLive = 3600;

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "will-rollback", PartitionKey = "pk1", Name = "Ghost" });
        batch.ReadItem("nonexistent"); // Will fail → rollback

        using var response = await batch.ExecuteAsync();
        response.IsSuccessStatusCode.Should().BeFalse();

        // The rolled-back item should not exist, and critically its timestamp should not leak.
        // If timestamps leak, a future item with the same key could inherit a stale timestamp
        // and expire prematurely via TTL.
        var act = () => _container.ReadItemAsync<TestDocument>("will-rollback", new PartitionKey("pk1"));
        await act.Should().ThrowAsync<CosmosException>();

        // Now create the item fresh — it should get a fresh timestamp, not a leaked one
        await _container.CreateItemAsync(
            new TestDocument { Id = "will-rollback", PartitionKey = "pk1", Name = "Real" },
            new PartitionKey("pk1"));

        // Item should be readable (not expired from a leaked old timestamp)
        var read = await _container.ReadItemAsync<TestDocument>("will-rollback", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Real");
    }

    [Fact]
    public async Task Batch_FailedRollback_DoesNotLeakChangeFeedEntries()
    {
        // Get initial change feed state
        var iteratorBefore = _container.GetChangeFeedIterator<TestDocument>(
            ChangeFeedStartFrom.Beginning(),
            ChangeFeedMode.Incremental);
        var beforeItems = new List<TestDocument>();
        while (iteratorBefore.HasMoreResults)
        {
            var feedResponse = await iteratorBefore.ReadNextAsync();
            if (feedResponse.StatusCode == HttpStatusCode.NotModified) break;
            beforeItems.AddRange(feedResponse);
        }

        // Execute a batch that will fail
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "ghost1", PartitionKey = "pk1", Name = "Ghost1" });
        batch.CreateItem(new TestDocument { Id = "ghost2", PartitionKey = "pk1", Name = "Ghost2" });
        batch.ReadItem("nonexistent"); // Will fail → rollback

        using var response = await batch.ExecuteAsync();
        response.IsSuccessStatusCode.Should().BeFalse();

        // Change feed should NOT contain phantom entries for ghost1/ghost2
        var iteratorAfter = _container.GetChangeFeedIterator<TestDocument>(
            ChangeFeedStartFrom.Beginning(),
            ChangeFeedMode.Incremental);
        var afterItems = new List<TestDocument>();
        while (iteratorAfter.HasMoreResults)
        {
            var feedResponse = await iteratorAfter.ReadNextAsync();
            if (feedResponse.StatusCode == HttpStatusCode.NotModified) break;
            afterItems.AddRange(feedResponse);
        }

        afterItems.Count.Should().Be(beforeItems.Count,
            "change feed should not contain phantom entries from a rolled-back batch");
    }
}


// ── Category 3: Request Options Passthrough ──

public class TransactionalBatchRequestOptionsTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Batch_PatchItem_WithIfMatchEtag_StaleEtag_FailsBatch()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original", Value = 10 },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.PatchItem("1", [PatchOperation.Set("/name", "Patched")],
            new TransactionalBatchPatchItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeFalse();
        response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task Batch_PatchItem_WithIfMatchEtag_CurrentEtag_Succeeds()
    {
        var createResponse = await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original", Value = 10 },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.PatchItem("1", [PatchOperation.Set("/name", "Patched")],
            new TransactionalBatchPatchItemRequestOptions { IfMatchEtag = createResponse.ETag });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();

        var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Patched");
    }
}


// ── Category 4: Size Limit Enforcement ──

public class TransactionalBatchSizeLimitTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Batch_StreamOperations_ContributeToSizeLimit()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        for (var i = 0; i < 10; i++)
        {
            var largeValue = new string('x', 300 * 1024); // 300KB each = 3MB total
            var json = JsonConvert.SerializeObject(new { id = $"s{i}", partitionKey = "pk1", data = largeValue });
            var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
            batch.CreateItemStream(stream);
        }

        using var response = await batch.ExecuteAsync();
        response.IsSuccessStatusCode.Should().BeFalse();
        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Batch_Exactly2MB_Succeeds()
    {
        // Create a payload that is exactly at the 2MB limit
        // Overhead from JSON field names + structure ~200 bytes
        // We want total batch size <= 2MB = 2,097,152 bytes
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        var dataSize = (2 * 1024 * 1024) - 200; // Leave room for JSON overhead
        var data = new string('a', dataSize);
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = data });

        // This should succeed or be just under the limit
        using var response = await batch.ExecuteAsync();
        // If it's under 2MB it succeeds; the point is it doesn't wrongly fail
        response.IsSuccessStatusCode.Should().BeTrue();
    }

    [Fact]
    public async Task Batch_JustOver2MB_FailsEntityTooLarge()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        // Create a single item that is clearly over 2MB in batch payload
        var data = new string('a', 2 * 1024 * 1024 + 1000);
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = data });

        using var response = await batch.ExecuteAsync();
        response.IsSuccessStatusCode.Should().BeFalse();
        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }
}


// ── Category 5: GetEnumerator / IEnumerable ──

public class TransactionalBatchEnumerationTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Batch_Response_CanBeEnumerated_WithForeach()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" });
        batch.CreateItem(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "B" });
        batch.CreateItem(new TestDocument { Id = "3", PartitionKey = "pk1", Name = "C" });

        using var response = await batch.ExecuteAsync();

        var results = new List<TransactionalBatchOperationResult>();
        foreach (var result in response)
        {
            results.Add(result);
        }

        results.Count.Should().Be(3);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
    }
}


// ── Category 6: Operation Result Metadata ──

public class TransactionalBatchResultMetadataTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Batch_CreateResult_HasETag()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();
        response[0].ETag.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task Batch_ReplaceResult_HasETag()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.ReplaceItem("1", new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();
        response[0].ETag.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task Batch_UpsertResult_HasETag()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.UpsertItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();
        response[0].ETag.Should().NotBeNullOrEmpty();
    }
}


// ── Category 7: Intra-Batch Operation Sequences ──

public class TransactionalBatchIntraBatchSequenceTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Batch_CreateThenRead_SameItemInBatch()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });
        batch.ReadItem("1");

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();
        response.Count.Should().Be(2);

        var readResult = response.GetOperationResultAtIndex<TestDocument>(1);
        readResult.Resource.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task Batch_CreateThenDelete_SameItemInBatch()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });
        batch.DeleteItem("1");

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();

        var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task Batch_DeleteThenCreate_SameId_InBatch()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.DeleteItem("1");
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Recreated" });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();

        var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Recreated");
    }

    [Fact]
    public async Task Batch_UpsertThenPatch_SameItemInBatch()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.UpsertItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 });
        batch.PatchItem("1", [PatchOperation.Set("/name", "Patched"), PatchOperation.Increment("/value", 5)]);

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();

        var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Patched");
        read.Resource.Value.Should().Be(15);
    }

    [Fact]
    public async Task Batch_MultiplePatches_SameItemInBatch()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 0 },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.PatchItem("1", [PatchOperation.Increment("/value", 10)]);
        batch.PatchItem("1", [PatchOperation.Increment("/value", 20)]);
        batch.PatchItem("1", [PatchOperation.Set("/name", "Triple-Patched")]);

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();

        var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Value.Should().Be(30);
        read.Resource.Name.Should().Be("Triple-Patched");
    }

    [Fact]
    public async Task Batch_CreateThenUpsert_SameItemInBatch()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Created" });
        batch.UpsertItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Upserted" });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();

        var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Upserted");
    }
}


// ── Category 8: Boundary Conditions ──

public class TransactionalBatchBoundaryTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Batch_Exactly100Operations_Succeeds()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        for (var i = 0; i < 100; i++)
        {
            batch.CreateItem(new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" });
        }

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();
        response.Count.Should().Be(100);
    }

    [Fact]
    public async Task Batch_ResponseIndexer_OutOfRange_ReturnsNull()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

        using var response = await batch.ExecuteAsync();

        // Accessing beyond the count should return null (not throw)
        var outOfRange = response[response.Count];
        outOfRange.Should().BeNull();
    }

    [Fact]
    public async Task Batch_ResponseIndexer_NegativeIndex_ReturnsNull()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

        using var response = await batch.ExecuteAsync();

        var negativeIndex = response[-1];
        negativeIndex.Should().BeNull();
    }
}


// ── Category 9: Concurrent and Re-execution ──

public class TransactionalBatchExecutionTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Batch_TwoBatches_SamePartition_Sequential_BothSucceed()
    {
        var batch1 = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch1.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "First" });
        using var response1 = await batch1.ExecuteAsync();
        response1.IsSuccessStatusCode.Should().BeTrue();

        var batch2 = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch2.CreateItem(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Second" });
        using var response2 = await batch2.ExecuteAsync();
        response2.IsSuccessStatusCode.Should().BeTrue();

        var read1 = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read1.Resource.Name.Should().Be("First");
        var read2 = await _container.ReadItemAsync<TestDocument>("2", new PartitionKey("pk1"));
        read2.Resource.Name.Should().Be("Second");
    }
}


// ── Category 10: Partition Key Enforcement ──

public class TransactionalBatchPartitionKeyTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Batch_AllOperations_UseBatchPartitionKey()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

        using var response = await batch.ExecuteAsync();
        response.IsSuccessStatusCode.Should().BeTrue();

        // Item should be readable with the batch's partition key
        var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Alice");

        // Item should NOT be readable with a different partition key
        var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk2"));
        await act.Should().ThrowAsync<CosmosException>();
    }
}


// ── Category 11: Response Properties ──

public class TransactionalBatchResponsePropertiesTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Batch_Response_RequestCharge_IsPopulated()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

        using var response = await batch.ExecuteAsync();

        response.RequestCharge.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task Batch_FailedResponse_RequestCharge_IsPopulated()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.ReadItem("nonexistent");

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeFalse();
        response.RequestCharge.Should().BeGreaterThan(0);
    }
}


// ── Skipped Tests + Divergent Behaviour Tests ──

public class TransactionalBatchDivergentBehaviourTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact(Skip = "ResourceStream is an abstract property on TransactionalBatchOperationResult. " +
                 "The InMemoryBatchResponse uses NSubstitute to mock operation results, and wiring up " +
                 "ResourceStream to return coherent MemoryStream instances for every write operation would " +
                 "require significant refactoring to use concrete result objects instead of mocks. " +
                 "Real Cosmos DB populates ResourceStream with the serialized document body for writes and reads.")]
    public async Task Batch_ResourceStream_AvailableOnResults()
    {
        // Real Cosmos DB: ResourceStream on each operation result contains the serialized document body.
        // This test would verify that Create/Replace/Upsert/Read results have a non-null ResourceStream
        // containing valid JSON that deserializes to the correct document.
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

        using var response = await batch.ExecuteAsync();

        // Would assert: response[0].ResourceStream.Should().NotBeNull();
        // Would assert: reading the stream yields {"id":"1","partitionKey":"pk1","name":"Alice",...}
        await Task.CompletedTask;
    }

    // Sister test documenting actual in-memory behaviour for ResourceStream
    [Fact]
    public async Task Batch_ResourceStream_InMemory_IsNotPopulatedOnMockedResults()
    {
        // InMemoryBatchResponse uses NSubstitute mocks for TransactionalBatchOperationResult.
        // NSubstitute returns default(Stream) = null for ResourceStream since it's not configured.
        // This documents the known divergence from real Cosmos DB where ResourceStream is always populated.
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();
        // In the real SDK, ResourceStream would be non-null.
        // In the emulator, it's null because NSubstitute returns default for unmocked properties.
        response[0].ResourceStream.Should().BeNull();
    }

    [Fact(Skip = "CancellationToken is accepted by ExecuteAsync but not propagated to individual container " +
                 "operations. Implementing this would require threading the token through each operation lambda " +
                 "and checking it between operations. The real Cosmos SDK checks cancellation between operations " +
                 "and throws OperationCanceledException.")]
    public async Task Batch_CancellationToken_Respected()
    {
        // Real Cosmos DB: Passing a cancelled CancellationToken to ExecuteAsync throws OperationCanceledException.
        // The SDK also checks the token between individual batch operations.
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // Would assert: await batch.ExecuteAsync(cts.Token) throws OperationCanceledException
        await Task.CompletedTask;
    }

    // Sister test documenting actual in-memory behaviour for CancellationToken
    [Fact]
    public async Task Batch_CancellationToken_NotCheckedBetweenOperations()
    {
        // The InMemoryTransactionalBatch accepts a CancellationToken on ExecuteAsync but does not
        // propagate it to individual operations or check it between operations. A pre-cancelled token
        // still allows the batch to complete successfully. This is a known divergence from real Cosmos DB.
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // In the emulator, the batch completes despite the cancelled token
        using var response = await batch.ExecuteAsync(cts.Token);
        response.IsSuccessStatusCode.Should().BeTrue();

        var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Alice");
    }
}

