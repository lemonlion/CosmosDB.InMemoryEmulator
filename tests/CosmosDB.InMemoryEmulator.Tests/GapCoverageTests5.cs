using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ════════════════════════════════════════════════════════════════════════════════
// GapCoverageTests5 — Container API documentation gap coverage tests
// Based on deep analysis of the Microsoft.Azure.Cosmos.Container API docs:
// https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container
// See container-api-test-gap-plan.md for full gap analysis
//
// NOTE: Many gaps identified in the plan are already covered by
// CosmosClientApiGapTests.cs (EnableContentResponseOnWrite, Throughput,
// Stream ETag handling, FeedRange overloads, ChangeFeedEstimator, ReadMany
// edge cases). This file covers the remaining unique gaps only.
// ════════════════════════════════════════════════════════════════════════════════

#region GAP 2: System metadata properties (_ts, _etag in response body)

/// <summary>
/// Validates that system metadata properties (<c>_ts</c>, <c>_etag</c>) are available
/// in the response body when reading items, as documented in the API remarks:
/// "Items contain meta data that can be obtained by mapping these meta data attributes
/// to properties in T."
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.readitemasync
/// </summary>
public class SystemMetadataPropertyTests5
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Read_ResponseBody_Contains_Ts_SystemProperty()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var response = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));

        // Real Cosmos DB includes _ts (Unix epoch seconds) in the response body
        response.Resource["_ts"].Should().NotBeNull();
        response.Resource["_ts"]!.Type.Should().Be(JTokenType.Integer);
        response.Resource["_ts"]!.Value<long>().Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task Read_ResponseBody_Contains_Etag_SystemProperty()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var response = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));

        // Real Cosmos DB includes _etag in the response body
        response.Resource["_etag"].Should().NotBeNull();
        response.Resource["_etag"]!.Type.Should().Be(JTokenType.String);
        response.Resource["_etag"]!.ToString().Should().NotBeEmpty();
    }

    [Fact]
    public async Task Read_Ts_UpdatedOnReplace()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var readBefore = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
        var tsBefore = readBefore.Resource["_ts"]?.Value<long>() ?? 0;

        // Small delay to ensure timestamp changes
        await Task.Delay(10);

        await _container.ReplaceItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" },
            "1", new PartitionKey("pk1"));

        var readAfter = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
        var tsAfter = readAfter.Resource["_ts"]?.Value<long>() ?? 0;

        tsAfter.Should().BeGreaterThanOrEqualTo(tsBefore);
    }
}

#endregion

#region GAP 3: Replace — Partition key immutability

/// <summary>
/// Validates that the partition key value is immutable during Replace operations.
/// Per API docs: "The item's partition key value is immutable. To change an item's
/// partition key value you must delete the original item and insert a new item."
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.replaceitemasync
/// </summary>
public class ReplacePartitionKeyImmutabilityTests5
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Replace_WithDifferentPkInBody_ItemStaysInOriginalPartition()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        // Replace with a different partition key value in the body
        var replacement = new TestDocument { Id = "1", PartitionKey = "pk2", Name = "Replaced" };
        await _container.ReplaceItemAsync(replacement, "1", new PartitionKey("pk1"));

        // Item is still accessible with original PK
        var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Replaced");
    }
}

#endregion

#region GAP 4: Patch — Partition key immutability

/// <summary>
/// Validates that the partition key value is immutable during Patch operations.
/// Per PatchItemStreamAsync docs: "The item's partition key value is immutable."
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.patchitemasync
/// </summary>
public class PatchPartitionKeyImmutabilityTests5
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Patch_Set_PartitionKeyField_ItemStaysInOriginalPartition()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        // Patch the partition key field to a new value
        await _container.PatchItemAsync<TestDocument>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/partitionKey", "pk2")]);

        // Item is still accessible with original PK
        var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Original");
    }
}

#endregion

#region GAP 5: Stream Delete — stale ETag returns 412 (not throw)

/// <summary>
/// Validates the stream API contract specifically for DeleteItemStreamAsync with stale ETag.
/// The Upsert/Patch/Replace variants are already tested in CosmosClientApiGapTests.cs
/// (StreamETagHandlingTests). This covers the missing Delete variant.
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.deleteitemstreamasync
/// </summary>
public class StreamDeleteETagTests5
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task DeleteItemStream_WithIfMatch_StaleETag_Returns412_DoesNotThrow()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

        var response = await _container.DeleteItemStreamAsync(
            "1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

        response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }
}

#endregion

#region GAP 8: GetItemLinqQueryable — continuationToken and linqSerializerOptions

/// <summary>
/// Validates that GetItemLinqQueryable can accept a continuation token and
/// CosmosLinqSerializerOptions without throwing, even though InMemoryContainer
/// ignores both parameters.
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.getitemlinqqueryable
/// </summary>
public class LinqQueryableParameterTests5
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Linq_WithContinuationToken_DoesNotThrow()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        // Should not throw when passing a continuation token
        var queryable = _container.GetItemLinqQueryable<TestDocument>(
            allowSynchronousQueryExecution: true,
            continuationToken: "some-token");

        queryable.ToList().Should().ContainSingle();
    }

    [Fact]
    public async Task Linq_WithLinqSerializerOptions_DoesNotThrow()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var options = new CosmosLinqSerializerOptions
        {
            PropertyNamingPolicy = CosmosPropertyNamingPolicy.CamelCase
        };

        // Should not throw when passing serializer options
        var queryable = _container.GetItemLinqQueryable<TestDocument>(
            allowSynchronousQueryExecution: true,
            linqSerializerOptions: options);

        queryable.ToList().Should().ContainSingle();
    }
}

#endregion

#region GAP 11: ReplaceContainerStreamAsync — Divergent behavior

/// <summary>
/// BEHAVIORAL DIFFERENCE: Real Cosmos DB ReplaceContainerStreamAsync updates the
/// container's actual properties. InMemoryContainer returns the supplied properties
/// in the response but does not persist them internally. Subsequent ReadContainerAsync
/// calls return the original properties. The basic ReplaceContainerStreamAsync_ReturnsOk
/// test is already in CosmosClientApiGapTests.cs; this covers the persistence gap.
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.replacecontainerstreamasync
/// </summary>
public class ContainerStreamDivergentBehaviorTests5
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task ReplaceContainerStream_DoesNotPersistChanges()
    {
        var newProperties = new ContainerProperties("new-name", "/newPk");
        await _container.ReplaceContainerStreamAsync(newProperties);

        var readResponse = await _container.ReadContainerAsync();

        // InMemoryContainer still returns original properties
        readResponse.Resource.Id.Should().Be("test-container");
        readResponse.Resource.PartitionKeyPath.Should().Be("/partitionKey");
    }
}

#endregion

#region GAP 12: Conflicts property

/// <summary>
/// Validates that the Conflicts property on the Container is accessible and returns a
/// non-null instance. InMemoryContainer returns an NSubstitute mock.
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.conflicts
/// </summary>
public class ContainerConflictsPropertyTests5
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public void Conflicts_Property_ReturnsNonNull()
    {
        _container.Conflicts.Should().NotBeNull();
    }
}

#endregion

#region GAP 13: Delete edge cases

/// <summary>
/// Validates edge cases for DeleteItemAsync, specifically that IfNoneMatchEtag
/// (which is meaningful for reads, not writes) does not interfere with delete.
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.deleteitemasync
/// </summary>
public class DeleteEdgeCaseTests5
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Delete_WithIfNoneMatchEtag_IsIgnored_DeleteSucceeds()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        // IfNoneMatch is for conditional reads, not deletes. Should be ignored.
        var response = await _container.DeleteItemAsync<TestDocument>(
            "1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfNoneMatchEtag = "\"some-etag\"" });

        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }
}

#endregion

#region GAP 14: CancellationToken support

/// <summary>
/// Validates that CancellationToken is respected across Container operations.
/// All methods accept a CancellationToken and should throw OperationCanceledException
/// when the token is already cancelled.
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.createitemasync
/// </summary>
public class CancellationTokenTests5
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task CreateItem_WithCancelledToken_ThrowsOperationCancelled()
    {
        var cts = new CancellationTokenSource();
        cts.Cancel();

        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        var act = () => _container.CreateItemAsync(item, new PartitionKey("pk1"),
            cancellationToken: cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task ReadItem_WithCancelledToken_ThrowsOperationCancelled()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var cts = new CancellationTokenSource();
        cts.Cancel();

        var act = () => _container.ReadItemAsync<TestDocument>(
            "1", new PartitionKey("pk1"), cancellationToken: cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task UpsertItem_WithCancelledToken_ThrowsOperationCancelled()
    {
        var cts = new CancellationTokenSource();
        cts.Cancel();

        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        var act = () => _container.UpsertItemAsync(item, new PartitionKey("pk1"),
            cancellationToken: cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task DeleteItem_WithCancelledToken_ThrowsOperationCancelled()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var cts = new CancellationTokenSource();
        cts.Cancel();

        var act = () => _container.DeleteItemAsync<TestDocument>(
            "1", new PartitionKey("pk1"), cancellationToken: cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task PatchItem_WithCancelledToken_ThrowsOperationCancelled()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var cts = new CancellationTokenSource();
        cts.Cancel();

        var act = () => _container.PatchItemAsync<TestDocument>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Patched")],
            cancellationToken: cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task CreateItemStream_WithCancelledToken_ThrowsOperationCancelled()
    {
        var cts = new CancellationTokenSource();
        cts.Cancel();

        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        var act = () => _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(json)),
            new PartitionKey("pk1"), cancellationToken: cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }
}

#endregion

#region GAP 17: Patch operations — atomicity on failure

/// <summary>
/// Validates that patch operations are atomic — if one operation in a batch fails,
/// none of the operations should be applied to the item.
/// Per API docs: "The patch operations are atomic and are executed sequentially."
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.patchitemasync
/// </summary>
public class PatchAtomicityTests5
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Patch_MultipleOps_IfOneFailsAllRollBack()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original", Value = 10 },
            new PartitionKey("pk1"));

        // Op 1: Set /name to "Changed" (would succeed in isolation)
        // Op 2: Increment /name by 1 (will fail because /name is now a string "Changed")
        var act = () => _container.PatchItemAsync<TestDocument>(
            "1", new PartitionKey("pk1"),
            [
                PatchOperation.Set("/name", "Changed"),
                PatchOperation.Increment("/name", 1),
            ]);

        // Should throw because op 2 fails (can't increment a string)
        await act.Should().ThrowAsync<Exception>();

        // Item should be unchanged — atomicity means op 1 was rolled back
        var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Original");
        read.Resource.Value.Should().Be(10);
    }
}

#endregion

#region GAP 18: ChangeFeed stream processor builder — Divergent behavior

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

    [Fact(Skip = "InMemoryContainer uses NoOpChangeFeedProcessor for the stream handler variant. " +
                 "The handler will never be invoked. Use the typed ChangeFeedHandler<T> overload " +
                 "for functional in-memory change feed processing. " +
                 "See sister test: ChangeFeedStreamHandler_IsNoOp_InMemory")]
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

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: Real Cosmos DB invokes the ChangeFeedStreamHandler with
    /// raw Stream data when changes are detected. InMemoryContainer uses a NoOp processor
    /// for the stream handler variant — it builds and starts successfully but the handler
    /// is never invoked. Use the typed <c>ChangeFeedHandler&lt;T&gt;</c> overload instead
    /// for functional in-memory change feed processing.
    /// </summary>
    [Fact]
    public async Task ChangeFeedStreamHandler_IsNoOp_InMemory()
    {
        var handlerInvoked = false;
        Container.ChangeFeedStreamHandler handler =
            (ChangeFeedProcessorContext context, Stream changes, CancellationToken token) =>
            {
                handlerInvoked = true;
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
        await Task.Delay(TimeSpan.FromMilliseconds(500));
        await processor.StopAsync();

        // Handler is never called because NoOpChangeFeedProcessor is used
        handlerInvoked.Should().BeFalse();
    }
}

#endregion

#region GAP 19: ReadManyItemsStreamAsync — Edge cases

/// <summary>
/// Tests edge cases for ReadManyItemsStreamAsync.
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.readmanyitemsstreamasync
/// </summary>
public class ReadManyStreamEdgeCaseTests5
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task ReadManyStream_EmptyList_ReturnsOkWithEmptyDocuments()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var emptyList = new List<(string id, PartitionKey pk)>();

        using var response = await _container.ReadManyItemsStreamAsync(emptyList);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        using var reader = new StreamReader(response.Content);
        var body = await reader.ReadToEndAsync();
        var jObj = JObject.Parse(body);
        ((JArray)jObj["Documents"]!).Should().BeEmpty();
    }

    [Fact]
    public async Task ReadManyStream_AllMissing_ReturnsOkWithEmptyDocuments()
    {
        var items = new List<(string id, PartitionKey pk)>
        {
            ("nonexistent1", new PartitionKey("pk1")),
            ("nonexistent2", new PartitionKey("pk2")),
        };

        using var response = await _container.ReadManyItemsStreamAsync(items);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        using var reader = new StreamReader(response.Content);
        var body = await reader.ReadToEndAsync();
        var jObj = JObject.Parse(body);
        ((JArray)jObj["Documents"]!).Should().BeEmpty();
    }
}

#endregion

#region GAP 20: Query continuation token — Edge cases

/// <summary>
/// Tests edge cases for continuation tokens in GetItemQueryIterator.
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.getitemqueryiterator
/// </summary>
public class QueryContinuationTokenEdgeCaseTests5
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task QueryIterator_WithInvalidContinuationToken_HandlesGracefully()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        // Invalid continuation token — should not crash, may return all or empty results
        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c",
            continuationToken: "not-a-valid-token");

        var act = async () =>
        {
            while (iterator.HasMoreResults)
                await iterator.ReadNextAsync();
        };

        // Should not throw — graceful handling of invalid tokens
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task QueryIterator_WithQueryDefinition_AndContinuationToken_Works()
    {
        for (var i = 1; i <= 5; i++)
            await _container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}", Value = i },
                new PartitionKey("pk1"));

        // Read first page with MaxItemCount=2
        var iterator1 = _container.GetItemQueryIterator<TestDocument>(
            new QueryDefinition("SELECT * FROM c ORDER BY c.value"),
            requestOptions: new QueryRequestOptions { MaxItemCount = 2 });

        var page1 = await iterator1.ReadNextAsync();
        var token = page1.ContinuationToken;

        page1.Should().HaveCount(2);

        // Resume from continuation token with QueryDefinition
        var iterator2 = _container.GetItemQueryIterator<TestDocument>(
            new QueryDefinition("SELECT * FROM c ORDER BY c.value"),
            continuationToken: token,
            requestOptions: new QueryRequestOptions { MaxItemCount = 2 });

        var remaining = new List<TestDocument>();
        while (iterator2.HasMoreResults)
        {
            var page = await iterator2.ReadNextAsync();
            remaining.AddRange(page);
        }

        remaining.Should().HaveCount(3);
    }
}

#endregion
