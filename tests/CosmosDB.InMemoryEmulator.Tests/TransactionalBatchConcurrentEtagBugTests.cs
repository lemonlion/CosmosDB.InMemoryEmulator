using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Reproduces a bug where concurrent transactional batch operations with ETag-based
/// optimistic concurrency don't properly serialize within the same partition.
/// Real Cosmos DB serializes batch execution within a logical partition, ensuring
/// the second batch sees effects of the first and fails on stale ETags.
/// </summary>
public class TransactionalBatchConcurrentEtagBugTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task ConcurrentBatches_SameItem_ETagConflict_OneFailsOnePasses()
    {
        // Seed the item
        var doc = JObject.FromObject(new { id = "counter", pk = "a", count = 0 });
        await _container.CreateItemAsync(doc, new PartitionKey("a"));

        // Read the item to get initial ETag
        var readResponse = await _container.ReadItemAsync<JObject>("counter", new PartitionKey("a"));
        var initialEtag = readResponse.ETag;

        // Two concurrent batches both trying to replace the same item with the same stale ETag
        var batch1 = _container.CreateTransactionalBatch(new PartitionKey("a"));
        batch1.ReplaceItem("counter",
            JObject.FromObject(new { id = "counter", pk = "a", count = 1 }),
            new TransactionalBatchItemRequestOptions { IfMatchEtag = initialEtag });

        var batch2 = _container.CreateTransactionalBatch(new PartitionKey("a"));
        batch2.ReplaceItem("counter",
            JObject.FromObject(new { id = "counter", pk = "a", count = 2 }),
            new TransactionalBatchItemRequestOptions { IfMatchEtag = initialEtag });

        // Execute both concurrently
        var results = await Task.WhenAll(batch1.ExecuteAsync(), batch2.ExecuteAsync());

        // One should succeed, the other should fail with PreconditionFailed
        var statuses = results.Select(r => r.StatusCode).OrderBy(s => s).ToArray();
        var successCount = statuses.Count(s => s == HttpStatusCode.OK);
        var failCount = statuses.Count(s => s == HttpStatusCode.PreconditionFailed);

        successCount.Should().Be(1, "exactly one batch should succeed");
        failCount.Should().Be(1, "exactly one batch should fail with PreconditionFailed (412)");
    }

    [Fact]
    public async Task ConcurrentBatches_CreateSameItem_OneConflicts()
    {
        // Two concurrent batches both trying to create the same item
        var batch1 = _container.CreateTransactionalBatch(new PartitionKey("a"));
        batch1.CreateItem(JObject.FromObject(new { id = "unique-doc", pk = "a", version = 1 }));

        var batch2 = _container.CreateTransactionalBatch(new PartitionKey("a"));
        batch2.CreateItem(JObject.FromObject(new { id = "unique-doc", pk = "a", version = 2 }));

        var results = await Task.WhenAll(batch1.ExecuteAsync(), batch2.ExecuteAsync());

        var statuses = results.Select(r => r.StatusCode).OrderBy(s => s).ToArray();
        var successCount = statuses.Count(s => s == HttpStatusCode.OK || s == HttpStatusCode.Created);
        var conflictCount = statuses.Count(s => s == HttpStatusCode.Conflict);

        successCount.Should().Be(1, "exactly one batch should succeed");
        conflictCount.Should().Be(1, "exactly one batch should fail with Conflict (409)");
    }

    [Fact]
    public async Task ConcurrentBatches_AtomicCounter_IncrementPattern()
    {
        // Seed the "latest" counter doc
        var latest = JObject.FromObject(new { id = "latest", pk = "a", attemptNumber = 0 });
        await _container.CreateItemAsync(latest, new PartitionKey("a"));

        // Simulate 3 concurrent "read-modify-write" operations via batches
        async Task<int> IncrementWithRetry()
        {
            for (var retry = 0; retry < 10; retry++)
            {
                var read = await _container.ReadItemAsync<JObject>("latest", new PartitionKey("a"));
                var currentNumber = read.Resource["attemptNumber"]!.Value<int>();
                var newNumber = currentNumber + 1;
                var etag = read.ETag;

                var batch = _container.CreateTransactionalBatch(new PartitionKey("a"));
                batch.ReplaceItem("latest",
                    JObject.FromObject(new { id = "latest", pk = "a", attemptNumber = newNumber }),
                    new TransactionalBatchItemRequestOptions { IfMatchEtag = etag });
                batch.CreateItem(JObject.FromObject(new { id = $"attempt-{newNumber}", pk = "a", number = newNumber }));

                var result = await batch.ExecuteAsync();
                if (result.IsSuccessStatusCode)
                    return newNumber;

                // Retry on conflict/precondition failed
                if (result.StatusCode is HttpStatusCode.Conflict or HttpStatusCode.PreconditionFailed)
                    continue;

                throw new Exception($"Unexpected batch status: {result.StatusCode}");
            }

            throw new Exception("Too many retries");
        }

        var tasks = Enumerable.Range(0, 3).Select(_ => IncrementWithRetry()).ToArray();
        var results = await Task.WhenAll(tasks);

        var sorted = results.OrderBy(x => x).ToArray();
        sorted.Should().BeEquivalentTo([1, 2, 3],
            "transactional batch with ETag should prevent duplicate attempt numbers");
    }
}
