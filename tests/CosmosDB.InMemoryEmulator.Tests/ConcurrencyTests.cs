using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using System.Net;
using System.Text;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;


public class ConcurrencyGapTests3
{
    [Fact]
    public async Task ConcurrentBatchOperations_Isolation()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        // Pre-create items for batch operations
        for (var i = 0; i < 20; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"pre-{i}", PartitionKey = "pk1", Name = $"Pre{i}" },
                new PartitionKey("pk1"));

        var batchTasks = Enumerable.Range(0, 5).Select(async batchIndex =>
        {
            var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
            batch.CreateItem(new TestDocument { Id = $"batch-{batchIndex}", PartitionKey = "pk1", Name = $"Batch{batchIndex}" });
            using var response = await batch.ExecuteAsync();
            return response.IsSuccessStatusCode;
        });

        var results = await Task.WhenAll(batchTasks);
        results.Should().OnlyContain(success => success);
    }

    [Fact]
    public async Task ConcurrentChangeFeedRead_ThreadSafe()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        for (var i = 0; i < 50; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));

        var tasks = Enumerable.Range(0, 10).Select(async _ =>
        {
            var checkpoint = container.GetChangeFeedCheckpoint() - 10;
            if (checkpoint < 0) checkpoint = 0;
            var iterator = container.GetChangeFeedIterator<TestDocument>(checkpoint);
            var results = new List<TestDocument>();
            while (iterator.HasMoreResults)
            {
                var page = await iterator.ReadNextAsync();
                results.AddRange(page);
            }

            results.Should().NotBeEmpty();
        });

        await Task.WhenAll(tasks);
    }
}


public class ConcurrencyGapTests
{
    [Fact]
    public async Task ConcurrentCreates_DifferentIds_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var tasks = Enumerable.Range(0, 100).Select(i =>
            container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1")));

        var results = await Task.WhenAll(tasks);

        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
        container.ItemCount.Should().Be(100);
    }

    [Fact]
    public async Task ConcurrentCreates_SameId_ExactlyOneSucceeds()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var successes = 0;
        var failures = 0;

        var tasks = Enumerable.Range(0, 50).Select(async i =>
        {
            try
            {
                await container.CreateItemAsync(
                    new TestDocument { Id = "same", PartitionKey = "pk1", Name = $"Item{i}" },
                    new PartitionKey("pk1"));
                Interlocked.Increment(ref successes);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
            {
                Interlocked.Increment(ref failures);
            }
        });

        await Task.WhenAll(tasks);

        successes.Should().Be(1);
        failures.Should().Be(49);
    }

    [Fact]
    public async Task ConcurrentReads_SameItem_AllReturnSameData()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var tasks = Enumerable.Range(0, 100).Select(_ =>
            container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1")));

        var results = await Task.WhenAll(tasks);

        results.Should().OnlyContain(r => r.Resource.Name == "Alice");
    }

    [Fact]
    public async Task ConcurrentUpserts_SameItem_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var tasks = Enumerable.Range(0, 100).Select(i =>
            container.UpsertItemAsync(
                new TestDocument { Id = "1", PartitionKey = "pk1", Name = $"Version{i}" },
                new PartitionKey("pk1")));

        var results = await Task.WhenAll(tasks);

        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
    }
}


public class ConcurrencyGapTests2
{
    [Fact]
    public async Task ConcurrentCreateAndRead_NoCorruption()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        // Pre-create items
        for (var i = 0; i < 50; i++)
        {
            await container.CreateItemAsync(
                new TestDocument { Id = $"pre-{i}", PartitionKey = "pk1", Name = $"Pre{i}" },
                new PartitionKey("pk1"));
        }

        // Concurrent writes and reads
        var writeTasks = Enumerable.Range(50, 50)
            .Select(i => container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1")));

        var readTasks = Enumerable.Range(0, 50)
            .Select(i => container.ReadItemAsync<TestDocument>($"pre-{i}", new PartitionKey("pk1")));

        var allTasks = writeTasks.Cast<Task>().Concat(readTasks.Cast<Task>());
        await Task.WhenAll(allTasks);

        container.ItemCount.Should().Be(100);
    }

    [Fact]
    public async Task ConcurrentQueryAndWrite_NoCorruption()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        for (var i = 0; i < 50; i++)
        {
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));
        }

        var writeTasks = Enumerable.Range(50, 50)
            .Select(i => container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"New{i}" },
                new PartitionKey("pk1")));

        var queryTasks = Enumerable.Range(0, 10).Select(async _ =>
        {
            var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
            var results = new List<TestDocument>();
            while (iterator.HasMoreResults)
            {
                var page = await iterator.ReadNextAsync();
                results.AddRange(page);
            }

            results.Should().NotBeEmpty();
        });

        await Task.WhenAll(writeTasks.Cast<Task>().Concat(queryTasks));
    }
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Category 1: Concurrent Delete Scenarios
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentDeleteTests
{
    [Fact]
    public async Task ConcurrentDeletes_SameItem_AllCompleteWithoutCrash()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Target" },
            new PartitionKey("pk1"));

        var tasks = Enumerable.Range(0, 50).Select(async _ =>
        {
            try
            {
                await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));
                return "deleted";
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                return "notfound";
            }
        });

        var results = await Task.WhenAll(tasks);
        results.Should().Contain("deleted");
        container.ItemCount.Should().Be(0);
    }

    [Fact]
    public async Task ConcurrentDeleteAndRead_ReadsEitherSucceedOrGetNotFound()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        for (var i = 0; i < 100; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));

        var deleteTasks = Enumerable.Range(0, 50).Select(async i =>
        {
            try { await container.DeleteItemAsync<TestDocument>($"{i}", new PartitionKey("pk1")); }
            catch (CosmosException) { }
        });

        var readTasks = Enumerable.Range(0, 50).Select(async i =>
        {
            try
            {
                var r = await container.ReadItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"));
                return r.StatusCode;
            }
            catch (CosmosException ex) { return ex.StatusCode; }
        });

        await Task.WhenAll(deleteTasks.Cast<Task>().Concat(readTasks.Select(async t => { await t; })));
    }

    [Fact]
    public async Task ConcurrentDeleteAndCreate_SameId_NoCorruption()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "x", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var tasks = Enumerable.Range(0, 100).Select(async i =>
        {
            try
            {
                if (i % 2 == 0)
                    await container.DeleteItemAsync<TestDocument>("x", new PartitionKey("pk1"));
                else
                    await container.CreateItemAsync(
                        new TestDocument { Id = "x", PartitionKey = "pk1", Name = $"V{i}" },
                        new PartitionKey("pk1"));
            }
            catch (CosmosException) { }
        });

        await Task.WhenAll(tasks);
        // Final state: item either exists or doesn't, but no corruption
        container.ItemCount.Should().BeLessThanOrEqualTo(1);
    }

    [Fact]
    public async Task ConcurrentDeleteAndUpsert_SameItem_StateIsConsistent()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var tasks = Enumerable.Range(0, 100).Select(async i =>
        {
            try
            {
                if (i % 2 == 0)
                    await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));
                else
                    await container.UpsertItemAsync(
                        new TestDocument { Id = "1", PartitionKey = "pk1", Name = $"V{i}" },
                        new PartitionKey("pk1"));
            }
            catch (CosmosException) { }
        });

        await Task.WhenAll(tasks);
        container.ItemCount.Should().BeLessThanOrEqualTo(1);
    }
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Category 2: Concurrent Patch Scenarios
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentPatchTests
{
    [Fact]
    public async Task ConcurrentPatches_SameItem_AllSucceed_LastWriteWins()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var tasks = Enumerable.Range(0, 50).Select(i =>
            container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
                new[] { PatchOperation.Set("/name", $"Patched{i}") }));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);

        var final = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        final.Resource.Name.Should().StartWith("Patched");
    }

    [Fact]
    public async Task ConcurrentPatches_SameItem_WithETag_ExactlyOneSucceeds()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));
        var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        var etag = read.ETag;

        var successes = 0;
        var preconditionFailed = 0;

        var tasks = Enumerable.Range(0, 50).Select(async i =>
        {
            try
            {
                await container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
                    new[] { PatchOperation.Set("/name", $"Patched{i}") },
                    new PatchItemRequestOptions { IfMatchEtag = etag });
                Interlocked.Increment(ref successes);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
            {
                Interlocked.Increment(ref preconditionFailed);
            }
        });

        await Task.WhenAll(tasks);
        successes.Should().Be(1);
        preconditionFailed.Should().Be(49);
    }

    [Fact]
    public async Task ConcurrentPatchAndReplace_SameItem_NoCorruption()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var patchTasks = Enumerable.Range(0, 25).Select(i =>
            container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
                new[] { PatchOperation.Set("/name", $"Patched{i}") }));

        var replaceTasks = Enumerable.Range(0, 25).Select(i =>
            container.ReplaceItemAsync(
                new TestDocument { Id = "1", PartitionKey = "pk1", Name = $"Replaced{i}" },
                "1", new PartitionKey("pk1")));

        await Task.WhenAll(patchTasks.Cast<Task>().Concat(replaceTasks));

        var final = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        final.Resource.Name.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task ConcurrentPatch_DifferentItems_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        for (var i = 0; i < 100; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));

        var tasks = Enumerable.Range(0, 100).Select(i =>
            container.PatchItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"),
                new[] { PatchOperation.Set("/name", $"Patched{i}") }));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
    }

    [Fact]
    public async Task ConcurrentPatch_IncrementOperation_DemonstratesLastWriteWins()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Counter", Value = 0 },
            new PartitionKey("pk1"));

        var tasks = Enumerable.Range(0, 100).Select(_ =>
            container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
                new[] { PatchOperation.Increment("/value", 1) }));

        await Task.WhenAll(tasks);

        var final = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        // Without locks, some increments may be lost (last-writer-wins)
        final.Resource.Value.Should().BeGreaterThan(0).And.BeLessThanOrEqualTo(100);
    }
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Category 3: Concurrent Replace Scenarios
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentReplaceTests
{
    [Fact]
    public async Task ConcurrentReplaces_SameItem_AllSucceed_LastWriteWins()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var tasks = Enumerable.Range(0, 50).Select(i =>
            container.ReplaceItemAsync(
                new TestDocument { Id = "1", PartitionKey = "pk1", Name = $"Version{i}" },
                "1", new PartitionKey("pk1")));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);

        var final = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        final.Resource.Name.Should().StartWith("Version");
    }

    [Fact]
    public async Task ConcurrentReplaces_SameItem_WithETag_ExactlyOneSucceeds()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));
        var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        var etag = read.ETag;

        var successes = 0;
        var preconditionFailed = 0;

        var tasks = Enumerable.Range(0, 50).Select(async i =>
        {
            try
            {
                await container.ReplaceItemAsync(
                    new TestDocument { Id = "1", PartitionKey = "pk1", Name = $"Version{i}" },
                    "1", new PartitionKey("pk1"),
                    new ItemRequestOptions { IfMatchEtag = etag });
                Interlocked.Increment(ref successes);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
            {
                Interlocked.Increment(ref preconditionFailed);
            }
        });

        await Task.WhenAll(tasks);
        successes.Should().Be(1);
        preconditionFailed.Should().Be(49);
    }

    [Fact]
    public async Task ConcurrentReplaces_DifferentItems_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        for (var i = 0; i < 100; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));

        var tasks = Enumerable.Range(0, 100).Select(i =>
            container.ReplaceItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Replaced{i}" },
                $"{i}", new PartitionKey("pk1")));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
    }
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Category 4: Concurrent Upsert Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentUpsertTests
{
    [Fact]
    public async Task ConcurrentUpserts_ItemDoesntExist_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var tasks = Enumerable.Range(0, 50).Select(i =>
            container.UpsertItemAsync(
                new TestDocument { Id = "same", PartitionKey = "pk1", Name = $"V{i}" },
                new PartitionKey("pk1")));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r =>
            r.StatusCode == HttpStatusCode.Created || r.StatusCode == HttpStatusCode.OK);

        container.ItemCount.Should().Be(1);
    }

    [Fact]
    public async Task ConcurrentUpserts_WithETag_ExactlyOneSucceeds()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));
        var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        var etag = read.ETag;

        var successes = 0;
        var preconditionFailed = 0;

        var tasks = Enumerable.Range(0, 50).Select(async i =>
        {
            try
            {
                await container.UpsertItemAsync(
                    new TestDocument { Id = "1", PartitionKey = "pk1", Name = $"V{i}" },
                    new PartitionKey("pk1"),
                    new ItemRequestOptions { IfMatchEtag = etag });
                Interlocked.Increment(ref successes);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
            {
                Interlocked.Increment(ref preconditionFailed);
            }
        });

        await Task.WhenAll(tasks);
        successes.Should().Be(1);
        preconditionFailed.Should().Be(49);
    }

    [Fact]
    public async Task ConcurrentUpserts_DifferentPartitionKeys_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var tasks = Enumerable.Range(0, 100).Select(i =>
            container.UpsertItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = $"pk{i}", Name = $"Item{i}" },
                new PartitionKey($"pk{i}")));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
        container.ItemCount.Should().Be(100);
    }
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Category 5: Transactional Batch Under Concurrency
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentBatchTests
{
    [Fact]
    public async Task TransactionalBatch_Rollback_RestoresTimestamps()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        // Read original _ts
        var before = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
        var tsBefore = (long)before.Resource["_ts"]!;

        // Wait a moment so _ts would change
        await Task.Delay(1100);

        // Batch that modifies item 1 then fails on a duplicate create
        var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.ReplaceItem("1", new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Modified" });
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Duplicate" }); // will conflict
        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeFalse();

        // After rollback, _ts should be restored to original value
        var after = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
        var tsAfter = (long)after.Resource["_ts"]!;
        tsAfter.Should().Be(tsBefore);
    }

    [Fact]
    public async Task ConcurrentBatches_SamePartition_DifferentItems_BothSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var batch1Task = Task.Run(async () =>
        {
            var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
            batch.CreateItem(new TestDocument { Id = "a1", PartitionKey = "pk1", Name = "A1" });
            batch.CreateItem(new TestDocument { Id = "a2", PartitionKey = "pk1", Name = "A2" });
            return await batch.ExecuteAsync();
        });

        var batch2Task = Task.Run(async () =>
        {
            var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
            batch.CreateItem(new TestDocument { Id = "b1", PartitionKey = "pk1", Name = "B1" });
            batch.CreateItem(new TestDocument { Id = "b2", PartitionKey = "pk1", Name = "B2" });
            return await batch.ExecuteAsync();
        });

        using var r1 = await batch1Task;
        using var r2 = await batch2Task;

        r1.IsSuccessStatusCode.Should().BeTrue();
        r2.IsSuccessStatusCode.Should().BeTrue();
        container.ItemCount.Should().Be(4);
    }
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Category 6: Concurrent Change Feed Operations
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentChangeFeedTests
{
    [Fact]
    public async Task ConcurrentChangeFeedRead_WhileWriting_NoMissedEntries()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        const int writeCount = 100;

        var writeTasks = Enumerable.Range(0, writeCount).Select(i =>
            container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1")));

        var readTasks = Enumerable.Range(0, 5).Select(async _ =>
        {
            await Task.Delay(10);
            var iterator = container.GetChangeFeedIterator<TestDocument>(0);
            var results = new List<TestDocument>();
            while (iterator.HasMoreResults)
            {
                var page = await iterator.ReadNextAsync();
                results.AddRange(page);
            }
            return results.Count;
        });

        await Task.WhenAll(writeTasks);
        var readResults = await Task.WhenAll(readTasks);

        // After writes complete, final read should see all items
        var finalIterator = container.GetChangeFeedIterator<TestDocument>(0);
        var finalResults = new List<TestDocument>();
        while (finalIterator.HasMoreResults)
        {
            var page = await finalIterator.ReadNextAsync();
            finalResults.AddRange(page);
        }
        finalResults.Should().HaveCount(writeCount);
    }
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Category 7: Concurrent Operations Across Partitions
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentCrossPartitionTests
{
    [Fact]
    public async Task ConcurrentOperations_DifferentPartitions_FullyIndependent()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var tasks = Enumerable.Range(0, 10).Select(async pk =>
        {
            var pkStr = $"pk{pk}";
            for (var i = 0; i < 10; i++)
            {
                var id = $"{pk}-{i}";
                await container.CreateItemAsync(
                    new TestDocument { Id = id, PartitionKey = pkStr, Name = $"Item{id}" },
                    new PartitionKey(pkStr));
            }

            // Read back all items in this partition
            var iterator = container.GetItemQueryIterator<TestDocument>(
                new QueryDefinition("SELECT * FROM c WHERE c.partitionKey = @pk")
                    .WithParameter("@pk", pkStr));
            var results = new List<TestDocument>();
            while (iterator.HasMoreResults)
                results.AddRange(await iterator.ReadNextAsync());

            results.Should().HaveCount(10);
        });

        await Task.WhenAll(tasks);
        container.ItemCount.Should().Be(100);
    }

    [Fact]
    public async Task CrossPartitionQuery_DuringConcurrentWrites()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        for (var i = 0; i < 50; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"pre-{i}", PartitionKey = $"pk{i % 5}", Name = $"Pre{i}" },
                new PartitionKey($"pk{i % 5}"));

        var writeTasks = Enumerable.Range(50, 50).Select(i =>
            container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = $"pk{i % 5}", Name = $"New{i}" },
                new PartitionKey($"pk{i % 5}")));

        var queryTasks = Enumerable.Range(0, 5).Select(async _ =>
        {
            var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
            var results = new List<TestDocument>();
            while (iterator.HasMoreResults)
                results.AddRange(await iterator.ReadNextAsync());
            results.Should().NotBeEmpty();
        });

        await Task.WhenAll(writeTasks.Cast<Task>().Concat(queryTasks));
        container.ItemCount.Should().Be(100);
    }
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Category 8: Unique Key Constraint Under Concurrency
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentUniqueKeyTests
{
    [Fact]
    public async Task ConcurrentCreates_UniqueKeyViolation_ExactlyOneSucceeds()
    {
        var containerProps = new ContainerProperties("test", "/partitionKey")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/name" } } }
            }
        };
        var container = new InMemoryContainer(containerProps);

        var successes = 0;
        var conflicts = 0;

        var tasks = Enumerable.Range(0, 50).Select(async i =>
        {
            try
            {
                await container.CreateItemAsync(
                    new TestDocument { Id = $"id{i}", PartitionKey = "pk1", Name = "SameName" },
                    new PartitionKey("pk1"));
                Interlocked.Increment(ref successes);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
            {
                Interlocked.Increment(ref conflicts);
            }
        });

        await Task.WhenAll(tasks);
        successes.Should().Be(1);
        conflicts.Should().Be(49);
    }
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Category 9: Concurrent Stream API Operations
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentStreamTests
{
    [Fact]
    public async Task ConcurrentCreateItemStream_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var tasks = Enumerable.Range(0, 100).Select(async i =>
        {
            var json = $"{{\"id\":\"{i}\",\"partitionKey\":\"pk1\",\"name\":\"Item{i}\"}}";
            var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
            using var response = await container.CreateItemStreamAsync(stream, new PartitionKey("pk1"));
            return response.StatusCode;
        });

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(s => s == HttpStatusCode.Created);
        container.ItemCount.Should().Be(100);
    }

    [Fact]
    public async Task ConcurrentUpsertItemStream_SameItem_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var tasks = Enumerable.Range(0, 50).Select(async i =>
        {
            var json = $"{{\"id\":\"1\",\"partitionKey\":\"pk1\",\"name\":\"V{i}\"}}";
            var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
            using var response = await container.UpsertItemStreamAsync(stream, new PartitionKey("pk1"));
            return response.StatusCode;
        });

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(s =>
            s == HttpStatusCode.Created || s == HttpStatusCode.OK);
        container.ItemCount.Should().Be(1);
    }

    [Fact]
    public async Task ConcurrentDeleteItemStream_SameItem_NoCorruption()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Target" },
            new PartitionKey("pk1"));

        var tasks = Enumerable.Range(0, 50).Select(async _ =>
        {
            using var response = await container.DeleteItemStreamAsync("1", new PartitionKey("pk1"));
            return response.StatusCode;
        });

        var results = await Task.WhenAll(tasks);
        results.Should().Contain(HttpStatusCode.NoContent);
        container.ItemCount.Should().Be(0);
    }
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Category 10: ReadMany Under Concurrency
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentReadManyTests
{
    [Fact]
    public async Task ConcurrentReadMany_WhileWriting_NoCorruption()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        for (var i = 0; i < 50; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));

        var writeTasks = Enumerable.Range(50, 50).Select(i =>
            container.UpsertItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"New{i}" },
                new PartitionKey("pk1")));

        var readTasks = Enumerable.Range(0, 10).Select(async _ =>
        {
            var ids = Enumerable.Range(0, 20)
                .Select(i => (i.ToString(), new PartitionKey("pk1")))
                .ToList();
            var response = await container.ReadManyItemsAsync<TestDocument>(ids);
            response.Resource.Should().NotBeEmpty();
        });

        await Task.WhenAll(writeTasks.Cast<Task>().Concat(readTasks));
    }
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Category 11: Container/Database Level Concurrency
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentContainerDatabaseTests
{
    [Fact]
    public async Task ConcurrentContainerCreation_SameDatabase()
    {
        var client = new InMemoryCosmosClient();
        var db = client.GetDatabase("db1");

        var tasks = Enumerable.Range(0, 50).Select(i =>
            db.CreateContainerIfNotExistsAsync($"container{i}", "/pk"));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created || r.StatusCode == HttpStatusCode.OK);
    }

    [Fact]
    public async Task ConcurrentDatabaseCreation()
    {
        var client = new InMemoryCosmosClient();

        var tasks = Enumerable.Range(0, 50).Select(i =>
            client.CreateDatabaseIfNotExistsAsync($"db{i}"));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created || r.StatusCode == HttpStatusCode.OK);
    }
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Category 12: Stress / Chaos Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrencyStressTests
{
    [Fact]
    public async Task HighContention_MixedOperations_NoCorruption()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        const int itemPool = 50;

        // Pre-create items
        for (var i = 0; i < itemPool; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}", Value = 0 },
                new PartitionKey("pk1"));

        var rng = new Random(42);
        var tasks = Enumerable.Range(0, 200).Select(async t =>
        {
            var id = $"{t % itemPool}";
            var op = t % 5;
            try
            {
                switch (op)
                {
                    case 0: // read
                        await container.ReadItemAsync<TestDocument>(id, new PartitionKey("pk1"));
                        break;
                    case 1: // upsert
                        await container.UpsertItemAsync(
                            new TestDocument { Id = id, PartitionKey = "pk1", Name = $"Upserted{t}" },
                            new PartitionKey("pk1"));
                        break;
                    case 2: // replace
                        await container.ReplaceItemAsync(
                            new TestDocument { Id = id, PartitionKey = "pk1", Name = $"Replaced{t}" },
                            id, new PartitionKey("pk1"));
                        break;
                    case 3: // patch
                        await container.PatchItemAsync<TestDocument>(id, new PartitionKey("pk1"),
                            new[] { PatchOperation.Set("/name", $"Patched{t}") });
                        break;
                    case 4: // delete + recreate
                        await container.DeleteItemAsync<TestDocument>(id, new PartitionKey("pk1"));
                        await container.CreateItemAsync(
                            new TestDocument { Id = id, PartitionKey = "pk1", Name = $"Recreated{t}" },
                            new PartitionKey("pk1"));
                        break;
                }
            }
            catch (CosmosException) { }
        });

        await Task.WhenAll(tasks);

        // Verify no corruption: all remaining items are readable
        for (var i = 0; i < itemPool; i++)
        {
            try
            {
                var r = await container.ReadItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"));
                r.Resource.Id.Should().Be($"{i}");
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound) { }
        }
    }
}
