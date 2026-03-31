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
