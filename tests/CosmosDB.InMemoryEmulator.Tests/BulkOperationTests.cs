using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for bulk operations — the pattern where <c>CosmosClientOptions.AllowBulkExecution = true</c>
/// is set and many concurrent point operations are fired via <c>Task.WhenAll</c>.
/// The SDK internally batches these into efficient service calls. The in-memory emulator
/// must handle this concurrency correctly.
/// </summary>
public class BulkOperationTests
{
    [Fact]
    public async Task BulkCreate_ManyItems_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var tasks = Enumerable.Range(0, 500).Select(i =>
            container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1")));

        var results = await Task.WhenAll(tasks);

        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
        container.ItemCount.Should().Be(500);

        // Verify all items are readable
        for (var i = 0; i < 500; i++)
        {
            var response = await container.ReadItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"));
            response.Resource.Name.Should().Be($"Item{i}");
        }
    }

    [Fact]
    public async Task BulkUpsert_ManyItems_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        // First round: all new items
        var createTasks = Enumerable.Range(0, 500).Select(i =>
            container.UpsertItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1")));

        var createResults = await Task.WhenAll(createTasks);
        createResults.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
        container.ItemCount.Should().Be(500);

        // Second round: all updates
        var updateTasks = Enumerable.Range(0, 500).Select(i =>
            container.UpsertItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Updated{i}" },
                new PartitionKey("pk1")));

        var updateResults = await Task.WhenAll(updateTasks);
        updateResults.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
        container.ItemCount.Should().Be(500);
    }

    [Fact]
    public async Task BulkDelete_ManyItems_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        // Pre-create items
        for (var i = 0; i < 200; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));

        container.ItemCount.Should().Be(200);

        // Bulk delete
        var tasks = Enumerable.Range(0, 200).Select(i =>
            container.DeleteItemAsync<TestDocument>($"{i}", new PartitionKey("pk1")));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.NoContent);
        container.ItemCount.Should().Be(0);
    }

    [Fact]
    public async Task BulkReplace_ManyItems_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        // Pre-create items
        for (var i = 0; i < 200; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));

        // Bulk replace
        var tasks = Enumerable.Range(0, 200).Select(i =>
            container.ReplaceItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Replaced{i}" },
                $"{i}",
                new PartitionKey("pk1")));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);

        // Verify all items were replaced
        for (var i = 0; i < 200; i++)
        {
            var response = await container.ReadItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"));
            response.Resource.Name.Should().Be($"Replaced{i}");
        }
    }

    [Fact]
    public async Task BulkPatch_ManyItems_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        // Pre-create items
        for (var i = 0; i < 200; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}", Value = 0 },
                new PartitionKey("pk1"));

        // Bulk patch
        var tasks = Enumerable.Range(0, 200).Select(i =>
            container.PatchItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"),
                new[] { PatchOperation.Set("/name", $"Patched{i}") }));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);

        // Verify all items were patched
        for (var i = 0; i < 200; i++)
        {
            var response = await container.ReadItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"));
            response.Resource.Name.Should().Be($"Patched{i}");
        }
    }

    [Fact]
    public async Task BulkRead_ManyItems_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        // Pre-create items
        for (var i = 0; i < 200; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));

        // Bulk read
        var tasks = Enumerable.Range(0, 200).Select(i =>
            container.ReadItemAsync<TestDocument>($"{i}", new PartitionKey("pk1")));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);

        for (var i = 0; i < 200; i++)
            results[i].Resource.Name.Should().Be($"Item{i}");
    }

    [Fact]
    public async Task BulkCreate_MixedPartitionKeys_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var tasks = Enumerable.Range(0, 500).Select(i =>
        {
            var pk = $"pk{i % 10}";
            return container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = pk, Name = $"Item{i}" },
                new PartitionKey(pk));
        });

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
        container.ItemCount.Should().Be(500);

        // Verify distribution: each PK has 50 items
        for (var pk = 0; pk < 10; pk++)
        {
            var iterator = container.GetItemQueryIterator<TestDocument>(
                new QueryDefinition("SELECT * FROM c WHERE c.partitionKey = @pk")
                    .WithParameter("@pk", $"pk{pk}"),
                requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey($"pk{pk}") });

            var items = new List<TestDocument>();
            while (iterator.HasMoreResults)
            {
                var page = await iterator.ReadNextAsync();
                items.AddRange(page);
            }

            items.Should().HaveCount(50);
        }
    }

    [Fact]
    public async Task BulkCreate_DuplicateIds_SomeConflict()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        // Create 100 items where IDs 0-49 are unique, and IDs 50-99 duplicate IDs 0-49
        var tasks = Enumerable.Range(0, 100).Select(i =>
        {
            var id = $"{i % 50}"; // IDs 0-49 used twice
            return Task.Run(async () =>
            {
                try
                {
                    var response = await container.CreateItemAsync(
                        new TestDocument { Id = id, PartitionKey = "pk1", Name = $"Item{i}" },
                        new PartitionKey("pk1"));
                    return (StatusCode: response.StatusCode, IsSuccess: true);
                }
                catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
                {
                    return (StatusCode: HttpStatusCode.Conflict, IsSuccess: false);
                }
            });
        });

        var results = await Task.WhenAll(tasks);

        var successes = results.Count(r => r.IsSuccess);
        var conflicts = results.Count(r => !r.IsSuccess);

        successes.Should().Be(50);
        conflicts.Should().Be(50);
        container.ItemCount.Should().Be(50);
    }

    [Fact]
    public async Task BulkMixedOperations_CreateUpsertDeleteReplace_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        // Pre-create items for replace and delete
        for (var i = 0; i < 50; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"existing-{i}", PartitionKey = "pk1", Name = $"Existing{i}" },
                new PartitionKey("pk1"));

        // Mixed concurrent operations
        var createTasks = Enumerable.Range(0, 50).Select(i =>
            container.CreateItemAsync(
                new TestDocument { Id = $"new-{i}", PartitionKey = "pk1", Name = $"New{i}" },
                new PartitionKey("pk1")).ContinueWith(t => t.Result.StatusCode));

        var upsertTasks = Enumerable.Range(0, 50).Select(i =>
            container.UpsertItemAsync(
                new TestDocument { Id = $"upserted-{i}", PartitionKey = "pk1", Name = $"Upserted{i}" },
                new PartitionKey("pk1")).ContinueWith(t => t.Result.StatusCode));

        var deleteTasks = Enumerable.Range(0, 25).Select(i =>
            container.DeleteItemAsync<TestDocument>($"existing-{i}", new PartitionKey("pk1"))
            .ContinueWith(t => t.Result.StatusCode));

        var replaceTasks = Enumerable.Range(25, 25).Select(i =>
            container.ReplaceItemAsync(
                new TestDocument { Id = $"existing-{i}", PartitionKey = "pk1", Name = $"Replaced{i}" },
                $"existing-{i}",
                new PartitionKey("pk1")).ContinueWith(t => t.Result.StatusCode));

        var allTasks = createTasks
            .Concat(upsertTasks)
            .Concat(deleteTasks)
            .Concat(replaceTasks);

        var results = await Task.WhenAll(allTasks);

        // All should have completed (no unhandled exceptions)
        results.Should().NotBeEmpty();

        // Final count: 50 pre-created - 25 deleted + 50 new + 50 upserted = 125
        container.ItemCount.Should().Be(125);
    }

    [Fact]
    public async Task InMemoryCosmosClient_ClientOptions_AllowBulkExecution_CanBeSet()
    {
        var client = new InMemoryCosmosClient();

        // AllowBulkExecution is a property on CosmosClientOptions — should be settable
        client.ClientOptions.AllowBulkExecution = true;
        client.ClientOptions.AllowBulkExecution.Should().BeTrue();

        client.ClientOptions.AllowBulkExecution = false;
        client.ClientOptions.AllowBulkExecution.Should().BeFalse();
    }

    [Fact]
    public async Task BulkCreate_ViaCosmosClient_WithAllowBulkExecution_AllSucceed()
    {
        var client = new InMemoryCosmosClient();
        client.ClientOptions.AllowBulkExecution = true;

        var database = client.GetDatabase("test-db");
        var container = database.GetContainer("test-container");

        var tasks = Enumerable.Range(0, 200).Select(i =>
            container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1")));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
    }

    [Fact]
    public async Task BulkOperations_ChangeFeed_RecordsAllChanges()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var checkpoint = container.GetChangeFeedCheckpoint();

        // Bulk create
        var tasks = Enumerable.Range(0, 200).Select(i =>
            container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1")));

        await Task.WhenAll(tasks);

        // Read change feed
        var iterator = container.GetChangeFeedIterator<TestDocument>(checkpoint);
        var changes = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            changes.AddRange(page);
        }

        changes.Should().HaveCount(200);
    }

    [Fact]
    public async Task BulkOperations_UniqueKeyViolation_ReturnsConflict()
    {
        var properties = new ContainerProperties("test", "/partitionKey")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/name" } } }
            }
        };
        var container = new InMemoryContainer(properties);

        // Create items with unique names concurrently, but some share the same name
        var tasks = Enumerable.Range(0, 100).Select(i =>
        {
            var name = $"Name{i % 50}"; // 50 unique names, each used twice
            return Task.Run(async () =>
            {
                try
                {
                    await container.CreateItemAsync(
                        new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = name },
                        new PartitionKey("pk1"));
                    return true;
                }
                catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
                {
                    return false;
                }
            });
        });

        var results = await Task.WhenAll(tasks);
        results.Count(r => r).Should().Be(50);
        results.Count(r => !r).Should().Be(50);
    }

    [Fact]
    public async Task BulkOperations_ETags_UpdatedPerOperation()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        // Bulk create
        var tasks = Enumerable.Range(0, 100).Select(i =>
            container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1")));

        var results = await Task.WhenAll(tasks);

        // Each item should have a unique ETag
        var etags = results.Select(r => r.ETag).ToList();
        etags.Should().OnlyHaveUniqueItems();
        etags.Should().NotContain(string.Empty);
        etags.Should().NotContainNulls();
    }

    [Fact]
    public async Task BulkCreate_StreamVariant_ManyItems_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var tasks = Enumerable.Range(0, 200).Select(i =>
        {
            var json = JsonConvert.SerializeObject(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" });
            var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
            return container.CreateItemStreamAsync(stream, new PartitionKey("pk1"));
        });

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
        container.ItemCount.Should().Be(200);
    }
}
