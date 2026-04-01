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

        // Verify individual operation status codes
        var createResults = results.Take(50).ToList();
        createResults.Should().OnlyContain(s => s == HttpStatusCode.Created);

        var upsertResults = results.Skip(50).Take(50).ToList();
        upsertResults.Should().OnlyContain(s => s == HttpStatusCode.Created);

        var deleteResults = results.Skip(100).Take(25).ToList();
        deleteResults.Should().OnlyContain(s => s == HttpStatusCode.NoContent);

        var replaceResults = results.Skip(125).Take(25).ToList();
        replaceResults.Should().OnlyContain(s => s == HttpStatusCode.OK);

        // Final count: 50 pre-created - 25 deleted + 50 new + 50 upserted = 125
        container.ItemCount.Should().Be(125);
    }

    [Fact]
    public void InMemoryCosmosClient_ClientOptions_AllowBulkExecution_CanBeSet()
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
        await database.CreateContainerIfNotExistsAsync("test-container", "/partitionKey");
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

    // ══════════════════════════════════════════════════════════════════════════
    // Phase 1: Stream Variant Coverage
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task BulkUpsert_StreamVariant_ManyItems_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var tasks = Enumerable.Range(0, 200).Select(i =>
        {
            var json = JsonConvert.SerializeObject(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" });
            return container.UpsertItemStreamAsync(new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));
        });

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);

        // Re-upsert all (updates)
        var updateTasks = Enumerable.Range(0, 200).Select(i =>
        {
            var json = JsonConvert.SerializeObject(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Updated{i}" });
            return container.UpsertItemStreamAsync(new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));
        });

        var updateResults = await Task.WhenAll(updateTasks);
        updateResults.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
    }

    [Fact]
    public async Task BulkReplace_StreamVariant_ManyItems_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        for (var i = 0; i < 200; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));

        var tasks = Enumerable.Range(0, 200).Select(i =>
        {
            var json = JsonConvert.SerializeObject(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Replaced{i}" });
            return container.ReplaceItemStreamAsync(new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json)), $"{i}", new PartitionKey("pk1"));
        });

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
    }

    [Fact]
    public async Task BulkDelete_StreamVariant_ManyItems_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        for (var i = 0; i < 200; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));

        var tasks = Enumerable.Range(0, 200).Select(i =>
            container.DeleteItemStreamAsync($"{i}", new PartitionKey("pk1")));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.NoContent);
        container.ItemCount.Should().Be(0);
    }

    [Fact]
    public async Task BulkPatch_StreamVariant_ManyItems_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        for (var i = 0; i < 200; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}", Value = 0 },
                new PartitionKey("pk1"));

        var tasks = Enumerable.Range(0, 200).Select(i =>
            container.PatchItemStreamAsync($"{i}", new PartitionKey("pk1"),
                new[] { PatchOperation.Set("/name", $"Patched{i}") }));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
    }

    [Fact]
    public async Task BulkRead_StreamVariant_ManyItems_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        for (var i = 0; i < 200; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));

        var tasks = Enumerable.Range(0, 200).Select(i =>
            container.ReadItemStreamAsync($"{i}", new PartitionKey("pk1")));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Phase 2: Error Handling / Negative Paths
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task BulkReplace_NonExistentItems_AllReturn404()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var tasks = Enumerable.Range(0, 50).Select(i =>
            Task.Run(async () =>
            {
                var ex = await Assert.ThrowsAsync<CosmosException>(() =>
                    container.ReplaceItemAsync(
                        new TestDocument { Id = $"missing-{i}", PartitionKey = "pk1", Name = "X" },
                        $"missing-{i}", new PartitionKey("pk1")));
                return ex.StatusCode;
            }));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(s => s == HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task BulkDelete_NonExistentItems_AllReturn404()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var tasks = Enumerable.Range(0, 50).Select(i =>
            Task.Run(async () =>
            {
                var ex = await Assert.ThrowsAsync<CosmosException>(() =>
                    container.DeleteItemAsync<TestDocument>($"missing-{i}", new PartitionKey("pk1")));
                return ex.StatusCode;
            }));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(s => s == HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task BulkPatch_NonExistentItems_AllReturn404()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var tasks = Enumerable.Range(0, 50).Select(i =>
            Task.Run(async () =>
            {
                var ex = await Assert.ThrowsAsync<CosmosException>(() =>
                    container.PatchItemAsync<TestDocument>($"missing-{i}", new PartitionKey("pk1"),
                        new[] { PatchOperation.Set("/name", "X") }));
                return ex.StatusCode;
            }));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(s => s == HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task BulkRead_NonExistentItems_AllReturn404()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var tasks = Enumerable.Range(0, 50).Select(i =>
            Task.Run(async () =>
            {
                var ex = await Assert.ThrowsAsync<CosmosException>(() =>
                    container.ReadItemAsync<TestDocument>($"missing-{i}", new PartitionKey("pk1")));
                return ex.StatusCode;
            }));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(s => s == HttpStatusCode.NotFound);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Phase 3: Request Options & Conditional Operations
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task BulkReplace_WithCorrectIfMatchEtag_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var createResults = new List<ItemResponse<TestDocument>>();
        for (var i = 0; i < 100; i++)
        {
            var r = await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));
            createResults.Add(r);
        }

        var tasks = Enumerable.Range(0, 100).Select(i =>
            container.ReplaceItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Replaced{i}" },
                $"{i}", new PartitionKey("pk1"),
                new ItemRequestOptions { IfMatchEtag = createResults[i].ETag }));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
    }

    [Fact]
    public async Task BulkReplace_WithStaleIfMatchEtag_AllFail412()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var etags = new List<string>();
        for (var i = 0; i < 100; i++)
        {
            var r = await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));
            etags.Add(r.ETag);
        }

        // Mutate all items so ETags change
        for (var i = 0; i < 100; i++)
            await container.ReplaceItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Mutated{i}" },
                $"{i}", new PartitionKey("pk1"));

        var tasks = Enumerable.Range(0, 100).Select(i =>
            Task.Run(async () =>
            {
                var ex = await Assert.ThrowsAsync<CosmosException>(() =>
                    container.ReplaceItemAsync(
                        new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Stale{i}" },
                        $"{i}", new PartitionKey("pk1"),
                        new ItemRequestOptions { IfMatchEtag = etags[i] }));
                return ex.StatusCode;
            }));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(s => s == HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task BulkCreate_WithEnableContentResponseOnWriteFalse_ResponseResourceIsNull()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var tasks = Enumerable.Range(0, 100).Select(i =>
            container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"),
                new ItemRequestOptions { EnableContentResponseOnWrite = false }));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
        results.Should().OnlyContain(r => r.Resource == null);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Phase 4: Concurrency Edge Cases
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task BulkCreate_SameIdDifferentPartitionKeys_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var tasks = Enumerable.Range(0, 50).Select(i =>
            container.CreateItemAsync(
                new TestDocument { Id = "shared-id", PartitionKey = $"pk{i}", Name = $"Item{i}" },
                new PartitionKey($"pk{i}")));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
        container.ItemCount.Should().Be(50);
    }

    [Fact]
    public async Task BulkUpsert_ConcurrentOnSameItem_LastWriterWins()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var tasks = Enumerable.Range(0, 50).Select(i =>
            container.UpsertItemAsync(
                new TestDocument { Id = "same-id", PartitionKey = "pk1", Name = $"Version{i}" },
                new PartitionKey("pk1")));

        var results = await Task.WhenAll(tasks);

        // All should succeed (mix of 201 Created and 200 OK)
        results.Should().OnlyContain(r =>
            r.StatusCode == HttpStatusCode.Created || r.StatusCode == HttpStatusCode.OK);

        // Only one item should exist
        container.ItemCount.Should().Be(1);

        // Final document should be consistent
        var final = await container.ReadItemAsync<TestDocument>("same-id", new PartitionKey("pk1"));
        final.Resource.Name.Should().StartWith("Version");
    }

    [Fact]
    public async Task BulkCreate_EmptyBatch_Succeeds()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var tasks = Enumerable.Empty<Task<ItemResponse<TestDocument>>>();
        var results = await Task.WhenAll(tasks);

        results.Should().BeEmpty();
        container.ItemCount.Should().Be(0);
    }

    [Fact]
    public async Task BulkCreate_SingleItem_Succeeds()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var tasks = new[]
        {
            container.CreateItemAsync(
                new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Solo" },
                new PartitionKey("pk1"))
        };

        var results = await Task.WhenAll(tasks);
        results.Should().ContainSingle().Which.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Phase 5: Data Integrity & Metadata
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task BulkCreate_SystemMetadata_TsAndEtagSetOnAllItems()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var tasks = Enumerable.Range(0, 100).Select(i =>
            container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1")));

        await Task.WhenAll(tasks);

        for (var i = 0; i < 100; i++)
        {
            var response = await container.ReadItemAsync<Newtonsoft.Json.Linq.JObject>($"{i}", new PartitionKey("pk1"));
            var doc = response.Resource;
            doc["_etag"]!.ToString().Should().NotBeNullOrEmpty();
            ((long)doc["_ts"]!).Should().BeGreaterThan(0);
        }
    }

    [Fact]
    public async Task BulkUpsert_ETags_ChangeOnUpdate()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var createTasks = Enumerable.Range(0, 100).Select(i =>
            container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1")));

        var originalEtags = (await Task.WhenAll(createTasks)).Select(r => r.ETag).ToList();

        var upsertTasks = Enumerable.Range(0, 100).Select(i =>
            container.UpsertItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Updated{i}" },
                new PartitionKey("pk1")));

        var newEtags = (await Task.WhenAll(upsertTasks)).Select(r => r.ETag).ToList();

        for (var i = 0; i < 100; i++)
            newEtags[i].Should().NotBe(originalEtags[i]);
    }

    [Fact]
    public async Task BulkCreate_ResponseMetadata_RequestChargeAndHeaders()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var tasks = Enumerable.Range(0, 100).Select(i =>
            container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1")));

        var results = await Task.WhenAll(tasks);

        foreach (var r in results)
        {
            r.RequestCharge.Should().BeGreaterThan(0);
            r.Headers.Should().NotBeNull();
        }
    }

    [Fact]
    public async Task BulkCreate_SpecialCharactersInIds_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var ids = new[] { "hello world", "café", "item-1.0", "a/b", "emoji\u2764", "dot.dash-under_score" };
        var tasks = ids.Select(id =>
            container.CreateItemAsync(
                new TestDocument { Id = id, PartitionKey = "pk1", Name = id },
                new PartitionKey("pk1")));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);

        foreach (var id in ids)
        {
            var response = await container.ReadItemAsync<TestDocument>(id, new PartitionKey("pk1"));
            response.Resource.Id.Should().Be(id);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Phase 6: Change Feed Interactions
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task BulkUpsert_ChangeFeed_RecordsAllUpdates()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        // Bulk create (100 new items)
        var createTasks = Enumerable.Range(0, 100).Select(i =>
            container.UpsertItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1")));
        await Task.WhenAll(createTasks);

        // Bulk update same items
        var updateTasks = Enumerable.Range(0, 100).Select(i =>
            container.UpsertItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Updated{i}" },
                new PartitionKey("pk1")));
        await Task.WhenAll(updateTasks);

        // Change feed from beginning should see 200 entries (100 creates + 100 updates)
        var iterator = container.GetChangeFeedIterator<TestDocument>(ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);
        var changes = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            if (page.StatusCode == HttpStatusCode.NotModified) break;
            changes.AddRange(page);
        }

        // Incremental change feed returns latest version per item
        changes.Should().HaveCount(100);
        changes.Should().OnlyContain(c => c.Name.StartsWith("Updated"));
    }

    [Fact]
    public async Task BulkDelete_ChangeFeed_RecordsTombstones()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        for (var i = 0; i < 100; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));

        var checkpointAfterCreate = container.GetChangeFeedCheckpoint();

        // Bulk delete all
        var deleteTasks = Enumerable.Range(0, 100).Select(i =>
            container.DeleteItemAsync<TestDocument>($"{i}", new PartitionKey("pk1")));
        await Task.WhenAll(deleteTasks);

        var checkpointAfterDelete = container.GetChangeFeedCheckpoint();

        // Each delete creates a tombstone entry
        (checkpointAfterDelete - checkpointAfterCreate).Should().Be(100);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Phase 7: Partition Key Variants
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task BulkCreate_WithPartitionKeyNone_ExtractsFromDocument()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var tasks = Enumerable.Range(0, 100).Select(i =>
            container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = $"pk{i % 5}", Name = $"Item{i}" }));

        var results = await Task.WhenAll(tasks);
        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
        container.ItemCount.Should().Be(100);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Phase 8: DeleteAllItemsByPartitionKey Integration
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task BulkCreate_ThenDeleteAllByPartitionKey_ContainerEmpty()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var tasks = Enumerable.Range(0, 500).Select(i =>
            container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1")));
        await Task.WhenAll(tasks);
        container.ItemCount.Should().Be(500);

        await container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("pk1"));
        container.ItemCount.Should().Be(0);
    }
}
