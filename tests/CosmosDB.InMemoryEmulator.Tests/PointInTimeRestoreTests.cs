using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using System.Net;
using System.Text;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class PointInTimeRestoreTests
{
    [Fact]
    public async Task RestoreToPointInTime_RestoresItemsAsOfGivenTimestamp()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
            new PartitionKey("pk"));

        var restorePoint = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk", Name = "Bob", Value = 20 },
            new PartitionKey("pk"));

        container.RestoreToPointInTime(restorePoint);

        container.ItemCount.Should().Be(1);
        var item = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
        item.Resource.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task RestoreToPointInTime_RestoresDeletedItems()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
            new PartitionKey("pk"));

        var restorePoint = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk"));

        container.ItemCount.Should().Be(0);

        container.RestoreToPointInTime(restorePoint);

        container.ItemCount.Should().Be(1);
        var item = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
        item.Resource.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task RestoreToPointInTime_RestoresOverwrittenValues()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Original", Value = 1 },
            new PartitionKey("pk"));

        var restorePoint = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        await container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Updated", Value = 2 },
            new PartitionKey("pk"));

        var current = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
        current.Resource.Name.Should().Be("Updated");

        container.RestoreToPointInTime(restorePoint);

        var restored = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
        restored.Resource.Name.Should().Be("Original");
        restored.Resource.Value.Should().Be(1);
    }

    [Fact]
    public async Task RestoreToPointInTime_BeforeAnyData_ResultsInEmptyContainer()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var beforeAnyData = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
            new PartitionKey("pk"));

        container.RestoreToPointInTime(beforeAnyData);

        container.ItemCount.Should().Be(0);
    }

    [Fact]
    public async Task RestoreToPointInTime_MultiplePartitionKeys_RestoresCorrectly()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));
        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk2", Name = "Bob" },
            new PartitionKey("pk2"));

        var restorePoint = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        await container.CreateItemAsync(
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie" },
            new PartitionKey("pk1"));
        await container.DeleteItemAsync<TestDocument>("2", new PartitionKey("pk2"));

        container.RestoreToPointInTime(restorePoint);

        container.ItemCount.Should().Be(2);
        var alice = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        alice.Resource.Name.Should().Be("Alice");
        var bob = await container.ReadItemAsync<TestDocument>("2", new PartitionKey("pk2"));
        bob.Resource.Name.Should().Be("Bob");
    }

    [Fact]
    public async Task RestoreToPointInTime_PreservesChangeFeedHistory()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
            new PartitionKey("pk"));

        var restorePoint = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk", Name = "Bob" },
            new PartitionKey("pk"));

        container.RestoreToPointInTime(restorePoint);

        // Change feed should still work after restore — new changes are recorded
        await container.CreateItemAsync(
            new TestDocument { Id = "3", PartitionKey = "pk", Name = "Charlie" },
            new PartitionKey("pk"));

        container.ItemCount.Should().Be(2);
    }

    [Fact]
    public async Task RestoreToPointInTime_MultipleUpdatesToSameItem_RestoresCorrectVersion()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "V1", Value = 1 },
            new PartitionKey("pk"));

        await Task.Delay(50);

        await container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "V2", Value = 2 },
            new PartitionKey("pk"));

        var restorePoint = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        await container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "V3", Value = 3 },
            new PartitionKey("pk"));

        container.RestoreToPointInTime(restorePoint);

        var item = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
        item.Resource.Name.Should().Be("V2");
        item.Resource.Value.Should().Be(2);
    }

    [Fact]
    public async Task RestoreToPointInTime_ItemCreatedAndDeletedBeforeRestorePoint_StaysDeleted()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Ephemeral" },
            new PartitionKey("pk"));

        await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk"));

        var restorePoint = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk", Name = "Later" },
            new PartitionKey("pk"));

        container.RestoreToPointInTime(restorePoint);

        container.ItemCount.Should().Be(0);
    }

    [Fact]
    public async Task RestoreToPointInTime_WithPatchOperations_RestoresPrePatchState()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Original", Value = 10 },
            new PartitionKey("pk"));

        var restorePoint = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        await container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk"),
            [PatchOperation.Set("/name", "Patched"), PatchOperation.Increment("/value", 5)]);

        var patched = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
        patched.Resource.Name.Should().Be("Patched");
        patched.Resource.Value.Should().Be(15);

        container.RestoreToPointInTime(restorePoint);

        var restored = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
        restored.Resource.Name.Should().Be("Original");
        restored.Resource.Value.Should().Be(10);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 1 — Bug Documentation (BUG 1: Stale _etag/_ts in restored JSON)
// ═══════════════════════════════════════════════════════════════════════════

public class PitrEtagConsistencyTests
{
    [Fact(Skip = "BUG 1: After PITR, _etags dictionary gets a fresh ETag but _items JSON " +
        "still contains the original _etag value from the change feed. response.ETag and " +
        "response.Resource._etag will differ. Fix: call EnrichWithSystemProperties on " +
        "restored JSON.")]
    public async Task RestoreToPointInTime_ETagsAreConsistentAfterRestore()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
            new PartitionKey("pk"));

        var restorePoint = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        await container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Updated" },
            new PartitionKey("pk"));

        container.RestoreToPointInTime(restorePoint);

        var restored = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk"));
        var bodyEtag = restored.Resource["_etag"]?.ToString();
        restored.ETag.Should().Be(bodyEtag,
            "response.ETag should match the _etag embedded in the JSON body");
    }

    [Fact]
    public async Task RestoreToPointInTime_ETagsAreRegeneratedAfterRestore()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
            new PartitionKey("pk"));

        var originalEtag = (await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"))).ETag;

        var restorePoint = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        await container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Updated" },
            new PartitionKey("pk"));

        container.RestoreToPointInTime(restorePoint);

        var restoredEtag = (await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"))).ETag;

        // ETag should be different from the original — it's regenerated on restore
        restoredEtag.Should().NotBe(originalEtag);
    }

    [Fact]
    public async Task RestoreToPointInTime_OldETagInvalidForConditionalOps()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
            new PartitionKey("pk"));

        var oldEtag = (await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"))).ETag;

        var restorePoint = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        await container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Modified" },
            new PartitionKey("pk"));

        container.RestoreToPointInTime(restorePoint);

        // Old ETag should be invalid for conditional replace
        var act = () => container.ReplaceItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "WithOldEtag" },
            "1", new PartitionKey("pk"),
            new ItemRequestOptions { IfMatchEtag = oldEtag });

        await act.Should().ThrowAsync<CosmosException>()
            .Where(e => e.StatusCode == HttpStatusCode.PreconditionFailed);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 2 — Core Functionality
// ═══════════════════════════════════════════════════════════════════════════

public class PitrCoreFunctionalityTests
{
    [Fact]
    public async Task RestoreToPointInTime_WithReplaceItem_RestoresPreReplaceState()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Original", Value = 1 },
            new PartitionKey("pk"));

        var restorePoint = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        await container.ReplaceItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Replaced", Value = 99 },
            "1", new PartitionKey("pk"));

        container.RestoreToPointInTime(restorePoint);

        var restored = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
        restored.Resource.Name.Should().Be("Original");
        restored.Resource.Value.Should().Be(1);
    }

    [Fact]
    public async Task RestoreToPointInTime_QueriesWorkAfterRestore()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
            new PartitionKey("pk"));
        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk", Name = "Bob" },
            new PartitionKey("pk"));

        var restorePoint = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        await container.DeleteItemAsync<TestDocument>("2", new PartitionKey("pk"));

        container.RestoreToPointInTime(restorePoint);

        var iterator = container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c WHERE c.name = 'Alice'");
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle().Which.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task RestoreToPointInTime_WithStreamOperations_RestoresCorrectly()
    {
        var container = new InMemoryContainer("test", "/pk");

        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","pk":"v1","name":"StreamItem"}""")),
            new PartitionKey("v1"));

        var restorePoint = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        await container.DeleteItemAsync<JObject>("1", new PartitionKey("v1"));

        container.RestoreToPointInTime(restorePoint);

        var read = await container.ReadItemAsync<JObject>("1", new PartitionKey("v1"));
        read.Resource["name"]!.ToString().Should().Be("StreamItem");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 3 — Edge Cases & Boundary Conditions
// ═══════════════════════════════════════════════════════════════════════════

public class PitrEdgeCaseTests
{
    [Fact]
    public async Task RestoreToPointInTime_ToFutureTimestamp_KeepsAllItems()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
            new PartitionKey("pk"));
        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk", Name = "B" },
            new PartitionKey("pk"));

        container.RestoreToPointInTime(DateTimeOffset.UtcNow.AddHours(1));

        container.ItemCount.Should().Be(2);
    }

    [Fact]
    public async Task RestoreToPointInTime_DoubleRestoreToSamePoint_IsIdempotent()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
            new PartitionKey("pk"));

        var restorePoint = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        await container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Updated", Value = 20 },
            new PartitionKey("pk"));

        container.RestoreToPointInTime(restorePoint);
        var firstRestore = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));

        container.RestoreToPointInTime(restorePoint);
        var secondRestore = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));

        secondRestore.Resource.Name.Should().Be(firstRestore.Resource.Name);
        secondRestore.Resource.Value.Should().Be(firstRestore.Resource.Value);
        container.ItemCount.Should().Be(1);
    }

    [Fact]
    public async Task RestoreToPointInTime_ConsecutiveRestoresToDifferentPoints()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "V1", Value = 1 },
            new PartitionKey("pk"));

        var t1 = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        await container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "V2", Value = 2 },
            new PartitionKey("pk"));

        var t2 = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk", Name = "New", Value = 99 },
            new PartitionKey("pk"));

        // Restore to t2 — should have 1 item at V2
        container.RestoreToPointInTime(t2);
        container.ItemCount.Should().Be(1);
        (await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk")))
            .Resource.Name.Should().Be("V2");

        // Restore to t1 — should have 1 item at V1
        container.RestoreToPointInTime(t1);
        container.ItemCount.Should().Be(1);
        (await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk")))
            .Resource.Name.Should().Be("V1");

        // Restore to future — should replay all and have 2 items
        container.RestoreToPointInTime(DateTimeOffset.UtcNow.AddHours(1));
        container.ItemCount.Should().Be(2);
    }

    [Fact]
    public async Task RestoreToPointInTime_WithPartitionKeyNone_RestoresCorrectly()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var item = new TestDocument { Id = "1", PartitionKey = "fromBody", Name = "AutoPK" };
        await container.CreateItemAsync(item, PartitionKey.None);

        var restorePoint = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("fromBody"));

        container.RestoreToPointInTime(restorePoint);

        container.ItemCount.Should().Be(1);
    }

    [Fact]
    public async Task RestoreToPointInTime_WithHierarchicalPartitionKeys_RestoresCorrectly()
    {
        var container = new InMemoryContainer("test", ["/tenant", "/region"]);
        var pk = new PartitionKeyBuilder().Add("acme").Add("us").Build();

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", tenant = "acme", region = "us", name = "Original" }),
            pk);

        var restorePoint = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        await container.UpsertItemAsync(
            JObject.FromObject(new { id = "1", tenant = "acme", region = "us", name = "Updated" }),
            pk);

        container.RestoreToPointInTime(restorePoint);

        var restored = await container.ReadItemAsync<JObject>("1", pk);
        restored.Resource["name"]!.ToString().Should().Be("Original");
    }

    [Fact]
    public async Task RestoreToPointInTime_EmptyContainer_NoOpRestore()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        container.RestoreToPointInTime(DateTimeOffset.UtcNow);
        container.ItemCount.Should().Be(0);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 4 — Feature Interactions
// ═══════════════════════════════════════════════════════════════════════════

public class PitrFeatureInteractionTests
{
    [Fact]
    public async Task RestoreToPointInTime_AfterClearItems_RestoresToEmpty()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
            new PartitionKey("pk"));

        var restorePointWhenItemExisted = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        container.ClearItems();

        // Change feed was wiped — PITR has no history to replay
        container.RestoreToPointInTime(restorePointWhenItemExisted);

        container.ItemCount.Should().Be(0,
            "ClearItems wipes the change feed, so PITR has no entries to replay");
    }

    [Fact]
    public async Task RestoreToPointInTime_AfterImportState_HasNoPreImportHistory()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "BeforeImport" },
            new PartitionKey("pk"));

        var exported = container.ExportState();

        // ImportState calls ClearItems() internally, wiping the change feed
        container.ImportState(exported);

        // Create a new item after import
        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk", Name = "AfterImport" },
            new PartitionKey("pk"));

        var restorePoint = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        await container.CreateItemAsync(
            new TestDocument { Id = "3", PartitionKey = "pk", Name = "Latest" },
            new PartitionKey("pk"));

        container.RestoreToPointInTime(restorePoint);

        // Only item "2" should exist — item "1" has no change feed history (import doesn't record),
        // item "3" was created after restore point
        container.ItemCount.Should().Be(1);
        var item = await container.ReadItemAsync<TestDocument>("2", new PartitionKey("pk"));
        item.Resource.Name.Should().Be("AfterImport");
    }

    [Fact]
    public async Task RestoreToPointInTime_ChangeFeedIteratorWorksAfterRestore()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Before" },
            new PartitionKey("pk"));

        var restorePoint = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        await container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Modified" },
            new PartitionKey("pk"));

        container.RestoreToPointInTime(restorePoint);

        // New operation after restore should be tracked in change feed
        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk", Name = "PostRestore" },
            new PartitionKey("pk"));

        var iterator = container.GetChangeFeedIterator<TestDocument>(
            ChangeFeedStartFrom.Beginning(),
            ChangeFeedMode.LatestVersion);

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            if (page.StatusCode == HttpStatusCode.NotModified) break;
            results.AddRange(page);
        }

        results.Should().Contain(r => r.Name == "PostRestore");
    }

    [Fact]
    public async Task RestoreToPointInTime_UniqueKeyConstraintsApplyAfterRestore()
    {
        var properties = new ContainerProperties("test", "/partitionKey")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/name" } } }
            }
        };
        var container = new InMemoryContainer(properties);

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk", Name = "Unique" },
            new PartitionKey("pk"));

        var restorePoint = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk"));

        container.RestoreToPointInTime(restorePoint);

        // Item "1" with name "Unique" is restored — creating another with same name should conflict
        var act = () => container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk", Name = "Unique" },
            new PartitionKey("pk"));

        await act.Should().ThrowAsync<CosmosException>()
            .Where(e => e.StatusCode == HttpStatusCode.Conflict);
    }

    [Fact(Skip = "DIVERGENT: After a failed transactional batch, change feed retains " +
        "'ghost' entries from operations that executed before the failure. RestoreSnapshot " +
        "rolls back _items/_etags but not _changeFeed. PITR replays these ghost entries, " +
        "potentially resurrecting items that were never committed.")]
    public async Task RestoreToPointInTime_AfterFailedBatch_GhostEntriesInChangeFeed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            new TestDocument { Id = "existing", PartitionKey = "pk", Name = "Existing" },
            new PartitionKey("pk"));

        var batch = container.CreateTransactionalBatch(new PartitionKey("pk"));
        batch.CreateItem(new TestDocument { Id = "ghost", PartitionKey = "pk", Name = "Ghost" });
        batch.CreateItem(new TestDocument { Id = "existing", PartitionKey = "pk", Name = "Conflict" });

        using var response = await batch.ExecuteAsync();
        // Batch should fail due to conflict on "existing"
        response.StatusCode.Should().NotBe(HttpStatusCode.OK);

        // Ghost item should not exist
        container.ItemCount.Should().Be(1);

        // But PITR might resurrect it from the change feed ghost entries
        container.RestoreToPointInTime(DateTimeOffset.UtcNow);
        container.ItemCount.Should().Be(1, "ghost items from failed batches should not be resurrected");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 5 — Thread Safety
// ═══════════════════════════════════════════════════════════════════════════

public class PitrConcurrencyTests
{
    [Fact]
    public async Task RestoreToPointInTime_ConcurrentReadsAndRestore_NoException()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        for (int i = 0; i < 20; i++)
        {
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk", Name = $"Item{i}" },
                new PartitionKey("pk"));
        }

        var restorePoint = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        for (int i = 20; i < 30; i++)
        {
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk", Name = $"Item{i}" },
                new PartitionKey("pk"));
        }

        // Concurrent reads + restore should not throw
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var readTask = Task.Run(async () =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    await container.ReadItemAsync<TestDocument>("0", new PartitionKey("pk"));
                }
                catch (CosmosException) { /* item may not exist during restore */ }
            }
        }, cts.Token);

        var restoreTask = Task.Run(() =>
        {
            for (int i = 0; i < 10; i++)
            {
                container.RestoreToPointInTime(restorePoint);
            }
        });

        await restoreTask;
        cts.Cancel();
        // If readTask throws an unhandled exception (not CosmosException), this will propagate
        await readTask;
    }
}
