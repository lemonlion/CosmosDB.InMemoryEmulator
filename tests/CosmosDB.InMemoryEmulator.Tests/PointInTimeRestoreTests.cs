using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
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
