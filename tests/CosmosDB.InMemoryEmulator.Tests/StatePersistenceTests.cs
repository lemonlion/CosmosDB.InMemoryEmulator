using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;
using System.Net;
using System.Text;

namespace CosmosDB.InMemoryEmulator.Tests;

public class StatePersistenceTests
{
    [Fact]
    public async Task ExportState_EmptyContainer_ReturnsEmptyJson()
    {
        var container = new InMemoryContainer("test-container", "/partitionKey");

        var json = container.ExportState();

        var parsed = JObject.Parse(json);
        parsed["items"]!.Should().BeOfType<JArray>();
        ((JArray)parsed["items"]!).Should().BeEmpty();
    }

    [Fact]
    public async Task ExportState_WithItems_SerializesAllItems()
    {
        var container = new InMemoryContainer("test-container", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));
        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new PartitionKey("pk1"));

        var json = container.ExportState();

        var parsed = JObject.Parse(json);
        var items = (JArray)parsed["items"]!;
        items.Should().HaveCount(2);
    }

    [Fact]
    public async Task ImportState_RestoresItems()
    {
        var source = new InMemoryContainer("source-container", "/partitionKey");
        await source.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));
        await source.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new PartitionKey("pk1"));

        var exportedJson = source.ExportState();

        var target = new InMemoryContainer("target-container", "/partitionKey");
        target.ImportState(exportedJson);

        target.ItemCount.Should().Be(2);
        var alice = await target.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        alice.Resource.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task ImportState_ClearsExistingDataBeforeImporting()
    {
        var container = new InMemoryContainer("test-container", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "existing", PartitionKey = "pk1", Name = "Existing" },
            new PartitionKey("pk1"));

        var stateJson = """{"items":[{"id":"new","partitionKey":"pk1","name":"New","value":0,"isActive":true,"tags":[]}]}""";
        container.ImportState(stateJson);

        container.ItemCount.Should().Be(1);
        var result = await container.ReadItemAsync<TestDocument>("new", new PartitionKey("pk1"));
        result.Resource.Name.Should().Be("New");
    }

    [Fact]
    public async Task ExportState_ToFile_And_ImportState_FromFile_RoundTrips()
    {
        var tempFile = Path.GetTempFileName();
        try
        {
            var source = new InMemoryContainer("source", "/partitionKey");
            await source.CreateItemAsync(
                new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 42, Tags = ["a", "b"] },
                new PartitionKey("pk1"));

            source.ExportStateToFile(tempFile);

            var target = new InMemoryContainer("target", "/partitionKey");
            target.ImportStateFromFile(tempFile);

            target.ItemCount.Should().Be(1);
            var item = await target.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
            item.Resource.Name.Should().Be("Alice");
            item.Resource.Value.Should().Be(42);
            item.Resource.Tags.Should().BeEquivalentTo(["a", "b"]);
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task ExportState_PreservesPartitionKeyIsolation()
    {
        var container = new InMemoryContainer("test-container", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk2", Name = "Different Alice" },
            new PartitionKey("pk2"));

        var json = container.ExportState();

        var target = new InMemoryContainer("target", "/partitionKey");
        target.ImportState(json);

        target.ItemCount.Should().Be(2);
        var pk1Item = await target.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        pk1Item.Resource.Name.Should().Be("Alice");
        var pk2Item = await target.ReadItemAsync<TestDocument>("1", new PartitionKey("pk2"));
        pk2Item.Resource.Name.Should().Be("Different Alice");
    }

    [Fact]
    public async Task ImportState_WithNestedObjects_PreservesStructure()
    {
        var source = new InMemoryContainer("source", "/partitionKey");
        await source.CreateItemAsync(new TestDocument
        {
            Id = "1",
            PartitionKey = "pk1",
            Name = "Test",
            Nested = new NestedObject { Description = "nested value", Score = 3.14 }
        }, new PartitionKey("pk1"));

        var json = source.ExportState();
        var target = new InMemoryContainer("target", "/partitionKey");
        target.ImportState(json);

        var item = await target.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        item.Resource.Nested.Should().NotBeNull();
        item.Resource.Nested!.Description.Should().Be("nested value");
        item.Resource.Nested!.Score.Should().Be(3.14);
    }

    [Fact]
    public void ImportState_WithInvalidJson_ThrowsException()
    {
        var container = new InMemoryContainer("test-container", "/partitionKey");

        var action = () => container.ImportState("not valid json");

        action.Should().Throw<JsonReaderException>();
    }

    [Fact]
    public async Task ExportState_ItemsAreQueryableAfterImport()
    {
        var source = new InMemoryContainer("source", "/partitionKey");
        await source.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));
        await source.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new PartitionKey("pk1"));

        var json = source.ExportState();
        var target = new InMemoryContainer("target", "/partitionKey");
        target.ImportState(json);

        var queryDef = new QueryDefinition("SELECT * FROM c WHERE c.value > 15");
        var iterator = target.GetItemQueryIterator<TestDocument>(queryDef);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Bob");
    }
}


public class StateImportExportTests
{
    [Fact]
    public async Task ExportState_ImportState_RoundTrip()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));
        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
            new PartitionKey("pk1"));

        var state = container.ExportState();
        state.Should().NotBeNullOrEmpty();

        var newContainer = new InMemoryContainer("test", "/partitionKey");
        newContainer.ImportState(state);

        var read = await newContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Alice");

        newContainer.ItemCount.Should().Be(2);
    }
}
