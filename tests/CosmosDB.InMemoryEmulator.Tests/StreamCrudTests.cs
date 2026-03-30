using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class StreamCrudTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));
    private static string ReadStream(Stream stream)
    {
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    [Fact]
    public async Task CreateItemStreamAsync_ValidItem_ReturnsCreated()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        var response = await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Content.Should().NotBeNull();
    }

    [Fact]
    public async Task CreateItemStreamAsync_Duplicate_ReturnsConflict()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        var response = await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task ReadItemStreamAsync_ExistingItem_ReturnsOkWithContent()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        var response = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Content.Should().NotBeNull();
        var body = ReadStream(response.Content);
        var jObj = JObject.Parse(body);
        jObj["name"]!.ToString().Should().Be("Test");
    }

    [Fact]
    public async Task ReadItemStreamAsync_NonExistent_ReturnsNotFound()
    {
        var response = await _container.ReadItemStreamAsync("nonexistent", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task UpsertItemStreamAsync_NewItem_ReturnsCreated()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        var response = await _container.UpsertItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task UpsertItemStreamAsync_ExistingItem_ReturnsOk()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Original"}""";
        await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        var updatedJson = """{"id":"1","partitionKey":"pk1","name":"Updated"}""";
        var response = await _container.UpsertItemStreamAsync(ToStream(updatedJson), new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task ReplaceItemStreamAsync_ExistingItem_ReturnsOk()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Original"}""";
        await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        var replacementJson = """{"id":"1","partitionKey":"pk1","name":"Replaced"}""";
        var response = await _container.ReplaceItemStreamAsync(ToStream(replacementJson), "1", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task ReplaceItemStreamAsync_NonExistent_ReturnsNotFound()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        var response = await _container.ReplaceItemStreamAsync(ToStream(json), "1", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task DeleteItemStreamAsync_ExistingItem_ReturnsNoContent()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        var response = await _container.DeleteItemStreamAsync("1", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task DeleteItemStreamAsync_NonExistent_ReturnsNotFound()
    {
        var response = await _container.DeleteItemStreamAsync("nonexistent", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task PatchItemStreamAsync_ExistingItem_ReturnsOk()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original", Value = 1 };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var patchOperations = new[] { PatchOperation.Replace("/name", "Patched") };
        var response = await _container.PatchItemStreamAsync("1", new PartitionKey("pk1"), patchOperations);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task PatchItemStreamAsync_NonExistent_ReturnsNotFound()
    {
        var patchOperations = new[] { PatchOperation.Replace("/name", "Patched") };
        var response = await _container.PatchItemStreamAsync("nonexistent", new PartitionKey("pk1"), patchOperations);

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }
}
