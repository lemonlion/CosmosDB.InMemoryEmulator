using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class ReadManyTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private async Task SeedItems()
    {
        var items = new[]
        {
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
            new TestDocument { Id = "3", PartitionKey = "pk2", Name = "Charlie" },
        };
        foreach (var item in items)
        {
            await _container.CreateItemAsync(item, new PartitionKey(item.PartitionKey));
        }
    }

    [Fact]
    public async Task ReadManyItemsAsync_AllExist_ReturnsAll()
    {
        await SeedItems();
        var itemsToRead = new List<(string id, PartitionKey pk)>
        {
            ("1", new PartitionKey("pk1")),
            ("3", new PartitionKey("pk2")),
        };

        var response = await _container.ReadManyItemsAsync<TestDocument>(itemsToRead);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Should().HaveCount(2);
        response.Resource.Select(d => d.Name).Should().Contain("Alice").And.Contain("Charlie");
    }

    [Fact]
    public async Task ReadManyItemsAsync_SomeNotExist_ReturnsOnlyExisting()
    {
        await SeedItems();
        var itemsToRead = new List<(string id, PartitionKey pk)>
        {
            ("1", new PartitionKey("pk1")),
            ("nonexistent", new PartitionKey("pk1")),
        };

        var response = await _container.ReadManyItemsAsync<TestDocument>(itemsToRead);

        response.Resource.Should().HaveCount(1);
        response.Resource.First().Name.Should().Be("Alice");
    }

    [Fact]
    public async Task ReadManyItemsAsync_NoneExist_ReturnsEmpty()
    {
        await SeedItems();
        var itemsToRead = new List<(string id, PartitionKey pk)>
        {
            ("nonexistent1", new PartitionKey("pk1")),
            ("nonexistent2", new PartitionKey("pk2")),
        };

        var response = await _container.ReadManyItemsAsync<TestDocument>(itemsToRead);

        response.Resource.Should().BeEmpty();
    }

    [Fact]
    public async Task ReadManyItemsAsync_EmptyList_ReturnsEmpty()
    {
        await SeedItems();
        var itemsToRead = new List<(string id, PartitionKey pk)>();

        var response = await _container.ReadManyItemsAsync<TestDocument>(itemsToRead);

        response.Resource.Should().BeEmpty();
    }

    [Fact]
    public async Task ReadManyItemsAsync_WrongPartitionKey_DoesNotReturn()
    {
        await SeedItems();
        var itemsToRead = new List<(string id, PartitionKey pk)>
        {
            ("1", new PartitionKey("pk2")),
        };

        var response = await _container.ReadManyItemsAsync<TestDocument>(itemsToRead);

        response.Resource.Should().BeEmpty();
    }

    [Fact]
    public async Task ReadManyItemsStreamAsync_ReturnsStreamWithDocuments()
    {
        await SeedItems();
        var itemsToRead = new List<(string id, PartitionKey pk)>
        {
            ("1", new PartitionKey("pk1")),
            ("2", new PartitionKey("pk1")),
        };

        using var response = await _container.ReadManyItemsStreamAsync(itemsToRead);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        using var reader = new StreamReader(response.Content);
        var body = await reader.ReadToEndAsync();
        var jObj = JObject.Parse(body);
        ((JArray)jObj["Documents"]!).Should().HaveCount(2);
    }
}
