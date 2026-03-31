using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;
using System.Text;

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


public class ReadManyGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private async Task SeedItems()
    {
        for (var i = 1; i <= 5; i++)
        {
            await _container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = $"pk{(i % 2) + 1}", Name = $"Item{i}" },
                new PartitionKey($"pk{(i % 2) + 1}"));
        }
    }

    [Fact]
    public async Task ReadMany_EmptyList_ReturnsEmptyResponse()
    {
        await SeedItems();
        var response = await _container.ReadManyItemsAsync<TestDocument>([]);

        response.Resource.Should().BeEmpty();
    }

    [Fact]
    public async Task ReadMany_SomeItemsMissing_ReturnsOnlyExisting()
    {
        await SeedItems();
        var items = new List<(string, PartitionKey)>
        {
            ("1", new PartitionKey("pk2")),
            ("missing", new PartitionKey("pk1")),
        };

        var response = await _container.ReadManyItemsAsync<TestDocument>(items);

        response.Resource.Should().ContainSingle().Which.Id.Should().Be("1");
    }

    [Fact]
    public async Task ReadMany_AllMissing_ReturnsEmpty()
    {
        await SeedItems();
        var items = new List<(string, PartitionKey)>
        {
            ("missing1", new PartitionKey("pk1")),
            ("missing2", new PartitionKey("pk2")),
        };

        var response = await _container.ReadManyItemsAsync<TestDocument>(items);

        response.Resource.Should().BeEmpty();
    }

    [Fact]
    public async Task ReadMany_MixedPartitionKeys()
    {
        await SeedItems();
        var items = new List<(string, PartitionKey)>
        {
            ("1", new PartitionKey("pk2")),
            ("2", new PartitionKey("pk1")),
        };

        var response = await _container.ReadManyItemsAsync<TestDocument>(items);

        response.Resource.Should().HaveCount(2);
    }
}


public class ReadManyEdgeCaseTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task ReadManyItemsStreamAsync_AllExist_ReturnsOkStream()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var items = new List<(string, PartitionKey)> { ("1", new PartitionKey("pk1")) };

        using var response = await _container.ReadManyItemsStreamAsync(items);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Content.Should().NotBeNull();
    }

    [Fact]
    public async Task ReadManyItemsAsync_DuplicateItems_InList_ReturnsDuplicates()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var items = new List<(string, PartitionKey)>
        {
            ("1", new PartitionKey("pk1")),
            ("1", new PartitionKey("pk1")),
        };

        var response = await _container.ReadManyItemsAsync<TestDocument>(items);

        // ReadMany with duplicates should return the item for each request
        response.Resource.Should().HaveCount(2);
    }

    [Fact]
    public async Task ReadManyItemsAsync_NullList_Throws()
    {
        var act = () => _container.ReadManyItemsAsync<TestDocument>(null!);
        await act.Should().ThrowAsync<Exception>();
    }
}


/// <summary>
/// Tests edge cases for ReadManyItemsStreamAsync.
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.readmanyitemsstreamasync
/// </summary>
public class ReadManyStreamEdgeCaseTests5
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task ReadManyStream_EmptyList_ReturnsOkWithEmptyDocuments()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var emptyList = new List<(string id, PartitionKey pk)>();

        using var response = await _container.ReadManyItemsStreamAsync(emptyList);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        using var reader = new StreamReader(response.Content);
        var body = await reader.ReadToEndAsync();
        var jObj = JObject.Parse(body);
        ((JArray)jObj["Documents"]!).Should().BeEmpty();
    }

    [Fact]
    public async Task ReadManyStream_AllMissing_ReturnsOkWithEmptyDocuments()
    {
        var items = new List<(string id, PartitionKey pk)>
        {
            ("nonexistent1", new PartitionKey("pk1")),
            ("nonexistent2", new PartitionKey("pk2")),
        };

        using var response = await _container.ReadManyItemsStreamAsync(items);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        using var reader = new StreamReader(response.Content);
        var body = await reader.ReadToEndAsync();
        var jObj = JObject.Parse(body);
        ((JArray)jObj["Documents"]!).Should().BeEmpty();
    }
}


public class ReadManyGapTests3
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task ReadMany_ResponseCount_MatchesFoundItems()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
            new PartitionKey("pk1"));

        var items = new List<(string, PartitionKey)>
        {
            ("1", new PartitionKey("pk1")),
            ("2", new PartitionKey("pk1")),
            ("3", new PartitionKey("pk1")), // doesn't exist
        };

        var response = await _container.ReadManyItemsAsync<TestDocument>(items);

        response.Resource.Should().HaveCount(2);
        response.Count.Should().Be(2);
    }
}


public class ReadManyGapTests2
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task ReadMany_DuplicateIds_InList()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var items = new List<(string, PartitionKey)>
        {
            ("1", new PartitionKey("pk1")),
            ("1", new PartitionKey("pk1")),
        };

        var response = await _container.ReadManyItemsAsync<TestDocument>(items);

        // Should return 1 or 2 depending on implementation
        response.Resource.Should().HaveCountGreaterThanOrEqualTo(1);
    }

    [Fact]
    public async Task ReadMany_LargeList_100Plus()
    {
        for (var i = 0; i < 110; i++)
        {
            await _container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));
        }

        var items = Enumerable.Range(0, 110)
            .Select(i => ($"{i}", new PartitionKey("pk1")))
            .ToList();

        var response = await _container.ReadManyItemsAsync<TestDocument>(items);

        response.Resource.Should().HaveCount(110);
    }

    [Fact]
    public async Task ReadMany_StreamVariant_ReturnsResponse()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var items = new List<(string, PartitionKey)> { ("1", new PartitionKey("pk1")) };

        var response = await _container.ReadManyItemsStreamAsync(items);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Content.Should().NotBeNull();
    }
}
