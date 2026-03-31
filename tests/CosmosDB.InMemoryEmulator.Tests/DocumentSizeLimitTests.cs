using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using System.Net;
using System.Text;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;


public class DocumentSizeGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Create_OverSizeLimit_ThrowsException()
    {
        var largeValue = new string('x', 3 * 1024 * 1024);
        var doc = new TestDocument { Id = "large", PartitionKey = "pk1", Name = largeValue };

        var act = () => _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task Upsert_OverSizeLimit_ThrowsException()
    {
        var largeValue = new string('x', 3 * 1024 * 1024);
        var doc = new TestDocument { Id = "large", PartitionKey = "pk1", Name = largeValue };

        var act = () => _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task Replace_OverSizeLimit_ThrowsException()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Small" },
            new PartitionKey("pk1"));

        var largeValue = new string('x', 3 * 1024 * 1024);
        var largeDoc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = largeValue };

        var act = () => _container.ReplaceItemAsync(largeDoc, "1", new PartitionKey("pk1"));

        await act.Should().ThrowAsync<CosmosException>();
    }
}


public class DocumentSizeGapTests2
{
    [Fact]
    public async Task StreamCreate_OverSizeLimit_AlsoFails()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var largeValue = new string('x', 3 * 1024 * 1024);
        var json = $"{{\"id\":\"1\",\"partitionKey\":\"pk1\",\"name\":\"{largeValue}\"}}";

        var act = () => container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

        await act.Should().ThrowAsync<CosmosException>();
    }
}


public class DocumentSizeGapTests4
{
    [Fact]
    public async Task Create_ExactlyAtSizeLimit_Succeeds()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        // Build a document that is close to but under 2MB
        // The overhead of id/partitionKey fields is ~60 bytes
        var targetSize = (2 * 1024 * 1024) - 200;
        var largeValue = new string('x', targetSize);
        var json = $"{{\"id\":\"1\",\"partitionKey\":\"pk1\",\"data\":\"{largeValue}\"}}";

        var response = await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(json)),
            new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }
}


public class DocumentSizeGapTests3
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Patch_ResultExceedsSizeLimit_Fails()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 1 },
            new PartitionKey("pk1"));

        var largeValue = new string('x', 3 * 1024 * 1024);

        var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", largeValue)]);

        await act.Should().ThrowAsync<CosmosException>();
    }
}
