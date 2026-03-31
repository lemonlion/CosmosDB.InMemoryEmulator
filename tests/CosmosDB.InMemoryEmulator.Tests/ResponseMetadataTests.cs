using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using System.Net;
using System.Text;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;


public class ResponseMetadataGapTests4
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Response_ActivityId_NotNull()
    {
        var response = await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        response.ActivityId.Should().NotBeNullOrEmpty();
        Guid.TryParse(response.ActivityId, out _).Should().BeTrue();
    }

    [Fact]
    public async Task Response_Headers_ContainStandardCosmosHeaders()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        var response = await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

        response.Headers.ETag.Should().NotBeNullOrEmpty();
        response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
        response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
    }
}


public class ResponseMetadataGapTests3
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Response_Diagnostics_NotNull()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        response.Diagnostics.Should().NotBeNull();
    }

    [Fact]
    public async Task StreamResponse_Headers_ContainETag_AfterWrite()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        var response = await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

        response.Headers.ETag.Should().NotBeNullOrEmpty();
    }
}


public class ResponseMetadataGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Response_RequestCharge_PositiveOnWrite()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        response.RequestCharge.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task Response_ETag_SetOnAllWriteOperations()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

        var create = await _container.CreateItemAsync(item, new PartitionKey("pk1"));
        create.ETag.Should().NotBeNullOrEmpty();

        var upsert = await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Upserted" },
            new PartitionKey("pk1"));
        upsert.ETag.Should().NotBeNullOrEmpty();

        var replace = await _container.ReplaceItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" },
            "1", new PartitionKey("pk1"));
        replace.ETag.Should().NotBeNullOrEmpty();

        var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.ETag.Should().NotBeNullOrEmpty();
    }
}
