using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using System.Net;
using System.Text;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;


public class TtlGapTests
{
    [Fact]
    public async Task ContainerTtl_ExpiredItems_NotReturnedByRead()
    {
        var container = new InMemoryContainer("ttl-container", "/partitionKey")
        {
            DefaultTimeToLive = 1
        };

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temp" },
            new PartitionKey("pk1"));

        await Task.Delay(TimeSpan.FromSeconds(2));

        var act = () => container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task ContainerTtl_ExpiredItems_NotReturnedByQuery()
    {
        var container = new InMemoryContainer("ttl-container", "/partitionKey")
        {
            DefaultTimeToLive = 1
        };

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temp" },
            new PartitionKey("pk1"));

        await Task.Delay(TimeSpan.FromSeconds(2));

        var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task ContainerTtl_NonExpiredItems_StillReturned()
    {
        var container = new InMemoryContainer("ttl-container", "/partitionKey")
        {
            DefaultTimeToLive = 60
        };

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "LongLived" },
            new PartitionKey("pk1"));

        var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("LongLived");
    }

    [Fact]
    public async Task ContainerTtl_NullMeansNoExpiration()
    {
        var container = new InMemoryContainer("ttl-container", "/partitionKey")
        {
            DefaultTimeToLive = null
        };

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "NoExpiry" },
            new PartitionKey("pk1"));

        var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("NoExpiry");
    }
}


public class TtlGapTests3
{
    [Fact]
    public async Task ContainerTtl_LazyEviction_OnRead()
    {
        var container = new InMemoryContainer("ttl-test", "/partitionKey")
        {
            DefaultTimeToLive = 2
        };

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        // Wait for expiration
        await Task.Delay(TimeSpan.FromSeconds(3));

        // Item should be evicted on read
        var act = () => container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }
}


public class TtlGapTests2
{
    [Fact]
    public async Task PerItemTtl_FromJsonField_Honored()
    {
        var container = new InMemoryContainer("ttl-test", "/partitionKey")
        {
            DefaultTimeToLive = 60
        };

        var json = """{"id":"1","partitionKey":"pk1","name":"Short","_ttl":1}""";
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

        await Task.Delay(TimeSpan.FromSeconds(2));

        var act = () => container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task ContainerTtl_UpdateResetsExpiration()
    {
        var container = new InMemoryContainer("ttl-test", "/partitionKey")
        {
            DefaultTimeToLive = 3
        };

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        await Task.Delay(TimeSpan.FromSeconds(2));

        // Update should reset the TTL clock
        await container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
            new PartitionKey("pk1"));

        await Task.Delay(TimeSpan.FromSeconds(2));

        // Should still be alive (3s TTL reset 2s ago)
        var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Updated");
    }
}


public class TtlGapTests4
{
    [Fact]
    public async Task ContainerTtl_DeletedItemNotEvictedTwice()
    {
        var container = new InMemoryContainer("ttl-test", "/partitionKey")
        {
            DefaultTimeToLive = 2
        };

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        // Wait for TTL to expire
        await Task.Delay(TimeSpan.FromSeconds(3));

        // Reading the deleted item should still give 404, no double-eviction errors
        var act = () => container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }
}
