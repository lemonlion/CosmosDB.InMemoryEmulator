using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Validates that <c>ItemResponse&lt;T&gt;</c> proxy creation works correctly on both
/// .NET 8 and .NET 10. This was reported as a potential bug but is NOT reproducible —
/// the NSubstitute/Castle.DynamicProxy approach works fine on both frameworks when T is
/// a public type. Using private/internal types for T will fail due to Castle proxy
/// accessibility requirements (not framework-specific).
/// </summary>
public class ItemResponseProxyNet10Tests
{
    public class SimpleDocument
    {
        [JsonProperty("id")]
        public string Id { get; set; } = default!;

        [JsonProperty("pk")]
        public string Pk { get; set; } = default!;

        [JsonProperty("name")]
        public string Name { get; set; } = default!;
    }

    [Fact]
    public async Task CreateItemAsync_ViaInMemoryCosmosClient_ShouldSucceed()
    {
        var client = new InMemoryCosmosClient();
        var dbResponse = await client.CreateDatabaseIfNotExistsAsync("testdb");
        var containerResponse = await dbResponse.Database.CreateContainerIfNotExistsAsync("testcontainer", "/pk");
        var container = containerResponse.Container;

        var item = new SimpleDocument { Id = "1", Pk = "pk1", Name = "test" };
        var response = await container.CreateItemAsync(item, new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Resource.Should().NotBeNull();
        response.Resource.Id.Should().Be("1");
        response.Resource.Name.Should().Be("test");
    }

    [Fact]
    public async Task UpsertItemAsync_ViaInMemoryCosmosClient_ShouldSucceed()
    {
        var client = new InMemoryCosmosClient();
        var dbResponse = await client.CreateDatabaseIfNotExistsAsync("testdb");
        var containerResponse = await dbResponse.Database.CreateContainerIfNotExistsAsync("testcontainer", "/pk");
        var container = containerResponse.Container;

        var item = new SimpleDocument { Id = "2", Pk = "pk1", Name = "upsert-test" };
        var response = await container.UpsertItemAsync(item, new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Resource.Should().NotBeNull();
        response.Resource.Name.Should().Be("upsert-test");
    }

    [Fact]
    public async Task ReadItemAsync_ViaInMemoryCosmosClient_ShouldSucceed()
    {
        var client = new InMemoryCosmosClient();
        var dbResponse = await client.CreateDatabaseIfNotExistsAsync("testdb");
        var containerResponse = await dbResponse.Database.CreateContainerIfNotExistsAsync("testcontainer", "/pk");
        var container = containerResponse.Container;

        var item = new SimpleDocument { Id = "3", Pk = "pk1", Name = "read-test" };
        await container.CreateItemAsync(item, new PartitionKey("pk1"));

        var response = await container.ReadItemAsync<SimpleDocument>("3", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Should().NotBeNull();
        response.Resource.Name.Should().Be("read-test");
    }
}
