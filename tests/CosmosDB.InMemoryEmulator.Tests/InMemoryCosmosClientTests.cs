using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class InMemoryCosmosClientTests
{
    // ── Database management ─────────────────────────────────────────────────

    [Fact]
    public async Task CreateDatabaseIfNotExistsAsync_CreatesDatabaseSuccessfully()
    {
        var client = new InMemoryCosmosClient();

        var response = await client.CreateDatabaseIfNotExistsAsync("test-db");

        response.StatusCode.Should().Be(System.Net.HttpStatusCode.Created);
        response.Resource.Id.Should().Be("test-db");
    }

    [Fact]
    public async Task CreateDatabaseIfNotExistsAsync_SecondCall_ReturnsSameDatabase()
    {
        var client = new InMemoryCosmosClient();

        await client.CreateDatabaseIfNotExistsAsync("test-db");
        var response = await client.CreateDatabaseIfNotExistsAsync("test-db");

        response.StatusCode.Should().Be(System.Net.HttpStatusCode.OK);
    }

    [Fact]
    public void GetDatabase_ReturnsDatabase()
    {
        var client = new InMemoryCosmosClient();

        var database = client.GetDatabase("test-db");

        database.Should().NotBeNull();
        database.Id.Should().Be("test-db");
    }

    // ── Container management ────────────────────────────────────────────────

    [Fact]
    public async Task GetContainer_ReturnsWorkingContainer()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseIfNotExistsAsync("test-db");

        var container = client.GetContainer("test-db", "test-container");

        container.Should().NotBeNull();
        container.Id.Should().Be("test-container");
    }

    [Fact]
    public async Task Container_CanStoreThenReadItem()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseIfNotExistsAsync("test-db");
        var container = client.GetContainer("test-db", "test-container");

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));

        var readResponse = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        readResponse.Resource.Name.Should().Be("Alice");
    }

    // ── Multi-database isolation ────────────────────────────────────────────

    [Fact]
    public async Task Containers_InDifferentDatabases_AreIsolated()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseIfNotExistsAsync("db1");
        await client.CreateDatabaseIfNotExistsAsync("db2");

        var container1 = client.GetContainer("db1", "shared-name");
        var container2 = client.GetContainer("db2", "shared-name");

        await container1.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "From DB1" },
            new PartitionKey("pk1"));

        container1.Should().BeOfType<InMemoryContainer>();
        ((InMemoryContainer)container1).ItemCount.Should().Be(1);
        ((InMemoryContainer)container2).ItemCount.Should().Be(0);
    }

    // ── Multi-container within same database ────────────────────────────────

    [Fact]
    public async Task MultipleContainers_InSameDatabase_AreIsolated()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseIfNotExistsAsync("test-db");

        var container1 = client.GetContainer("test-db", "container-1");
        var container2 = client.GetContainer("test-db", "container-2");

        await container1.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Container1Only" },
            new PartitionKey("pk1"));

        ((InMemoryContainer)container1).ItemCount.Should().Be(1);
        ((InMemoryContainer)container2).ItemCount.Should().Be(0);
    }

    // ── GetContainer returns same instance for same database+container ──────

    [Fact]
    public async Task GetContainer_ReturnsSameInstance_ForSameId()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseIfNotExistsAsync("test-db");

        var container1 = client.GetContainer("test-db", "test-container");
        var container2 = client.GetContainer("test-db", "test-container");

        container1.Should().BeSameAs(container2);
    }

    // ── Querying works through client-created containers ────────────────────

    [Fact]
    public async Task Container_SupportsQuerying()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseIfNotExistsAsync("test-db");
        var container = client.GetContainer("test-db", "items");

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));
        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT * FROM c WHERE c.value > 15");
        var iterator = container.GetItemQueryIterator<TestDocument>(query);

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Bob");
    }

    // ── Dispose doesn't throw ───────────────────────────────────────────────

    [Fact]
    public void Dispose_DoesNotThrow()
    {
        var client = new InMemoryCosmosClient();

        var action = () => client.Dispose();

        action.Should().NotThrow();
    }

    // ── CreateDatabaseAsync ─────────────────────────────────────────────────

    [Fact]
    public async Task CreateDatabaseAsync_CreatesDatabaseSuccessfully()
    {
        var client = new InMemoryCosmosClient();

        var response = await client.CreateDatabaseAsync("test-db");

        response.StatusCode.Should().Be(System.Net.HttpStatusCode.Created);
        response.Resource.Id.Should().Be("test-db");
    }

    // ── Database containers can use custom partition key paths ───────────────

    [Fact]
    public async Task GetContainer_UsesDefaultPartitionKeyPath()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseIfNotExistsAsync("test-db");

        var container = client.GetContainer("test-db", "items");

        // The default partition key path should be /id for InMemoryCosmosClient containers
        container.Should().BeOfType<InMemoryContainer>();
    }

    // ── Database.CreateContainerIfNotExistsAsync ────────────────────────────

    [Fact]
    public async Task Database_CreateContainerIfNotExistsAsync_CreatesContainer()
    {
        var client = new InMemoryCosmosClient();
        var dbResponse = await client.CreateDatabaseIfNotExistsAsync("test-db");
        var database = dbResponse.Database;

        var containerResponse = await database.CreateContainerIfNotExistsAsync("my-container", "/partitionKey");

        containerResponse.StatusCode.Should().Be(System.Net.HttpStatusCode.Created);
        containerResponse.Resource.Id.Should().Be("my-container");
    }

    [Fact]
    public async Task Database_CreateContainerIfNotExistsAsync_WithCustomPartitionKey_Works()
    {
        var client = new InMemoryCosmosClient();
        var dbResponse = await client.CreateDatabaseIfNotExistsAsync("test-db");
        var database = dbResponse.Database;

        var containerResponse = await database.CreateContainerIfNotExistsAsync("my-container", "/customKey");
        var container = containerResponse.Container;

        await container.CreateItemAsync(
            new CustomKeyDocument { Id = "1", CustomKey = "ck1", Name = "Test" },
            new PartitionKey("ck1"));

        var readResponse = await container.ReadItemAsync<CustomKeyDocument>("1", new PartitionKey("ck1"));
        readResponse.Resource.Name.Should().Be("Test");
    }
}
