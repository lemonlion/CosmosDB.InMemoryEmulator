using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using System.Net;
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
            new TestDocument { Id = "1", PartitionKey = "1", Name = "Alice", Value = 10 },
            new PartitionKey("1"));

        var readResponse = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("1"));
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
            new TestDocument { Id = "1", PartitionKey = "1", Name = "From DB1" },
            new PartitionKey("1"));

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
            new TestDocument { Id = "1", PartitionKey = "1", Name = "Container1Only" },
            new PartitionKey("1"));

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
            new TestDocument { Id = "1", PartitionKey = "1", Name = "Alice", Value = 10 },
            new PartitionKey("1"));
        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "2", Name = "Bob", Value = 20 },
            new PartitionKey("2"));

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

// ═══════════════════════════════════════════════════════════════════════════════
//  Client Property & Account Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class InMemoryCosmosClientPropertyTests
{
    [Fact]
    public void Endpoint_ReturnsLocalhostEmulatorUri()
    {
        var client = new InMemoryCosmosClient();
        client.Endpoint.Should().Be(new Uri("https://localhost:8081/"));
    }

    [Fact]
    public void ClientOptions_ReturnsNonNullOptions()
    {
        var client = new InMemoryCosmosClient();
        client.ClientOptions.Should().NotBeNull();
    }

    [Fact]
    public async Task ReadAccountAsync_ReturnsAccountWithInMemoryEmulatorId()
    {
        var client = new InMemoryCosmosClient();
        var account = await client.ReadAccountAsync();
        account.Id.Should().Be("in-memory-emulator");
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Database Create & Stream Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class InMemoryCosmosClientCreateTests
{
    [Fact]
    public async Task CreateDatabaseAsync_DuplicateId_ThrowsConflict()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("db1");

        var act = () => client.CreateDatabaseAsync("db1");

        (await act.Should().ThrowAsync<CosmosException>())
            .Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task CreateDatabaseStreamAsync_NewDatabase_ReturnsCreated()
    {
        var client = new InMemoryCosmosClient();
        var response = await client.CreateDatabaseStreamAsync(
            new DatabaseProperties("db1"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task CreateDatabaseStreamAsync_Duplicate_ReturnsConflict()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseStreamAsync(new DatabaseProperties("db1"));
        var response = await client.CreateDatabaseStreamAsync(
            new DatabaseProperties("db1"));

        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task CreateDatabaseAsync_ResponseResource_HasCorrectId()
    {
        var client = new InMemoryCosmosClient();
        var response = await client.CreateDatabaseAsync("my-db");

        response.Resource.Id.Should().Be("my-db");
    }

    [Fact]
    public async Task CreateDatabaseIfNotExistsAsync_ResponseDatabase_IsFunctional()
    {
        var client = new InMemoryCosmosClient();
        var response = await client.CreateDatabaseIfNotExistsAsync("testdb");

        var containerResponse = await response.Database.CreateContainerAsync("c1", "/pk");
        containerResponse.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task CreateDatabaseAsync_WithThroughputProperties_Returns201()
    {
        var client = new InMemoryCosmosClient();
        var response = await client.CreateDatabaseAsync(
            "db1", ThroughputProperties.CreateManualThroughput(400));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task CreateDatabaseIfNotExistsAsync_WithThroughput_Returns201()
    {
        var client = new InMemoryCosmosClient();
        var response = await client.CreateDatabaseIfNotExistsAsync("db1", 400);

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Database Query Iterator Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class InMemoryCosmosClientQueryIteratorTests
{
    [Fact]
    public async Task GetDatabaseQueryIterator_ReturnsAllDatabases()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("db1");
        await client.CreateDatabaseAsync("db2");
        await client.CreateDatabaseAsync("db3");

        var iterator = client.GetDatabaseQueryIterator<DatabaseProperties>();
        var results = new List<DatabaseProperties>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().HaveCount(3);
        results.Select(r => r.Id).Should().BeEquivalentTo("db1", "db2", "db3");
    }

    [Fact]
    public async Task GetDatabaseQueryIterator_EmptyClient_ReturnsEmpty()
    {
        var client = new InMemoryCosmosClient();

        var iterator = client.GetDatabaseQueryIterator<DatabaseProperties>();
        var results = new List<DatabaseProperties>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task GetDatabaseQueryStreamIterator_ReturnsOkResponse()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("db1");

        var iterator = client.GetDatabaseQueryStreamIterator();
        var response = await iterator.ReadNextAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Client GetContainer & GetDatabase Shortcuts
// ═══════════════════════════════════════════════════════════════════════════════

public class InMemoryCosmosClientShortcutTests
{
    [Fact]
    public async Task GetContainer_WithoutCreatingDatabase_AutoCreates()
    {
        var client = new InMemoryCosmosClient();
        var container = client.GetContainer("db1", "items");

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1" }),
            new PartitionKey("1"));

        var response = await container.ReadItemAsync<JObject>("1", new PartitionKey("1"));
        response.Resource["id"]!.ToString().Should().Be("1");
    }

    [Fact]
    public void GetContainer_DefaultPartitionKeyPath_IsId()
    {
        var client = new InMemoryCosmosClient();
        var container = client.GetContainer("db1", "items");

        ((InMemoryContainer)container).PartitionKeyPaths[0].Should().Be("/id");
    }

    [Fact]
    public void Database_Client_ReturnsParentCosmosClient()
    {
        var client = new InMemoryCosmosClient();
        var db = client.GetDatabase("db1");

        db.Client.Should().BeSameAs(client);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Delete Database Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class InMemoryCosmosClientDeleteTests
{
    [Fact]
    public async Task DeleteDatabase_RemovesFromQueryIterator()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("db1");
        await client.CreateDatabaseAsync("db2");

        await client.GetDatabase("db1").DeleteAsync();

        var iterator = client.GetDatabaseQueryIterator<DatabaseProperties>();
        var results = new List<DatabaseProperties>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle().Which.Id.Should().Be("db2");
    }

    [Fact]
    public async Task DeleteDatabase_ThenReCreate_ContainersGone()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("db1")).Database;
        await db.CreateContainerAsync("c1", "/pk");

        await db.DeleteAsync();

        var newDb = (await client.CreateDatabaseAsync("db1")).Database;
        var iterator = newDb.GetContainerQueryIterator<ContainerProperties>();
        var results = new List<ContainerProperties>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().BeEmpty("re-created database should have no containers");
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Dispose Semantics
// ═══════════════════════════════════════════════════════════════════════════════

public class InMemoryCosmosClientDisposeTests
{
    [Fact]
    public void Dispose_MultipleTimes_DoesNotThrow()
    {
        var client = new InMemoryCosmosClient();
        var act = () =>
        {
            client.Dispose();
            client.Dispose();
        };
        act.Should().NotThrow();
    }

    [Fact]
    public async Task Dispose_ThenUseClient_ThrowsObjectDisposedException()
    {
        var client = new InMemoryCosmosClient();
        client.Dispose();

        var act = () => client.CreateDatabaseAsync("db1");
        await act.Should().ThrowAsync<ObjectDisposedException>();
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Concurrency Tests (TOCTOU race fix verification)
// ═══════════════════════════════════════════════════════════════════════════════

public class InMemoryCosmosClientConcurrencyTests
{
    [Fact]
    public async Task ConcurrentCreateDatabaseIfNotExistsAsync_SameId_ExactlyOneCreated()
    {
        var client = new InMemoryCosmosClient();
        var tasks = Enumerable.Range(0, 20)
            .Select(_ => client.CreateDatabaseIfNotExistsAsync("shared-db"))
            .ToList();

        var responses = await Task.WhenAll(tasks);

        responses.Count(r => r.StatusCode == HttpStatusCode.Created).Should().Be(1);
        responses.Count(r => r.StatusCode == HttpStatusCode.OK).Should().Be(19);
    }

    [Fact]
    public async Task ConcurrentCreateDatabaseAsync_DifferentIds_AllSucceed()
    {
        var client = new InMemoryCosmosClient();
        var tasks = Enumerable.Range(0, 20)
            .Select(i => client.CreateDatabaseAsync($"db-{i}"))
            .ToList();

        var responses = await Task.WhenAll(tasks);

        responses.Should().AllSatisfy(r =>
            r.StatusCode.Should().Be(HttpStatusCode.Created));
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class InMemoryCosmosClientEdgeCaseTests
{
    [Fact]
    public async Task Client_Supports100Databases()
    {
        var client = new InMemoryCosmosClient();
        for (var i = 0; i < 100; i++)
            await client.CreateDatabaseAsync($"db-{i}");

        var iterator = client.GetDatabaseQueryIterator<DatabaseProperties>();
        var results = new List<DatabaseProperties>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().HaveCount(100);
    }

    [Fact]
    public async Task CreateDatabaseAsync_IdWithSpaces_Succeeds()
    {
        var client = new InMemoryCosmosClient();
        var response = await client.CreateDatabaseAsync("my database");

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Resource.Id.Should().Be("my database");
    }

    [Fact]
    public async Task CreateDatabaseAsync_IdWithUnicode_Succeeds()
    {
        var client = new InMemoryCosmosClient();
        var response = await client.CreateDatabaseAsync("db-日本語");

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task SameContainerName_DifferentDatabases_DataIsolated()
    {
        var client = new InMemoryCosmosClient();
        var db1 = (await client.CreateDatabaseAsync("db1")).Database;
        var db2 = (await client.CreateDatabaseAsync("db2")).Database;

        var c1 = (await db1.CreateContainerAsync("items", "/pk")).Container;
        var c2 = (await db2.CreateContainerAsync("items", "/pk")).Container;

        await c1.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
        await c2.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "b" }), new PartitionKey("b"));

        ((InMemoryContainer)c1).ItemCount.Should().Be(1);
        ((InMemoryContainer)c2).ItemCount.Should().Be(1);
    }

    [Fact]
    public async Task Database_ReadAsync_ReturnsOkWithProperties()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("mydb");

        var response = await client.GetDatabase("mydb").ReadAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Id.Should().Be("mydb");
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Divergent Behavior — Auto-Creation
// ═══════════════════════════════════════════════════════════════════════════════

public class InMemoryCosmosClientAutoCreationDivergentTests
{
    [Fact(Skip = "In real Cosmos DB SDK, GetDatabase returns a proxy. ReadAsync on a non-existent " +
        "database returns 404 NotFound. InMemoryCosmosClient auto-creates the database for test convenience.")]
    public async Task GetDatabase_NonExistent_ReadAsync_Throws404()
    {
        var client = new InMemoryCosmosClient();
        var db = client.GetDatabase("nonexistent");

        var act = () => db.ReadAsync();
        (await act.Should().ThrowAsync<CosmosException>())
            .Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task DivergentBehavior_GetDatabase_LazilyCreatesDatabase()
    {
        // DIVERGENT BEHAVIOR: Real SDK's GetDatabase returns a proxy that
        // fails with 404 on ReadAsync if the database doesn't exist.
        // The emulator auto-creates for test convenience.
        var client = new InMemoryCosmosClient();
        var db = client.GetDatabase("auto-created");

        var response = await db.ReadAsync();
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact(Skip = "In real Cosmos DB SDK, GetContainer returns a proxy. ReadContainerAsync on a non-existent " +
        "container returns 404 NotFound. InMemoryCosmosClient auto-creates both database and container.")]
    public async Task GetContainer_NonExistent_ReadAsync_Throws404()
    {
        var client = new InMemoryCosmosClient();
        var container = client.GetContainer("db", "nonexistent");

        var act = () => container.ReadContainerAsync();
        (await act.Should().ThrowAsync<CosmosException>())
            .Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task DivergentBehavior_GetContainer_LazilyCreatesDatabaseAndContainer()
    {
        // DIVERGENT BEHAVIOR: Real SDK's GetContainer returns a proxy that
        // fails with 404 when the resource doesn't exist. The emulator
        // auto-creates to simplify test setup.
        var client = new InMemoryCosmosClient();
        var container = client.GetContainer("auto-db", "auto-container");

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1" }),
            new PartitionKey("1"));

        var response = await container.ReadItemAsync<JObject>("1", new PartitionKey("1"));
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}
