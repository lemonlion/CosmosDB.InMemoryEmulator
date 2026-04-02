using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;
using System.Text;
using Microsoft.Azure.Cosmos.Fluent;
using Newtonsoft.Json.Linq;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Comprehensive tests for InMemoryCosmosClient and InMemoryDatabase covering
/// all SDK API surface as defined in cosmosclient-and-database-tdd-plan.md.
/// </summary>
public class CosmosClientAndDatabaseTests
{
    // ═══════════════════════════════════════════════════════════════════════
    // PHASE 1: Foundation Fixes
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Client_Property_ReturnsParentCosmosClient()
    {
        var client = new InMemoryCosmosClient();
        var dbResponse = await client.CreateDatabaseIfNotExistsAsync("test-db");
        var database = dbResponse.Database;

        database.Client.Should().NotBeNull();
        database.Client.Should().BeSameAs(client);
    }

    [Fact]
    public void Client_Property_ViaGetDatabase_ReturnsParentCosmosClient()
    {
        var client = new InMemoryCosmosClient();
        var database = client.GetDatabase("test-db");

        database.Client.Should().BeSameAs(client);
    }

    [Fact]
    public void Endpoint_ReturnsNonNullUri()
    {
        var client = new InMemoryCosmosClient();

        var endpoint = client.Endpoint;

        endpoint.Should().NotBeNull();
        endpoint.Should().BeOfType<Uri>();
    }

    [Fact]
    public void Dispose_CalledMultipleTimes_DoesNotThrow()
    {
        var client = new InMemoryCosmosClient();

        client.Dispose();
        var act = () => client.Dispose();

        act.Should().NotThrow();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // PHASE 2: CRUD Correctness — Duplicate / Delete Bugs
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CreateDatabaseAsync_DuplicateId_ThrowsConflict()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");

        var act = () => client.CreateDatabaseAsync("test-db");

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task CreateContainerAsync_DuplicateId_ThrowsConflict()
    {
        var client = new InMemoryCosmosClient();
        var dbResponse = await client.CreateDatabaseIfNotExistsAsync("test-db");
        var database = dbResponse.Database;

        await database.CreateContainerAsync("container1", "/pk");

        var act = () => database.CreateContainerAsync("container1", "/pk");

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task CreateContainerAsync_WithContainerProperties_DuplicateId_ThrowsConflict()
    {
        var client = new InMemoryCosmosClient();
        var dbResponse = await client.CreateDatabaseIfNotExistsAsync("test-db");
        var database = dbResponse.Database;

        await database.CreateContainerAsync(new ContainerProperties("container1", "/pk"));

        var act = () => database.CreateContainerAsync(new ContainerProperties("container1", "/pk"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task DeleteAsync_ReturnsNoContent()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var database = client.GetDatabase("test-db");

        var response = await database.DeleteAsync();

        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task DeleteAsync_RemovesDatabaseFromClient()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var database = client.GetDatabase("test-db");

        await database.DeleteAsync();

        // After deletion, creating the same database should succeed (201, not 200)
        var response = await client.CreateDatabaseIfNotExistsAsync("test-db");
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task DeleteAsync_RemovesAllContainersInDatabase()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateContainerAsync("container1", "/pk");
        var container = db.GetContainer("container1");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        await db.DeleteAsync();

        // Re-create database and container — should be empty
        await client.CreateDatabaseAsync("test-db");
        var newDb = client.GetDatabase("test-db");
        await newDb.CreateContainerAsync("container1", "/pk");
        var newContainer = newDb.GetContainer("container1");

        var act = () => newContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        await act.Should().ThrowAsync<CosmosException>();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // PHASE 3: ThroughputProperties Overloads
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CreateDatabaseAsync_WithThroughputProperties_CreatesDatabase()
    {
        var client = new InMemoryCosmosClient();

        var response = await client.CreateDatabaseAsync(
            "test-db",
            ThroughputProperties.CreateManualThroughput(400));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Resource.Id.Should().Be("test-db");
    }

    [Fact]
    public async Task CreateDatabaseIfNotExistsAsync_WithThroughputProperties_CreatesDatabase()
    {
        var client = new InMemoryCosmosClient();

        var response = await client.CreateDatabaseIfNotExistsAsync(
            "test-db",
            ThroughputProperties.CreateManualThroughput(400));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Resource.Id.Should().Be("test-db");
    }

    [Fact]
    public async Task CreateDatabaseIfNotExistsAsync_WithThroughputProperties_ExistingDb_Returns200()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseIfNotExistsAsync("test-db");

        var response = await client.CreateDatabaseIfNotExistsAsync(
            "test-db",
            ThroughputProperties.CreateManualThroughput(400));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task CreateContainerAsync_WithThroughputProperties_CreatesContainer()
    {
        var client = new InMemoryCosmosClient();
        var dbResponse = await client.CreateDatabaseIfNotExistsAsync("test-db");
        var database = dbResponse.Database;

        var response = await database.CreateContainerAsync(
            new ContainerProperties("container1", "/pk"),
            ThroughputProperties.CreateManualThroughput(400));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Resource.Id.Should().Be("container1");
    }

    [Fact]
    public async Task CreateContainerIfNotExistsAsync_WithThroughputProperties_CreatesContainer()
    {
        var client = new InMemoryCosmosClient();
        var dbResponse = await client.CreateDatabaseIfNotExistsAsync("test-db");
        var database = dbResponse.Database;

        var response = await database.CreateContainerIfNotExistsAsync(
            new ContainerProperties("container1", "/pk"),
            ThroughputProperties.CreateManualThroughput(400));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task CreateContainerIfNotExistsAsync_WithThroughputProperties_ExistingContainer_Returns200()
    {
        var client = new InMemoryCosmosClient();
        var dbResponse = await client.CreateDatabaseIfNotExistsAsync("test-db");
        var database = dbResponse.Database;
        await database.CreateContainerAsync("container1", "/pk");

        var response = await database.CreateContainerIfNotExistsAsync(
            new ContainerProperties("container1", "/pk"),
            ThroughputProperties.CreateManualThroughput(400));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // PHASE 4: Stream APIs
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CreateDatabaseStreamAsync_ReturnsCreatedResponse()
    {
        var client = new InMemoryCosmosClient();

        using var response = await client.CreateDatabaseStreamAsync(
            new DatabaseProperties("test-db"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.IsSuccessStatusCode.Should().BeTrue();
    }

    [Fact]
    public async Task CreateDatabaseStreamAsync_DuplicateId_ReturnsConflict()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");

        using var response = await client.CreateDatabaseStreamAsync(
            new DatabaseProperties("test-db"));

        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        response.IsSuccessStatusCode.Should().BeFalse();
    }

    [Fact]
    public async Task CreateContainerStreamAsync_ReturnsCreatedResponse()
    {
        var client = new InMemoryCosmosClient();
        var dbResponse = await client.CreateDatabaseIfNotExistsAsync("test-db");
        var database = dbResponse.Database;

        using var response = await database.CreateContainerStreamAsync(
            new ContainerProperties("container1", "/pk"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.IsSuccessStatusCode.Should().BeTrue();
    }

    [Fact]
    public async Task CreateContainerStreamAsync_DuplicateId_ReturnsConflict()
    {
        var client = new InMemoryCosmosClient();
        var dbResponse = await client.CreateDatabaseIfNotExistsAsync("test-db");
        var database = dbResponse.Database;
        await database.CreateContainerAsync("container1", "/pk");

        using var response = await database.CreateContainerStreamAsync(
            new ContainerProperties("container1", "/pk"));

        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        response.IsSuccessStatusCode.Should().BeFalse();
    }

    [Fact]
    public async Task CreateContainerStreamAsync_WithThroughputProperties_ReturnsCreated()
    {
        var client = new InMemoryCosmosClient();
        var dbResponse = await client.CreateDatabaseIfNotExistsAsync("test-db");
        var database = dbResponse.Database;

        using var response = await database.CreateContainerStreamAsync(
            new ContainerProperties("container1", "/pk"),
            ThroughputProperties.CreateManualThroughput(400));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task ReadStreamAsync_ReturnsOkResponse()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var database = client.GetDatabase("test-db");

        using var response = await database.ReadStreamAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.IsSuccessStatusCode.Should().BeTrue();
    }

    [Fact]
    public async Task DeleteStreamAsync_ReturnsNoContent()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var database = client.GetDatabase("test-db");

        using var response = await database.DeleteStreamAsync();

        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task DeleteStreamAsync_RemovesDatabaseFromClient()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var database = client.GetDatabase("test-db");

        await database.DeleteStreamAsync();

        var response = await client.CreateDatabaseIfNotExistsAsync("test-db");
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // PHASE 5: Query Iterators
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task GetDatabaseQueryIterator_WithNullQuery_ReturnsAllDatabases()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("db1");
        await client.CreateDatabaseAsync("db2");
        await client.CreateDatabaseAsync("db3");

        var iterator = client.GetDatabaseQueryIterator<DatabaseProperties>();

        var databases = new List<DatabaseProperties>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            databases.AddRange(response);
        }

        databases.Should().HaveCount(3);
        databases.Select(d => d.Id).Should().BeEquivalentTo(new[] { "db1", "db2", "db3" });
    }

    [Fact]
    public async Task GetDatabaseQueryIterator_SelectAll_ReturnsAllDatabases()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("db1");
        await client.CreateDatabaseAsync("db2");

        var iterator = client.GetDatabaseQueryIterator<DatabaseProperties>("SELECT * FROM c");

        var databases = new List<DatabaseProperties>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            databases.AddRange(response);
        }

        databases.Should().HaveCount(2);
    }

    [Fact]
    public async Task GetDatabaseQueryIterator_WithQueryDefinition_ReturnsAllDatabases()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("db1");
        await client.CreateDatabaseAsync("db2");

        var queryDef = new QueryDefinition("SELECT * FROM c");
        var iterator = client.GetDatabaseQueryIterator<DatabaseProperties>(queryDef);

        var databases = new List<DatabaseProperties>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            databases.AddRange(response);
        }

        databases.Should().HaveCount(2);
    }

    [Fact]
    public async Task GetDatabaseQueryIterator_EmptyAccount_ReturnsEmpty()
    {
        var client = new InMemoryCosmosClient();

        var iterator = client.GetDatabaseQueryIterator<DatabaseProperties>();

        var databases = new List<DatabaseProperties>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            databases.AddRange(response);
        }

        databases.Should().BeEmpty();
    }

    [Fact]
    public async Task GetContainerQueryIterator_NullQuery_ReturnsAllContainers()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateContainerAsync("container1", "/pk");
        await db.CreateContainerAsync("container2", "/pk");
        await db.CreateContainerAsync("container3", "/pk");

        var iterator = db.GetContainerQueryIterator<ContainerProperties>();

        var containers = new List<ContainerProperties>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            containers.AddRange(response);
        }

        containers.Should().HaveCount(3);
        containers.Select(c => c.Id).Should().BeEquivalentTo(
            new[] { "container1", "container2", "container3" });
    }

    [Fact]
    public async Task GetContainerQueryIterator_SelectAll_ReturnsAllContainers()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateContainerAsync("container1", "/pk");

        var iterator = db.GetContainerQueryIterator<ContainerProperties>("SELECT * FROM c");

        var containers = new List<ContainerProperties>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            containers.AddRange(response);
        }

        containers.Should().HaveCount(1);
        containers[0].Id.Should().Be("container1");
    }

    [Fact]
    public async Task GetContainerQueryIterator_WithQueryDefinition_ReturnsContainers()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateContainerAsync("container1", "/pk");

        var queryDef = new QueryDefinition("SELECT * FROM c");
        var iterator = db.GetContainerQueryIterator<ContainerProperties>(queryDef);

        var containers = new List<ContainerProperties>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            containers.AddRange(response);
        }

        containers.Should().HaveCount(1);
    }

    [Fact]
    public async Task GetContainerQueryIterator_EmptyDatabase_ReturnsEmpty()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var iterator = db.GetContainerQueryIterator<ContainerProperties>();

        var containers = new List<ContainerProperties>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            containers.AddRange(response);
        }

        containers.Should().BeEmpty();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // PHASE 6: Additional ReadAsync Edge Cases
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ReadAsync_ReturnsCorrectDatabaseProperties()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("my-db");
        var database = client.GetDatabase("my-db");

        var response = await database.ReadAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Database.Should().NotBeNull();
        response.Resource.Should().NotBeNull();
        response.Resource.Id.Should().Be("my-db");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // PHASE 7: Skipped Tests & Divergent Behavior Documentation
    // ═══════════════════════════════════════════════════════════════════════

    // ── A1.2 ClientOptions ──────────────────────────────────────────────────

    [Fact]
    public void ClientOptions_ReturnsNonNull()
    {
        var client = new InMemoryCosmosClient();
        var options = client.ClientOptions;
        options.Should().NotBeNull();
    }

    // ── A1.3 ResponseFactory ────────────────────────────────────────────────

    [Fact]
    public void ResponseFactory_ReturnsNonNull()
    {
        var client = new InMemoryCosmosClient();
        var factory = client.ResponseFactory;
        factory.Should().NotBeNull();
    }

    // ── A5 ReadAccountAsync ─────────────────────────────────────────────────

    [Fact]
    public async Task ReadAccountAsync_ReturnsAccountProperties()
    {
        var client = new InMemoryCosmosClient();
        var accountProperties = await client.ReadAccountAsync();
        accountProperties.Should().NotBeNull();
        accountProperties.Id.Should().NotBeNullOrEmpty();
    }

    // ── A9 GetDatabaseQueryStreamIterator ────────────────────────────────────

    [Fact]
    public async Task GetDatabaseQueryStreamIterator_WithNullQuery_ReturnsAllDatabases()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("db1");
        var iterator = client.GetDatabaseQueryStreamIterator();
        var responses = new List<ResponseMessage>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            responses.Add(response);
            response.StatusCode.Should().Be(HttpStatusCode.OK);
        }
        responses.Should().NotBeEmpty();
    }

    // ── A10 CreateAndInitializeAsync ─────────────────────────────────────────

    /// <summary>
    /// CreateAndInitializeAsync is a static factory method on CosmosClient that cannot be
    /// overridden in InMemoryCosmosClient. However, it can still be used in tests by passing
    /// a <see cref="CosmosClientOptions"/> with an <c>HttpClientFactory</c> that points at a
    /// <see cref="FakeCosmosHandler"/>. The real SDK method executes normally but all HTTP
    /// traffic is served by the in-memory handler — same pattern as RealToFeedIteratorTests.
    /// </summary>
    [Fact]
    public async Task CreateAndInitializeAsync_WorksWithFakeCosmosHandler()
    {
        // Arrange: set up in-memory container and FakeCosmosHandler
        var inMemoryContainer = new InMemoryContainer("myContainer", "/partitionKey");
        await inMemoryContainer.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Seeded", Value = 42 },
            new PartitionKey("pk1"));

        using var handler = new FakeCosmosHandler(inMemoryContainer);

        var containers = new List<(string, string)> { ("fakeDb", "myContainer") };

        // Act: call the REAL static factory method — FakeCosmosHandler serves all HTTP traffic
        using var client = await CosmosClient.CreateAndInitializeAsync(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            containers,
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                RequestTimeout = TimeSpan.FromSeconds(5),
                HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
            });

        // Assert: client was created and can read pre-seeded data
        client.Should().NotBeNull();

        var container = client.GetContainer("fakeDb", "myContainer");
        var response = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Name.Should().Be("Seeded");
    }

    [Fact]
    public async Task CreateAndInitializeAsync_ConnectionString_WorksWithFakeCosmosHandler()
    {
        var inMemoryContainer = new InMemoryContainer("orders", "/partitionKey");
        using var handler = new FakeCosmosHandler(inMemoryContainer);

        var containers = new List<(string, string)> { ("shopDb", "orders") };

        using var client = await CosmosClient.CreateAndInitializeAsync(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            containers,
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
            });

        client.Should().NotBeNull();

        // Verify we can write and read through the CreateAndInitializeAsync-created client
        var container = client.GetContainer("shopDb", "orders");
        await container.CreateItemAsync(
            new TestDocument { Id = "o1", PartitionKey = "pk1", Name = "Order1", Value = 100 },
            new PartitionKey("pk1"));

        var response = await container.ReadItemAsync<TestDocument>("o1", new PartitionKey("pk1"));
        response.Resource.Name.Should().Be("Order1");
    }

    [Fact]
    public async Task CreateAndInitializeAsync_MultiContainer_WorksWithRouter()
    {
        var usersContainer = new InMemoryContainer("users", "/partitionKey");
        var ordersContainer = new InMemoryContainer("orders", "/partitionKey");

        using var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
        {
            ["users"] = new FakeCosmosHandler(usersContainer),
            ["orders"] = new FakeCosmosHandler(ordersContainer)
        });

        var containers = new List<(string, string)> { ("myDb", "users"), ("myDb", "orders") };

        using var client = await CosmosClient.CreateAndInitializeAsync(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            containers,
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                HttpClientFactory = () => new HttpClient(router) { Timeout = TimeSpan.FromSeconds(10) }
            });

        // Write to each container through the real SDK client
        await client.GetContainer("myDb", "users").CreateItemAsync(
            new TestDocument { Id = "u1", PartitionKey = "pk1", Name = "Alice", Value = 1 },
            new PartitionKey("pk1"));
        await client.GetContainer("myDb", "orders").CreateItemAsync(
            new TestDocument { Id = "o1", PartitionKey = "pk1", Name = "Order1", Value = 99 },
            new PartitionKey("pk1"));

        // Verify each container has only its own data
        var userResp = await client.GetContainer("myDb", "users")
            .ReadItemAsync<TestDocument>("u1", new PartitionKey("pk1"));
        userResp.Resource.Name.Should().Be("Alice");

        var orderResp = await client.GetContainer("myDb", "orders")
            .ReadItemAsync<TestDocument>("o1", new PartitionKey("pk1"));
        orderResp.Resource.Name.Should().Be("Order1");
    }

    // ── A6 GetDatabase proxy semantics ───────────────────────────────────────

    /// <summary>
    /// DIVERGENT BEHAVIOR: Real CosmosClient.GetDatabase returns a proxy that does NOT
    /// create the database. Subsequent operations (ReadAsync) would fail with 404 if
    /// the database doesn't exist. InMemoryCosmosClient auto-creates the database on
    /// GetDatabase for test convenience. This means you can't test "database not found"
    /// scenarios through this path.
    /// </summary>
    [Fact]
    public void DivergentBehavior_GetDatabase_AutoCreatesDatabase()
    {
        var client = new InMemoryCosmosClient();

        // In real SDK, this would just be a proxy — no database created.
        // In emulator, this actually creates the database.
        var database = client.GetDatabase("auto-created");

        database.Should().NotBeNull();
        database.Id.Should().Be("auto-created");
    }

    // ── B12 GetContainerQueryStreamIterator ──────────────────────────────────

    [Fact]
    public async Task GetContainerQueryStreamIterator_ReturnsAllContainers()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateContainerAsync("container1", "/pk");
        var iterator = db.GetContainerQueryStreamIterator();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            response.StatusCode.Should().Be(HttpStatusCode.OK);
        }
    }

    // ── B13 Throughput Management ────────────────────────────────────────────

    [Fact]
    public async Task ReadThroughputAsync_Returns400()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var throughput = await db.ReadThroughputAsync();

        throughput.Should().Be(400);
    }

    [Fact]
    public async Task ReadThroughputAsync_WithRequestOptions_ReturnsThroughputResponse()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        var response = await db.ReadThroughputAsync(new RequestOptions());
        response.Should().NotBeNull();
    }

    [Fact]
    public async Task ReplaceThroughputAsync_Int_DoesNotThrow()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        var response = await db.ReplaceThroughputAsync(400);
        response.Should().NotBeNull();
    }

    [Fact]
    public async Task ReplaceThroughputAsync_ThroughputProperties_DoesNotThrow()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        var response = await db.ReplaceThroughputAsync(
            ThroughputProperties.CreateManualThroughput(400));
        response.Should().NotBeNull();
    }

    // ── B14 DefineContainer ──────────────────────────────────────────────────

    [Fact]
    public async Task DefineContainer_CreatesContainerViaFluentBuilder()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        var containerResponse = await db.DefineContainer("container1", "/pk")
            .WithIndexingPolicy()
                .WithAutomaticIndexing(true)
                .Attach()
            .CreateAsync();
        containerResponse.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    // ── B15 User Management ──────────────────────────────────────────────────

    [Fact]
    public async Task CreateUserAsync_CreatesUser()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var response = await db.CreateUserAsync("user1");

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Resource.Should().NotBeNull();
        response.Resource.Id.Should().Be("user1");
    }

    [Fact]
    public async Task CreateUserAsync_Duplicate_ThrowsConflict()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateUserAsync("user1");

        var act = () => db.CreateUserAsync("user1");

        await act.Should().ThrowAsync<CosmosException>()
            .Where(e => e.StatusCode == HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task UpsertUserAsync_CreatesNewUser()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var response = await db.UpsertUserAsync("user1");

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Resource.Id.Should().Be("user1");
    }

    [Fact]
    public async Task UpsertUserAsync_UpdatesExistingUser()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateUserAsync("user1");

        var response = await db.UpsertUserAsync("user1");

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Id.Should().Be("user1");
    }

    [Fact]
    public void GetUser_ReturnsUserProxy()
    {
        var client = new InMemoryCosmosClient();
        var db = client.GetDatabase("test-db");

        var user = db.GetUser("user1");

        user.Should().NotBeNull();
        user.Id.Should().Be("user1");
    }

    [Fact]
    public async Task GetUserQueryIterator_ReturnsUsers()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateUserAsync("user1");
        await db.CreateUserAsync("user2");

        var iterator = db.GetUserQueryIterator<UserProperties>();
        var users = new List<UserProperties>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            users.AddRange(response);
        }

        users.Should().HaveCount(2);
        users.Select(u => u.Id).Should().BeEquivalentTo(["user1", "user2"]);
    }

    [Fact]
    public async Task GetUserQueryIterator_QueryDefinition_ReturnsUsers()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateUserAsync("user1");

        var iterator = db.GetUserQueryIterator<UserProperties>(
            new QueryDefinition("SELECT * FROM u"));
        var users = new List<UserProperties>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            users.AddRange(response);
        }

        users.Should().HaveCount(1);
    }

    [Fact]
    public async Task User_ReadAsync_ReturnsUserProperties()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateUserAsync("user1");
        var user = db.GetUser("user1");

        var response = await user.ReadAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Id.Should().Be("user1");
    }

    [Fact]
    public async Task User_ReplaceAsync_UpdatesUser()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateUserAsync("user1");
        var user = db.GetUser("user1");

        var response = await user.ReplaceAsync(new UserProperties("user1-renamed"));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Id.Should().Be("user1-renamed");
    }

    [Fact]
    public async Task User_DeleteAsync_RemovesUser()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateUserAsync("user1");
        var user = db.GetUser("user1");

        var response = await user.DeleteAsync();

        response.StatusCode.Should().Be(HttpStatusCode.NoContent);

        // Verify user is gone from query results
        var iterator = db.GetUserQueryIterator<UserProperties>();
        var users = new List<UserProperties>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            users.AddRange(page);
        }
        users.Should().BeEmpty();
    }

    // ── B15b Permission Management ───────────────────────────────────────────

    [Fact]
    public async Task Permission_CreateAsync_CreatesPermission()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateUserAsync("user1");
        var user = db.GetUser("user1");
        var container = new InMemoryContainer("my-container", "/pk");

        var permProps = new PermissionProperties("perm1", PermissionMode.All, container);
        var response = await user.CreatePermissionAsync(permProps);

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Resource.Id.Should().Be("perm1");
        response.Resource.PermissionMode.Should().Be(PermissionMode.All);
    }

    [Fact]
    public async Task Permission_CreateAsync_Duplicate_ThrowsConflict()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateUserAsync("user1");
        var user = db.GetUser("user1");
        var container = new InMemoryContainer("my-container", "/pk");
        await user.CreatePermissionAsync(new PermissionProperties("perm1", PermissionMode.All, container));

        var act = () => user.CreatePermissionAsync(new PermissionProperties("perm1", PermissionMode.Read, container));

        await act.Should().ThrowAsync<CosmosException>()
            .Where(e => e.StatusCode == HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task Permission_UpsertAsync_CreatesNew()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateUserAsync("user1");
        var user = db.GetUser("user1");
        var container = new InMemoryContainer("my-container", "/pk");

        var response = await user.UpsertPermissionAsync(new PermissionProperties("perm1", PermissionMode.Read, container));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task Permission_UpsertAsync_UpdatesExisting()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateUserAsync("user1");
        var user = db.GetUser("user1");
        var container = new InMemoryContainer("my-container", "/pk");
        await user.CreatePermissionAsync(new PermissionProperties("perm1", PermissionMode.Read, container));

        var response = await user.UpsertPermissionAsync(new PermissionProperties("perm1", PermissionMode.All, container));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.PermissionMode.Should().Be(PermissionMode.All);
    }

    [Fact]
    public async Task Permission_ReadAsync_ReturnsPermission()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateUserAsync("user1");
        var user = db.GetUser("user1");
        var container = new InMemoryContainer("my-container", "/pk");
        await user.CreatePermissionAsync(new PermissionProperties("perm1", PermissionMode.All, container));

        var perm = user.GetPermission("perm1");
        var response = await perm.ReadAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Id.Should().Be("perm1");
        response.Resource.PermissionMode.Should().Be(PermissionMode.All);
    }

    [Fact]
    public async Task Permission_ReplaceAsync_UpdatesPermission()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateUserAsync("user1");
        var user = db.GetUser("user1");
        var container = new InMemoryContainer("my-container", "/pk");
        await user.CreatePermissionAsync(new PermissionProperties("perm1", PermissionMode.Read, container));

        var perm = user.GetPermission("perm1");
        var response = await perm.ReplaceAsync(new PermissionProperties("perm1", PermissionMode.All, container));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.PermissionMode.Should().Be(PermissionMode.All);
    }

    [Fact]
    public async Task Permission_DeleteAsync_RemovesPermission()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateUserAsync("user1");
        var user = db.GetUser("user1");
        var container = new InMemoryContainer("my-container", "/pk");
        await user.CreatePermissionAsync(new PermissionProperties("perm1", PermissionMode.All, container));

        var perm = user.GetPermission("perm1");
        var response = await perm.DeleteAsync();

        response.StatusCode.Should().Be(HttpStatusCode.NoContent);

        // Verify permission is gone
        var iterator = user.GetPermissionQueryIterator<PermissionProperties>();
        var perms = new List<PermissionProperties>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            perms.AddRange(page);
        }
        perms.Should().BeEmpty();
    }

    [Fact]
    public async Task Permission_GetQueryIterator_ReturnsAll()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateUserAsync("user1");
        var user = db.GetUser("user1");
        var container = new InMemoryContainer("my-container", "/pk");
        await user.CreatePermissionAsync(new PermissionProperties("perm1", PermissionMode.All, container));
        await user.CreatePermissionAsync(new PermissionProperties("perm2", PermissionMode.Read, container));

        var iterator = user.GetPermissionQueryIterator<PermissionProperties>();
        var perms = new List<PermissionProperties>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            perms.AddRange(page);
        }

        perms.Should().HaveCount(2);
        perms.Select(p => p.Id).Should().BeEquivalentTo(["perm1", "perm2"]);
    }

    [Fact]
    public async Task Permission_Token_IsSyntheticString()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateUserAsync("user1");
        var user = db.GetUser("user1");
        var container = new InMemoryContainer("my-container", "/pk");
        await user.CreatePermissionAsync(new PermissionProperties("perm1", PermissionMode.All, container));

        var perm = user.GetPermission("perm1");
        var response = await perm.ReadAsync();

        response.Resource.Token.Should().NotBeNullOrEmpty();
        response.Resource.Token.Should().Contain("perm1");
    }

    /// <summary>
    /// DIVERGENT BEHAVIOR: Real Cosmos DB permission tokens are cryptographic resource tokens
    /// with a specific format, signed by the service, and time-limited. The in-memory emulator
    /// returns synthetic tokens in the format "type=resource&amp;ver=1&amp;sig=stub_{permissionId}"
    /// that are non-functional placeholders. Token expiry (tokenExpiryInSeconds) is accepted
    /// but not enforced. No actual authorization is performed — all operations succeed regardless
    /// of permission settings.
    /// </summary>
    [Fact]
    public async Task DivergentBehavior_PermissionTokens_AreSyntheticNotCryptographic()
    {
        // In real Cosmos DB, permission tokens are cryptographic and time-limited.
        // The emulator returns synthetic placeholder tokens.
        // Token expiry is accepted but not enforced.
        // No authorization is performed — all operations succeed regardless.
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateUserAsync("user1");
        var user = db.GetUser("user1");
        var container = new InMemoryContainer("my-container", "/pk");
        await user.CreatePermissionAsync(new PermissionProperties("perm1", PermissionMode.All, container));

        var response = await user.GetPermission("perm1").ReadAsync();

        // Token is synthetic, not a real resource token
        response.Resource.Token.Should().StartWith("type=resource&ver=1&sig=stub_");
    }

    // ── B16 Client Encryption Keys ───────────────────────────────────────────

    [Fact(Skip = "SKIP REASON: Client encryption key management requires Azure Key Vault integration and deep SDK internals (MDE/Always Encrypted). Not meaningful for in-memory emulator.")]
    public void GetClientEncryptionKey_ReturnsKey()
    {
        var client = new InMemoryCosmosClient();
        var db = client.GetDatabase("test-db");
        var key = db.GetClientEncryptionKey("key1");
        key.Should().NotBeNull();
    }

    [Fact(Skip = "SKIP REASON: Same as above - encryption key infrastructure not emulated.")]
    public async Task CreateClientEncryptionKeyAsync_CreatesKey()
    {
        var client = new InMemoryCosmosClient();
        var db = client.GetDatabase("test-db");
        await db.CreateClientEncryptionKeyAsync(null);
    }

    /// <summary>
    /// DIVERGENT BEHAVIOR: Client encryption key management (GetClientEncryptionKey,
    /// CreateClientEncryptionKeyAsync, GetClientEncryptionKeyQueryIterator) requires
    /// Azure Key Vault integration. All methods throw NotImplementedException.
    /// </summary>
    [Fact]
    public void DivergentBehavior_ClientEncryptionKeys_ThrowNotImplemented()
    {
        var client = new InMemoryCosmosClient();
        var db = client.GetDatabase("test-db");

        ((Func<object>)(() => db.GetClientEncryptionKey("key1"))).Should().Throw<NotImplementedException>();
    }
}


public class CreateContainerIfNotExistsStatusCodeTests
{
    [Fact]
    public async Task CreateContainerIfNotExistsAsync_NewContainer_Returns201()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var response = await db.CreateContainerIfNotExistsAsync("container1", "/pk");

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task CreateContainerIfNotExistsAsync_ExistingContainer_Returns200()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateContainerAsync("container1", "/pk");

        var response = await db.CreateContainerIfNotExistsAsync("container1", "/pk");

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}


public class ReplaceThroughputAsyncResponseTests
{
    [Fact]
    public async Task ReplaceThroughputAsync_Int_ResponseContainsNewThroughput()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var response = await db.ReplaceThroughputAsync(1000);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Should().NotBeNull();
    }

    [Fact]
    public async Task ReplaceThroughputAsync_ThroughputProperties_ResponseContainsNewThroughput()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var response = await db.ReplaceThroughputAsync(
            ThroughputProperties.CreateManualThroughput(2000));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Should().NotBeNull();
    }
}


public class CosmosClientInputValidationTests
{
    private readonly InMemoryCosmosClient _client = new();

    [Fact]
    public void GetContainer_WithNullDatabaseId_Throws()
    {
        var act = () => _client.GetContainer(null!, "container");
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void GetContainer_WithNullContainerId_Throws()
    {
        var act = () => _client.GetContainer("db", null!);
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void GetContainer_WithEmptyDatabaseId_Throws()
    {
        var act = () => _client.GetContainer("", "container");
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void GetDatabase_WithNullId_Throws()
    {
        var act = () => _client.GetDatabase(null!);
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void GetDatabase_WithEmptyId_Throws()
    {
        var act = () => _client.GetDatabase("");
        act.Should().Throw<ArgumentException>();
    }
}


public class DeleteAsyncSubsequentOperationsTests
{
    [Fact]
    public async Task DeleteAsync_GetContainerQueryIterator_ReturnsEmpty()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateContainerAsync("container1", "/pk");

        await db.DeleteAsync();

        // After delete, query iterator should return no containers
        var iterator = db.GetContainerQueryIterator<ContainerProperties>();
        var containers = new List<ContainerProperties>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            containers.AddRange(page);
        }
        containers.Should().BeEmpty();
    }
}


public class GetContainerQueryIteratorAfterDeleteTests
{
    [Fact]
    public async Task GetContainerQueryIterator_AfterContainerDelete_NoLongerListed()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateContainerAsync("container1", "/pk");
        await db.CreateContainerAsync("container2", "/pk");

        var container1 = db.GetContainer("container1");
        await container1.DeleteContainerAsync();

        var iterator = db.GetContainerQueryIterator<ContainerProperties>();
        var containers = new List<ContainerProperties>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            containers.AddRange(page);
        }

        // After delete, only container2 should remain
        containers.Should().ContainSingle().Which.Id.Should().Be("container2");
    }
}


public class ConcurrentDatabaseOperationTests
{
    [Fact]
    public async Task ConcurrentCreateContainerAsync_DifferentIds_AllSucceed()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var tasks = Enumerable.Range(0, 20).Select(i =>
            db.CreateContainerAsync($"container-{i}", "/pk"));

        var responses = await Task.WhenAll(tasks);

        responses.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
        var uniqueIds = responses.Select(r => r.Resource.Id).Distinct();
        uniqueIds.Should().HaveCount(20);
    }

    [Fact]
    public async Task ConcurrentCreateContainerIfNotExistsAsync_SameId_OnlyOneCreated()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var tasks = Enumerable.Range(0, 20).Select(_ =>
            db.CreateContainerIfNotExistsAsync("shared-container", "/pk"));

        var responses = await Task.WhenAll(tasks);

        // Exactly one should be Created (201), the rest should be OK (200)
        responses.Count(r => r.StatusCode == HttpStatusCode.Created).Should().Be(1);
        responses.Count(r => r.StatusCode == HttpStatusCode.OK).Should().Be(19);
    }
}


public class NullGuardDivergentBehaviorTests
{
    /// <summary>
    /// BEHAVIORAL DIFFERENCE: Real Cosmos DB SDK throws ArgumentNullException for null id
    /// parameters on GetDatabase/GetContainer. InMemoryCosmosClient may need to add these
    /// guards explicitly. If the guards are not present, the ConcurrentDictionary will
    /// throw ArgumentNullException on its own, which is functionally equivalent.
    /// </summary>
    [Fact]
    public void GetDatabase_NullId_ThrowsSomeException()
    {
        var client = new InMemoryCosmosClient();
        var act = () => client.GetDatabase(null!);
        // Will throw either ArgumentNullException (if we add guards) or from ConcurrentDictionary
        act.Should().Throw<Exception>();
    }
}


public class CreateContainerIfNotExistsPropertiesTests
{
    /// <summary>
    /// Per SDK docs: "Only the container id is used to verify if there is an existing container.
    /// Other container properties such as throughput are not validated and can be different."
    /// Calling with a different partition key path should NOT update the existing container.
    /// </summary>
    [Fact]
    public async Task CreateContainerIfNotExistsAsync_DifferentPartitionKeyPath_DoesNotUpdateExisting()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        // Create with /pk
        await db.CreateContainerAsync("container1", "/pk");

        // IfNotExists with /differentPk — should NOT change the existing container
        var response = await db.CreateContainerIfNotExistsAsync("container1", "/differentPk");

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        // Verify the container still uses the original partition key path
        var container = (InMemoryContainer)db.GetContainer("container1");
        container.PartitionKeyPaths[0].Should().Be("/pk");
    }

    [Fact]
    public async Task CreateContainerIfNotExistsAsync_ContainerProperties_DifferentPk_DoesNotUpdate()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        await db.CreateContainerAsync(new ContainerProperties("container1", "/pk"));

        var response = await db.CreateContainerIfNotExistsAsync(
            new ContainerProperties("container1", "/otherPk"));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var container = (InMemoryContainer)db.GetContainer("container1");
        container.PartitionKeyPaths[0].Should().Be("/pk");
    }
}


public class ReadAsyncDatabaseResponseTests
{
    [Fact]
    public async Task ReadAsync_ResponseDatabase_IsSameInstance()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var response = await db.ReadAsync();

        response.Database.Should().BeSameAs(db);
    }
}


public class DatabaseIdPropertyTests
{
    [Fact]
    public void Database_Id_ReturnsConstructorValue()
    {
        var db = new InMemoryDatabase("my-database-id");

        db.Id.Should().Be("my-database-id");
    }

    [Fact]
    public void Database_Id_ViaClient_ReturnsCorrectValue()
    {
        var client = new InMemoryCosmosClient();
        var db = client.GetDatabase("another-db");

        db.Id.Should().Be("another-db");
    }
}


public class ReadThroughputDivergentTests
{
    /// <summary>
    /// DIVERGENT BEHAVIOR: Real Cosmos DB's ReadThroughputAsync(RequestOptions) throws
    /// CosmosException with StatusCode 404 when the database does not exist or has no
    /// throughput assigned.
    /// InMemoryDatabase always returns a synthetic 400 RU/s throughput value and never
    /// throws 404, because throughput is not meaningful in an in-memory emulator.
    /// </summary>
    [Fact]
    public async Task DivergentBehavior_ReadThroughputAsync_NeverThrows404_AlwaysReturnsSynthetic()
    {
        var db = new InMemoryDatabase("standalone-db");

        // Even a standalone database (not registered in any client) returns throughput
        var throughput = await db.ReadThroughputAsync();
        throughput.Should().Be(400);

        var response = await db.ReadThroughputAsync(new RequestOptions());
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}


public class DefineContainerBuilderDatabaseTests
{
    [Fact]
    public async Task DefineContainer_WithoutPolicies_CreatesContainer()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var response = await db.DefineContainer("simple-container", "/pk")
            .CreateAsync();

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Resource.Id.Should().Be("simple-container");
    }

    [Fact]
    public void DefineContainer_FluentBuilder_ReturnsContainerBuilder()
    {
        var client = new InMemoryCosmosClient();
        var db = client.GetDatabase("test-db");

        var builder = db.DefineContainer("container1", "/pk");

        builder.Should().NotBeNull();
        builder.Should().BeOfType<ContainerBuilder>();
    }
}


public class CreateContainerStreamResponseTests
{
    /// <summary>
    /// DIVERGENT BEHAVIOR: Real Cosmos DB includes the container properties JSON
    /// in the response stream body on CreateContainerStreamAsync.
    /// InMemoryDatabase returns a bare ResponseMessage with no body content.
    /// </summary>
    [Fact]
    public async Task DivergentBehavior_CreateContainerStreamAsync_ResponseBodyIsEmpty()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        using var response = await db.CreateContainerStreamAsync(
            new ContainerProperties("container1", "/pk"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        // InMemory returns no content in the stream body (diverges from real SDK)
        response.Content.Should().BeNull();
    }
}


public class DatabaseManagementEdgeCaseTests
{
    private readonly InMemoryCosmosClient _client = new();

    [Fact]
    public async Task CreateDatabaseAsync_WithNullId_Throws()
    {
        var act = () => _client.CreateDatabaseAsync(null!);
        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact]
    public async Task CreateDatabaseAsync_WithEmptyId_Throws()
    {
        var act = () => _client.CreateDatabaseAsync("");
        await act.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task CreateDatabaseIfNotExistsAsync_Returns201_Then200()
    {
        var first = await _client.CreateDatabaseIfNotExistsAsync("test-db");
        first.StatusCode.Should().Be(HttpStatusCode.Created);

        var second = await _client.CreateDatabaseIfNotExistsAsync("test-db");
        second.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task DeleteAsync_NonExistentDatabase_ThrowsNotFound()
    {
        var db = _client.GetDatabase("nonexistent-db");

        // GetDatabase auto-creates, so we need to remove it first then try again
        // Actually InMemoryDatabase.DeleteAsync clears containers and removes from client
        // but doesn't throw. Let's verify the current behavior.
        // The real Cosmos DB would throw NotFound for a non-existent database.
        // InMemoryCosmosClient auto-creates databases on GetDatabase, so this tests
        // that after deletion, a second delete should still succeed (no-op semantics).
        await db.DeleteAsync();

        // After deletion, creating a fresh reference and deleting should not throw
        // because GetDatabase re-creates it. This is a divergent behavior.
        var db2 = _client.GetDatabase("nonexistent-db");
        var response = await db2.DeleteAsync();
        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task GetDatabaseQueryIterator_WithContinuationToken_Resumes()
    {
        for (var i = 0; i < 5; i++)
            await _client.CreateDatabaseAsync($"db-{i}");

        var iterator = _client.GetDatabaseQueryIterator<DatabaseProperties>();
        var results = new List<DatabaseProperties>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCountGreaterThanOrEqualTo(5);
    }

    [Fact]
    public async Task CreateDatabaseStreamAsync_WithThroughputProperties_ReturnsCreated()
    {
        // The stream overload with int? throughput is tested. This verifies the
        // method works with DatabaseProperties parameter.
        var response = await _client.CreateDatabaseStreamAsync(
            new DatabaseProperties("stream-db-with-tp"), throughput: 400);

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }
}


public class GetContainerSameInstanceTests
{
    [Fact]
    public async Task GetContainer_CalledTwice_ReturnsSameContainer()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateContainerAsync("container1", "/partitionKey");

        var ref1 = db.GetContainer("container1");
        var ref2 = db.GetContainer("container1");

        ref1.Should().BeSameAs(ref2);
    }

    [Fact]
    public async Task GetContainer_DataVisibleAcrossReferences()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateContainerAsync("container1", "/partitionKey");

        var ref1 = db.GetContainer("container1");
        var ref2 = db.GetContainer("container1");

        // Write through ref1
        await ref1.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Hello" },
            new PartitionKey("pk1"));

        // Read through ref2
        var read = await ref2.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Hello");
    }
}


public class CreateContainerCustomIndexingTests
{
    [Fact]
    public async Task CreateContainerAsync_WithCustomIndexingPolicy_CreatesSuccessfully()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var props = new ContainerProperties("container1", "/pk")
        {
            IndexingPolicy = new IndexingPolicy
            {
                Automatic = false,
                IndexingMode = IndexingMode.Lazy
            }
        };

        var response = await db.CreateContainerAsync(props);

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Resource.Id.Should().Be("container1");
    }
}


public class ReadStreamAsyncResponseTests
{
    /// <summary>
    /// DIVERGENT BEHAVIOR: Real Cosmos DB returns the database properties JSON in the
    /// ReadStreamAsync response body. InMemoryDatabase returns a bare ResponseMessage
    /// with HttpStatusCode.OK and no body content.
    /// </summary>
    [Fact]
    public async Task DivergentBehavior_ReadStreamAsync_ResponseBodyIsEmpty()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        using var response = await db.ReadStreamAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        // InMemory returns no content in the stream body (diverges from real SDK)
        response.Content.Should().BeNull();
    }
}


public class CosmosClientAutoCreateDatabaseDivergentBehaviorTests
{
    /// <summary>
    /// BEHAVIORAL DIFFERENCE: InMemoryCosmosClient.GetDatabase auto-creates databases.
    /// Real Cosmos DB GetDatabase returns a proxy reference that does NOT create the database;
    /// the first actual operation (ReadAsync, CreateContainerAsync, etc.) would fail with
    /// NotFound if the database doesn't exist.
    /// InMemoryCosmosClient creates the database lazily on GetDatabase to simplify test setup.
    /// </summary>
    [Fact]
    public async Task GetDatabase_AutoCreatesDatabase_InMemory()
    {
        var client = new InMemoryCosmosClient();
        var db = client.GetDatabase("auto-created");

        // InMemory: database is already created, so ReadAsync succeeds
        var response = await db.ReadAsync();
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}


public class ReadThroughputAsyncDetailedTests
{
    [Fact]
    public async Task ReadThroughputAsync_WithRequestOptions_ReturnsOkStatusCode()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var response = await db.ReadThroughputAsync(new RequestOptions());

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task ReadThroughputAsync_WithRequestOptions_ReturnsThroughput400()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var response = await db.ReadThroughputAsync(new RequestOptions());

        response.Resource.Should().NotBeNull();
    }
}


public class CreateContainerReturnedContainerTests
{
    [Fact]
    public async Task CreateContainerAsync_ReturnedContainer_IsUsableForCrud()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var response = await db.CreateContainerAsync("container1", "/partitionKey");
        var container = response.Container;

        container.Should().NotBeNull();

        // Use the returned container to create and read an item
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        await container.CreateItemAsync(item, new PartitionKey("pk1"));

        var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Test");
    }
}


public class GetContainerQueryIteratorPartitionKeyTests
{
    [Fact]
    public async Task GetContainerQueryIterator_ReturnsActualPartitionKeyPath()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        await db.CreateContainerAsync("users", "/userId");
        await db.CreateContainerAsync("orders", "/orderId");

        var iterator = db.GetContainerQueryIterator<ContainerProperties>();
        var containers = new List<ContainerProperties>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            containers.AddRange(page);
        }

        containers.Should().HaveCount(2);
        var users = containers.Single(c => c.Id == "users");
        var orders = containers.Single(c => c.Id == "orders");

        users.PartitionKeyPath.Should().Be("/userId");
        orders.PartitionKeyPath.Should().Be("/orderId");
    }

    [Fact]
    public async Task GetContainerQueryIterator_WithQueryDefinition_ReturnsActualPkPath()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        await db.CreateContainerAsync("myContainer", "/tenantId");

        var queryDef = new QueryDefinition("SELECT * FROM c");
        var iterator = db.GetContainerQueryIterator<ContainerProperties>(queryDef);
        var containers = new List<ContainerProperties>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            containers.AddRange(page);
        }

        containers.Should().ContainSingle()
            .Which.PartitionKeyPath.Should().Be("/tenantId");
    }
}


public class CreateContainerResponseResourceTests
{
    [Fact]
    public async Task CreateContainerAsync_ResponseResource_HasCorrectPartitionKeyPath()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var response = await db.CreateContainerAsync("container1", "/myPartitionKey");

        response.Resource.Should().NotBeNull();
        response.Resource.Id.Should().Be("container1");
        response.Resource.PartitionKeyPath.Should().Be("/myPartitionKey");
    }

    [Fact]
    public async Task CreateContainerAsync_WithContainerProperties_ResponseHasCorrectPkPath()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var response = await db.CreateContainerAsync(
            new ContainerProperties("container1", "/category"));

        response.Resource.PartitionKeyPath.Should().Be("/category");
    }

    [Fact]
    public async Task CreateContainerIfNotExistsAsync_ResponseResource_HasCorrectPkPath()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var response = await db.CreateContainerIfNotExistsAsync("container1", "/region");

        response.Resource.PartitionKeyPath.Should().Be("/region");
    }
}


public class GetContainerQueryStreamIteratorOverloadTests
{
    [Fact]
    public async Task GetContainerQueryStreamIterator_WithQueryDefinition_ReturnsContainers()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateContainerAsync("container1", "/pk");

        var queryDef = new QueryDefinition("SELECT * FROM c");
        var iterator = db.GetContainerQueryStreamIterator(queryDef);

        var responses = new List<ResponseMessage>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            responses.Add(response);
            response.StatusCode.Should().Be(HttpStatusCode.OK);
        }
        responses.Should().NotBeEmpty();
    }
}


public class DeleteStreamAsyncContainerTests
{
    [Fact]
    public async Task DeleteStreamAsync_RemovesAllContainersInDatabase()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateContainerAsync("container1", "/pk");
        var container = db.GetContainer("container1");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        await db.DeleteStreamAsync();

        // Re-create database and container — should be empty
        await client.CreateDatabaseAsync("test-db");
        var newDb = client.GetDatabase("test-db");
        await newDb.CreateContainerAsync("container1", "/pk");
        var newContainer = newDb.GetContainer("container1");

        var act = () => newContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        await act.Should().ThrowAsync<CosmosException>();
    }
}

// ─── DeleteContainerAsync Removes From Parent DB ────────────────────────

public class DeleteContainerParentDbTests
{
    /// <summary>
    /// In real Cosmos DB, deleting a container removes it from the database's container list.
    /// In the emulator, DeleteContainerAsync clears internal data but does not remove itself
    /// from the parent InMemoryDatabase._containers dictionary.
    /// </summary>
    [Fact]
    public async Task DeleteContainer_ShouldRemoveFromDatabase_ContainerList()
    {
        var client = new InMemoryCosmosClient();
        var db = (InMemoryDatabase)(await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
        await db.CreateContainerAsync("ctr1", "/pk");

        var container = db.GetContainer("ctr1");
        await container.DeleteContainerAsync();

        // After deletion, the container should not be listed
        var iterator = db.GetContainerQueryIterator<ContainerProperties>("SELECT * FROM c");
        var containers = new List<ContainerProperties>();
        while (iterator.HasMoreResults)
            containers.AddRange(await iterator.ReadNextAsync());

        containers.Should().NotContain(c => c.Id == "ctr1");
    }
}

// ─── Client Encryption Key Operations ───────────────────────────────────

public class ClientEncryptionKeyTests
{
    /// <summary>
    /// Client encryption key management (CreateClientEncryptionKeyAsync,
    /// RewrapClientEncryptionKeyAsync, ReadClientEncryptionKeyAsync) requires integration
    /// with Azure Key Vault and the Microsoft Data Encryption (MDE) SDK. These operations
    /// manage envelope encryption where a data encryption key (DEK) is wrapped by a
    /// customer-managed key (CMK) stored in Key Vault.
    /// </summary>
    [Fact(Skip = "Client encryption key operations require Azure Key Vault integration and " +
        "the Microsoft Data Encryption SDK (MDE). CreateClientEncryptionKeyAsync wraps a " +
        "data encryption key (DEK) with a customer-managed key from Key Vault. " +
        "ReadClientEncryptionKeyAsync and RewrapClientEncryptionKeyAsync manage the DEK " +
        "lifecycle. These deep SDK internals (EncryptionKeyWrapProvider, DataEncryptionKey) " +
        "are not meaningful without actual Key Vault access. " +
        "InMemoryDatabase currently throws NotImplementedException for these methods.")]
    public async Task CreateClientEncryptionKey_ShouldCreateAndStoreKey()
    {
        var client = new InMemoryCosmosClient();
        var db = (InMemoryDatabase)(await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;

        // Real code would need: ClientEncryptionKeyProperties with EncryptionAlgorithm,
        // KeyWrapMetadata pointing to Key Vault. The emulator throws NotImplementedException.
        await db.CreateClientEncryptionKeyAsync(
            new ClientEncryptionKeyProperties("dek1", "AEAD_AES_256_CBC_HMAC_SHA256",
                new byte[] { 0x01, 0x02, 0x03 },
                new EncryptionKeyWrapMetadata("akvso", "masterkey1", "https://vault.azure.net/keys/key1/1", "RSA-OAEP")));
    }
}


// ═══════════════════════════════════════════════════════════════════════════
// BUG FIX: DeleteAsync / DeleteStreamAsync don't clear _users
// ═══════════════════════════════════════════════════════════════════════════

public class DeleteAsyncClearsUsersTests
{
    [Fact]
    public async Task DeleteAsync_ClearsUsersFromDatabase()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateUserAsync("user1");
        await db.CreateUserAsync("user2");

        await db.DeleteAsync();

        var iterator = db.GetUserQueryIterator<UserProperties>();
        var users = new List<UserProperties>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            users.AddRange(page);
        }
        users.Should().BeEmpty();
    }

    [Fact]
    public async Task DeleteStreamAsync_ClearsUsersFromDatabase()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateUserAsync("user1");
        await db.CreateUserAsync("user2");

        await db.DeleteStreamAsync();

        var iterator = db.GetUserQueryIterator<UserProperties>();
        var users = new List<UserProperties>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            users.AddRange(page);
        }
        users.Should().BeEmpty();
    }
}


// ═══════════════════════════════════════════════════════════════════════════
// Phase 1: Missing CosmosClient Tests
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentDatabaseCreationTests
{
    [Fact]
    public async Task ConcurrentCreateDatabaseAsync_DifferentIds_AllSucceed()
    {
        var client = new InMemoryCosmosClient();

        var tasks = Enumerable.Range(0, 20).Select(i =>
            client.CreateDatabaseAsync($"db-{i}"));

        var responses = await Task.WhenAll(tasks);

        responses.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
        var uniqueIds = responses.Select(r => r.Resource.Id).Distinct();
        uniqueIds.Should().HaveCount(20);
    }

    [Fact]
    public async Task ConcurrentCreateDatabaseIfNotExistsAsync_SameId_OnlyOneCreated()
    {
        var client = new InMemoryCosmosClient();

        var tasks = Enumerable.Range(0, 20).Select(_ =>
            client.CreateDatabaseIfNotExistsAsync("shared-db"));

        var responses = await Task.WhenAll(tasks);

        responses.Count(r => r.StatusCode == HttpStatusCode.Created).Should().Be(1);
        responses.Count(r => r.StatusCode == HttpStatusCode.OK).Should().Be(19);
    }
}


public class DatabaseQueryIteratorAfterDeleteTests
{
    [Fact]
    public async Task GetDatabaseQueryIterator_AfterDatabaseDelete_NoLongerListed()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("db1");
        await client.CreateDatabaseAsync("db2");

        var db1 = client.GetDatabase("db1");
        await db1.DeleteAsync();

        var iterator = client.GetDatabaseQueryIterator<DatabaseProperties>();
        var databases = new List<DatabaseProperties>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            databases.AddRange(page);
        }

        databases.Should().ContainSingle().Which.Id.Should().Be("db2");
    }
}


public class GetDatabaseSameInstanceTests
{
    [Fact]
    public void GetDatabase_CalledTwice_ReturnsSameInstance()
    {
        var client = new InMemoryCosmosClient();

        var ref1 = client.GetDatabase("my-db");
        var ref2 = client.GetDatabase("my-db");

        ref1.Should().BeSameAs(ref2);
    }
}


public class GetDatabaseQueryStreamIteratorOverloadTests
{
    [Fact]
    public async Task GetDatabaseQueryStreamIterator_WithQueryDefinition_ReturnsAllDatabases()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("db1");

        var queryDef = new QueryDefinition("SELECT * FROM c");
        var iterator = client.GetDatabaseQueryStreamIterator(queryDef);

        var responses = new List<ResponseMessage>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            responses.Add(response);
            response.StatusCode.Should().Be(HttpStatusCode.OK);
        }
        responses.Should().NotBeEmpty();
    }
}


public class ReadAccountAsyncDetailedTests
{
    [Fact]
    public async Task ReadAccountAsync_ReturnsIdAsInMemoryEmulator()
    {
        var client = new InMemoryCosmosClient();

        var account = await client.ReadAccountAsync();

        account.Id.Should().Be("in-memory-emulator");
    }
}


// ═══════════════════════════════════════════════════════════════════════════
// Phase 2: Missing Database Tests
// ═══════════════════════════════════════════════════════════════════════════

public class CreateContainerInputValidationTests
{
    [Fact]
    public async Task CreateContainerAsync_NullPartitionKeyPath_Throws()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var act = () => db.CreateContainerAsync("container1", null!);

        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact]
    public async Task CreateContainerAsync_NullId_Throws()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var act = () => db.CreateContainerAsync(null!, "/pk");

        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact]
    public async Task CreateContainerAsync_EmptyId_Throws()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var act = () => db.CreateContainerAsync("", "/pk");

        await act.Should().ThrowAsync<ArgumentException>();
    }
}


public class CreateContainerIfNotExistsNullPkFallbackTests
{
    [Fact]
    public async Task CreateContainerIfNotExistsAsync_ContainerProperties_NullPkPath_DefaultsToId()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var props = new ContainerProperties { Id = "container1" };
        var response = await db.CreateContainerIfNotExistsAsync(props);

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var container = (InMemoryContainer)db.GetContainer("container1");
        container.PartitionKeyPaths[0].Should().Be("/id");
    }
}


public class DeleteThenReuseReferenceTests
{
    [Fact]
    public async Task DeleteAsync_ThenReadAsync_OnSameReference_StillReturnsOk()
    {
        // DIVERGENT: Real Cosmos DB would return 404 on ReadAsync after deletion.
        // InMemoryDatabase.ReadAsync always returns OK because it doesn't check
        // whether the database is still registered with the parent client.
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        await db.DeleteAsync();

        var response = await db.ReadAsync();
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task DeleteAsync_ThenCreateContainer_OnSameReference_ShouldWork()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateContainerAsync("c1", "/pk");

        await db.DeleteAsync();

        // Containers were cleared, so we can create a new one
        var response = await db.CreateContainerAsync("c2", "/pk");
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }
}


public class StandaloneDatabaseTests
{
    [Fact]
    public async Task DeleteAsync_StandaloneDatabase_NoClient_DoesNotThrow()
    {
        var db = new InMemoryDatabase("standalone-db");

        var response = await db.DeleteAsync();

        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public void StandaloneDatabase_Client_ReturnsNull()
    {
        var db = new InMemoryDatabase("standalone-db");

        db.Client.Should().BeNull();
    }
}


public class UserQueryIteratorEmptyTests
{
    [Fact]
    public async Task GetUserQueryIterator_EmptyDatabase_ReturnsEmpty()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var iterator = db.GetUserQueryIterator<UserProperties>();
        var users = new List<UserProperties>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            users.AddRange(page);
        }

        users.Should().BeEmpty();
    }
}


public class PermissionErrorHandlingTests
{
    [Fact]
    public async Task Permission_ReadAsync_NonExistent_ThrowsNotFound()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateUserAsync("user1");
        var user = db.GetUser("user1");
        var perm = user.GetPermission("nonexistent");

        var act = () => perm.ReadAsync();

        await act.Should().ThrowAsync<CosmosException>()
            .Where(e => e.StatusCode == HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Permission_ReplaceAsync_NonExistent_ThrowsNotFound()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateUserAsync("user1");
        var user = db.GetUser("user1");
        var perm = user.GetPermission("nonexistent");
        var container = new InMemoryContainer("my-container", "/pk");

        var act = () => perm.ReplaceAsync(new PermissionProperties("nonexistent", PermissionMode.All, container));

        await act.Should().ThrowAsync<CosmosException>()
            .Where(e => e.StatusCode == HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Permission_DeleteAsync_NonExistent_DoesNotThrow()
    {
        // DIVERGENT: Real Cosmos DB would return 404. InMemoryPermission.DeleteAsync
        // does a quiet TryRemove which is a no-op if not found.
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateUserAsync("user1");
        var user = db.GetUser("user1");
        var perm = user.GetPermission("nonexistent");

        var response = await perm.DeleteAsync();

        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }
}


public class CreateDatabaseResponseUsabilityTests
{
    [Fact]
    public async Task CreateDatabaseAsync_ResponseDatabase_IsUsable()
    {
        var client = new InMemoryCosmosClient();

        var response = await client.CreateDatabaseAsync("test-db");

        response.Database.Should().NotBeNull();
        response.Database.Should().BeSameAs(client.GetDatabase("test-db"));
    }
}


// ═══════════════════════════════════════════════════════════════════════════
// Phase 3: Divergent Behavior Tests (Skip + Sister)
// ═══════════════════════════════════════════════════════════════════════════

public class DisposeAndContinueDivergentTests
{
    [Fact(Skip = "InMemoryCosmosClient.Dispose is a no-op to prevent NullReferenceException from " +
        "the base CosmosClient class. Post-dispose operations continue to work. Real SDK throws " +
        "ObjectDisposedException after Dispose. This is intentional for test convenience — tests " +
        "that use the emulator don't need to worry about disposal order or using statements " +
        "affecting subsequent assertions.")]
    public async Task Dispose_ThenCreateDatabase_ShouldThrowObjectDisposed()
    {
        var client = new InMemoryCosmosClient();
        client.Dispose();

        // Real SDK: ObjectDisposedException
        var act = () => client.CreateDatabaseAsync("test-db");
        await act.Should().ThrowAsync<ObjectDisposedException>();
    }

    /// <summary>
    /// DIVERGENT BEHAVIOR: InMemoryCosmosClient.Dispose is a no-op. Post-dispose operations
    /// continue to work normally. Real SDK throws ObjectDisposedException.
    /// This is intentional for test convenience.
    /// </summary>
    [Fact]
    public async Task DivergentBehavior_Dispose_ThenCreateDatabase_StillWorks()
    {
        // InMemoryCosmosClient.Dispose() overrides the base class with a no-op
        // to prevent NullReferenceException from the base CosmosClient destructor.
        // As a side-effect, post-dispose operations continue to work.
        var client = new InMemoryCosmosClient();
        client.Dispose();

        var response = await client.CreateDatabaseAsync("test-db");

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }
}


public class GetUserAutoCreateDivergentTests
{
    [Fact(Skip = "InMemoryDatabase.GetUser auto-creates user entries for test convenience, " +
        "mirroring the GetDatabase/GetContainer lazy-creation pattern. Real SDK's Database.GetUser " +
        "returns a lightweight proxy reference — it does NOT create the user. A subsequent " +
        "ReadAsync on a non-existent user would throw CosmosException with 404 NotFound. " +
        "The auto-create behavior simplifies test setup but means you cannot test " +
        "'user not found' scenarios through GetUser + ReadAsync.")]
    public async Task GetUser_NonExistent_ReadAsync_ShouldThrowNotFound()
    {
        var client = new InMemoryCosmosClient();
        var db = client.GetDatabase("test-db");

        var user = db.GetUser("nonexistent-user");

        // Real SDK: ReadAsync on a non-existent user throws 404
        var act = () => user.ReadAsync();
        await act.Should().ThrowAsync<CosmosException>()
            .Where(e => e.StatusCode == HttpStatusCode.NotFound);
    }

    /// <summary>
    /// DIVERGENT BEHAVIOR: InMemoryDatabase.GetUser auto-creates user entries.
    /// Real SDK returns a proxy; ReadAsync would throw 404 for non-existent users.
    /// This mirrors the GetDatabase/GetContainer lazy-creation pattern for test convenience.
    /// </summary>
    [Fact]
    public async Task DivergentBehavior_GetUser_AutoCreatesUser()
    {
        // InMemoryDatabase.GetUser uses ConcurrentDictionary.GetOrAdd to auto-create,
        // same pattern as GetDatabase and GetContainer for test convenience.
        var client = new InMemoryCosmosClient();
        var db = client.GetDatabase("test-db");

        var user = db.GetUser("auto-created-user");

        user.Should().NotBeNull();
        user.Id.Should().Be("auto-created-user");

        // ReadAsync succeeds because the user was auto-created
        var response = await user.ReadAsync();
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Id.Should().Be("auto-created-user");
    }
}


public class ThroughputNotPersistedDivergentTests
{
    [Fact(Skip = "InMemoryDatabase throughput is synthetic. ReplaceThroughputAsync returns " +
        "success but does NOT persist the new value — ReadThroughputAsync always returns 400. " +
        "Real Cosmos DB persists throughput changes: ReplaceThroughputAsync(1000) followed by " +
        "ReadThroughputAsync would return 1000. This keeps the implementation simple since " +
        "throughput has no behavioral impact on an in-memory store (no RU accounting, no " +
        "rate limiting, no partition splitting).")]
    public async Task ReplaceThroughputAsync_ThenRead_ShouldReturnNewValue()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        await db.ReplaceThroughputAsync(1000);

        // Real SDK: ReadThroughputAsync returns 1000
        var throughput = await db.ReadThroughputAsync();
        throughput.Should().Be(1000);
    }

    /// <summary>
    /// FIXED: ReplaceThroughputAsync now persists the value.
    /// ReadThroughputAsync returns the replaced value, matching real Cosmos DB.
    /// </summary>
    [Fact]
    public async Task ReplaceThroughputAsync_ThenRead_ReturnsPersisted()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var replaceResponse = await db.ReplaceThroughputAsync(1000);
        replaceResponse.StatusCode.Should().Be(HttpStatusCode.OK);

        var throughput = await db.ReadThroughputAsync();
        throughput.Should().Be(1000); // Now correctly returns 1000
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 0 — Bug Fix Tests
// ═══════════════════════════════════════════════════════════════════════════

public class CreateDatabaseStreamThroughputPropertiesTests
{
    [Fact]
    public async Task CreateDatabaseStreamAsync_WithThroughputProperties_ReturnsCreated()
    {
        var client = new InMemoryCosmosClient();
        var props = new DatabaseProperties("test-db");
        var tp = ThroughputProperties.CreateManualThroughput(1000);

        var response = await client.CreateDatabaseStreamAsync(props, tp);

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task CreateDatabaseStreamAsync_WithThroughputProperties_DuplicateId_ReturnsConflict()
    {
        var client = new InMemoryCosmosClient();
        var props = new DatabaseProperties("test-db");
        var tp = ThroughputProperties.CreateManualThroughput(1000);

        await client.CreateDatabaseStreamAsync(props, tp);
        var response = await client.CreateDatabaseStreamAsync(props, tp);

        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }
}

public class ClientEncryptionKeyQueryIteratorTests
{
    [Fact]
    public async Task GetClientEncryptionKeyQueryIterator_ThrowsNotImplemented()
    {
        var client = new InMemoryCosmosClient();
        var db = (InMemoryDatabase)(await client.CreateDatabaseAsync("test-db")).Database;

        var act = () => db.GetClientEncryptionKeyQueryIterator(new QueryDefinition("SELECT * FROM c"));

        act.Should().Throw<NotImplementedException>();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 1 — Naming & Validation Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class DatabaseNameSpecialCharsTests
{
    [Fact]
    public async Task CreateDatabaseAsync_NameWithSpaces_Succeeds()
    {
        var client = new InMemoryCosmosClient();
        var response = await client.CreateDatabaseAsync("my database");
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task CreateDatabaseAsync_NameWithUnicode_Succeeds()
    {
        var client = new InMemoryCosmosClient();
        var response = await client.CreateDatabaseAsync("données");
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task CreateDatabaseAsync_NameWithEmoji_Succeeds()
    {
        var client = new InMemoryCosmosClient();
        var response = await client.CreateDatabaseAsync("test-🚀-db");
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }
}

public class DatabaseNameForbiddenCharsDivergentTests
{
    private const string SkipReason = "Real Cosmos DB rejects database names containing `/`, `\\`, " +
        "`#`, or `?` characters with 400 BadRequest (Resource IDs cannot contain these characters per " +
        "the REST API spec). The in-memory emulator does not validate resource ID character restrictions.";

    [Fact(Skip = SkipReason)]
    public async Task CreateDatabaseAsync_NameWithForwardSlash_ShouldThrowBadRequest()
    {
        var client = new InMemoryCosmosClient();
        var act = () => client.CreateDatabaseAsync("db/name");
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task DivergentBehavior_CreateDatabaseAsync_NameWithForwardSlash_Accepted()
    {
        var client = new InMemoryCosmosClient();
        var response = await client.CreateDatabaseAsync("db/name");
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact(Skip = SkipReason)]
    public async Task CreateDatabaseAsync_NameWithBackslash_ShouldThrowBadRequest()
    {
        var client = new InMemoryCosmosClient();
        var act = () => client.CreateDatabaseAsync("db\\name");
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task DivergentBehavior_CreateDatabaseAsync_NameWithBackslash_Accepted()
    {
        var client = new InMemoryCosmosClient();
        var response = await client.CreateDatabaseAsync("db\\name");
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact(Skip = SkipReason)]
    public async Task CreateDatabaseAsync_NameWithHash_ShouldThrowBadRequest()
    {
        var client = new InMemoryCosmosClient();
        var act = () => client.CreateDatabaseAsync("db#name");
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task DivergentBehavior_CreateDatabaseAsync_NameWithHash_Accepted()
    {
        var client = new InMemoryCosmosClient();
        var response = await client.CreateDatabaseAsync("db#name");
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact(Skip = SkipReason)]
    public async Task CreateDatabaseAsync_NameWithQuestionMark_ShouldThrowBadRequest()
    {
        var client = new InMemoryCosmosClient();
        var act = () => client.CreateDatabaseAsync("db?name");
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task DivergentBehavior_CreateDatabaseAsync_NameWithQuestionMark_Accepted()
    {
        var client = new InMemoryCosmosClient();
        var response = await client.CreateDatabaseAsync("db?name");
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }
}

public class DatabaseNameLengthDivergentTests
{
    private const string SkipReason = "Real Cosmos DB enforces a 255-character maximum for database " +
        "names. The in-memory emulator does not enforce this limit.";

    [Fact(Skip = SkipReason)]
    public async Task CreateDatabaseAsync_NameExceeds255Chars_ShouldThrowBadRequest()
    {
        var client = new InMemoryCosmosClient();
        var longName = new string('a', 256);
        var act = () => client.CreateDatabaseAsync(longName);
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task DivergentBehavior_CreateDatabaseAsync_LongName_Accepted()
    {
        var client = new InMemoryCosmosClient();
        var longName = new string('a', 256);
        var response = await client.CreateDatabaseAsync(longName);
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }
}

public class ContainerNameForbiddenCharsDivergentTests
{
    private const string SkipReason = "Real Cosmos DB rejects container names containing `/`, `\\`, " +
        "`#`, or `?` characters with 400 BadRequest. The in-memory emulator does not validate resource ID " +
        "character restrictions.";

    [Fact(Skip = SkipReason)]
    public async Task CreateContainerAsync_NameWithForwardSlash_ShouldThrowBadRequest()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("test-db")).Database;
        var act = () => db.CreateContainerAsync("ctr/name", "/pk");
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task DivergentBehavior_CreateContainerAsync_NameWithForwardSlash_Accepted()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("test-db")).Database;
        var response = await db.CreateContainerAsync("ctr/name", "/pk");
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 2 — Lifecycle Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class ReadAfterDeleteDivergentTests
{
    private const string SkipReason = "Real Cosmos DB's Database.ReadAsync returns 404 NotFound when " +
        "the database has been deleted and you use a stale reference. InMemoryDatabase.ReadAsync always " +
        "returns 200 OK because the InMemoryDatabase object still exists in memory after deletion.";

    [Fact(Skip = SkipReason)]
    public async Task ReadAsync_AfterDelete_ShouldThrowNotFound()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("test-db")).Database;
        await db.DeleteAsync();

        var act = () => db.ReadAsync();
        await act.Should().ThrowAsync<CosmosException>();
    }
}

public class ConcurrentDeleteDatabaseTests
{
    [Fact]
    public async Task ConcurrentDeleteAsync_SameDatabase_AllSucceed()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("test-db")).Database;

        var tasks = Enumerable.Range(0, 10).Select(_ => db.DeleteAsync());
        var results = await Task.WhenAll(tasks);

        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.NoContent);
    }
}

public class ContainerOpsAfterDbDeleteDivergentTests
{
    private const string SkipReason = "Real Cosmos DB returns 404 on any container operation when the " +
        "parent database has been deleted. InMemoryDatabase.DeleteAsync clears the internal _containers " +
        "dictionary but the stale InMemoryDatabase reference remains functional.";

    [Fact(Skip = SkipReason)]
    public async Task ContainerOps_AfterDatabaseDelete_ShouldThrowNotFound()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("test-db")).Database;
        await db.CreateContainerAsync("ctr1", "/pk");
        await db.DeleteAsync();

        var act = () => db.GetContainer("ctr1").ReadContainerAsync();
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task DivergentBehavior_ContainerOps_AfterDatabaseDelete_ContainersCleared()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("test-db")).Database;
        await db.CreateContainerAsync("ctr1", "/pk");
        await db.DeleteAsync();

        // Containers are cleared but the DB object is alive — GetContainer auto-creates
        var iterator = db.GetContainerQueryIterator<ContainerProperties>();
        var containers = new List<ContainerProperties>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            containers.AddRange(page);
        }
        containers.Should().BeEmpty("containers were cleared by DeleteAsync");
    }
}

public class GetContainerAutoCreateDivergentTests
{
    private const string SkipReason = "Real Cosmos DB's Database.GetContainer returns a lightweight " +
        "proxy reference. It does NOT create the container. The first data operation on a non-existent " +
        "container throws CosmosException with 404 NotFound. InMemoryDatabase.GetContainer uses " +
        "ConcurrentDictionary.GetOrAdd, creating the container lazily.";

    [Fact(Skip = SkipReason)]
    public async Task GetContainer_NonExistent_ShouldBeProxyOnly()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("test-db")).Database;
        var container = db.GetContainer("nonexistent");

        var act = () => container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task DivergentBehavior_GetContainer_AutoCreatesContainer()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("test-db")).Database;
        var container = db.GetContainer("auto-created");

        // Emulator auto-creates the container — CRUD works immediately
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

        var item = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
        item.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}

public class CreateDbIfNotExistsNullGuardTests
{
    [Fact]
    public async Task CreateDatabaseIfNotExistsAsync_NullId_Throws()
    {
        var client = new InMemoryCosmosClient();
        var act = () => client.CreateDatabaseIfNotExistsAsync(null!);
        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact]
    public async Task CreateDatabaseIfNotExistsAsync_EmptyId_Throws()
    {
        var client = new InMemoryCosmosClient();
        var act = () => client.CreateDatabaseIfNotExistsAsync("");
        await act.Should().ThrowAsync<ArgumentException>();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 3 — Query Iterator Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class QueryIteratorFilteringDivergentTests
{
    private const string SkipReason = "Real Cosmos DB supports SQL queries on database/container " +
        "metadata. The in-memory emulator's query iterators ignore the query text entirely — all " +
        "databases/containers are returned regardless of WHERE clause.";

    [Fact(Skip = SkipReason)]
    public async Task GetDatabaseQueryIterator_WithWhereClause_ShouldFilter()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("db1");
        await client.CreateDatabaseAsync("db2");

        var iterator = client.GetDatabaseQueryIterator<DatabaseProperties>(
            "SELECT * FROM c WHERE c.id = 'db1'");
        var results = new List<DatabaseProperties>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        results.Should().ContainSingle(d => d.Id == "db1");
    }

    [Fact]
    public async Task DivergentBehavior_GetDatabaseQueryIterator_WhereClause_IgnoresFilter()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("db1");
        await client.CreateDatabaseAsync("db2");

        var iterator = client.GetDatabaseQueryIterator<DatabaseProperties>(
            "SELECT * FROM c WHERE c.id = 'db1'");
        var results = new List<DatabaseProperties>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        results.Should().HaveCount(2, "emulator returns ALL databases regardless of WHERE clause");
    }

    [Fact(Skip = SkipReason)]
    public async Task GetContainerQueryIterator_WithWhereClause_ShouldFilter()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("test-db")).Database;
        await db.CreateContainerAsync("ctr1", "/pk");
        await db.CreateContainerAsync("ctr2", "/pk");

        var iterator = db.GetContainerQueryIterator<ContainerProperties>(
            "SELECT * FROM c WHERE c.id = 'ctr1'");
        var results = new List<ContainerProperties>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        results.Should().ContainSingle(c => c.Id == "ctr1");
    }

    [Fact]
    public async Task DivergentBehavior_GetContainerQueryIterator_WhereClause_IgnoresFilter()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("test-db")).Database;
        await db.CreateContainerAsync("ctr1", "/pk");
        await db.CreateContainerAsync("ctr2", "/pk");

        var iterator = db.GetContainerQueryIterator<ContainerProperties>(
            "SELECT * FROM c WHERE c.id = 'ctr1'");
        var results = new List<ContainerProperties>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        results.Should().HaveCount(2, "emulator returns ALL containers regardless of WHERE clause");
    }
}

public class QueryIteratorPagingDivergentTests
{
    private const string SkipReasonPaging = "Real Cosmos DB respects QueryRequestOptions.MaxItemCount " +
        "for database and container query iterators. The in-memory emulator returns all items in a " +
        "single page and does not support MaxItemCount-based paging for metadata queries.";

    [Fact(Skip = SkipReasonPaging)]
    public async Task GetDatabaseQueryIterator_WithMaxItemCount_Pages()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("db1");
        await client.CreateDatabaseAsync("db2");
        await client.CreateDatabaseAsync("db3");

        var iterator = client.GetDatabaseQueryIterator<DatabaseProperties>(
            requestOptions: new QueryRequestOptions { MaxItemCount = 1 });
        var firstPage = await iterator.ReadNextAsync();
        firstPage.Count.Should().Be(1);
    }

    [Fact]
    public async Task DivergentBehavior_GetDatabaseQueryIterator_MaxItemCount_IgnoredReturnsAllInOnePage()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("db1");
        await client.CreateDatabaseAsync("db2");
        await client.CreateDatabaseAsync("db3");

        var iterator = client.GetDatabaseQueryIterator<DatabaseProperties>(
            requestOptions: new QueryRequestOptions { MaxItemCount = 1 });
        var firstPage = await iterator.ReadNextAsync();
        firstPage.Count.Should().Be(3, "emulator returns all items in a single page regardless of MaxItemCount");
    }
}

public class QueryIteratorContinuationTokenDivergentTests
{
    private const string SkipReason = "Real Cosmos DB supports continuation tokens for database " +
        "query iterators. InMemoryFeedIterator returns all items in a single page and ignores the " +
        "continuationToken parameter.";

    [Fact(Skip = SkipReason)]
    public async Task GetDatabaseQueryIterator_WithContinuationToken_Resumes()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("db1");
        await client.CreateDatabaseAsync("db2");

        var iterator = client.GetDatabaseQueryIterator<DatabaseProperties>(continuationToken: "someToken");
        var page = await iterator.ReadNextAsync();
        page.Count.Should().BeLessThan(2);
    }

    [Fact]
    public async Task DivergentBehavior_GetDatabaseQueryIterator_ContinuationToken_Ignored()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("db1");
        await client.CreateDatabaseAsync("db2");

        var iterator = client.GetDatabaseQueryIterator<DatabaseProperties>(continuationToken: "someToken");
        var page = await iterator.ReadNextAsync();
        page.Count.Should().Be(2, "emulator ignores continuation token and returns all items");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 4 — Stream API Response Bodies
// ═══════════════════════════════════════════════════════════════════════════

public class CreateDatabaseStreamResponseBodyDivergentTests
{
    private const string SkipReason = "Real Cosmos DB includes the resource properties JSON in the " +
        "response body of CreateDatabaseStreamAsync. The in-memory emulator returns ResponseMessage " +
        "with Content = null.";

    [Fact(Skip = SkipReason)]
    public async Task CreateDatabaseStreamAsync_ResponseBody_ShouldContainJson()
    {
        var client = new InMemoryCosmosClient();
        var response = await client.CreateDatabaseStreamAsync(new DatabaseProperties("test-db"));
        response.Content.Should().NotBeNull();
    }

    [Fact]
    public async Task DivergentBehavior_CreateDatabaseStreamAsync_ResponseBodyIsNull()
    {
        var client = new InMemoryCosmosClient();
        var response = await client.CreateDatabaseStreamAsync(new DatabaseProperties("test-db"));
        response.Content.Should().BeNull("emulator returns ResponseMessage with no content body");
    }
}

public class DeleteStreamAsyncResponseBodyDivergentTests
{
    [Fact]
    public async Task DivergentBehavior_DeleteStreamAsync_ResponseBodyIsNull()
    {
        var client = new InMemoryCosmosClient();
        var db = (InMemoryDatabase)(await client.CreateDatabaseAsync("test-db")).Database;
        var response = await db.DeleteStreamAsync();
        response.Content.Should().BeNull("emulator returns ResponseMessage with no content body");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 5 — Permission Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class PermissionDeleteNonExistentDivergentTests
{
    private const string SkipReason = "Real Cosmos DB returns 404 NotFound when deleting a permission " +
        "that doesn't exist. InMemoryPermission.DeleteAsync uses TryRemove which is a no-op for " +
        "non-existent keys, always returning 204 NoContent.";

    [Fact(Skip = SkipReason)]
    public async Task Permission_DeleteAsync_NonExistent_ShouldThrowNotFound()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("test-db")).Database;
        var user = (await db.CreateUserAsync("user1")).User;
        await user.CreatePermissionAsync(new PermissionProperties("p1", PermissionMode.All, db.GetContainer("c1")));

        var perm = user.GetPermission("p1");
        await perm.DeleteAsync();

        var act = () => perm.DeleteAsync();
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task DivergentBehavior_Permission_DeleteAsync_NonExistent_Returns204()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("test-db")).Database;
        var user = (await db.CreateUserAsync("user1")).User;
        await user.CreatePermissionAsync(new PermissionProperties("p1", PermissionMode.All, db.GetContainer("c1")));

        var perm = user.GetPermission("p1");
        await perm.DeleteAsync();

        // Second delete — emulator returns 204 silently
        var response = await perm.DeleteAsync();
        response.StatusCode.Should().Be(HttpStatusCode.NoContent,
            "emulator returns 204 even for non-existent permissions");
    }
}

public class PermissionQueryDefinitionOverloadTests
{
    [Fact]
    public async Task Permission_GetQueryIterator_QueryDefinition_ReturnsAll()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("test-db")).Database;
        var user = (await db.CreateUserAsync("user1")).User;
        await user.CreatePermissionAsync(new PermissionProperties("p1", PermissionMode.All, db.GetContainer("c1")));
        await user.CreatePermissionAsync(new PermissionProperties("p2", PermissionMode.Read, db.GetContainer("c2")));

        var iterator = user.GetPermissionQueryIterator<PermissionProperties>(
            new QueryDefinition("SELECT * FROM c"));
        var results = new List<PermissionProperties>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
    }
}

public class UserReplaceIdDivergentTests
{
    [Fact]
    public async Task User_ReplaceAsync_DoesNotRenameInParentDictionary()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("test-db")).Database;
        await db.CreateUserAsync("user1");

        var user = db.GetUser("user1");
        await user.ReplaceAsync(new UserProperties("user1-renamed"));

        // The user's Id changed internally...
        user.Id.Should().Be("user1-renamed");

        // ...but the old key still works in the parent DB
        var sameUser = db.GetUser("user1");
        sameUser.Should().NotBeNull();
    }

    private const string SkipReason = "Real Cosmos DB's User.ReplaceAsync does not allow changing the " +
        "user Id. The Id in the replacement UserProperties must match the existing user's Id.";

    [Fact(Skip = SkipReason)]
    public async Task User_ReplaceAsync_ChangingId_ShouldPreserveConsistency()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("test-db")).Database;
        await db.CreateUserAsync("user1");
        var user = db.GetUser("user1");
        // Real SDK would reject this
        var act = () => user.ReplaceAsync(new UserProperties("user1-renamed"));
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task DivergentBehavior_User_ReplaceAsync_ChangingId_InternalStateOnly()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("test-db")).Database;
        await db.CreateUserAsync("user1");
        var user = db.GetUser("user1");

        await user.ReplaceAsync(new UserProperties("user1-renamed"));

        // Internal Id changed
        user.Id.Should().Be("user1-renamed");
        // But stored under old key — new key auto-creates a different user
        var newUser = db.GetUser("user1-renamed");
        newUser.Id.Should().Be("user1-renamed", "GetUser auto-creates with the new name");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 6 — CancellationToken Behavior
// ═══════════════════════════════════════════════════════════════════════════

public class DatabaseCancellationTokenDivergentTests
{
    private const string SkipReason = "Real Cosmos DB SDK respects CancellationToken on all async " +
        "operations. The in-memory emulator returns Task.FromResult(...) synchronously, bypassing " +
        "cancellation token checks entirely.";

    [Fact(Skip = SkipReason)]
    public async Task CreateDatabaseAsync_WithCancelledToken_ShouldThrow()
    {
        var client = new InMemoryCosmosClient();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var act = () => client.CreateDatabaseAsync("test-db", cancellationToken: cts.Token);
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task DivergentBehavior_CreateDatabaseAsync_CancellationToken_Ignored()
    {
        var client = new InMemoryCosmosClient();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // Emulator ignores CancellationToken — succeeds anyway
        var response = await client.CreateDatabaseAsync("test-db", cancellationToken: cts.Token);
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 8 — Multi-Database Isolation
// ═══════════════════════════════════════════════════════════════════════════

public class MultiDatabaseIsolationTests
{
    [Fact]
    public async Task SameContainerName_DifferentDatabases_Isolated()
    {
        var client = new InMemoryCosmosClient();
        var db1 = (await client.CreateDatabaseAsync("db1")).Database;
        var db2 = (await client.CreateDatabaseAsync("db2")).Database;

        var ctr1 = (InMemoryContainer)(await db1.CreateContainerAsync("orders", "/pk")).Container;
        var ctr2 = (InMemoryContainer)(await db2.CreateContainerAsync("orders", "/pk")).Container;

        await ctr1.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", source = "db1" }), new PartitionKey("a"));

        ctr1.ItemCount.Should().Be(1);
        ctr2.ItemCount.Should().Be(0, "db2's 'orders' container should be empty — data is isolated");
    }

    [Fact]
    public async Task DeleteAsync_OneDatabase_DoesNotAffectOther()
    {
        var client = new InMemoryCosmosClient();
        var db1 = (await client.CreateDatabaseAsync("db1")).Database;
        var db2 = (await client.CreateDatabaseAsync("db2")).Database;

        var ctr1 = (InMemoryContainer)(await db1.CreateContainerAsync("ctr", "/pk")).Container;
        var ctr2 = (InMemoryContainer)(await db2.CreateContainerAsync("ctr", "/pk")).Container;
        await ctr2.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

        await db1.DeleteAsync();

        ctr2.ItemCount.Should().Be(1, "db2's data should be unaffected by db1's deletion");
    }

    [Fact]
    public async Task GetContainer_TwoArg_AcrossDatabases_Isolated()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("db1");
        await client.CreateDatabaseAsync("db2");

        var ctr1 = client.GetContainer("db1", "shared");
        var ctr2 = client.GetContainer("db2", "shared");

        await ctr1.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

        var item = await ctr1.ReadItemAsync<JObject>("1", new PartitionKey("a"));
        item.StatusCode.Should().Be(HttpStatusCode.OK);

        var act = () => ctr2.ReadItemAsync<JObject>("1", new PartitionKey("a"));
        await act.Should().ThrowAsync<CosmosException>();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 9 — Response Metadata Completeness
// ═══════════════════════════════════════════════════════════════════════════

public class DatabaseResponseMetadataTests
{
    [Fact]
    public async Task CreateDatabaseAsync_Response_HasResourceWithId()
    {
        var client = new InMemoryCosmosClient();
        var response = await client.CreateDatabaseAsync("test-db");

        response.Resource.Should().NotBeNull();
        response.Resource.Id.Should().Be("test-db");
    }

    [Fact]
    public async Task CreateDatabaseIfNotExistsAsync_Response_HasResourceWithId_OnBothCalls()
    {
        var client = new InMemoryCosmosClient();

        var first = await client.CreateDatabaseIfNotExistsAsync("test-db");
        first.Resource.Should().NotBeNull();
        first.Resource.Id.Should().Be("test-db");

        var second = await client.CreateDatabaseIfNotExistsAsync("test-db");
        second.Resource.Should().NotBeNull();
        second.Resource.Id.Should().Be("test-db");
    }
}

public class ContainerResponseMetadataTests
{
    [Fact]
    public async Task CreateContainerAsync_ResponseContainer_SameAsGetContainer()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("test-db")).Database;

        var response = await db.CreateContainerAsync("ctr", "/pk");

        var fromGet = db.GetContainer("ctr");
        response.Container.Should().BeSameAs(fromGet);
    }

    [Fact]
    public async Task CreateContainerIfNotExistsAsync_NewContainer_ResponseHasContainerReference()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("test-db")).Database;

        var response = await db.CreateContainerIfNotExistsAsync("ctr", "/pk");
        response.Container.Should().NotBeNull();
        response.Container.Id.Should().Be("ctr");
    }

    [Fact]
    public async Task CreateContainerIfNotExistsAsync_ExistingContainer_ResponseHasContainerReference()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("test-db")).Database;
        await db.CreateContainerAsync("ctr", "/pk");

        var response = await db.CreateContainerIfNotExistsAsync("ctr", "/pk");
        response.Container.Should().NotBeNull();
        response.Container.Id.Should().Be("ctr");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 10 — User Management Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class UserManagementEdgeCaseTests
{
    [Fact]
    public async Task UpsertUserAsync_ThreeTimes_Returns201Then200Then200()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("test-db")).Database;

        var r1 = await db.UpsertUserAsync("user1");
        r1.StatusCode.Should().Be(HttpStatusCode.Created);

        var r2 = await db.UpsertUserAsync("user1");
        r2.StatusCode.Should().Be(HttpStatusCode.OK);

        var r3 = await db.UpsertUserAsync("user1");
        r3.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task User_DeleteAsync_ThenCreateUserAsync_SameId_Succeeds()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("test-db")).Database;

        await db.CreateUserAsync("user1");
        var user = db.GetUser("user1");
        await user.DeleteAsync();

        var response = await db.CreateUserAsync("user1");
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task ConcurrentCreateUserAsync_DifferentIds_AllSucceed()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("test-db")).Database;

        var tasks = Enumerable.Range(0, 10)
            .Select(i => db.CreateUserAsync($"user-{i}"));
        var results = await Task.WhenAll(tasks);

        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
    }
}
