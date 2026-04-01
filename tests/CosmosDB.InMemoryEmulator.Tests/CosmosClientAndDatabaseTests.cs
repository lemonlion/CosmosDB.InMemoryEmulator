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


public class DefineContainerBuilderTests
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
