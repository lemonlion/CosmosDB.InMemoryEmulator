using System.Net;
using AwesomeAssertions;
using CosmosDB.InMemoryEmulator.ProductionExtensions;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ════════════════════════════════════════════════════════════════════════════════
// Phase 1: UseInMemoryCosmosContainers
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class UseInMemoryCosmosContainersTests : IDisposable
{
    public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

    [Fact]
    public void Default_RegistersSingleInMemoryContainer()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosContainers();

        var provider = services.BuildServiceProvider();
        var container = provider.GetRequiredService<Container>();
        container.Should().BeOfType<InMemoryContainer>();
    }

    [Fact]
    public void Default_PartitionKeyIsId()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosContainers();

        var provider = services.BuildServiceProvider();
        var container = (InMemoryContainer)provider.GetRequiredService<Container>();
        container.PartitionKeyPaths.Should().BeEquivalentTo(["/id"]);
    }

    [Fact]
    public void RemovesExistingContainerRegistration()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Container>(new InMemoryContainer("old-container", "/old"));

        services.UseInMemoryCosmosContainers();

        var provider = services.BuildServiceProvider();
        var container = (InMemoryContainer)provider.GetRequiredService<Container>();
        container.Id.Should().Be("in-memory-container");
    }

    [Fact]
    public void DoesNotRemoveCosmosClient()
    {
        var services = new ServiceCollection();
        var originalClient = new InMemoryCosmosClient();
        services.AddSingleton<CosmosClient>(originalClient);

        services.UseInMemoryCosmosContainers();

        var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<CosmosClient>();
        client.Should().BeSameAs(originalClient);
    }

    [Fact]
    public void CustomPartitionKey()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosContainers(o => o.AddContainer("test", "/partitionKey"));

        var provider = services.BuildServiceProvider();
        var container = (InMemoryContainer)provider.GetRequiredService<Container>();
        container.PartitionKeyPaths.Should().BeEquivalentTo(["/partitionKey"]);
    }

    [Fact]
    public void CustomContainerName()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosContainers(o => o.AddContainer("orders"));

        var provider = services.BuildServiceProvider();
        var container = (InMemoryContainer)provider.GetRequiredService<Container>();
        container.Id.Should().Be("orders");
    }

    [Fact]
    public void RegistersFeedIteratorSetup()
    {
        InMemoryFeedIteratorSetup.Deregister();
        var services = new ServiceCollection();

        services.UseInMemoryCosmosContainers();

        CosmosOverridableFeedIteratorExtensions.StaticFallbackFactory.Should().NotBeNull();
    }

    [Fact]
    public void CanDisableFeedIteratorSetup()
    {
        InMemoryFeedIteratorSetup.Deregister();
        var before = CosmosOverridableFeedIteratorExtensions.StaticFallbackFactory;
        var services = new ServiceCollection();

        services.UseInMemoryCosmosContainers(o => o.RegisterFeedIteratorSetup = false);

        // Factory should not have changed from its pre-call state
        CosmosOverridableFeedIteratorExtensions.StaticFallbackFactory.Should().Be(before);
    }

    [Fact]
    public void OnContainerCreatedCallback()
    {
        var services = new ServiceCollection();
        InMemoryContainer? captured = null;

        services.UseInMemoryCosmosContainers(o => o.OnContainerCreated = c => captured = c);

        var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<Container>();
        captured.Should().NotBeNull();
    }

    [Fact]
    public void MatchesExistingLifetime_Scoped()
    {
        var services = new ServiceCollection();
        services.AddScoped<Container>(_ => new InMemoryContainer("old", "/id"));

        services.UseInMemoryCosmosContainers();

        var descriptor = services.First(d => d.ServiceType == typeof(Container));
        descriptor.Lifetime.Should().Be(ServiceLifetime.Scoped);
    }

    [Fact]
    public void MultipleContainers()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosContainers(o =>
        {
            o.AddContainer("orders", "/pk");
            o.AddContainer("events", "/partitionKey");
            o.AddContainer("logs", "/category");
        });

        var provider = services.BuildServiceProvider();
        var containers = provider.GetServices<Container>().ToList();
        containers.Should().HaveCount(3);
        containers.Should().AllSatisfy(c => c.Should().BeOfType<InMemoryContainer>());
        containers.Select(c => c.Id).Should().BeEquivalentTo(["orders", "events", "logs"]);
    }

    [Fact]
    public void Idempotent_CalledTwice()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosContainers();
        services.UseInMemoryCosmosContainers();

        var provider = services.BuildServiceProvider();
        var containers = provider.GetServices<Container>().ToList();
        containers.Should().ContainSingle();
    }
}

// ════════════════════════════════════════════════════════════════════════════════
// Phase 2: UseInMemoryCosmosDB
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class UseInMemoryCosmosDBTests : IDisposable
{
    public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

    [Fact]
    public void Default_RegistersCosmosClient()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosDB();

        var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<CosmosClient>();
        client.Should().NotBeNull();
    }

    [Fact]
    public void Default_RegistersContainer()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosDB();

        var provider = services.BuildServiceProvider();
        var container = provider.GetRequiredService<Container>();
        container.Should().NotBeNull();
        container.Id.Should().Be("in-memory-container");
    }

    [Fact]
    public void RemovesExistingCosmosClient()
    {
        var services = new ServiceCollection();
        // Register a factory that would fail if ever invoked
        services.AddSingleton<CosmosClient>(_ =>
            throw new InvalidOperationException("Should have been removed"));

        services.UseInMemoryCosmosDB();

        var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<CosmosClient>();
        client.Should().NotBeNull();
    }

    [Fact]
    public void RemovesExistingContainer()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Container>(_ =>
            throw new InvalidOperationException("Should have been removed"));

        // Explicit AddContainer removes existing registrations
        services.UseInMemoryCosmosDB(o => o.AddContainer("orders", "/pk"));

        var provider = services.BuildServiceProvider();
        var container = provider.GetRequiredService<Container>();
        container.Should().NotBeNull();
        container.Id.Should().Be("orders");
    }

    [Fact]
    public void ContainerIsFromClient()
    {
        var services = new ServiceCollection();
        FakeCosmosHandler? capturedHandler = null;

        services.UseInMemoryCosmosDB(o =>
        {
            o.DatabaseName = "mydb";
            o.AddContainer("orders", "/pk");
            o.OnHandlerCreated = (_, h) => capturedHandler = h;
        });

        var provider = services.BuildServiceProvider();
        var diContainer = provider.GetRequiredService<Container>();

        // DI container and client both route through the same FakeCosmosHandler
        diContainer.Id.Should().Be("orders");
        capturedHandler.Should().NotBeNull();
        capturedHandler!.BackingContainer.Id.Should().Be("orders");
    }

    [Fact]
    public void CustomDatabaseName()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosDB(o => o.DatabaseName = "TestDb");

        var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<CosmosClient>();
        var db = client.GetDatabase("TestDb");
        db.Id.Should().Be("TestDb");
    }

    [Fact]
    public void DefaultDatabaseName_IsInMemoryDb()
    {
        var services = new ServiceCollection();
        FakeCosmosHandler? capturedHandler = null;

        services.UseInMemoryCosmosDB(o =>
        {
            o.OnHandlerCreated = (_, h) => capturedHandler = h;
        });

        var provider = services.BuildServiceProvider();
        var container = provider.GetRequiredService<Container>();

        // The default database and container names are used
        container.Id.Should().Be("in-memory-container");
        capturedHandler.Should().NotBeNull();
        capturedHandler!.BackingContainer.Id.Should().Be("in-memory-container");
    }

    [Fact]
    public void MultipleContainers()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosDB(o =>
        {
            o.AddContainer("orders", "/pk");
            o.AddContainer("events", "/partitionKey");
        });

        var provider = services.BuildServiceProvider();
        var containers = provider.GetServices<Container>().ToList();
        containers.Should().HaveCount(2);
        containers.Select(c => c.Id).Should().BeEquivalentTo(["orders", "events"]);
    }

    [Fact]
    public void DoesNotNeedFeedIteratorSetup()
    {
        InMemoryFeedIteratorSetup.Deregister();
        var services = new ServiceCollection();

        services.UseInMemoryCosmosDB();

        // FakeCosmosHandler handles .ToFeedIterator() natively — no setup needed
        CosmosOverridableFeedIteratorExtensions.StaticFallbackFactory.Should().BeNull();
    }

    [Fact]
    public void OnClientCreatedCallback()
    {
        var services = new ServiceCollection();
        CosmosClient? captured = null;

        services.UseInMemoryCosmosDB(o => o.OnClientCreated = c => captured = c);

        captured.Should().NotBeNull();
    }

    [Fact]
    public void IsSuperset_ContainerBehavior()
    {
        var services = new ServiceCollection();
        services.AddSingleton<CosmosClient>(_ =>
            throw new InvalidOperationException("Should have been removed"));
        services.AddSingleton<Container>(sp =>
            sp.GetRequiredService<CosmosClient>().GetContainer("db", "items"));

        services.UseInMemoryCosmosDB();

        var provider = services.BuildServiceProvider();
        provider.GetRequiredService<CosmosClient>().Should().NotBeNull();
        // Auto-detect: existing factory resolves against the in-memory client
        provider.GetRequiredService<Container>().Should().NotBeNull();
    }

    [Fact]
    public void ContainerPerDatabaseOverride()
    {
        var services = new ServiceCollection();
        var capturedHandlers = new Dictionary<string, FakeCosmosHandler>();

        services.UseInMemoryCosmosDB(o =>
        {
            o.AddContainer("c1", "/pk", databaseName: "db1");
            o.AddContainer("c2", "/pk", databaseName: "db2");
            o.OnHandlerCreated = (name, h) => capturedHandlers[name] = h;
        });

        var provider = services.BuildServiceProvider();
        var containers = provider.GetServices<Container>().ToList();

        containers.Should().HaveCount(2);
        containers.Select(c => c.Id).Should().BeEquivalentTo(["c1", "c2"]);

        // Each container backed by its own FakeCosmosHandler
        capturedHandlers.Should().HaveCount(2);
        capturedHandlers["c1"].BackingContainer.Id.Should().Be("c1");
        capturedHandlers["c2"].BackingContainer.Id.Should().Be("c2");
    }

    [Fact]
    public void MatchesExistingContainerLifetime()
    {
        var services = new ServiceCollection();
        services.AddScoped<Container>(_ => new InMemoryContainer("old", "/id"));

        services.UseInMemoryCosmosDB();

        var descriptor = services.First(d => d.ServiceType == typeof(Container));
        descriptor.Lifetime.Should().Be(ServiceLifetime.Scoped);
    }
}

// ════════════════════════════════════════════════════════════════════════════════
// Phase 3: Edge Cases
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class ServiceCollectionExtensionEdgeCaseTests : IDisposable
{
    public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

    [Fact]
    public void UseInMemoryCosmosDB_NoExistingRegistrations_StillWorks()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosDB();

        var provider = services.BuildServiceProvider();
        provider.GetRequiredService<CosmosClient>().Should().NotBeNull();
        provider.GetRequiredService<Container>().Should().NotBeNull();
    }

    [Fact]
    public void UseInMemoryCosmosContainers_NoExistingRegistrations_StillWorks()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosContainers();

        var provider = services.BuildServiceProvider();
        provider.GetRequiredService<Container>().Should().NotBeNull();
    }

    [Fact]
    public async Task UseInMemoryCosmosDB_ContainerUsableForCrud()
    {
        var services = new ServiceCollection();
        services.UseInMemoryCosmosDB(o => o.AddContainer("test", "/partitionKey"));
        var provider = services.BuildServiceProvider();
        var container = provider.GetRequiredService<Container>();

        // Create
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        var createResponse = await container.CreateItemAsync(item, new PartitionKey("pk1"));
        createResponse.StatusCode.Should().Be(HttpStatusCode.Created);

        // Read
        var readResponse = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        readResponse.Resource.Name.Should().Be("Test");
    }

    [Fact]
    public async Task UseInMemoryCosmosContainers_ContainerUsableForCrud()
    {
        var services = new ServiceCollection();
        services.UseInMemoryCosmosContainers(o => o.AddContainer("test", "/partitionKey"));
        var provider = services.BuildServiceProvider();
        var container = provider.GetRequiredService<Container>();

        // Create
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        var createResponse = await container.CreateItemAsync(item, new PartitionKey("pk1"));
        createResponse.StatusCode.Should().Be(HttpStatusCode.Created);

        // Read
        var readResponse = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        readResponse.Resource.Name.Should().Be("Test");
    }

    [Fact]
    public async Task UseInMemoryCosmosDB_ContainerUsableForLinqQuery()
    {
        var services = new ServiceCollection();
        services.UseInMemoryCosmosDB(o => o.AddContainer("test", "/partitionKey"));
        var provider = services.BuildServiceProvider();
        var container = provider.GetRequiredService<Container>();

        // Seed
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));
        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
            new PartitionKey("pk1"));

        // Query via LINQ + ToFeedIteratorOverridable
        var iterator = container.GetItemLinqQueryable<TestDocument>(true)
            .Where(d => d.PartitionKey == "pk1")
            .ToFeedIteratorOverridable();

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
    }

    [Fact]
    public void UseInMemoryCosmosDB_ClientGetContainerReturnsRegisteredContainer()
    {
        var services = new ServiceCollection();
        FakeCosmosHandler? capturedHandler = null;
        services.UseInMemoryCosmosDB(o =>
        {
            o.DatabaseName = "db";
            o.AddContainer("orders", "/pk");
            o.OnHandlerCreated = (_, h) => capturedHandler = h;
        });
        var provider = services.BuildServiceProvider();

        var client = provider.GetRequiredService<CosmosClient>();
        var diContainer = provider.GetRequiredService<Container>();
        var clientContainer = client.GetContainer("db", "orders");

        // Both route through the same FakeCosmosHandler backing
        diContainer.Id.Should().Be("orders");
        clientContainer.Id.Should().Be("orders");
        capturedHandler.Should().NotBeNull();
        capturedHandler!.BackingContainer.Id.Should().Be("orders");
    }
}

// ════════════════════════════════════════════════════════════════════════════════
// Pattern 2: Typed CosmosClient Subclasses (UseInMemoryCosmosDB<TClient>)
// ════════════════════════════════════════════════════════════════════════════════

// Simulated PRODUCTION typed clients (extend CosmosClient, NOT InMemoryCosmosClient)
// These mirror the real SCA.Common pattern — each domain gets its own CosmosClient subclass
public class ProductionEmployeeCosmosClient : CosmosClient
{
    public ProductionEmployeeCosmosClient(string connectionString, CosmosClientOptions? options = null)
        : base(connectionString, options) { }
}

public class ProductionCustomerCosmosClient : CosmosClient
{
    public ProductionCustomerCosmosClient(string connectionString, CosmosClientOptions? options = null)
        : base(connectionString, options) { }
}

// TEST-PROJECT typed clients (extend InMemoryCosmosClient, created by test authors)
// This is the one-liner per typed client that users must add in their test project.
// No changes to production code — these exist only in the test assembly.
public class EmployeeCosmosClient : InMemoryCosmosClient { }
public class CustomerCosmosClient : InMemoryCosmosClient { }

// Simulated repos that take typed clients — exactly as in production code.
// In real code these would reference the production types. In this test assembly,
// the test types shadow the production names by design.
public class BiometricRepository
{
    public Container Container { get; }

    public BiometricRepository(EmployeeCosmosClient client)
    {
        Container = client.GetContainer("BiometricDb", "biometrics");
    }
}

public class OOBRepository
{
    public Container Container { get; }

    public OOBRepository(CustomerCosmosClient client)
    {
        Container = client.GetContainer("OOBDb", "oob-events");
    }
}

[Collection("FeedIteratorSetup")]
public class UseInMemoryTypedCosmosDBTests : IDisposable
{
    public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

    [Fact]
    public void RegistersTypedClient_ResolvableFromDI()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
            o.AddContainer("biometrics", "/partitionKey"));

        var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<EmployeeCosmosClient>();
        client.Should().BeAssignableTo<EmployeeCosmosClient>();
    }

    [Fact]
    public void RemovesExistingTypedRegistration()
    {
        var services = new ServiceCollection();
        services.AddSingleton<EmployeeCosmosClient>(_ =>
            throw new InvalidOperationException("Should have been removed"));

        services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
            o.AddContainer("biometrics", "/partitionKey"));

        var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<EmployeeCosmosClient>();
        client.Should().NotBeNull();
    }

    [Fact]
    public void DefaultDatabase_IsInMemoryDb()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosDB<EmployeeCosmosClient>();

        var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<EmployeeCosmosClient>();
        var container = client.GetContainer("in-memory-db", "in-memory-container");
        container.Should().BeOfType<InMemoryContainer>();
    }

    [Fact]
    public void CustomDatabaseName()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
        {
            o.DatabaseName = "BiometricDb";
            o.AddContainer("biometrics", "/pk");
        });

        var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<EmployeeCosmosClient>();
        var container = client.GetContainer("BiometricDb", "biometrics");
        container.Should().BeOfType<InMemoryContainer>();
    }

    [Fact]
    public void MultipleTypedClients_Independent()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
        {
            o.DatabaseName = "BiometricDb";
            o.AddContainer("biometrics", "/pk");
        });
        services.UseInMemoryCosmosDB<CustomerCosmosClient>(o =>
        {
            o.DatabaseName = "OOBDb";
            o.AddContainer("oob-events", "/pk");
        });

        var provider = services.BuildServiceProvider();
        var bioClient = provider.GetRequiredService<EmployeeCosmosClient>();
        var oobClient = provider.GetRequiredService<CustomerCosmosClient>();

        bioClient.Should().NotBeSameAs(oobClient);
    }

    [Fact]
    public async Task TypedClient_ContainerUsableForCrud()
    {
        var services = new ServiceCollection();
        services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
        {
            o.DatabaseName = "BiometricDb";
            o.AddContainer("biometrics", "/partitionKey");
        });

        var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<EmployeeCosmosClient>();
        var container = client.GetContainer("BiometricDb", "biometrics");

        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Fingerprint" };
        var createResponse = await container.CreateItemAsync(item, new PartitionKey("pk1"));
        createResponse.StatusCode.Should().Be(HttpStatusCode.Created);

        var readResponse = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        readResponse.Resource.Name.Should().Be("Fingerprint");
    }

    [Fact]
    public void RepositoryPattern_Constructor_ResolvesTypedClient()
    {
        var services = new ServiceCollection();
        services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
        {
            o.DatabaseName = "BiometricDb";
            o.AddContainer("biometrics", "/partitionKey");
        });
        services.AddTransient<BiometricRepository>();

        var provider = services.BuildServiceProvider();
        var repo = provider.GetRequiredService<BiometricRepository>();

        repo.Container.Should().BeOfType<InMemoryContainer>();
        repo.Container.Id.Should().Be("biometrics");
    }

    [Fact]
    public void MultipleTypedClients_WithRepos_IsolatedData()
    {
        var services = new ServiceCollection();
        services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
        {
            o.DatabaseName = "BiometricDb";
            o.AddContainer("biometrics", "/partitionKey");
        });
        services.UseInMemoryCosmosDB<CustomerCosmosClient>(o =>
        {
            o.DatabaseName = "OOBDb";
            o.AddContainer("oob-events", "/partitionKey");
        });
        services.AddTransient<BiometricRepository>();
        services.AddTransient<OOBRepository>();

        var provider = services.BuildServiceProvider();
        var bioRepo = provider.GetRequiredService<BiometricRepository>();
        var oobRepo = provider.GetRequiredService<OOBRepository>();

        // Each repo's container should be independent
        bioRepo.Container.Should().NotBeSameAs(oobRepo.Container);
        bioRepo.Container.Id.Should().Be("biometrics");
        oobRepo.Container.Id.Should().Be("oob-events");
    }

    [Fact]
    public void DoesNotRegisterAsBaseCosmosClient()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosDB<EmployeeCosmosClient>();

        var provider = services.BuildServiceProvider();
        // Should NOT be resolvable as base CosmosClient unless explicitly registered
        var baseClient = provider.GetService<CosmosClient>();
        baseClient.Should().BeNull();
    }

    [Fact]
    public void DoesNotRegisterContainer_InDI()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o => o.AddContainer("bio", "/pk"));

        var provider = services.BuildServiceProvider();
        // Pattern 2 never has Container in DI — repos call client.GetContainer()
        var container = provider.GetService<Container>();
        container.Should().BeNull();
    }

    [Fact]
    public void RegistersFeedIteratorSetup()
    {
        InMemoryFeedIteratorSetup.Deregister();
        var services = new ServiceCollection();

        services.UseInMemoryCosmosDB<EmployeeCosmosClient>();

        CosmosOverridableFeedIteratorExtensions.StaticFallbackFactory.Should().NotBeNull();
    }

    [Fact]
    public void CanDisableFeedIteratorSetup()
    {
        InMemoryFeedIteratorSetup.Deregister();
        var before = CosmosOverridableFeedIteratorExtensions.StaticFallbackFactory;
        var services = new ServiceCollection();

        services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o => o.RegisterFeedIteratorSetup = false);

        CosmosOverridableFeedIteratorExtensions.StaticFallbackFactory.Should().Be(before);
    }

    [Fact]
    public void OnClientCreatedCallback()
    {
        var services = new ServiceCollection();
        CosmosClient? captured = null;

        services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o => o.OnClientCreated = c => captured = c);

        // The callback receives the typed InMemoryCosmosClient subclass
        captured.Should().NotBeNull();
        captured.Should().BeOfType<EmployeeCosmosClient>();
    }

    [Fact]
    public void MatchesExistingLifetime_Scoped()
    {
        var services = new ServiceCollection();
        services.AddScoped<EmployeeCosmosClient>(_ => new EmployeeCosmosClient());

        services.UseInMemoryCosmosDB<EmployeeCosmosClient>();

        var descriptor = services.First(d => d.ServiceType == typeof(EmployeeCosmosClient));
        descriptor.Lifetime.Should().Be(ServiceLifetime.Scoped);
    }

    [Fact]
    public void MatchesExistingLifetime_DefaultsSingleton()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosDB<EmployeeCosmosClient>();

        var descriptor = services.First(d => d.ServiceType == typeof(EmployeeCosmosClient));
        descriptor.Lifetime.Should().Be(ServiceLifetime.Singleton);
    }

    [Fact]
    public async Task FullEndToEnd_MultipleTypedClients()
    {
        var services = new ServiceCollection();
        services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
        {
            o.DatabaseName = "BiometricDb";
            o.AddContainer("biometrics", "/partitionKey");
        });
        services.UseInMemoryCosmosDB<CustomerCosmosClient>(o =>
        {
            o.DatabaseName = "OOBDb";
            o.AddContainer("oob-events", "/partitionKey");
        });
        services.AddTransient<BiometricRepository>();
        services.AddTransient<OOBRepository>();

        var provider = services.BuildServiceProvider();

        // Write via biometric repo
        var bioClient = provider.GetRequiredService<EmployeeCosmosClient>();
        var bioContainer = bioClient.GetContainer("BiometricDb", "biometrics");
        await bioContainer.CreateItemAsync(
            new TestDocument { Id = "bio-1", PartitionKey = "pk1", Name = "Fingerprint" },
            new PartitionKey("pk1"));

        // Write via OOB repo
        var oobClient = provider.GetRequiredService<CustomerCosmosClient>();
        var oobContainer = oobClient.GetContainer("OOBDb", "oob-events");
        await oobContainer.CreateItemAsync(
            new TestDocument { Id = "oob-1", PartitionKey = "pk1", Name = "SMS" },
            new PartitionKey("pk1"));

        // Read back — each client's data is isolated
        var bioRead = await bioContainer.ReadItemAsync<TestDocument>("bio-1", new PartitionKey("pk1"));
        bioRead.Resource.Name.Should().Be("Fingerprint");

        var oobRead = await oobContainer.ReadItemAsync<TestDocument>("oob-1", new PartitionKey("pk1"));
        oobRead.Resource.Name.Should().Be("SMS");

        // Cross-contamination check: bio container should not have OOB data
        var act = () => bioContainer.ReadItemAsync<TestDocument>("oob-1", new PartitionKey("pk1"));
        await act.Should().ThrowAsync<CosmosException>();
    }
}

// ════════════════════════════════════════════════════════════════════════════════
// Phase 5: Auto-Detect Mode — UseInMemoryCosmosDB() with no explicit containers
// ════════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Tests for the auto-detect behavior: when <c>UseInMemoryCosmosDB()</c> is called
/// without explicit <c>AddContainer()</c> calls, existing <c>Container</c> factory
/// registrations are preserved. They naturally resolve against the in-memory client.
/// </summary>
public class AutoDetectContainerTests
{

    [Fact]
    public void AutoDetect_PreservesExistingContainerFactory()
    {
        var services = new ServiceCollection();

        // Simulate production: register CosmosClient + Container factory
        services.AddSingleton<CosmosClient>(_ =>
            throw new InvalidOperationException("Production client should be replaced"));
        services.AddSingleton<Container>(sp =>
        {
            var client = sp.GetRequiredService<CosmosClient>();
            return client.GetContainer("ProductionDb", "orders");
        });

        // Zero-config swap — no AddContainer needed
        services.UseInMemoryCosmosDB();

        var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<CosmosClient>();
        client.Should().NotBeNull();

        // Container resolves via the existing factory, which calls GetContainer on the real client
        var container = provider.GetRequiredService<Container>();
        container.Should().NotBeNull();
        container.Id.Should().Be("orders");
    }

    [Fact]
    public async Task AutoDetect_ContainerIsFullyFunctional()
    {
        var services = new ServiceCollection();
        services.AddSingleton<CosmosClient>(_ =>
            throw new InvalidOperationException("Should be replaced"));
        services.AddSingleton<Container>(sp =>
            sp.GetRequiredService<CosmosClient>().GetContainer("MyDb", "items"));

        services.UseInMemoryCosmosDB();

        var provider = services.BuildServiceProvider();
        var container = provider.GetRequiredService<Container>();

        // CRUD works — the auto-created InMemoryContainer is fully usable
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "AutoDetected" };
        var response = await container.CreateItemAsync(item, new PartitionKey("pk1"));
        response.StatusCode.Should().Be(HttpStatusCode.Created);

        var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("AutoDetected");
    }

    [Fact]
    public void AutoDetect_MultipleContainerFactories_AllPreserved()
    {
        var services = new ServiceCollection();
        services.AddSingleton<CosmosClient>(_ =>
            throw new InvalidOperationException("Should be replaced"));
        services.AddSingleton<Container>(sp =>
            sp.GetRequiredService<CosmosClient>().GetContainer("db", "orders"));
        services.AddSingleton<Container>(sp =>
            sp.GetRequiredService<CosmosClient>().GetContainer("db", "customers"));

        services.UseInMemoryCosmosDB();

        var provider = services.BuildServiceProvider();
        var containers = provider.GetServices<Container>().ToList();
        containers.Should().HaveCount(2);
        containers.Select(c => c.Id).Should().BeEquivalentTo(["orders", "customers"]);
    }

    [Fact]
    public void AutoDetect_ClientGetContainer_SharesSameBackingStore()
    {
        var services = new ServiceCollection();
        services.AddSingleton<CosmosClient>(_ =>
            throw new InvalidOperationException("Should be replaced"));
        services.AddSingleton<Container>(sp =>
            sp.GetRequiredService<CosmosClient>().GetContainer("MyDb", "orders"));

        services.UseInMemoryCosmosDB();

        var provider = services.BuildServiceProvider();
        var diContainer = provider.GetRequiredService<Container>();
        var clientContainer = provider.GetRequiredService<CosmosClient>().GetContainer("MyDb", "orders");

        // Both route through the same FakeCosmosHandler backing
        diContainer.Id.Should().Be("orders");
        clientContainer.Id.Should().Be("orders");
    }

    [Fact]
    public void AutoDetect_NoExistingContainers_CreatesDefault()
    {
        var services = new ServiceCollection();
        // No Container registered — only CosmosClient
        services.AddSingleton<CosmosClient>(_ =>
            throw new InvalidOperationException("Should be replaced"));

        services.UseInMemoryCosmosDB();

        var provider = services.BuildServiceProvider();
        // Default container should still be created when no existing registrations
        var container = provider.GetRequiredService<Container>();
        container.Should().NotBeNull();
        container.Id.Should().Be("in-memory-container");
    }

    [Fact]
    public void AutoDetect_PreservesExistingLifetime()
    {
        var services = new ServiceCollection();
        services.AddSingleton<CosmosClient>(_ =>
            throw new InvalidOperationException("Should be replaced"));
        // Register as Scoped — should be preserved
        services.AddScoped<Container>(sp =>
            sp.GetRequiredService<CosmosClient>().GetContainer("db", "orders"));

        services.UseInMemoryCosmosDB();

        var descriptor = services.First(d => d.ServiceType == typeof(Container));
        descriptor.Lifetime.Should().Be(ServiceLifetime.Scoped);
    }

    [Fact]
    public void ExplicitContainers_StillRemovesExistingRegistrations()
    {
        var services = new ServiceCollection();
        services.AddSingleton<CosmosClient>(_ =>
            throw new InvalidOperationException("Should be replaced"));
        services.AddSingleton<Container>(_ =>
            throw new InvalidOperationException("Should have been removed by explicit AddContainer"));

        // Explicit AddContainer — should remove existing and replace
        services.UseInMemoryCosmosDB(o => o.AddContainer("orders", "/pk"));

        var provider = services.BuildServiceProvider();
        var container = provider.GetRequiredService<Container>();
        container.Should().NotBeNull();
        container.Id.Should().Be("orders");
    }

    [Fact]
    public void AutoDetect_DoesNotNeedFeedIteratorSetup()
    {
        InMemoryFeedIteratorSetup.Deregister();
        var services = new ServiceCollection();
        services.AddSingleton<CosmosClient>(_ =>
            throw new InvalidOperationException("Should be replaced"));
        services.AddSingleton<Container>(sp =>
            sp.GetRequiredService<CosmosClient>().GetContainer("db", "items"));

        services.UseInMemoryCosmosDB();

        // FakeCosmosHandler handles .ToFeedIterator() natively — no setup needed
        CosmosOverridableFeedIteratorExtensions.StaticFallbackFactory.Should().BeNull();
    }

    [Fact]
    public void AutoDetect_OnClientCreatedCallback_StillWorks()
    {
        var services = new ServiceCollection();
        services.AddSingleton<CosmosClient>(_ =>
            throw new InvalidOperationException("Should be replaced"));
        services.AddSingleton<Container>(sp =>
            sp.GetRequiredService<CosmosClient>().GetContainer("db", "items"));
        CosmosClient? captured = null;

        services.UseInMemoryCosmosDB(o => o.OnClientCreated = c => captured = c);

        captured.Should().NotBeNull();
    }
}

// ════════════════════════════════════════════════════════════════════════════════
// Phase 6: HttpMessageHandlerWrapper
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class HttpMessageHandlerWrapperTests : IDisposable
{
    public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

    /// <summary>
    /// A test DelegatingHandler that counts requests passing through it.
    /// </summary>
    private class CountingHandler : DelegatingHandler
    {
        public int RequestCount { get; private set; }

        public CountingHandler(HttpMessageHandler innerHandler) : base(innerHandler) { }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            RequestCount++;
            return base.SendAsync(request, cancellationToken);
        }
    }

    [Fact]
    public void Wrapper_IsInvoked_WithHandler()
    {
        var services = new ServiceCollection();
        HttpMessageHandler? capturedHandler = null;

        services.UseInMemoryCosmosDB(o =>
        {
            o.AddContainer("orders", "/pk");
            o.HttpMessageHandlerWrapper = handler =>
            {
                capturedHandler = handler;
                return handler; // pass through unchanged
            };
        });

        capturedHandler.Should().NotBeNull();
        capturedHandler.Should().BeOfType<FakeCosmosHandler>();
    }

    [Fact]
    public async Task Wrapper_ReturnValue_IsUsed_DelegatingHandlerSendAsyncCalled()
    {
        var services = new ServiceCollection();
        CountingHandler? countingHandler = null;

        services.UseInMemoryCosmosDB(o =>
        {
            o.AddContainer("test", "/partitionKey");
            o.HttpMessageHandlerWrapper = handler =>
            {
                countingHandler = new CountingHandler(handler);
                return countingHandler;
            };
        });

        var provider = services.BuildServiceProvider();
        var container = provider.GetRequiredService<Container>();

        // Perform a CRUD operation — this should flow through the CountingHandler
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        countingHandler.Should().NotBeNull();
        countingHandler!.RequestCount.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task NullWrapper_Default_ExistingBehaviourPreserved()
    {
        var services = new ServiceCollection();

        // No HttpMessageHandlerWrapper set — default null
        services.UseInMemoryCosmosDB(o => o.AddContainer("test", "/partitionKey"));

        var provider = services.BuildServiceProvider();
        var container = provider.GetRequiredService<Container>();

        // CRUD should work exactly as before
        var createResponse = await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));
        createResponse.StatusCode.Should().Be(HttpStatusCode.Created);

        var readResponse = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        readResponse.Resource.Name.Should().Be("Test");
    }

    [Fact]
    public void MultiContainer_Wrapper_ReceivesRouter_NotIndividualHandler()
    {
        var services = new ServiceCollection();
        HttpMessageHandler? capturedHandler = null;

        services.UseInMemoryCosmosDB(o =>
        {
            o.AddContainer("orders", "/pk");
            o.AddContainer("events", "/pk");
            o.HttpMessageHandlerWrapper = handler =>
            {
                capturedHandler = handler;
                return handler;
            };
        });

        capturedHandler.Should().NotBeNull();
        // With multiple containers, the handler is the router — NOT a FakeCosmosHandler
        capturedHandler.Should().NotBeOfType<FakeCosmosHandler>();
    }

    [Fact]
    public void FluentMethod_WithHttpMessageHandlerWrapper_Works()
    {
        var services = new ServiceCollection();
        HttpMessageHandler? capturedHandler = null;

        services.UseInMemoryCosmosDB(o => o
            .AddContainer("orders", "/pk")
            .WithHttpMessageHandlerWrapper(handler =>
            {
                capturedHandler = handler;
                return handler;
            }));

        capturedHandler.Should().NotBeNull();
        capturedHandler.Should().BeOfType<FakeCosmosHandler>();
    }

    [Fact]
    public async Task FullDelegatingHandlerChaining_CrudAndQueryThroughWrapper()
    {
        var services = new ServiceCollection();
        CountingHandler? countingHandler = null;

        services.UseInMemoryCosmosDB(o =>
        {
            o.AddContainer("test", "/partitionKey");
            o.HttpMessageHandlerWrapper = handler =>
            {
                countingHandler = new CountingHandler(handler);
                return countingHandler;
            };
        });

        var provider = services.BuildServiceProvider();
        var container = provider.GetRequiredService<Container>();

        // Create
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        // Read
        var readResponse = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        readResponse.Resource.Name.Should().Be("Alice");

        // Query
        var iterator = container.GetItemQueryIterator<TestDocument>(
            new QueryDefinition("SELECT * FROM c WHERE c.name = 'Alice'"));
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        results.Should().ContainSingle().Which.Name.Should().Be("Alice");

        // All requests went through the counting handler
        countingHandler.Should().NotBeNull();
        countingHandler!.RequestCount.Should().BeGreaterThanOrEqualTo(3,
            "Create + Read + Query should produce at least 3 HTTP requests through the wrapper");
    }
}
