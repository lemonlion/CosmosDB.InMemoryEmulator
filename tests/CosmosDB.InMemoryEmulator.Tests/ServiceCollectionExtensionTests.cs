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

public class UseInMemoryCosmosDBTests : IDisposable
{
    public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

    [Fact]
    public void Default_RegistersInMemoryCosmosClient()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosDB();

        var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<CosmosClient>();
        client.Should().BeOfType<InMemoryCosmosClient>();
    }

    [Fact]
    public void Default_RegistersInMemoryContainer()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosDB();

        var provider = services.BuildServiceProvider();
        var container = provider.GetRequiredService<Container>();
        container.Should().BeOfType<InMemoryContainer>();
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
        client.Should().BeOfType<InMemoryCosmosClient>();
    }

    [Fact]
    public void RemovesExistingContainer()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Container>(_ =>
            throw new InvalidOperationException("Should have been removed"));

        services.UseInMemoryCosmosDB();

        var provider = services.BuildServiceProvider();
        var container = provider.GetRequiredService<Container>();
        container.Should().BeOfType<InMemoryContainer>();
    }

    [Fact]
    public void ContainerIsFromClient()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosDB(o =>
        {
            o.DatabaseName = "mydb";
            o.AddContainer("orders", "/pk");
        });

        var provider = services.BuildServiceProvider();
        var client = (InMemoryCosmosClient)provider.GetRequiredService<CosmosClient>();
        var diContainer = provider.GetRequiredService<Container>();
        var clientContainer = client.GetContainer("mydb", "orders");

        diContainer.Should().BeSameAs(clientContainer);
    }

    [Fact]
    public void CustomDatabaseName()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosDB(o => o.DatabaseName = "TestDb");

        var provider = services.BuildServiceProvider();
        var client = (InMemoryCosmosClient)provider.GetRequiredService<CosmosClient>();
        var db = client.GetDatabase("TestDb");
        db.Id.Should().Be("TestDb");
    }

    [Fact]
    public void DefaultDatabaseName_IsInMemoryDb()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosDB();

        var provider = services.BuildServiceProvider();
        var client = (InMemoryCosmosClient)provider.GetRequiredService<CosmosClient>();
        var container = provider.GetRequiredService<Container>();

        // The default database should be "in-memory-db"
        var containerFromClient = client.GetContainer("in-memory-db", "in-memory-container");
        container.Should().BeSameAs(containerFromClient);
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
    public void RegistersFeedIteratorSetup()
    {
        InMemoryFeedIteratorSetup.Deregister();
        var services = new ServiceCollection();

        services.UseInMemoryCosmosDB();

        CosmosOverridableFeedIteratorExtensions.StaticFallbackFactory.Should().NotBeNull();
    }

    [Fact]
    public void OnClientCreatedCallback()
    {
        var services = new ServiceCollection();
        InMemoryCosmosClient? captured = null;

        services.UseInMemoryCosmosDB(o => o.OnClientCreated = c => captured = c);

        captured.Should().NotBeNull();
    }

    [Fact]
    public void IsSuperset_ContainerBehavior()
    {
        var services = new ServiceCollection();
        services.AddSingleton<CosmosClient>(_ =>
            throw new InvalidOperationException("Should have been removed"));
        services.AddSingleton<Container>(_ =>
            throw new InvalidOperationException("Should have been removed"));

        services.UseInMemoryCosmosDB();

        var provider = services.BuildServiceProvider();
        provider.GetRequiredService<CosmosClient>().Should().BeOfType<InMemoryCosmosClient>();
        provider.GetRequiredService<Container>().Should().BeOfType<InMemoryContainer>();
    }

    [Fact]
    public void ContainerPerDatabaseOverride()
    {
        var services = new ServiceCollection();

        services.UseInMemoryCosmosDB(o =>
        {
            o.AddContainer("c1", "/pk", databaseName: "db1");
            o.AddContainer("c2", "/pk", databaseName: "db2");
        });

        var provider = services.BuildServiceProvider();
        var client = (InMemoryCosmosClient)provider.GetRequiredService<CosmosClient>();
        var containers = provider.GetServices<Container>().ToList();

        containers.Should().HaveCount(2);

        // Each container should be from a different database
        var c1 = client.GetContainer("db1", "c1");
        var c2 = client.GetContainer("db2", "c2");
        containers.Should().Contain(c1);
        containers.Should().Contain(c2);
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
        services.UseInMemoryCosmosDB(o =>
        {
            o.DatabaseName = "db";
            o.AddContainer("orders", "/pk");
        });
        var provider = services.BuildServiceProvider();

        var client = provider.GetRequiredService<CosmosClient>();
        var diContainer = provider.GetRequiredService<Container>();
        var clientContainer = client.GetContainer("db", "orders");

        // Critical: repos that call client.GetContainer() must get the same instance
        diContainer.Should().BeSameAs(clientContainer);
    }
}
