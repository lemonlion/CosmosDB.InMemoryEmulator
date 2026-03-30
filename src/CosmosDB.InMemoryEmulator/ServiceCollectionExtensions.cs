using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// Extension methods for replacing Cosmos DB service registrations with in-memory equivalents.
/// Designed to be called from <c>ConfigureTestServices</c> inside <c>WebApplicationFactory</c>.
/// </summary>
public static class ServiceCollectionExtensions
{
    private const string DefaultDatabaseName = "in-memory-db";

    /// <summary>
    /// Replaces all registered <see cref="CosmosClient"/> and <see cref="Container"/>
    /// instances in the service collection with in-memory equivalents.
    /// Also registers <see cref="InMemoryFeedIteratorSetup"/> so that
    /// <c>.ToFeedIteratorOverridable()</c> works correctly.
    /// </summary>
    public static IServiceCollection UseInMemoryCosmosDB(
        this IServiceCollection services,
        Action<InMemoryCosmosOptions>? configure = null)
    {
        var options = new InMemoryCosmosOptions();
        configure?.Invoke(options);

        var databaseName = options.DatabaseName ?? DefaultDatabaseName;

        // Determine how many Container registrations existed and their lifetime
        var existingContainerDescriptors = services
            .Where(d => d.ServiceType == typeof(Container))
            .ToList();
        var containerLifetime = existingContainerDescriptors.FirstOrDefault()?.Lifetime
                                ?? ServiceLifetime.Singleton;

        // Always replace CosmosClient
        services.RemoveAll<CosmosClient>();

        // Create the InMemoryCosmosClient
        var client = new InMemoryCosmosClient();

        if (options.Containers.Count > 0)
        {
            // Explicit mode: remove existing Container registrations and replace with configured ones
            services.RemoveAll<Container>();

            foreach (var containerConfig in options.Containers)
            {
                var dbName = containerConfig.DatabaseName ?? databaseName;
                var db = (InMemoryDatabase)client.GetDatabase(dbName);
                db.GetOrCreateContainer(containerConfig.ContainerName, containerConfig.PartitionKeyPath);
            }

            foreach (var containerConfig in options.Containers)
            {
                var dbName = containerConfig.DatabaseName ?? databaseName;
                var container = client.GetContainer(dbName, containerConfig.ContainerName);
                services.Add(new ServiceDescriptor(typeof(Container), _ => container, containerLifetime));
            }
        }
        else if (existingContainerDescriptors.Count > 0)
        {
            // Auto-detect mode: keep existing Container registrations.
            // They resolve against the new InMemoryCosmosClient via GetContainer().
        }
        else
        {
            // No existing registrations and no explicit config: create a default container
            var db = (InMemoryDatabase)client.GetDatabase(databaseName);
            db.GetOrCreateContainer("in-memory-container", "/id");
            var container = client.GetContainer(databaseName, "in-memory-container");
            services.Add(new ServiceDescriptor(typeof(Container), _ => container, ServiceLifetime.Singleton));
        }

        options.OnClientCreated?.Invoke(client);

        // Register the client
        services.AddSingleton<CosmosClient>(client);

        if (options.RegisterFeedIteratorSetup)
            InMemoryFeedIteratorSetup.Register();

        return services;
    }

    /// <summary>
    /// Replaces all registered <see cref="Container"/> instances in the service collection
    /// with in-memory equivalents. Does NOT replace <see cref="CosmosClient"/>.
    /// Also registers <see cref="InMemoryFeedIteratorSetup"/> so that
    /// <c>.ToFeedIteratorOverridable()</c> works correctly.
    /// </summary>
    public static IServiceCollection UseInMemoryCosmosContainers(
        this IServiceCollection services,
        Action<InMemoryContainerOptions>? configure = null)
    {
        var options = new InMemoryContainerOptions();
        configure?.Invoke(options);

        // Determine existing lifetime
        var existingDescriptor = services.FirstOrDefault(d => d.ServiceType == typeof(Container));
        var lifetime = existingDescriptor?.Lifetime ?? ServiceLifetime.Singleton;

        // Remove existing Container registrations
        services.RemoveAll<Container>();

        // Determine container configs
        var containers = options.Containers.Count > 0
            ? options.Containers
            : [new ContainerConfig("in-memory-container")];

        // Register InMemoryContainers
        foreach (var containerConfig in containers)
        {
            var container = new InMemoryContainer(
                containerConfig.ContainerName,
                containerConfig.PartitionKeyPath);

            options.OnContainerCreated?.Invoke(container);

            services.Add(new ServiceDescriptor(typeof(Container), _ => container, lifetime));
        }

        if (options.RegisterFeedIteratorSetup)
            InMemoryFeedIteratorSetup.Register();

        return services;
    }

    /// <summary>
    /// Replaces a typed <typeparamref name="TClient"/> registration with an in-memory equivalent.
    /// Designed for Pattern 2 (SCA.Common style) where multiple typed <see cref="CosmosClient"/>
    /// subclasses are registered in DI and repos resolve the specific typed client.
    /// <para>
    /// <typeparamref name="TClient"/> must extend <see cref="InMemoryCosmosClient"/>.
    /// In your <b>test project</b>, create a one-line subclass that shadows the production type:
    /// <code>
    /// // In test project — shadows the production EmployeeCosmosClient
    /// public class EmployeeCosmosClient : InMemoryCosmosClient { }
    /// </code>
    /// No changes to production code are needed. The test subclass must be in the same namespace
    /// or use a <c>using</c> alias so that DI resolves the test type for repos that depend on
    /// <c>EmployeeCosmosClient</c>.
    /// </para>
    /// <para>
    /// Does NOT register <see cref="Container"/> in DI — Pattern 2 repos call
    /// <c>client.GetContainer()</c> directly. Does NOT register as the base <see cref="CosmosClient"/>
    /// type — each typed client is independent.
    /// </para>
    /// </summary>
    public static IServiceCollection UseInMemoryCosmosDB<TClient>(
        this IServiceCollection services,
        Action<InMemoryCosmosOptions>? configure = null)
        where TClient : InMemoryCosmosClient, new()
    {
        var options = new InMemoryCosmosOptions();
        configure?.Invoke(options);

        var databaseName = options.DatabaseName ?? DefaultDatabaseName;

        // Determine existing lifetime
        var existingDescriptor = services.FirstOrDefault(d => d.ServiceType == typeof(TClient));
        var lifetime = existingDescriptor?.Lifetime ?? ServiceLifetime.Singleton;

        // Remove existing registration for the typed client
        services.RemoveAll<TClient>();

        // Create the typed client instance
        var client = new TClient();

        // Pre-create databases and containers
        var containers = options.Containers.Count > 0
            ? options.Containers
            : [new ContainerConfig("in-memory-container")];

        foreach (var containerConfig in containers)
        {
            var dbName = containerConfig.DatabaseName ?? databaseName;
            var db = (InMemoryDatabase)client.GetDatabase(dbName);
            db.GetOrCreateContainer(containerConfig.ContainerName, containerConfig.PartitionKeyPath);
        }

        options.OnClientCreated?.Invoke(client);

        // Register as the typed client only
        services.Add(new ServiceDescriptor(typeof(TClient), _ => client, lifetime));

        if (options.RegisterFeedIteratorSetup)
            InMemoryFeedIteratorSetup.Register();

        return services;
    }
}
