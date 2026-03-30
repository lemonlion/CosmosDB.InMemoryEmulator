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

        // Remove existing registrations
        services.RemoveAll<CosmosClient>();
        services.RemoveAll<Container>();

        // Create the InMemoryCosmosClient
        var client = new InMemoryCosmosClient();

        // Determine container configs
        var containers = options.Containers.Count > 0
            ? options.Containers
            : [new ContainerConfig("in-memory-container")];

        // Pre-create databases and containers with correct partition key paths
        foreach (var containerConfig in containers)
        {
            var dbName = containerConfig.DatabaseName ?? databaseName;
            var db = (InMemoryDatabase)client.GetDatabase(dbName);
            db.GetOrCreateContainer(containerConfig.ContainerName, containerConfig.PartitionKeyPath);
        }

        options.OnClientCreated?.Invoke(client);

        // Register the client
        services.AddSingleton<CosmosClient>(client);

        // Register containers
        foreach (var containerConfig in containers)
        {
            var dbName = containerConfig.DatabaseName ?? databaseName;
            var container = client.GetContainer(dbName, containerConfig.ContainerName);
            services.Add(new ServiceDescriptor(typeof(Container), _ => container, containerLifetime));
        }

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
}
