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
    private const string FakeConnectionString = "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;";

    /// <summary>
    /// Replaces all registered <see cref="CosmosClient"/> and <see cref="Container"/>
    /// instances in the service collection with in-memory equivalents backed by
    /// <see cref="FakeCosmosHandler"/>. This provides the highest fidelity: a real
    /// <see cref="CosmosClient"/> exercises the full SDK HTTP pipeline, with
    /// <see cref="FakeCosmosHandler"/> intercepting requests in-process.
    /// LINQ <c>.ToFeedIterator()</c> works without any production code changes.
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

        // Determine container configs
        List<ContainerConfig> containerConfigs;
        if (options.Containers.Count > 0)
        {
            containerConfigs = options.Containers;
            // Explicit mode: remove existing Container registrations
            services.RemoveAll<Container>();
        }
        else if (existingContainerDescriptors.Count == 0)
        {
            // No existing registrations and no explicit config: create a default container
            containerConfigs = [new ContainerConfig("in-memory-container")];
        }
        else
        {
            // Auto-detect mode: no explicit containers, but there are existing registrations.
            // We still need to create a handler. Use a default container.
            containerConfigs = [new ContainerConfig("in-memory-container")];
        }

        // Create InMemoryContainers and FakeCosmosHandlers
        var handlers = new Dictionary<string, FakeCosmosHandler>();
        foreach (var config in containerConfigs)
        {
            var container = config.ContainerProperties != null
                ? new InMemoryContainer(config.ContainerProperties)
                : new InMemoryContainer(config.ContainerName, config.PartitionKeyPath);

            if (options.StatePersistenceDirectory is not null)
            {
                var dbName = config.DatabaseName ?? databaseName;
                var fileName = $"{dbName}_{config.ContainerName}.json";
                container.StateFilePath = Path.Combine(options.StatePersistenceDirectory, fileName);
                container.LoadPersistedState();
            }

            var handler = new FakeCosmosHandler(container);
            // Use compound key (db/container) when databaseName is specified
            if (config.DatabaseName is not null)
            {
                handlers[$"{config.DatabaseName}/{config.ContainerName}"] = handler;
            }
            // Also register container-only key (for backward compat and single-db scenarios),
            // but only if no other container with the same name but different database exists.
            if (!handlers.ContainsKey(config.ContainerName))
            {
                handlers[config.ContainerName] = handler;
            }
            options.OnHandlerCreated?.Invoke(config.ContainerName, handler);
        }

        // Build the HTTP handler (single or router)
        HttpMessageHandler httpHandler = handlers.Count == 1
            ? handlers.Values.First()
            : FakeCosmosHandler.CreateRouter(handlers);

        // Apply optional wrapper (e.g. DelegatingHandler for logging/tracking)
        var finalHandler = options.HttpMessageHandlerWrapper?.Invoke(httpHandler) ?? httpHandler;

        // Create a real CosmosClient with the FakeCosmosHandler
        var client = new CosmosClient(
            FakeConnectionString,
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                HttpClientFactory = () => new HttpClient(finalHandler)
            });

        options.OnClientCreated?.Invoke(client);

        // Register Container(s) in DI
        if (options.Containers.Count > 0 || existingContainerDescriptors.Count == 0)
        {
            foreach (var config in containerConfigs)
            {
                var dbName = config.DatabaseName ?? databaseName;
                var containerName = config.ContainerName;
                services.Add(new ServiceDescriptor(
                    typeof(Container),
                    _ => client.GetContainer(dbName, containerName),
                    containerLifetime));
            }
        }
        // else: auto-detect mode — keep existing Container registrations as-is

        // Register the real CosmosClient backed by FakeCosmosHandler
        services.AddSingleton<CosmosClient>(client);

        // No need for InMemoryFeedIteratorSetup — FakeCosmosHandler handles .ToFeedIterator() natively

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
            var container = containerConfig.ContainerProperties != null
                ? new InMemoryContainer(containerConfig.ContainerProperties)
                : new InMemoryContainer(
                    containerConfig.ContainerName,
                    containerConfig.PartitionKeyPath);

            if (options.StatePersistenceDirectory is not null)
            {
                var fileName = $"{containerConfig.ContainerName}.json";
                container.StateFilePath = Path.Combine(options.StatePersistenceDirectory, fileName);
                container.LoadPersistedState();
            }

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
            InMemoryContainer container;
            if (containerConfig.ContainerProperties != null)
                container = db.GetOrCreateContainer(containerConfig.ContainerProperties);
            else
                container = db.GetOrCreateContainer(containerConfig.ContainerName, containerConfig.PartitionKeyPath);

            if (options.StatePersistenceDirectory is not null)
            {
                var fileName = $"{dbName}_{containerConfig.ContainerName}.json";
                container.StateFilePath = Path.Combine(options.StatePersistenceDirectory, fileName);
                container.LoadPersistedState();
            }
        }

        options.OnClientCreated?.Invoke(client);

        // Register as the typed client only
        services.Add(new ServiceDescriptor(typeof(TClient), _ => client, lifetime));

        if (options.RegisterFeedIteratorSetup)
            InMemoryFeedIteratorSetup.Register();

        return services;
    }
}
