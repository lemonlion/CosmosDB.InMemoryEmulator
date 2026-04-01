using Microsoft.Azure.Cosmos;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// Options for <see cref="ServiceCollectionExtensions.UseInMemoryCosmosDB"/>.
/// Configures how CosmosClient and Container registrations are replaced with in-memory equivalents.
/// </summary>
public class InMemoryCosmosOptions
{
    /// <summary>
    /// Container configurations. If empty, the method registers a single default
    /// container with partition key "/id".
    /// </summary>
    public List<ContainerConfig> Containers { get; } = new();

    /// <summary>
    /// The database name to use. Falls back to "in-memory-db" if not specified.
    /// </summary>
    public string? DatabaseName { get; set; }

    /// <summary>
    /// When true (default), calls <see cref="InMemoryFeedIteratorSetup.Register"/> so that
    /// <c>.ToFeedIteratorOverridable()</c> works for LINQ queries.
    /// </summary>
    public bool RegisterFeedIteratorSetup { get; set; } = true;

    /// <summary>
    /// Callback invoked with the <see cref="CosmosClient"/> after it is created.
    /// Use this to capture the client reference for test assertions, etc.
    /// </summary>
    public Action<CosmosClient>? OnClientCreated { get; set; }

    /// <summary>
    /// Callback invoked with each <see cref="FakeCosmosHandler"/> after it is created.
    /// Use this to capture handler references for fault injection, request logging,
    /// or to access the backing <see cref="InMemoryContainer"/> via
    /// <see cref="FakeCosmosHandler.BackingContainer"/>.
    /// </summary>
    public Action<string, FakeCosmosHandler>? OnHandlerCreated { get; set; }

    /// <summary>
    /// Adds a container configuration.
    /// </summary>
    public InMemoryCosmosOptions AddContainer(
        string containerName,
        string partitionKeyPath = "/id",
        string? databaseName = null)
    {
        Containers.Add(new ContainerConfig(containerName, partitionKeyPath, databaseName));
        return this;
    }
}
