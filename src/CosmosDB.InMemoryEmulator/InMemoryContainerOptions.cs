using Microsoft.Azure.Cosmos;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// Options for <see cref="ServiceCollectionExtensions.UseInMemoryCosmosContainers"/>.
/// Configures how Container registrations are replaced with in-memory equivalents.
/// </summary>
public class InMemoryContainerOptions
{
    /// <summary>
    /// Container configurations. If empty, a single default InMemoryContainer
    /// with partition key "/id" is registered.
    /// </summary>
    public List<ContainerConfig> Containers { get; } = new();

    /// <summary>
    /// When true (default), calls <see cref="InMemoryFeedIteratorSetup.Register"/>.
    /// </summary>
    public bool RegisterFeedIteratorSetup { get; set; } = true;

    /// <summary>
    /// Callback invoked with each <see cref="InMemoryContainer"/> after it is created.
    /// </summary>
    public Action<InMemoryContainer>? OnContainerCreated { get; set; }

    /// <summary>
    /// When set, each container automatically loads its state from this directory on creation
    /// and saves its state back on disposal. Files are named <c>{ContainerName}.json</c>.
    /// If the directory or file does not exist, the container starts empty—state is saved on first disposal.
    /// <para>
    /// This enables persisting container data between test runs without any manual
    /// <see cref="InMemoryContainer.ExportState"/>/<see cref="InMemoryContainer.ImportState"/> calls.
    /// </para>
    /// </summary>
    public string? StatePersistenceDirectory { get; set; }

    /// <summary>
    /// Adds a container configuration.
    /// </summary>
    public InMemoryContainerOptions AddContainer(
        string containerName = "in-memory-container",
        string partitionKeyPath = "/id")
    {
        Containers.Add(new ContainerConfig(containerName, partitionKeyPath));
        return this;
    }

    /// <summary>
    /// Adds a container configuration using full <see cref="ContainerProperties"/>,
    /// which supports UniqueKeyPolicy, DefaultTimeToLive, hierarchical partition keys, etc.
    /// </summary>
    public InMemoryContainerOptions AddContainer(ContainerProperties containerProperties)
    {
        Containers.Add(new ContainerConfig(
            containerProperties.Id,
            containerProperties.PartitionKeyPath,
            ContainerProperties: containerProperties));
        return this;
    }
}
