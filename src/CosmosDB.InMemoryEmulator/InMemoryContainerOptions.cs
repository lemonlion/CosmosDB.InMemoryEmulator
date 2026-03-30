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
    /// Adds a container configuration.
    /// </summary>
    public InMemoryContainerOptions AddContainer(
        string containerName = "in-memory-container",
        string partitionKeyPath = "/id")
    {
        Containers.Add(new ContainerConfig(containerName, partitionKeyPath));
        return this;
    }
}
