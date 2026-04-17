using Microsoft.Azure.Cosmos;

namespace CosmosDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Per-test-class fixture that creates real containers on the emulator shared
/// via <see cref="EmulatorSession"/>. Each test gets a uniquely-named container
/// (base name + short GUID) so queries like <c>SELECT * FROM c</c> only see
/// the current test's data and there is no cross-test bleed.
/// </summary>
public sealed class EmulatorTestFixture : ITestContainerFixture
{
    private readonly EmulatorSession _session;
    private readonly List<string> _createdContainers = [];

    public TestTarget Target => _session.Target;
    public bool IsEmulator => true;

    public EmulatorTestFixture(EmulatorSession session)
    {
        _session = session;
        if (!session.IsEmulator || session.EmulatorClient is null || session.EmulatorDatabase is null)
            throw new InvalidOperationException(
                $"EmulatorTestFixture requires an initialised emulator session. Target={session.Target}");
    }

    public Task<Container> CreateContainerAsync(
        string containerName,
        string partitionKeyPath,
        Action<ContainerProperties>? configure = null)
        => CreateContainerCoreAsync(containerName, name => new ContainerProperties(name, partitionKeyPath), configure);

    public Task<Container> CreateContainerAsync(
        string containerName,
        IReadOnlyList<string> partitionKeyPaths,
        Action<ContainerProperties>? configure = null)
        => CreateContainerCoreAsync(containerName, name => new ContainerProperties(name, partitionKeyPaths), configure);

    private async Task<Container> CreateContainerCoreAsync(
        string containerName, Func<string, ContainerProperties> propsFactory, Action<ContainerProperties>? configure)
    {
        var uniqueName = $"{containerName}-{Guid.NewGuid():N}";
        _createdContainers.Add(uniqueName);

        var props = propsFactory(uniqueName);
        configure?.Invoke(props);

        // Partition services can return 503 when the emulator is still starting
        // up a new container. The SDK does not retry those automatically for
        // control-plane ops, so we do it here.
        var response = await EmulatorRetry.RunAsync(
            () => _session.EmulatorDatabase!.CreateContainerIfNotExistsAsync(props),
            $"CreateContainer({props.Id})");
        return response.Container;
    }

    // Cleanup matters even with unique per-test names: the emulator's partition
    // pool is finite (PARTITION_COUNT=10 locally, 3 in CI), so leaving dozens
    // of test containers alive across a run will exhaust slots and cause
    // subsequent CreateContainer calls to 503. Locally the Docker container is
    // usually thrown away, but in CI the same emulator service serves every
    // test class in the job.
    public async ValueTask DisposeAsync()
    {
        if (_session.EmulatorDatabase is null) return;

        foreach (var name in _createdContainers)
        {
            try
            {
                await _session.EmulatorDatabase.GetContainer(name).DeleteContainerAsync();
            }
            catch
            {
                // Best-effort cleanup — container may already be gone.
            }
        }
        _createdContainers.Clear();
    }
}
