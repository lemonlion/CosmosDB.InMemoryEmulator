using Microsoft.Azure.Cosmos;

namespace CosmosDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Fixture that creates containers backed by FakeCosmosHandler + InMemoryContainer.
/// Uses the real CosmosClient SDK pipeline (HTTP, serialization, LINQ→SQL)
/// for an apples-to-apples comparison with the emulator fixture.
/// </summary>
public sealed class InMemoryTestFixture : ITestContainerFixture
{
    private readonly List<(CosmosClient Client, FakeCosmosHandler Handler)> _tracked = [];

    public TestTarget Target => TestTarget.InMemory;
    public bool IsEmulator => false;

    public Task<Container> CreateContainerAsync(
        string containerName,
        string partitionKeyPath,
        Action<ContainerProperties>? configure = null)
    {
        var props = new ContainerProperties(containerName, partitionKeyPath);
        configure?.Invoke(props);
        return Task.FromResult(BuildContainer(props));
    }

    public Task<Container> CreateContainerAsync(
        string containerName,
        IReadOnlyList<string> partitionKeyPaths,
        Action<ContainerProperties>? configure = null)
    {
        var props = new ContainerProperties(containerName, partitionKeyPaths);
        configure?.Invoke(props);
        return Task.FromResult(BuildContainer(props));
    }

    private Container BuildContainer(ContainerProperties props)
    {
        var inMemory = new InMemoryContainer(props);
        var handler = new FakeCosmosHandler(inMemory);
        var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                RequestTimeout = TimeSpan.FromSeconds(10),
                HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
            });
        _tracked.Add((client, handler));
        return client.GetContainer("db", props.Id);
    }

    public ValueTask DisposeAsync()
    {
        foreach (var (client, handler) in _tracked)
        {
            client.Dispose();
            handler.Dispose();
        }
        _tracked.Clear();
        return ValueTask.CompletedTask;
    }
}
