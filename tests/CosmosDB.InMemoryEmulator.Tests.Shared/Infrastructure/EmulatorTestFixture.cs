using Microsoft.Azure.Cosmos;

namespace CosmosDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Fixture that creates real containers on a running Cosmos DB emulator.
/// Uses unique container names (UUID suffix) to avoid cross-test pollution.
/// Cleans up containers on dispose.
/// </summary>
public sealed class EmulatorTestFixture : ITestContainerFixture
{
    private const string DefaultEndpoint = "https://localhost:8081";
    private const string Key = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
    private const string DatabaseName = "parity-validation";

    private readonly CosmosClient _client;
    private readonly List<Container> _containersToCleanup = [];
    private Database? _database;

    public TestTarget Target { get; }
    public bool IsEmulator => true;

    public EmulatorTestFixture(TestTarget target = TestTarget.EmulatorLinux, string? endpoint = null)
    {
        Target = target;
        var resolvedEndpoint = endpoint ?? DefaultEndpoint;
        var isHttps = resolvedEndpoint.StartsWith("https://", StringComparison.OrdinalIgnoreCase);

        var options = new CosmosClientOptions
        {
            ConnectionMode = ConnectionMode.Gateway,
            MaxRetryAttemptsOnRateLimitedRequests = 3,
            RequestTimeout = TimeSpan.FromSeconds(30),
        };

        if (isHttps)
        {
            options.HttpClientFactory = () => new HttpClient(
                new HttpClientHandler
                {
                    ServerCertificateCustomValidationCallback =
                        HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
                })
            { Timeout = TimeSpan.FromSeconds(30) };
        }

        _client = new CosmosClient(resolvedEndpoint, Key, options);

        // TODO: Consider adding a readiness check here (like EmulatorDetector) so that
        // tests fail fast with a clear message when the emulator endpoint is unreachable,
        // instead of timing out with opaque connection errors.
    }

    public async Task<Container> CreateContainerAsync(
        string containerName,
        string partitionKeyPath,
        Action<ContainerProperties>? configure = null)
    {
        var uniqueName = $"{containerName}-{Guid.NewGuid():N}";
        var props = new ContainerProperties(uniqueName, partitionKeyPath);
        configure?.Invoke(props);
        return await CreateRealContainerAsync(props);
    }

    public async Task<Container> CreateContainerAsync(
        string containerName,
        IReadOnlyList<string> partitionKeyPaths,
        Action<ContainerProperties>? configure = null)
    {
        var uniqueName = $"{containerName}-{Guid.NewGuid():N}";
        var props = new ContainerProperties(uniqueName, partitionKeyPaths);
        configure?.Invoke(props);
        return await CreateRealContainerAsync(props);
    }

    private async Task<Container> CreateRealContainerAsync(ContainerProperties props)
    {
        _database ??= (await _client.CreateDatabaseIfNotExistsAsync(DatabaseName)).Database;
        var response = await _database.CreateContainerIfNotExistsAsync(props);
        _containersToCleanup.Add(response.Container);
        return response.Container;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var container in _containersToCleanup)
        {
            try { await container.DeleteContainerAsync(); }
            catch { /* best effort cleanup */ }
        }
        _containersToCleanup.Clear();
        _client.Dispose();
    }
}
