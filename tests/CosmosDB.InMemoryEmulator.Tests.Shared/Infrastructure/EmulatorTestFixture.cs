using System.Collections.Concurrent;
using Microsoft.Azure.Cosmos;

namespace CosmosDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Fixture that creates real containers on a running Cosmos DB emulator.
/// Shares a single CosmosClient and caches containers by base name so that
/// all test methods within the same class reuse the same container, drastically
/// reducing the number of create/delete cycles against the emulator.
/// </summary>
public sealed class EmulatorTestFixture : ITestContainerFixture
{
    private const string DefaultEndpoint = "https://localhost:8081";
    private const string Key = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
    private const string DatabaseName = "parity-validation";
    private const int MaxRetries = 10;

    // Shared across all fixture instances for the entire test run.
    // The emulator is resource-constrained; reusing a single client and
    // caching containers avoids the create/delete churn that crashes it.
    private static CosmosClient? _sharedClient;
    private static Database? _sharedDatabase;
    private static readonly ConcurrentDictionary<string, Container> _containerCache = new();
    private static readonly SemaphoreSlim _initLock = new(1, 1);
    private static string _resolvedEndpoint = DefaultEndpoint;

    public TestTarget Target { get; }
    public bool IsEmulator => true;

    public EmulatorTestFixture(TestTarget target = TestTarget.EmulatorLinux, string? endpoint = null)
    {
        Target = target;
        if (endpoint != null)
            _resolvedEndpoint = endpoint;
    }

    public async Task<Container> CreateContainerAsync(
        string containerName,
        string partitionKeyPath,
        Action<ContainerProperties>? configure = null)
    {
        if (_containerCache.TryGetValue(containerName, out var cached))
            return cached;

        await _initLock.WaitAsync();
        try
        {
            if (_containerCache.TryGetValue(containerName, out cached))
                return cached;

            var uniqueName = $"{containerName}-{Guid.NewGuid():N}";
            var props = new ContainerProperties(uniqueName, partitionKeyPath);
            configure?.Invoke(props);
            var container = await CreateRealContainerAsync(props);
            _containerCache.TryAdd(containerName, container);
            return container;
        }
        finally
        {
            _initLock.Release();
        }
    }

    public async Task<Container> CreateContainerAsync(
        string containerName,
        IReadOnlyList<string> partitionKeyPaths,
        Action<ContainerProperties>? configure = null)
    {
        if (_containerCache.TryGetValue(containerName, out var cached))
            return cached;

        await _initLock.WaitAsync();
        try
        {
            if (_containerCache.TryGetValue(containerName, out cached))
                return cached;

            var uniqueName = $"{containerName}-{Guid.NewGuid():N}";
            var props = new ContainerProperties(uniqueName, partitionKeyPaths);
            configure?.Invoke(props);
            var container = await CreateRealContainerAsync(props);
            _containerCache.TryAdd(containerName, container);
            return container;
        }
        finally
        {
            _initLock.Release();
        }
    }

    private async Task<Container> CreateRealContainerAsync(ContainerProperties props)
    {
        var client = GetOrCreateClient();
        _sharedDatabase ??= await CreateDatabaseWithRetryAsync(client);
        return (await CreateContainerWithRetryAsync(props)).Container;
    }

    private static CosmosClient GetOrCreateClient()
    {
        if (_sharedClient != null) return _sharedClient;

        var endpoint = _resolvedEndpoint;
        var isHttps = endpoint.StartsWith("https://", StringComparison.OrdinalIgnoreCase);

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

        _sharedClient = new CosmosClient(endpoint, Key, options);
        return _sharedClient;
    }

    private static async Task<Database> CreateDatabaseWithRetryAsync(CosmosClient client)
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                return (await client.CreateDatabaseIfNotExistsAsync(DatabaseName)).Database;
            }
            catch (CosmosException ex) when (attempt < MaxRetries && IsTransient(ex))
            {
                await Task.Delay(GetBackoff(attempt));
            }
        }
    }

    private static async Task<ContainerResponse> CreateContainerWithRetryAsync(ContainerProperties props)
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                return await _sharedDatabase!.CreateContainerIfNotExistsAsync(props);
            }
            catch (CosmosException ex) when (attempt < MaxRetries && IsTransient(ex))
            {
                await Task.Delay(GetBackoff(attempt));
            }
        }
    }

    private static bool IsTransient(CosmosException ex) =>
        ex.StatusCode is
            System.Net.HttpStatusCode.ServiceUnavailable or  // 503
            System.Net.HttpStatusCode.InternalServerError or // 500
            System.Net.HttpStatusCode.RequestTimeout or      // 408
            System.Net.HttpStatusCode.TooManyRequests or     // 429
            System.Net.HttpStatusCode.NotFound;              // 404 (substatus 1013: collection not yet available)

    private static TimeSpan GetBackoff(int attempt) =>
        TimeSpan.FromSeconds(Math.Min(Math.Pow(2, attempt), 8)); // 1s, 2s, 4s, 8s, 8s, ...

    public ValueTask DisposeAsync()
    {
        // No-op: the shared client, database, and cached containers persist for the
        // entire test run. The CI emulator container is torn down after tests complete,
        // so explicit cleanup is unnecessary and would add harmful churn.
        return ValueTask.CompletedTask;
    }
}
