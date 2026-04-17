using System.Collections.Concurrent;
using System.Net.Sockets;
using Microsoft.Azure.Cosmos;

namespace CosmosDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Fixture that creates real containers on a running Cosmos DB emulator.
/// All emulator resilience is self-contained here — the CI workflow only
/// needs to start the emulator and wait for its HTTP endpoint.
///
/// Key strategies:
///   • Single shared CosmosClient for the entire test run
///   • Container cache keyed by name (one create per test class, not per method)
///   • Patient retry with exponential backoff for all transient emulator errors
///     including write-path 503s, connection refused, and 404 "not yet available"
///   • Throttle delay between container creations to avoid overwhelming the emulator
/// </summary>
public sealed class EmulatorTestFixture : ITestContainerFixture
{
    private const string DefaultEndpoint = "https://localhost:8081";
    private const string Key = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
    private const string DatabaseName = "parity-validation";

    /// <summary>Maximum number of retry attempts for any single Cosmos operation.</summary>
    private const int MaxRetries = 20;

    /// <summary>Maximum backoff delay between retries (seconds).</summary>
    private const double MaxBackoffSeconds = 30;

    /// <summary>
    /// Cooldown after creating a container, giving the emulator time to settle
    /// its partition services before the next creation request.
    /// </summary>
    private static readonly TimeSpan ContainerCreationCooldown = TimeSpan.FromSeconds(1);

    // Shared across all fixture instances for the entire test run.
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

            var props = new ContainerProperties(containerName, partitionKeyPath);
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

            var props = new ContainerProperties(containerName, partitionKeyPaths);
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

        var container = (await CreateContainerWithRetryAsync(props)).Container;
        await Task.Delay(ContainerCreationCooldown);
        return container;
    }

    private static CosmosClient GetOrCreateClient()
    {
        if (_sharedClient != null) return _sharedClient;

        var endpoint = _resolvedEndpoint;
        var isHttps = endpoint.StartsWith("https://", StringComparison.OrdinalIgnoreCase);

        var options = new CosmosClientOptions
        {
            ConnectionMode = ConnectionMode.Gateway,
            MaxRetryAttemptsOnRateLimitedRequests = 5,
            MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(30),
            RequestTimeout = TimeSpan.FromSeconds(65),
        };

        if (isHttps)
        {
            options.HttpClientFactory = () => new HttpClient(
                new HttpClientHandler
                {
                    ServerCertificateCustomValidationCallback =
                        HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
                })
            { Timeout = TimeSpan.FromSeconds(65) };
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
            catch (Exception ex) when (attempt < MaxRetries && IsTransient(ex))
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
            catch (Exception ex) when (attempt < MaxRetries && IsTransient(ex))
            {
                await Task.Delay(GetBackoff(attempt));
            }
        }
    }

    /// <summary>
    /// Determines whether an exception represents a transient emulator error
    /// that should be retried. Covers the full progression of emulator failures:
    /// write-path 503 → internal 500 → timeout 408 → rate limit 429 →
    /// 404 "collection not yet available" → connection refused.
    /// </summary>
    private static bool IsTransient(Exception ex) => ex switch
    {
        CosmosException ce => ce.StatusCode is
            System.Net.HttpStatusCode.ServiceUnavailable or
            System.Net.HttpStatusCode.InternalServerError or
            System.Net.HttpStatusCode.RequestTimeout or
            System.Net.HttpStatusCode.TooManyRequests or
            System.Net.HttpStatusCode.NotFound,
        HttpRequestException => true,
        SocketException => true,
        _ => ex.InnerException != null && IsTransient(ex.InnerException),
    };

    private static TimeSpan GetBackoff(int attempt) =>
        TimeSpan.FromSeconds(Math.Min(Math.Pow(2, attempt), MaxBackoffSeconds));

    public ValueTask DisposeAsync()
    {
        // No-op: the shared client, database, and cached containers persist for the
        // entire test run. The CI emulator container is torn down after tests complete,
        // so explicit cleanup is unnecessary and would add harmful churn.
        return ValueTask.CompletedTask;
    }
}
