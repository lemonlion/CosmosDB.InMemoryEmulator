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
///   • Container cache by name (one create per test class, not per method)
///   • Self-warm-up: proves write-path readiness by creating a throwaway
///     container before any real test containers are created
///   • Patient retry with exponential backoff for all transient emulator errors
///   • Cooldown between container creations to avoid overwhelming the emulator
/// </summary>
public sealed class EmulatorTestFixture : ITestContainerFixture
{
    private const string DefaultEndpoint = "https://localhost:8081";
    private const string Key = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
    private const string DatabaseName = "parity-validation";
    private const int MaxRetries = 30;
    private const double MaxBackoffSeconds = 10;
    private static readonly TimeSpan ContainerCreationCooldown = TimeSpan.FromSeconds(2);

    private static CosmosClient? _sharedClient;
    private static Database? _sharedDatabase;
    private static bool _warmUpComplete;
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

            await EnsureWarmAsync();

            var props = new ContainerProperties(containerName, partitionKeyPath);
            configure?.Invoke(props);
            var container = await CreateContainerCoreAsync(props);
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

            await EnsureWarmAsync();

            var props = new ContainerProperties(containerName, partitionKeyPaths);
            configure?.Invoke(props);
            var container = await CreateContainerCoreAsync(props);
            _containerCache.TryAdd(containerName, container);
            return container;
        }
        finally
        {
            _initLock.Release();
        }
    }

    /// <summary>
    /// Ensures the emulator's write path is fully ready. Creates the database
    /// and a throwaway container, absorbing the initial 503s that the emulator
    /// returns while its partition services are still starting up.
    /// </summary>
    private static async Task EnsureWarmAsync()
    {
        if (_warmUpComplete) return;

        var client = GetOrCreateClient();

        Console.WriteLine("[EmulatorTestFixture] Warming up emulator write path...");

        _sharedDatabase = (await RetryAsync(
            () => client.CreateDatabaseIfNotExistsAsync(DatabaseName),
            "CreateDatabase")).Database;

        // Create and delete a throwaway container to prove write-path readiness.
        // This absorbs the initial 503/1007 period that can last several minutes.
        var warmupProps = new ContainerProperties("_warmup", "/id");
        await RetryAsync(
            () => _sharedDatabase.CreateContainerIfNotExistsAsync(warmupProps),
            "WarmupContainer");

        try
        {
            await _sharedDatabase.GetContainer("_warmup").DeleteContainerAsync();
        }
        catch
        {
            // Best-effort cleanup — the container will be gone when the emulator shuts down.
        }

        Console.WriteLine("[EmulatorTestFixture] Emulator write path is ready.");
        _warmUpComplete = true;
    }

    private static async Task<Container> CreateContainerCoreAsync(ContainerProperties props)
    {
        var container = (await RetryAsync(
            () => _sharedDatabase!.CreateContainerIfNotExistsAsync(props),
            $"CreateContainer({props.Id})")).Container;
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

    private static async Task<T> RetryAsync<T>(Func<Task<T>> operation, string operationName)
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                return await operation();
            }
            catch (Exception ex) when (attempt < MaxRetries && IsTransient(ex))
            {
                var delay = GetBackoff(attempt);
                Console.WriteLine(
                    $"[EmulatorTestFixture] {operationName} attempt {attempt + 1} failed " +
                    $"({ex.GetType().Name}), retrying in {delay.TotalSeconds:F0}s...");
                await Task.Delay(delay);
            }
        }
    }

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
        // No-op: cached containers persist for the entire test run.
        // The CI emulator is torn down after tests complete.
        return ValueTask.CompletedTask;
    }
}
