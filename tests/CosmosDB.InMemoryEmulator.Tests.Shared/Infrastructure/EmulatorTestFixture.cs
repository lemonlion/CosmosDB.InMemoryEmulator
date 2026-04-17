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
///   • Each test gets a uniquely-named container (base name + short GUID),
///     so no per-test item cleanup is needed and queries like
///     "SELECT * FROM c" always see only the current test's data
///   • Self-warm-up: proves write-path readiness by writing and reading
///     a document before any real test containers are created
///   • Patient retry with exponential backoff for all transient emulator errors
/// </summary>
public sealed class EmulatorTestFixture : ITestContainerFixture
{
    private const string DefaultEndpoint = "https://localhost:8081";
    private const string Key = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
    private const string DatabaseName = "parity-validation";
    private const int MaxRetries = 10;
    private const double MaxBackoffSeconds = 10;
    private const int WarmupMaxRetries = 30;
    private const double WarmupMaxBackoffSeconds = 15;

    private static CosmosClient? _sharedClient;
    private static Database? _sharedDatabase;
    private static bool _warmUpComplete;
    private static Exception? _warmUpFailure;
    private static readonly SemaphoreSlim _initLock = new(1, 1);
    private static string _resolvedEndpoint = DefaultEndpoint;

    private readonly List<string> _createdContainers = [];

    public TestTarget Target { get; }
    public bool IsEmulator => true;

    public EmulatorTestFixture(TestTarget target = TestTarget.EmulatorLinux, string? endpoint = null)
    {
        Target = target;
        if (endpoint != null)
            _resolvedEndpoint = endpoint;
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

    /// <summary>
    /// Ensures the emulator's write path is fully ready. Creates the database
    /// and a warmup container, then writes and reads a document to prove the
    /// full read/write path is operational — absorbing the initial 503s that
    /// the emulator returns while its partition services are still starting up.
    /// </summary>
    private static async Task EnsureWarmAsync()
    {
        if (_warmUpComplete) return;
        if (_warmUpFailure != null)
            throw new InvalidOperationException("Emulator warmup failed previously. See inner exception.", _warmUpFailure);

        await _initLock.WaitAsync();
        try
        {
            if (_warmUpComplete) return;
            if (_warmUpFailure != null)
                throw new InvalidOperationException("Emulator warmup failed previously. See inner exception.", _warmUpFailure);

            var client = GetOrCreateClient();

            Console.WriteLine("[EmulatorTestFixture] Warming up emulator write path...");

            _sharedDatabase = (await RetryAsync(
                () => client.CreateDatabaseIfNotExistsAsync(DatabaseName),
                "CreateDatabase", WarmupMaxRetries, WarmupMaxBackoffSeconds)).Database;

            // Use a unique container name to force a real creation, not a no-op IfNotExists
            var warmupName = $"_warmup-{Guid.NewGuid():N}";
            var warmupProps = new ContainerProperties(warmupName, "/id");
            var warmup = (await RetryAsync(
                () => _sharedDatabase.CreateContainerIfNotExistsAsync(warmupProps),
                "WarmupContainer", WarmupMaxRetries, WarmupMaxBackoffSeconds)).Container;

            // Prove the full read/write path is operational
            await RetryAsync(
                () => warmup.UpsertItemAsync(new { id = "probe", value = 1 }),
                "WarmupWrite", WarmupMaxRetries, WarmupMaxBackoffSeconds);
            await RetryAsync(
                () => warmup.ReadItemAsync<object>("probe", new PartitionKey("probe")),
                "WarmupRead", WarmupMaxRetries, WarmupMaxBackoffSeconds);

            // Clean up the warmup container
            try { await warmup.DeleteContainerAsync(); } catch { }

            Console.WriteLine("[EmulatorTestFixture] Emulator write path is ready.");
            _warmUpComplete = true;
        }
        catch (Exception ex)
        {
            _warmUpFailure = ex;
            Console.WriteLine($"[EmulatorTestFixture] Warmup FAILED after exhausting retries: {ex.Message}");
            throw;
        }
        finally
        {
            _initLock.Release();
        }
    }

    private async Task<Container> CreateContainerCoreAsync(
        string containerName, Func<string, ContainerProperties> propsFactory, Action<ContainerProperties>? configure)
    {
        await EnsureWarmAsync();

        var uniqueName = $"{containerName}-{Guid.NewGuid():N}";
        _createdContainers.Add(uniqueName);

        var props = propsFactory(uniqueName);
        configure?.Invoke(props);

        return (await RetryAsync(
            () => _sharedDatabase!.CreateContainerIfNotExistsAsync(props),
            $"CreateContainer({props.Id})")).Container;
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
            MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(20),
            RequestTimeout = TimeSpan.FromSeconds(10),
        };

        if (isHttps)
        {
            options.HttpClientFactory = () => new HttpClient(
                new HttpClientHandler
                {
                    ServerCertificateCustomValidationCallback =
                        HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
                })
            { Timeout = TimeSpan.FromSeconds(10) };
        }

        _sharedClient = new CosmosClient(endpoint, Key, options);
        return _sharedClient;
    }

    private static Task<T> RetryAsync<T>(Func<Task<T>> operation, string operationName) =>
        RetryAsync(operation, operationName, MaxRetries, MaxBackoffSeconds);

    private static async Task<T> RetryAsync<T>(
        Func<Task<T>> operation, string operationName, int maxRetries, double maxBackoffSeconds)
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                return await operation();
            }
            catch (Exception ex) when (attempt < maxRetries && IsTransient(ex))
            {
                var delay = TimeSpan.FromSeconds(Math.Min(Math.Pow(2, attempt), maxBackoffSeconds));
                Console.WriteLine(
                    $"[EmulatorTestFixture] {operationName} attempt {attempt + 1}/{maxRetries} failed " +
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

    public async ValueTask DisposeAsync()
    {
        foreach (var name in _createdContainers)
        {
            try
            {
                await _sharedDatabase!.GetContainer(name).DeleteContainerAsync();
            }
            catch
            {
                // Best-effort cleanup — container may already be gone
            }
        }
        _createdContainers.Clear();
    }
}
