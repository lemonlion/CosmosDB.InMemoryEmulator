using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests.Performance;

public class LoadTests(ITestOutputHelper output)
{
    private static readonly int _targetCallsPerSecond =
        int.TryParse(Environment.GetEnvironmentVariable("LOAD_TEST_RPS"), out var rps) ? rps : 500;

    private static readonly int _durationSeconds =
        int.TryParse(Environment.GetEnvironmentVariable("LOAD_TEST_DURATION_SECONDS"), out var dur) ? dur : 60;

    private static readonly int _totalOperations = _targetCallsPerSecond * _durationSeconds;

    [Fact]
    public async Task ReadHeavyLoad_80PercentReads_20PercentWrites()
    {
        var container = new InMemoryContainer("read-heavy", "/partitionKey");
        var stats = new LoadStats();

        await SeedDocuments(container, count: 1000);

        await RunLoad(container, stats, readWeight: 80, writeWeight: 20, seedCount: 1000);

        ReportAndAssert(stats, "Read-Heavy (80/20)");
    }

    [Fact]
    public async Task WriteHeavyLoad_20PercentReads_80PercentWrites()
    {
        var container = new InMemoryContainer("write-heavy", "/partitionKey");
        var stats = new LoadStats();

        await SeedDocuments(container, count: 200);

        await RunLoad(container, stats, readWeight: 20, writeWeight: 80, seedCount: 200);

        ReportAndAssert(stats, "Write-Heavy (20/80)");
    }

    [Fact]
    public async Task EvenMixLoad_50PercentReads_50PercentWrites()
    {
        var container = new InMemoryContainer("even-mix", "/partitionKey");
        var stats = new LoadStats();

        await SeedDocuments(container, count: 500);

        await RunLoad(container, stats, readWeight: 50, writeWeight: 50, seedCount: 500);

        ReportAndAssert(stats, "Even Mix (50/50)");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Load runner
    // ═══════════════════════════════════════════════════════════════════════════

    private async Task RunLoad(
        InMemoryContainer container,
        LoadStats stats,
        int readWeight,
        int writeWeight,
        int seedCount)
    {
        var knownIds = new ConcurrentDictionary<string, string>();

        foreach (var id in Enumerable.Range(1, seedCount))
        {
            knownIds.TryAdd(id.ToString(), "seed");
        }

        var nextId = new AtomicCounter(10_000);
        var maxConcurrency = Math.Max(_targetCallsPerSecond * 2, 200);
        var semaphore = new SemaphoreSlim(maxConcurrency, maxConcurrency);
        var tasks = new List<Task>(_totalOperations);
        var overallStopwatch = Stopwatch.StartNew();
        var intervalMs = 1000.0 / _targetCallsPerSecond;

        for (var operationIndex = 0; operationIndex < _totalOperations; operationIndex++)
        {
            var targetTime = TimeSpan.FromMilliseconds(operationIndex * intervalMs);
            var elapsed = overallStopwatch.Elapsed;
            if (targetTime > elapsed)
            {
                await Task.Delay(targetTime - elapsed);
            }

            await semaphore.WaitAsync();

            var isRead = Random.Shared.Next(100) < readWeight;
            var capturedIndex = operationIndex;

            tasks.Add(Task.Run(async () =>
            {
                var operationStopwatch = Stopwatch.StartNew();
                try
                {
                    if (isRead)
                    {
                        await ExecuteReadOperation(container, knownIds, stats);
                    }
                    else
                    {
                        await ExecuteWriteOperation(container, knownIds, stats, nextId);
                    }

                    operationStopwatch.Stop();
                    stats.RecordLatency(operationStopwatch.Elapsed);
                }
                catch (CosmosException cosmosException) when (cosmosException.StatusCode == HttpStatusCode.NotFound)
                {
                    operationStopwatch.Stop();
                    stats.RecordLatency(operationStopwatch.Elapsed);
                    Interlocked.Increment(ref stats.NotFound);
                }
                catch (Exception exception)
                {
                    operationStopwatch.Stop();
                    stats.RecordLatency(operationStopwatch.Elapsed);
                    Interlocked.Increment(ref stats.Errors);
                    stats.ErrorMessages.Enqueue($"Op {capturedIndex}: {exception.GetType().Name}: {exception.Message}");
                }
                finally
                {
                    semaphore.Release();
                }
            }));
        }

        stats.SchedulingTime = overallStopwatch.Elapsed;

        await Task.WhenAll(tasks);

        overallStopwatch.Stop();
        stats.WallClockTime = overallStopwatch.Elapsed;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Read operations (with verification)
    // ═══════════════════════════════════════════════════════════════════════════

    private static async Task ExecuteReadOperation(
        InMemoryContainer container,
        ConcurrentDictionary<string, string> knownIds,
        LoadStats stats)
    {
        var readType = Random.Shared.Next(4);

        switch (readType)
        {
            case 0:
                await ReadAndVerifySingle(container, knownIds, stats);
                break;
            case 1:
                await ReadAndVerifyQuery(container, stats);
                break;
            case 2:
                await ReadAndVerifyQueryWithFilter(container, knownIds, stats);
                break;
            case 3:
                await ReadAndVerifyPointRead(container, knownIds, stats);
                break;
        }
    }

    private static async Task ReadAndVerifySingle(
        InMemoryContainer container,
        ConcurrentDictionary<string, string> knownIds,
        LoadStats stats)
    {
        var ids = knownIds.Keys.ToArray();
        if (ids.Length == 0)
        {
            Interlocked.Increment(ref stats.Reads);
            return;
        }

        var targetId = ids[Random.Shared.Next(ids.Length)];
        if (!knownIds.TryGetValue(targetId, out var partitionKey))
        {
            Interlocked.Increment(ref stats.Reads);
            return;
        }

        var response = await container.ReadItemAsync<LoadDocument>(targetId, new PartitionKey(partitionKey));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Id.Should().Be(targetId);
        response.Resource.PartitionKey.Should().Be(partitionKey);
        Interlocked.Increment(ref stats.Reads);
    }

    private static async Task ReadAndVerifyPointRead(
        InMemoryContainer container,
        ConcurrentDictionary<string, string> knownIds,
        LoadStats stats)
    {
        var ids = knownIds.Keys.ToArray();
        if (ids.Length == 0)
        {
            Interlocked.Increment(ref stats.Reads);
            return;
        }

        var targetId = ids[Random.Shared.Next(ids.Length)];
        if (!knownIds.TryGetValue(targetId, out var partitionKey))
        {
            Interlocked.Increment(ref stats.Reads);
            return;
        }

        var response = await container.ReadItemAsync<LoadDocument>(targetId, new PartitionKey(partitionKey));

        response.Resource.Should().NotBeNull();
        response.Resource.Data.Should().NotBeNullOrEmpty();
        Interlocked.Increment(ref stats.Reads);
    }

    private static async Task ReadAndVerifyQuery(
        InMemoryContainer container,
        LoadStats stats)
    {
        var iterator = container.GetItemQueryIterator<LoadDocument>(
            new QueryDefinition("SELECT TOP 10 * FROM c ORDER BY c.counter DESC"));

        var results = new List<LoadDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCountLessThanOrEqualTo(10);

        for (var resultIndex = 1; resultIndex < results.Count; resultIndex++)
        {
            results[resultIndex].Counter.Should().BeLessThanOrEqualTo(results[resultIndex - 1].Counter);
        }

        Interlocked.Increment(ref stats.Reads);
    }

    private static async Task ReadAndVerifyQueryWithFilter(
        InMemoryContainer container,
        ConcurrentDictionary<string, string> knownIds,
        LoadStats stats)
    {
        var ids = knownIds.Keys.ToArray();
        if (ids.Length == 0)
        {
            Interlocked.Increment(ref stats.Reads);
            return;
        }

        var targetId = ids[Random.Shared.Next(ids.Length)];
        var iterator = container.GetItemQueryIterator<LoadDocument>(
            new QueryDefinition("SELECT * FROM c WHERE c.id = @id")
                .WithParameter("@id", targetId));

        var results = new List<LoadDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        if (results.Count == 1)
        {
            results[0].Id.Should().Be(targetId);
        }

        Interlocked.Increment(ref stats.Reads);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Write operations (mixed)
    // ═══════════════════════════════════════════════════════════════════════════

    private static async Task ExecuteWriteOperation(
        InMemoryContainer container,
        ConcurrentDictionary<string, string> knownIds,
        LoadStats stats,
        AtomicCounter nextId)
    {
        var writeType = Random.Shared.Next(5);

        switch (writeType)
        {
            case 0:
                await WriteCreate(container, knownIds, stats, nextId);
                break;
            case 1:
                await WriteUpsert(container, knownIds, stats, nextId);
                break;
            case 2:
                await WriteReplace(container, knownIds, stats);
                break;
            case 3:
                await WritePatch(container, knownIds, stats);
                break;
            case 4:
                await WriteDelete(container, knownIds, stats);
                break;
        }
    }

    private static async Task WriteCreate(
        InMemoryContainer container,
        ConcurrentDictionary<string, string> knownIds,
        LoadStats stats,
        AtomicCounter nextId)
    {
        var id = nextId.Increment().ToString();
        var partitionKey = $"pk-{id}";
        var document = CreateDocument(id, partitionKey);

        var response = await container.CreateItemAsync(document, new PartitionKey(partitionKey));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        knownIds.TryAdd(id, partitionKey);
        Interlocked.Increment(ref stats.Creates);
    }

    private static async Task WriteUpsert(
        InMemoryContainer container,
        ConcurrentDictionary<string, string> knownIds,
        LoadStats stats,
        AtomicCounter nextId)
    {
        var updateExisting = Random.Shared.Next(2) == 0;

        if (updateExisting)
        {
            var ids = knownIds.Keys.ToArray();
            if (ids.Length > 0)
            {
                var targetId = ids[Random.Shared.Next(ids.Length)];
                if (!knownIds.TryGetValue(targetId, out var partitionKey))
                {
                    Interlocked.Increment(ref stats.Upserts);
                    return;
                }

                var document = CreateDocument(targetId, partitionKey);

                var response = await container.UpsertItemAsync(document, new PartitionKey(partitionKey));

                response.StatusCode.Should().Be(HttpStatusCode.OK);
                Interlocked.Increment(ref stats.Upserts);
                return;
            }
        }

        var id = nextId.Increment().ToString();
        var newPartitionKey = $"pk-{id}";
        var newDocument = CreateDocument(id, newPartitionKey);

        var createResponse = await container.UpsertItemAsync(newDocument, new PartitionKey(newPartitionKey));

        createResponse.StatusCode.Should().Be(HttpStatusCode.Created);
        knownIds.TryAdd(id, newPartitionKey);
        Interlocked.Increment(ref stats.Upserts);
    }

    private static async Task WriteReplace(
        InMemoryContainer container,
        ConcurrentDictionary<string, string> knownIds,
        LoadStats stats)
    {
        var ids = knownIds.Keys.ToArray();
        if (ids.Length == 0)
        {
            Interlocked.Increment(ref stats.Replaces);
            return;
        }

        var targetId = ids[Random.Shared.Next(ids.Length)];
        if (!knownIds.TryGetValue(targetId, out var partitionKey))
        {
            Interlocked.Increment(ref stats.Replaces);
            return;
        }

        var document = CreateDocument(targetId, partitionKey);

        var response = await container.ReplaceItemAsync(document, targetId, new PartitionKey(partitionKey));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        Interlocked.Increment(ref stats.Replaces);
    }

    private static async Task WritePatch(
        InMemoryContainer container,
        ConcurrentDictionary<string, string> knownIds,
        LoadStats stats)
    {
        var ids = knownIds.Keys.ToArray();
        if (ids.Length == 0)
        {
            Interlocked.Increment(ref stats.Patches);
            return;
        }

        var targetId = ids[Random.Shared.Next(ids.Length)];
        if (!knownIds.TryGetValue(targetId, out var patchPartitionKey))
        {
            Interlocked.Increment(ref stats.Patches);
            return;
        }

        var patchOperations = new List<PatchOperation>
        {
            PatchOperation.Increment("/counter", 1),
            PatchOperation.Set("/data", $"patched-{Guid.NewGuid():N}")
        };

        var response = await container.PatchItemAsync<LoadDocument>(
            targetId, new PartitionKey(patchPartitionKey), patchOperations);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        Interlocked.Increment(ref stats.Patches);
    }

    private static async Task WriteDelete(
        InMemoryContainer container,
        ConcurrentDictionary<string, string> knownIds,
        LoadStats stats)
    {
        var ids = knownIds.Keys.ToArray();
        if (ids.Length == 0)
        {
            Interlocked.Increment(ref stats.Deletes);
            return;
        }

        var targetId = ids[Random.Shared.Next(ids.Length)];
        if (!knownIds.TryRemove(targetId, out var partitionKey))
        {
            Interlocked.Increment(ref stats.Deletes);
            return;
        }

        var response = await container.DeleteItemAsync<LoadDocument>(
            targetId, new PartitionKey(partitionKey));

        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
        Interlocked.Increment(ref stats.Deletes);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Seeding
    // ═══════════════════════════════════════════════════════════════════════════

    private static async Task SeedDocuments(InMemoryContainer container, int count)
    {
        for (var seedIndex = 1; seedIndex <= count; seedIndex++)
        {
            var id = seedIndex.ToString();
            var document = CreateDocument(id, "seed");
            await container.CreateItemAsync(document, new PartitionKey("seed"));
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Helpers
    // ═══════════════════════════════════════════════════════════════════════════

    private static LoadDocument CreateDocument(string id, string partitionKey) => new()
    {
        Id = id,
        PartitionKey = partitionKey,
        Counter = Random.Shared.Next(1000),
        Data = $"data-{Guid.NewGuid():N}",
        Timestamp = DateTimeOffset.UtcNow.ToString("O")
    };

    private void ReportAndAssert(LoadStats stats, string scenarioName)
    {
        var totalOps = stats.Reads + stats.Creates + stats.Upserts
                       + stats.Replaces + stats.Patches + stats.Deletes
                       + stats.NotFound + stats.Errors;

        var opsPerSecond = totalOps / stats.SchedulingTime.TotalSeconds;

        output.WriteLine($"║  Config: {_targetCallsPerSecond} rps × {_durationSeconds}s = {_totalOperations:N0} ops");
        output.WriteLine($"╔══════════════════════════════════════════════════╗");
        output.WriteLine($"║  {scenarioName,-46}  ║");
        output.WriteLine($"╠══════════════════════════════════════════════════╣");
        output.WriteLine($"║  Scheduling time   : {stats.SchedulingTime.TotalSeconds,8:F1}s{"",-17}║");
        output.WriteLine($"║  Wall clock        : {stats.WallClockTime.TotalSeconds,8:F1}s{"",-17}║");
        output.WriteLine($"║  Total operations  : {totalOps,8:N0}{"",-18}║");
        output.WriteLine($"║  Throughput        : {opsPerSecond,8:F1} ops/s{"",-12}║");
        output.WriteLine($"╠══════════════════════════════════════════════════╣");
        output.WriteLine($"║  Reads             : {stats.Reads,8:N0}{"",-18}║");
        output.WriteLine($"║  Creates           : {stats.Creates,8:N0}{"",-18}║");
        output.WriteLine($"║  Upserts           : {stats.Upserts,8:N0}{"",-18}║");
        output.WriteLine($"║  Replaces          : {stats.Replaces,8:N0}{"",-18}║");
        output.WriteLine($"║  Patches           : {stats.Patches,8:N0}{"",-18}║");
        output.WriteLine($"║  Deletes           : {stats.Deletes,8:N0}{"",-18}║");
        output.WriteLine($"╠══════════════════════════════════════════════════╣");
        output.WriteLine($"║  Expected 404s     : {stats.NotFound,8:N0}{"",-18}║");
        output.WriteLine($"║  Unexpected errors : {stats.Errors,8:N0}{"",-18}║");
        output.WriteLine($"╠══════════════════════════════════════════════════╣");
        output.WriteLine($"║  Mean latency      : {stats.MeanLatency,8:F3}ms{"",-15}║");
        output.WriteLine($"║  P50 latency       : {stats.GetPercentile(50),8:F3}ms{"",-15}║");
        output.WriteLine($"║  P95 latency       : {stats.GetPercentile(95),8:F3}ms{"",-15}║");
        output.WriteLine($"║  P99 latency       : {stats.GetPercentile(99),8:F3}ms{"",-15}║");
        output.WriteLine($"║  Max latency       : {stats.GetPercentile(100),8:F3}ms{"",-15}║");
        output.WriteLine($"╚══════════════════════════════════════════════════╝");

        foreach (var error in stats.ErrorMessages.Take(10))
        {
            output.WriteLine($"  ERROR: {error}");
        }

        totalOps.Should().Be(_totalOperations,
            $"all {_totalOperations:N0} operations should complete");

        stats.Errors.Should().Be(0,
            "no unexpected errors should occur under sustained load");

        opsPerSecond.Should().BeGreaterThanOrEqualTo(_targetCallsPerSecond * 0.95,
            $"throughput should sustain at least 95% of target ({_targetCallsPerSecond} ops/s)");

        stats.GetPercentile(99).Should().BeLessThan(200,
            "P99 latency should stay under 200ms for in-memory operations");
    }
}

public class LoadDocument
{
    [JsonProperty("id")]
    public string Id { get; set; } = default!;

    [JsonProperty("partitionKey")]
    public string PartitionKey { get; set; } = default!;

    [JsonProperty("counter")]
    public int Counter { get; set; }

    [JsonProperty("data")]
    public string Data { get; set; } = default!;

    [JsonProperty("timestamp")]
    public string Timestamp { get; set; } = default!;
}

public class LoadStats
{
    public long Reads;
    public long Creates;
    public long Upserts;
    public long Replaces;
    public long Patches;
    public long Deletes;
    public long NotFound;
    public long Errors;
    public TimeSpan SchedulingTime;
    public TimeSpan WallClockTime;

    public ConcurrentQueue<string> ErrorMessages { get; } = new();

    private readonly ConcurrentBag<double> _latenciesMs = [];

    public void RecordLatency(TimeSpan latency) => _latenciesMs.Add(latency.TotalMilliseconds);

    public double MeanLatency => _latenciesMs.Count > 0 ? _latenciesMs.Average() : 0;

    public double GetPercentile(int percentile)
    {
        var sorted = _latenciesMs.OrderBy(latency => latency).ToArray();
        if (sorted.Length == 0)
        {
            return 0;
        }

        if (percentile >= 100)
        {
            return sorted[^1];
        }

        var index = (int)Math.Ceiling(percentile / 100.0 * sorted.Length) - 1;
        return sorted[Math.Max(0, index)];
    }
}

public class AtomicCounter(int initial)
{
    private int _value = initial;
    public int Increment() => Interlocked.Increment(ref _value);
}
