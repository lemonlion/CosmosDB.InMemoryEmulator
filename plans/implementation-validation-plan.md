# Implementation Validation Plan: In-Memory Emulator vs Real Cosmos DB Emulators

## Problem Statement

We have 7,500+ tests that validate the in-memory emulator's behavior, but no systematic way to confirm that this behavior **matches the real Cosmos DB emulator(s)**. Critically, there are **three distinct emulator implementations** that may each behave differently. We need a multi-target test infrastructure that can run the same tests against all backends and surface any divergences.

## The Three Emulator Implementations

There are three separate Cosmos DB emulator implementations, each with different internals, feature sets, and behavioral characteristics:

| Emulator | Image / Install | Engine | Query Plan Strategy | Feature Gaps |
|----------|----------------|--------|--------------------|----|
| **Windows Desktop** | MSI / `winget install Microsoft.Azure.CosmosDB.Emulator` | Original native engine (most mature) | SDK uses native `ServiceInterop.dll` for **local** query plan computation | Most complete; supports stored procs, triggers, UDFs |
| **Linux Docker (legacy)** | `mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest` | Windows engine adapted for Linux | SDK **falls back to gateway HTTP endpoint** for query plans (different code path!) | Same features as Windows, but SDK behavior differs |
| **Linux Docker (vnext-preview)** | `mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview` | **Complete rewrite** — entirely new codebase | Gateway only, HTTP by default (must opt into HTTPS) | ❌ No stored procedures, ❌ No triggers, ❌ No UDFs, ❌ No custom index policies, ❌ No Request Units |

### Why This Matters

These three implementations can produce **different results for the same operation**:

1. **Query plan differences (Windows vs Linux):** The .NET SDK uses `ServiceInterop.dll` on Windows to compute query plans locally. On Linux, it calls the emulator's gateway HTTP endpoint. These are **different code paths in the SDK itself**, not just in the emulator. A query that works on Windows might fail on Linux due to gateway query plan parsing differences.

2. **Engine differences (legacy vs vnext):** The vnext emulator is a ground-up rewrite. Features like stored procedures, triggers, and UDFs are explicitly "not planned." Even supported features (batch, change feed, queries) may have subtle behavioral differences from the legacy engine.

3. **Protocol differences (vnext):** The vnext emulator defaults to HTTP (not HTTPS), uses different ports (1234 for data explorer), and has different startup semantics. The .NET SDK requires HTTPS, so vnext must be started with `--protocol https`.

4. **Our in-memory implementation is a fourth implementation** — adding a 4th axis of potential divergence.

### Validation Matrix

The ideal validation confirms that our in-memory emulator matches ALL real emulators:

```
                    Windows     Linux Legacy    Linux vnext
                    Desktop     Docker           Docker (preview)
                    ─────────   ──────────────   ─────────────────
CRUD                ✅ test     ✅ test          ✅ test
Queries (SQL)       ✅ test     ✅ test          ✅ test
Queries (LINQ)      ✅ test     ✅ test          ✅ test
Patch               ✅ test     ✅ test          ✅ test
Batch               ✅ test     ✅ test          ✅ test
Change Feed         ✅ test     ✅ test          ✅ test
ETags               ✅ test     ✅ test          ✅ test
TTL                 ✅ test     ✅ test          ✅ test
Stored Procedures   ✅ test     ✅ test          ❌ not supported
Triggers            ✅ test     ✅ test          ❌ not supported
UDFs                ✅ test     ✅ test          ❌ not supported
Hierarchical PK     ✅ test     ✅ test          ✅ test
Feed Ranges         ✅ test     ✅ test          ⚠️ may differ
```

## Current State

### Test Patterns in Use Today

| Pattern | How Container is Obtained | SDK Fidelity | # of Test Files | Parity Validation? |
|---------|--------------------------|--------------|-----------------|-------------------|
| **Direct InMemoryContainer** | `new InMemoryContainer("name", "/pk")` | Lower (no HTTP, no SDK serialization, LINQ-to-Objects) | ~40 files | ❌ No — different code paths, not comparable |
| **FakeCosmosHandler** | `new FakeCosmosHandler(container)` → real `CosmosClient` | **Highest** (full HTTP pipeline, SDK serialization, LINQ→SQL) | ~15 files | ✅ Yes — same SDK pipeline as real emulator |
| **InMemoryCosmosClient** | `new InMemoryCosmosClient()` | Lower (same as direct InMemoryContainer) | ~2 files | ❌ No — bypasses SDK pipeline |
| **DI (UseInMemoryCosmosDB)** | `services.UseInMemoryCosmosDB(...)` | **Highest** (uses FakeCosmosHandler internally) | ~3 files | ✅ Yes — same SDK pipeline |

### Scope Decision: Only FakeCosmosHandler Tests Get Parity-Validated

The three in-memory integration approaches exercise **fundamentally different code paths**:

```
Direct InMemoryContainer:    Test → InMemoryContainer (LINQ-to-Objects, Newtonsoft)
InMemoryCosmosClient:        Test → InMemoryContainer (LINQ-to-Objects, Newtonsoft)
FakeCosmosHandler:           Test → CosmosClient → HTTP → FakeCosmosHandler → InMemoryContainer
Real Emulator:               Test → CosmosClient → HTTP → Real Emulator
                                    ^^^^^^^^^^^^^^^^^^^^
                                    Same SDK pipeline
```

Only `FakeCosmosHandler` shares the same SDK pipeline as a real emulator (HTTP request construction, SDK serialization, LINQ→SQL translation, query plan generation, partition key range discovery). Comparing direct `InMemoryContainer` tests against a real emulator would surface "differences" that are really just architectural differences (LINQ-to-Objects vs LINQ→SQL, Newtonsoft vs configured serializer), not bugs.

Therefore:
- **FakeCosmosHandler tests** → parity-validated against all emulator targets
- **Direct InMemoryContainer / InMemoryCosmosClient tests** → stay as-is (unit/component tests, not parity-validated)
- **DI tests** → parity-validated where they use `UseInMemoryCosmosDB()` (which uses FakeCosmosHandler internally)

### Key Observations

1. **FakeCosmosHandler tests are the primary validation surface** — they already use a real `CosmosClient`, so swapping `HttpClientFactory` to point at a real emulator vs the handler is the only change needed.
2. **Direct InMemoryContainer tests remain valuable** — they're fast unit tests that validate the in-memory implementation's internal logic. They just don't need emulator parity validation.
3. **Some tests use in-memory-only APIs** (e.g., `container.RegisterStoredProcedure(...)`, `container.DefaultTimeToLive = ...`) — these cannot run against a real emulator without different setup.
4. **Three files document known behavioral differences** (BehavioralDifferenceTests, DivergentBehaviorTests, SkippedBehaviorTests) — these must be tagged to skip/expect-failure on emulator runs.
5. **The project already uses the legacy Linux emulator** in `emulator-benchmark.yml` — but only for performance benchmarks, not behavioral validation.

### Emulator Availability

| Platform | Emulator | Endpoint | Status |
|----------|----------|----------|--------|
| **Windows** | Desktop Emulator (MSI) | `https://localhost:8081` | Developer opt-in, not in CI |
| **Linux (legacy)** | Docker: `azure-cosmos-emulator:latest` | `https://localhost:8081` | In `emulator-benchmark.yml` (manual trigger) |
| **Linux (vnext)** | Docker: `azure-cosmos-emulator:vnext-preview` | `https://localhost:8081` | Not yet configured |
| **CI (GitHub Actions)** | Docker service container | `https://localhost:8081` | Manual trigger only |

---

## Proposed Approach: Environment-Driven Multi-Target Test Switching

### Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                       Test Class                              │
│  [Fact] async Task CreateItem_ReturnsCreated()                │
│  {                                                            │
│      var container = _fixture.GetContainer("c", "/pk");       │
│      // ... test using Container API ...                      │
│  }                                                            │
└───────────────┬──────────────────────────────────────────────┘
                │
         ┌──────┴──────┐
         │ ITestFixture │  ← COSMOS_TEST_TARGET env var selects implementation
         └──────┬──────┘
                │
    ┌───────────┼───────────┬───────────────────┐
    ▼           ▼           ▼                   ▼
┌─────────┐ ┌──────────┐ ┌──────────────┐ ┌──────────────┐
│InMemory │ │ Windows  │ │ Linux Legacy │ │ Linux vnext  │
│ Fixture │ │ Emulator │ │   Emulator   │ │   Emulator   │
│         │ │ Fixture  │ │   Fixture    │ │   Fixture    │
│FakeCosmos│ │          │ │              │ │              │
│Handler  │ │localhost  │ │ localhost    │ │ localhost    │
│         │ │:8081     │ │ :8081        │ │ :8081        │
│         │ │(HTTPS)   │ │ (HTTPS)      │ │ (HTTPS)      │
└─────────┘ └──────────┘ └──────────────┘ └──────────────┘
                │               │                │
                ▼               ▼                ▼
           ServiceInterop   Gateway HTTP     Gateway HTTP
           (local query     (query plan      (query plan
            plan DLL)        fallback)        fallback)
```

### COSMOS_TEST_TARGET Values

| Value | Backend | Where it runs |
|-------|---------|---------------|
| `inmemory` (default) | InMemoryContainer + FakeCosmosHandler | Everywhere, fast |
| `emulator-windows` | Windows Desktop Emulator | Windows dev machines |
| `emulator-linux` | Docker legacy `azure-cosmos-emulator:latest` | Linux, CI, Docker Desktop |
| `emulator-vnext` | Docker vnext `azure-cosmos-emulator:vnext-preview` | Linux, CI, Docker Desktop |

### Core Infrastructure Components

#### 1. `ITestContainerFixture` Interface

```csharp
public interface ITestContainerFixture : IAsyncDisposable
{
    /// <summary>
    /// Returns a Container backed by InMemory or one of the real emulators.
    /// Creates the database/container on real emulators if needed.
    /// </summary>
    Task<Container> CreateContainerAsync(
        string containerName,
        string partitionKeyPath,
        Action<ContainerProperties>? configure = null);

    /// <summary>
    /// Returns a CosmosClient (real SDK client in all cases).
    /// </summary>
    CosmosClient Client { get; }

    /// <summary>
    /// The test target this fixture runs against.
    /// </summary>
    TestTarget Target { get; }

    /// <summary>
    /// True if running against ANY real emulator (Windows, Linux legacy, or vnext).
    /// </summary>
    bool IsEmulator { get; }

    /// <summary>
    /// True if running against the vnext emulator (which lacks stored procs, triggers, UDFs).
    /// </summary>
    bool IsVnext { get; }

    /// <summary>
    /// Cleans up containers created during the test.
    /// </summary>
    Task CleanupAsync();
}

public enum TestTarget
{
    InMemory,           // Default — FakeCosmosHandler + InMemoryContainer
    EmulatorWindows,    // Windows Desktop Emulator (native ServiceInterop.dll)
    EmulatorLinux,      // Linux Docker legacy (gateway HTTP query plan fallback)
    EmulatorVnext       // Linux Docker vnext-preview (complete rewrite, feature gaps)
}
```

#### 2. `InMemoryTestFixture` Implementation — Always uses FakeCosmosHandler

```csharp
public class InMemoryTestFixture : ITestContainerFixture
{
    public TestTarget Target => TestTarget.InMemory;
    public bool IsEmulator => false;
    public bool IsVnext => false;

    public Task<Container> CreateContainerAsync(string name, string pkPath, ...)
    {
        // Always uses FakeCosmosHandler for apples-to-apples comparison with real emulators.
        // Tests using direct InMemoryContainer stay as-is and are NOT parity-validated.
        var container = new InMemoryContainer(name, pkPath);
        var handler = new FakeCosmosHandler(container);
        var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                HttpClientFactory = () => new HttpClient(handler)
            });
        return Task.FromResult(client.GetContainer("db", name));
    }
}
```

#### 3. `EmulatorTestFixture` Implementation (handles all three emulator types)

```csharp
public class EmulatorTestFixture : ITestContainerFixture
{
    private const string Endpoint = "https://localhost:8081";
    private const string Key = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

    public TestTarget Target { get; }
    public bool IsEmulator => true;
    public bool IsVnext => Target == TestTarget.EmulatorVnext;

    public EmulatorTestFixture(TestTarget target)
    {
        Target = target;
        Client = new CosmosClient(Endpoint, Key, new CosmosClientOptions
        {
            // Gateway mode required for all emulators (Linux doesn't support Direct)
            ConnectionMode = ConnectionMode.Gateway,
            HttpClientFactory = () =>
            {
                var handler = new HttpClientHandler
                {
                    ServerCertificateCustomValidationCallback =
                        HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
                };
                return new HttpClient(handler);
            }
        });
    }

    public async Task<Container> CreateContainerAsync(string name, string pkPath, ...)
    {
        var db = await Client.CreateDatabaseIfNotExistsAsync("validation-db");
        // Unique container name per test to avoid cross-test pollution
        var uniqueName = $"{name}-{Guid.NewGuid():N}";
        var response = await db.Database.CreateContainerIfNotExistsAsync(uniqueName, pkPath);
        _containersToCleanup.Add(response.Container);
        return response.Container;
    }
}
```

#### 4. `TestFixtureFactory` — reads environment and creates the right fixture

```csharp
public static class TestFixtureFactory
{
    public static ITestContainerFixture Create()
    {
        var target = Environment.GetEnvironmentVariable("COSMOS_TEST_TARGET")?.ToLowerInvariant() switch
        {
            "emulator-windows" => TestTarget.EmulatorWindows,
            "emulator-linux"   => TestTarget.EmulatorLinux,
            "emulator-vnext"   => TestTarget.EmulatorVnext,
            _                  => TestTarget.InMemory
        };

        return target == TestTarget.InMemory
            ? new InMemoryTestFixture()
            : new EmulatorTestFixture(target);
    }
}
```

#### 5. Skip Attributes — granular per-target

```csharp
/// <summary>
/// Skips the test if running against the vnext emulator
/// (which doesn't support stored procs, triggers, UDFs, etc.)
/// </summary>
public class SkipOnVnextFactAttribute : FactAttribute
{
    public SkipOnVnextFactAttribute(string reason = "Not supported on vnext emulator")
    {
        var target = Environment.GetEnvironmentVariable("COSMOS_TEST_TARGET");
        if (target?.Equals("emulator-vnext", StringComparison.OrdinalIgnoreCase) == true)
            Skip = reason;
    }
}

/// <summary>
/// Skips the test if the target emulator is not available.
/// Uses a cached connectivity check.
/// </summary>
public class RequiresEmulatorFactAttribute : FactAttribute
{
    public override string? Skip =>
        EmulatorDetector.IsAvailable ? null : "Cosmos DB emulator not available";
}

public static class EmulatorDetector
{
    private static readonly Lazy<bool> _isAvailable = new(() =>
    {
        try
        {
            using var handler = new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = (_, _, _, _) => true
            };
            using var http = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(3) };
            var result = http.GetAsync("https://localhost:8081/").Result;
            return result.StatusCode is HttpStatusCode.OK or HttpStatusCode.Unauthorized;
        }
        catch { return false; }
    });

    public static bool IsAvailable => _isAvailable.Value;
}
```

#### 6. Test Traits for Categorization

```csharp
// Tests that ONLY make sense for in-memory (test internal APIs, behavioral differences)
[Trait("Target", "InMemoryOnly")]

// Tests that should run against ALL targets (default — no trait needed)
[Trait("Target", "All")]

// Tests that require features missing from vnext (stored procs, triggers, UDFs)
[Trait("Target", "ExcludeVnext")]

// Tests that document KNOWN divergences (skip on all emulators)
[Trait("Target", "KnownDivergence")]
```

---

## Implementation Phases

### Phase 1: Shared Test Infrastructure

Create the foundation that all validation tests will use.

**Deliverables:**
- `ITestContainerFixture` interface
- `InMemoryTestFixture` implementation
- `EmulatorTestFixture` implementation (with container cleanup)
- `EmulatorDetector` static helper (cached connectivity check)
- `RequiresEmulatorFactAttribute` for auto-skip
- Trait constants (`TestTargets.InMemoryOnly`, `TestTargets.Both`, etc.)
- A `TestFixtureFactory` static class that reads `COSMOS_TEST_TARGET` env var and returns the appropriate fixture

**Location:** `tests/CosmosDB.InMemoryEmulator.Tests/Infrastructure/`

### Phase 2: Pilot Migration — FakeCosmosHandler CRUD Tests

Migrate `FakeCosmosHandlerCrudTests` as the proof-of-concept. These are the lowest-friction tests to adapt because they already use a real `CosmosClient`.

**Current pattern:**
```csharp
public class FakeCosmosHandlerCrudTests : IDisposable
{
    private readonly InMemoryContainer _inMemoryContainer;
    private readonly FakeCosmosHandler _handler;
    private readonly CosmosClient _client;
    private readonly Container _container;

    public FakeCosmosHandlerCrudTests()
    {
        _inMemoryContainer = new InMemoryContainer("test-crud", "/partitionKey");
        _handler = new FakeCosmosHandler(_inMemoryContainer);
        _client = new CosmosClient(..., new CosmosClientOptions
        {
            HttpClientFactory = () => new HttpClient(_handler)
        });
        _container = _client.GetContainer("db", "test-crud");
    }
}
```

**New pattern:**
```csharp
public class FakeCosmosHandlerCrudTests : IAsyncLifetime
{
    private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create();
    private Container _container = null!;

    public async ValueTask InitializeAsync()
    {
        _container = await _fixture.CreateContainerAsync("test-crud", "/partitionKey");
    }

    public async ValueTask DisposeAsync() => await _fixture.CleanupAsync();

    [Fact]
    public async Task Handler_CreateItem_ReturnsCreated()
    {
        // Identical test body — works against both targets
        var doc = new TestDocument { Id = "c1", PartitionKey = "pk1", Name = "Alice", Value = 10 };
        var response = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }
}
```

**Validate:** Run the same 15+ CRUD tests against both in-memory and emulator; confirm identical results.

### Phase 3: FakeCosmosHandler Test Migration

Migrate FakeCosmosHandler test files to the fixture pattern. **Only these files** get parity-validated — direct `InMemoryContainer` tests (CrudTests, QueryTests, etc.) stay as-is.

| Priority | File | Test Count | Notes |
|----------|------|-----------|-------|
| 1 | FakeCosmosHandlerCrudTests.cs | ~20 | Already done in Phase 2 pilot |
| 2 | FakeCosmosHandlerQueryAdvancedTests.cs | ~30 | SQL query parity |
| 3 | FakeCosmosHandlerBatchTests.cs | ~15 | Transactional batch parity |
| 4 | FakeCosmosHandlerPartitionKeyTests.cs | ~15 | PK routing parity |
| 5 | FakeCosmosHandlerHierarchicalPkTests.cs | ~10 | Hierarchical PK parity |
| 6 | FakeCosmosHandlerBulkTests.cs | ~10 | Bulk execution parity |
| 7 | FakeCosmosHandlerChangeFeedTests.cs | ~15 | Change feed parity |
| 8 | FakeCosmosHandlerLinqTests.cs | ~20 | LINQ→SQL translation parity |
| 9 | FakeCosmosHandlerReadManyTests.cs | ~10 | ReadMany parity |
| 10 | FakeCosmosHandlerTtlTests.cs | ~10 | TTL parity |
| 11 | FakeCosmosHandlerTests.cs | ~20 | General handler tests |
| 12 | FakeCosmosHandlerCrudHardeningTests.cs | ~15 | Edge case CRUD parity |
| 13 | FakeCosmosHandlerAdvancedFeatureTests.cs | ~15 | Advanced feature parity |

**What stays untouched:**

Direct `InMemoryContainer` tests (~40 files like `CrudTests.cs`, `QueryTests.cs`, `PatchItemTests.cs`) remain as fast unit tests validating internal logic. They use different code paths (LINQ-to-Objects, no HTTP, Newtonsoft-only serialization) so emulator comparison would be apples-to-oranges.

### Phase 4: Tag Non-Parity Tests

Mark tests that cannot or should not run against real emulators:

| File | Trait | Reason |
|------|-------|--------|
| All direct InMemoryContainer tests (~40 files) | `InMemoryOnly` | Different code paths, not parity-comparable |
| BehavioralDifferenceTests.cs | `InMemoryOnly` | Tests intentional differences |
| DivergentBehaviorTests.cs | `InMemoryOnly` | Tests known divergences |
| SkippedBehaviorTests.cs | `All` (these SHOULD pass on emulator) | Actually validates emulator behavior |
| FaultInjectionTests.cs | `InMemoryOnly` | Fault injection is an in-memory-only feature |
| StatePersistenceTests.cs | `InMemoryOnly` | Tests in-memory state serialization |
| SdkCompatibilityTests.cs | `InMemoryOnly` | Tests reflection/SDK internals |
| ServiceCollectionExtensionTests.cs | `InMemoryOnly` | Tests DI wiring |
| WebApplicationFactoryIntegrationTests.cs | `InMemoryOnly` | Tests ASP.NET integration |
| StoredProcedureTests.cs | `ExcludeVnext` | vnext doesn't support stored procs |
| TriggerTests.cs / JsTriggerTests.cs | `ExcludeVnext` | vnext doesn't support triggers |

### Phase 5: CI/CD Pipelines — Multi-Emulator Validation

### Phase 5: Shared Scripts — Used by Both CI and Local

The key principle: **CI and local use the exact same scripts**, just called differently. CI calls them as individual workflow steps; locally, an orchestrator script chains them.

#### Script Inventory

```
scripts/
├── start-emulator.ps1          # Starts Docker emulator, waits for readiness
├── run-tests.ps1               # Runs test suite with a given COSMOS_TEST_TARGET
├── compare-trx.ps1             # Compares TRX files, outputs parity table
└── validate-parity.ps1         # Orchestrator: calls the above three in sequence
```

#### `scripts/start-emulator.ps1` — Emulator Lifecycle

```powershell
<#
.SYNOPSIS
    Starts the Cosmos DB emulator in Docker and waits for readiness.
.PARAMETER Image
    Docker image to use. Defaults to legacy emulator.
.PARAMETER Port
    Host port to map. Default 8081.
.PARAMETER ContainerName
    Docker container name. Default 'cosmosdb-emulator'.
.EXAMPLE
    .\scripts\start-emulator.ps1
    .\scripts\start-emulator.ps1 -Image mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview
#>
param(
    [string]$Image = 'mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest',
    [string]$ContainerName = 'cosmosdb-emulator',
    [int]$Port = 8081,
    [int]$TimeoutSeconds = 300
)

$ErrorActionPreference = 'Stop'

# Start container
docker run --detach --name $ContainerName `
    --publish "${Port}:8081" `
    --publish "10250-10256:10250-10256" `
    --env AZURE_COSMOS_EMULATOR_PARTITION_COUNT=3 `
    --env AZURE_COSMOS_EMULATOR_ENABLE_DATA_PERSISTENCE=false `
    --env AZURE_COSMOS_EMULATOR_IP_ADDRESS_OVERRIDE=127.0.0.1 `
    $Image

# Wait for readiness (same logic CI uses)
$elapsed = 0
while ($elapsed -lt $TimeoutSeconds) {
    try {
        $status = (Invoke-WebRequest -Uri "https://localhost:${Port}/" `
            -SkipCertificateCheck -TimeoutSec 3).StatusCode
        if ($status -in 200, 401) {
            Write-Host "Emulator ready after ${elapsed}s (HTTP $status)" -ForegroundColor Green
            Start-Sleep 15  # Extra buffer for internal initialization
            return
        }
    } catch {}
    Start-Sleep 5; $elapsed += 5
    Write-Host "Waiting for emulator... (${elapsed}s)"
}
Write-Error "Emulator did not start within ${TimeoutSeconds}s"
```

#### `scripts/run-tests.ps1` — Test Execution

```powershell
<#
.SYNOPSIS
    Runs the test suite against a specified target backend.
.PARAMETER Target
    Backend to test against: inmemory, emulator-linux
.PARAMETER Framework
    Target framework. Default net8.0.
.PARAMETER Filter
    Additional xUnit filter expression.
.PARAMETER OutputDir
    Directory for TRX output files. Default ./test-results
.EXAMPLE
    .\scripts\run-tests.ps1 -Target inmemory
    .\scripts\run-tests.ps1 -Target emulator-linux
    .\scripts\run-tests.ps1 -Target emulator-linux -Filter "FullyQualifiedName~Crud"
#>
param(
    [Parameter(Mandatory)]
    [ValidateSet('inmemory', 'emulator-linux')]
    [string]$Target,

    [string]$Framework = 'net8.0',
    [string]$Filter,
    [string]$OutputDir = './test-results'
)

$ErrorActionPreference = 'Stop'
$env:COSMOS_TEST_TARGET = $Target

# Build filter: always exclude InMemoryOnly tests when targeting emulator
$filterExpr = if ($Target -ne 'inmemory') { 'Target!=InMemoryOnly' } else { '' }
if ($Filter) {
    $filterExpr = if ($filterExpr) { "$filterExpr&$Filter" } else { $Filter }
}

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
$trxFile = "$Target-results.trx"

$testArgs = @(
    'test', 'tests/CosmosDB.InMemoryEmulator.Tests',
    '--configuration', 'Release',
    '--framework', $Framework,
    '--no-build',
    '--logger', "trx;LogFileName=$trxFile",
    '--results-directory', $OutputDir
)
if ($filterExpr) { $testArgs += '--filter'; $testArgs += $filterExpr }

dotnet @testArgs

Write-Host "Results: $OutputDir/$trxFile" -ForegroundColor Cyan
```

#### `scripts/compare-trx.ps1` — Parity Comparison

```powershell
<#
.SYNOPSIS
    Compares two or more TRX files and outputs a parity report.
.PARAMETER ResultsDir
    Directory containing TRX files. Default ./test-results
.PARAMETER OutputFormat
    Output format: 'console' (default) or 'markdown' (for GitHub Step Summary).
.EXAMPLE
    .\scripts\compare-trx.ps1
    .\scripts\compare-trx.ps1 -OutputFormat markdown >> $env:GITHUB_STEP_SUMMARY
#>
param(
    [string]$ResultsDir = './test-results',
    [ValidateSet('console', 'markdown')]
    [string]$OutputFormat = 'console'
)

# Parse all TRX files in the directory
# Build matrix: Test × Target → Pass/Fail/Skip
# Categorize:
#   ✅ Parity    — same result across all targets
#   ⚠️ Divergent — passes on some, fails on others
#   🔍 Suspect   — in-memory passes, emulator fails
# Output table to console or markdown
```

#### `scripts/validate-parity.ps1` — Local Orchestrator

```powershell
<#
.SYNOPSIS
    One-command parity validation: starts emulator, runs both test suites,
    compares results.
.PARAMETER Filter
    Optional xUnit filter to narrow the test scope.
.PARAMETER SkipBuild
    Skip the build step (if you've already built).
.EXAMPLE
    .\scripts\validate-parity.ps1
    .\scripts\validate-parity.ps1 -Filter "FullyQualifiedName~Crud"
#>
param(
    [string]$Filter,
    [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'
$scriptsDir = $PSScriptRoot

# Step 0: Build (once, shared by both runs)
if (-not $SkipBuild) {
    Write-Host "`n=== Building ===" -ForegroundColor Cyan
    dotnet build CosmosDB.InMemoryEmulator.sln --configuration Release
}

# Step 1: Run in-memory baseline
Write-Host "`n=== Running in-memory tests ===" -ForegroundColor Cyan
& "$scriptsDir/run-tests.ps1" -Target inmemory -Filter $Filter

# Step 2: Start emulator (if not already running)
$emulatorRunning = docker ps --filter name=cosmosdb-emulator --format '{{.Names}}' 2>$null
if (-not $emulatorRunning) {
    Write-Host "`n=== Starting emulator ===" -ForegroundColor Cyan
    & "$scriptsDir/start-emulator.ps1"
}

# Step 3: Run emulator tests
Write-Host "`n=== Running emulator tests ===" -ForegroundColor Cyan
& "$scriptsDir/run-tests.ps1" -Target emulator-linux -Filter $Filter

# Step 4: Compare
Write-Host "`n=== Parity Report ===" -ForegroundColor Cyan
& "$scriptsDir/compare-trx.ps1"

# Step 5: Cleanup
Write-Host "`nStopping emulator..." -ForegroundColor DarkGray
docker rm -f cosmosdb-emulator 2>$null
```

#### CI Workflow — Uses the Same Scripts

```yaml
name: Emulator Parity Validation
on:
  schedule:
    - cron: '0 6 * * 1'    # Weekly Monday 6am UTC
  workflow_dispatch:

jobs:
  parity-check:
    runs-on: ubuntu-latest
    services:
      cosmosdb:
        image: mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest
        ports:
          - 8081:8081
          - 10250-10256:10250-10256
        env:
          AZURE_COSMOS_EMULATOR_PARTITION_COUNT: 3
          AZURE_COSMOS_EMULATOR_ENABLE_DATA_PERSISTENCE: "false"
          AZURE_COSMOS_EMULATOR_IP_ADDRESS_OVERRIDE: "127.0.0.1"

    steps:
      - uses: actions/checkout@v5
      - uses: actions/setup-dotnet@v5
        with:
          dotnet-version: 8.0.x

      # Reuses the SAME wait logic from start-emulator.ps1
      - name: Wait for Emulator
        shell: pwsh
        run: |
          # Inline wait (emulator started by GH Actions service container)
          $elapsed = 0; $timeout = 300
          while ($elapsed -lt $timeout) {
            try {
              $s = (Invoke-WebRequest https://localhost:8081/ -SkipCertificateCheck -TimeoutSec 3).StatusCode
              if ($s -in 200,401) { Write-Host "Ready ($elapsed s)"; Start-Sleep 15; break }
            } catch {}
            Start-Sleep 5; $elapsed += 5
          }

      - name: Build
        run: dotnet build CosmosDB.InMemoryEmulator.sln --configuration Release

      # Same scripts as local
      - name: Run In-Memory Tests
        shell: pwsh
        run: ./scripts/run-tests.ps1 -Target inmemory

      - name: Run Emulator Tests
        shell: pwsh
        run: ./scripts/run-tests.ps1 -Target emulator-linux

      - name: Parity Report
        if: always()
        shell: pwsh
        run: ./scripts/compare-trx.ps1 -OutputFormat markdown >> $env:GITHUB_STEP_SUMMARY

      - name: Upload Results
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: parity-results
          path: test-results/
```

### Phase 6: Parity Reporting

The `compare-trx.ps1` script compares results across targets:

1. **Parses TRX files** from each test run (in-memory, emulator-linux)
2. **Builds a parity matrix** — each test × each target → Pass/Fail/Skip:

```
Test Name                        │ InMemory  │ Emulator  │ Status
─────────────────────────────────┼───────────┼───────────┼────────
CreateItem_ReturnsCreated        │  ✅ Pass  │  ✅ Pass  │ ✅ Parity
Query_OrderBy_Ascending          │  ✅ Pass  │  ❌ Fail  │ 🔍 Suspect
StoredProc_Execute               │  ✅ Pass  │  ⏭ Skip  │ ℹ️ Skipped
ChangeFeed_Incremental           │  ❌ Fail  │  ❌ Fail  │ ❌ Both fail
Handler_DuplicateId              │  ✅ Pass  │  ✅ Pass  │ ✅ Parity
```

3. **Categorizes discrepancies** with actionable labels:
   - **✅ Parity** — same result on both targets
   - **🔍 InMemory Suspect** — in-memory passes but emulator fails (likely bug in our implementation)
   - **⚠️ Emulator Gap** — emulator fails but in-memory passes (could be emulator limitation)
   - **📝 Known Gap** — expected divergence (documented in BehavioralDifferenceTests)

4. **Summary statistics:**
   - Total tests compared
   - Parity percentage
   - Number of discrepancies by category

---

## Handling Known Differences

### Strategy for Tests Expected to Diverge

For tests where behavior intentionally or unavoidably differs between in-memory and the emulator:

```csharp
public static class PlatformAssert
{
    /// <summary>
    /// Assert different expected values for in-memory vs emulator.
    /// Documents known divergences inline.
    /// </summary>
    public static void AssertForTarget<T>(
        ITestContainerFixture fixture,
        T actual,
        T expectedInMemory,
        T expectedEmulator,
        string divergenceReason)
    {
        var expected = fixture.IsEmulator ? expectedEmulator : expectedInMemory;
        actual.Should().Be(expected,
            because: fixture.IsEmulator
                ? "real emulator behavior"
                : $"in-memory divergence: {divergenceReason}");
    }
}
```

This keeps both behaviors visible in the same test, making divergences self-documenting and reviewable.

---

## Platform-Specific Considerations

### Windows Desktop Emulator
- **Install:** MSI or `winget install Microsoft.Azure.CosmosDB.Emulator`
- **Endpoint:** `https://localhost:8081` (default)
- **Certificate:** Self-signed, trusted by local cert store
- **SDK behavior:** Uses native `ServiceInterop.dll` for **local** query plan computation — this DLL is bundled with the SDK NuGet package and only works on Windows
- **Feature set:** Most complete — stored procs, triggers, UDFs, custom indexing all work
- **Limitation:** Windows only, no Docker, single instance

### Linux Docker Emulator (Legacy)
- **Image:** `mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest`
- **Endpoint:** `https://localhost:8081` (mapped from container)
- **Certificate:** Must disable SSL validation (`DangerousAcceptAnyServerCertificateValidator`)
- **SDK behavior:** Falls back to **gateway HTTP endpoint** for query plans — this is a DIFFERENT code path in the SDK itself
- **Feature set:** Same as Windows (in theory), but:
  - Query plan parsing differences due to gateway fallback
  - Partition handling may differ
  - Startup time: 60-120 seconds
- **Ports:** 8081 + 10250-10256

### Linux Docker Emulator (vnext-preview)
- **Image:** `mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview`
- **Endpoint:** `http://localhost:8081` by default (HTTP!), must use `--protocol https` for .NET SDK
- **Certificate:** Self-signed when HTTPS enabled
- **SDK behavior:** Gateway only (same as legacy Linux)
- **Feature set:** **Significantly reduced** — complete rewrite:
  - ❌ Stored procedures — "Not planned"
  - ❌ Triggers — "Not planned"
  - ❌ UDFs — "Not planned"
  - ❌ Custom index policies — "Not yet implemented"
  - ❌ Request Units — "Not yet implemented"
  - ❌ Collection feed — "Not yet implemented"
  - ✅ CRUD, queries, batch, bulk, change feed, patch, TTL
- **Ports:** 8081 (API) + 1234 (Data Explorer)
- **Startup:** Much faster than legacy (~5-10 seconds)

### Query Plan Divergence — The Hidden Difference

This is the most subtle cross-platform issue. The Cosmos SDK contains two query plan strategies:

```
Windows:  SDK → ServiceInterop.dll (native) → local query plan → execute
Linux:    SDK → HTTP POST /querySqlQuery → emulator computes plan → return → execute
```

These can produce different results because:
1. The native DLL and the gateway endpoint may parse SQL differently
2. The gateway endpoint may support different SQL features
3. Error handling differs (native throws exceptions, gateway returns HTTP errors)
4. The FakeCosmosHandler intercepts the gateway HTTP call on Linux but not the native DLL call on Windows

**This means a test passing on Windows emulator but failing on Linux emulator is NOT necessarily a bug in either emulator** — it could be a difference in the SDK's query planning strategy. Our in-memory emulator needs to handle BOTH code paths correctly.

---

## Test Isolation Strategy

Real emulator tests are slower and share a single endpoint. Key concerns:

1. **Container naming:** Each test class gets a unique container name (UUID suffix) to avoid cross-test data pollution
2. **Cleanup:** Containers are deleted in `DisposeAsync` to avoid bloating the emulator
3. **Parallelism:** Limit emulator test parallelism (xUnit `maxParallelThreads`) to avoid overwhelming the emulator
4. **Database:** Use a dedicated `validation-db` database, created once per test run
5. **Timeout:** Emulator operations are slower; increase test timeout to 30s per test

```csharp
// xunit.runner.json for emulator runs
{
  "maxParallelThreads": 4,       // Limit for emulator (vs unlimited for in-memory)
  "parallelizeAssembly": false,
  "parallelizeTestCollections": true
}
```

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| **Emulator startup time (60-120s)** | Slow local/CI feedback | Pre-warm in Docker service container; `start-emulator.ps1` reusable |
| **Emulator flakiness (429s, timeouts)** | False negatives in parity report | Retry policy in fixture; mark consistently flaky tests |
| **Container cleanup failures** | Stale data across runs | Use unique container names per test (UUID suffix); nuke DB in global setup |
| **SDK query plan differs (Win vs Linux)** | Queries may behave differently | Document in parity report; our primary target is Linux (CI runs there) |
| **FakeCosmosHandler test refactoring (~13 files)** | Merge conflicts | Feature branch per phase; pilot with one file first |
| **TRX comparison false positives** | Noise in parity report | Filter known divergences (BehavioralDifferenceTests); categorize clearly |

---

## Summary of Todos

1. **Create shared test infrastructure** — `ITestContainerFixture`, `TestTarget` enum, `InMemoryTestFixture` (always FakeCosmosHandler), `EmulatorTestFixture`, `EmulatorDetector`, `RequiresEmulatorFactAttribute`, `TestFixtureFactory` (reads `COSMOS_TEST_TARGET`)
2. **Pilot migration** — Adapt FakeCosmosHandlerCrudTests as proof-of-concept
3. **Migrate remaining FakeCosmosHandler test files** (~13 files, all low friction). Direct InMemoryContainer tests (~40 files) stay untouched.
4. **Tag tests with traits** — FakeCosmosHandler tests: `Target=All`. Direct InMemoryContainer tests: `Target=InMemoryOnly`.
5. **Create shared scripts** — `scripts/start-emulator.ps1`, `scripts/run-tests.ps1`, `scripts/compare-trx.ps1`, `scripts/validate-parity.ps1` (orchestrator). Used by both CI and local.
6. **Create CI workflow** — `emulator-parity.yml` using shared scripts. Legacy Linux emulator only. Runs in-memory + emulator suites, compares TRX files, outputs GitHub Step Summary.
7. **Create parity reporting** — `compare-trx.ps1` parses TRX files, categorizes as Parity / Suspect / Gap, outputs console table or markdown.
8. **Handle known divergences** — `PlatformAssert` helper for in-memory vs emulator expected values.
9. **Document the validation strategy** — Update README: `COSMOS_TEST_TARGET`, trait system, running `validate-parity.ps1` locally, interpreting parity reports.
