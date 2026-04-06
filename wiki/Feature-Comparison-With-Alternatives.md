## vs Official Cosmos DB Emulators

Microsoft provides three official emulators, each with a significantly different architecture:

| | [Windows Emulator](https://learn.microsoft.com/en-us/azure/cosmos-db/emulator) (GA) | [Linux Docker](https://learn.microsoft.com/en-us/azure/cosmos-db/how-to-develop-emulator?tabs=docker-linux) (`:latest`) | [Linux vNext](https://learn.microsoft.com/en-us/azure/cosmos-db/emulator-linux) (`:vnext-preview`) |
|---|---|---|---|
| **Architecture** | Standalone Windows desktop application (MSI installer). Runs as a Windows service. | Docker image — the Windows emulator ported to a Linux container via SQLPAL compatibility layer. Essentially Windows binaries running on Linux. | Docker image — a ground-up rewrite. Lightweight gate-level simulation backed by PostgreSQL internally. |
| **Image / install size** | ~2 GB install + 10 GB disk | ~2 GB Docker image | ~200 MB Docker image |
| **Typical startup time** | 10–30 s | 30–60 s (can take minutes in CI) | 3–5 s |
| **Connection mode** | Gateway + Direct | Gateway + Direct | Gateway only |
| **Supported APIs** | NoSQL, MongoDB, Cassandra, Gremlin, Table | NoSQL, MongoDB, Cassandra, Gremlin, Table | NoSQL only |
| **Platform** | Windows only (64-bit Server 2016+, Win 10/11) | Linux / macOS / Windows (Docker) — but no Apple Silicon or ARM | Linux / macOS / Windows (Docker) — **ARM64 supported** |
| **Status** | GA (but lagging behind the cloud service) | Effectively abandoned — still GA in name, but [critically broken](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/86) for many users | Preview — active development, frequent releases |

### Setup & Operations

| Feature | CosmosDB.InMemoryEmulator | Windows Emulator (GA) | Linux Docker (`:latest`) | Linux vNext (preview) | Real Azure Cosmos DB |
|---------|:---:|:---:|:---:|:---:|:---:|
| Pricing | ✅ Free | ✅ Free | ✅ Free | ✅ Free | ❌ Pay-per-RU + storage |
| In-process (no external deps) | ✅ | ❌ Installer | ❌ Docker | ❌ Docker | ❌ Requires network |
| Fast startup | ✅ Instant | ❌ 10–30 s | ❌ 30–60 s | ⚠️ 3–5 s | ❌ Provisioning minutes |
| Works offline | ✅ | ✅ | ✅ | ✅ | ❌ Requires internet |
| CI-friendly | ✅ | ⚠️ Flaky (Windows runners) | ❌ Frequently broken in CI ([#86](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/86)) | ⚠️ Docker setup required; regressions between releases | ⚠️ Needs secrets, network, costs |
| Multiple instances / parallel | ✅ One per test | ❌ Single instance only | ⚠️ Multiple containers possible | ⚠️ Multiple containers possible | ⚠️ Shared state; needs cleanup |
| Test isolation | ✅ New instance per test | ⚠️ Shared state | ⚠️ Shared state | ⚠️ Shared state | ❌ Shared; risk of data leakage |
| Stable under load | ✅ No sockets/network | ⚠️ Socket exhaustion, 407s, hangs | ❌ Crashes, 429s, 503s under non-trivial load | ⚠️ CPU spikes after prolonged use ([#267](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/267)) | ✅ (at cost) |
| No crashes / corruption | ✅ Pure in-memory | ⚠️ Can spike CPU, enter corrupted state | ❌ PAL PANIC core dumps, fatal errors, evaluation expiry crashes | ⚠️ Regressions break features between releases ([patch](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/246), [batch](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/247), [aggregates](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/274)) | ✅ |
| Platform support | ✅ Any (.NET) | ❌ Windows only | ⚠️ Linux / macOS / Windows; no ARM | ✅ Linux / macOS / Windows; ARM64 supported | ✅ Any platform |
| No special reset needed | ✅ Dispose and recreate | ⚠️ May need "Reset Data" or `/DisableRIO` | ⚠️ Sometimes needs container replacement | ✅ Recreate container | ⚠️ Manual cleanup |
| Custom auth keys | ✅ Any / none | ⚠️ Well-known key; restart to change | ⚠️ Well-known key | ⚠️ Well-known key (configurable via `--key-file`) | ✅ |
| Serverless throughput mode | ✅ (no RU enforcement) | ❌ Provisioned only | ❌ Provisioned only | ❌ Provisioned only | ✅ |
| Unlimited containers | ✅ | ⚠️ Degrades past 10 fixed / 5 unlimited | ⚠️ Same limits as Windows | ✅ | ✅ |
| Connection mode | ✅ Gateway (via FakeCosmosHandler) | ✅ Gateway + Direct | ✅ Gateway + Direct | ⚠️ Gateway only | ✅ Gateway + Direct |
| Evaluation period / expiry | ✅ None | ✅ None | ❌ Periodic expiry; must pull latest image | ✅ None | ✅ None |
| SSL certificate setup | ✅ None needed | ⚠️ Self-signed; auto-installed | ⚠️ Manual cert export + install | ⚠️ Manual cert or use `--protocol http` | ✅ Managed |
| Community sentiment | ✅ | ⚠️ Grudging acceptance on Windows | ❌ Widely despised — "broken", "souring our perception of Cosmos" ([dozens of complaints](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/86)) | ⚠️ Promising but immature — "[frequent regressions](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues?q=is%3Aissue+label%3Abug)" in preview | ✅ Trusted (at cost) |

### CRUD & Data Operations

| Feature | CosmosDB.InMemoryEmulator | Windows Emulator | Linux Docker | Linux vNext | Real Azure Cosmos DB |
|---------|:---:|:---:|:---:|:---:|:---:|
| CreateItem / ReadItem / UpsertItem / ReplaceItem / DeleteItem | ✅ | ✅ | ✅ | ✅ | ✅ |
| Stream API (all CRUD variants) | ✅ | ✅ | ✅ | ✅ | ✅ |
| ReadMany | ✅ | ✅ | ✅ | ✅ | ✅ |
| Patch operations (Set/Add/Replace/Remove/Increment) | ✅ | ✅ | ✅ | ⚠️ Has had regressions ([#246](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/246)) | ✅ |
| Conditional patching (filter predicate) | ✅ | ✅ | ✅ | ⚠️ Precondition handling had bugs ([#242](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/242)) | ✅ |
| Transactional batches (atomic, rollback) | ✅ | ✅ | ✅ | ⚠️ Batch had 403 / missing-id regressions ([#243](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/243), [#247](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/247)) | ✅ |
| Bulk operations | ✅ | ✅ | ✅ | ❌ (.NET SDK limitation) | ✅ |
| ETag concurrency (IfMatch / IfNoneMatch) | ✅ | ✅ | ✅ | ✅ | ✅ |
| TTL / expiration (container + per-item) | ✅ (lazy eviction) | ✅ | ✅ | ⚠️ TTL removal was incomplete ([#239](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/239)) | ✅ |
| Document size limit (2 MB) | ✅ Enforced | ✅ | ✅ | ✅ | ✅ |
| System properties (`_ts`, `_etag`) | ✅ | ✅ Full | ✅ Full | ✅ Full | ✅ Full (`_rid`, `_self`, `_attachments` too) |
| Delete all items by partition key | ✅ | ✅ | ✅ | ✅ | ✅ |
| Unique key policy enforcement | ✅ | ✅ | ✅ | ✅ | ✅ |
| JSON property order preservation | ✅ | ✅ | ✅ | ❌ Not preserved ([#268](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/268), [#276](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/276)) | ✅ |

### Query Language

| Feature | CosmosDB.InMemoryEmulator | Windows Emulator | Linux Docker | Linux vNext | Real Azure Cosmos DB |
|---------|:---:|:---:|:---:|:---:|:---:|
| SQL query parser | ✅ 120+ functions | ✅ Full | ✅ Full | ⚠️ Most functions; some gaps ([#236](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/236)) | ✅ Full |
| `SELECT` / `FROM` / `WHERE` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `ORDER BY` (single & multi-field) | ✅ | ✅ | ✅ | ✅ | ✅ |
| `GROUP BY` / `HAVING` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `DISTINCT` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `TOP` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `OFFSET ... LIMIT` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `JOIN` (array expansion) | ✅ Unlimited | ⚠️ Max 5 per query | ⚠️ Max 5 per query | ✅ | ✅ |
| Subqueries / `EXISTS` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `IN` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `BETWEEN` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `LIKE` | ✅ | ✅ | ✅ | ✅ | ✅ |
| Parameterised queries | ✅ | ✅ | ✅ | ✅ | ✅ |
| `VALUE` keyword (scalar projection) | ✅ | ✅ | ✅ | ⚠️ SELECT VALUE had issues ([#258](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/258), [#270](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/270)) | ✅ |
| Aggregate functions (COUNT, SUM, AVG, MIN, MAX) | ✅ | ✅ | ✅ | ⚠️ Had regressions ([#273](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/273), [#274](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/274)) | ✅ |
| String functions (26 incl. REGEXMATCH, StringTo*) | ✅ | ✅ | ✅ | ✅ | ✅ |
| Math functions (30 incl. Int*, NumberBin, trig) | ✅ | ✅ | ✅ | ✅ | ✅ |
| Type-checking functions (10 incl. IS_INTEGER) | ✅ | ✅ | ✅ | ✅ | ✅ |
| Array functions (10 incl. SetIntersect, SetUnion) | ✅ | ✅ | ✅ | ✅ | ✅ |
| Date/time functions (15 incl. DateTimeBin) | ✅ | ✅ | ✅ | ✅ | ✅ |
| Spatial functions (ST_DISTANCE, ST_WITHIN, etc.) | ✅ 6 functions | ✅ | ✅ | ✅ | ✅ |
| Conditional functions (IIF, COALESCE) | ✅ | ✅ | ✅ | ✅ | ✅ |
| Non-ASCII characters in queries | ✅ | ✅ | ✅ | ❌ Returns 500 ([#263](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/263)) | ✅ |
| SQL comments (`--`, `/* */`) | ✅ | ✅ | ✅ | ❌ Can cause errors ([#272](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/272)) | ✅ |
| Cross-partition queries | ✅ | ✅ | ✅ | ❌ Not yet implemented | ✅ |
| Computed properties | ✅ | ✅ | ✅ | ❌ | ✅ |
| Custom index policy | ⚠️ Stored, not enforced | ✅ | ✅ | ❌ Not yet implemented ([#233](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/233)) | ✅ |
| Query pagination (MaxItemCount, continuation) | ✅ | ✅ | ✅ | ⚠️ Continuation token issues reported ([#259](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/259)) | ✅ |
| Large result sets | ✅ | ✅ | ✅ | ⚠️ HTTP 500 on large results ([#269](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/269)) | ✅ |

### Advanced Features

| Feature | CosmosDB.InMemoryEmulator | Windows Emulator | Linux Docker | Linux vNext | Real Azure Cosmos DB |
|---------|:---:|:---:|:---:|:---:|:---:|
| Change feed (Incremental / Latest Version) | ✅ | ✅ | ✅ | ✅ | ✅ |
| Change feed (AllVersionsAndDeletes via checkpoint) | ✅ | ✅ | ✅ | ✅ | ✅ |
| Change feed processor (polling) | ✅ | ✅ | ✅ | ✅ | ✅ |
| Change feed manual checkpoint processor | ✅ | ✅ | ✅ | ✅ | ✅ |
| Change feed stream iterator | ✅ | ✅ | ✅ | ✅ | ✅ |
| Delete tombstones in change feed | ✅ | ✅ | ✅ | ✅ | ✅ |
| Stored procedures | ✅ (C# handlers) | ✅ (JavaScript) | ✅ (JavaScript) | ❌ Not planned | ✅ (JavaScript) |
| User-defined functions (UDFs) | ✅ (C# handlers) | ✅ (JavaScript) | ✅ (JavaScript) | ❌ Not planned | ✅ (JavaScript) |
| Triggers (pre/post) | ✅ (C# + optional JS via Jint) | ✅ (JavaScript) | ✅ (JavaScript) | ❌ Not planned | ✅ (JavaScript) |
| Full-text search (FULLTEXTCONTAINS, FULLTEXTSCORE) | ✅ (approximate, no BM25) | ✅ | ✅ | ✅ | ✅ (BM25) |
| Vector search (VECTORDISTANCE) | ✅ Brute-force (cosine, dot, euclidean) | ✅ | ✅ | ✅ | ✅ (ANN indexing) |
| FeedRange scoping (queries) | ✅ (MurmurHash3) | ✅ | ✅ | ✅ | ✅ (automatic) |
| FeedRange scoping (change feed) | ✅ (MurmurHash3) | ✅ | ✅ | ✅ | ✅ |
| LINQ integration | ✅ | ✅ | ✅ | ✅ | ✅ |
| `.ToFeedIterator()` (via FakeCosmosHandler) | ✅ | ✅ | ✅ | ✅ | ✅ |
| Composite / hierarchical partition keys | ✅ | ✅ | ✅ | ⚠️ Null values rejected ([#279](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/279)) | ✅ |
| Multi-container routing (CreateRouter) | ✅ | ✅ | ✅ | ✅ | ✅ |
| Fault injection (custom HTTP responses) | ✅ | ❌ | ❌ | ❌ | ❌ |
| State export/import (JSON) | ✅ | ❌ | ❌ | ❌ | ❌ |
| Point-in-time restore | ✅ (change feed replay) | ❌ | ❌ | ❌ | ✅ (continuous backup) |
| Users & permissions (CRUD) | ✅ (stub, no auth enforced) | ✅ | ✅ | ❌ Returns 400 ([#275](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/275)) | ✅ |
| DI integration (UseInMemoryCosmosDB) | ✅ | ❌ | ❌ | ❌ | ❌ |
| Custom handler wrapping (DelegatingHandler pipeline) | ✅ | ❌ | ❌ | ❌ | ❌ |
| Request charge / diagnostics | ⚠️ Synthetic (1.0 RU) | ✅ | ✅ | ❌ Not yet implemented | ✅ |
| All consistency levels | ⚠️ Simulated (implicit strong) | ⚠️ Session & Strong only | ⚠️ Session & Strong only | ⚠️ Limited | ✅ |
| IndexingPolicy enforcement | ⚠️ Stored, not enforced | ✅ | ✅ | ❌ Not yet implemented | ✅ |
| Conflict resolution policy | ⚠️ Stored, not enforced | ✅ | ✅ | ❌ | ✅ |
| Client encryption (Always Encrypted) | ❌ | ✅ | ✅ | ❌ | ✅ |
| Analytical store (Synapse Link) | ❌ | ❌ | ❌ | ❌ | ✅ |
| Multi-region / geo-replication | ❌ | ❌ | ❌ | ❌ | ✅ |
| Resource tokens / RBAC enforcement | ❌ | ✅ | ✅ | ❌ | ✅ |
| OpenTelemetry integration | ❌ | ❌ | ❌ | ✅ | ✅ |
| Data explorer UI | ❌ | ✅ | ✅ | ✅ (port 1234) | ✅ (Azure Portal) |
| Data persistence across restarts | ❌ (in-memory) | ✅ | ⚠️ Volume mount required | ⚠️ Via `--data-path` mount | ✅ |
| Multiple database APIs (Mongo, Cassandra, Gremlin, Table) | ❌ NoSQL only | ✅ All five | ✅ All five | ❌ NoSQL only | ✅ All five |

## Performance

Measured on a standard GitHub Actions runner (2-core, free tier) at 400 ops/s sustained for 60 seconds across three workload profiles (72,000 total operations):

| Metric | Read-Heavy (80/20) | Even Mix (50/50) | Write-Heavy (20/80) |
|--------|:---:|:---:|:---:|
| **P50 latency** | 0.168 ms | 0.133 ms | 0.114 ms |
| **P95 latency** | 29.2 ms | 18.0 ms | 9.9 ms |
| **P99 latency** | 59.2 ms | 27.1 ms | 18.7 ms |
| **Errors** | 0 / 24,000 | 0 / 24,000 | 0 / 24,000 |

- **Startup time**: zero — no external process, no Docker, no network.
- **Tail latencies** on a 2-core runner are almost entirely GC pauses and thread pool scheduling, not emulator overhead. On a developer machine with more cores, expect tighter P95/P99.
- **Zero errors** across all 72,000 operations. The official emulator is known to produce spurious 503s, connection resets, and port conflicts under sustained load in CI.

### vs Real Azure Cosmos DB SLA

For reference, Microsoft's [SLA-backed latency guarantees](https://learn.microsoft.com/en-us/azure/cosmos-db/consistency-levels#consistency-levels-and-latency) for real Azure Cosmos DB are:

| Metric | Real Azure Cosmos DB (SLA) | In-Memory Emulator |
|--------|:---:|:---:|
| **P50 read** | ≤ 4 ms | 0.1–0.2 ms |
| **P50 write** | ≤ 5 ms | 0.1–0.2 ms |
| **P99 read** | < 10 ms | * |
| **P99 write** | < 10 ms | * |

\* Our P99 numbers (19–59 ms) are measured on a 2-core shared CI runner and reflect thread pool / GC pressure, not emulator overhead. P50 is the fairer comparison since it's less affected by noisy-neighbour CI infrastructure.

> Sources: [Consistency levels and latency](https://learn.microsoft.com/en-us/azure/cosmos-db/consistency-levels#consistency-levels-and-latency) — "Read latency for all consistency levels is guaranteed to be less than 10 milliseconds at the 99th percentile. Average read latency, at the 50th percentile, is typically 4 milliseconds or less." [Global distribution](https://learn.microsoft.com/en-us/azure/cosmos-db/distribute-data-globally#key-benefits-of-global-distribution) — "Guaranteed reads and writes served in less than 10 milliseconds at the 99th percentile."

<details>
<summary>Raw CI output (click to expand)</summary>

```
Read-Heavy (80/20)   — P50: 0.168ms, P95: 29.224ms, P99: 59.176ms, 0 errors / 24,000 ops
Even Mix (50/50)     — P50: 0.133ms, P95: 18.028ms, P99: 27.116ms, 0 errors / 24,000 ops
Write-Heavy (20/80)  — P50: 0.114ms, P95:  9.923ms, P99: 18.716ms, 0 errors / 24,000 ops
```

</details>

## vs Community Alternatives

| Feature | CosmosDB.InMemoryEmulator | [Cosmium](https://github.com/pikami/cosmium) | [FakeCosmosDb](https://github.com/timabell/FakeCosmosDb) | [FakeCosmosEasy](https://github.com/rentready/fake-cosmos-easy) |
|---------|:---:|:---:|:---:|:---:|
| Language | C# (NuGet) | Go (binary / Docker) | C# (NuGet) | C# (NuGet) |
| In-process (no external deps) | ✅ | ❌ Separate process | ✅ | ✅ |
| **CRUD Operations** | | | | |
| Create / Read / Upsert / Replace / Delete | ✅ | ✅ | ✅ | ⚠️ Basic |
| Stream API variants | ✅ | ❌ | ❌ | ❌ |
| ReadMany | ✅ | ❌ | ❌ | ❌ |
| Patch operations | ✅ | ❌ | ❌ | ❌ |
| Document size limits (2 MB) | ✅ | ❌ | ❌ | ❌ |
| **Queries** | | | | |
| SQL query parser | ✅ 120+ functions | ⚠️ ~60 functions | ⚠️ Basic | ⚠️ Basic |
| `GROUP BY` / `HAVING` | ✅ | ✅ | ❌ | ❌ |
| `DISTINCT` / `TOP` / `OFFSET LIMIT` | ✅ | ✅ | ❌ | ❌ |
| Subqueries / `EXISTS` | ✅ | ✅ | ❌ | ❌ |
| `JOIN` (array expansion) | ✅ Unlimited | ✅ | ❌ | ❌ |
| `BETWEEN` / `LIKE` / `IN` | ✅ | ⚠️ IN only (no BETWEEN/LIKE) | ❌ | ❌ |
| Parameterised queries | ✅ | ✅ | ❌ | ✅ |
| Aggregate functions (COUNT, SUM, AVG, MIN, MAX) | ✅ | ✅ | ❌ | ❌ |
| String functions (26) | ✅ | ⚠️ 19 (no REGEXMATCH, StringTo*) | ⚠️ Basic (CONTAINS, STARTSWITH) | ❌ |
| Math functions (30 incl. Int*, trig) | ✅ | ✅ ~30 | ❌ | ❌ |
| Type-checking functions (10) | ✅ | ✅ | ❌ | ❌ |
| Array functions (10 incl. Set*) | ✅ | ⚠️ 6 (no ObjectToArray, Choose) | ❌ | ❌ |
| Date/time functions (15) | ✅ | ❌ | ❌ | ❌ |
| Spatial functions (6 ST_*) | ✅ | ❌ | ❌ | ❌ |
| Conditional functions (IIF, COALESCE) | ✅ | ✅ IIF only | ❌ | ❌ |
| Cross-partition queries | ✅ | ✅ | ❌ | ❌ |
| Query pagination (continuation tokens) | ✅ | ✅ | ✅ | ❌ |
| **Advanced Features** | | | | |
| Transactional batches | ✅ (atomic, rollback) | ⚠️ Non-atomic "bulk" | ❌ | ❌ |
| Change feed | ✅ (3 modes + processor) | ❌ | ❌ | ❌ |
| ETag concurrency | ✅ | ❌ | ❌ | ❌ |
| TTL / expiration | ✅ | ❌ | ❌ | ❌ |
| Stored procedures | ✅ (C# handlers) | ❌ | ❌ | ❌ |
| UDFs in queries | ✅ (C# handlers) | ❌ | ❌ | ❌ |
| Triggers (pre/post) | ✅ (C# + optional JS) | ❌ | ❌ | ❌ |
| Full-text search | ✅ (approximate) | ❌ | ❌ | ❌ |
| Vector search (VECTORDISTANCE) | ✅ | ❌ | ❌ | ❌ |
| FeedRange support | ✅ | ❌ | ❌ | ❌ |
| LINQ integration | ✅ | ❌ | ❌ | ❌ |
| Composite / hierarchical partition keys | ✅ | ❌ | ❌ | ❌ |
| Unique key policy enforcement | ✅ | ❌ | ❌ | ❌ |
| Fault injection | ✅ | ❌ | ❌ | ❌ |
| State export/import | ✅ | ✅ (JSON persist) | ❌ | ❌ |
| Point-in-time restore | ✅ | ❌ | ❌ | ❌ |
| Multi-container routing | ✅ | ✅ (multi-database) | ❌ | ❌ |
| DI integration | ✅ | ❌ | ❌ | ❌ |
| Custom handler wrapping (DelegatingHandler) | ✅ | ❌ | ❌ | ❌ |
| Users & permissions | ✅ (stub) | ❌ | ❌ | ❌ |
| Response metadata (RU, ETag, activity ID) | ✅ | ✅ | ❌ | ❌ |
| **Project Health** | | | | |
| Actively maintained | ✅ | ✅ | ✅ | ❌ (3 years stale) |
| License | MIT | MIT | MIT | GPL-3.0 |
