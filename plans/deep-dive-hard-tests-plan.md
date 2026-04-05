# Deep Dive: Remaining 171 Skipped Tests — Realistic Fix Plan

## Executive Summary

After v2.0.58, **171 tests remain skipped**. This analysis categorises every one and produces a realistic
implementation roadmap.

| Bucket | Tests | Fixable In-Core | Needs Extension Package | Permanent / Doc-Only |
|--------|:-----:|:---------------:|:-----------------------:|:--------------------:|
| Stored Procedures (JS) | 10 | 4 | 6 | 0 |
| JsTrigger Advanced | 11 | 7 | 2 | 2 |
| Proxy Semantics | 2 | 2 | 0 | 0 |
| Partition Key Type Discrimination | 4 | 4 | 0 | 0 |
| Query Pipeline / FakeCosmosHandler | 7 | 5 | 0 | 1+1 borderline |
| Change Feed Advanced | 7 | 4 | 0 | 3 |
| Index Simulation | 7 | 0 | 0 | 7 |
| Response Metadata / Divergent | 44 | ~9 | 0 | ~35 |
| LINQ Restrictions | 7 | 3 | 0 | 4 |
| Full-text / Vector Search | 11 | 4 | 0 | 5+2 hard |
| WAF Integration | 11 | 2 | 0 | 9 |
| Other (misc) | 27 | ~10 | 0 | ~17 |
| **TOTAL** | **~148** | **~54** | **~8** | **~86** |

> The remaining ~171 breaks down as: **~54 fixable in-core**, **~8 needing extension packages**,
> **~86 permanent / documentation-only**, and **~23 that are hard but potentially achievable
> with significant effort**.

---

## Part 1: Extension Packages (New Packages Following JsTriggers Pattern)

### Package: `CosmosDB.InMemoryEmulator.JsStoredProcedures`

**Pattern:** Identical to `JsTriggers` — interface in core, Jint implementation in optional package.

**Core changes needed:**
1. Add `IJsStoredProcedureEngine` interface to core:
   ```csharp
   public interface IJsStoredProcedureEngine
   {
       string Execute(string jsBody, PartitionKey pk, dynamic[] args,
                      IReadOnlyDictionary<string, JObject> partitionItems);
   }
   ```
2. Add `JsStoredProcedureEngine` property on `InMemoryContainer`
3. In `ExecuteStoredProcedureAsync<T>`: try C# handler first → then JS engine → then throw helpful message

**Package implementation:**
- Jint-based engine providing the Cosmos server-side API:
  - `getContext().getCollection()` → read/query/create/replace/delete documents (scoped to PK)
  - `getContext().getResponse().setBody()`
  - `console.log()` → captured for `x-ms-documentdb-script-log-results` header
- 10-second timeout via `CancellationTokenSource`
- 2MB response size limit check
- Partition scoping: only access items within the specified partition key

**Tests unlocked (6 require JS engine):**
| Test | Current Skip Reason | What's Needed |
|------|-------------------|---------------|
| `ExecuteStoredProcedure_JavaScriptBody_ShouldExecute` | JS body not executed | JS engine |
| `ExecuteStoredProcedure_NonStringGenericType_ShouldDeserialize` | Only `<string>` mocked | JS engine + generic deserialisation |
| `ExecuteStoredProcedure_EnableScriptLogging_ShouldReturnLogs` | No script logging | JS engine + console capture |
| `StoredProcedure_CrossPartition_ShouldBeScoped` | No partition scoping | JS engine + constrained context |
| `ExecuteStoredProcedureStreamAsync_ShouldReturnStream` | No stream variant | JS engine + stream wrapping |
| `CrudStreamVariants_ShouldWork` | No stream CRUD mocks | Medium — NSubstitute + stream wrappers |

**Tests fixable in-core WITHOUT the package (4):**
| Test | Fix |
|------|-----|
| `GetStoredProcedureQueryIterator_ShouldEnumerateProcedures` | Return `InMemoryFeedIterator<StoredProcedureProperties>` from `_storedProcedureProperties` |
| `ExecuteStoredProcedure_10SecondTimeout_ShouldThrow` | Add timeout wrapper around handler execution |
| `ExecuteStoredProcedure_2MBResponseLimit_ShouldFail` | Check response string length against 2MB |
| `StoredProcedure_SystemMetadata_ShouldHaveEtagTimestamp` | Populate `_self`, `_rid`, `_ts`, `_etag` on `StoredProcedureProperties` |

**Effort:** ~3-5 days for the full package (Jint `getContext().getCollection()` is the hard part).
**Effort for in-core sproc fixes:** ~2-4 hours.

---

### JsTriggers Package Enhancements (existing package)

Several tests need enhancements to the **existing** `CosmosDB.InMemoryEmulator.JsTriggers` package:

| Test | Enhancement | Effort |
|------|------------|--------|
| `PostTrigger_Js_SetBody_ModifiesResponse` | Change `IJsTriggerEngine.ExecutePostTrigger` return type from `void` to `JObject?`; thread modified body through all write paths | **Medium** — interface change is breaking |
| `Trigger_Collection_Access` | Add `getContext().getCollection()` to trigger Jint context (same plumbing as sproc engine) | **Hard** — biggest change |
| `PreTrigger_Js_InfiniteLoop_TimesOut` | Already works — skip reason is "too slow for CI" | **N/A** |
| `PreTrigger_Js_MaxStatements_Exceeded` | Already works — skip reason is "too slow for CI" | **N/A** |

**Tests fixable in-core (touching InMemoryContainer, not JsTriggers package):**
| Test | Fix | Effort |
|------|-----|--------|
| `PreTrigger_Js_GetResponse_Throws` | Wire `getResponse()` in pre-trigger context to throw 400 | **Easy** |
| `MultipleTriggers_RealCosmosOnlyExecutesOne` | Enforce single trigger execution | **Easy** (option flag) |
| `PostTrigger_Js_GetRequest_Available` | Pass original request body to post-trigger context | **Easy-Medium** |
| `PreTrigger_Js_ModifiesIdField_Undefined` | After pre-trigger, compare id with original; reject if changed | **Easy** |
| `PreTrigger_Js_ModifiesPartitionKey_Undefined` | Same for PK field | **Easy** |
| `ChangeFeed_RolledBack_OnPostTriggerFailure` | Move change feed recording to after post-trigger success | **Medium** |
| `Trigger_Js_TransactionalBatch_NotSupported` | Add trigger options to `InMemoryTransactionalBatch` per-operation | **Medium** |

---

## Part 2: In-Core Fixes (No External Packages)

### Phase 1: Quick Wins (~15 tests, ~1-2 days)

These are isolated, low-risk changes.

#### 2.1 Proxy Semantics (2 tests)
**Files:** `InMemoryCosmosClient.cs`, `InMemoryDatabase.cs`

| Test | Fix |
|------|-----|
| `GetDatabase_NonExistent_ReadAsync_Throws404` | Track explicitly-created databases; `ReadAsync` returns 404 for auto-created ones |
| `GetContainer_NonExistent_ReadAsync_Throws404` | Same for containers |

**Approach:** Add a `HashSet<string> _explicitlyCreatedDatabases` in `InMemoryCosmosClient`.
`GetDatabase()` still auto-creates for convenience, but `ReadAsync` on auto-created → 404.
`CreateDatabaseAsync` / `CreateDatabaseIfNotExistsAsync` adds to the explicit set.
Same pattern for containers in `InMemoryDatabase`.

**Risk:** Low — backward compatible (auto-create still works for CRUD, only metadata reads change).

#### 2.2 LINQ Enforcement (3 tests)
**File:** `InMemoryContainer.cs`, `InMemoryFeedIteratorSetup.cs`

| Test | Fix | Effort |
|------|-----|--------|
| `AllowSynchronousQueryExecution_False_ShouldThrow` | Check `linqSerializerOptions.AllowSynchronousQueryExecution` and throw `InvalidOperationException` | **Easy** |
| `ContinuationToken_OnLinq_ShouldResumeFromCheckpoint` | Pass continuationToken to `InMemoryFeedIterator` from `GetItemLinqQueryable` | **Medium** |
| `MaxItemCount_ShouldPaginate` | Wire `requestOptions?.MaxItemCount` through to `InMemoryFeedIterator` | **Medium** |

#### 2.3 Response Metadata Quick Fixes (4 tests)
| Test | Fix | Effort |
|------|-----|--------|
| `x-ms-item-count header` | Set `Headers["x-ms-item-count"]` on `InMemoryFeedResponse` | **Easy** |
| `SubStatusCodes on CosmosException` | Add `SubStatusCode` parameter to key exception-throwing paths | **Easy-Medium** |
| `ETag monotonic ordering` | Use incrementing counter instead of random GUID for `_etag` | **Easy** |
| `ConflictResolutionPolicy stored+returned` | Already stored — ensure it's returned on `ReadContainerAsync` | **Easy** |

#### 2.4 ReadMany IfNoneMatchEtag (1 test)
**File:** `InMemoryContainer.cs`

| Test | Fix | Effort |
|------|-----|--------|
| `IfNoneMatchEtag_Honored` | Check `ReadManyRequestOptions.IfNoneMatchEtag` against current item ETag; return 304 | **Easy-Medium** |

#### 2.5 Stored Procedure Metadata (4 tests — no JS needed)
See Part 1 above (QueryIterator, timeout, 2MB limit, system metadata).

#### 2.6 WAF Routing Fix (1 test)
**File:** `FakeCosmosHandler.cs`

| Test | Fix | Effort |
|------|-----|--------|
| `PerContainerDatabaseName_MultipleDbsSameContainerName` | Route by `database/container` composite key, not just container name | **Medium** |

---

### Phase 2: Medium Effort (~18 tests, ~3-5 days)

#### 2.7 Partition Key Type Discrimination (4 tests)
**File:** `InMemoryContainer.cs` — `PartitionKeyToString` method

**Current problem:** `PartitionKeyToString` converts both `PK(42)` and `PK("42")` to `"42"`,
losing type information. `PartitionKey.None` and `PartitionKey.Null` both map to `null`.

**Fix:**
```
PartitionKey.None → "__pk_none__" (sentinel)
PartitionKey.Null → null (keeps current behavior)
PK(42)   → use raw JSON: [42]     (or type-prefixed: "n:42")
PK("42") → use raw JSON: ["42"]   (or type-prefixed: "s:42")
PK(true) → use raw JSON: [true]   (or type-prefixed: "b:true")
```

**Impact:** Pervasive — `PartitionKeyToString` is called ~15 times across CRUD, queries, change feed,
batch, ReadMany. Need to update all comparison paths. Recommend using `partitionKey.ToString()` (the raw
JSON) as the storage key directly, which already preserves type.

**Tests unlocked:**
- `PartitionKeyNone_ShouldNotMatchPartitionKeyNull`
- `PartitionKey_NumericVsString_ShouldBeDistinct`
- `PartitionKey_BooleanVsString_ShouldBeDistinct`
- `PartitionKey_MissingInItem_FallsBackToId` (related)

**Risk:** Medium — pervasive change, needs thorough regression testing.

#### 2.8 Change Feed Improvements (4 tests)
**File:** `InMemoryContainer.cs`, `InMemoryStreamFeedIterator.cs`

| Test | Fix | Effort |
|------|-----|--------|
| `ChangeFeedStream_PageSizeHint_LimitsResults` | Add `PageSizeHint` property to `InMemoryStreamFeedIterator`; paginate results | **Medium** |
| `ChangeFeed_FromNow_WithFeedRange_ScopesToRange` | Handle `ChangeFeedStartFromNow` + `FeedRange` in `FilterChangeFeedByStartFrom` | **Medium** |
| `ChangeFeed_AllVersionsAndDeletes_ViaSDKEnum` | Map `ChangeFeedMode.AllVersionsAndDeletes` to existing checkpoint-based logic | **Medium** |
| `ChangeFeed_Processor_DeliversOnlyLatestVersion` | Deduplicate by id in processor batches, keeping latest version | **Medium** |

#### 2.9 SQL Parser / Query Improvements (6 tests)
**File:** `CosmosSqlParser.cs`, `InMemoryContainer.cs`

| Test | Fix | Effort |
|------|-----|--------|
| `LENGTH() SQL function` | Add `LENGTH` case to function evaluator | **Easy** |
| `GROUP BY with function expressions` (`GROUP BY LOWER(c.name)`) | Evaluate function expressions as GROUP BY keys | **Medium** |
| `SELECT c.*` identical to `SELECT *` | Handle `c.*` in SELECT parser | **Medium** |
| `Object literal arguments in function calls` | Parse `{foo: 1}` as function argument | **Medium** |
| `GROUP BY + ORDER BY on aggregate alias` | Support ORDER BY referencing an alias defined in GROUP BY SELECT | **Medium** |
| `Numeric PK through FakeCosmosHandler` | Parse numeric PK from `x-ms-documentdb-partitionkey` header | **Medium** |

#### 2.10 Trigger & Change Feed Ordering (3 tests — in-core)
| Test | Fix | Effort |
|------|-----|--------|
| `ChangeFeed_RolledBack_OnPostTriggerFailure` | Record change feed AFTER post-trigger success | **Medium** |
| `GetTriggerQueryIterator` | Return `InMemoryFeedIterator<TriggerProperties>` from stored triggers | **Easy** |
| `ChangeFeed cleans on post-trigger rollback` | Remove change feed entry if post-trigger throws | **Medium** |

#### 2.11 Document Size Accounting (3 tests)
**File:** `InMemoryContainer.cs`, `InMemoryTransactionalBatch.cs`

| Test | Fix | Effort |
|------|-----|--------|
| `SystemProperties_CountToward2MB` | Include `_ts`, `_etag`, `_rid`, `_self` in size calculation | **Medium** |
| `DeleteMetadata_CountsTowardBatchSize` | Count delete operation envelope toward batch limit | **Medium** |
| `ReadMetadata_CountsTowardBatchSize` | Count read operation response toward batch limit | **Medium** |

---

### Phase 3: Hard (Significant Effort) (~10 tests, ~1-2 weeks)

#### 2.12 FakeCosmosHandler Document Wrapping (3 tests)
**File:** `FakeCosmosHandler.cs`

The Cosmos SDK expects specific JSON document structures for advanced queries:

| Query Type | Expected Format | Difficulty |
|-----------|----------------|------------|
| `GROUP BY` | `{"groupByItems": [...], "payload": {...}}` per group | **Hard** — undocumented SDK expectation |
| `DISTINCT + ORDER BY` | Combined `distinctType: "Ordered"` + `orderByItems` | **Hard** — SDK merge-sort pipeline |
| `Multiple aggregates` | One partial-aggregate document per aggregate | **Hard** — SDK reconstructs row from partials |

**Approach:** Reverse-engineer the SDK's `GroupByDocumentQueryExecutionContext`,
`DistinctDocumentQueryExecutionContext`, and `AggregateDocumentQueryExecutionContext`
to understand exact expected JSON format. Test against SDK source code.

**Risk:** High — undocumented internal API. SDK version upgrades may break the format.

#### 2.13 Full-text Search — RRF & Stop Words (2 tests)
| Test | Fix | Effort |
|------|-----|--------|
| `RRF_BasicFusion_ShouldCombineScores` | Implement Reciprocal Rank Fusion scoring + `ORDER BY RANK RRF(...)` syntax | **Hard** |
| `FullTextContains_StopWordRemoval` | Add English stop-word list filtering to FULLTEXTCONTAINS | **Medium** |

#### 2.14 Computed Properties — Undefined Propagation (2 tests)
| Test | Fix | Effort |
|------|-----|--------|
| `Undefined_PropagatesThrough_Functions` | Cross-cutting: all function evaluators must propagate `undefined` (not null) for missing properties | **Hard** — touches many evaluator paths |
| `CONCAT_UndefinedArg_ReturnsUndefined` | Same — `CONCAT` with undefined arg → undefined | **Hard** (same underlying change) |

#### 2.15 TTL Proactive Eviction (2 tests)
| Test | Fix | Effort |
|------|-----|--------|
| `TTL_Eviction_RecordsDeleteInChangeFeed` | Hook lazy eviction to call `RecordDeleteTombstone()` | **Medium** |
| `Queries_EvictExpiredItems_FromMemory` | Actually remove items from `_items` during query-time scan | **Medium** |

**Note:** True proactive eviction (background timer) is architectural, but query-time eviction + change feed
tombstone recording is achievable.

---

## Part 3: Permanent / Documentation-Only (~86 tests)

These tests are **intentionally skipped** and document known divergences that are either
impossible or impractical to fix in an in-memory emulator. They should remain skipped with
clear explanations.

### Authentication & Encryption (3 tests)
- Client encryption key management (Azure Key Vault integration)
- Not meaningful for in-memory testing

### RU Charges (8 tests across 5 files)
- Real Cosmos: reads ~1 RU, writes ~5-10 RU, queries vary by complexity
- Emulator: always 1.0 RU — acceptable for testing
- Would need the entire Cosmos cost model to implement

### Diagnostics (5 tests)
- Real Cosmos: detailed timing, latency, retry info, endpoint data
- Emulator: minimal stubs — no distributed system to diagnose

### Session Token Format (5 tests)
- Real: `0:-1#12345` with partition range and LSN progression
- Emulator: static `0:0#1` — acceptable; monotonic LSN would need MVCC

### Continuation Token Format (5 tests)
- Real: opaque base64 JSON with partition range info
- Emulator: plain integer offsets — functionally equivalent

### Consistency Levels (2 tests)
- Would need multi-replica simulation — entire distributed systems problem

### Indexing Policy Enforcement (7 tests)
- Would need B-tree / inverted index / spatial index simulation
- Emulator stores policy but full-scans everything — by design

### Analytical Store / Synapse Link (1 test)
- Entirely separate subsystem

### LINQ Over-Acceptance (4 tests — LinqToFeedIteratorTests)
- GroupBy, Last, Aggregate, Reverse work in emulator because IQueryable → LINQ-to-Objects
- Real SDK LINQ provider rejects them — this is by design

### Permission Tokens (1 test)
- Real: HMAC-signed tokens with resource scope + expiry
- Emulator: synthetic stubs

### Concurrency Isolation (3 tests)
- Batch not isolated from concurrent direct CRUD
- RestoreSnapshot has brief window — inherent to ConcurrentDictionary swap
- Would need transaction isolation levels (SERIALIZABLE etc.)

### WAF Architecture (8 tests)
- Scoped lifetimes, HTTP pagination, change feed via HTTP headers, container delete + DI
- These are application/framework integration concerns, not emulator core

### Query Plan Metadata (2 tests)
- `dCountInfo`, `hybridSearchQueryInfo` — query plan fields are hardcoded approximations

### SDK Compatibility Internals (3 tests)
- queryEngineConfiguration, indexing policy in collection metadata, range partitioning format

### Null-Coalescing in LINQ (1 test)
- SDK LINQ provider cannot translate `??` — not an emulator issue

### Bulk Operations Architecture (2 tests)
- InMemoryContainer is synchronous — throughput optimization meaningless

### Load Test Flakiness (2 tests)
- GC pressure / timing-dependent assertions

### ContainerInternal Cast (1 test — ChangeFeedTests:872)
- `ChangeFeedProcessorBuilder.WithLeaseContainer()` casts to `ContainerInternal` (internal SDK type)
- Cannot implement without reflection or InternalsVisibleTo

### Other Permanent (3 tests)
- PITR creates new account (line 1086)
- ARRAY_CONTAINS_ANY element-vs-array semantics (intentional convenience)
- Nested array hashing in set operations (edge case)

---

## Recommended Implementation Order

### Wave 1: Quick Wins — Target v2.0.59 (~15 tests)
**Estimated effort: 1-2 days**

1. Proxy semantics (2 tests) — `GetDatabase`/`GetContainer` 404 behavior
2. Stored procedure metadata — QueryIterator, timeout, 2MB, system properties (4 tests)
3. LINQ enforcement — `allowSynchronousQueryExecution`, continuationToken, MaxItemCount (3 tests)
4. Response metadata — x-ms-item-count, ETag ordering (2 tests)
5. ReadMany IfNoneMatchEtag (1 test)
6. TriggerQueryIterator (1 test)
7. LENGTH() SQL function (1 test)
8. WAF database+container routing (1 test)

### Wave 2: Medium Core Fixes — Target v2.0.60 (~18 tests)
**Estimated effort: 3-5 days**

1. Partition key type discrimination (4 tests) — pervasive but well-scoped
2. SQL parser improvements — GROUP BY+functions, c.*, object literals (4 tests)
3. Change feed improvements — PageSizeHint, FromNow+FeedRange, AVAD enum, latest-only (4 tests)
4. Trigger in-core fixes — pre-trigger id/PK guard, setBody=void, change feed ordering (4 tests)
5. Document size accounting — system properties in 2MB calc (2 tests)

### Wave 3: JsTrigger Enhancements — Target v2.0.61 (~7 tests)
**Estimated effort: 2-3 days (JsTriggers package changes)**

1. `IJsTriggerEngine.ExecutePostTrigger` → return `JObject?` for setBody (1 test)
2. Pre-trigger `getResponse()` throws 400 (1 test)
3. Post-trigger `getRequest()` available (1 test)
4. Pre-trigger id/PK mutation prevention (2 tests)
5. Change feed rollback on post-trigger failure (1 test)
6. TransactionalBatch trigger options (1 test)

### Wave 4: Hard Core Fixes — Target v2.0.62 (~6 tests)
**Estimated effort: 1-2 weeks**

1. FakeCosmosHandler GROUP BY document wrapping (1 test — unlocks 2 related)
2. Undefined propagation in function evaluators (2 tests)
3. TTL eviction + change feed tombstones (2 tests)
4. Full-text stop word filtering (1 test)

### Wave 5 (Optional): JS Stored Procedures Package
**Estimated effort: 3-5 days for new package**

1. Create `IJsStoredProcedureEngine` interface in core
2. Create `CosmosDB.InMemoryEmulator.JsStoredProcedures` package with Jint
3. Implement `getContext().getCollection()` — CRUD within partition scope
4. Script logging, timeout, response size limit
5. Unlocks 6 tests that need JS execution

### Wave 6 (Optional): Stretch Goals
- DISTINCT + ORDER BY document wrapping (~1 test)
- Multiple aggregates document wrapping (~1 test)
- RRF scoring (~1 test)
- Parameterised vectors in VectorDistance (~1 test)

---

## Architecture Decision: In-Core vs Extension Package

| Feature | Recommendation | Rationale |
|---------|---------------|-----------|
| Stored procedure JS execution | **Extension package** | Jint dependency (same as JsTriggers) |
| Trigger `getCollection()` access | **Extension package** (shared with sprocs) | Same Jint + context API |
| All other fixes | **In-core** | No external dependencies needed |

A single `CosmosDB.InMemoryEmulator.JsServerSide` package could combine both stored procedure
and advanced trigger features (getCollection, setBody, getRequest) since they share the same
Jint context API. Alternatively, enhance the existing `JsTriggers` package to cover the trigger
improvements and create a separate `JsStoredProcedures` package.

---

## Summary: Test Count Impact

| Wave | Tests Unskipped | Cumulative Skipped → |
|------|:--------------:|:-------------------:|
| Current state | — | 171 |
| Wave 1: Quick Wins | ~15 | ~156 |
| Wave 2: Medium Core | ~18 | ~138 |
| Wave 3: JsTrigger Enhancements | ~7 | ~131 |
| Wave 4: Hard Core | ~6 | ~125 |
| Wave 5: JS Stored Procs | ~6 | ~119 |
| Wave 6: Stretch | ~4 | ~115 |
| **Best achievable** | **~56** | **~115** |

The remaining ~115 are permanent documentation tests that verify known divergences between the
in-memory emulator and real Cosmos DB. These are valuable as living documentation and should
remain skipped with clear explanations.
