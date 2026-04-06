This document lists all known areas where the in-memory emulator differs from real Azure Cosmos DB. These are documented so you can make informed decisions about which behaviours matter for your tests.

For a full list of supported features, see [Features](Features). For a side-by-side comparison with the official Microsoft emulator, see [Feature Comparison](Feature-Comparison-With-Alternatives). For SQL function coverage, see [SQL Queries](SQL-Queries).

## Limitations

| Area | Status | Notes |
|------|--------|-------|
| Spatial functions | ⚠️ Approximations | All 6 functions implemented with real geometry (haversine, point-in-polygon, spherical excess); results may differ slightly from Cosmos DB. See [SQL Queries — Geospatial](SQL-Queries#geospatial-functions) |
| Analytical store (Synapse) | ❌ Not simulated | OLAP context not available |
| IndexingPolicy | ⚠️ Stub | Accepted and stored but doesn't affect query performance |
| TTL eviction | ⚠️ Lazy | Expired items removed on next read/query/write, not proactively. TTL-triggered evictions do not produce change feed events. Supports container-level `DefaultTimeToLive`, per-item `_ttl` override, and `_ttl = -1` (never expire). See [Features — TTL](Features#ttl--expiration) |
| Resource IDs | ⚠️ Synthetic | Valid format but doesn't match real Cosmos RIDs |
| Throughput (RU/s) | ⚠️ Synthetic | Persisted via `ReplaceThroughputAsync` and returned by `ReadThroughputAsync`, but has no effect on performance or throttling |
| `AllVersionsAndDeletes` mode | ⚠️ Via checkpoint | Use `GetChangeFeedIterator<T>(checkpoint)` for all versions and delete tombstones; `ChangeFeedMode.AllVersionsAndDeletes` is internal in older SDK versions. See [Features — Change Feed](Features#change-feed) |
| Users / permissions | ✅ Stub store | CRUD operations return synthetic responses with fake tokens; no authorization enforced. See [Features — Users & Permissions](Features#users--permissions) |
| Client encryption keys | ❌ Not implemented | Requires Azure Key Vault; not meaningful for in-memory testing |
| Conflict resolution policy | ⚠️ Stored only | Policy is stored on `ContainerProperties` and returned but not enforced at runtime |
| Full-text search | ⚠️ Approximate | `FULLTEXTCONTAINS`, `FULLTEXTCONTAINSALL`, `FULLTEXTCONTAINSANY`, `FULLTEXTSCORE`, `ORDER BY RANK` implemented with case-insensitive substring matching (no stemming, no BM25 scoring, no indexing policy required). See [Features — Full-Text Search](Features#full-text-search-approximate) |
| Change feed stream processor | ⚠️ Partial | `GetChangeFeedProcessorBuilder` with a `Stream` handler creates an `InMemoryChangeFeedStreamProcessor`, but `WithLeaseContainer()` may fail because the SDK casts the lease `Container` to the internal `ContainerInternal` class. Use the typed `ChangeFeedProcessor<T>` handler instead. See [Features — Change Feed](Features#change-feed) |
| Vector search | ⚠️ Brute-force only | `VECTORDISTANCE` fully supported (cosine, dot product, Euclidean) but no vector index policy, no dimension limits, no ANN indexing. Zero-magnitude vectors return `null`. ~~Unknown distance functions fall back to cosine~~ (fixed in v2.0.53: now throws). ~~Extra args (>4) silently ignored~~ (fixed in v2.0.53: now rejects with 400). Infinity/NaN results return `null`. See [Features — Vector Search](Features#vector-search) |
| GROUP BY + aggregate(function) | ⚠️ Not supported | `GROUP BY` with aggregate functions wrapping function calls (e.g. `AVG(VectorDistance(...))`) fails — the aggregate pipeline only supports property paths, not arbitrary expressions |
| ~~Patch Increment concurrency~~ | ⚠️ Not atomic | Concurrent `Increment` patches on the same item may lose updates (read-then-write without per-item locking). Real Cosmos serialises patches per-partition. **Triggers now fire on patch** (v2.0.58): `PatchItemAsync` calls `ExecutePreTriggers`/`ExecutePostTriggers` when specified in `PatchItemRequestOptions` |
| Stream methods + oversized docs | ⚠️ Throws | `ValidateDocumentSize()` throws `CosmosException` in stream methods instead of returning `ResponseMessage` with 413 |
| Change feed processor dedup | ⚠️ All versions | Processor delivers all versions (including intermediates), not just latest version per item. Handlers should be idempotent |
| Change feed `_lsn` | ❌ Not present | Real Cosmos DB includes `_lsn` (logical sequence number) in change feed items; emulator does not |
| TTL eviction + change feed | ⚠️ Silent | TTL-evicted items do not produce a change feed entry (tombstone). Items silently disappear |
| Computed properties: undefined propagation | ⚠️ Returns null | When a CP expression references a missing field, real Cosmos evaluates to undefined (property absent); emulator evaluates to null (property present). Applies to LOWER, UPPER, TRIM, CONCAT, etc. |
| Computed properties: validation | ✅ Fixed in v2.0.54 | ~~No validation~~ **Fixed in v2.0.53–v2.0.54**: max 20 CPs enforced, reserved system property names rejected, SELECT VALUE syntax validated, prohibited clauses rejected (WHERE/ORDER BY/GROUP BY/TOP/DISTINCT/OFFSET/LIMIT/JOIN), cross-CP reference checking enforced |
| Computed properties: LINQ | ❌ Not available | LINQ queries (`GetItemLinqQueryable`) bypass the SQL engine — computed properties are not evaluated |
| Computed properties: SELECT c.* | ❌ Parse error | `SELECT c.*` (aliased wildcard) throws parse error. Use `SELECT *` instead |
| ~~Computed properties: Patch guard~~ | ✅ Fixed in v2.0.54 | Patch operations targeting computed property paths are now rejected with `CosmosException(400)`, matching real Cosmos DB |
| Concurrency: multi-dictionary atomicity | ⚠️ Brief inconsistency | Writes to `_items`, `_etags`, `_timestamps` are not atomic relative to each other. Under extreme concurrency, a reader may briefly see inconsistent state. Final state is always consistent |
| Concurrency: ETag check-then-act | ⚠️ Not serialised | ETag validation (`IfMatchEtag`) is not atomic with the subsequent write. Under extreme concurrency, more than one ETag-protected write may succeed. Real Cosmos serialises writes per partition |
| Concurrency: batch isolation | ⚠️ Not globally isolated | Batch execution is not globally isolated from non-batch operations. Concurrent readers may see intermediate batch state during execution or briefly see empty state during rollback |
| ~~Container.Database property~~ | ✅ Fixed in v2.0.54 | `InMemoryContainer.Database` now returns the same cached instance on each access. When created via `InMemoryDatabase`, `container.Database.Id` returns the parent database name |
| ~~UniqueKeyPolicy on Replace~~ | ✅ Fixed in v2.0.53 | `ReplaceContainerAsync` now preserves the original `UniqueKeyPolicy` if the replacement properties omit it |
| ~~DeleteContainer + triggers~~ | ✅ Fixed in v2.0.53 | `DeleteContainerAsync` now clears stored procedures, UDFs, and triggers alongside items/ETags/timestamps/change feed |
| ~~CancellationToken on management~~ | ✅ Fixed in v2.0.56–v2.0.57 | `CreateDatabaseAsync`, `CreateDatabaseIfNotExistsAsync` (v2.0.56), and container management methods `ReadContainerAsync`, `ReplaceContainerAsync`, `DeleteContainerAsync` + stream variants (v2.0.57) now check `CancellationToken` |
| ~~Resource ID validation~~ | ✅ Fixed in v2.0.56 | Database and container names containing `/`, `\`, `#`, `?` or exceeding 255 characters are now rejected with `CosmosException(400)`, matching real Cosmos DB |
| Metadata query filtering | ⚠️ Not supported | `GetDatabaseQueryIterator` and `GetContainerQueryIterator` ignore WHERE clauses and query text — all items are returned regardless of filter |
| Metadata query paging | ⚠️ Single page | `MaxItemCount` and continuation tokens are ignored for database/container query iterators. All items returned in a single page |
| ~~GetContainer auto-creates~~ | ✅ Fixed in v2.0.59 | ~~`Database.GetContainer("nonexistent")` creates the container immediately~~ **Fixed**: `GetDatabase` / `GetContainer` now return proxy-like references. `ReadAsync` / `ReadContainerAsync` return 404 if the resource was not explicitly created via `CreateDatabaseAsync` / `CreateContainerAsync`. CRUD on auto-created containers still works for test convenience |
| ~~Stream API response bodies~~ | ✅ Fixed in v2.0.59 | ~~`CreateDatabaseStreamAsync` returns `ResponseMessage` with `Content = null`~~ **Fixed**: `CreateDatabaseStreamAsync` now includes resource JSON in response body. `DeleteStreamAsync` still returns null content. ~~`ErrorMessage` is null for error responses~~ (fixed in v2.0.55: ErrorMessage now set for all error status codes) |
| ~~Permission.DeleteAsync 404~~ | ✅ Fixed in v2.0.59 | ~~Deleting a non-existent permission returns 204 NoContent~~ **Fixed**: Now throws `CosmosException(404)`, matching real SDK |
| ~~User.ReplaceAsync Id change~~ | ✅ Fixed in v2.0.59 | ~~Emulator allows changing user Id via ReplaceAsync~~ **Fixed**: Now throws `CosmosException(400)` when replacement Id differs from existing user Id, matching real SDK |
| ~~System property `_rid`~~ | ✅ Fixed in v2.0.57 | Documents now include a synthetic `_rid` (base64-encoded GUID). Format differs from real Cosmos RIDs but the property is present. `DOCUMENTID()` returns it correctly |
| ~~System property `_self`~~ | ✅ Fixed in v2.0.57 | Documents now include a synthetic `_self` URI path (e.g. `dbs/db/colls/col/docs/{id}`). Format is approximate |
| `GetCurrentDateTime()` per-row | ⚠️ Per-row evaluation | Real Cosmos DB evaluates `GetCurrentDateTime()` once per query. Emulator evaluates per-row, so results may differ by nanoseconds. Use `GetCurrentDateTimeStatic()` for consistent per-query values |
| Nanosecond precision | ℹ️ 100ns resolution | Both emulator and real Cosmos DB have 100-nanosecond (1 tick) resolution. `DateTimeAdd('ns', 50, ...)` truncates to 0 ticks |
| CosmosResponseFactory | ⚠️ NSubstitute stub | `CosmosClient.ResponseFactory` returns an NSubstitute mock whose methods return default/null. Real SDK provides a factory for deserializing response messages |
| ~~Unsupported SQL error type~~ | ✅ Fixed in v2.0.54 | Unsupported SQL functions and malformed SQL now throw `CosmosException(HttpStatusCode.BadRequest)`, matching real Cosmos DB. **Stored procedure handler errors** also wrapped in `CosmosException(400)` since v2.0.55 |
| Permission tokens | ⚠️ Synthetic | Permission tokens have format `type=resource&ver=1&sig=stub_<id>` with fake signatures. Real Cosmos generates HMAC-signed resource tokens |
| Composite index not required | ⚠️ Not enforced | Multi-field `ORDER BY` works without composite index definitions. Real Cosmos requires composite indexes for multi-field ordering |

---

## Behavioural Differences

These are areas where the emulator produces different results from real Cosmos DB. Each has a corresponding test documenting the difference.

> **Note:** Tests marked "(skipped)" are deliberately skipped — they document a known gap rather than a passing assertion.

### 1. Partition Key Fallback (Missing Field)

**Real Cosmos DB:** When `PartitionKey.None` is used and the partition key field is missing from the document, the document is stored with a system-defined fallback PK.

**InMemoryContainer:** Falls back to the `id` field as the PK value when the PK path field is missing.

**Impact:** Low. Only affects documents missing their partition key field entirely.

**Test:** `PartitionKeyFallbackDivergentBehaviorTests.PartitionKey_None_WithMissingPkField_Succeeds_InMemory`

---

### 2. FeedRange Count

**Real Cosmos DB:** Returns multiple `FeedRange` instances based on physical partition distribution.

**InMemoryContainer:** Defaults to 1 `FeedRange`. Set `FeedRangeCount` to a higher value to simulate multiple physical partitions. FeedRange-scoped queries and change feed iterators use MurmurHash3-based partition key hashing to filter items to the correct range.

**Impact:** Low. Use `container.FeedRangeCount = N` for code that parallelises across feed ranges. See [Features — FeedRange Support](Features#feedrange-support) for examples.

**Test:** `FeedRangeDivergentBehaviorTests.GetFeedRanges_DefaultsSingle_SetFeedRangeCountForMultiple`

---

### 3. Incremental Change Feed (Multiple Updates)

**Real Cosmos DB (Incremental mode):** Returns only the latest version of each item, but the timing and batching of "latest" can vary based on physical partition distribution.

**InMemoryContainer:** Strictly returns only the latest version per item across all updates.

**Impact:** None for most applications. The in-memory behaviour is actually more predictable.

---

### 4. Triggers Use C# Handlers (JavaScript Optional)

**Real Cosmos DB:** Pre-triggers and post-triggers execute server-side JavaScript before/after create, replace, and delete operations. A pre-trigger can modify the document before it's persisted.

**InMemoryContainer:** Triggers execute via `RegisterTrigger()` with C# handlers (`Func<JObject, JObject>` for pre-triggers, `Action<JObject>` for post-triggers). JavaScript trigger bodies registered via `CreateTriggerAsync` are stored but **not interpreted by default**. To enable JavaScript trigger execution, install the optional `CosmosDB.InMemoryEmulator.JsTriggers` package and call `container.UseJsTriggers()`. Pre-triggers can modify documents before persistence; post-trigger failures roll back the write.

**Impact:** Low. Use `RegisterTrigger()` to implement trigger logic in C# instead of JavaScript, or use `UseJsTriggers()` to interpret JS bodies. Trigger invocations are controlled via `ItemRequestOptions.PreTriggers` / `PostTriggers`, matching the real SDK API surface. See [Features — Triggers](Features#triggers) for usage examples.

**Test:** `TriggerExecutionTests.PreTrigger_ShouldModifyDocumentOnCreate`, `TriggerExecutionTests.PreTrigger_CreateTriggerAsyncAlone_DoesNotFireWithoutRegisterTrigger`

---

### 5. Continuation Tokens are Plain Integers

**Real Cosmos DB:** Continuation tokens are opaque, base64-encoded JSON strings containing internal cursor state, partition information, and version metadata.

**InMemoryContainer:** Continuation tokens are simple integer offsets (e.g. `"3"`, `"10"`). They function correctly for pagination but are not format-compatible with real Cosmos DB tokens.

**Impact:** Low. Only matters if your code parses or inspects the continuation token format. Pagination behaviour (requesting pages, resuming from a token) works correctly.

**Test:** `ContinuationTokenFormatTests.ContinuationToken_EmulatorBehavior_IsPlainIntegerOffset`

---

### 6. ConflictResolutionPolicy Not Enforced

**Real Cosmos DB:** `ConflictResolutionPolicy` controls how write conflicts are resolved in multi-region setups — e.g. Last Writer Wins using a path, or a custom stored procedure.

**InMemoryContainer:** The policy is stored on `ContainerProperties` and returned correctly, but has no runtime effect. Since the emulator is single-region and single-instance, no conflicts arise.

**Impact:** None for single-region testing. Only matters if you test multi-region conflict scenarios.

**Test:** `ConflictResolutionPolicyTests.ConflictResolution_EmulatorBehavior_PolicyStoredButNotEnforced`

---

### 7. PartitionKey.None vs PartitionKey.Null Treated Identically

**Real Cosmos DB:** `PartitionKey.None` and `PartitionKey.Null` have different semantics. `None` means "extract PK from document body" while `Null` means "the partition key value is explicitly null".

**InMemoryContainer:** Both `PartitionKey.None` and `PartitionKey.Null` are mapped to the same internal representation. Items created with one can be read with the other.

**Impact:** Low. Only affects code that distinguishes between documents with no partition key vs documents with an explicit null partition key.

**Test:** `PartitionKeyNoneVsNullTests.PartitionKeyNoneVsNull_EmulatorBehavior_TreatedIdentically`

---

### 8. Cross-Partition Aggregates With Multiple Ranges

**Real Cosmos DB:** Aggregates like `COUNT`, `SUM`, `AVG` are computed per-partition and merged server-side into a single result.

**InMemoryContainer:** When `PartitionKeyRangeCount > 1` in `InMemoryCosmosOptions`, `FakeCosmosHandler` repeats query results for each simulated range, which multiplies aggregate results (e.g. `COUNT` returns `N × rangeCount`).

**Impact:** Low. Only affects non-default `PartitionKeyRangeCount` configurations. Default value of 1 works correctly.

**Test:** `CrossPartitionAggregateTests.CrossPartition_Count_ShouldNotMultiplyResults` (skipped)

---

### 9. Array Functions Don't Accept Literal Arrays

**Real Cosmos DB:** `ARRAY_CONTAINS([1,2,3], 2)` works with inline array literals.

**InMemoryContainer:** Array functions (`ARRAY_CONTAINS`, `ARRAY_LENGTH`, `ARRAY_SLICE`) only accept identifier references (e.g. `c.tags`), not inline array literals.

**Impact:** Low. Literal arrays in SQL queries are uncommon in practice.

**Test:** `ArrayFunctionLiteralTests.ArrayContains_WithLiteralArray_ShouldWork` (skipped)

---

### 10. GetCurrentDateTime() Evaluated Per-Row

**Real Cosmos DB:** `GetCurrentDateTime()` returns a consistent timestamp for all rows in a single query execution.

**InMemoryContainer:** Each row evaluation calls `DateTime.UtcNow` independently, so timestamps may differ by sub-millisecond amounts across rows.

**Impact:** Negligible. The drift is sub-millisecond and irrelevant for all practical purposes.

**Test:** `GetCurrentDateTimeConsistencyTests.GetCurrentDateTime_ShouldReturnSameValueForAllRows` (skipped)

---

### 11. LINQ Queryable Options Ignored

**Real Cosmos DB:** `GetItemLinqQueryable` respects `linqSerializerOptions` and `continuationToken` parameters.

**InMemoryContainer:** Both `linqSerializerOptions` and `continuationToken` parameters are ignored. The emulator uses Newtonsoft.Json internally and doesn't support custom LINQ serializer options.

**Impact:** Low. Only affects code that customises LINQ serialization or uses continuation tokens with LINQ queries. For workarounds when using `.ToFeedIterator()` with LINQ, see [Feed Iterator Usage Guide](Feed-Iterator-Usage-Guide).

**Test:** `LinqQueryableOptionsTests.GetItemLinqQueryable_WithSerializerOptions_ShouldRespectOptions` (skipped)

---

### ~~12. Undefined vs Null Not Distinguished in ORDER BY~~ (FIXED in v2.0.54)

**Fixed:** ORDER BY now implements Cosmos DB's full type ordering: undefined < null < boolean < number < string < array < object. Missing fields (undefined) sort before explicit null values. Cross-type comparisons use type rank.

---

### 13. Full-Text Search Uses Naive Matching (Not BM25)

**Real Cosmos DB:** Full-text search requires a full-text indexing policy on the container. Without it, all `FULLTEXT*` queries return HTTP 400. Functions use NLP tokenization with stemming and stop-word removal. `FULLTEXTSCORE` computes BM25 relevance scores with inverse document frequency and document length normalization.

**InMemoryContainer:** Full-text functions work on any container without indexing configuration — no `FullTextPolicy` or `FullTextIndexes` on `IndexingPolicy` required. Uses case-insensitive substring matching (no stemming). `FULLTEXTSCORE` returns a naive term-frequency count (total occurrences of search terms).

**Impact:** Low. Relative ordering is usually correct. Absolute scores differ. No stemming means "running" won't match "runs". No indexing policy validation means queries won't fail on misconfigured containers (real Cosmos would return 400).

---

### 14. Patch Increment Not Atomic Under Concurrency

**Real Cosmos DB:** Patch operations (including `Increment`) are serialised per-partition, so concurrent `Increment` operations on the same field are atomic — no lost updates.

**InMemoryContainer:** Patch uses read-then-write without per-item locking. Concurrent `Increment` operations on the same item may lose updates due to the read-then-write race condition.

**Impact:** Low. Only affects concurrent patch increment operations on the same item. Sequential patches, or patches on different items, work correctly.

**Test:** `BulkOperationTests.BulkPatch_ConcurrentOnSameItem_AtomicIncrement` (skipped), `BulkOperationTests.BulkPatch_ConcurrentOnSameItem_LastWriterWins_DivergentBehaviour`

---

### 15. ~~Stream Methods Throw on Oversized Documents~~ (FIXED in v2.0.18)

**Fixed:** Stream methods now return `ResponseMessage` with HTTP 413 status code instead of throwing, matching real Cosmos DB behavior.

### 16. Stored Procedure Size Limits Not Enforced

**Real Cosmos DB:** Stored procedure body ≤ 256KB, request/response ≤ 2MB per operation.

**InMemoryContainer:** No size validation on stored procedure delegates or their return values. A stored procedure returning 5MB+ will succeed.

**Impact:** Low. Stored procedures in the emulator are C# delegates, not JavaScript bodies with server-side execution limits.

**Tests:** `StoredProcedureSizeLimitDivergenceTests.StoredProcedure_OversizedResponse_RealCosmosWouldReject` (skipped), `StoredProcedure_OversizedResponse_EmulatorAllowsIt_Divergence`

### 17. Batch Delete/Read Operations Don't Contribute to Batch Size

**Real Cosmos DB:** Delete and read operation metadata counts toward the 2MB batch payload limit.

**InMemoryContainer:** Only create/upsert/replace/patch item serialization contributes to `_estimatedBatchSize`. Delete and read don't add bytes.

**Impact:** Negligible — delete/read metadata is small (just the item ID).

**Tests:** `BatchDeleteReadSizeAccountingTests.Batch_DeleteOperations_ContributeToBatchSize_RealCosmosWouldCount` (skipped), `Batch_DeleteOperations_DoNotContributeToBatchSize_Divergence`

### 18. Post-trigger Can Inflate Document Past 2MB

**Real Cosmos DB:** Re-validates document size after post-trigger execution and rolls back if exceeded.

**InMemoryContainer:** Post-triggers run after commit with no size re-validation. A post-trigger that adds 3MB+ to a small document will succeed.

**Impact:** Low. Post-triggers rarely inflate documents significantly.

**Tests:** `PostTriggerSizeDivergenceTests.PostTrigger_InflatesDocumentPast2MB_RealCosmosWouldReject` (skipped), `PostTrigger_InflatesDocumentPast2MB_EmulatorAllowsIt_Divergence`

### 19. ARRAY_CONTAINS_ANY/ALL Array Form vs Variadic Semantics

**Real Cosmos DB:** `ARRAY_CONTAINS_ANY(arr, val1, val2, ...)` uses variadic syntax where each argument after the array is a single scalar value to search for.

**InMemoryContainer:** Additionally supports array form `ARRAY_CONTAINS_ANY(arr, [val1, val2])` as a convenience. When the second argument is a document property that is an array (e.g., `ARRAY_CONTAINS_ANY(c.tags, c.otherTags)`), the emulator iterates the array's elements. Real Cosmos DB would treat the array as a single value to search for.

**Impact:** Low. The variadic form (official Cosmos syntax) works correctly.

**Tests:** `ArrayContainsAny_DocumentArrayArg_RealCosmosTreatsSingleValue` (skipped), `ArrayContainsAny_DocumentArrayArg_EmulatorIteratesElements`

### 20. Object Literal Arguments in Variadic Function Form

**Real Cosmos DB:** `ARRAY_CONTAINS_ANY(arr, {name:"x"})` passes an inline object as a variadic argument.

**InMemoryContainer:** The SQL parser may not support inline object expressions as function arguments in variadic form.

**Impact:** Very low. Use array form `ARRAY_CONTAINS_ANY(arr, [{name:"x"}])` as a workaround.

**Tests:** `ArrayContainsAny_WithObjectElementInVariadicForm_Works` (skipped), `ArrayContainsAny_WithObjectElement_EmulatorArrayForm`

---

### 21. ETag Format Is Quoted GUID (Not Opaque Timestamp)

**Real Cosmos DB:** ETags are opaque timestamp-based values (e.g., `"0800a2b4-0000-0100-0000-674ebc600000"`).

**InMemoryContainer:** ETags are quoted GUIDs (`"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"`). The format difference is cosmetic — ETag format is not part of the API contract and should only be used for equality comparison.

**Impact:** None. Tests should compare ETags for equality, not parse their contents.

**Tests:** `BehavioralDifferenceTests.ETag_Format_IsQuotedGuid`, `ETagResponseTests.ETag_Format_IsQuotedGuid_OnAllWriteOperations`

**Tests:** `FullTextSearchDivergentBehaviorTests.FullTextScore_UsesNaiveTermFrequency_NotBM25`, `FullTextSearchDivergentBehaviorTests.FullTextContains_NoStemming_LiteralMatchOnly`, `FullTextSearchDivergentBehaviorTests.FullTextContains_WorksWithoutFullTextIndexPolicy`, `FullTextContains_WithoutFullTextIndex_ShouldThrow400` (skipped)

See [Features — Full-Text Search](Features#full-text-search-approximate) for supported functions and examples.

---

### 22. ChangeFeedProcessor Stream Handler

**Real Cosmos DB:** `GetChangeFeedProcessorBuilder` with a `Stream` change handler works alongside a lease container for distributed processing.

**InMemoryContainer:** The builder method creates an `InMemoryChangeFeedStreamProcessor` internally, but the SDK's `WithLeaseContainer()` casts the lease `Container` to `ContainerInternal` (an internal abstract class) to access lease management APIs. `InMemoryContainer` extends the public `Container` class but not `ContainerInternal`, so this cast may fail with `InvalidCastException`. The typed `ChangeFeedProcessor<T>` handler works correctly.

**Impact:** Low. Use the typed `ChangeFeedProcessor<T>` handler instead of the `Stream` variant. See [Features — Change Feed Processor](Features#change-feed-processor) for examples.

**Test:** `ChangeFeedProcessor_StreamHandler_InvokesHandler` (skipped)

---

### 23. Manual Checkpoint Change Feed Processor

**Real Cosmos DB:** `GetChangeFeedProcessorBuilderWithManualCheckpoint` creates a processor where the handler must explicitly call `checkpointAsync()` to save progress. If the handler does not call `checkpointAsync()`, the same batch of changes is redelivered on the next poll cycle.

**InMemoryContainer:** Fully implemented. The processor polls every 50ms, invokes the handler with changes, and only advances the checkpoint when the handler calls `checkpointAsync()`. If the handler throws, the checkpoint is not advanced and the same batch is redelivered.

**Impact:** None — behaviour matches real Cosmos DB.

**Test:** `ChangeFeed_ManualCheckpoint_InvokesHandler`

---

### 24. Vector Search — No Index Policy or Dimension Limits

**Real Cosmos DB:** Requires a `vectorEmbeddings` container policy (path, dataType, dimensions, distanceFunction) and optionally a `vectorIndexes` indexing policy (flat, quantizedFlat, diskANN). Flat index limited to 505 dimensions; quantizedFlat/diskANN to 4096. `TOP N` is recommended with `ORDER BY VectorDistance`.

**InMemoryContainer:** No vector policy or index configuration needed. `VECTORDISTANCE` works immediately with brute-force exact computation. No dimensionality limits (tested up to 2000). No `TOP N` requirement.

**Impact:** Low. The emulator gives correct distance/similarity results. The only difference is that real Cosmos DB requires upfront configuration and has performance-related constraints that don't apply to in-memory testing.

**Tests:** `VectorDistance_RequiresVectorPolicy_InRealCosmos` (skipped), `VectorDistance_FlatIndexMax505Dimensions_InRealCosmos` (skipped), `VectorDistance_RequiresTopNWithOrderBy_InRealCosmos` (skipped), `VectorDistance_FiveArgs_RealCosmosRejectsExtraArgs`, `VectorDistance_UnknownDistanceFunction_RealCosmosRejects`, `VectorDistance_ParameterizedQuery_RealCosmosSupports` (skipped), `VectorDistance_WithNaNInVector_RealCosmosBehaviour` (skipped)

### 24a. Vector Search — Infinity/NaN Guard

**Real Cosmos DB:** Vectors are bounded by dimension limits (max 4096) and typically contain normalised embeddings, so Infinity/NaN results are extremely unlikely.

**InMemoryContainer:** If extreme values (e.g. `1e308`) produce Infinity or NaN in dot product or euclidean distance calculations, the emulator returns `null` instead of a non-finite value. This prevents invalid JSON serialization.

**Impact:** Very low. Only affects non-real-world inputs with extreme magnitudes.

---

### 25. PITR — Multiple Restores Replay Full Change Feed

**Real Cosmos DB:** PITR creates a new account from continuous backup — you can't multi-restore on the same container.

**InMemoryContainer:** `RestoreToPointInTime` replays the in-memory append-only change feed. After restoring to T1 and making new writes, a second restore to T2 > T1 replays original entries between T1 and T2, resurrecting items the first restore removed.

**Impact:** Low. For isolated restore scenarios, call `ClearItems()` between restores.

**Tests:** `RestoreToPointInTime_PostRestoreWritesShouldNotAffectEarlierRestore` (skipped), `RestoreToPointInTime_PostRestoreWritesThenSecondRestore_ActualBehavior`

---


### 26. System Property Formats Are Synthetic

**Real Cosmos DB:** Every document includes `_rid` (resource ID), `_self` (resource link), and `_attachments` (attachment link) system properties with specific internal formats.

**InMemoryContainer (v2.0.57+):** All five system properties are present: `_ts`, `_etag`, `_rid`, `_self`, and `_attachments`. However, formats are synthetic:
- `_rid` — base64-encoded GUID (real Cosmos uses a hierarchical resource ID)
- `_self` — synthetic URI path `dbs/db/colls/col/docs/{id}` (real Cosmos encodes resource IDs)
- `_attachments` — always `"attachments/"` (matches real Cosmos)

**Impact:** Low. Only matters if your code parses `_rid` or `_self` format. Equality checks and presence checks work correctly.

**Tests:** `SystemProperties_AllPresent_OnDocuments`

---

### 27. Session Token Format Is Synthetic

**Real Cosmos DB:** Session tokens encode partition key range IDs and logical sequence numbers (e.g. `0:-1#12345`).

**InMemoryContainer:** Session tokens use format `0:<hex-guid>` -- structurally valid but not partition-aware.

**Impact:** Low. Only matters if your code parses session token internals.

**Test:** `SessionToken_IsSyntheticGuidFormat`

---

### 28. Diagnostics Object Is a Mock

**Real Cosmos DB:** `CosmosDiagnostics` contains detailed timing, request latency, endpoint information, and retry details.

**InMemoryContainer:** Returns a shared NSubstitute mock with `GetClientElapsedTime()` returning `TimeSpan.Zero` and an empty `ToString()`.

**Impact:** Low. Only matters if your code inspects diagnostics for observability.

**Test:** `Diagnostics_ReturnsMock_EmptyToString`

---

### 29. Error Sub-Status Codes Always Zero

**Real Cosmos DB:** Sub-status codes provide fine-grained error classification (e.g. 1001 for timeout, 1003 for rate limiting).

**InMemoryContainer:** All `CosmosException` instances have `SubStatusCode = 0`.

**Impact:** Low. Only matters if your error handling logic branches on sub-status codes.

**Test:** `CosmosException_SubStatusCode_AlwaysZero`

---

### ~~30. Database Property Returns New Mock Each Access~~ (FIXED in v2.0.54)

**Fixed:** `InMemoryContainer.Database` now returns the same cached instance on every access. When created via `InMemoryDatabase`, `container.Database.Id` returns the parent database name.

**Test:** `Database_ReturnsCachedInstance`

---

### 31. Change Feed Processor Delivers All Versions (Not Latest Only)

**Real Cosmos DB:** Change feed processors in latest-version mode deliver only the final version of each item per poll cycle, deduplicating intermediate updates.

**InMemoryContainer:** All processor types internally use `GetChangeFeedIterator<T>(checkpoint)` which returns the checkpoint-based all-versions path, delivering intermediate versions that a real latest-version processor would not deliver.

**Impact:** Low. Handlers should be designed for idempotent processing per Cosmos DB's "at least once" delivery guarantee regardless.

**Test:** `ChangeFeed_Processor_DeliversAllVersions_IncludingIntermediates` (sister), `ChangeFeed_Processor_DeliversOnlyLatestVersion` (skipped)

---

### 32. Change Feed `_lsn` Property Not Present

**Real Cosmos DB:** Includes `_lsn` (logical sequence number / batch ID) in change feed items.

**InMemoryContainer:** Does not include the `_lsn` property. The checkpoint-based iterator uses an integer offset instead.

**Impact:** Low. `_lsn` is rarely used by application code.

**Test:** N/A (documented only)

---

### 33. TTL Eviction Does Not Produce Change Feed Entries

**Real Cosmos DB:** TTL eviction may produce change feed entries (depending on the mode).

**InMemoryContainer:** TTL eviction is lazy (items removed on next read access). TTL-evicted items silently disappear without a tombstone in the change feed.

**Impact:** Low. Most applications do not rely on TTL eviction appearing in the change feed.

**Test:** `ChangeFeed_TTL_ExpiredItem_SilentlyRemoved_NoChangeFeedEntry` (sister), `ChangeFeed_TTL_Eviction_RecordsDeleteInChangeFeed` (skipped)

---

### 34. Stream Change Feed Iterator Eager Evaluation

**Real Cosmos DB:** Change feed iterators (both typed and stream) return changes that occur after the iterator is created when using `ChangeFeedStartFrom.Now()`.

**InMemoryContainer:** `GetChangeFeedStreamIterator(ChangeFeedStartFrom.Now())` evaluates eagerly (captures snapshot at creation time). Items added after creation are NOT visible. `GetChangeFeedIterator<T>(ChangeFeedStartFrom.Now())` uses lazy evaluation and DOES see post-creation items.

**Impact:** Low. Only affects code that uses the stream variant of the change feed iterator with `Now()` start and expects to see items added after iterator creation.

**Test:** `ChangeFeed_Now_StreamIterator_EagerSnapshot_Sister` (sister), `ChangeFeed_Now_ThenAddItems_StreamIterator_MayNotSeeNewItems` (skipped)

---

### 35. DISTINCT + ORDER BY Combination via FakeCosmosHandler

**Real Cosmos DB:** The SDK's merge-sort pipeline for cross-partition queries requires a specific document wrapping format that combines `distinctType: Ordered` with `orderByItems` metadata.

**FakeCosmosHandler:** The query plan correctly reports both DISTINCT and ORDER BY, but the document wrapping format may not match the exact SDK expectations for the combined case.

**Impact:** Low. DISTINCT + ORDER BY works correctly when queries run directly against `InMemoryContainer`. The limitation only affects the `FakeCosmosHandler` HTTP pipeline.

**Test:** `Handler_DistinctWithOrderBy_Divergent_WorksViaContainerDirectly` (sister), `Handler_DistinctWithOrderBy_ReturnsOrderedDistinctValues` (skipped)

---

### 36. Multiple Aggregates in a Single SELECT via FakeCosmosHandler

**Real Cosmos DB:** The SDK reconstructs rows from partial aggregates returned by multiple partitions. The document wrapping format for multi-aggregate queries (e.g. `SELECT COUNT(1) as cnt, SUM(c.value) as total`) is undocumented.

**FakeCosmosHandler:** Single aggregates (COUNT, SUM, AVG, MIN, MAX) work correctly through the handler, but combining multiple aggregates in one SELECT may fail due to document wrapping.

**Impact:** Low. Multi-aggregate queries work correctly when run directly against `InMemoryContainer`. The limitation only affects the `FakeCosmosHandler` HTTP pipeline.

**Test:** `Handler_MultipleAggregates_Divergent_WorksViaContainerDirectly` (sister), `Handler_MultipleAggregates_ReturnsAllValues` (skipped)

---

### 37. Change Feed Processor Context FeedRange Always Returns PartitionKey.None

**Real Cosmos DB:** `ChangeFeedProcessorContext.FeedRange` returns the `FeedRangeEpk` of the specific partition range being processed by the lease.

**InMemoryContainer:** Always returns `FeedRange.FromPartitionKey(PartitionKey.None)` since the emulator uses a single-lease model without partition-level tracking.

**Impact:** Low. Only affects code that inspects `context.FeedRange` inside a change feed processor handler to determine partition affinity.

**Test:** `ChangeFeed_Processor_Context_FeedRange_EmulatorBehavior_AlwaysReturnsNone` (sister), `ChangeFeed_Processor_Context_FeedRange_ReflectsActualProcessingRange` (skipped)

---

### 38. FeedRange.FromPartitionKey() Returns All Items

**Real Cosmos DB:** `FeedRange.FromPartitionKey(pk)` creates a FeedRange scoped to the single partition containing that key. Using it with `GetItemQueryIterator` or `GetChangeFeedIterator` returns only items in that partition.

**InMemoryContainer:** `FeedRange.FromPartitionKey(pk)` produces PK-based JSON (`{"PK":["pk-value"]}`) which `ParseFeedRangeBoundaries` does not recognise (it expects EPK format `{"Range":{"min":"...","max":"..."}}`). The FeedRange falls back to returning all items, effectively ignoring the partition scoping.

**Workaround:** Use `QueryRequestOptions { PartitionKey = new PartitionKey("pk-5") }` instead of `FeedRange.FromPartitionKey()` when querying a single partition.

**Test:** `QueryIterator_FeedRangeFromPartitionKey_EmulatorBehavior_ReturnsAllItems` (sister), `QueryIterator_FeedRangeFromPartitionKey_ScopesToSinglePartition` (skipped)

---

### 39. Full-Text Search Uses Naive Substring Matching (No Stop Words, No BM25)

**Real Cosmos DB:** Full-text search functions (`FullTextContains`, `FullTextContainsAll`, `FullTextContainsAny`, `FullTextScore`) use a text analysis pipeline that tokenizes text, removes stop words (e.g., "the", "is", "and"), and scores results using BM25 ranking. Searching for a stop word may return no results.

**InMemoryContainer:** All full-text functions use simple case-insensitive `string.Contains()` substring matching. `FullTextScore` counts non-overlapping occurrences via `IndexOf` (naive term frequency, not BM25). Stop words are not removed — searching for "the" matches any text containing that substring.

**Workaround:** For BM25 ranking or stop-word behaviour, test with the real Cosmos DB. The emulator is suitable for testing query structure and basic filtering logic.

**Test:** `FullTextContains_StopWords_EmulatorMatchesAll` (sister), `FullTextContains_StopWordRemoval_ShouldIgnoreCommonWords` (skipped)

---

### 40. Trigger PatchItemAsync Support

**Real Cosmos DB:** Triggers can fire on `PatchItemAsync` operations.

**InMemoryContainer:** `PatchItemAsync` does not call `ExecutePreTriggers`/`ExecutePostTriggers`. Triggers specified in `PatchItemRequestOptions` are silently ignored.

**Impact:** Low. Most trigger use-cases target Create/Replace/Delete operations. If your code relies on triggers firing during patch operations, test with the real Cosmos DB.

**Test:** `DivergentBehavior_PatchIgnoresTriggers` (sister), `Trigger_Js_PatchOperation_FiresTrigger` (skipped)

---

### 41. Trigger TransactionalBatch Support

**Real Cosmos DB:** Triggers can fire on `TransactionalBatch` operations.

**InMemoryContainer:** `TransactionalBatch` does not support `Pre`/`PostTriggers` options. Triggers are not invoked for batch operations.

**Impact:** Low. Batch operations are typically used for multi-item atomic writes without trigger-based side effects.

**Test:** `DivergentBehavior_TransactionalBatch_IgnoresTriggers` (sister), `Trigger_Js_TransactionalBatch_NotSupported` (skipped)

---

### 42. Change Feed Not Rolled Back on Post-Trigger Failure

**Real Cosmos DB:** A failed post-trigger means the transaction did not commit, so the change feed does not record it.

**InMemoryContainer:** Change feed entries are recorded before post-trigger execution. If the post-trigger fails and the write is rolled back, the change feed still contains an orphan entry for the failed operation.

**Impact:** Low. Change feed consumers should be idempotent. This only matters if your code asserts that failed post-triggers leave no change feed footprint.

**Test:** `DivergentBehavior_ChangeFeed_NotRolledBack_OnPostTriggerFailure` (sister), `ChangeFeed_RolledBack_OnPostTriggerFailure` (skipped)

---
## Geospatial Functions

All six geospatial functions are implemented with real geometric calculations:

| Function | Implementation |
|----------|---------------|
| `ST_DISTANCE(a, b)` | Haversine formula (metres) |
| `ST_WITHIN(point, region)` | Point-in-polygon (ray casting) and point-in-circle (haversine radius) |
| `ST_INTERSECTS(geo1, geo2)` | Point-point, point-polygon, and polygon-polygon overlap detection |
| `ST_ISVALID(geojson)` | Full GeoJSON structure validation (Point, Polygon, LineString, MultiPoint) |
| `ST_ISVALIDDETAILED(geojson)` | Returns `{ valid, reason }` with specific validation error messages |
| `ST_AREA(polygon)` | Spherical excess formula |

Results are geometrically correct but may differ slightly from Cosmos DB's exact values due to different calculation methods. For precision-critical geospatial testing, use the real Cosmos DB. See [SQL Queries — Geospatial Functions](SQL-Queries#geospatial-functions) for the full function reference and [Features — Geospatial Functions](Features#geospatial-functions) for usage examples.

---

## Request Charges (RU)

All operations return a synthetic request charge of `1.0 RU`. There is no attempt to simulate real RU consumption. If your code makes decisions based on RU consumption, test with the real Cosmos DB. See [Feature Comparison](Feature-Comparison-With-Alternatives) for a side-by-side feature matrix.

---

## Consistency Levels

All operations execute with immediate consistency (equivalent to `Strong` in real Cosmos DB). There is no simulation of eventual consistency, session consistency, or stale reads. If your code is sensitive to consistency level behaviour, test with the real Cosmos DB. See [Feature Comparison](Feature-Comparison-With-Alternatives) for a side-by-side feature matrix.

---

### 43. Partition Key Type Discrimination

**Real Cosmos DB:** Numeric, string, and boolean partition keys are stored as distinct types. `PartitionKey(42)` and `PartitionKey("42")` are different partitions.

**InMemoryContainer:** `PartitionKeyToString` converts all PK values to their string representation, losing type information. `PK(42)` and `PK("42")` both become `"42"`, causing collision.

**Impact:** Medium. Workloads using numeric or boolean PKs alongside string PKs with the same textual representation will silently collide.

**Test:** `PartitionKey_NumericVsString_EmulatorBehavior_Collides` (sister), `PartitionKey_NumericVsString_ShouldBeDistinct` (skipped)

---

### 44. PK Mismatch Validation

**Real Cosmos DB:** Returns HTTP 400 when the explicit partition key parameter differs from the partition key value in the document body.

**InMemoryContainer:** Silently accepts the mismatch and uses the explicit PK parameter for storage. The document body retains its original PK value.

**Impact:** Low. Most code passes matching PK values. If your code validates PK consistency, test with the real Cosmos DB.

**Test:** `Create_PkMismatchWithBody_EmulatorBehavior_SilentlyAccepts` (sister), `Create_PkMismatchWithBody_ShouldThrowBadRequest` (skipped)

---

### 45. Patch Partition Key Field Modification

**Real Cosmos DB:** Returns HTTP 400 when a patch operation modifies the partition key field.

**InMemoryContainer:** Silently allows patching the PK field. The document body changes but the storage key still uses the original PK.

**Impact:** Low. Most code does not patch PK fields. If your code relies on this validation, test with the real Cosmos DB.

**Test:** `Patch_ModifyingPartitionKeyField_EmulatorBehavior_SilentlyModifies` (sister), `Patch_ModifyingPartitionKeyField_ShouldThrowBadRequest` (skipped)

---

### 46. Partition Key Size Limit

**Real Cosmos DB:** Partition key values are limited to 2KB. Larger values are rejected.

**InMemoryContainer:** No PK size validation. Arbitrary-length PK values are accepted.

**Impact:** Low. Most PK values are small strings.

**Test:** `PartitionKey_MaxSize_2KB_EmulatorBehavior_Accepts` (sister), `PartitionKey_MaxSize_2KB_ShouldReject` (skipped)

---

### 47. SQL Function Arg-Order & Undefined Gaps

**Real Cosmos DB:** `StringJoin(array, separator)` takes the array first. `ToNumber('abc')` returns `undefined` (property omitted). `ARRAY_CONTAINS([1,null,3], null)` returns `true`.

**InMemoryContainer:** Prior to v2.0.36, `StringJoin` had swapped args (`separator, array`). `ToNumber` on invalid strings returned `null` instead of `undefined`. `ARRAY_CONTAINS` returned `false` when searching for `null`.

**Impact:** Low. Fixed in v2.0.36. Documented for historical reference.

**Tests:** `StringJoin_JoinsArrayWithSeparator`, `ToNumber_InvalidString_ReturnsUndefined`, `ArrayContains_NullElement_MatchesNull`

---

### 48. Gateway Query Plan — COUNT(DISTINCT) / dCountInfo Not Supported

**Real Cosmos DB:** `COUNT(DISTINCT c.field)` queries produce a `dCountInfo` field in the query plan containing the distinct expression. The SDK uses this to set up a dedicated DCOUNT pipeline accumulator.

**InMemoryContainer:** Does not produce `dCountInfo`. The `COUNT` aggregate is detected but without distinct semantics. `COUNT(DISTINCT ...)` queries may return incorrect results when executed through `FakeCosmosHandler` on non-Windows platforms.

**Impact:** Low. Only affects `COUNT(DISTINCT ...)` queries routed through the gateway query plan endpoint (non-Windows).

**Tests:** `QueryPlan_CountDistinct_SetsDCountInfo` (skipped), `QueryPlan_CountDistinct_DivergentBehavior_NoDCountInfo` (sister)

---

### 49. Gateway Query Plan — hybridSearchQueryInfo Not Supported

**Real Cosmos DB:** Hybrid search queries (combining vector distance with full-text search via `ORDER BY RANK RRF(...)`) produce a `hybridSearchQueryInfo` field in the query plan.

**InMemoryContainer:** Does not produce `hybridSearchQueryInfo`. Hybrid search queries are not supported through the gateway query plan endpoint.

**Impact:** Low. Vector search and full-text search work independently.

**Tests:** `QueryPlan_HybridSearch_SetsHybridSearchQueryInfo` (skipped), `QueryPlan_HybridSearch_DivergentBehavior_IgnoredGracefully` (sister)

---

### 50. ReadMany — CancellationToken Not Observed

**Real Cosmos DB:** `ReadManyItemsAsync` and `ReadManyItemsStreamAsync` respect CancellationToken during network I/O, throwing `OperationCanceledException` if cancelled.

**InMemoryContainer:** CancellationToken is accepted but ignored. Operations execute synchronously via `Task.FromResult`.

**Impact:** Very low. Only affects code that relies on cancellation during ReadMany.

**Tests:** `ReadMany_CancelledToken_ThrowsOperationCanceledException` (skipped), `ReadMany_CancelledToken_StillReturnsResults_Divergent` (sister)

---

### 51. ReadMany — IfNoneMatchEtag Ignored

**Real Cosmos DB:** `ReadManyRequestOptions.IfNoneMatchEtag` returns 304 Not Modified when the composite response ETag matches.

**InMemoryContainer:** Does not compute composite response ETags. Always returns 200 OK with full result set.

**Impact:** Low. Conditional ReadMany reads are uncommon.

**Tests:** `ReadMany_WithIfNoneMatchEtag_Returns304WhenUnchanged` (skipped), `ReadMany_WithIfNoneMatchEtag_AlwaysReturns200_Divergent` (sister)

---

### 52. InMemoryFeedIterator — Offset-Based Continuation Tokens

**Real Cosmos DB:** Uses opaque JSON continuation tokens that encode internal state (partition range, logical offset, etc.).

**InMemoryFeedIterator:** Uses simple integer offset strings (e.g., "2", "4"). Code that parses continuation tokens expecting JSON structures will break.

**Impact:** Low. Production code should treat continuation tokens as opaque strings.

**Tests:** `InMemoryFeedIterator_ContinuationToken_IsOffsetBased_Divergent`

---

### 53. String.Length in LINQ WHERE Clauses

**Real Cosmos DB:** Cosmos SDK translates `d.Name.Length` to `LENGTH(c.name)` which is correctly evaluated server-side.

**InMemoryEmulator:** The SQL parser does not support `LENGTH()` in LINQ-generated SQL through the FakeCosmosHandler query pipeline.

**Impact:** Low. Use raw SQL `SELECT * FROM c WHERE LENGTH(c.name) > N` via `GetItemQueryIterator` as a workaround.

**Tests:** `ToFeedIterator_WithStringLength_FiltersCorrectly` (skipped), `ToFeedIterator_WithStringLength_Divergent_ReturnsAllItemsBecauseLengthIgnored` (sister)

---

### 54. GroupBy via LINQ

**Real Cosmos DB:** Cosmos SDK LINQ provider generates GROUP BY SQL with server-side aggregation.

**InMemoryEmulator:** The FakeCosmosHandler query path has limited GROUP BY support for LINQ-generated queries arriving through the real SDK HTTP path.

**Impact:** Medium. Use raw SQL `SELECT c.field, COUNT(1) FROM c GROUP BY c.field` via `GetItemQueryIterator` as a workaround.

**Tests:** `ToFeedIterator_WithGroupBy_GroupsCorrectly` (skipped), `ToFeedIterator_WithGroupBy_Divergent_ThrowsOrReturnsUngrouped` (sister)

---

### 55. Numeric Partition Keys via Real SDK

**Real Cosmos DB:** Supports numeric, boolean, and string partition key values natively.

**InMemoryEmulator:** The FakeCosmosHandler extracts partition keys from `x-ms-documentdb-partitionkey` header as JSON strings. Numeric PK values may not be parsed correctly through the real SDK HTTP path.

**Impact:** Low. Use string partition keys with numeric values as an alternative.

**Tests:** `NumericPartitionKey_CreatesAndQueriesCorrectly` (skipped), `NumericPartitionKey_Divergent_StringPkWorksAsAlternative` (sister)

---

### 56. DatabaseResponse / ContainerResponse Metadata

**Real Cosmos DB:** `DatabaseResponse` and `ContainerResponse` include full metadata: `RequestCharge`, `ActivityId`, `Diagnostics`, and populated `Headers`.

**InMemoryEmulator:** These responses are built via NSubstitute mocks that only set `StatusCode` and `Resource`. `RequestCharge`, `ActivityId`, `Diagnostics`, and `Headers` are not populated.

**Impact:** Low. Use stream APIs (`CreateDatabaseStreamAsync`, `ReadStreamAsync`, etc.) which do return proper headers.

**Tests:** `DatabaseResponse_HasRequestCharge_ActivityId_Diagnostics` (skipped), `DatabaseResponse_EmulatorBehavior_OnlyHasStatusCodeAndResource` (sister), `ContainerResponse_HasRequestCharge_ActivityId_Diagnostics` (skipped), `ContainerResponse_EmulatorBehavior_OnlyHasStatusCodeAndResource` (sister)

---

### 57. ETag Format — Random GUIDs vs Sequential Versions

**Real Cosmos DB:** ETags encode version numbers or timestamps providing monotonic ordering that can be used for conflict resolution.

**InMemoryEmulator:** ETags are random GUIDs per write. Optimistic concurrency (IfMatchEtag) works correctly, but ETags cannot be used for ordering or version comparison.

**Impact:** Low. Optimistic concurrency via IfMatchEtag/IfNoneMatchEtag works correctly.

**Tests:** `ETag_ShouldBeSequentialVersion_NotRandomGuid` (skipped), `ETag_EmulatorBehavior_IsRandomGuidPerWrite` (sister)

---

### 58. FeedResponse x-ms-item-count Header

**Real Cosmos DB:** `FeedResponse.Headers` includes `x-ms-item-count` indicating the number of items in the page.

**InMemoryEmulator:** Feed response headers include `x-ms-activity-id` and `x-ms-request-charge` but not `x-ms-item-count`. Use `FeedResponse.Count` instead.

**Impact:** Low. The `Count` property on `FeedResponse<T>` is populated correctly.

**Tests:** `FeedResponseHeaders_ShouldContainItemCount` (skipped), `FeedResponseHeaders_EmulatorBehavior_HasActivityIdAndRequestCharge` (sister)

---

### 59. CosmosException.Diagnostics

**Real Cosmos DB:** `CosmosException.Diagnostics` contains detailed diagnostic information about the failed operation.

**InMemoryEmulator:** `Diagnostics` is not populated on `CosmosException` instances. Other exception metadata (`StatusCode`, `ActivityId`, `RequestCharge`, `SubStatusCode`) are populated correctly.

**Impact:** Low. Use `StatusCode`, `ActivityId`, and `RequestCharge` for error handling.

**Tests:** `CosmosException_HasDiagnostics_OnError` (skipped), `CosmosException_HasDiagnostics_Divergent_DiagnosticsIsNull` (sister)

---

### 60. Static Session Token

**Real Cosmos DB:** Session tokens are monotonically increasing (e.g., `0:456#789`) and the SDK uses them for session consistency guarantees across reads and writes.

**InMemoryEmulator:** Always returns `"0:0#1"` as session token. The token does not progress across writes.

**Impact:** Low. Session consistency is not enforced; the static token satisfies SDK expectations for header presence.

**Tests:** `SessionToken_ShouldProgress_AcrossWrites` (skipped), `SessionToken_AlwaysReturnsStaticValue_Divergence` (sister)

---

### 61. Partition Key Range Assignment

**Real Cosmos DB:** Uses actual range-based partitioning with server-managed splits and merges.

**InMemoryEmulator:** Assigns partition key ranges via hash modulo. Queries return correct results, but range IDs and fan-out mechanics differ from real Cosmos DB.

**Impact:** Low. Cross-partition queries work correctly; only the internal range assignment mechanism differs.

**Tests:** `MultiPartition_FanOut_QueryExecutesAcrossAllRanges` (skipped), `MultiPartition_FanOut_UsesSimplifiedHashModulo_Divergence` (sister)

---

### 62. Account Metadata queryEngineConfiguration

**Real Cosmos DB:** Returns configuration reflecting actual query engine limits and capabilities for the account.

**InMemoryEmulator:** Returns hardcoded permissive configuration that allows all query features.

**Impact:** Low. All query patterns work; limits are not enforced.

**Tests:** `AccountMetadata_QueryEngineConfiguration_MatchesRealCosmos` (skipped), `AccountMetadata_QueryEngineConfiguration_IsPermissive_Divergence` (sister)

---

### 63. Collection Metadata Indexing Policy

**Real Cosmos DB:** Returns the actual configured indexing policy including spatial indexes, composite indexes, and excluded paths.

**InMemoryEmulator:** Returns a simplified permissive default that does not restrict any query patterns.

**Impact:** Low. All queries execute regardless of indexing configuration.

**Tests:** `CollectionMetadata_IndexingPolicy_MatchesRealCosmos` (skipped), `CollectionMetadata_IndexingPolicy_ReturnsPermissiveDefault_Divergence` (sister)

---

### 64. RegisterFeedIteratorSetup Asymmetry

**Behavior:** `InMemoryCosmosOptions.RegisterFeedIteratorSetup` is only used by `UseInMemoryCosmosDB<TClient>()` and `UseInMemoryCosmosContainers()`. It is ignored by `UseInMemoryCosmosDB()` because `FakeCosmosHandler` handles `.ToFeedIterator()` natively.

**Impact:** None. Setting the property on `UseInMemoryCosmosDB()` has no effect but doesn't cause errors.

**Tests:** `UseInMemoryCosmosDB_RegisterFeedIteratorSetup_HasNoEffect`

---

### 65. MaxItemCount=0 Returns All Items Instead of HTTP 400

**Behavior:** Setting `MaxItemCount=0` on `QueryRequestOptions` returns all items in one page. Real Cosmos DB returns HTTP 400 Bad Request for zero as an invalid page size.

**Impact:** None in practice — no production code intentionally sets `MaxItemCount=0`. The emulator treats any non-positive value as "return all items" via the `PageSize` property guard.

**Tests:** `MaxItemCount_Zero_ShouldReturn400_InRealCosmos` (skipped), `Divergent_MaxItemCount_Zero_ReturnsAllItems`

---

### 66. ORDER BY Null/Undefined Type Precedence

**Behavior:** When documents have missing or null fields, the emulator uses .NET's default `JToken` comparison for ORDER BY sorting. Real Cosmos DB follows a strict type-precedence: `undefined < null < false < true < numbers < strings`.

**Impact:** Low — queries with ORDER BY on fields that may be null/undefined could return items in a different order than real Cosmos DB.

**Tests:** `CrossPartitionOrderBy_NullOrdering_ShouldFollowCosmosSpec` (skipped), `Divergent_CrossPartitionOrderBy_NullsSortByDotNetDefault`

---

### 67. TTL Lazy Eviction (No Proactive Background Deletion)

**Behavior:** TTL eviction is lazy — expired items are only filtered out when a read or query accesses them. Real Cosmos DB proactively evicts expired items via a background process.

**Impact:** None for test scenarios — expired items are never returned from reads or queries. The only observable difference is that `ItemCount` may still include expired items until they are accessed.

**Tests:** `TTL_ProactiveEviction_ShouldDeleteWithoutRead` (skipped), `Divergent_TTL_LazyEviction_ItemVisibleUntilAccessed`

---

### 68. ImportState Missing "items" Key Silently Clears Container

**Behavior:** Calling `ImportState()` with valid JSON that lacks an `"items"` key (e.g., `{"foo":"bar"}`) clears all existing data and imports nothing. `ClearItems()` runs unconditionally before the `"items"` check.

**Impact:** Low — data loss risk if JSON structure is wrong. The behavior is documented by tests.

**Tests:** `ImportState_MissingItemsKey_ClearsContainer`, `ImportState_ItemsKeyIsNotArray_Behavior`

---

### 69. ImportState Missing "id" Field Defaults to Empty String

**Behavior:** If an imported item has no `"id"` field, the id defaults to `""`. Multiple items without `"id"` in the same partition key overwrite each other (last one wins). Real Cosmos DB requires an `"id"` field and returns 400 if missing.

**Impact:** Low — no production code omits the `"id"` field intentionally.

**Tests:** `ImportState_ItemsMissingId_UsesEmptyStringAsId`

---

### 70. ImportState Missing Partition Key Falls Back to ID

**Behavior:** When an imported item lacks the partition key field, the emulator's `ExtractPartitionKeyValue` falls back to using the document's `id` value as the partition key. Real Cosmos DB stores such items under `PartitionKey.Null`.

**Impact:** Low — affects only edge cases where documents genuinely lack their partition key field.

**Tests:** `ImportState_ItemMissingPartitionKeyField_Behavior`

---

### 71. Export During Concurrent Writes Not Atomic

**Behavior:** `ExportState()` enumerates a `ConcurrentDictionary` which provides a point-in-time snapshot of keys but may include some concurrent writes. The export is not a perfect atomic snapshot.

**Impact:** None for typical test scenarios — concurrent export+writes is an unusual pattern.

**Tests:** `ExportState_DuringConcurrentWrites_MayNotBeAtomicSnapshot`

---

### 72. Stored Procedure CRUD Metadata and C# Handlers Are Independent Stores

**Behavior:** `CreateStoredProcedureAsync` creates metadata in `_storedProcedureProperties`; `RegisterStoredProcedure` registers a C# handler in `_storedProcedures`. These are independent stores. A handler can exist without metadata (404 on Read but Execute works) and vice versa (Read works but Execute returns default). `DeleteStoredProcedureAsync` removes from both stores, but `DeregisterStoredProcedure` only removes the handler.

**Impact:** Low — this design intentionally separates CRUD metadata from execution logic, giving test authors flexibility.

**Tests:** `StoredProcedureDualStoreTests` (7 tests)

---

### 73. Stored Procedures Use C# Handlers, Not JavaScript

**Behavior:** `CreateStoredProcedureAsync` stores JavaScript body as metadata but does not interpret or execute it. Use `RegisterStoredProcedure()` to provide a C# handler invoked by `ExecuteStoredProcedureAsync<string>`.

**Impact:** Medium — this is a fundamental design choice. For JavaScript trigger execution, use the `CosmosDB.InMemoryEmulator.JsTriggers` package.

**Tests:** `ExecuteStoredProcedure_JavaScriptBody_ShouldExecute` (skip), `ExecuteStoredProcedure_WithCSharpHandler_ExecutesLogicInstead` (sister)

---

### 74. ExecuteStoredProcedureAsync Only Mocked for `<string>`

**Behavior:** Only `ExecuteStoredProcedureAsync<string>` is mocked. For other result types, deserialize from the string result: `JsonConvert.DeserializeObject<MyType>(response.Resource)`.

**Impact:** Low — workaround is straightforward.

**Tests:** `ExecuteStoredProcedure_NonStringGenericType_ShouldDeserialize` (skip), `ExecuteStoredProcedure_StringWithManualDeserialization_Workaround` (sister)

---

### 75. Stored Procedure Stream Variants Not Implemented

**Behavior:** `ExecuteStoredProcedureStreamAsync`, `CreateStoredProcedureStreamAsync`, `ReadStoredProcedureStreamAsync`, `ReplaceStoredProcedureStreamAsync`, `DeleteStoredProcedureStreamAsync` are not mocked on the NSubstitute Scripts proxy. Use the typed CRUD variants.

**Impact:** Low — typed variants provide equivalent functionality.

**Tests:** `ExecuteStoredProcedureStreamAsync_ShouldReturnStream` (skip), `CrudStreamVariants_ShouldWork` (skip)

---

### 76. No Partition Scoping for Stored Procedure Handlers

**Behavior:** Real Cosmos DB stored procedures can only access documents within the specified partition key. The emulator's C# handlers have unrestricted container access via closure. Workaround: scope your handler's queries using `QueryRequestOptions.PartitionKey`.

**Impact:** Medium — handlers can accidentally cross partition boundaries. Use the scoping pattern shown in tests.

**Tests:** `StoredProcedure_CrossPartition_ShouldBeScoped` (skip), `StoredProcedure_CrossPartition_EmulatorWorkaround_ScopeManually` (sister)

---

### 77. No Execution Timeout or Response Size Limits for Stored Procedures

**Behavior:** Real Cosmos enforces a 10-second timeout and 2MB response limit. The emulator does not enforce these constraints.

**Impact:** Low — primarily affects load/stress testing scenarios.

**Tests:** `ExecuteStoredProcedure_10SecondTimeout_ShouldThrow` (skip), `ExecuteStoredProcedure_2MBResponseLimit_ShouldFail` (skip)

---

### 78. Handler Exceptions Propagate Directly

**Behavior:** In real Cosmos DB, stored procedure errors are wrapped in `CosmosException` (400 Bad Request). The emulator propagates the raw handler exception. To simulate Cosmos error behavior, throw `CosmosException` explicitly from your handler.

**Impact:** Low — gives test authors full control over exception types.

**Tests:** `ExecuteStoredProcedure_HandlerError_ShouldReturn400` (skip), `ExecuteStoredProcedure_HandlerThrowsCosmosException_PropagatesWithStatusCode` (sister)

---

### 79. UDF Read/Replace/Delete Not Implemented

**Behavior:** Only `CreateUserDefinedFunctionAsync` is mocked on the Scripts proxy. Read, Replace, and Delete operations for UDFs are not available.

**Impact:** Low — UDFs can be registered/deregistered via `RegisterUdf`/`DeregisterUdf` for functional testing.

---

### 80. Stream API ErrorMessage Not Set

**Behavior:** Error `ResponseMessage` objects from stream API calls do not set `ErrorMessage`. Real Cosmos DB includes human-readable error descriptions. Callers should rely on `StatusCode` for error handling.

**Impact:** None for typical test scenarios — `StatusCode` is the standard way to detect errors.

**Tests:** `Stream_ErrorResponse_ContainsErrorMessage` (skip), `Divergent_Stream_ErrorResponse_ErrorMessageIsNull` (sister)

---

### 81. Stream Stored Procedure Variants Not Implemented

**Behavior:** `ExecuteStoredProcedureStreamAsync`, `CreateStoredProcedureStreamAsync` and other stream CRUD variants for stored procedures are not mocked. Use the typed CRUD variants.

**Impact:** Low — typed variants provide equivalent functionality.

**Tests:** `ExecuteStoredProcedureStreamAsync_ShouldReturnStream` (skip), `CrudStreamVariants_ShouldWork` (skip)

### 82. ARRAY_CONTAINS Does Case-Insensitive String Comparison

**Behavior:** The emulator's `ARRAY_CONTAINS` compares string elements case-insensitively. Real Cosmos DB uses case-sensitive ordinal comparison, so `ARRAY_CONTAINS(["Alice"], "alice")` returns false in production but true in the emulator.

**Impact:** Low — most arrays contain non-string elements or exact-case matches.

**Tests:** `Query_ArrayContains_StringElement_IsCaseSensitive` (skip)

### 83. GROUP BY Does Not Support Function Expressions

**Behavior:** The emulator does not support function calls in `GROUP BY` clauses (e.g., `GROUP BY LOWER(c.name)`). Real Cosmos DB evaluates the function per document and groups by the computed value.

**Impact:** Low — workaround is to use a subquery or compute the value in the SELECT clause with a field alias.

**Tests:** `Query_GroupByLower_MergesCaseVariants` (skip)

### 84. Transactional Batch Response Diagnostics Returns Zero Elapsed Time

**Behavior:** `response.Diagnostics.GetClientElapsedTime()` always returns `TimeSpan.Zero` because there is no real network I/O. Real Cosmos DB includes per-operation timing and retry details.

**Impact:** Low — most users don't access diagnostics on batch responses.

**Tests:** `Batch_Diagnostics_ContainsRequestLatency` (skip), `Batch_Diagnostics_InMemory_ReturnsZeroElapsedTime` (sister)

### 85. Transactional Batch ActivityId Is Synthetic

**Behavior:** `response.ActivityId` returns a fixed GUID (`00000000-...`). Real Cosmos DB returns a unique GUID per request.

**Impact:** Very low — ActivityId is used for diagnostics/support, not functionality.

**Tests:** `Batch_Response_ActivityId_IsPopulated` (passes — verifies non-null)

### 86. Transactional Batch RequestCharge Is Always 1.0 RU

**Behavior:** `response.RequestCharge` always returns `1.0` regardless of operation count or type. Real Cosmos DB charges ~5.3 RU per create, ~1 RU per read, summed per batch.

**Impact:** Low — only affects RU estimation/budgeting logic.

**Tests:** `Batch_RequestCharge_ScalesWithOperationCount` (skip), `Batch_RequestCharge_InMemory_AlwaysReturns1RU` (sister)

### 87. Transactional Batch Headers Session Tokens Are Synthetic

**Behavior:** Batch response headers don't include real session tokens. Real Cosmos DB returns per-operation LSN-based session tokens.

**Impact:** Low — session tokens are used for consistency guarantees not applicable in-memory.

**Tests:** `Batch_Headers_ContainSessionToken` (skip)

### 88. Transactional Batch Does Not Validate PK Mismatch

**Behavior:** The emulator does not validate that the document's partition key value matches the batch's `PartitionKey`. Real Cosmos DB returns 400 BadRequest on mismatch. The emulator stores the item using the batch's partition key.

**Impact:** Medium — could mask bugs where document PK differs from batch PK.

**Tests:** `Batch_PartitionKeyMismatch_Document_Vs_BatchPK_ThrowsBadRequest` (skip), `Batch_PartitionKeyMismatch_InMemory_UsesDocumentPK` (sister)

### 89. Transactional Batch Not Isolated From Concurrent Direct CRUD

**Behavior:** The emulator's batch uses snapshot/restore for atomicity but doesn't hold a global lock during execution. Concurrent direct CRUD can see intermediate batch state.

**Impact:** Low — only relevant in multi-threaded test scenarios mixing batch + direct CRUD.

**Tests:** `Batch_ConcurrentBatchAndDirectCrud_IsolationGuaranteed` (skip)

### 90. Pre-Trigger Exceptions Not Wrapped as CosmosException

**Behavior:** When a C# pre-trigger handler throws an exception, the emulator propagates it as-is (e.g. `InvalidOperationException`). Real Cosmos DB wraps trigger failures in a `CosmosException` with a `BadRequest` status code.

**Impact:** Low — test code should catch the specific exception type thrown by the handler rather than relying on `CosmosException` wrapping.

**Tests:** `PreTrigger_ThrowingHandler_AbortsCreate`, `PreTrigger_ThrowingHandler_AbortsUpsert`, `PreTrigger_ThrowingHandler_AbortsReplace`

### 91. CreateItemAsync Response Does Not Reflect Pre-Trigger Mutations

**Behavior:** When a pre-trigger modifies the document (e.g. adds a field), the stored document correctly includes the modification, but the `ItemResponse<T>.Resource` returned by `CreateItemAsync` reflects the original unmodified input. Real Cosmos DB returns the post-trigger document in the response.

**Impact:** Low — read the item back via `ReadItemAsync` to see trigger modifications.

**Tests:** `RegisterTrigger_SameIdTwice_OverwritesHandler`, `RegisterTrigger_CaseSensitive_DifferentTriggers`, `DeregisterTrigger_ThenReRegister_Works`

### 92. Patch Operation Does Not Support Triggers

**Behavior:** The emulator's `PatchItemAsync` does not execute pre-triggers or post-triggers. Real Cosmos DB fires triggers on patch operations.

**Impact:** Medium — if your application relies on triggers firing during patch operations, this won't be tested.

**Tests:** `PatchItem_TriggersNotFired_Divergent` (skip), `PatchItem_InMemory_TriggersNotFired` (sister)

### 93. Change Feed Not Rolled Back on Post-Trigger Failure

**Behavior:** When a post-trigger fails and the write is rolled back, the change feed entry is NOT removed. Real Cosmos DB only records change feed entries for successfully committed writes.

**Impact:** Low — only affects tests that verify change feed entries after post-trigger rollback.

**Tests:** `PostTrigger_Rollback_ChangeFeedNotReverted_Divergent` (skip), `PostTrigger_Rollback_ChangeFeedNotReverted_InMemory` (sister)

### 94. GetTriggerQueryIterator Not Implemented

**Behavior:** The emulator does not implement `Scripts.GetTriggerQueryIterator()`. Calling it throws `NotImplementedException`.

**Impact:** Low — trigger enumeration is rarely needed in tests.

**Tests:** `GetTriggerQueryIterator_NotImplemented` (skip)

### 95. DefaultTimeToLive=0 Accepted (Real Cosmos Rejects It)

**Behavior:** Real Cosmos DB rejects `DefaultTimeToLive=0` with a 400 Bad Request. The emulator treats 0 as "TTL enabled, no default expiry" (same as -1). The API requires null (off), -1 (on, no default), or > 0.

**Impact:** Low — only relevant if testing container creation validation.

**Tests:** `ContainerTtl_ZeroDefault_ShouldReturn400` (skip), `ContainerTtl_ZeroDefault_EmulatorTreatsAsNoExpiration` (sister)

### 96. Per-Item _ttl=0 Causes Immediate Expiry (Real Cosmos Rejects It)

**Behavior:** Real Cosmos DB rejects `_ttl=0` — the value must be -1 or a positive integer. The emulator's `IsExpired()` treats `_ttl=0` as "elapsed >= 0" which is always true, causing immediate expiry.

**Impact:** Low — only relevant if testing per-item TTL validation edge cases.

**Tests:** `PerItemTtl_Zero_ShouldReturn400` (skip), `PerItemTtl_Zero_EmulatorExpiresImmediately` (sister)

### 97. Queries Don't Evict Expired Items From Memory

**Behavior:** When querying, the emulator filters out expired items but does NOT evict them from memory. Items remain in `_items`, `_etags`, and `_timestamps` until a direct CRUD operation triggers `EvictIfExpired()`. Real Cosmos DB has a background garbage collection process.

**Impact:** Low — memory usage may be slightly higher than expected with TTL-heavy workloads in tests.

**Tests:** `Query_ShouldEvictExpiredItemsFromMemory` (skip), `Query_EmulatorFiltersButDoesNotEvictExpiredItems` (sister)

### 98. UseInMemoryCosmosDB DI Doesn't Expose ContainerProperties Configuration

**Behavior:** The `AddContainer()` API on `InMemoryCosmosOptions` only accepts container name, partition key path, and optional database name. Unique key policies, indexing policies, and other `ContainerProperties` settings must be configured by capturing the backing `InMemoryContainer` via the `OnHandlerCreated` callback.

**Impact:** Low — advanced container properties can be set via the callback pattern.

**Tests:** `UniqueKeyPolicy_ViaWaf_NotExposedViaAddContainer` (skip)

### 99. Stored Procedure and Trigger Registration Requires Backing Container

**Behavior:** `RegisterStoredProcedure()` and `RegisterTrigger()` are `InMemoryContainer` extension methods, not available on the abstract `Container` class resolved from DI. Use `OnHandlerCreated` to capture `FakeCosmosHandler.BackingContainer`.

**Impact:** Low — the callback pattern provides access to the backing container for registration.

**Tests:** `StoredProcedure_ViaWaf_RequiresBackingContainerAccess` (skip), `Trigger_ViaWaf_RequiresBackingContainerAndHeaders` (skip)

### 100. Container Deletion Conflicts With DI Singleton Lifetime

**Behavior:** Deleting a container via `DeleteContainerAsync()` removes it from the in-memory database, but the DI container retains its reference to the now-deleted singleton. Subsequent DI resolutions return the same deleted container instance.

**Impact:** Low — container deletion is rarely needed in test scenarios.

**Tests:** `DeleteContainer_ViaWaf_DiStillHoldsReference` (skip)

### 101. FakeCosmosHandler Routes by Container Name Only

**Behavior:** `FakeCosmosHandler` routes requests by container name, not by database+container combination. Two containers with the same name in different databases would collide. Real Cosmos SDK sends requests to different endpoints based on database URI in the path.

**Impact:** Low — most test scenarios use unique container names.

**Tests:** `PerContainerDatabaseName_MultipleDbsSameContainerName_ShouldIsolate` (skip), `PerContainerDatabaseName_DifferentContainerNames_IsolatesCorrectly` (sister)

---

## Intentionally Out of Scope

These features require infrastructure that is fundamentally incompatible with an in-memory emulator. Each has a skipped test documenting why.

| Feature | Reason |
|---------|--------|
| Client encryption keys | Requires Azure Key Vault integration and MDE/Always Encrypted SDK internals |
