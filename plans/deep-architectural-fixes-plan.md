# Deep Architectural Fixes Plan — Remaining 153 Skipped Tests

## Executive Summary

After cataloging all 153 remaining skipped tests, the breakdown is:

| Classification | Count | Notes |
|---|---|---|
| **INTENTIONAL_DIVERGENCE** | 58 | By-design tradeoffs (synthetic RU, lazy TTL, etc.) — leave skipped |
| **BLOCKED** | 5 | SDK internals (ContainerInternal cast, encryption keys, CosmosDiagnostics) |
| **FIXABLE_SMALL** (< 20 LOC) | 7 | Quick wins for next batch |
| **FIXABLE_MEDIUM** (20-100 LOC) | 11 | Targeted feature additions |
| **FIXABLE_HARD** (100+ LOC / new abstractions) | 12 | Architectural work covered below |

**Maximum fixable: 30 tests** (7 small + 11 medium + 12 hard).
After those, the remaining 63 are either intentional divergences or blocked. These are the hard ceiling.

The 58 intentional divergences are deliberately kept. These represent conscious design decisions (synthetic RU charges, lazy TTL, LINQ permissiveness, etc.) where matching real Cosmos would either add complexity with no testing value, or make the emulator harder to use. Each one has a sister test documenting the actual behavior.

---

## Wave A — Quick Wins (7 tests, ~1 session)

These require < 20 lines each and can be bundled into the next commit.

| # | Test | File | Fix |
|---|------|------|-----|
| A1 | `GetDatabaseQueryIterator_WithMaxItemCount_Pages` | CosmosClientAndDatabaseTests:2780 | Pass `maxItemCount` from `QueryRequestOptions` to `InMemoryFeedIterator` constructor in `GetDatabaseQueryIterator` |
| A2 | `GetDatabaseQueryIterator_WithContinuationToken_Resumes` | CosmosClientAndDatabaseTests:2815 | Parse continuation token as integer offset, skip that many items |
| A3 | `UniqueKeyPolicy_ViaWaf_NotExposedViaAddContainer` | WAFIntegrationTests:1873 | Add `AddContainer(ContainerProperties)` overload to `UseInMemoryCosmosDB` builder |
| A4 | `StoredProcedure_OversizedResponse_RealCosmosWouldReject` | DocumentSizeLimitTests:1208 | Check already implemented in v2.0.59; likely stale skip — try unskip |
| A5 | `PostTrigger_InflatesDocumentPast2MB_RealCosmosWouldReject` | DocumentSizeLimitTests:1268 | Re-validate document size after post-trigger execution in `CreateItemAsync` before committing |
| A6 | `GetCurrentDateTime_ShouldReturnSameValueForAllRows` | DivergentBehaviorTests:96 | Snapshot `DateTime.UtcNow` once per query execution (add a thread-local/parameter in `EvaluateQuery`) |
| A7 | `SetDifference_NonExistentProperty_EmulatorReturnsEmptyArray` | ExtendedArrayFunctionTests:1242 | Skip reason says "now correctly returns undefined"; try unskip — likely already passing |

---

## Wave B — Targeted Feature Additions (11 tests, ~2-3 sessions)

Each needs 20-100 lines but is self-contained.

### B1. FeedRange.FromPartitionKey() support (1 test)
**Test:** `FeedRangeFilteringTests:1401`
**Fix:** In `ParseFeedRangeBoundaries`, detect `{"PK":[...]}` JSON format from `FeedRange.FromPartitionKey()`. Extract the PK value and filter items by exact partition key match instead of hash-range comparison.
**Effort:** ~30 lines in InMemoryContainer's feed range parsing.

### B2. Stream iterator lazy evaluation (2 tests)
**Tests:** `FeedRangeFilteringTests:1473`, `FeedRangeTests:967`
**Fix:** Refactor `InMemoryStreamFeedIterator` to use a `Func<IEnumerable<T>>` factory delegate (same pattern as `InMemoryFeedIterator<T>`). Defer snapshot capture to `ReadNextAsync()`.
**Effort:** ~60 lines refactoring InMemoryStreamFeedIterator.

### B3. Stream change feed pagination (1 test)
**Test:** `ChangeFeedTests:1931`
**Fix:** Implement `PageSizeHint` in `InMemoryStreamFeedIterator`. Track offset state, return at most N docs per page, update `HasMoreResults`.
**Effort:** ~40 lines.

### B4. AllVersionsAndDeletes typed iterator (1 test)
**Test:** `ChangeFeedTests:3666`
**Fix:** In typed `GetChangeFeedIterator<T>`, detect `ChangeFeedMode.AllVersionsAndDeletes` and switch to full-history enumeration (creates + updates + delete tombstones).
**Effort:** ~30 lines.

### B5. Change feed processor deduplication (1 test)
**Test:** `ChangeFeedTests:3703`
**Fix:** In `InMemoryChangeFeedProcessor`, deduplicate the batch by `id` before delivering to handler — keep only latest version per item.
**Effort:** ~20 lines.

### B6. VectorDistance parameterized queries (1 test)
**Test:** `VectorSearchTests:1782`
**Fix:** In the SQL parameter pipeline, handle array-type parameters (JArray) passed to `VectorDistance()` without string-serializing them.
**Effort:** ~25 lines.

### B7. FakeCosmosHandler WHERE delegation (1 test)
**Test:** `RealToFeedIteratorTests:1375` (LENGTH through handler)
**Fix:** Instead of simplifying SQL, pass full WHERE clauses (including function calls) to `InMemoryContainer` for evaluation. The container already supports all SQL functions.
**Effort:** ~50 lines refactoring query path in FakeCosmosHandler. Potentially unlocks many more passing queries through the handler path.

### B8. Hierarchical PK prefix queries (1 test)
**Test:** `PartitionKeyTests:1060`
**Fix:** Add prefix-match support for hierarchical partition key queries. When a partial key is provided (e.g., first 2 of 3 components), filter items matching that prefix.
**Effort:** ~40 lines.

### B9. Concurrent Patch Increment atomicity (1 test)
**Test:** `BulkOperationTests:1513`
**Fix:** Add per-item lock (`ConcurrentDictionary<string, SemaphoreSlim>`) in `PatchItemAsync` to serialize concurrent increments on the same item.
**Effort:** ~30 lines.

---

## Wave C — Architectural Work (12 tests, ~4-6 sessions)

These require new abstractions, packages, or significant refactoring.

### C1. JavaScript Stored Procedure Engine — New Optional Package (6 tests)

**Pattern:** Follows the exact same pattern as `CosmosDB.InMemoryEmulator.JsTriggers`.

**New package: `CosmosDB.InMemoryEmulator.JsStoredProcedures`**

| Component | Description |
|---|---|
| `ISprocEngine` interface (in core) | `string Execute(string jsBody, PartitionKey pk, dynamic[] args, ISprocContext context)` |
| `ISprocContext` interface (in core) | Provides `CreateDocument`, `ReadDocument`, `QueryDocuments`, `ReplaceDocument`, `DeleteDocument` — scoped to the partition key |
| `JintSprocEngine` (in new package) | Implements `ISprocEngine` using Jint. Wires up the Cosmos server-side API: `getContext()`, `getResponse()`, `getCollection()`, `console.log()` |
| `UseJsStoredProcedures()` extension | Sets `container.SprocEngine = new JintSprocEngine()` |

**Tests that would pass:**

| Test | What it needs |
|---|---|
| `ExecuteStoredProcedure_JavaScriptBody_ShouldExecute` (864) | Basic JS execution: `response.setBody(prefix + '-result')` |
| `ExecuteStoredProcedure_NonStringGenericType_ShouldDeserialize` (927) | Needs `ExecuteStoredProcedureAsync<T>` generic support — requires also fixing the NSubstitute mock to handle arbitrary `<T>` |
| `ExecuteStoredProcedureStreamAsync_ShouldReturnStream` (958) | Needs stream variant mock |
| `CrudStreamVariants_ShouldWork` (970) | Needs stream CRUD mocks on Scripts |
| `EnableScriptLogging_ShouldReturnLogs` (982) | Capture `console.log()` output, return via headers |
| `StoredProcedure_CrossPartition_ShouldBeScoped` (1064) | `ISprocContext` enforces partition scoping on collection queries |

**Cosmos JS Server-Side API to implement:**

```javascript
// Context
var context = getContext();
var response = context.getResponse();
var collection = context.getCollection();

// Response
response.setBody(value);       // set return value (already in triggers)

// Collection (scoped to partition)
collection.createDocument(selfLink, doc, options, callback);
collection.readDocument(docLink, options, callback);
collection.queryDocuments(selfLink, query, options, callback);
collection.replaceDocument(docLink, doc, options, callback);
collection.deleteDocument(docLink, options, callback);
collection.getSelfLink();

// Console
console.log(message);          // captured for x-ms-documentdb-script-log-results
```

**Architecture decision:** The `ISprocContext` wrapper provides a sandboxed view of `InMemoryContainer` scoped to the specified partition key. The JS engine receives this context, which prevents cross-partition access.

**Effort:** ~400-500 lines across 4 files (interface + engine + extension + container integration).

**Dependency:** Jint (already a dependency of JsTriggers; could be shared or duplicated).

**Key question:** Should JsTriggers and JsStoredProcedures share a single Jint-based package (`CosmosDB.InMemoryEmulator.JsEngine`) or remain separate? A unified package would:
- Avoid duplicating Jint wiring/sandboxing code
- Allow shared `getContext()`/`getCollection()` implementation
- Let users opt into "full JS support" with a single `container.UseJavaScript()` call
- Triggers + sprocs share the same server-side API surface

**Recommendation:** Create a **unified `CosmosDB.InMemoryEmulator.JsEngine` package** that replaces `JsTriggers` and adds sproc support. The extension becomes `container.UseJavaScript()` which enables both triggers and stored procedures. The existing `JsTriggers` package becomes a thin redirect/compat shim.

### C2. Enhanced JsTrigger Engine — Same Package (5 tests)

These are gaps in the existing JS trigger implementation that the unified engine would fix:

| Test | Feature needed |
|---|---|
| `PostTrigger_Js_SetBody_ModifiesResponse` (1422) | `response.setBody()` in post-triggers — change `ExecutePostTrigger` return type to `JObject?` |
| `PostTrigger_Js_GetRequest_Available` (1677) | Wire `getContext().getRequest()` in post-trigger context — pass original request body |
| `Trigger_Collection_Access` (1621) | Wire `getContext().getCollection()` for CRUD/queries — same `ISprocContext` used by sprocs |
| `PreTrigger_Js_ModifiesIdField_Undefined` (2827) | Compare pre/post trigger document — reject id changes |
| `PreTrigger_Js_ModifiesPartitionKey_Undefined` (2847) | Compare pre/post trigger document — reject PK changes |

**Shared with C1:** The `getCollection()` API is identical between sprocs and triggers. Building it for sprocs automatically enables it for triggers.

**Effort:** ~200 lines incremental on top of C1.

### C3. FakeCosmosHandler GROUP BY / DISTINCT+ORDER BY — Hybrid Approach (4 tests)

**Tests:**
- `FakeCosmosHandlerTests:924` — GROUP BY
- `FakeCosmosHandlerTests:1993` — DISTINCT + ORDER BY
- `FakeCosmosHandlerTests:2026` — Multiple aggregates
- `RealToFeedIteratorTests:2035` — LINQ GroupBy via SDK

**Problem:** The Cosmos SDK's internal query pipeline stages (`GroupByQueryPipelineStage`, `OrderByQueryPipelineStage`, `AggregateQueryPipelineStage`) expect responses in specific **undocumented wire formats**. The handler already wraps ORDER BY results correctly, but GROUP BY, multi-aggregate, and DISTINCT+ORDER BY are broken.

**Approach: Hybrid** (see `plans/c3-wire-format-analysis.md` for full analysis)

Use **query plan bypass** for GROUP BY and multi-aggregate (avoid complex undocumented wrapping), and a **minor wire format fix** for DISTINCT+ORDER BY (tweak to existing proven ORDER BY wrapping).

| Scenario | Strategy | Detail |
|----------|----------|--------|
| GROUP BY | Query plan bypass | Suppress `groupByExpressions`, `aggregates`, `groupByAliasToAggregateType` in query plan. `InMemoryContainer` evaluates GROUP BY fully; return pre-computed results. |
| Multi-aggregate | Query plan bypass | Suppress `aggregates` in query plan. `InMemoryContainer` evaluates aggregates fully; return pre-computed results. |
| DISTINCT + ORDER BY | Wire format fix | Fix `BuildOrderByRewrittenQuery` to use SELECT expression as payload instead of `c`. ~20 LOC tweak to existing working code. |
| LINQ GroupBy | Query plan bypass | Same as GROUP BY (LINQ generates GROUP BY SQL). |

**Implementation steps:**
1. In `HandleQueryPlanAsync`: detect GROUP BY / multi-aggregate queries → suppress pipeline flags (set `groupByExpressions=[]`, `aggregates=[]`, `groupByAliasToAggregateType={}`, `hasNonStreamingOrderBy=false` for GROUP BY)
2. In `BuildOrderByRewrittenQuery`: when DISTINCT is present, use SELECT expression(s) as payload instead of `c`
3. In `HandleQueryAsync`: ensure `SimplifySdkQuery` correctly passes through GROUP BY and multi-aggregate queries for direct execution by `InMemoryContainer`

**Effort:** ~40 lines. Low risk.
**Risk:** Low — bypass avoids undocumented wire formats; DISTINCT+ORDER BY fix extends proven code.

### C4. Undefined Propagation in SQL Functions (3 tests)

**Tests:**
- `ComputedPropertyTests:770` — undefined propagation through functions
- `ComputedPropertyTests:884` — CONCAT with undefined arg
- `ComputedPropertyTests:1473` — ARRAY_CONTAINS on CP result (related)

**Problem:** Real Cosmos DB distinguishes between `null` and `undefined` (absent property). When a function receives an undefined argument, it propagates undefined (the property is omitted from the result). The emulator evaluates to `null` instead.

**Fix:** Introduce a sentinel `UndefinedValue` type throughout the SQL evaluator. Every `EvaluateSqlFunction` path needs to check for undefined inputs and propagate appropriately.

**Effort:** ~200 lines touching ~120 SQL function evaluation branches. High risk of regressions.

---

## Wave D — Nice-to-Haves (remaining FIXABLE tests after C)

These are lower priority but could be added incrementally:

| Test | Fix | Effort |
|---|------|--------|
| `Query_GroupByLower_MergesCaseVariants` (1158) | Expression-based GROUP BY keys in SQL parser | Hard |
| `GroupBy_WithOrderByAggregate_SortsByAggregateValue` (4586) | Alias resolution in ORDER BY after aggregation | Medium |
| `TTL_ProducesChangeFeedDeleteEvent` (946) | Wire TTL eviction into `RecordDeleteTombstone()` | Medium |
| `ChangeFeed_FromNow_WithFeedRange` (2104) | Compose lazy evaluation with range filtering | Medium |
| `ChangeFeed_ContinuationToken` (2274) | Parse continuation token from internal SDK types via reflection | Hard |
| `PartitionKey_NumericVsString_Distinct` (850+878) | Type-preserving PK storage format | Hard |
| `Batch_Isolation_FromConcurrentReaders` (1589+1637) | Per-partition global locking | Hard |
| `FakeCosmosHandler db+container routing` (WAF:1811) | Compound routing key in handler | Medium |
| `ComputedProperty_SelectCStar` (844) | Parser handles `c.*` syntax | Medium |
| `ArrayContains_WithLiteralArray` (76) | Expression evaluation for function args | Medium |
| `ArrayContainsAny_WithObjectElement` (1357) | Inline object expression parsing | Medium |
| `FeedRange processor context` (1688) | Multi-lease processor simulation | Hard |
| `Null-coalescing operator test` (2139) | Just assert NotSupportedException | Small |

---

## Recommended Execution Order

### Phase 1: Quick Wins (Wave A) — Next commit
- 7 tests, ~1 session
- Low risk, high value
- Gets us from 153 → ~146 skipped

### Phase 2: Targeted Features (Wave B) — 2-3 commits
- 11 tests across 2-3 sessions
- Medium risk, high value
- Gets us from ~146 → ~135 skipped
- **B7 (FakeCosmosHandler WHERE delegation) is highest ROI** — unlocks future query-through-handler fixes

### Phase 3: Unified JS Engine (Wave C1 + C2) — 2-3 commits
- 11 tests (6 sproc + 5 trigger)
- Medium-high risk, very high value
- Gets us from ~135 → ~124 skipped
- **This is the most impactful architectural change.** A single `container.UseJavaScript()` call enables full JS execution for both triggers and stored procedures.
- Creates a reusable pattern for any future "server-side JS" needs

### Phase 4: FakeCosmosHandler Hybrid Fix (Wave C3) — 1 commit
- 4 tests, ~40 LOC
- **Hybrid approach:** query plan bypass for GROUP BY / multi-aggregate + wire format tweak for DISTINCT+ORDER BY
- Low risk — avoids undocumented wire formats; DISTINCT+ORDER BY extends proven ORDER BY code
- Gets us from ~124 → ~120 skipped
- See `plans/c3-wire-format-analysis.md` for detailed analysis of all approaches considered.

### Phase 5: Undefined Propagation (Wave C4) — 1 commit
- 3 tests
- Medium-high risk (touches many code paths)
- Consider only if users report null-vs-undefined issues

### Phase 6: Nice-to-Haves (Wave D) — ongoing
- 13+ tests
- Cherry-pick based on user demand

---

## Hard Ceiling Analysis

After all waves (A through D):

| Category | Count | Disposition |
|---|---|---|
| Fixable (A+B+C+D) | ~30 | Gets us from 153 → ~123 skipped |
| Intentional divergences | 58 | Stay skipped by design |
| Blocked (SDK internals) | 5 | Cannot fix without SDK changes |
| Duplicate/overlapping | ~60 | Tests that document the same divergence from different angles |

The **realistic floor is ~120-125 skipped tests**. Below that requires either:
- Changing fundamental design decisions (lazy TTL → proactive, LINQ-to-Objects → SQL translation)
- Implementing an RU cost model
- Implementing index enforcement
- Accessing SDK internal types

None of those are worth the complexity for an in-memory testing emulator.

---

## New Optional Package Design: `CosmosDB.InMemoryEmulator.JsEngine`

### Current State
```
CosmosDB.InMemoryEmulator (core)
├── IJsTriggerEngine.cs   → 2 methods: ExecutePreTrigger, ExecutePostTrigger
└── InMemoryContainer.cs  → public IJsTriggerEngine JsTriggerEngine { get; set; }

CosmosDB.InMemoryEmulator.JsTriggers (optional)
├── JintTriggerEngine.cs  → Jint-based implementation
└── JsTriggerExtensions.cs → container.UseJsTriggers()
```

### Proposed State
```
CosmosDB.InMemoryEmulator (core)
├── IJsEngine.cs          → Combined interface (replaces IJsTriggerEngine)
│   ├── ExecutePreTrigger(jsBody, document) → JObject
│   ├── ExecutePostTrigger(jsBody, document, requestBody?) → JObject?
│   ├── ExecuteStoredProcedure(jsBody, pk, args, context) → string
│   └── CapturedLogs { get; }
├── ISprocContext.cs       → Partition-scoped CRUD for JS sandbox
│   ├── CreateDocument(doc) → JObject
│   ├── ReadDocument(id) → JObject
│   ├── QueryDocuments(query) → IEnumerable<JObject>
│   ├── ReplaceDocument(id, doc) → JObject
│   ├── DeleteDocument(id) → void
│   └── SelfLink { get; }
└── InMemoryContainer.cs   → public IJsEngine JsEngine { get; set; }
                             └── InMemorySprocContext : ISprocContext (internal)

CosmosDB.InMemoryEmulator.JsEngine (replaces JsTriggers)
├── JintJsEngine.cs        → Jint-based implementation of IJsEngine
├── JsEngineExtensions.cs  → container.UseJavaScript()
└── Depends on: Jint 4.2.1

CosmosDB.InMemoryEmulator.JsTriggers (compat shim)
└── JsTriggerExtensions.cs → container.UseJsTriggers() calls container.UseJavaScript()
    (deprecated, points users to JsEngine package)
```

### Migration Path
1. `IJsTriggerEngine` → `IJsEngine` with backward-compatible methods
2. `UseJsTriggers()` continues to work, internally creates `JintJsEngine`
3. `UseJavaScript()` is the new unified entry point
4. Tests using `UseJsTriggers()` continue working without changes

### Key Design Decisions

**Q: Should `ISprocContext` expose raw `InMemoryContainer` or a restricted interface?**
A: Restricted interface. The context only allows CRUD within the specified partition key. This matches real Cosmos behavior where stored procedures are partition-scoped.

**Q: How do we handle `console.log()` capture for script logging?**
A: The `IJsEngine` interface exposes `CapturedLogs` property. After execution, `ExecuteStoredProcedureAsync` reads it and sets the `x-ms-documentdb-script-log-results` header.

**Q: What about the `<T>` generic type limitation for stored procedures?**
A: The JS engine always returns a string (JSON). The NSubstitute mock for `ExecuteStoredProcedureAsync<T>` deserializes the string result to `T` using `JsonConvert.DeserializeObject<T>()`. This requires replacing the current string-only mock with a generic-capable approach (possibly using `ForPartsOf<>` or a custom substitute).

**Q: Callback-based vs synchronous collection API?**
A: Real Cosmos uses callbacks (`collection.createDocument(link, doc, opts, callback)`). In Jint, we can wire these as synchronous calls that invoke the callback immediately, since everything is in-memory and synchronous. This avoids the complexity of async JS execution.

---

## Summary

| Wave | Tests Fixed | Cumulative Skipped | Effort | Risk |
|------|------------|-------------------|--------|------|
| A (Quick Wins) | 7 | ~146 | Low | Low |
| B (Targeted) | 11 | ~135 | Medium | Low-Medium |
| C1+C2 (JS Engine) | 11 | ~124 | High | Medium |
| C3 (Hybrid Fix) | 4 | ~120 | Low | Low |
| C4 (Undefined) | 3 | ~117 | High | Medium |
| D (Nice-to-Haves) | ~13 | ~104 | Varies | Varies |
| **Total fixable** | **~49** | **~104** | | |
| Intentional + Blocked | 63 | — | N/A | — |

### Post-Implementation Note

The C3 hybrid approach was chosen after detailed analysis. See `plans/c3-wire-format-analysis.md` for the full comparison of wire format wrapping vs query plan bypass vs hybrid.
