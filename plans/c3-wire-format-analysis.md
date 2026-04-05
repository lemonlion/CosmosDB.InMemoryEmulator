# C3: FakeCosmosHandler Wire Format — Approach Analysis

## Problem Statement

Four tests are skipped because `FakeCosmosHandler`'s HTTP query pipeline cannot handle GROUP BY, DISTINCT+ORDER BY, and multi-aggregate queries. The queries **already work** when executed directly against `InMemoryContainer` — the problem is exclusively in how response documents flow through the Cosmos SDK's internal query pipeline stages when the handler returns HTTP responses.

### The 4 Affected Tests

| Test | Query | Error |
|------|-------|-------|
| `Handler_GroupByQuery_ReturnsGroupedResults` | `SELECT c.name, COUNT(1) AS cnt FROM c GROUP BY c.name` | `JsonException: Unexpected character while parsing path indexer: {` |
| `Handler_DistinctWithOrderBy_ReturnsOrderedDistinctValues` | `SELECT DISTINCT c.name FROM c ORDER BY c.name ASC` | SDK merge-sort pipeline rejects response format |
| `Handler_MultipleAggregates_ReturnsAllValues` | `SELECT COUNT(1) as cnt, SUM(c.value) as total FROM c` | Multi-aggregate wrapping format mismatch |
| `ToFeedIterator_WithGroupBy_GroupsCorrectly` | LINQ `.GroupBy(d => d.Name).Select(g => new { ... })` | Same as GROUP BY above |

### Current Architecture

```
User Query (SQL or LINQ)
    ↓
Cosmos SDK Client
    ↓
FakeCosmosHandler (HTTP interceptor)
    ├── HandleQueryPlanAsync() → returns queryInfo (pipeline configuration)
    └── HandleQueryAsync()    → returns response documents
    ↓
SDK Query Pipeline (SDK-internal, runs client-side)
    ├── OrderByQueryPipelineStage   (merge-sort by orderByItems)
    ├── DistinctQueryPipelineStage  (hash-based dedup)
    ├── GroupByQueryPipelineStage   (accumulate groupByItems → aggregate)
    └── AggregateQueryPipelineStage (accumulate partial aggregates)
    ↓
Final results to user
```

The handler controls TWO outputs: the **query plan** (which tells the SDK which pipeline stages to activate) and the **response documents** (which the pipeline stages consume). Both approaches exploit this dual control.

---

## What Already Works

Before comparing approaches, it's important to note what `FakeCosmosHandler` already handles successfully:

| Scenario | Query Plan | Document Wrapping | Status |
|----------|-----------|-------------------|--------|
| **ORDER BY** | `hasNonStreamingOrderBy=true`, `orderBy=["Ascending"]` | `{_rid, orderByItems:[{item:val}], payload:doc}` | ✅ Working |
| **Single VALUE aggregate** | `aggregates=["Count"]`, `hasSelectValue=true` | `[{item: value}]` (AVG: `[{item:{sum,count}}]`) | ✅ Working |
| **DISTINCT (no ORDER BY)** | `distinctType="Unordered"` | Raw documents (no wrapping needed) | ✅ Working |
| **Simple queries** | No special flags | Raw documents | ✅ Working |
| **OFFSET/LIMIT** | `offset`, `limit` fields | Raw documents (SDK applies offset/limit) | ✅ Working |

---

## Approach 1: Wire Format Wrapping

**Strategy:** Keep the query plan telling the SDK to use its pipeline stages (GROUP BY, DISTINCT, aggregate). Wrap response documents in the exact internal format each pipeline stage expects.

### What Each Scenario Needs

#### GROUP BY

The SDK's `GroupByQueryPipelineStage` expects each document to be a `RewrittenGroupByProjection`:

```json
{
  "groupByItems": [{"item": "Alice"}],
  "payload": {
    "name": "Alice",
    "cnt": {"item": 2}
  }
}
```

The SDK rewrite for `SELECT c.name, COUNT(1) AS cnt FROM c GROUP BY c.name` would be:
```sql
SELECT [{"item": c.name}] AS groupByItems,
       {"name": c.name, "cnt": {"item": COUNT(1)}} AS payload
FROM c
GROUP BY c.name
```

**Implementation:**
1. Add `BuildGroupByRewrittenQuery(parsed)` — constructs the rewritten SQL
2. BUT: `InMemoryContainer` cannot execute this rewritten form (it has JSON object literals in SELECT)
3. So instead: execute the **original** GROUP BY query on `InMemoryContainer`, then **wrap** each result row:
   - For each GROUP BY key field → `groupByItems` array entry
   - For each aggregate field → wrap value in `{"item": value}` inside payload
   - For each non-aggregate field → include raw in payload
4. Set `rewrittenQuery` to the SDK-style rewritten form (for the SDK to parse, even though we don't execute it)

**Complication:** The SDK's `GroupingTable` accumulates partial results via `SingleGroupAggregator.AddValues(payload)`. This means:
- The handler must return **per-row partial results**, not pre-aggregated totals
- For `COUNT(1)`: each row should contribute `{"item": 1}` (the SDK sums them)
- For `SUM(c.value)`: each row should contribute `{"item": c.value}` (the SDK sums them)
- For `AVG(c.value)`: each row should contribute `{"item": {"sum": c.value, "count": 1}}`

This means the handler **cannot execute GROUP BY in InMemoryContainer and wrap the final results**. It must instead:
1. Execute a **non-aggregated** query with just the raw data
2. Wrap each raw document as `{groupByItems:[...], payload:{...}}` with individual values
3. Let the SDK's GroupByQueryPipelineStage do the actual aggregation

This is a fundamental constraint — the SDK pipeline stages expect to do the aggregation themselves from partial inputs.

#### DISTINCT + ORDER BY

The `OrderByQueryPipelineStage` wraps/unwraps `{_rid, orderByItems, payload}`. The `DistinctQueryPipelineStage` runs on top, receiving the unwrapped payload.

**Current problem:** `BuildOrderByRewrittenQuery` always uses `c AS payload` (full document). For `SELECT DISTINCT c.name FROM c ORDER BY c.name`, the payload should be just `c.name`, not the full document.

**Implementation:**
1. When DISTINCT is combined with ORDER BY, adjust `BuildOrderByRewrittenQuery` to set payload to the SELECT expression(s), not `c`
2. For `SELECT DISTINCT c.name FROM c ORDER BY c.name ASC`:
   ```sql
   SELECT c._rid, [{"item": c.name}] AS orderByItems, c.name AS payload
   FROM c ORDER BY c.name ASC
   ```
3. The SDK's ORDER BY stage extracts `c.name` values, DISTINCT stage deduplicates them

**Relative complexity:** Low. This is a tweak to existing ORDER BY wrapping.

#### Multiple Aggregates (non-VALUE)

For `SELECT COUNT(1) as cnt, SUM(c.value) as total FROM c`, the SDK expects each document wrapped as:

```json
{
  "payload": {
    "cnt": {"item": 1},
    "total": {"item": <c.value>}
  }
}
```

(Non-VALUE aggregate query → `RewrittenAggregateProjections` extracts from `cosmosObject["payload"]`)

**Implementation:**
1. Execute the raw (non-aggregated) query to get individual documents
2. Wrap each document: `{"payload": {"cnt": {"item": 1}, "total": {"item": doc.value}}}`
3. The SDK's `AggregateQueryPipelineStage` / `SingleGroupAggregator` accumulates the values

**Same constraint as GROUP BY:** Cannot pass pre-computed aggregates. Must pass individual document contributions for each aggregate.

### Pros — Wire Format Wrapping

1. **Faithful to SDK architecture.** The query plan accurately describes the query semantics. The SDK's pipeline stages (GROUP BY, DISTINCT, aggregates) operate as designed. If the SDK has bugs in these stages, the emulator would reproduce them — higher fidelity.

2. **Pagination comes for free.** The SDK's GROUP BY stage already handles `pageSize` when draining the GroupingTable. The handler doesn't need custom pagination logic.

3. **Continuation tokens handled by SDK.** For DISTINCT (ordered), the SDK manages continuation tokens through `DistinctContinuationToken`. The handler doesn't need to implement this.

4. **Consistent with existing ORDER BY pattern.** The handler already wraps ORDER BY documents in `{_rid, orderByItems, payload}` successfully. Extending this to GROUP BY and aggregates follows the same pattern.

5. **DISTINCT + ORDER BY fix is trivial.** It's just a tweak to `BuildOrderByRewrittenQuery` to use the SELECT expression instead of `c` as payload. Could be done independently.

### Cons — Wire Format Wrapping

1. **Undocumented internal wire formats.** The `groupByItems + payload` format, the `{"item": value}` wrapping for aggregates, and the AVG `{"sum", "count"}` format are all internal SDK implementation details. They are NOT part of any public API contract or documentation. They could change in any SDK release.

2. **Must return raw (non-aggregated) documents.** The SDK pipeline expects to do aggregation itself from partial inputs. This means the handler must execute a **different query** than what the user wrote — it must strip GROUP BY and aggregates to get raw documents, then wrap each one. This adds query rewriting complexity.

3. **Aggregate-specific wrapping logic.** Each aggregate type (COUNT, SUM, AVG, MIN, MAX) has its own wrapping format. AVG is particularly special (`{"sum": val, "count": 1}`). Adding new aggregate types in future SDK versions would require matching their internal format.

4. **Two code paths doing the same work.** `InMemoryContainer` already correctly evaluates GROUP BY, aggregates, and DISTINCT. With this approach, those capabilities would go unused for the handler path — the handler would instead strip them from the query and re-implement the wrapping so the SDK can re-do the aggregation from scratch.

5. **GROUP BY wrapping is architecturally complex.** The handler needs to:
   - Parse the GROUP BY fields and aggregate functions from the SELECT
   - Execute a stripped query (no GROUP BY, no aggregates) against InMemoryContainer
   - Map each result document to the wrapped format with correct alias/aggregate structure
   - Handle all aggregate types correctly
   - Set the `rewrittenQuery` to the SDK-style rewritten form
   - This is ~200-300 lines of intricate wrapping code

6. **High coupling to SDK internals.** A breaking change in `GroupByQueryPipelineStage`, `SingleGroupAggregator`, or `RewrittenAggregateProjections` would silently break the emulator. The failure mode would be cryptic deserialization errors, not clean test failures.

7. **Testing the wrapping is hard.** To verify the wrapping is correct, you'd need to test against specific SDK versions and potentially trace real Cosmos wire traffic for comparison. The wrapping format isn't documented anywhere you can regression-test against.

---

## Approach 2: Query Plan Bypass

**Strategy:** Execute the full query in `InMemoryContainer` (which already handles GROUP BY, aggregates, DISTINCT+ORDER BY correctly). Return the final, fully-computed results. Modify the query plan to tell the SDK "no pipeline processing needed" — disable all pipeline stages so the SDK passes results through untouched.

### How It Works

For a GROUP BY query like `SELECT c.name, COUNT(1) AS cnt FROM c GROUP BY c.name`:

1. `HandleQueryPlanAsync()` returns:
   ```json
   {
     "distinctType": "None",
     "groupByExpressions": [],
     "aggregates": [],
     "groupByAliasToAggregateType": {},
     "hasNonStreamingOrderBy": false,
     "rewrittenQuery": "SELECT c.name, COUNT(1) AS cnt FROM c GROUP BY c.name"
   }
   ```
   (All pipeline-triggering flags set to their "disabled" values)

2. `HandleQueryAsync()` executes the query against `InMemoryContainer`, which returns:
   ```json
   [{"name": "Alice", "cnt": 2}, {"name": "Bob", "cnt": 1}]
   ```

3. The SDK receives these documents and — because no pipeline stages are activated — passes them through to the user untouched.

### Implementation Detail

In `HandleQueryPlanAsync`, after building the queryInfo:

```csharp
// For queries that InMemoryContainer handles fully (GROUP BY, multi-aggregate),
// suppress SDK pipeline stages and return pre-computed results
if (parsed.GroupByFields is { Length: > 0 } || HasMultipleAggregates(parsed))
{
    queryInfo["distinctType"] = "None";
    queryInfo["groupByExpressions"] = new JArray();
    queryInfo["groupByAliases"] = new JArray();
    queryInfo["groupByAliasToAggregateType"] = new JObject();
    queryInfo["aggregates"] = new JArray();
    queryInfo["hasNonStreamingOrderBy"] = false;
    queryInfo["orderBy"] = new JArray();
    queryInfo["orderByExpressions"] = new JArray();
    // rewrittenQuery = original SQL (InMemoryContainer evaluates it fully)
}
```

And in `HandleQueryAsync`, the existing `SimplifySdkQuery` path would handle these queries as simple non-wrapped responses.

### Pros — Query Plan Bypass

1. **Dramatically simpler implementation.** ~30 lines of query plan flag suppression vs ~300 lines of document wrapping. The handler already evaluates these queries correctly via `InMemoryContainer` — it just needs to stop telling the SDK to post-process them.

2. **No dependency on internal wire formats.** Never needs to produce `groupByItems`, `payload`, or `{"item": value}` wrapping. Immune to SDK internal format changes.

3. **Lower maintenance burden.** If the SDK adds new aggregate types, new pipeline stages, or changes wire formats, the bypass approach is unaffected — it never participates in pipeline processing.

4. **Leverages existing, tested code.** `InMemoryContainer`'s GROUP BY, aggregation, and DISTINCT logic is already covered by direct-access tests. The handler path simply passes through the same results.

5. **Fix for DISTINCT + ORDER BY is still available.** DISTINCT without ORDER BY already works (raw documents, `distinctType="Unordered"`). For DISTINCT + ORDER BY, the bypass approach would suppress the DISTINCT flag and let `InMemoryContainer` handle both DISTINCT and ORDER BY natively.

6. **Predictable failure mode.** If a query doesn't work, it fails cleanly at the `InMemoryContainer` level (wrong result or unsupported syntax), not with a cryptic SDK deserialization exception.

7. **Easy to extend.** Any future query feature that `InMemoryContainer` supports can be immediately available through the handler by adding it to the "suppress pipeline" condition.

### Cons — Query Plan Bypass

1. **Behavioral divergence for GROUP BY pagination.** Real Cosmos sends partial GROUP BY results across pages; the SDK's `GroupByQueryPipelineStage` accumulates across continuations. With bypass, all GROUP BY results arrive in a single response. For most test workloads this doesn't matter, but tests specifically checking GROUP BY pagination behavior would see different behavior.

2. **Continuation tokens differ.** The SDK's GROUP BY stage explicitly disallows continuation tokens (`"Continuation token is not supported for queries with GROUP BY"`). So this is actually a non-issue for GROUP BY. But for DISTINCT+ORDER BY, the SDK normally manages `DistinctContinuationToken` — bypassing DISTINCT means the handler would need to implement its own continuation logic if pagination is needed.

3. **SDK pipeline bugs not reproduced.** If the SDK's GROUP BY or aggregate pipeline stages have bugs, the bypass approach would NOT reproduce them. Tests passing against the emulator might fail against real Cosmos (or vice versa). This cuts both ways — it could also mean tests pass against the emulator when they'd fail against a buggy SDK stage.

4. **Lies to the SDK about query semantics.** The query plan says "no GROUP BY, no aggregates" for a query that clearly has them. If the SDK ever validates query plan consistency against the SQL (e.g., "this query has GROUP BY syntax but the plan says no grouping"), the bypass would break. Currently the SDK does NOT do this validation — it trusts the query plan unconditionally.

5. **ORDER BY wrapping inconsistency.** Simple ORDER BY queries already use wire format wrapping (Approach 1 pattern). Adopting Approach 2 for GROUP BY/aggregates creates two different strategies within the same handler — some queries wrap, others bypass. This dual approach could be confusing for future maintainers.

6. **Cannot bypass ORDER BY stage for DISTINCT+ORDER BY.** The ORDER BY stage is deeply integrated with merge-sort across partitions. Disabling `hasNonStreamingOrderBy` might cause the SDK to return unsorted results even if `InMemoryContainer` sorted them (the SDK has its own ideas about sort order across partition ranges). This means for DISTINCT+ORDER BY, the handler may still need to produce ORDER BY-wrapped documents AND suppress only the DISTINCT flag.

---

## Scenario-by-Scenario Comparison

| Scenario | Approach 1 (Wire Format) | Approach 2 (Bypass) |
|----------|--------------------------|---------------------|
| **GROUP BY** | ~200 LOC; strip aggregates from query, wrap each raw doc as `{groupByItems, payload}` with per-aggregate `{"item": val}` formatting. Must handle COUNT/SUM/AVG/MIN/MAX individually. | ~10 LOC; suppress GROUP BY/aggregate flags in query plan. Return `InMemoryContainer` results directly. |
| **DISTINCT + ORDER BY** | ~20 LOC; fix `BuildOrderByRewrittenQuery` payload to use SELECT expression instead of `c`. | ~10 LOC; suppress DISTINCT flag, keep ORDER BY wrapping. **BUT**: may need testing to confirm the SDK's ORDER BY stage extracts payload correctly when downstream expects non-distinct results. |
| **Multi-aggregate** | ~80 LOC; strip aggregates from query, wrap each doc as `{"payload": {"cnt": {"item": 1}, "total": {"item": val}}}`. Same constraint: must pass raw rows, not pre-aggregated. | ~10 LOC; suppress aggregate flags. Return `InMemoryContainer` results directly. |
| **LINQ GroupBy** | Same as GROUP BY (LINQ generates SQL GROUP BY under the hood) | Same as GROUP BY |

---

## Risk Matrix

| Risk | Approach 1 | Approach 2 |
|------|-----------|-----------|
| SDK version upgrade breaks it | **HIGH** — wire formats are internal | **LOW** — only depends on query plan flags |
| Implementation complexity | **HIGH** — 300+ LOC of intricate wrapping | **LOW** — 30 LOC of flag suppression |
| Behavioral fidelity | **HIGH** — SDK pipeline runs as designed | **MEDIUM** — pagination/continuation may differ |
| Maintenance burden | **HIGH** — each new aggregate type needs wrapping | **LOW** — `InMemoryContainer` handles new features natively |
| Future SDK validation | **LOW** risk — wire formats are consumed, not validated | **MEDIUM** risk — SDK might validate plan/SQL consistency |
| Inconsistency with existing code | **LOW** — extends existing ORDER BY pattern | **MEDIUM** — two strategies in one handler |

---

## Hybrid Option

A third possibility: use **Approach 2 (bypass) for GROUP BY and multi-aggregate**, and **Approach 1 (wire format fix) for DISTINCT + ORDER BY**.

Rationale:
- GROUP BY and multi-aggregate wrapping is the most complex and fragile part (~280 LOC, SDK-internal format dependency)
- DISTINCT + ORDER BY fix is a minor tweak (~20 LOC) to existing, battle-tested ORDER BY wrapping
- This gets all 4 tests passing with minimal new risk

| Component | Approach | Effort | Risk |
|-----------|----------|--------|------|
| GROUP BY | Bypass | ~10 LOC | Low |
| Multi-aggregate | Bypass | ~10 LOC | Low |
| DISTINCT + ORDER BY | Wire format fix | ~20 LOC | Low |
| LINQ GroupBy | Bypass (same as GROUP BY) | 0 LOC | Low |
| **Total** | **Hybrid** | **~40 LOC** | **Low** |

---

## Recommendation

**The hybrid approach is the clear winner.** It:
- Gets all 4 tests passing with ~40 lines of code
- Avoids the high-risk, high-complexity GROUP BY and aggregate wrapping
- Fixes DISTINCT + ORDER BY with a minimal tweak to proven code
- Has the lowest maintenance burden going forward
- Can be revisited if SDK validation changes ever break the bypass

The only scenario where Approach 1 (full wire format wrapping) would be preferable is if users need pixel-perfect reproduction of SDK pipeline stage behavior (e.g., testing GROUP BY pagination across continuations with specific page sizes). For a testing emulator, this level of fidelity is unlikely to be needed.

---

## Implementation Checklist (when ready to proceed)

- [ ] In `HandleQueryPlanAsync`: add condition for GROUP BY / multi-aggregate → suppress pipeline flags
- [ ] In `BuildOrderByRewrittenQuery`: when DISTINCT is present, use SELECT expression as payload instead of `c`
- [ ] In `HandleQueryAsync`: ensure `SimplifySdkQuery` correctly passes through GROUP BY and multi-aggregate queries for direct execution
- [ ] Unskip all 4 tests
- [ ] Delete any corresponding sister tests
- [ ] Run full regression on both TFMs
