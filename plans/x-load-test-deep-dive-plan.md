# EmulatorLoadTests.cs Deep Dive — Test Coverage & Bug Fix Plan

**Date**: 2026-04-01  
**Current version**: 2.0.4  
**Target version**: 2.0.5  
**File**: `tests/CosmosDB.InMemoryEmulator.Tests.Performance/EmulatorLoadTests.cs`  
**Companion**: `tests/CosmosDB.InMemoryEmulator.Tests.Performance/LoadTests.cs`  

---

## Overview

`EmulatorLoadTests.cs` runs sustained-load scenarios against the **real Cosmos DB emulator** (localhost:8081).  
`LoadTests.cs` runs the same patterns against the **InMemoryContainer**.

Both files share the same `LoadDocument`, `LoadStats`, and `AtomicCounter` helper types (defined in `LoadTests.cs`).  
Both exercise: Create, Upsert, Replace, Patch, Delete, Point-Read, Query (TOP 10 ordered), Query (filtered by id).

The analysis below covers **both files** since they are mirrors of each other and share bugs/gaps.

---

## PHASE 1 — BUG FIXES (in existing code)

### Bug 1: Race condition in `WriteUpsert` — assertion expects OK but item may have been deleted
- **Files**: `EmulatorLoadTests.cs` line ~370, `LoadTests.cs` line ~370
- **Issue**: `WriteUpsert` (update-existing branch) does `TryGetValue` on `knownIds`, then calls `UpsertItemAsync`. Between those two calls, `WriteDelete` on another thread can delete the item from the container AND remove it from `knownIds`. When the upsert reaches Cosmos, the item no longer exists, so Cosmos **creates** it fresh and returns `HttpStatusCode.Created` (201). But the assertion is `response.StatusCode.Should().Be(HttpStatusCode.OK)`. This fails, gets caught as an unexpected error, and inflates the error count.
- **Fix**: Change assertion to allow both `OK` and `Created`: `response.StatusCode.Should().BeOneOf(HttpStatusCode.OK, HttpStatusCode.Created)`. When it's `Created`, also re-add to `knownIds`.
- **Severity**: Medium — causes spurious test failures under high contention.

### Bug 2: `WriteReplace` / `WritePatch` — 404s counted as NotFound, not as the operation type
- **Files**: Both files' `WriteReplace` and `WritePatch`
- **Issue**: If another thread deletes an item between `TryGetValue` and `ReplaceItemAsync`/`PatchItemAsync`, a `CosmosException(404)` is thrown. The catch block in `RunLoad` catches it and increments `stats.NotFound`, but the replace/patch stat counter is **never incremented**. This means `totalOps` can be less than `_totalOperations` if many 404s happen during replaces.
- **Analysis**: Actually, this is NOT a data bug — 404s are added to totalOps in `ReportAndAssert`. The real issue is that the operation-type breakdown is misleading (some replace/patch operations counted as NotFound instead). No functional fix needed, but a comment should clarify this is expected.
- **Severity**: Low — misleading stats but no assertion failure.

### Bug 3: `ReadAndVerifySingle` vs `ReadAndVerifyPointRead` are functionally identical
- **Files**: Both files
- **Issue**: Both methods call `ReadItemAsync<LoadDocument>` with the same pattern. The only difference is the assertions. These are two of the four read-path branches (50% of reads) doing the exact same Cosmos operation. This reduces effective read coverage diversity — no queries, ReadMany, or other read patterns get that budget.
- **Fix**: Replace `ReadAndVerifyPointRead` with a different operation (e.g., ReadMany, or a cross-partition query) to improve coverage.
- **Severity**: Low — reduced coverage, not a crash bug.

### Bug 4: All seed documents share partition key "seed" — unrealistic partition distribution
- **Files**: Both files' `SeedDocuments`
- **Issue**: All seeded documents use `partitionKey = "seed"`. This means 100% of initial reads hit the single "seed" partition. Writes create documents in unique partitions (`pk-{id}`). This doesn't model realistic partition distribution.
- **Fix**: Distribute seeds across multiple partition keys (e.g., `$"pk-{seedIndex % 20}"`).
- **Severity**: Low — reduces realism of load profile.

### Bug 5: `EmulatorLoadTests` has no performance assertions at all
- **Files**: `EmulatorLoadTests.cs` `ReportAndAssert`
- **Issue**: Unlike `LoadTests.cs` which asserts throughput ≥ 95% of target and P99 < 200ms, `EmulatorLoadTests.cs` only asserts `totalOps == _totalOperations` and `errors == 0`. This is deliberate (real emulator is slower/variable), but there should be at least a sanity-check latency assertion (e.g., P99 < 5000ms) to catch hangs.
- **Fix**: Add a generous P99 latency bound for the real emulator (e.g., < 5000ms).
- **Severity**: Low.

---

## PHASE 2 — NEW TESTS (missing coverage)

All new tests go in **`LoadTests.cs`** (InMemoryContainer) with companion stubs in **`EmulatorLoadTests.cs`** (real emulator). Tests marked with 🔴 need corresponding implementation changes. Tests marked with ⏭️ will be skipped with detailed reasons.

### Category A: Missing Operation Types Under Load

| # | Test Name | Description | TDD? |
|---|-----------|-------------|------|
| A1 | `ReadManyUnderLoad_VerifiesAllItemsReturned` | Exercise `ReadManyItemsAsync` with batches of 5-10 random IDs from `knownIds`. Verify all returned, correct ids. | ✅ |
| A2 | `TransactionalBatchUnderLoad_AtomicCreateAndRead` | Create transactional batches (create 3 items + read 1 existing) under load. Verify atomicity — all succeed or all fail. | ✅ |
| A3 | `DeleteAllByPartitionKeyUnderLoad` | Periodically call `DeleteAllItemsByPartitionKeyStreamAsync` for a specific PK while other ops target different PKs. Verify correct deletion and no cross-PK damage. | ✅ |
| A4 | `StreamApiOperationsUnderLoad` | Mix `CreateItemStreamAsync`, `ReadItemStreamAsync`, `UpsertItemStreamAsync` alongside normal operations. Verify response streams deserialize correctly. | ✅ |
| A5 | `ChangeFeedReadDuringWrites` | Start a change feed iterator, continuously read while writes happen. Verify all written items eventually appear in feed with correct order. | ✅ |

### Category B: Query Diversity Under Load

| # | Test Name | Description | TDD? |
|---|-----------|-------------|------|
| B1 | `CrossPartitionQueryUnderLoad` | `SELECT * FROM c WHERE c.counter > @threshold` without specifying partition key. Verify results span multiple partitions. | ✅ |
| B2 | `AggregateQueryUnderLoad` | `SELECT COUNT(1) FROM c` and `SELECT VALUE SUM(c.counter) FROM c` periodically during load. Verify count is consistent with known tracking. | ✅ |
| B3 | `DistinctQueryUnderLoad` | `SELECT DISTINCT c.partitionKey FROM c` during writes. Verify no duplicates in result. | ✅ |
| B4 | `OffsetLimitPaginationUnderLoad` | `SELECT * FROM c ORDER BY c.id OFFSET @offset LIMIT 10` during writes. Verify page sizes ≤ 10. | ✅ |

### Category C: Concurrency Edge Cases Under Sustained Load

| # | Test Name | Description | TDD? |
|---|-----------|-------------|------|
| C1 | `ETagConflictsUnderLoad` | Read an item to get its ETag, then concurrently attempt to `ReplaceItemAsync` with `IfMatchEtag`. Verify exactly one succeeds, others get 412 PreconditionFailed or 200 (last-writer-wins if no etag). | ✅ |
| C2 | `CreateAfterDelete_SameId_Succeeds` | Delete an item then immediately re-create with same ID. Verify create succeeds with 201. Run many in parallel. | ✅ |
| C3 | `DoubleDelete_Returns404` | Two threads both try `DeleteItemAsync` on same item. One should get 204, the other 404. Verify neither crashes and stats are consistent. | ✅ |
| C4 | `HotPartitionLoadTest` | Concentrate 80% of operations on a single partition key. Verify no starvation or degradation for remaining 20% on other partitions. | ✅ |
| C5 | `HighCardinalityPartitionKeys` | Every write uses a unique partition key. Verify system handles thousands of distinct PKs. | ✅ |

### Category D: Stress Patterns

| # | Test Name | Description | TDD? |
|---|-----------|-------------|------|
| D1 | `BurstTrafficPattern_SpikeThenCalm` | 10x target RPS for 2 seconds, then 0.1x for 8 seconds, repeat. Verify no errors accumulate during spikes. | ✅ |
| D2 | `GradualRampUp_LinearIncrease` | Start at 10% target RPS, increase by 10% each second until 100%. Verify latency remains stable. | ✅ |
| D3 | `WriteOnlyStress` | 100% writes (no reads). Verify container can absorb sustained write-only traffic with monotonically increasing item count. | ✅ |
| D4 | `ReadOnlyStress_LargeDataset` | Seed 10,000 items, then 100% reads. Verify zero errors and stable latency. | ✅ |

### Category E: Data Integrity Verification

| # | Test Name | Description | TDD? |
|---|-----------|-------------|------|
| E1 | `PostLoadStateVerification` | After standard load run, iterate all items in container and verify every item in `knownIds` exists and every container item is in `knownIds` (no orphans). | ✅ |
| E2 | `PatchCounterMonotonicity` | Seed items with counter=0, run only patch-increment operations. After load, verify all counters equal the number of patches they received. | ✅ |
| E3 | `LargeDocumentPayload_UnderLoad` | Documents with 100KB+ data field. Verify no truncation or corruption under sustained CRUD. | ⏭️ |

### Category F: Container/Database Lifecycle Under Load

| # | Test Name | Description | TDD? |
|---|-----------|-------------|------|
| F1 | `MultipleContainersConcurrentLoad` | Run load tests against 3 different containers simultaneously. Verify complete isolation — no cross-container leaks. | ✅ |

---

## PHASE 3 — SKIP DECISIONS

Tests that are too complex or require unsupported features will be marked `[Fact(Skip = "...")]` with a detailed reason, plus a `_DivergentBehavior` sister test.

### E3: `LargeDocumentPayload_UnderLoad`
- **Skip reason**: "Document size enforcement varies between InMemoryContainer (2MB limit checked on serialized JSON) and real Cosmos DB (which measures payload differently and has ~2MB item limit). Under load, the overhead of generating/verifying 100KB+ documents makes the test flaky due to memory pressure and GC, not due to Cosmos behavior."
- **Sister test**: `LargeDocumentPayload_DivergentBehavior` — demonstrates that creating a document near the limit succeeds in both, but the exact boundary differs. InMemoryContainer uses Newtonsoft.Json serialized byte count; real Cosmos uses binary encoding overhead.

---

## PHASE 4 — IMPLEMENTATION ORDER (TDD: Red → Green → Refactor)

Each step follows strict TDD:
1. Write the failing test (Red)
2. Implement the minimal code to make it pass (Green)
3. Refactor shared helpers, clean up (Refactor)

### Step 1: Fix existing bugs
- [ ] Fix Bug 1 (WriteUpsert race) — in both files
- [ ] Fix Bug 3 (deduplicate ReadAndVerifySingle/PointRead) — replace PointRead with ReadMany
- [ ] Fix Bug 4 (seed partition distribution) — distribute across partitions
- [ ] Fix Bug 5 (add EmulatorLoadTests latency assertion)
- [ ] Add clarifying comment for Bug 2 (NotFound stats explanation)

### Step 2: Category E — Data Integrity (highest value, catches real concurrency bugs)
- [ ] E1 — PostLoadStateVerification
- [ ] E2 — PatchCounterMonotonicity

### Step 3: Category A — Missing Operation Types
- [ ] A1 — ReadMany under load
- [ ] A2 — TransactionalBatch under load
- [ ] A3 — DeleteAllByPartitionKey under load
- [ ] A4 — Stream API operations under load
- [ ] A5 — Change feed during writes

### Step 4: Category C — Concurrency Edge Cases
- [ ] C1 — ETag conflicts
- [ ] C2 — Create after delete
- [ ] C3 — Double delete
- [ ] C4 — Hot partition
- [ ] C5 — High cardinality PKs

### Step 5: Category B — Query Diversity
- [ ] B1 — Cross-partition query
- [ ] B2 — Aggregate query
- [ ] B3 — DISTINCT query
- [ ] B4 — OFFSET/LIMIT pagination

### Step 6: Category D — Stress Patterns
- [ ] D1 — Burst traffic
- [ ] D2 — Gradual ramp-up
- [ ] D3 — Write-only stress
- [ ] D4 — Read-only stress large dataset

### Step 7: Category F — Container Lifecycle
- [ ] F1 — Multiple containers concurrent load

### Step 8: Skip + Divergent tests
- [ ] E3 — Skip + write LargeDocumentPayload_DivergentBehavior

### Step 9: Refactor shared helpers
- [ ] Extract common load-runner infrastructure if helpers have grown
- [ ] Ensure both files (LoadTests.cs and EmulatorLoadTests.cs) stay in sync

---

## PHASE 5 — DOCUMENTATION UPDATES

### Wiki: Known-Limitations.md
- No new limitations expected (tests exercise already-supported features)
- If any test reveals a real gap (e.g., ReadMany under sustained concurrent load has a bug), document the limitation there

### Wiki: Features.md
- Add "Load testing verified" note to relevant features (CRUD, Patch, Query, ReadMany, TransactionalBatch, Change Feed, Stream API, DeleteAllByPartitionKey, ETag concurrency)

### Wiki: Feature-Comparison-With-Alternatives.md
- Update "Performance benchmarks" section with any new P50/P95/P99 numbers from the expanded test suite
- Add row for "Concurrent operation safety" in feature matrix if not already present

### README.md
- No changes expected unless a major new feature is added

### Other wiki pages
- Review Troubleshooting.md for any concurrency-related FAQs that should be added

---

## PHASE 6 — VERSION, TAG, PUSH

- [ ] Increment version: `2.0.4` → `2.0.5` in `src/CosmosDB.InMemoryEmulator/CosmosDB.InMemoryEmulator.csproj`
- [ ] `git add -A`
- [ ] `git commit -m "v2.0.5: Comprehensive load test coverage — bug fixes, concurrency edge cases, data integrity verification"`
- [ ] `git tag v2.0.5`
- [ ] `git push`
- [ ] `git push --tags`
- [ ] Update wiki:
  - `cd c:\git\CosmosDB.InMemoryEmulator.wiki`
  - `git add -A`
  - `git commit -m "v2.0.5: Update features/comparison for expanded load test coverage"`
  - `git push`

---

## TRACKING

| Phase | Status |
|-------|--------|
| Phase 1 — Bug Fixes | ⬜ Not started |
| Phase 2 — New Tests (write failing) | ⬜ Not started |
| Phase 3 — Skip + Divergent tests | ⬜ Not started |
| Phase 4 — Implementation (green) | ⬜ Not started |
| Phase 5 — Documentation | ⬜ Not started |
| Phase 6 — Version/Tag/Push | ⬜ Not started |
