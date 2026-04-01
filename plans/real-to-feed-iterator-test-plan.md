# RealToFeedIteratorTests - Deep Dive Test Coverage Plan

**Created:** 2026-04-01  
**Target version:** 2.0.5  
**File:** `tests/CosmosDB.InMemoryEmulator.Tests/RealToFeedIteratorTests.cs`  
**Status:** PLANNING (not yet implemented)

---

## Domain Overview

`RealToFeedIteratorTests` validates the full end-to-end pipeline:

```
Real CosmosClient → SDK LINQ → .ToFeedIterator() → HTTP request
  → FakeCosmosHandler (intercepts) → InMemoryContainer (executes)
  → HTTP response → SDK deserialisation → typed results
```

This is the **highest-fidelity** test path in the emulator — it proves the SDK's own
query generation, HTTP serialisation, continuation tokens, cross-partition fanout, and
aggregate merging all work correctly against the fake handler. Gaps here represent
real production risks that lower-level unit tests cannot catch.

---

## Current Coverage Summary (45+ tests)

### What's Already Covered

| Category | Tests | Details |
|----------|-------|---------|
| Basic retrieval | 1 | All items, cross-partition |
| Where clause | 7 | Simple, compound, bool, negated bool, OR, chained, nested property |
| String filtering | 3 | Contains, StartsWith, null-check |
| OrderBy | 4 | Asc, Desc (×2), multi-field (ThenBy) |
| Select projection | 5 | Single field, anonymous, multi-field, computed, Where+Select (×2) |
| Pagination | 3 | Take, OrderBy+Take, Skip+Take |
| Aggregates | 6 | Count, CountAfterWhere, Sum, Average, Min, Max |
| Distinct | 1 | Select+Distinct |
| SelectMany | 1 | Flatten arrays |
| Fault injection | 2 | Query-level 503, metadata-level 503 |
| IS_DEFINED | 1 | Preserves user-written null checks |
| Handler plumbing | 3 | Unknown route 404, RequestLog, QueryLog |

### Nested Test Classes (already exist in file)

| Class | Tests | Coverage |
|-------|-------|----------|
| `FakeCosmosHandlerOptionsTests` | 5 | Cache TTL, max entries, multi-range (query/orderby/filter), defaults |
| `SdkCompatibilityTests` | 1 | VerifySdkCompatibilityAsync |
| `MultiContainerRoutingTests` | 2 | Router dispatch, router OrderBy |
| `HashBasedPartitionRoutingTests` | 4 | Distribution, merge-sort, filter, dynamic metadata |
| `ReflectionBasedRegistrationTests` | 2 | Register/deregister, invalid queryable error |

---

## Identified Bugs & Issues

### BUG-1: Duplicate OrderByDescending tests
**File:** RealToFeedIteratorTests.cs  
**Issue:** Two tests (`WithOrderByDescending_ReturnsSortedDescending` and `WithOrderByDescending_ReversesOrder`) test essentially the same thing with slightly different data arrangements. The second adds no incremental coverage.  
**Fix:** Remove `WithOrderByDescending_ReversesOrder` or repurpose it (e.g., test OrderByDescending on a string field instead of int, or test OrderByDescending + Take).

### BUG-2: Potential stale data in `[Collection("FeedIteratorSetup")]` shared tests
**File:** RealToFeedIteratorTests.cs  
**Issue:** The `RealToFeedIteratorTests` class creates a new `InMemoryContainer` per instance, so this should be fine. However, the `[Collection("FeedIteratorSetup")]` attribute serialises all tests in this collection. Need to **verify** there are no other test classes in this collection that might interfere. If none exist, the `[Collection]` attribute is unnecessary overhead (serialises tests that could run in parallel).  
**Action:** Investigate whether the collection is needed. If only this class uses it, consider removing it for faster parallel execution.

### BUG-3: Missing response metadata validation
**File:** RealToFeedIteratorTests.cs  
**Issue:** None of the 45+ tests validate `FeedResponse<T>` metadata (StatusCode, RequestCharge, ActivityId, Headers, Diagnostics). The handler returns synthetic metadata (1.0 RU charge, GUID activity IDs), but tests never assert on these. If the handler accidentally stops returning required headers, tests still pass. This is validated separately in `FakeCosmosHandlerCrudTests` but not through the LINQ/FeedIterator path.  
**Fix:** Add at least one test that asserts on response metadata from a LINQ query.

---

## Planned New Tests

### Phase 1: Missing String Operations (via LINQ → real SDK)

| # | Test Name | Description | Difficulty |
|---|-----------|-------------|------------|
| T01 | `ToFeedIterator_WithStringEndsWith_FiltersCorrectly` | `Where(d => d.Name.EndsWith("ce"))` → returns "Alice". StartsWith and Contains both tested, EndsWith is the missing third. | Easy |
| T02 | `ToFeedIterator_WithStringToLower_FiltersCorrectly` | `Where(d => d.Name.ToLower() == "alice")` — tests case-insensitive comparison via LINQ-to-SQL translation. | Medium — SDK translates to `LOWER()`. If InMemoryContainer doesn't handle `LOWER()` in WHERE context from LINQ, will need skip+divergent. |
| T03 | `ToFeedIterator_WithStringToUpper_InProjection` | `Select(d => new { Upper = d.Name.ToUpper() })` — tests UPPER() in projection. | Medium — same concern as T02. |
| T04 | `ToFeedIterator_WithStringReplace_InProjection` | `Select(d => new { Clean = d.Name.Replace("a", "x") })` — REPLACE() function. | Medium |
| T05 | `ToFeedIterator_WithStringTrim_InFilter` | `Where(d => d.Name.Trim() == "Alice")` — LTRIM/RTRIM function. | Medium |
| T06 | `ToFeedIterator_WithSubstring_InProjection` | `Select(d => d.Name.Substring(0, 3))` — SUBSTRING() function. | Medium |
| T07 | `ToFeedIterator_WithStringIndexOf_InFilter` | `Where(d => d.Name.IndexOf("li") >= 0)` — INDEX_OF() function. | Medium |
| T08 | `ToFeedIterator_WithStringConcat_InProjection` | `Select(d => string.Concat(d.Name, "-", d.PartitionKey))` — CONCAT() function. | Medium |

### Phase 2: Missing LINQ Operators

| # | Test Name | Description | Difficulty |
|---|-----------|-------------|------------|
| T09 | `ToFeedIterator_WithThenByDescending_AppliesSecondaryDescSort` | `OrderBy(d => d.Value).ThenByDescending(d => d.Name)` — ThenByDescending not tested. | Easy |
| T10 | `ToFeedIterator_WithDistinctAfterWhere_ReturnsFilteredUnique` | `Where(d => d.IsActive).Select(d => d.Value).Distinct()` — Distinct + Where combo. | Easy |
| T11 | `ToFeedIterator_WithSumAfterWhere_ReturnsSumOfFiltered` | `.Where(d => d.IsActive).Select(d => d.Value).SumAsync()` — Aggregate after filter. | Easy |
| T12 | `ToFeedIterator_WithMinAfterWhere_ReturnsMinOfFiltered` | `.Where(d => d.Value > 10).Select(d => d.Value).MinAsync()` | Easy |
| T13 | `ToFeedIterator_WithMaxAfterWhere_ReturnsMaxOfFiltered` | `.Where(d => d.Value > 10).Select(d => d.Value).MaxAsync()` | Easy |
| T14 | `ToFeedIterator_WithAverageAfterWhere_ReturnsAverageOfFiltered` | `.Where(d => d.IsActive).Select(d => (double)d.Value).AverageAsync()` | Easy |
| T15 | `ToFeedIterator_WithSelectManyAndWhere_FlattensAndFilters` | `SelectMany(d => d.Tags).Where(t => t.StartsWith("a"))` — Combined flatten+filter. | Medium |
| T16 | `ToFeedIterator_WithCountAfterDistinct_ReturnsDistinctCount` | `.Select(d => d.Value).Distinct().CountAsync()` — Combined Distinct+Count. | Medium |

### Phase 3: Edge Cases & Empty/Boundary Conditions

| # | Test Name | Description | Difficulty |
|---|-----------|-------------|------------|
| T17 | `ToFeedIterator_OnEmptyContainer_ReturnsEmpty` | No seed data at all. Query on empty container returns 0 results without error. | Easy |
| T18 | `ToFeedIterator_WithEmptyStringFilter_MatchesEmptyStringValues` | `Where(d => d.Name == "")` — edge case for empty string matching. | Easy |
| T19 | `ToFeedIterator_WithTakeZero_ReturnsEmpty` | `.Take(0)` — boundary: SDK may translate to `TOP 0`. Cosmos returns empty. | Easy-Medium — Need to verify SDK behaviour. |
| T20 | `ToFeedIterator_WithSkipBeyondData_ReturnsEmpty` | `.OrderBy(d => d.Value).Skip(100).Take(10)` when only 3 items exist. | Easy |
| T21 | `ToFeedIterator_WithSingleItem_ReturnsOneItem` | Container with exactly 1 item, verified through various operators. | Easy |
| T22 | `ToFeedIterator_WithDuplicateValues_HandledCorrectly` | All items have same Name, verify Count, Distinct etc. work correctly. | Easy |
| T23 | `ToFeedIterator_WithNullNestedProperty_DoesNotThrow` | Query on nested property when some docs have null nested objects. | Easy |
| T24 | `ToFeedIterator_WithSpecialCharactersInValues_QueriesCorrectly` | Values with unicode, quotes, backslashes in Name field. | Medium |

### Phase 4: CRUD Through Real SDK Client

The existing tests only use the real SDK client for **queries** (LINQ → ToFeedIterator).
`FakeCosmosHandlerCrudTests` covers CRUD but uses the handler directly. These tests
verify the full SDK → HTTP → handler → InMemoryContainer path for CRUD operations.

| # | Test Name | Description | Difficulty |
|---|-----------|-------------|------------|
| T25 | `RealClient_CreateItem_Succeeds` | `_realContainer.CreateItemAsync(doc, pk)` through real SDK. | Easy |
| T26 | `RealClient_ReadItem_Succeeds` | `_realContainer.ReadItemAsync<T>(id, pk)` through real SDK. | Easy |
| T27 | `RealClient_UpsertItem_Succeeds` | `_realContainer.UpsertItemAsync(doc, pk)` through real SDK. | Easy |
| T28 | `RealClient_ReplaceItem_Succeeds` | `_realContainer.ReplaceItemAsync(doc, id, pk)` through real SDK. | Easy |
| T29 | `RealClient_DeleteItem_Succeeds` | `_realContainer.DeleteItemAsync<T>(id, pk)` through real SDK. | Easy |
| T30 | `RealClient_PatchItem_Succeeds` | `_realContainer.PatchItemAsync<T>(id, pk, operations)` through real SDK. | Medium |
| T31 | `RealClient_CrudThenQuery_RoundTrip` | Create, update, query → proves CRUD and query share same backing data. | Easy |
| T32 | `RealClient_CreateItem_DuplicateId_ThrowsConflict` | Verify 409 Conflict propagates through real SDK. | Easy |
| T33 | `RealClient_ReadItem_NotFound_ThrowsNotFound` | Verify 404 propagates through real SDK. | Easy |
| T34 | `RealClient_DeleteThenRead_ThrowsNotFound` | Delete + read → 404. | Easy |
| T35 | `RealClient_ReplaceItem_WithStaleETag_ThrowsPreconditionFailed` | ETag optimistic concurrency through real SDK. | Medium |

### Phase 5: Response Metadata Validation

| # | Test Name | Description | Difficulty |
|---|-----------|-------------|------------|
| T36 | `ToFeedIterator_ResponseContainsRequestCharge` | Assert `response.RequestCharge > 0` from FeedResponse. | Easy |
| T37 | `ToFeedIterator_ResponseContainsActivityId` | Assert `response.ActivityId` is non-empty GUID. | Easy |
| T38 | `ToFeedIterator_ResponseContainsCorrectStatusCode` | Assert `response.StatusCode == HttpStatusCode.OK`. | Easy |
| T39 | `ToFeedIterator_ResponseContainsItemCount` | Assert response count matches items returned. | Easy |
| T40 | `RealClient_CrudResponse_ContainsRequestCharge` | Assert RequestCharge on create/read/replace/delete responses. | Easy |

### Phase 6: Advanced Scenarios

| # | Test Name | Description | Difficulty |
|---|-----------|-------------|------------|
| T41 | `ToFeedIterator_WithConditionalProjection_UseTernary` | `Select(d => new { Label = d.IsActive ? "Active" : "Inactive" })` | Hard — SDK may not support ternary in LINQ-to-SQL. Will likely SKIP with divergent test. |
| T42 | `ToFeedIterator_WithNullCoalescing_InProjection` | `Select(d => d.Nested.Description ?? "N/A")` | Hard — Same concern. SKIP candidate. |
| T43 | `ToFeedIterator_WithMathOperations_InProjection` | `Select(d => Math.Abs(d.Value - 25))` | Hard — SDK Math translation uncertain. SKIP candidate. |
| T44 | `ToFeedIterator_WithConcurrentIterators_DoNotInterfere` | Two separate LINQ queries iterated simultaneously. | Medium |
| T45 | `ToFeedIterator_WithLargeResultSet_HandlesCorrectly` | 100+ items, verify all returned. | Easy |
| T46 | `ToFeedIterator_WithWhereOnPartitionKey_FiltersToPartition` | `Where(d => d.PartitionKey == "pk1")` — partition key filter in LINQ. | Easy |
| T47 | `ToFeedIterator_WithNestedPropertyOrderBy_SortsCorrectly` | `OrderBy(d => d.Nested.Score)` — sorting on nested property. | Medium |
| T48 | `ToFeedIterator_WithNestedPropertyProjection_ProjectsCorrectly` | `Select(d => d.Nested.Description)` | Easy |
| T49 | `ToFeedIterator_WithArrayContains_FiltersCorrectly` | `Where(d => d.Tags.Contains("a"))` — ARRAY_CONTAINS translation. | Medium |
| T50 | `MultipleRanges_AggregateMergeSortsCorrectly` | `CountAsync` / `SumAsync` with `PartitionKeyRangeCount = 3`. Aggregate fan-out merging. | Medium |
| T51 | `MultipleRanges_DistinctWorksAcrossRanges` | `Distinct()` with multiple partition ranges — SDK must merge. | Medium |
| T52 | `MultipleRanges_SkipTakeWorksAcrossRanges` | `Skip(1).Take(2)` with multiple ranges. | Medium |

---

## Tests Marked as SKIP (Too Difficult to Implement)

These will be added as `[Fact(Skip = "...")]` with a detailed skip reason, plus a
sister test showing the divergent/actual behaviour.

### SKIP-1: T41 — Ternary conditional in projection
```
[Fact(Skip = "Cosmos LINQ provider does not translate C# ternary operator (?:) to SQL. " +
    "SDK throws NotSupportedException. Not a FakeCosmosHandler issue — this is a real Cosmos SDK limitation.")]
```
**Sister test:** `ToFeedIterator_WithConditionalProjection_ShowsDivergentBehavior`  
Shows that the SDK throws `NotSupportedException` when attempting ternary in Select.
_(Will verify this is actually the SDK behaviour first — if SDK does translate it, will implement normally.)_

### SKIP-2: T42 — Null coalescing in projection
```
[Fact(Skip = "Cosmos LINQ provider does not translate C# null-coalescing operator (??) to SQL. " +
    "SDK throws NotSupportedException or generates invalid SQL. Not a FakeCosmosHandler issue.")]
```
**Sister test:** `ToFeedIterator_WithNullCoalescing_ShowsDivergentBehavior`  
_(Same verification approach — try it first, skip only if truly unsupported.)_

### SKIP-3: T43 — Math.Abs in projection
```
[Fact(Skip = "Cosmos LINQ provider does not translate Math.Abs() to SQL ABS(). " +
    "SDK may throw or generate incorrect SQL. Not a FakeCosmosHandler issue.")]
```
**Sister test:** `ToFeedIterator_WithMathOperations_ShowsDivergentBehavior`  
_(Same verification approach.)_

**IMPORTANT:** All SKIP decisions are provisional. During TDD implementation, each will
be attempted first. Only if the SDK itself rejects the LINQ expression will it be skipped.
If the SDK translates it correctly but FakeCosmosHandler/InMemoryContainer fails, that's
a **bug to fix**, not a skip.

---

## Implementation Order (TDD: Red-Green-Refactor)

### Step 0: Fix bugs
1. Remove or repurpose duplicate `WithOrderByDescending_ReversesOrder` test (BUG-1)
2. Investigate `[Collection("FeedIteratorSetup")]` necessity (BUG-2)
3. (BUG-3 is addressed by Phase 5)

### Step 1: Phase 3 — Edge cases (T17-T24, easiest, most likely green immediately)
### Step 2: Phase 1 — String operations (T01-T08, may uncover handler gaps)
### Step 3: Phase 2 — Missing LINQ operators (T09-T16)
### Step 4: Phase 5 — Response metadata validation (T36-T40)
### Step 5: Phase 4 — CRUD through real SDK (T25-T35)
### Step 6: Phase 6 — Advanced scenarios (T41-T52, hardest, most skips)

Within each step:
1. Write RED test (should fail or reveal missing functionality)
2. Implement GREEN fix in handler/container if needed
3. REFACTOR test and production code
4. If fix is too complex, mark test `[Fact(Skip = "...")]` with full reason
5. Write sister divergent behaviour test for any skipped test

---

## Documentation Updates Required

After all tests pass (or are appropriately skipped):

### Wiki Updates
1. **Known-Limitations.md** — Add any new limitations discovered during implementation
2. **Feature-Comparison-With-Alternatives.md** — Update LINQ capabilities column if new features added
3. **Features.md** — Update LINQ/FakeCosmosHandler feature descriptions if capabilities expanded
4. **Feed-Iterator-Usage-Guide.md** — Add notes about any new edge cases discovered

### README Updates
5. **README.md** — Update feature list if significant new query capabilities added

### Version & Release
6. Increment patch version: `2.0.4` → `2.0.5` in `CosmosDB.InMemoryEmulator.csproj`
7. Git commit with clear message describing all test additions and any bug fixes
8. Git tag `v2.0.5`
9. `git push` + `git push --tags`

---

## Progress Tracking

| Phase | Status | Tests Written | Tests Passing | Tests Skipped |
|-------|--------|---------------|---------------|---------------|
| Bug fixes | NOT STARTED | - | - | - |
| Phase 1: String ops | NOT STARTED | 0/8 | 0/8 | 0 |
| Phase 2: LINQ ops | NOT STARTED | 0/8 | 0/8 | 0 |
| Phase 3: Edge cases | NOT STARTED | 0/8 | 0/8 | 0 |
| Phase 4: CRUD | NOT STARTED | 0/11 | 0/11 | 0 |
| Phase 5: Metadata | NOT STARTED | 0/5 | 0/5 | 0 |
| Phase 6: Advanced | NOT STARTED | 0/12 | 0/12 | 0 |
| Docs/wiki | NOT STARTED | - | - | - |
| Version/tag/push | NOT STARTED | - | - | - |

**Total planned:** 52 new tests + 3 bug fixes + ~3 skip+divergent pairs + docs + version bump
