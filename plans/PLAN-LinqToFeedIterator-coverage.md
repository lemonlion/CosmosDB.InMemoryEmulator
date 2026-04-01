# Plan: LinqToFeedIteratorTests.cs — Coverage Deep-Dive & TDD

**Status:** PLANNING (not yet implemented)  
**Target file:** `tests/CosmosDB.InMemoryEmulator.Tests/LinqToFeedIteratorTests.cs`  
**Current version:** 2.0.4 → will become **2.0.5** after implementation  

---

## 1. Current State Analysis

### Existing Test Classes in LinqToFeedIteratorTests.cs

| Class | Tests | Coverage Area |
|-------|-------|---------------|
| `LinqToFeedIteratorTests` | 7 | ToFeedIterator throws, ToFeedIteratorOverridable with Where/OrderBy/Select/Empty, Deregister lifecycle |
| `LinqGapTests2` | 6 | Compound Where, ThenByDescending, Skip/Take, Count, FirstOrDefault, Any |
| `LinqQueryableParameterTests5` | 2 | ContinuationToken param accepted, LinqSerializerOptions param accepted |
| `LinqGapTests3` | 2 | PartitionKey filtering, Contains (IN-style) |
| `LinqGapTests` | 4 | Where equality, OrderBy, Select projection, Count |

**Total existing tests: 21**

### Key Architecture Facts

- `GetItemLinqQueryable<T>()` returns LINQ-to-Objects (`IOrderedQueryable<T>`)
- Items are deserialized via Newtonsoft.Json from internal `_items` ConcurrentDictionary
- The dummy `.OrderBy(item => 0)` satisfies the `IOrderedQueryable<T>` return type contract
- `continuationToken` and `linqSerializerOptions` parameters are **ignored** (documented as L4/limitation #11)
- `requestOptions.PartitionKey` IS respected
- `requestOptions.MaxItemCount` is NOT used in the LINQ path (only in SQL query feed iterators)
- `ToFeedIteratorOverridable()` uses AsyncLocal + static fallback + real SDK resolution chain
- `InMemoryFeedIterator<T>` supports pagination via `maxItemCount`, offset-based continuation tokens

---

## 2. Identified Gaps — New Tests to Write

### A. LINQ Operator Coverage Gaps

| # | Test Name | Category | Description | Difficulty |
|---|-----------|----------|-------------|------------|
| A1 | `Linq_OrderByDescending` | Ordering | OrderByDescending not tested in isolation | Easy |
| A2 | `Linq_ThenBy_AfterOrderBy` | Ordering | ThenBy (ascending) after OrderBy — only ThenByDescending tested | Easy |
| A3 | `Linq_SelectMany_FlattenNestedCollection` | Projection | SelectMany on Tags array — flattens nested collections | Medium |
| A4 | `Linq_Distinct_RemovesDuplicateProjections` | Filtering | `.Select(d => d.PartitionKey).Distinct()` | Easy |
| A5 | `Linq_GroupBy_ByPartitionKey` | Aggregation | GroupBy is LINQ-to-Objects so works, but Cosmos SQL doesn't support GroupBy via LINQ — mark as divergent if needed | Medium |
| A6 | `Linq_Sum_Aggregate` | Aggregation | `.Sum(d => d.Value)` | Easy |
| A7 | `Linq_Average_Aggregate` | Aggregation | `.Average(d => d.Value)` | Easy |
| A8 | `Linq_Min_Max_Aggregates` | Aggregation | `.Min(d => d.Value)` and `.Max(d => d.Value)` | Easy |
| A9 | `Linq_Last_ThrowsOrReturns` | Terminal | `.Last()` and `.LastOrDefault()` — works in LINQ-to-Objects but not in Cosmos SDK LINQ | Medium |
| A10 | `Linq_Single_WithOneMatch` | Terminal | `.Single(predicate)` and `.SingleOrDefault(predicate)` | Easy |
| A11 | `Linq_All_PredicateCheck` | Terminal | `.All(d => d.Value > 0)` | Easy |
| A12 | `Linq_Where_NullComparison` | Filtering | `.Where(d => d.Nested == null)` — null handling | Easy |
| A13 | `Linq_Where_NestedPropertyAccess` | Filtering | `.Where(d => d.Nested.Score > 5.0)` | Easy |
| A14 | `Linq_Where_StringContains` | Filtering | `.Where(d => d.Name.Contains("li"))` | Easy |
| A15 | `Linq_Where_StringStartsWith` | Filtering | `.Where(d => d.Name.StartsWith("A"))` | Easy |
| A16 | `Linq_Where_StringEndsWith` | Filtering | `.Where(d => d.Name.EndsWith("ce"))` | Easy |
| A17 | `Linq_Where_OrCondition` | Filtering | `.Where(d => d.Name == "Alice" \|\| d.Name == "Bob")` | Easy |
| A18 | `Linq_Where_NotCondition` | Filtering | `.Where(d => !(d.IsActive))` | Easy |
| A19 | `Linq_Take_WithoutOrderBy` | Pagination | `.Take(1)` without prior ordering — verify it doesn't crash | Easy |
| A20 | `Linq_Skip_WithoutTake` | Pagination | `.Skip(1)` alone — verify remaining items returned | Easy |

### B. ToFeedIteratorOverridable — Integration Gaps

| # | Test Name | Category | Description | Difficulty |
|---|-----------|----------|-------------|------------|
| B1 | `ToFeedIteratorOverridable_WithSkipTake_ReturnsPaginatedResults` | Pagination | LINQ Skip/Take piped through ToFeedIteratorOverridable | Easy |
| B2 | `ToFeedIteratorOverridable_WithCount_ThrowsBecauseNotQueryable` | Edge case | `.Count()` returns int, not `IQueryable<T>` — can't call ToFeedIteratorOverridable on it; verify correct usage pattern | Easy |
| B3 | `ToFeedIteratorOverridable_WithSelectAnonymousType_Works` | Projection | `.Select(d => new { d.Name, d.Value }).ToFeedIteratorOverridable()` | Medium |
| B4 | `ToFeedIteratorOverridable_WithPartitionKeyFilter_RespectsPartition` | Filtering | PartitionKey via requestOptions + ToFeedIteratorOverridable | Easy |
| B5 | `ToFeedIteratorOverridable_EmptyContainer_ReturnsEmptyIterator` | Edge case | No items seeded, iterator should return empty | Easy |
| B6 | `ToFeedIteratorOverridable_WithMultiplePages_IteratesCorrectly` | Pagination | Verify the InMemoryFeedIterator pagination when maxItemCount would apply — note: current ToFeedIteratorOverridable doesn't pass maxItemCount so all items come in one page | Medium |
| B7 | `ToFeedIteratorOverridable_WithCompoundLinqChain_Works` | Chaining | `.Where().OrderBy().Skip().Take().Select().ToFeedIteratorOverridable()` | Easy |
| B8 | `ToFeedIteratorOverridable_CalledMultipleTimesOnSameQueryable_EachIteratorIndependent` | Isolation | Same queryable → two iterators → both should work independently | Easy |
| B9 | `ToFeedIteratorOverridable_WithDistinct_ReturnsUniqueItems` | Dedup | `.Select(d => d.PartitionKey).Distinct().ToFeedIteratorOverridable()` | Easy |

### C. Registration / Lifecycle Gaps

| # | Test Name | Category | Description | Difficulty |
|---|-----------|----------|-------------|------------|
| C1 | `Register_SetsStaticFallbackFactory` | Setup | Verify StaticFallbackFactory is non-null after Register() | Easy |
| C2 | `Deregister_ClearsStaticFallbackFactory` | Teardown | Verify StaticFallbackFactory is null after Deregister() | Easy |
| C3 | `ToFeedIteratorOverridable_WithoutRegister_ThrowsArgumentOutOfRange` | Error path | Without Register(), calling ToFeedIteratorOverridable falls through to real SDK and throws | Easy |
| C4 | `Register_IsIdempotent_CallingTwiceDoesNotThrow` | Setup | Double Register() should be safe | Easy |

### D. Edge Cases & Error Handling

| # | Test Name | Category | Description | Difficulty |
|---|-----------|----------|-------------|------------|
| D1 | `Linq_EmptyContainer_AllOperatorsReturnEmpty` | Edge case | Where/Select/OrderBy/Count on empty container | Easy |
| D2 | `Linq_SingleItem_AllOperatorsWork` | Edge case | Verify all operators work with exactly 1 item | Easy |
| D3 | `Linq_LargeDataset_HandlesCorrectly` | Scale | 1000+ items, verify LINQ perf doesn't degrade | Easy |
| D4 | `Linq_Where_OnExpiredItem_ExcludesIt` | TTL | Item with TTL that's expired should not appear in LINQ results | Medium |
| D5 | `Linq_AfterDelete_ItemNotReturned` | Consistency | Delete item then query — should not appear | Easy |
| D6 | `Linq_AfterUpsert_ReturnUpdatedItem` | Consistency | Upsert then query — should see latest value | Easy |
| D7 | `Linq_WithNullTags_DoesNotThrow` | Null safety | Query where Tags is null default — shouldn't NPE on Contains | Easy |
| D8 | `Linq_CastToBaseType_Works` | Type safety | `GetItemLinqQueryable<dynamic>()` or base type | Medium |

### E. Divergent Behavior Tests (Skip + Sister Test Pattern)

These tests document where InMemoryContainer LINQ behavior **diverges** from real Cosmos SDK LINQ.

| # | Test Name (Skipped) | Sister Test (Passing) | Description |
|---|--------------------|-----------------------|-------------|
| E1 | `Linq_GroupBy_ShouldThrow_InRealCosmos` (SKIP) | `Linq_GroupBy_WorksInMemory_DivergentFromCosmos` | Real Cosmos LINQ doesn't support GroupBy — our LINQ-to-Objects does. Sister test shows it works in-memory with inline comment explaining the divergence. |
| E2 | `Linq_Last_ShouldThrow_InRealCosmos` (SKIP) | `Linq_Last_WorksInMemory_DivergentFromCosmos` | Real Cosmos LINQ doesn't support `.Last()` / `.LastOrDefault()`. LINQ-to-Objects does. |
| E3 | `Linq_Aggregate_ShouldThrow_InRealCosmos` (SKIP) | `Linq_CustomAggregate_WorksInMemory_DivergentFromCosmos` | `.Aggregate()` not supported by Cosmos LINQ provider, works in LINQ-to-Objects. |
| E4 | `Linq_MaxItemCount_ViaRequestOptions_ShouldLimitResults` (SKIP) | `Linq_MaxItemCount_ViaRequestOptions_IsIgnored_DivergentFromCosmos` | Real Cosmos respects `requestOptions.MaxItemCount` for LINQ; InMemoryContainer **ignores** it (items are returned in LINQ-to-Objects, pagination only applies to FeedIterator). |
| E5 | `Linq_ContinuationToken_ShouldResumeQuery` (SKIP) | `Linq_ContinuationToken_IsIgnored_DivergentFromCosmos` | ContinuationToken param is accepted but ignored; all items are returned. Already documented as L4 but no explicit passing sister test exists. |

---

## 3. Identified Bugs / Issues

| # | Issue | Severity | Details |
|---|-------|----------|---------|
| BUG1 | **Duplicate test coverage** — `LinqGapTests.Linq_Count` and `LinqGapTests2.Linq_Count_Aggregate` test the same thing | Low | Redundant — not a bug per se, but messy. Leave as-is (not asked to refactor). |
| BUG2 | **Class naming inconsistency** — `LinqGapTests`, `LinqGapTests2`, `LinqGapTests3`, `LinqQueryableParameterTests5` have poor naming and numbering gaps | Low | Not a bug. Leave as-is unless refactoring is in scope. |
| BUG3 | **Missing `[Collection("FeedIteratorSetup")]`** on `LinqGapTests2`, `LinqGapTests3`, `LinqGapTests` — if these ever use ToFeedIteratorOverridable they'd conflict with LinqToFeedIteratorTests' Register/Deregister | Low | Currently safe since those classes use `allowSynchronousQueryExecution: true` and don't touch ToFeedIteratorOverridable. But fragile. |
| BUG4 | **`ToFeedIteratorOverridable` doesn't pass `maxItemCount` to InMemoryFeedIterator** — the factory in InMemoryFeedIteratorSetup.CreateInMemoryFeedIterator calls `new InMemoryFeedIterator<T>(queryable.AsEnumerable())` without maxItemCount, so all items arrive in one page regardless | Low | This is by-design for the current usage pattern (pagination via LINQ operators), but diverges from how real `ToFeedIterator()` works with `QueryRequestOptions.MaxItemCount`. Document as divergent behavior. |
| BUG5 | **No test verifies `FeedResponse` metadata from InMemoryFeedIterator** — no test checks `StatusCode`, `RequestCharge`, `ContinuationToken`, `Count`, `Diagnostics` properties of the response returned by ToFeedIteratorOverridable | Medium | Should verify response properties match expectations. |

---

## 4. Implementation Order (TDD: Red → Green → Refactor)

### Phase 1: New LINQ Operator Tests (A-series) — Pure LINQ-to-Objects
Write tests that use `GetItemLinqQueryable<T>(allowSynchronousQueryExecution: true)` directly.

1. A1-A2: OrderByDescending, ThenBy
2. A12-A18: Where clause variants (null, nested, string ops, OR, NOT)
3. A3-A4: SelectMany, Distinct
4. A5-A11: Aggregates and terminal operators (Sum, Avg, Min, Max, Single, All, Last)
5. A19-A20: Skip/Take edge cases

**Expected: All GREEN immediately** (LINQ-to-Objects should handle all of these natively).

### Phase 2: Divergent Behavior Tests (E-series)
For each: write the "ideal" test first (RED/SKIP), then write the sister test showing actual behavior (GREEN).

1. E1: GroupBy divergence
2. E2: Last() divergence
3. E3: Aggregate() divergence
4. E4: MaxItemCount ignored divergence
5. E5: ContinuationToken ignored divergence

### Phase 3: ToFeedIteratorOverridable Integration (B-series)
Tests that pipe LINQ chains through `ToFeedIteratorOverridable`.

1. B5: Empty container
2. B1, B7: Skip/Take + compound chains
3. B3, B9: Anonymous projection, Distinct
4. B4: PartitionKey filtering
5. B6: Multi-page iteration (document divergence if maxItemCount not wired)
6. B8: Iterator independence/isolation

### Phase 4: Registration Lifecycle (C-series)
1. C1-C4: Setup/teardown lifecycle tests

### Phase 5: Edge Cases (D-series)
1. D1-D2: Empty/single-item
2. D5-D6: Delete/Upsert consistency
3. D7: Null safety
4. D4: TTL expiry
5. D8: Dynamic/base type casting
6. D3: Large dataset

### Phase 6: Response Metadata (from BUG5)
1. New test: `ToFeedIteratorOverridable_Response_HasCorrectMetadata` — verify StatusCode=OK, RequestCharge=1, Count matches, ContinuationToken=null for single-page results, Diagnostics non-null.

---

## 5. Tests That Might Be Too Difficult → Skip with Reason

| Test | Skip Reason | Sister Test |
|------|-------------|-------------|
| E1: `Linq_GroupBy_ShouldThrow_InRealCosmos` | "SKIP: Cosmos SDK LINQ provider does not support GroupBy. Real Cosmos throws NotSupportedException when translating GroupBy to SQL. InMemoryContainer uses LINQ-to-Objects where GroupBy works natively. Implementing GroupBy-to-SQL translation is out of scope for the emulator." | `Linq_GroupBy_WorksInMemory_DivergentFromCosmos` |
| E2: `Linq_Last_ShouldThrow_InRealCosmos` | "SKIP: Cosmos SDK LINQ provider does not support Last/LastOrDefault. Real Cosmos throws NotSupportedException. InMemoryContainer uses LINQ-to-Objects where Last works natively. Implementing this restriction would break legitimate in-memory usage patterns." | `Linq_Last_WorksInMemory_DivergentFromCosmos` |
| E3: `Linq_Aggregate_ShouldThrow_InRealCosmos` | "SKIP: Cosmos SDK LINQ provider does not support custom Aggregate(). Real Cosmos throws NotSupportedException. InMemoryContainer uses LINQ-to-Objects where Aggregate works natively." | `Linq_CustomAggregate_WorksInMemory_DivergentFromCosmos` |
| E4: `Linq_MaxItemCount_ViaRequestOptions_ShouldLimitResults` | "SKIP: GetItemLinqQueryable returns IOrderedQueryable (LINQ-to-Objects). The MaxItemCount in QueryRequestOptions is only used by the SQL query pipeline and FeedIterator pagination, not by the LINQ materialisation path. Wiring MaxItemCount into LINQ-to-Objects would require wrapping the queryable in a custom Take() — adding complexity for minimal real-world impact since users control pagination via .Take() in LINQ." | `Linq_MaxItemCount_ViaRequestOptions_IsIgnored_DivergentFromCosmos` |
| E5: `Linq_ContinuationToken_ShouldResumeQuery` | "SKIP: L4 — continuationToken parameter on GetItemLinqQueryable is ignored. The emulator returns all items as a LINQ-to-Objects queryable; there's no query plan to resume. Users should use .ToFeedIterator() / .ToFeedIteratorOverridable() for pagination with continuation tokens." | `Linq_ContinuationToken_IsIgnored_DivergentFromCosmos` |

---

## 6. Post-Implementation Checklist

- [ ] All new tests written and passing (or correctly skipped with sister tests)
- [ ] No regressions in existing 21 tests
- [ ] Run full test suite: `dotnet test --verbosity minimal`
- [ ] Update wiki **Known-Limitations.md**: Add any new discovered limitations (E4 MaxItemCount, E1-E3 unsupported operators)
- [ ] Update wiki **Features.md**: Add note about full LINQ operator support including GroupBy/Last/Aggregate (with divergence caveat)
- [ ] Update wiki **Feature-Comparison-With-Alternatives.md**: Add LINQ operator row if missing
- [ ] Update wiki **Known-Limitations.md**: Ensure L4 (LINQ options ignored) is still accurate; add MaxItemCount note
- [ ] Update main **README.md**: No changes likely needed (already claims full LINQ support)
- [ ] Bump version in all three `.csproj` files:
  - `src/CosmosDB.InMemoryEmulator/CosmosDB.InMemoryEmulator.csproj`: 2.0.4 → 2.0.5
  - `src/CosmosDB.InMemoryEmulator.ProductionExtensions/CosmosDB.InMemoryEmulator.ProductionExtensions.csproj`: 0.9.0 → 0.9.1 (only if code changes in that project)
  - `src/CosmosDB.InMemoryEmulator.JsTriggers/CosmosDB.InMemoryEmulator.JsTriggers.csproj`: no change expected
- [ ] Git: `git add -A && git commit -m "v2.0.5: Comprehensive LINQ-to-FeedIterator test coverage + divergent behavior documentation"`
- [ ] Git: `git tag v2.0.5 && git push && git push --tags`
- [ ] Wiki commit: `cd wiki && git add -A && git commit -m "v2.0.5: Update limitations and features for LINQ coverage" && git push`

---

## 7. Estimated New Test Count

| Phase | New Tests |
|-------|-----------|
| A: LINQ operators | 20 |
| B: ToFeedIteratorOverridable integration | 9 |
| C: Registration lifecycle | 4 |
| D: Edge cases | 8 |
| E: Divergent behavior (skipped + sister) | 10 (5 skipped + 5 passing) |
| BUG5: Response metadata | 1 |
| **Total new tests** | **52** |
| **Total after (existing + new)** | **73** |

---

## 8. Files to Modify

| File | Changes |
|------|---------|
| `tests/CosmosDB.InMemoryEmulator.Tests/LinqToFeedIteratorTests.cs` | Add all new test classes/methods |
| `src/CosmosDB.InMemoryEmulator/CosmosDB.InMemoryEmulator.csproj` | Version 2.0.4 → 2.0.5 |
| `c:\git\CosmosDB.InMemoryEmulator.wiki\Known-Limitations.md` | Add MaxItemCount note, update L4, add LINQ operator divergences |
| `c:\git\CosmosDB.InMemoryEmulator.wiki\Features.md` | Add note about GroupBy/Last/Aggregate working in-memory |
| `c:\git\CosmosDB.InMemoryEmulator.wiki\Feature-Comparison-With-Alternatives.md` | Add LINQ operator support row if missing |

---

*Plan created: ready for TDD implementation on approval.*
