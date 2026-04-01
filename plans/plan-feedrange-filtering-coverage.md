# FeedRange Filtering — Test Coverage & Bug Fix Plan

**Date:** 2026-04-01
**Current version:** 2.0.4
**Target version:** 2.0.5
**Methodology:** TDD — red-green-refactor. Write failing test first, then implement fix/feature.

---

## Status Legend

| Symbol | Meaning |
|--------|---------|
| ⬜ | Not started |
| 🔄 | In progress |
| ✅ | Completed |
| ⏭️ | Skipped (with reason) |

---

## Bugs Found

### BUG-1: FakeCosmosHandler vs InMemoryContainer per-range distribution inconsistency ⬜

**Severity:** Medium
**File:** `PartitionKeyHash.cs`, `FakeCosmosHandler.cs`, `InMemoryContainer.cs`

**Description:**
`FakeCosmosHandler.FilterDocumentsByRange` uses `PartitionKeyHash.GetRangeIndex(pkValue, count)` which is `hash % rangeCount` (modulo).
`InMemoryContainer.FilterByFeedRange` uses `IsHashInRange(hash, min, max)` which is boundary-based: `hash >= min && hash < max` where boundaries divide the uint32 space evenly (`step = 0x1_0000_0000 / count`).

These two algorithms produce **different per-range assignments** for non-power-of-2 range counts:
- Example: hash=1, rangeCount=3 → `1 % 3 = 1` (range 1) vs `1 in [0, 0x55555555)` (range 0)
- The existing test `FakeCosmosHandler_And_InMemoryContainer_ProduceConsistentRanges` only checks **union-level** consistency (all items returned), not per-range consistency.

**Fix approach:** Change `GetRangeIndex` to use boundary-based assignment matching `IsHashInRange` logic, or make `FilterDocumentsByRange` use the same boundary-parsing approach. The boundary-based approach is the correct one since it matches the FeedRange boundaries reported to the SDK.

**Test (red):** Add `FakeCosmosHandler_And_InMemoryContainer_PerRange_Consistent` test with 3 ranges (odd count) verifying each individual PKRange returns the same item set as the equivalent FeedRange.

**Implementation (green):** Update `PartitionKeyHash.GetRangeIndex` to use boundary-based division: `(int)(hash / step)` clamped to `rangeCount - 1`, where `step = 0x1_0000_0000L / rangeCount`. This aligns with `GetFeedRangesAsync` and `IsHashInRange`.

---

### BUG-2: GetChangeFeedStreamIterator uses eager evaluation for Now/Time ⬜

**Severity:** Low
**File:** `InMemoryContainer.cs`

**Description:**
`GetChangeFeedIterator<T>` uses **lazy evaluation** (closure-based `Func<List<T>>`) for `Now` and `Time` start types, meaning items added after iterator creation but before `ReadNextAsync` are included.
`GetChangeFeedStreamIterator` uses **eager evaluation** for ALL start types — it locks, snapshots, and returns immediately. This means `Now(range)` via stream won't pick up items added after creation.

**Impact:** Inconsistent behavior between typed and stream change feed iterators. Low severity because most production code uses the typed variant.

**Test (red):** Add `ChangeFeedStream_Now_WithFeedRange_IsEager_DivergentBehavior` showing that stream Now() doesn't pick up post-creation items (sister test to demonstrate divergence, skip the "correct" behavior test with reason).

**Implementation assessment:** Making stream iterators lazy is complex because `CreateStreamFeedIterator` doesn't support lazy evaluation. If too difficult, mark as skipped with detailed skip reason and create a divergent behavior test.

---

## New Tests — Grouped by Category

### Category A: FeedRangeCount Edge Cases

#### A1. FeedRangeCount_Zero_BehavesLikeOne ⬜
- Set `FeedRangeCount = 0`, call `GetFeedRangesAsync()`, expect 1 range.
- Implementation note: `Math.Max(1, FeedRangeCount)` already handles this. Test confirms.
- **File:** `FeedRangeFilteringTests.cs` — add to `FeedRangeCountTests`

#### A2. FeedRangeCount_Negative_BehavesLikeOne ⬜
- Set `FeedRangeCount = -5`, call `GetFeedRangesAsync()`, expect 1 range covering "" to "FF".
- **File:** `FeedRangeFilteringTests.cs` — add to `FeedRangeCountTests`

#### A3. FeedRangeCount_Large_ProducesContiguousRanges ⬜
- Set `FeedRangeCount = 256`, verify all 256 ranges are contiguous and cover "" to "FF".
- Seed 500 items, verify union across all ranges = 500 items, no duplicates.
- **File:** `FeedRangeFilteringTests.cs` — add to `FeedRangeCountTests`

#### A4. FeedRangeCount_ChangedAfterDataInsertion_StillFiltersCorrectly ⬜
- Create container with FeedRangeCount=1, insert 20 items, change to FeedRangeCount=4.
- Query each FeedRange, verify union = 20 items, no duplicates.
- Tests that ranges are computed dynamically, not cached at insertion time.
- **File:** `FeedRangeFilteringTests.cs` — add to `FeedRangeCountTests`

#### A5. GetFeedRangesAsync_CalledMultipleTimes_ReturnsSameRanges ⬜
- Call `GetFeedRangesAsync()` twice, verify identical range boundaries.
- **File:** `FeedRangeFilteringTests.cs` — add to `FeedRangeCountTests`

---

### Category B: Query + FeedRange Combinations

#### B1. QueryIterator_WithFeedRange_TopN_LimitsWithinRange ⬜
- FeedRangeCount=2, 50 items, query `SELECT TOP 3 * FROM c` with each range.
- Each range should return at most 3 items (not 3 from the whole container).
- **File:** `FeedRangeFilteringTests.cs` — add to `FeedRangeQueryFilteringTests`

#### B2. QueryIterator_WithFeedRange_OrderBy_SortsWithinRange ⬜
- FeedRangeCount=2, 20 items, query `SELECT * FROM c ORDER BY c.name` with a range.
- Results within each range should be sorted by name.
- **File:** `FeedRangeFilteringTests.cs` — add to `FeedRangeQueryFilteringTests`

#### B3. QueryIterator_WithFeedRange_OffsetLimit_PaginatesWithinRange ⬜
- FeedRangeCount=2, 20 items, query `SELECT * FROM c OFFSET 2 LIMIT 3` with a range.
- Should skip 2 and take 3 within the range's scoped items.
- **File:** `FeedRangeFilteringTests.cs` — add to `FeedRangeQueryFilteringTests`

#### B4. QueryIterator_WithFeedRange_Count_AggregatesWithinRange ⬜
- FeedRangeCount=4, 40 items, query `SELECT VALUE COUNT(1) FROM c` with each range.
- Sum of counts across all ranges should equal 40.
- Individual range counts should be less than 40 (at least one).
- **File:** `FeedRangeFilteringTests.cs` — add to `FeedRangeQueryFilteringTests`

#### B5. QueryIterator_WithFeedRange_Distinct_DeduplicatesWithinRange ⬜
- FeedRangeCount=2, items with duplicate names across different PKs, query `SELECT DISTINCT VALUE c.name FROM c` with each range.
- Verify DISTINCT operates within the FeedRange scope.
- **File:** `FeedRangeFilteringTests.cs` — add to `FeedRangeQueryFilteringTests`

#### B6. QueryIterator_WithFeedRange_ParameterizedQuery_FiltersCorrectly ⬜
- FeedRangeCount=2, 20 items, query `SELECT * FROM c WHERE c.name = @name` with parameter.
- Verify FeedRange + parameterized WHERE works correctly.
- **File:** `FeedRangeFilteringTests.cs` — add to `FeedRangeQueryFilteringTests`

#### B7. QueryIterator_WithFeedRange_Projection_ReturnsProjectedFields ⬜
- FeedRangeCount=2, query `SELECT c.id, c.name FROM c` with a range.
- Verify projected results still scoped to range.
- **File:** `FeedRangeFilteringTests.cs` — add to `FeedRangeQueryFilteringTests`

---

### Category C: Partition Key Varieties with FeedRange

#### C1. BooleanPartitionKey_FeedRange_NoItemsLost ⬜
- Partition key `/active` with boolean values true/false, FeedRangeCount=2.
- Insert 20 items (10 true, 10 false), verify union across ranges = 20.
- **File:** `FeedRangeEdgeCaseTests.cs` — add to `FeedRangePartitionKeyEdgeCaseTests`

#### C2. GuidPartitionKey_FeedRange_NoItemsLost ⬜
- Partition key is a GUID string, FeedRangeCount=4.
- Insert 20 items with different GUIDs, verify union = 20.
- **File:** `FeedRangeEdgeCaseTests.cs` — add to `FeedRangePartitionKeyEdgeCaseTests`

#### C3. EmptyStringPartitionKey_FeedRange_ItemNotLost ⬜
- Insert item with partition key = "" (empty string), FeedRangeCount=4.
- Verify item appears in exactly one range.
- **File:** `FeedRangeEdgeCaseTests.cs` — add to `FeedRangePartitionKeyEdgeCaseTests`

#### C4. UnicodePartitionKey_FeedRange_NoItemsLost ⬜
- Insert items with unicode PKs (emoji, CJK characters, diacritics), FeedRangeCount=4.
- Verify all items found across ranges. MurmurHash3 operates on UTF8 bytes so this should work.
- **File:** `FeedRangeEdgeCaseTests.cs` — add to `FeedRangePartitionKeyEdgeCaseTests`

#### C5. ThreeLevelHierarchicalPartitionKey_FeedRange_NoItemsLost ⬜
- Container with 3 partition key paths, FeedRangeCount=4.
- Insert 20 items, verify union = 20.
- **File:** `FeedRangeEdgeCaseTests.cs` — add to `FeedRangePartitionKeyEdgeCaseTests`

---

### Category D: Change Feed + FeedRange Advanced Scenarios

#### D1. ChangeFeed_AfterUpdates_WithFeedRange_ReturnsLatestVersion ⬜
- FeedRangeCount=4, create 10 items, update 5 of them.
- Query change feed (Incremental, Beginning) with each range.
- Union should have 10 items (latest versions, since Incremental deduplicates).
- **File:** `FeedRangeEdgeCaseTests.cs` — add to `FeedRangeChangeFeedEdgeCaseTests`

#### D2. ChangeFeed_AfterDeletes_WithFeedRange_ExcludesDeletedInIncremental ⬜
- FeedRangeCount=4, create 10 items, delete 3 of them.
- Query change feed (Incremental, Beginning) with each range.
- Union should have 7 items (deleted items excluded in Incremental mode).
- **File:** `FeedRangeEdgeCaseTests.cs` — add to `FeedRangeChangeFeedEdgeCaseTests`

#### D3. ChangeFeed_Now_ThenAddItems_TypedIterator_PicksUpNewItems ⬜
- FeedRangeCount=2, create iterator with Now(range), then add 10 items.
- Read from typed iterator — should see the 10 new items (lazy evaluation).
- This tests the lazy evaluation behavior unique to the typed iterator.
- **File:** `FeedRangeEdgeCaseTests.cs` — add to `FeedRangeChangeFeedEdgeCaseTests`

#### D4. ChangeFeedStream_Now_WithFeedRange_IsEager_DivergentBehavior ⬜
- **DIVERGENT BEHAVIOR TEST** — sister to D3.
- FeedRangeCount=2, create stream iterator with Now(range), then add items.
- Stream iterator uses eager evaluation, so it won't see post-creation items.
- **Skip the "correct lazy" test** for stream with reason: "Stream change feed iterator uses eager evaluation; typed variant uses lazy. See BUG-2."
- **File:** `FeedRangeEdgeCaseTests.cs` — add to `FeedRangeChangeFeedEdgeCaseTests`

---

### Category E: Consistency & Determinism

#### E1. SamePartitionKey_MultipleItems_AllInSameRange ⬜
- FeedRangeCount=4, insert 10 items all with same PK.
- Query each range — exactly one range should have all 10 items, others 0.
- **File:** `FeedRangeEdgeCaseTests.cs` — add to `FeedRangeHashBoundaryTests`

#### E2. FeedRange_RoundTrip_ToJsonString_FromJsonString ⬜
- Get ranges from container, serialize each to JSON string, deserialize back.
- Use deserialized range with query — should produce same results as original.
- **File:** `FeedRangeEdgeCaseTests.cs` — add to `FeedRangeParsingTests`

---

### Category F: Empty / Boundary Scenarios

#### F1. EmptyContainer_QueryWithFeedRange_ReturnsEmpty ⬜
- FeedRangeCount=4, empty container, query with each range.
- Should return 0 results without error.
- **File:** `FeedRangeEdgeCaseTests.cs` — add to `FeedRangeParsingTests`

#### F2. EmptyContainer_ChangeFeedWithFeedRange_ReturnsEmpty ⬜
- FeedRangeCount=4, empty container, change feed beginning with each range.
- Should return 0 results / HasMoreResults=false without error.
- **File:** `FeedRangeEdgeCaseTests.cs` — add to `FeedRangeChangeFeedEdgeCaseTests`

---

### Category G: FakeCosmosHandler Per-Range Consistency

#### G1. FakeCosmosHandler_And_InMemoryContainer_PerRange_Consistent ⬜
- **Tests BUG-1.** Use FeedRangeCount=3 (odd, non-power-of-2).
- Seed 50 items. For each range index:
  - Query via FakeCosmosHandler SDK path (uses GetRangeIndex/modulo internally)
  - Query via InMemoryContainer FeedRange path (uses IsHashInRange/boundary)
  - Compare per-range item sets — they should be identical.
- **This test should FAIL before BUG-1 fix and PASS after.**
- **File:** `FeedRangeEdgeCaseTests.cs` — add to `FakeCosmosHandlerConsistencyTests`

---

### Category H: Stream Iterator Parity

#### H1. QueryStreamIterator_WithFeedRange_WhereClause_FiltersCorrectly ⬜
- Mirror of typed test `QueryIterator_WithFeedRange_WhereClause_FiltersCorrectly` but using stream.
- FeedRangeCount=2, 20 items, stream query with WHERE + FeedRange.
- Union of all streamed ranges = 20.
- **File:** `FeedRangeFilteringTests.cs` — add to `FeedRangeQueryFilteringTests`

#### H2. ChangeFeedStream_Time_WithFeedRange_FiltersBothTimeAndRange ⬜
- Mirror of typed test `ChangeFeed_Time_WithFeedRange_FiltersBothTimeAndRange` but stream.
- FeedRangeCount=4, 20 early + 20 late items, stream change feed with Time + range.
- Union of late-only items across all ranges = 20.
- **File:** `FeedRangeEdgeCaseTests.cs` — add to `FeedRangeChangeFeedEdgeCaseTests`

---

## Implementation Order (TDD Sequence)

### Phase 1: Bug fixes (highest value)
1. ⬜ Write test G1 (per-range consistency) — expect RED
2. ⬜ Fix BUG-1 in `PartitionKeyHash.GetRangeIndex` — expect GREEN
3. ⬜ Verify all existing tests still pass — REFACTOR
4. ⬜ Write test D4 (stream eager divergent behavior) — document BUG-2
5. ⬜ Assess BUG-2 fix feasibility; if too complex, skip with detailed reason + divergent test

### Phase 2: FeedRangeCount edge cases (A1-A5)
6. ⬜ Write tests A1-A5 — most should be GREEN immediately (existing code handles them)
7. ⬜ Fix any failures

### Phase 3: Query + FeedRange combinations (B1-B7)
8. ⬜ Write tests B1-B7 — verify each
9. ⬜ Fix any failures (likely: aggregates with FeedRange may need attention)

### Phase 4: Partition key varieties (C1-C5)
10. ⬜ Write tests C1-C5 — most should be GREEN (MurmurHash3 is type-agnostic)
11. ⬜ Fix any failures

### Phase 5: Change feed advanced (D1-D3)
12. ⬜ Write tests D1-D3
13. ⬜ Fix any failures

### Phase 6: Consistency, empty, stream parity (E1-E2, F1-F2, H1-H2)
14. ⬜ Write tests E1-E2, F1-F2, H1-H2
15. ⬜ Fix any failures

### Phase 7: Documentation & Release
16. ⬜ Update wiki Known-Limitations.md:
    - If BUG-2 remains unfixed, add limitation about stream change feed eager evaluation
    - Remove any limitation that was fixed
17. ⬜ Update wiki Features.md:
    - Add any new tested capabilities (e.g., query operations working with FeedRange)
18. ⬜ Update wiki Feature-Comparison-With-Alternatives.md:
    - Update comparison if any capabilities changed
19. ⬜ Update README.md if needed
20. ⬜ Bump version in .csproj: 2.0.4 → 2.0.5
21. ⬜ `git add -A; git commit; git tag v2.0.5; git push; git push --tags`
22. ⬜ Update wiki: `cd wiki; git add -A; git commit; git push`

---

## Test Count Summary

| Category | New Tests | Fix Required? |
|----------|-----------|---------------|
| A: FeedRangeCount edge cases | 5 | Likely no |
| B: Query + FeedRange combos | 7 | Possibly aggregates |
| C: PK varieties | 5 | Likely no |
| D: Change feed advanced | 4 | BUG-2 assessment |
| E: Consistency & determinism | 2 | Likely no |
| F: Empty/boundary | 2 | Likely no |
| G: FakeCosmosHandler consistency | 1 | BUG-1 fix |
| H: Stream parity | 2 | Likely no |
| **Total** | **28** | **1 confirmed, 1 assessment** |

---

## Files to Modify

| File | Changes |
|------|---------|
| `tests/.../FeedRangeFilteringTests.cs` | Add A1-A5, B1-B7, H1 |
| `tests/.../FeedRangeEdgeCaseTests.cs` | Add C1-C5, D1-D4, E1-E2, F1-F2, G1, H2 |
| `src/.../PartitionKeyHash.cs` | Fix BUG-1: change GetRangeIndex to boundary-based |
| `src/.../InMemoryContainer.cs` | Possibly fix BUG-2 (lazy stream iterators) |
| `src/.../CosmosDB.InMemoryEmulator.csproj` | Version bump 2.0.4 → 2.0.5 |
| Wiki: `Known-Limitations.md` | Update for BUG-2 if skipped |
| Wiki: `Features.md` | Add new tested capabilities |
| Wiki: `Feature-Comparison-With-Alternatives.md` | Update if needed |
| `README.md` | Update if needed |
