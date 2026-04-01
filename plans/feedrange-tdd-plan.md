# FeedRange Deep Dive — TDD Plan

> **Version:** v2.0.5 (patch increment from v2.0.4)
> **Status:** PLAN ONLY — not yet implemented
> **Approach:** TDD — red-green-refactor. Write test first, see it fail, implement fix, see it pass.
> **Target file:** `tests/CosmosDB.InMemoryEmulator.Tests/FeedRangeTests.cs` (new tests added here, existing split files kept as-is)
> **Divergent tests:** Tests too hard to implement get `[Fact(Skip = "...")]` with a detailed reason and a passing sister test showing actual emulator behavior.

---

## Existing Coverage Summary

### FeedRangeTests.cs (5 tests)
- `GetFeedRanges_ReturnsSingleRange_ByDefault`
- `GetFeedRanges_AlwaysReturnsSingleRange` (with 100 items)
- `GetFeedRanges_WithMultipleRanges_ReturnsMultiple` (FeedRangeCount=4)
- `FeedRange_UsableWithQueryIterator`
- `FeedRange_UsableWithChangeFeedIterator`
- `GetFeedRanges_DefaultsSingle_SetFeedRangeCountForMultiple` (divergent doc)

### FeedRangeFilteringTests.cs (11 tests)
- Count defaults/configured/single
- FeedRangeEpk structure validation
- Full range coverage and contiguity
- Query iterator filtering (typed + stream + WHERE clause)
- Change feed Beginning/Now/stream scoping
- Subset-not-all validation

### FeedRangeEdgeCaseTests.cs (14 tests)
- Odd count contiguity (3, 5, 7)
- Item uniqueness across ranges (50 items, 100 ranges)
- 8-digit hex boundary format
- Composite PK / null PK / numeric PK
- Malformed FeedRange JSON graceful fallback
- Change feed Time+FeedRange, Beginning without FeedRange, pagination
- FakeCosmosHandler consistency
- PartitionKeyHash unit tests (RangeBoundaryToHex, GetRangeIndex)

---

## Bugs Found

### BUG-1: Composite PK hashing inconsistency between query and change feed paths (POTENTIAL)
- **Location:** `InMemoryContainer.cs` — `FilterByFeedRange` vs `FilterChangeFeedEntriesByFeedRange`
- **Detail:** `FilterByFeedRange` calls `ExtractPartitionKeyValueFromJson(json)` which joins ALL PK path values with `|`, substituting `""` for null components. `FilterChangeFeedEntriesByFeedRange` uses `entry.PartitionKey` which was set via `ExtractPartitionKeyValue(PartitionKey, JObject)` → `PartitionKeyToString()`. For composite PKs with null components, `PartitionKeyToString` may produce a different string than `ExtractPartitionKeyValueFromJson` because `ExtractPartitionKeyValue` only joins *non-null* parts when extracting from the document.
- **Impact:** An item could appear in range X via a query iterator but range Y via a change feed iterator if its composite PK has null components.
- **Test strategy:** Create items with composite PKs where one component is null, query via both paths with FeedRange, compare range assignment.
- **Fix complexity:** Low — unify the PK extraction logic.

### BUG-2: Change feed stream iterator doesn't use lazy evaluation for "Now" start
- **Location:** `InMemoryContainer.cs` — `GetChangeFeedStreamIterator`
- **Detail:** The typed `GetChangeFeedIterator<T>` uses lazy evaluation (`InMemoryFeedIterator<T>(() => {...})`) for "Now" and "Time" start types so items added after iterator creation are visible. The stream version `GetChangeFeedStreamIterator` evaluates eagerly via `FilterChangeFeedByStartFrom`, meaning items added after "Now" iterator creation are never seen.
- **Impact:** Code using `GetChangeFeedStreamIterator(ChangeFeedStartFrom.Now(range))` won't see new items, while the typed version will. Behavioral asymmetry.
- **Test strategy:** Create stream iterator from Now(range), add items, read — expect them to appear (will fail = RED).
- **Fix complexity:** Medium — need to make the stream iterator support lazy evaluation too, or document as limitation.

---

## New Tests — Organized by Category

### Category 1: FeedRangeCount Validation Edge Cases

#### T1.1: FeedRangeCount_Zero_ClampedToOne
- **What:** Set `FeedRangeCount = 0`, call `GetFeedRangesAsync()`.
- **Expect:** Returns 1 range (clamped by `Math.Max(1, FeedRangeCount)`).
- **Status:** Should pass (implementation already clamps). Write to confirm.

#### T1.2: FeedRangeCount_Negative_ClampedToOne
- **What:** Set `FeedRangeCount = -5`, call `GetFeedRangesAsync()`.
- **Expect:** Returns 1 range.
- **Status:** Should pass (clamped). Write to confirm.

#### T1.3: FeedRangeCount_VeryLarge_256Ranges
- **What:** Set `FeedRangeCount = 256`, seed 500 items, verify all items accounted for.
- **Expect:** 256 contiguous ranges, union of all ranges = all 500 items, no duplicates.
- **Status:** Should pass. Validates large range counts don't break hash distribution.

#### T1.4: FeedRangeCount_Two_SmallestMultiPartition
- **What:** Set `FeedRangeCount = 2`, seed 50 items, verify split.
- **Expect:** 2 ranges, each holding a subset, union = all items.
- **Status:** Should pass. Not explicitly covered for count=2.

#### T1.5: FeedRangeCount_ChangedBetweenCalls_ProducesDifferentRanges
- **What:** Set `FeedRangeCount = 2`, get ranges. Set to `4`, get ranges again.
- **Expect:** First call → 2 ranges. Second call → 4 ranges. Boundaries differ. Items still correctly distributed in new ranges.
- **Status:** Should pass but needs explicit test.

---

### Category 2: Partition Affinity & Hashing Consistency

#### T2.1: SamePartitionKey_AlwaysMapsToSameRange
- **What:** Create 10 items with the same PK, FeedRangeCount=4. Query each range.
- **Expect:** All 10 items appear in exactly 1 range, the other 3 ranges have 0 of these items.
- **Status:** Should pass. Important property not explicitly tested.

#### T2.2: MurmurHash3_Deterministic_SameInputSameOutput
- **What:** Call `PartitionKeyHash.MurmurHash3("test")` multiple times.
- **Expect:** Same result every time.
- **Status:** Should pass. Unit test for hash determinism.

#### T2.3: HashDistribution_RoughlyEven_With1000Items
- **What:** FeedRangeCount=4, 1000 items with unique PKs. Count items per range.
- **Expect:** No range has 0 items, no range has > 500 items (rough evenness).
- **Status:** Should pass if hash function has good distribution.

#### T2.4: QueryAndChangeFeed_SameItem_InSameRange
- **What:** FeedRangeCount=4, create items. For each range, get items via query iterator AND change feed iterator. Compare.
- **Expect:** Both paths produce identical sets for each range.
- **Status:** Should pass for simple string PKs. This validates cross-path consistency.

#### T2.5: QueryAndChangeFeed_CompositePK_SameRange — BUG-1 TEST
- **What:** FeedRangeCount=4, create items with composite PKs (including one with a null component). For each range, get items via query AND change feed. Compare.
- **Expect:** Both paths produce identical sets. **May fail if BUG-1 is real.**
- **Status:** RED if bug exists → fix → GREEN.

---

### Category 3: Partition Key Type Edge Cases

#### T3.1: EmptyStringPartitionKey_LandsInOneRange
- **What:** Create item with PK = `""` (empty string), FeedRangeCount=4.
- **Expect:** Item appears in exactly 1 range.
- **Status:** Should pass.

#### T3.2: UnicodePartitionKey_LandsInOneRange
- **What:** Create items with PKs containing Unicode (emoji, CJK, Arabic).
- **Expect:** Each item in exactly 1 range, union = all items.
- **Status:** Should pass (MurmurHash3 works on UTF-8 bytes).

#### T3.3: VeryLongPartitionKey_LandsInOneRange
- **What:** Create item with PK = 1000-char string, FeedRangeCount=4.
- **Expect:** Item appears in exactly 1 range.
- **Status:** Should pass.

#### T3.4: BooleanPartitionKey_LandsInOneRange
- **What:** Create items with `new PartitionKey(true)` and `new PartitionKey(false)`, FeedRangeCount=4.
- **Expect:** Each in exactly 1 range.
- **Status:** Should pass — depends on how PartitionKeyToString handles booleans. Needs verification.

#### T3.5: HierarchicalPartitionKey_ThreeLevel_NoItemsLost
- **What:** Create items with 3-level hierarchical PK, FeedRangeCount=4.
- **Expect:** All items found across ranges, no duplicates.
- **Status:** Should pass — composite PK test with 3 levels.

---

### Category 4: Query Features with FeedRange

#### T4.1: Aggregate_COUNT_WithFeedRange_CorrectPerRange
- **What:** FeedRangeCount=4, seed 20 items. For each range, run `SELECT VALUE COUNT(1) FROM c`.
- **Expect:** Sum of all range counts = 20. Each range count >= 0.
- **Status:** Should pass (aggregates run on already-filtered items).

#### T4.2: Aggregate_SUM_WithFeedRange
- **What:** FeedRangeCount=2, seed items with known values. Sum per range.
- **Expect:** Sum of sums across ranges = total sum.
- **Status:** Should pass.

#### T4.3: OrderBy_WithFeedRange_OrdersWithinRange
- **What:** FeedRangeCount=2, `SELECT * FROM c ORDER BY c.name ASC` scoped to a range.
- **Expect:** Results within that range are correctly ordered.
- **Status:** Should pass.

#### T4.4: Top_WithFeedRange
- **What:** FeedRangeCount=2, `SELECT TOP 3 * FROM c` scoped to a range.
- **Expect:** Returns at most 3 items from that range.
- **Status:** Should pass.

#### T4.5: OffsetLimit_WithFeedRange
- **What:** FeedRangeCount=2, `SELECT * FROM c OFFSET 2 LIMIT 3` scoped to a range.
- **Expect:** Correct pagination within the range's items.
- **Status:** Should pass.

#### T4.6: Distinct_WithFeedRange
- **What:** FeedRangeCount=2, items with duplicate names. `SELECT DISTINCT c.name FROM c` per range.
- **Expect:** Distinct within each range. Union may have duplicates across ranges (matches real Cosmos behavior).
- **Status:** Should pass.

#### T4.7: GroupBy_WithFeedRange
- **What:** FeedRangeCount=2, `SELECT c.partitionKey, COUNT(1) as cnt FROM c GROUP BY c.partitionKey` per range.
- **Expect:** Groups only include items in that range.
- **Status:** Should pass.

---

### Category 5: Change Feed + FeedRange Advanced Scenarios

#### T5.1: ChangeFeed_Updates_ScopedToFeedRange
- **What:** FeedRangeCount=4, create items, then upsert some. Get change feed from beginning per range.
- **Expect:** Incremental mode returns latest version per item, scoped to the correct range.
- **Status:** Should pass.

#### T5.2: ChangeFeed_Deletes_ScopedToFeedRange
- **What:** FeedRangeCount=4, create items, delete some. Get change feed with `GetChangeFeedIterator<T>(checkpoint)` (all versions mode).
- **Expect:** Delete tombstones appear in the correct range.
- **Status:** Needs verification — checkpoint-based iterator doesn't take FeedRange.
- **Note:** May need to be skipped if checkpoint iterator doesn't support FeedRange.

#### T5.3: ChangeFeedStreamIterator_Now_WithFeedRange_SeesNewItems — BUG-2 TEST
- **What:** FeedRangeCount=4. Create stream iterator from `ChangeFeedStartFrom.Now(range)`. Add items. Read.
- **Expect:** New items visible.
- **Status:** **RED — will fail due to BUG-2 (eager evaluation)**. Fix or document.
- **Skip reason if too hard:** "GetChangeFeedStreamIterator evaluates eagerly for Now/Time starts, unlike the typed GetChangeFeedIterator<T> which uses lazy evaluation. Stream iterator cannot see items added after creation. Use typed iterator instead."
- **Sister test:** `ChangeFeedTypedIterator_Now_WithFeedRange_SeesNewItems` — shows the typed version works correctly.

#### T5.4: ChangeFeed_FullDelta_Mode_WithFeedRange
- **What:** If `ChangeFeedMode.FullFidelity` or `AllVersionsAndDeletes` is available, test with FeedRange.
- **Expect:** Filter applies to all versions mode too.
- **Status:** Depends on SDK version. May need to skip.

#### T5.5: ChangeFeed_EmptyRange_ReturnsNotModified
- **What:** FeedRangeCount=4, add items to only some PKs such that at least one range has no items. Get change feed for empty range.
- **Expect:** StatusCode = NotModified (304), no items.
- **Status:** Should pass.

---

### Category 6: Empty Container & Edge Cases

#### T6.1: EmptyContainer_FeedRangeQuery_ReturnsEmpty
- **What:** FeedRangeCount=4, no items. Query each range.
- **Expect:** All ranges return 0 items.
- **Status:** Should pass.

#### T6.2: EmptyContainer_FeedRangeChangeFeed_ReturnsEmpty
- **What:** FeedRangeCount=4, no items. Get change feed per range from beginning.
- **Expect:** All ranges return 0 items.
- **Status:** Should pass.

#### T6.3: SingleItem_HighRangeCount_MostRangesEmpty
- **What:** FeedRangeCount=50, create 1 item. Query all ranges.
- **Expect:** Exactly 1 range has the item, 49 return empty.
- **Status:** Should pass (already partially tested with 100 ranges).

#### T6.4: AllItems_SamePartitionKey_OnlyOneRangePopulated
- **What:** FeedRangeCount=4, create 20 items all with PK="same".
- **Expect:** All 20 items in exactly 1 range. Other 3 ranges empty.
- **Status:** Should pass. Important edge case.

---

### Category 7: FeedRange Interop & Error Cases

#### T7.1: NullFeedRange_QueryReturnsAllItems
- **What:** Call `GetItemQueryIterator<T>(null, queryDef)` where the FeedRange overload is used with null.
- **Expect:** Returns all items (no filtering).
- **Status:** Should pass — `ParseFeedRangeBoundaries(null)` returns `(null, null)`.

#### T7.2: FeedRange_FromPartitionKey_UsedWithQueryIterator
- **What:** Create a FeedRange via `FeedRange.FromPartitionKey(new PartitionKey("pk1"))`, use with query iterator.
- **Expect:** Returns only items with that PK, OR falls back to all items (since it's a different FeedRange format).
- **Status:** Needs investigation. `ParseFeedRangeBoundaries` expects Range/min/max JSON; `FeedRange.FromPartitionKey` produces `{"PK":...}` format, which would fall to the catch block → returns all items. This is a known limitation.
- **Skip reason:** "FeedRange.FromPartitionKey creates a PK-based FeedRange (not EPK-based). ParseFeedRangeBoundaries only handles EPK ranges with Range/min/max JSON. PK-based FeedRanges fall back to returning all items. Real Cosmos DB correctly scopes to the single partition."
- **Sister test:** Show that EPK-based FeedRanges from GetFeedRangesAsync work correctly.

#### T7.3: FeedRange_CreatedManually_ValidHex_WorksCorrectly
- **What:** Create FeedRange from JSON `{"Range":{"min":"00000000","max":"80000000"}}` manually. Query with it.
- **Expect:** Only items whose PK hash falls in [0x00000000, 0x80000000) are returned.
- **Status:** Should pass.

#### T7.4: FeedRange_OverlappingRanges_ItemsCanAppearInBoth
- **What:** Create two overlapping manual FeedRanges. Query with each.
- **Expect:** Items in the overlap appear in both results. (This tests that ranges are independent filters.)
- **Status:** Should pass.

#### T7.5: FeedRange_WithPartitionKeyInRequestOptions_BothApply
- **What:** FeedRangeCount=4. Query with both a FeedRange AND `QueryRequestOptions.PartitionKey`.
- **Expect:** Both filters apply — items must match the PK AND fall within the range.
- **Status:** Needs investigation. The PK filter is applied in `FilterItemsByQuery` and the FeedRange filter in `FilterByFeedRange`. Both should apply sequentially.

---

### Category 8: FakeCosmosHandler Integration

#### T8.1: FakeCosmosHandler_QueryThroughSDK_MatchesFeedRangeQuery
- **What:** Already covered by `FakeCosmosHandler_And_InMemoryContainer_ProduceConsistentRanges` but this test specifically validates that FeedRange-scoped SDK queries (if possible) match direct container queries.
- **Status:** Skip — the SDK doesn't expose a direct FeedRange-scoped query through the standard client. Existing test covers the important case.

---

## Documentation Updates (Post-Implementation)

### Wiki Updates
1. **Known-Limitations.md** — Add entry for FeedRange.FromPartitionKey not supported (T7.2)
2. **Known-Limitations.md** — Add entry for change feed stream iterator eager evaluation (BUG-2, if not fixed)
3. **Features.md** — Update FeedRange section with new edge cases covered
4. **Feature-Comparison-With-Alternatives.md** — Update if any new limitations are added

### README.md
- No changes needed unless a major new limitation is found

### Version
- Increment to `v2.0.5` in `CosmosDB.InMemoryEmulator.csproj`
- `git tag v2.0.5`
- `git push && git push --tags`

---

## Implementation Order

1. **Write all tests first** (RED phase for new tests)
2. **Fix BUG-1** if confirmed (composite PK hashing inconsistency)
3. **Decide on BUG-2** (fix or document as limitation with skip + sister test)
4. **GREEN phase** — make all tests pass
5. **Refactor** — clean up any duplication
6. **Update docs** — wiki, README as needed
7. **Version bump, tag, push**

---

## Test Execution Plan

### Phase 1: Categories 1 + 2 (Configuration & Hashing)
- T1.1–T1.5, T2.1–T2.5
- BUG-1 confirmed or refuted here

### Phase 2: Categories 3 + 6 (PK Types & Empty/Edge)
- T3.1–T3.5, T6.1–T6.4

### Phase 3: Categories 4 + 5 (Query Features & Change Feed)
- T4.1–T4.7, T5.1–T5.5
- BUG-2 confirmed and resolved here

### Phase 4: Category 7 (Interop & Errors)
- T7.1–T7.5

### Phase 5: Docs & Release
- Wiki updates, version bump, tag, push

---

## Progress Tracker

| Test ID | Description | Status |
|---------|-------------|--------|
| T1.1 | FeedRangeCount=0 clamped to 1 | ⬜ Not started |
| T1.2 | FeedRangeCount=-5 clamped to 1 | ⬜ Not started |
| T1.3 | FeedRangeCount=256, 500 items | ⬜ Not started |
| T1.4 | FeedRangeCount=2, 50 items | ⬜ Not started |
| T1.5 | FeedRangeCount changed mid-flight | ⬜ Not started |
| T2.1 | Same PK → same range | ⬜ Not started |
| T2.2 | MurmurHash3 determinism | ⬜ Not started |
| T2.3 | Hash distribution evenness | ⬜ Not started |
| T2.4 | Query vs change feed consistency | ⬜ Not started |
| T2.5 | Composite PK consistency (BUG-1) | ⬜ Not started |
| T3.1 | Empty string PK | ⬜ Not started |
| T3.2 | Unicode PK | ⬜ Not started |
| T3.3 | Very long PK | ⬜ Not started |
| T3.4 | Boolean PK | ⬜ Not started |
| T3.5 | 3-level hierarchical PK | ⬜ Not started |
| T4.1 | COUNT with FeedRange | ⬜ Not started |
| T4.2 | SUM with FeedRange | ⬜ Not started |
| T4.3 | ORDER BY with FeedRange | ⬜ Not started |
| T4.4 | TOP with FeedRange | ⬜ Not started |
| T4.5 | OFFSET/LIMIT with FeedRange | ⬜ Not started |
| T4.6 | DISTINCT with FeedRange | ⬜ Not started |
| T4.7 | GROUP BY with FeedRange | ⬜ Not started |
| T5.1 | Change feed updates + FeedRange | ⬜ Not started |
| T5.2 | Change feed deletes + FeedRange | ⬜ Not started |
| T5.3 | Stream iterator Now lazy eval (BUG-2) | ⬜ Not started |
| T5.4 | AllVersionsAndDeletes + FeedRange | ⬜ Not started |
| T5.5 | Empty range → NotModified | ⬜ Not started |
| T6.1 | Empty container query | ⬜ Not started |
| T6.2 | Empty container change feed | ⬜ Not started |
| T6.3 | 1 item / 50 ranges | ⬜ Not started |
| T6.4 | All items same PK | ⬜ Not started |
| T7.1 | Null FeedRange → all items | ⬜ Not started |
| T7.2 | FeedRange.FromPartitionKey (skip+sister) | ⬜ Not started |
| T7.3 | Manual valid hex FeedRange | ⬜ Not started |
| T7.4 | Overlapping ranges | ⬜ Not started |
| T7.5 | FeedRange + PK RequestOptions | ⬜ Not started |
| BUG-1 | Composite PK hash fix | ⬜ Not started |
| BUG-2 | Stream iterator lazy eval fix/doc | ⬜ Not started |
| DOCS | Wiki / comparison / features | ⬜ Not started |
| RELEASE | v2.0.5 tag + push | ⬜ Not started |
