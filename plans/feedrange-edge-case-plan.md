# FeedRange Edge Case Deep Dive — Implementation Plan

> **Status**: PLANNING — TDD Red-Green-Refactor approach  
> **Current version**: v2.0.4 → Target: v2.0.5  
> **Approach**: Test-first, red-green-refactor. Difficult behaviours get SKIP + divergent behavior sister test.

---

## Bugs Found

### BUG-1: Modulo vs Interval Hashing Inconsistency (MAJOR)

| Field | Detail |
|-------|--------|
| **Location** | `FakeCosmosHandler.FilterDocumentsByRange` (line ~923) |
| **Root Cause** | Uses `hash % rangeCount` (modulo) to assign items to ranges |
| **Contrast** | `InMemoryContainer.FilterByFeedRange` (line ~1320) uses interval-based `IsHashInRange(hash, min, max)` with hex boundaries |
| **Impact** | Items land in **different** per-range buckets depending on whether you query via the SDK path (`CosmosClient` → `FakeCosmosHandler`) or the `InMemoryContainer.GetItemQueryIterator(feedRange, ...)` path |
| **Example** | Hash `0x80000001` with 4 ranges: interval → range 2 (falls in `[0x80000000, 0xC0000000)`), modulo → range 1 (`0x80000001 % 4 = 1`) |
| **Fix** | Change `FakeCosmosHandler.FilterDocumentsByRange` to compute interval boundaries (`step = 0x1_0000_0000L / count`) and check `hash >= min && hash < max`, matching `InMemoryContainer.IsHashInRange` |

### BUG-2: Null PK + Change Feed + FeedRange = NullReferenceException

| Field | Detail |
|-------|--------|
| **Location** | `InMemoryContainer.FilterChangeFeedEntriesByFeedRange` (line ~1343) |
| **Root Cause** | Calls `MurmurHash3(entry.PartitionKey)` without null-check. `PartitionKeyToString(PartitionKey.None)` returns `null`, which is stored in the change feed entry's `PartitionKey` field. `Encoding.UTF8.GetBytes(null)` throws `ArgumentNullException`. |
| **Contrast** | The query path uses `ExtractPartitionKeyValueFromJson` which returns `""` for missing PK fields — no crash there |
| **Fix** | Use `entry.PartitionKey ?? ""` before calling `MurmurHash3` in `FilterChangeFeedEntriesByFeedRange` |

### BUG-3 (Potential): FakeCosmosHandler doesn't auto-sync with container.FeedRangeCount

| Field | Detail |
|-------|--------|
| **Location** | `FakeCosmosHandler(InMemoryContainer)` constructor uses `options.PartitionKeyRangeCount` (default 1), not `container.FeedRangeCount` |
| **Impact** | If user sets `container.FeedRangeCount = 4` but creates handler with default options, the handler reports 1 PKRange while the container has 4 FeedRanges |
| **Verdict** | Document with a test — may be intentional design (separate config), but users should be warned |

---

## Test Plan — 30 New Tests

### Phase 1: Hash Boundary Coverage Gaps (tests #1–#7)

All tests go in class `FeedRangeHashBoundaryTests` in `FeedRangeEdgeCaseTests.cs`.

| # | Test Name | Description | Expected Outcome |
|---|-----------|-------------|------------------|
| 1 | `FeedRanges_AreContiguous_WithEvenCounts` | Theory with [2,4,8,16] — currently only odd counts are tested | All contiguous, first starts `""`, last ends `"FF"` |
| 2 | `FeedRanges_AreContiguous_WithLargeCounts` | Theory with [64, 128, 256] — stress-test boundary generation | All 8-digit hex boundaries, contiguous |
| 3 | `ItemsWithIdenticalPKs_LandInSameRange` | 10 items all with pk="shared", FeedRangeCount=4 | All 10 appear in exactly 1 range, 0 in other 3 |
| 4 | `EmptyContainer_FeedRangeQuery_ReturnsEmpty` | FeedRangeCount=4, 0 items, query each range | Each range returns 0 items |
| 5 | `FeedRangeCount_EqualToItemCount_EachItemInOneRange` | 10 unique-PK items, FeedRangeCount=10 | Union covers all 10, each item in exactly 1 range |
| 6 | `GetFeedRangesAsync_IsIdempotent` | Call GetFeedRangesAsync twice | Identical JSON for each range |
| 7 | `FeedRange_JsonRoundTrip_PreservesBoundaries` | Serialize range → FeedRange.FromJsonString → re-serialize | Same JSON string |

### Phase 2: Partition Key Edge Cases (tests #8–#13)

All tests go in class `FeedRangePartitionKeyEdgeCaseTests`.

| # | Test Name | Description | Expected Outcome |
|---|-----------|-------------|------------------|
| 8 | `EmptyStringPK_FeedRange_ConsistentHashing` | Items with `pk=""`, FeedRangeCount=4 | All land in exactly 1 range; union = all items |
| 9 | `VeryLongPK_FeedRange_ConsistentHashing` | PK = 2000-char string, FeedRangeCount=4 | Hashes without error, lands in 1 range |
| 10 | `UnicodeAndSpecialCharsPK_FeedRange_ConsistentHashing` | PKs: emoji 🎉, CJK 中文, newline \n, tab \t | All items found across ranges, no exceptions |
| 11 | `BooleanPartitionKey_FeedRange_ConsistentHashing` | PKs: "true", "false" (as string values) | Both items found across ranges |
| 12 | `GuidPartitionKey_FeedRange_Distribution` | 50 Guid PKs, FeedRangeCount=8 | Items spread across multiple ranges (not all in 1) — verifies hash distribution |
| 13 | `ThreeLevelCompositePartitionKey_FeedRange_NoItemsLost` | 3-path hierarchical PK `/a/b/c` | **MAY SKIP**: if SDK doesn't support 3-level PK, skip with reason + sister test showing actual behavior |

### Phase 3: FeedRange Parsing Edge Cases (tests #14–#17)

All tests go in class `FeedRangeParsingTests`.

| # | Test Name | Description | Expected Outcome |
|---|-----------|-------------|------------------|
| 14 | `FeedRange_WithReversedMinMax_ReturnsAllItems` | Construct range with min="80000000", max="40000000" | Graceful: returns all items (fallback) or empty (strict) |
| 15 | `FeedRange_WithZeroWidthRange_ReturnsEmpty` | min=max="40000000" | Empty result — no hash satisfies `hash >= X && hash < X` |
| 16 | `FeedRange_CustomPartialRange_FiltersCorrectly` | User-crafted `["40000000","80000000")` | Only items whose hash falls in that sub-range |
| 17 | `FeedRange_FromPartitionKey_Behavior` | `FeedRange.FromPartitionKey(new PartitionKey("test"))` | **MAY SKIP**: FromPartitionKey produces different JSON format than EPK; skip with reason + sister test showing the JSON structure |

### Phase 4: Change Feed + FeedRange Edge Cases (tests #18–#22)

All tests go in class `FeedRangeChangeFeedEdgeCaseTests`.

| # | Test Name | Description | Expected Outcome |
|---|-----------|-------------|------------------|
| 18 | `ChangeFeed_NullPK_WithFeedRange_DoesNotThrow` | **🔴 BUG-2 RED TEST** — Item with `PartitionKey.None`, change feed with FeedRange scoping | Should NOT throw NRE; item found in exactly 1 range's change feed |
| 19 | `ChangeFeed_AfterUpdates_ItemsStayInSameRange` | Create items, `.ReplaceItemAsync()` on them, change feed per range | Updated versions appear in same range as original (PK doesn't change) |
| 20 | `ChangeFeed_AfterDeletes_TombstonesInCorrectRange` | Create then delete items, AllVersionsAndDeletes change feed per range | **MAY SKIP**: if AVAD mode not supported with FeedRange, skip + sister test |
| 21 | `ChangeFeed_ContinuationToken_WithFeedRange_ResumesCorrectly` | Read half the change feed in a range, get continuation, resume | No items missed or duplicated after resume |
| 22 | `ChangeFeed_CompositeKey_FeedRange_ConsistentWithQuery` | Composite PK items, compare per-range change feed vs per-range query | Same items appear in same ranges for both paths |

### Phase 5: FakeCosmosHandler Consistency (tests #23–#26)

All tests go in class `FakeCosmosHandlerConsistencyTests`.

| # | Test Name | Description | Expected Outcome |
|---|-----------|-------------|------------------|
| 23 | `FakeCosmosHandler_PerRange_MatchesInMemoryContainer_PerRange` | **🔴 BUG-1 RED TEST** — Query items per-range via SDK path vs per-range via InMemoryContainer | Same items in each corresponding range (WILL FAIL until BUG-1 fixed) |
| 24 | `FakeCosmosHandler_DefaultPKRangeCount_VsContainerFeedRangeCount` | container.FeedRangeCount=4, handler with default options | Document: handler has 1 range, SDK returns all items in 1 page |
| 25 | `FakeCosmosHandler_ReadFeed_WithMultipleRanges_ReturnsAllItems` | ReadFeed (not query) through SDK with PartitionKeyRangeCount=4 | All items found across ranges |
| 26 | `FakeCosmosHandler_OrderBy_WithinRange_CorrectOrder` | `ORDER BY c.name` through SDK with PartitionKeyRangeCount=4 | Items correctly ordered within each range's results |

### Phase 6: Stream API Coverage (tests #27–#28)

New class `FeedRangeStreamTests` in `FeedRangeEdgeCaseTests.cs`.

| # | Test Name | Description | Expected Outcome |
|---|-----------|-------------|------------------|
| 27 | `GetItemQueryStreamIterator_WithFeedRange_MatchesTypedIterator` | Compare stream results per range vs typed results per range | Identical item sets |
| 28 | `GetChangeFeedStreamIterator_WithFeedRange_MatchesTypedIterator` | Compare stream change feed per range vs typed change feed per range | Identical item sets |

### Phase 7: Concurrency & Stability (tests #29–#30)

New class `FeedRangeConcurrencyTests` in `FeedRangeEdgeCaseTests.cs`.

| # | Test Name | Description | Expected Outcome |
|---|-----------|-------------|------------------|
| 29 | `ConcurrentReads_AcrossDifferentFeedRanges_ThreadSafe` | `Task.WhenAll` querying all 8 ranges simultaneously | No exceptions, union = all items |
| 30 | `ItemsCreatedDuringIteration_SnapshotBehavior` | Start iterating a range, create new items mid-iteration | Well-defined behavior (no crash); document whether new items appear or not |

---

## Implementation Order (TDD Red-Green-Refactor)

### Round 1: Red Tests for Bugs
- [ ] Write test #18 (BUG-2: null PK + change feed NRE) → **expect RED**
- [ ] Write test #23 (BUG-1: modulo vs interval per-range mismatch) → **expect RED**
- [ ] Fix BUG-2 in `InMemoryContainer.FilterChangeFeedEntriesByFeedRange`: `entry.PartitionKey ?? ""` → **GREEN**
- [ ] Fix BUG-1 in `FakeCosmosHandler.FilterDocumentsByRange`: switch from modulo to interval-based logic → **GREEN**
- [ ] Run full test suite — verify no regressions

### Round 2: Hash Boundary Tests (#1–#7)
- [ ] Write tests #1–#7 → expect all **GREEN** (coverage expansion, no code changes expected)

### Round 3: Partition Key Edge Cases (#8–#13)
- [ ] Write tests #8–#12 → expect **GREEN**
- [ ] Write test #13 (3-level composite PK) → investigate; **SKIP** if needed + divergent test

### Round 4: Parsing Edge Cases (#14–#17)
- [ ] Write tests #14–#16 → expect **GREEN**
- [ ] Write test #17 (FromPartitionKey) → investigate JSON format; **SKIP** if needed + divergent test

### Round 5: Change Feed Edge Cases (#19–#22)
- [ ] Write tests #19, #21, #22 → expect **GREEN**
- [ ] Write test #20 (AVAD tombstones) → investigate; **SKIP** if needed + divergent test

### Round 6: Handler Consistency (#24–#26)
- [ ] Write tests #24–#26 → expect **GREEN**

### Round 7: Stream & Concurrency (#27–#30)
- [ ] Write tests #27–#30 → expect **GREEN**

### Round 8: Documentation & Release
- [ ] Update `Known-Limitations.md` — add note about BUG-1 fix (modulo→interval change may affect users who relied on per-range behavior through SDK)
- [ ] Update `Features.md` — tighten FeedRange section to mention SDK/handler consistency
- [ ] Update `Feature-Comparison-With-Alternatives.md` — if any comparison cells change
- [ ] Update `README.md` — if feature description changes
- [ ] Bump version in `CosmosDB.InMemoryEmulator.csproj` to `2.0.5`
- [ ] `git add -A; git commit -m "v2.0.5: Fix FeedRange hashing consistency + 30 new edge case tests"; git tag v2.0.5; git push; git push --tags`
- [ ] Push wiki changes

---

## Tests That May Need SKIP + Divergent Sister Test

| Test # | Name | Why it might be hard | Divergent behavior to document |
|--------|------|---------------------|-------------------------------|
| 13 | `ThreeLevelCompositePartitionKey` | SDK may cap at 2-level hierarchical PK | Sister test shows what happens: error? silent truncation? |
| 17 | `FeedRange_FromPartitionKey_Behavior` | `FromPartitionKey` produces JSON like `{"PK":"[\"test\"]"}` not `{"Range":{...}}` — ParseFeedRangeBoundaries would get (null,null) fallback → returns all items | Sister test shows the JSON format difference and that it returns all items (no filtering) |
| 20 | `ChangeFeed_AfterDeletes_TombstonesInCorrectRange` | AllVersionsAndDeletes mode may not be fully supported with FeedRange | Sister test shows what the change feed actually returns for deleted items per range |

---

## Files to Modify

| File | Changes |
|------|---------|
| `tests/.../FeedRangeEdgeCaseTests.cs` | Add ~30 new tests across 7 existing + 2 new test classes |
| `src/.../InMemoryContainer.cs` | BUG-2 fix: null-coalesce in `FilterChangeFeedEntriesByFeedRange` |
| `src/.../FakeCosmosHandler.cs` | BUG-1 fix: replace modulo with interval logic in `FilterDocumentsByRange` |
| `src/.../CosmosDB.InMemoryEmulator.csproj` | Version bump 2.0.4 → 2.0.5 |
| Wiki: `Known-Limitations.md` | Update/add notes about fixed behavioral differences |
| Wiki: `Features.md` | Tighten FeedRange feature description |
| Wiki: `Feature-Comparison-With-Alternatives.md` | Update if cells change |
| `README.md` | Update if feature description changes |

---

## Analysis: Why the Modulo vs Interval Bug Exists

The codebase has two independent mechanisms for partitioning items across ranges:

1. **`InMemoryContainer`** — divides the `uint32` hash space into N equal **intervals** (`[0, step)`, `[step, 2*step)`, ...) and checks which interval a hash falls into using `IsHashInRange`.

2. **`FakeCosmosHandler`** — uses `hash % N` to assign a **modulo index** to each item.

Both compute the same hex boundaries for reporting (in `GetFeedRangesAsync` and `GetPartitionKeyRanges`), but use different algorithms for the actual filtering. The boundaries are cosmetic in the handler — the real filtering is modulo-based.

**For example with 4 ranges:**
- Interval boundaries: `[0, 0x40000000)`, `[0x40000000, 0x80000000)`, `[0x80000000, 0xC0000000)`, `[0xC0000000, 0xFFFFFFFF]`
- Hash `0x80000001`: interval → range 2, modulo `% 4` → range 1 (since `0x80000001 % 4 = 1`)
- Hash `0x40000002`: interval → range 1, modulo `% 4` → range 2 (since `0x40000002 % 4 = 2`)

The union of all ranges is complete in both cases (every item appears exactly once), but the per-range assignment differs. This means if a user queries range 0 via the SDK and range 0 via `InMemoryContainer.GetItemQueryIterator(feedRange[0], ...)`, they get different items.
