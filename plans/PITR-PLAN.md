# Point-in-Time Restore (PITR) — Deep Dive Test Coverage & Bug Fix Plan

## Current State

**Version:** 2.0.4  
**Test file:** `tests/CosmosDB.InMemoryEmulator.Tests/PointInTimeRestoreTests.cs`  
**Implementation:** `src/CosmosDB.InMemoryEmulator/InMemoryContainer.cs` lines 237–276

### Existing Tests (9)

| # | Test Name | What It Covers |
|---|-----------|----------------|
| 1 | `RestoreToPointInTime_RestoresItemsAsOfGivenTimestamp` | Basic: create 2 items, restore to before 2nd → only 1st remains |
| 2 | `RestoreToPointInTime_RestoresDeletedItems` | Delete item, restore to before delete → item reappears |
| 3 | `RestoreToPointInTime_RestoresOverwrittenValues` | Upsert overwrites, restore → original values |
| 4 | `RestoreToPointInTime_BeforeAnyData_ResultsInEmptyContainer` | Restore to before any writes → empty container |
| 5 | `RestoreToPointInTime_MultiplePartitionKeys_RestoresCorrectly` | Items across pk1/pk2, create/delete after restore point → both PKs correct |
| 6 | `RestoreToPointInTime_PreservesChangeFeedHistory` | New items can be created after restore, change feed works |
| 7 | `RestoreToPointInTime_MultipleUpdatesToSameItem_RestoresCorrectVersion` | V1→V2→V3, restore to after V2 → gets V2 |
| 8 | `RestoreToPointInTime_ItemCreatedAndDeletedBeforeRestorePoint_StaysDeleted` | Item created+deleted before restore point → stays gone |
| 9 | `RestoreToPointInTime_WithPatchOperations_RestoresPrePatchState` | Patch Set/Increment, restore → original values |

---

## Bugs Found

### BUG 1: Stale `_etag` and `_ts` in Restored JSON Body

**Severity:** Medium  
**Location:** `InMemoryContainer.RestoreToPointInTime()` lines 268–275

**Problem:** During restore, the code sets:
- `_etags[key]` = new GUID (fresh ETag)
- `_timestamps[key]` = `pointInTime`
- `_items[key]` = **original JSON from change feed** (contains the OLD `_etag` and `_ts` values)

This means:
- `response.ETag` (from `_etags` dictionary) ≠ `response.Resource._etag` (from JSON body)
- `_ts` in the JSON body reflects original write time, but `_timestamps[key]` is `pointInTime`

After a normal `CreateItemAsync`, these are consistent because `EnrichWithSystemProperties` writes the same `_etag`/`_ts` into both the JSON and the dictionaries. PITR breaks this invariant.

**Fix:** After setting the new `_etag` and `_timestamps` for a restored item, re-enrich the JSON:
```csharp
// In RestoreToPointInTime, inside the foreach loop:
var etag = $"\"{Guid.NewGuid()}\"";
_items[key] = EnrichWithSystemProperties(kvp.Value.Json, etag, pointInTime);
_etags[key] = etag;
_timestamps[key] = pointInTime;
```

**TDD approach:** Write a test first that reads an item after restore and asserts `response.ETag` matches the `_etag` inside the deserialized JSON body. It will fail (RED). Then apply the fix (GREEN).

---

## Missing Test Coverage

### Category A: Core Functionality Gaps

#### TEST A1: `RestoreToPointInTime_WithReplaceItem_RestoresPreReplaceState`
- **Why:** Existing tests cover Upsert and Patch but NOT `ReplaceItemAsync`. Replace uses a different code path (requires item to exist, different conflict semantics).
- **Approach:** Create item, capture restore point, `ReplaceItemAsync` with new data, restore, verify original data.
- **Difficulty:** Easy — should just work.

#### TEST A2: `RestoreToPointInTime_QueriesWorkAfterRestore`
- **Why:** No test verifies SQL queries work on restored data. The restore clears `_items` and repopulates — need to confirm the query engine can read restored state.
- **Approach:** Create items, restore, run `SELECT * FROM c WHERE c.name = 'Alice'`, verify result.
- **Difficulty:** Easy.

#### TEST A3: `RestoreToPointInTime_ETagsAreRegeneratedAfterRestore`
- **Why:** After restore, ETags should be fresh. Old ETags from before restore should be invalid for conditional operations. No test validates this.
- **Approach:** Create item, save ETag, restore, try `ReplaceItemAsync` with old ETag via `IfMatchEtag` → expect 412 Precondition Failed.
- **Difficulty:** Easy — should just work.
- **Also validates BUG 1** — read item after restore, assert `response.ETag` equals `_etag` in JSON body.

#### TEST A4: `RestoreToPointInTime_WithStreamOperations_RestoresCorrectly`
- **Why:** Stream variants (`CreateItemStreamAsync`, `UpsertItemStreamAsync`, etc.) use the same `RecordChangeFeed` path, but this isn't tested.
- **Approach:** Use `CreateItemStreamAsync` to create items, restore, verify.
- **Difficulty:** Easy.

### Category B: Edge Cases & Boundary Conditions

#### TEST B1: `RestoreToPointInTime_ToFutureTimestamp_KeepsAllItems`
- **Why:** What happens when `pointInTime` is in the future? All change feed entries pass the `<= pointInTime` filter, so all items should remain. No test covers this.
- **Approach:** Create items, restore to `DateTimeOffset.UtcNow.AddHours(1)`, verify all items present.
- **Difficulty:** Easy.

#### TEST B2: `RestoreToPointInTime_ToExactOperationTimestamp_IncludesOperation`
- **Why:** The filter uses `<=` (inclusive). Need to verify that an item created at exactly `T` is included when restoring to `T`. This is a boundary condition.
- **Approach:** This is tricky because `RecordChangeFeed` captures `DateTimeOffset.UtcNow` at call time, not a passed-in timestamp. We can't easily set the exact timestamp. Instead, test by capturing the timestamp range and verifying the `<=` semantics hold via an item that exists and one that doesn't.
- **Difficulty:** Medium — may need creative approach (e.g., small delay and restore to `UtcNow` after first op but before second).

#### TEST B3: `RestoreToPointInTime_DoubleRestoreToSamePoint_IsIdempotent`
- **Why:** Restoring twice to the same point should yield identical state. After the first restore, the change feed still has all historical entries, and the restore clears `_items`/`_etags`/`_timestamps` before replaying. So a second restore should produce the same result.
- **Approach:** Create items, modify, restore to T, record item count + values, restore to T again, assert identical.
- **Difficulty:** Easy.

#### TEST B4: `RestoreToPointInTime_ConsecutiveRestoresToDifferentPoints`
- **Why:** Restore to T1 (earlier), then to T2 (later), then back to T1. Each restore should replay the full change feed from scratch. No test covers this.
- **Approach:** Create item at T0, modify at T1, modify at T2, create new item at T3. Restore to T2 → 1 item with T2 state. Restore to T3 → 2 items. Restore to T1 → 1 item with T1 state.
- **Difficulty:** Easy.

#### TEST B5: `RestoreToPointInTime_WithPartitionKeyNone_RestoresCorrectly`
- **Why:** Items with `PartitionKey.None` go through `ExtractPartitionKeyValue` which falls back to the `id` field. Need to confirm PITR handles this.
- **Approach:** Create items with `PartitionKey.None`, restore, verify.
- **Difficulty:** Easy.

#### TEST B6: `RestoreToPointInTime_WithHierarchicalPartitionKeys_RestoresCorrectly`
- **Why:** Composite partition keys are stored pipe-delimited in the change feed (`"pk1|pk2"`). Need to verify PITR correctly matches these keys during replay.
- **Approach:** Create container with composite PK paths, create items, modify after restore point, restore, verify.
- **Difficulty:** Medium — need composite PK setup.

#### TEST B7: `RestoreToPointInTime_EmptyContainer_NoOpRestore`
- **Why:** Calling restore on an empty container should be a no-op. No items, no change feed entries → empty result.
- **Approach:** Create empty container, restore to any time, assert still empty.
- **Difficulty:** Easy.

### Category C: Interaction With Other Features

#### TEST C1: `RestoreToPointInTime_AfterClearItems_RestoresToEmpty`
- **Why:** `ClearItems()` wipes the change feed. A subsequent PITR should find no entries and result in an empty container, regardless of what was there before.
- **Approach:** Create items, `ClearItems()`, try restore to a point when items existed → empty because change feed was wiped.
- **Difficulty:** Easy — validates an important invariant.

#### TEST C2: `RestoreToPointInTime_AfterImportState_HasNoHistory`
- **Why:** `ImportState()` calls `ClearItems()` internally, destroying the change feed. Items added via import have no change feed entries. PITR should only see post-import operations.
- **Approach:** Create items, export state, import state, try restore to pre-import → empty. Create new item after import, capture time, create another, restore → only first new item.
- **Difficulty:** Easy.

#### TEST C3: `RestoreToPointInTime_TTLExpiredItem_IsResurrectedByRestore`
- **Why:** TTL eviction does NOT write to `_changeFeed` (no delete tombstone). The original create is still in the change feed. PITR replay will resurrect the item.
- **Approach:** Set `DefaultTimeToLive = 1`, create item, wait for expiry, verify item is gone, restore to when it existed → item is back.
- **Difficulty:** Medium — needs TTL timing.
- **Behavioral note:** This is faithful to real Cosmos PITR behavior — continuous backup retains items independently of TTL.

#### TEST C4: `RestoreToPointInTime_RestoredItemsTTLTimerResetsToRestorePoint`
- **Why:** After restore, `_timestamps[key] = pointInTime`. If TTL is configured, the expiry timer counts from `pointInTime`, not from the original create time. For a restore point that was, say, 30 seconds ago, an item with TTL=60 would have 30 seconds already elapsed.
- **Approach:** Set `DefaultTimeToLive = 2`, create item, wait 1s, capture restore point, wait 1s more (item expires), restore to restore point. Item should exist but have ~1s already elapsed on the TTL (since _timestamps is set to restore point, which was 1s after creation, but the elapsed time calc is based on _timestamps vs UtcNow).
- **Difficulty:** Hard — timing-sensitive. May skip if flaky.

#### TEST C5: `RestoreToPointInTime_AfterFailedTransactionalBatch_GhostEntriesInChangeFeed`
- **Why:** When a transactional batch fails, `RestoreSnapshot()` rolls back `_items` and `_etags` but does NOT remove the `_changeFeed` entries for operations that were executed before the failure. These "ghost" entries mean PITR could resurrect items that were never actually committed.
- **Approach:** Create batch with one successful create and one conflicting create (409). Batch fails, state rolled back. But change feed has the first create. PITR to post-batch time → the "ghost" item appears.
- **Difficulty:** Hard — this is a genuine behavioral difference/bug.
- **Decision:** Document as a **divergent behavior** (skipped test with detailed explanation) + sister test showing actual behavior.

#### TEST C6: `RestoreToPointInTime_ChangeFeedIteratorWorksAfterRestore`
- **Why:** After restore, the change feed should still function for new operations. No test specifically verifies reading the change feed iterator post-restore yields correct results.
- **Approach:** Create items, restore, create new item, read change feed → new item appears.
- **Difficulty:** Medium — need change feed iterator setup.

#### TEST C7: `RestoreToPointInTime_UniqueKeyConstraintsApplyAfterRestore`
- **Why:** After restore, if unique key paths are configured, new writes should still enforce uniqueness against the restored data.
- **Approach:** Create container with unique key `/name`, create item with name="Alice", capture restore point, delete it, restore, try to create another item with name="Alice" → 409 Conflict.
- **Difficulty:** Medium.

### Category D: Thread Safety

#### TEST D1: `RestoreToPointInTime_ConcurrentReadsAndRestore_NoException`
- **Why:** `RestoreToPointInTime` locks `_changeFeedLock` for the feed snapshot but then clears and writes to `_items` (ConcurrentDictionary), `_etags`, and `_timestamps` without any additional lock. Concurrent reads during the clear→repopulate window could see partial state.
- **Approach:** Spin up tasks that continuously read items while another task calls `RestoreToPointInTime`. Assert no unhandled exceptions (not necessarily correct data — just no crashes).
- **Difficulty:** Medium — concurrency test, may be flaky.
- **Decision:** This is a known limitation of the emulator. Write test but mark as informational — the emulator is designed for unit tests, not production concurrency.

---

## Bugs Summary

| # | Bug | Severity | TDD Test | Fix Location |
|---|-----|----------|----------|--------------|
| BUG1 | Stale `_etag`/`_ts` in restored JSON body vs dictionaries | Medium | TEST A3 | `RestoreToPointInTime()` L268–275 |

---

## Tests Implementation Order (TDD: Red → Green → Refactor)

### Phase 1: Bug Fix (BUG1)
1. ✅ Write TEST A3 (`ETagsAreRegeneratedAfterRestore`) — assert `response.ETag` matches embedded `_etag`. **RED**
2. ✅ Fix `RestoreToPointInTime` to re-enrich JSON with new `_etag` and `pointInTime` as `_ts`. **GREEN**
3. ✅ Refactor if needed.

### Phase 2: Core Functionality (straightforward, should all pass)
4. Write TEST A1 (`WithReplaceItem_RestoresPreReplaceState`) — should pass immediately.
5. Write TEST A2 (`QueriesWorkAfterRestore`) — should pass immediately.
6. Write TEST A4 (`WithStreamOperations_RestoresCorrectly`) — should pass immediately.

### Phase 3: Edge Cases
7. Write TEST B1 (`ToFutureTimestamp_KeepsAllItems`) — should pass immediately.
8. Write TEST B2 (`ToExactOperationTimestamp_IncludesOperation`) — should pass immediately.
9. Write TEST B3 (`DoubleRestoreToSamePoint_IsIdempotent`) — should pass immediately.
10. Write TEST B4 (`ConsecutiveRestoresToDifferentPoints`) — should pass immediately.
11. Write TEST B5 (`WithPartitionKeyNone_RestoresCorrectly`) — should pass immediately.
12. Write TEST B6 (`WithHierarchicalPartitionKeys_RestoresCorrectly`) — should pass immediately.
13. Write TEST B7 (`EmptyContainer_NoOpRestore`) — should pass immediately.

### Phase 4: Feature Interactions
14. Write TEST C1 (`AfterClearItems_RestoresToEmpty`) — should pass immediately.
15. Write TEST C2 (`AfterImportState_HasNoHistory`) — should pass immediately.
16. Write TEST C3 (`TTLExpiredItem_IsResurrectedByRestore`) — should pass immediately.
17. Write TEST C4 (`RestoredItemsTTLTimerResetsToRestorePoint`) — timing-sensitive, may skip if flaky.
18. Write TEST C5 (`AfterFailedTransactionalBatch_GhostEntriesInChangeFeed`) — **SKIP** with detailed reasoning + divergent behavior sister test.
19. Write TEST C6 (`ChangeFeedIteratorWorksAfterRestore`) — should pass immediately.
20. Write TEST C7 (`UniqueKeyConstraintsApplyAfterRestore`) — should pass immediately.

### Phase 5: Thread Safety
21. Write TEST D1 (`ConcurrentReadsAndRestore_NoException`) — informational, may skip if flaky.

---

## Documentation Updates

### Wiki: Known-Limitations.md
- If TEST C5 (ghost change feed entries after failed batch) is confirmed as a divergent behavior, add a new **Behavioural Difference** entry:
  > **PITR After Failed Transactional Batch:** `RestoreToPointInTime` replays the change feed, which includes entries from batch operations that were executed before the batch failure — even though `RestoreSnapshot` rolled back `_items`/`_etags`. This can cause "ghost" items to appear after PITR. Real Cosmos DB PITR would not include uncommitted batch operations.

- Add a **Limitation** note about PITR after `ClearItems()`/`ImportState()`:
  > **PITR history cleared by ClearItems/ImportState:** `ClearItems()` and `ImportState()` wipe the internal change feed. Any PITR restore after these calls can only see operations performed after the clear/import.

### Wiki: Features.md
- Update the PITR section to mention:
  - ETag regeneration on restore
  - TTL interaction (items are restorable regardless of TTL expiry)
  - Change feed history note (already documented)

### Wiki: Feature-Comparison-With-Alternatives.md
- No changes needed — PITR is already listed as ✅.

### Main README.md
- No changes needed — PITR is already listed in features.

### Package README (src/CosmosDB.InMemoryEmulator/README.md)
- No changes needed — PITR is mentioned in the feature list.

---

## Version & Release

- Bump version: `2.0.4` → `2.0.5` in `src/CosmosDB.InMemoryEmulator/CosmosDB.InMemoryEmulator.csproj`
- Git commit message: `v2.0.5: PITR deep dive — fix stale _etag/_ts on restore, add 20+ edge case tests`
- Git tag: `v2.0.5`
- Git push + push tags

---

## Tracking

| Phase | Status |
|-------|--------|
| Phase 1: Bug Fix (BUG1) | ⬜ Not started |
| Phase 2: Core Functionality (A1–A4) | ⬜ Not started |
| Phase 3: Edge Cases (B1–B7) | ⬜ Not started |
| Phase 4: Feature Interactions (C1–C7) | ⬜ Not started |
| Phase 5: Thread Safety (D1) | ⬜ Not started |
| Documentation Updates | ⬜ Not started |
| Version bump, tag, push | ⬜ Not started |
