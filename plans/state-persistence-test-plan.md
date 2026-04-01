# State Persistence Test Coverage Plan

**Date:** 2026-04-01  
**Current version:** 2.0.4 → Will become **2.0.5**  
**Target file:** `tests/CosmosDB.InMemoryEmulator.Tests/StatePersistenceTests.cs`  
**Source under test:** `src/CosmosDB.InMemoryEmulator/InMemoryContainer.cs` — methods: `ExportState()`, `ImportState(string)`, `ExportStateToFile(string)`, `ImportStateFromFile(string)`, `ClearItems()`, `ItemCount`

---

## Current Test Inventory (10 tests, 2 classes)

### StatePersistenceTests (9 tests)
1. `ExportState_EmptyContainer_ReturnsEmptyJson` — ✅ exists
2. `ExportState_WithItems_SerializesAllItems` — ✅ exists
3. `ImportState_RestoresItems` — ✅ exists
4. `ImportState_ClearsExistingDataBeforeImporting` — ✅ exists
5. `ExportState_ToFile_And_ImportState_FromFile_RoundTrips` — ✅ exists
6. `ExportState_PreservesPartitionKeyIsolation` — ✅ exists
7. `ImportState_WithNestedObjects_PreservesStructure` — ✅ exists
8. `ImportState_WithInvalidJson_ThrowsException` — ✅ exists
9. `ExportState_ItemsAreQueryableAfterImport` — ✅ exists

### StateImportExportTests (1 test, duplicate class — merge candidate)
10. `ExportState_ImportState_RoundTrip` — ✅ exists (largely duplicates #3)

---

## Identified Bugs / Issues

### BUG-1: `ImportState` does not validate unique key constraints
**Severity:** Medium  
**Location:** `InMemoryContainer.ImportState()` lines 199-222  
**Problem:** The `ImportState` method directly writes to `_items` without calling `ValidateUniqueKeys()`. If exported JSON (hand-crafted or migrated from a container with different unique key policy) contains duplicate unique key values within the same partition, the import silently succeeds and the container is left in an invalid state. Subsequent writes may behave unpredictably.  
**Fix:** After importing all items, validate unique key constraints and throw if violated — OR — validate per-item during import (matching CreateItemAsync behavior). Decision: validate per-item and throw `CosmosException(409)` on the offending item, leaving the container in a partially-imported state with a clear error message. This matches the "fail fast" principle.

### BUG-2: `ImportState` does not record change feed entries
**Severity:** Medium  
**Location:** `InMemoryContainer.ImportState()` lines 199-222  
**Problem:** Normal `CreateItemAsync` calls `RecordChangeFeed()` to track changes. `ImportState` bypasses this entirely. After importing state, the change feed is empty. This means:
- Change feed processors won't see any of the imported items
- Point-in-time restore (`RestoreToPointInTime`) can't restore to any state that was set up via ImportState
- `GetChangeFeedCheckpoint()` / change feed iterators return nothing

**Decision:** This is arguably _correct by design_ — ImportState is a snapshot restore, not a replay of creation events. But it should be **documented** as a known behavior. We'll add a test that documents this explicitly rather than "fix" it, since recording change feed entries for bulk-imported items could cause unexpected side effects.

### BUG-3: `ExportState` exports system properties (`_etag`, `_ts`) that get double-enriched on import
**Severity:** Low  
**Location:** `ExportState()` exports raw `_items` values which already contain `_etag`/`_ts`. `ImportState()` then calls `EnrichWithSystemProperties()` which overwrites them. This is actually _correct_ behavior (new etags/timestamps on import), but the exported JSON is larger than necessary since system properties are included but then discarded.  
**Decision:** Not a functional bug. The export includes all stored data faithfully, and import correctly regenerates system properties. No fix needed — but worth testing that old etags are NOT preserved (by design).

### BUG-4: `ImportState` with empty `"items":[]` array works, but missing `"items"` key silently imports nothing
**Severity:** Low  
**Location:** `ImportState()` line 207: `if (state["items"] is JArray items)` — if the key doesn't exist, the `ClearItems()` on line 205 has already run, so existing data is wiped with nothing imported.  
**Decision:** This is a silent data loss scenario. Should either throw or at minimum be tested as expected behavior. We'll add a test documenting this.

---

## New Tests to Write

### Category A: Edge Cases for ExportState

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| A1 | `ExportState_SingleItem_ProducesValidJson` | Verify export with exactly 1 item produces valid parseable JSON with correct structure | todo |
| A2 | `ExportState_IncludesSystemProperties` | Verify _etag and _ts are present in exported items | todo |
| A3 | `ExportState_LargeNumberOfItems_AllSerialized` | Export 500+ items, verify count matches | todo |
| A4 | `ExportState_WithSpecialCharactersInValues_RoundTrips` | Items with Unicode, emoji, quotes, backslashes, newlines in string fields | todo |
| A5 | `ExportState_WithNullValues_PreservesNulls` | Items with null fields preserve null vs missing distinction | todo |
| A6 | `ExportState_WithNumericTypes_PreservesPrecision` | int, long, double, decimal values survive export | todo |
| A7 | `ExportState_WithBooleanValues_PreservesType` | true/false not converted to strings | todo |
| A8 | `ExportState_WithArrays_PreservesArrays` | Items containing arrays (empty, populated, nested) | todo |
| A9 | `ExportState_WithDeeplyNestedObjects_PreservesAll` | 5+ levels of nesting | todo |
| A10 | `ExportState_OutputIsIndentedJson` | Verify formatting is human-readable (Formatting.Indented) | todo |
| A11 | `ExportState_CalledTwice_ProducesSameOutput` | Idempotency (excluding _etag which is stable between calls) | todo |

### Category B: Edge Cases for ImportState

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| B1 | `ImportState_EmptyItemsArray_ResultsInEmptyContainer` | `{"items":[]}` clears and imports nothing — container is empty | todo |
| B2 | `ImportState_MissingItemsKey_ClearsContainer` | `{"foo":"bar"}` — existing data is wiped, no items imported (documents BUG-4 behavior) | todo |
| B3 | `ImportState_EmptyJsonObject_ClearsContainer` | `{}` — same as above | todo |
| B4 | `ImportState_GeneratesNewETags` | After import, etags are different from the exported etags | todo |
| B5 | `ImportState_GeneratesNewTimestamps` | After import, _ts values are fresh (not the original timestamps) | todo |
| B6 | `ImportState_WithItemsMissingId_UsesEmptyStringAsId` | Items without "id" field — code uses `?? ""` — verify behavior | todo |
| B7 | `ImportState_NullJson_ThrowsArgumentNullException` | Passing null to ImportState | todo |
| B8 | `ImportState_EmptyString_ThrowsException` | Passing "" to ImportState | todo |
| B9 | `ImportState_ValidJsonButNotObject_Throws` | e.g. `"[]"` or `"123"` — JObject.Parse should throw | todo |
| B10 | `ImportState_DuplicateIds_SamePartitionKey_LastWins` | Two items with same id+pk in the import array — last one should win since it's a dictionary | todo |
| B11 | `ImportState_DuplicateIds_DifferentPartitionKeys_BothStored` | Same id, different pk — both should exist | todo |
| B12 | `ImportState_CalledMultipleTimes_OnlyLastImportSurvives` | Import A, then import B — only B's data exists | todo |
| B13 | `ImportState_WithExtraJsonProperties_PreservesThem` | Items with fields not in TestDocument — should be stored as raw JSON | todo |

### Category C: Change Feed Interaction

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| C1 | `ImportState_DoesNotPopulateChangeFeed` | After import, change feed iterator returns no results (documents BUG-2 decision) | todo |
| C2 | `ImportState_SubsequentWritesAppearInChangeFeed` | After import, new CreateItem/Upsert operations DO appear in change feed | todo |
| C3 | `ClearItems_ClearsChangeFeed` | After ClearItems, change feed is empty | todo |
| C4 | `ExportState_DoesNotIncludeChangeFeedHistory` | Export is a snapshot, no change feed data in JSON | todo |

### Category D: TTL Interaction

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| D1 | `ImportState_WithDefaultTTL_ImportedItemsRespectTTL` | Set DefaultTimeToLive on target container, import items, verify they expire | todo |
| D2 | `ImportState_WithPerItemTTL_ItemsExpireCorrectly` | Import items that have `_ttl` field, verify lazy eviction works | todo |
| D3 | `ExportState_WithTTLItems_IncludesTtlField` | Items that have `_ttl` in stored JSON are included in export | todo |

### Category E: Hierarchical / Composite Partition Keys

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| E1 | `ExportImport_HierarchicalPartitionKey_RoundTrips` | Container with multiple PK paths — items survive round-trip | todo |
| E2 | `ImportState_IntoContainerWithDifferentPartitionKeyPath_ReKeysItems` | Export from /pk1, import into container with /pk2 — items get re-keyed based on target container's PK path | todo |

### Category F: Unique Key Policy Interaction

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| F1 | `ImportState_ViolatesUniqueKeyPolicy_BehaviorDocumented` | Import items that would violate unique keys — **SKIP if hard to implement**, document as known limitation with sister test showing what happens | todo |

### Category G: File-Based Operations

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| G1 | `ExportStateToFile_CreatesFile` | File exists after export | todo |
| G2 | `ExportStateToFile_OverwritesExistingFile` | Calling twice overwrites | todo |
| G3 | `ImportStateFromFile_FileNotFound_Throws` | Non-existent file path throws FileNotFoundException | todo |
| G4 | `ImportStateFromFile_EmptyFile_Throws` | Empty file content throws | todo |
| G5 | `ExportStateToFile_And_ImportStateFromFile_LargeDataset` | 100+ items file round-trip | todo |

### Category H: ClearItems

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| H1 | `ClearItems_EmptiesAllStorage` | _items, _etags, _timestamps all cleared, ItemCount is 0 | todo |
| H2 | `ClearItems_OnEmptyContainer_DoesNotThrow` | No error when already empty | todo |
| H3 | `ClearItems_ThenCreateItem_WorksNormally` | Container is fully functional after clear | todo |

### Category I: Concurrency

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| I1 | `ExportState_WhileWritesHappening_DoesNotThrow` | Concurrent reads/writes don't crash export (ConcurrentDictionary is thread-safe for reads) | todo |
| I2 | `ImportState_WhileReadsHappening_DoesNotCorrupt` | This is tricky — import clears + repopulates, concurrent reads may see partial state. Document as known limitation if it can't be atomic. | todo |

### Category J: Cross-Container Export/Import

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| J1 | `ExportFromOneContainer_ImportToAnother_DifferentNames` | Container name doesn't affect import | todo |
| J2 | `ExportState_ImportState_MultipleTimes_NoStateLeakage` | No leftover data between multiple export/import cycles | todo |

### Category K: Data Fidelity

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| K1 | `ImportState_ItemsCanBeUpdatedAfterImport` | Upsert/Replace/Patch work on imported items | todo |
| K2 | `ImportState_ItemsCanBeDeletedAfterImport` | DeleteItem works on imported items | todo |
| K3 | `ImportState_ItemsReadableViaReadItemStream` | Stream API works on imported items | todo |
| K4 | `ImportState_ItemsReadableViaReadMany` | ReadManyItemsAsync works on imported items | todo |

---

## Refactoring Notes

- **Merge `StateImportExportTests` into `StatePersistenceTests`**: The second class `StateImportExportTests` has a single test that's a near-duplicate of `ImportState_RestoresItems`. Merge it into the main class and delete the duplicate.
- **Remove unused `async` in `ExportState_EmptyContainer_ReturnsEmptyJson`**: This test is `async Task` but doesn't use `await`. Should be `void` or add a pragmatic `await Task.CompletedTask`.

---

## Total New Tests: ~47

## Implementation Order (TDD Red-Green-Refactor)

### Phase 0: Housekeeping
- [ ] Merge `StateImportExportTests` into `StatePersistenceTests`
- [ ] Fix async warning in `ExportState_EmptyContainer_ReturnsEmptyJson`

### Phase 1: Export Edge Cases (A1-A11)
All tests against existing `ExportState()` — no source changes expected.

### Phase 2: Import Edge Cases (B1-B13)
Tests against existing `ImportState()`. BUG-4 documented. B6 (missing id) and B10 (duplicates) may reveal unexpected behavior.

### Phase 3: Change Feed + TTL Interaction (C1-C4, D1-D3)
Document BUG-2 as known behavior via tests. TTL tests may need careful timing.

### Phase 4: Advanced Scenarios (E1-E2, F1, G1-G5)
Hierarchical PK round-tripping, unique key interaction, file operations.

### Phase 5: ClearItems + Concurrency (H1-H3, I1-I2)
ClearItems tests are straightforward. Concurrency tests need careful design.

### Phase 6: Cross-Container + Data Fidelity (J1-J2, K1-K4)
Verify imported items are fully functional citizens.

---

## Documentation Updates Required

### Wiki Updates
1. **Known-Limitations.md**: Add entry for:
   - Import does not populate change feed (by design)
   - Import does not validate unique key constraints (BUG-1 — either fix or document)
   - Import with missing "items" key silently clears container (BUG-4 — either fix or document)
   - Import is not atomic — concurrent reads during import may see partial state

2. **Features.md**: Verify state persistence section mentions:
   - File-based export/import
   - System property regeneration on import
   - ClearItems() method
   - Change feed not preserved across import

3. **Comparison.md**: Ensure state persistence row exists showing advantage over Microsoft emulator (which has no snapshot/restore capability for individual containers)

### README.md
- Verify state persistence is listed in features
- No changes expected if already listed

### Version
- Bump `Version` in `CosmosDB.InMemoryEmulator.csproj`: `2.0.4` → `2.0.5`
- Git tag: `v2.0.5`
- Commit message: `v2.0.5: State persistence test coverage - edge cases, change feed interaction, TTL, concurrency`

---

## Skip Candidates (with sister tests)

If implementation proves too difficult during TDD:

| Test | Likely Skip Reason | Sister Test |
|------|--------------------|-------------|
| F1 (unique key violation on import) | Would require ImportState to validate unique keys, which is a significant behavior change and could break existing usage patterns | Sister test: `ImportState_IgnoresUniqueKeyPolicy_EmulatorBehavior` — shows that import bypasses unique key checks, inline commented |
| I2 (concurrent import + reads) | ImportState is inherently non-atomic (clear then repopulate). Making it atomic would require a lock around the entire import, which is a design change | Sister test: `ImportState_IsNotAtomic_EmulatorBehavior` — shows that mid-import reads can see empty/partial state, inline commented |
| D1/D2 (TTL after import) | TTL is lazily evaluated based on timestamps, and ImportState sets timestamps to `DateTimeOffset.UtcNow`. Testing TTL requires waiting or mocking time, which may be fragile | If skipped: Sister test shows item is created with fresh timestamp and TTL clock starts from import time |

---

## Progress Tracking

| Phase | Status | Notes |
|-------|--------|-------|
| Phase 0: Housekeeping | not-started | |
| Phase 1: Export Edge Cases | not-started | |
| Phase 2: Import Edge Cases | not-started | |
| Phase 3: Change Feed + TTL | not-started | |
| Phase 4: Advanced Scenarios | not-started | |
| Phase 5: ClearItems + Concurrency | not-started | |
| Phase 6: Cross-Container + Fidelity | not-started | |
| Documentation Updates | not-started | |
| Version Bump + Tag + Push | not-started | |
