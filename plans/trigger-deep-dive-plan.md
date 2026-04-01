# Trigger Deep Dive — TDD Plan

## Scope

Deep dive into `TriggerTests.cs` (C# handler triggers) and `JsTriggerTests.cs` (JavaScript triggers) to identify missing test coverage, bugs in the implementation, and edge cases. All new tests go into the existing test files. TDD: test first, red-green-refactor.

---

## Files Under Analysis

| File | Lines | Classes | Tests |
|------|-------|---------|-------|
| `TriggerTests.cs` | ~600 | `TriggerRegistrationTests`, `PreTriggerExecutionTests`, `PostTriggerExecutionTests` | ~25 |
| `JsTriggerTests.cs` | ~1860 | `PreTriggerJsTests`, `PostTriggerJsTests`, `MixedTriggerTests`, `PreTriggerJsAdditionalTests`, `PostTriggerJsAdditionalTests`, `CombinedTriggerTests`, `TriggerScriptsCrudTests`, `JsTriggerDivergentBehaviorTests`, `JintTriggerEngineEdgeCaseTests`, `UseJsTriggersExtensionTests` | ~55 |

Implementation files:
- `InMemoryContainer.cs` — `ExecutePreTriggers()` (L1714), `ExecutePostTriggers()` (L1759), `RegisterTrigger()`, `DeregisterTrigger()`
- `IJsTriggerEngine.cs` — interface
- `JintTriggerEngine.cs` — Jint-based JS interpretation

---

## Bug Report

### BUG-1: Delete operations do NOT fire triggers (CRITICAL)

**Location:** `InMemoryContainer.cs` lines 619-639 (`DeleteItemAsync`) and 931-955 (`DeleteItemStreamAsync`)

**Problem:** Neither `DeleteItemAsync<T>` nor `DeleteItemStreamAsync` call `ExecutePreTriggers` or `ExecutePostTriggers`. Real Azure Cosmos DB supports `TriggerOperation.Delete` — the SDK enum has this value, and the emulator's own `OperationNameToTriggerOp` helper already maps `"Delete"` to `TriggerOperation.Delete`. The JsTriggerTests file already has tests that expect delete triggers to work (`PreTrigger_Js_DeleteOperation_Fires`, `PostTrigger_Js_DeleteOperation_Fires`, `PostTrigger_Js_ThrowOnDelete_RollsBackDelete`) — these tests will currently FAIL because the implementation is missing.

**Fix:** Add `ExecutePreTriggers`/`ExecutePostTriggers` calls to both delete methods, with proper rollback semantics (re-insert item on post-trigger failure). Pre-trigger for delete needs special handling: there's no document mutation (you can't modify a document being deleted), so the pre-trigger should receive the existing document for inspection/validation and can throw to abort the delete (like a "veto" pre-trigger).

**Rollback design for DeleteItemAsync:**
```
1. Read existing item JSON before removal
2. Execute pre-triggers with existing document (throw = abort delete)
3. Remove item from _items, _etags, _timestamps
4. Execute post-triggers with the deleted document
5. On post-trigger exception: re-insert saved item, re-insert etag, re-insert timestamp → throw
```

**Rollback design for DeleteItemStreamAsync:**
```
Same as above but return error ResponseMessage instead of throwing
```

### BUG-2: UpsertItemStreamAsync has incomplete rollback on post-trigger failure

**Location:** `InMemoryContainer.cs` lines 852-862

**Problem:** When `existed == true` and a post-trigger fails, the rollback comment says `"Stream rollback is best-effort — we don't have the previous json"` and does NOTHING. But we CAN capture `previousJson` before overwriting, just as `UpsertItemAsync` does. The non-stream `UpsertItemAsync` at L528-542 correctly captures and restores `previousJson`, `previousEtag`, and `previousTimestamp`. The stream variant should do the same.

**Fix:** Capture `previousJson`, `previousEtag`, `previousTimestamp` in `UpsertItemStreamAsync` before overwriting (same pattern as `UpsertItemAsync`), and restore them in the rollback catch block.

### BUG-3: ReplaceItemStreamAsync has incomplete rollback on post-trigger failure

**Location:** `InMemoryContainer.cs` lines 920-928

**Problem:** The comment says `"Stream rollback is best-effort"` and does nothing — no restoration of the previous item. But the non-stream `ReplaceItemAsync` at L608-613 correctly restores `previousJson`, `previousEtag`, and `previousTimestamp`. The stream variant already has the previous item available (it checks `_items.ContainsKey(key)` earlier) so it should capture and restore it.

**Fix:** Capture `previousJson`, `previousEtag`, `previousTimestamp` in `ReplaceItemStreamAsync` before overwriting, and restore them on post-trigger failure.

### BUG-4: Change feed not rolled back on post-trigger failure

**Location:** All write methods call `RecordChangeFeed()` BEFORE `ExecutePostTriggers()`. If the post-trigger fails and the write is rolled back, the change feed still contains the record for the rolled-back operation. In real Cosmos DB, if a post-trigger fails, the entire operation is transactional — the change feed should not contain the failed write.

**Severity:** Low — most test scenarios won't observe this, and change feed semantics in real Cosmos have "at least once" delivery, but this is technically incorrect.

**Fix (optional):** Either move `RecordChangeFeed()` after successful post-trigger execution, or add change feed rollback in the catch blocks. Mark as skipped divergent behavior if too complex.

---

## Missing Test Coverage — TriggerTests.cs (C# Handlers)

### Phase 1: Registration Edge Cases

| # | Test Name | Status | Description |
|---|-----------|--------|-------------|
| T1 | `RegisterTrigger_DuplicateId_OverwritesPrevious` | NEW | Register two triggers with same ID — second should overwrite first |
| T2 | `DeregisterTrigger_NonExistent_NoThrow` | NEW | Deregistering a trigger that doesn't exist should not throw |
| T3 | `RegisterTrigger_PostTrigger_StoresHandler` | NEW | Verify post-trigger Action handler is stored (symmetric with pre-trigger test) |
| T4 | `CreateTriggerAsync_DuplicateId_ThrowsConflict` | NEW | Creating a trigger with an ID that already exists should throw 409 Conflict |
| T5 | `ReplaceTriggerAsync_NotFound_ThrowsNotFound` | NEW | Replacing a trigger that doesn't exist should throw 404 |

### Phase 2: Pre-Trigger Execution — Delete Operations

| # | Test Name | Status | Description |
|---|-----------|--------|-------------|
| T6 | `PreTrigger_OnDelete_ThrowingTriggerAborts` | NEW + BUG-1 FIX | Register a throwing pre-trigger for Delete; the delete should be aborted |
| T7 | `PreTrigger_OnDelete_NonThrowingAllowsDeletion` | NEW + BUG-1 FIX | Register a non-throwing pre-trigger for Delete; item should be deleted |
| T8 | `PreTrigger_OnDeleteStream_ThrowingTriggerAborts` | NEW + BUG-1 FIX | Same as T6 but via stream API |
| T9 | `PreTrigger_OnDeleteStream_NonThrowingAllowsDeletion` | NEW + BUG-1 FIX | Same as T7 but via stream API |

### Phase 3: Post-Trigger Execution — Delete Operations

| # | Test Name | Status | Description |
|---|-----------|--------|-------------|
| T10 | `PostTrigger_FiresAfterDelete` | NEW + BUG-1 FIX | Verify post-trigger fires after successful delete |
| T11 | `PostTrigger_ExceptionOnDelete_RollsBackDelete` | NEW + BUG-1 FIX | Throwing post-trigger on delete should re-insert the deleted item |
| T12 | `PostTrigger_OnDeleteStream_Fires` | NEW + BUG-1 FIX | Stream variant of T10 |
| T13 | `PostTrigger_OnDeleteStream_ExceptionRollsBack` | NEW + BUG-1 FIX | Stream variant of T11 |

### Phase 4: Post-Trigger Rollback Correctness

| # | Test Name | Status | Description |
|---|-----------|--------|-------------|
| T14 | `PostTrigger_ExceptionOnReplace_RollsBackToOriginal` | NEW | Replace with failing post-trigger — original item should be fully restored (all fields, etag, timestamp) |
| T15 | `PostTrigger_ExceptionOnUpsert_ExistingItem_RollsBack` | NEW | Upsert over existing with failing post-trigger — original item should be restored |
| T16 | `PostTrigger_ExceptionOnUpsert_NewItem_RollsBack` | NEW | Upsert new item with failing post-trigger — item should not exist |

### Phase 5: Pre-Trigger Edge Cases

| # | Test Name | Status | Description |
|---|-----------|--------|-------------|
| T17 | `PreTrigger_ThrowingHandler_ThrowsCosmosException` | NEW | Pre-trigger C# handler that throws — should produce CosmosException |
| T18 | `PreTrigger_ChangesPartitionKey_StoresWithModifiedKey` | NEW | Pre-trigger that modifies the partition key value — verify storage uses modified value. This is a potential edge case since PK extraction happens after trigger execution. |
| T19 | `PreTrigger_ChangesId_StoresWithModifiedId` | NEW | Pre-trigger that modifies the "id" field — verify storage uses modified value |
| T20 | `PreTrigger_ReturnsNull_ThrowsOrHandlesGracefully` | NEW | Pre-trigger handler returns null — should throw or handle gracefully |
| T21 | `PreTrigger_EmptyPreTriggersList_TriggerNotFired` | NEW | `PreTriggers = new List<string>()` (empty, not null) — trigger should not fire |

### Phase 6: Post-Trigger Edge Cases

| # | Test Name | Status | Description |
|---|-----------|--------|-------------|
| T22 | `PostTrigger_ReceivesEnrichedDocument` | NEW | Post-trigger should receive document WITH system properties (_etag, _ts) |
| T23 | `PostTrigger_MultiplePostTriggers_ChainInOrder` | NEW | Multiple post-triggers in list all fire in order |
| T24 | `PostTrigger_EmptyPostTriggersList_NoFire` | NEW | `PostTriggers = new List<string>()` (empty, not null) — no trigger fires |
| T25 | `PostTrigger_ExceptionPreservesOriginalExceptionType` | NEW | Verify the CosmosException wrapping preserves useful info |

### Phase 7: Pre + Post Combined (C# Handlers)

| # | Test Name | Status | Description |
|---|-----------|--------|-------------|
| T26 | `PreAndPostTrigger_BothFireOnCreate` | NEW | Pre-trigger modifies doc, post-trigger sees modification — both via C# handlers |
| T27 | `PreTrigger_Throws_PostTrigger_NotFired` | NEW | If pre-trigger throws, the post-trigger should never execute |
| T28 | `PostTrigger_RollbackDoesNotAffectOtherItems` | NEW | Verify rollback of one item doesn't corrupt other items in the container |

### Phase 8: Operation Matching Edge Cases

| # | Test Name | Status | Description |
|---|-----------|--------|-------------|
| T29 | `PreTrigger_OperationMismatch_Delete_TriggerNotFired` | NEW + BUG-1 FIX | Register Create-only trigger, use on Delete — trigger should not fire |
| T30 | `PostTrigger_OperationMismatch_Delete_TriggerNotFired` | NEW + BUG-1 FIX | Register Create-only post-trigger, use on Delete — should not fire |
| T31 | `PreTrigger_TriggerOperationAll_FiresOnDelete` | NEW + BUG-1 FIX | TriggerOperation.All trigger should fire on Delete too |

---

## Missing Test Coverage — JsTriggerTests.cs (JavaScript Triggers)

### Phase 9: JS Delete Trigger Implementation Tests

Note: The existing tests `PreTrigger_Js_DeleteOperation_Fires` and `PostTrigger_Js_DeleteOperation_Fires` already exist but will currently FAIL due to BUG-1. These need the BUG-1 fix to pass. Confirm they pass after fix.

| # | Test Name | Status | Description |
|---|-----------|--------|-------------|
| J1 | (existing) `PreTrigger_Js_DeleteOperation_Fires` | VERIFY AFTER BUG-1 FIX | Confirm this passes once delete triggers are implemented |
| J2 | (existing) `PostTrigger_Js_DeleteOperation_Fires` | VERIFY AFTER BUG-1 FIX | Confirm this passes once delete triggers are implemented |
| J3 | (existing) `PostTrigger_Js_ThrowOnDelete_RollsBackDelete` | VERIFY AFTER BUG-1 FIX | Confirm this passes once delete triggers are implemented |
| J4 | `PreTrigger_Js_DeleteStream_Fires` | NEW + BUG-1 FIX | JS pre-trigger on DeleteItemStreamAsync |
| J5 | `PostTrigger_Js_DeleteStream_Fires` | NEW + BUG-1 FIX | JS post-trigger on DeleteItemStreamAsync |

### Phase 10: JS Post-Trigger Rollback (Stream variants)

| # | Test Name | Status | Description |
|---|-----------|--------|-------------|
| J6 | `PostTrigger_Js_RollsBack_OnExceptionDuringUpsertStream` | NEW + BUG-2 FIX | JS post-trigger fails during UpsertItemStreamAsync for existing item — should rollback |
| J7 | `PostTrigger_Js_RollsBack_OnExceptionDuringReplaceStream` | NEW + BUG-3 FIX | JS post-trigger fails during ReplaceItemStreamAsync — should rollback |

### Phase 11: JS Edge Cases

| # | Test Name | Status | Description |
|---|-----------|--------|-------------|
| J8 | `PreTrigger_Js_InfiniteLoop_TimesOut` | NEW | JS body with `while(true){}` — should throw after 5-second timeout |
| J9 | `PreTrigger_Js_ExceedsMaxStatements_Fails` | NEW | JS body with >10,000 statements — should throw |
| J10 | `PreTrigger_Js_EmptyBody_NoError` | NEW | Empty JS function body — should not throw, document unchanged |
| J11 | `PreTrigger_Js_BodyWithNoFunction_Fails` | NEW | JS body that's just a statement like `var x = 1;` — InvokeFirstFunction finds no function, no-op? Or throw? |
| J12 | `PostTrigger_Js_LargeDocument_HandledCorrectly` | NEW | Post-trigger with 150+ field document — verify no serialization issues |
| J13 | `PostTrigger_Js_UnicodeContent` | NEW | Post-trigger receives document with unicode content — verify no encoding issues |
| J14 | `PreTrigger_Js_NumericPrecision` | NEW | JS trigger sets a very large number or floating point — verify round-trip precision |
| J15 | `PreTrigger_Js_BooleanValues` | NEW | JS trigger sets true/false — verify correct JToken type in stored document |
| J16 | `PreTrigger_Js_DateHandling` | NEW | JS trigger creates Date object — verify serialization to string |

### Phase 12: Script CRUD Edge Cases

| # | Test Name | Status | Description |
|---|-----------|--------|-------------|
| J17 | `CreateTriggerAsync_DuplicateId_ThrowsConflict` | NEW | Creating a JS trigger with existing ID should throw 409 |
| J18 | `ReplaceTriggerAsync_ChangesType_PreToPost` | NEW | Replace a Pre trigger to become Post — verify new type is respected |
| J19 | `ReplaceTriggerAsync_ChangesOperation_CreateToAll` | NEW | Replace a Create trigger to All — verify new operation scope |
| J20 | `DeleteTriggerAsync_ThenRecreate_Works` | NEW | Delete a trigger then create with same ID — should work |

### Phase 13: JS Trigger + TransactionalBatch (if applicable)

| # | Test Name | Status | Description |
|---|-----------|--------|-------------|
| J21 | `TransactionalBatch_CreateWithPreTrigger_TriggersNotSupported` | NEW (SKIP candidate) | Real Cosmos DB does not support triggers in transactional batch. Verify emulator behavior. If triggers ARE supported (divergent), document as skip + sister test. |

---

## Divergent Behavior Tests to Add

These are tests that document known differences between the emulator and real Cosmos DB. Each has a skipped test explaining real behavior and a sister test showing emulator behavior.

| # | Divergence | Skip Test | Sister Test |
|---|------------|-----------|-------------|
| D1 | Pre-trigger on delete receives document but can't modify stored result | `PreTrigger_OnDelete_CanModifyDeletedDoc_RealCosmos` | `PreTrigger_OnDelete_ReceivesDocForInspectionOnly_InEmulator` |
| D2 | Change feed records for rolled-back writes | `ChangeFeed_PostTriggerRollback_NoRecord_RealCosmos` | `ChangeFeed_PostTriggerRollback_RecordStillPresent_InEmulator` |
| D3 | Patch operations with triggers | `PatchWithTrigger_Supported_RealCosmos` | `PatchWithTrigger_NotSupported_InEmulator` |

---

## Implementation Order (TDD Red-Green-Refactor)

### Wave 1: BUG-1 Fix (Delete Triggers) — Highest Priority
1. Write T6, T7, T8, T9, T10, T11, T12, T13 (C# handler delete trigger tests) → RED
2. Verify J1, J2, J3 existing JS delete tests also fail → RED
3. Implement delete trigger support in `DeleteItemAsync` and `DeleteItemStreamAsync` → GREEN
4. Write T29, T30, T31 (operation matching on delete) → should be GREEN immediately
5. Write J4, J5 (JS delete stream tests) → should be GREEN immediately
6. Refactor if needed

### Wave 2: BUG-2 & BUG-3 Fix (Stream Rollback)
1. Write J6 (UpsertStream rollback test) → RED
2. Fix `UpsertItemStreamAsync` rollback to capture previousJson → GREEN
3. Write J7 (ReplaceStream rollback test) → RED
4. Fix `ReplaceItemStreamAsync` rollback to capture previousJson → GREEN
5. Refactor if needed

### Wave 3: Registration & CRUD Edge Cases
1. Write T1-T5, J17-J20 → expect mix of RED/GREEN
2. Implement any missing validation → GREEN
3. Refactor if needed

### Wave 4: Pre-Trigger Edge Cases
1. Write T17-T21 → expect mix of RED/GREEN
2. Implement fixes for any unexpected failures → GREEN

### Wave 5: Post-Trigger Edge Cases
1. Write T22-T25 → expect mix of RED/GREEN
2. Implement fixes for any unexpected failures → GREEN

### Wave 6: Combined & Interaction Tests
1. Write T26-T28 → expect GREEN (mostly interaction tests)

### Wave 7: JS Edge Cases
1. Write J8-J16 → expect mix of RED/GREEN
2. Fix any discovered issues → GREEN

### Wave 8: Divergent Behavior Documentation
1. Write D1-D3 skip + sister test pairs

### Wave 9: Verify Existing Tests Still Pass
1. Run full TriggerTests.cs suite
2. Run full JsTriggerTests.cs suite
3. Run full test suite to check for regressions

---

## Documentation Updates

### Wiki Known Limitations (External GitHub Wiki)
Add to known limitations:
- [ ] Delete trigger pre-trigger receives document for inspection only (cannot modify the result of a delete)
- [ ] Change feed may contain records for writes that were rolled back by post-trigger failures
- [ ] Patch operations do not support triggers (use pre/post hooks via CRUD operations instead)
- [ ] Update existing divergent behavior entries to reference the new sister tests

### Wiki Feature Comparison
- [ ] Update trigger feature row to indicate delete trigger support (after BUG-1 fix)

### Wiki Features List
- [ ] Update triggers entry to mention delete trigger support
- [ ] Add note about stream variant rollback completeness (after BUG-2/BUG-3 fix)

### README.md (root)
- [ ] No changes needed — trigger description is already accurate at high level
- [ ] Update test count after adding new tests (currently says "1350+ tests")

### README.md (src/CosmosDB.InMemoryEmulator/)
- [ ] No changes needed — no trigger-specific content

---

## Version Bump & Release

- [ ] Increment patch version: `2.0.4` → `2.0.5` in `src/CosmosDB.InMemoryEmulator/CosmosDB.InMemoryEmulator.csproj`
- [ ] Git tag: `v2.0.5`
- [ ] Git push with tags

---

## Test Count Summary

| Category | New Tests | Bug Fix Tests | Verify Existing | Divergent Pairs |
|----------|-----------|---------------|-----------------|-----------------|
| C# Handler Registration | 5 (T1-T5) | — | — | — |
| C# Handler Pre-Trigger Delete | 4 (T6-T9) | BUG-1 | — | — |
| C# Handler Post-Trigger Delete | 4 (T10-T13) | BUG-1 | — | — |
| C# Handler Post Rollback | 3 (T14-T16) | — | — | — |
| C# Handler Pre Edge Cases | 5 (T17-T21) | — | — | — |
| C# Handler Post Edge Cases | 4 (T22-T25) | — | — | — |
| C# Handler Combined | 3 (T26-T28) | — | — | — |
| C# Handler Op Matching | 3 (T29-T31) | BUG-1 | — | — |
| JS Delete Triggers | 2 (J4-J5) | BUG-1 | 3 (J1-J3) | — |
| JS Stream Rollback | 2 (J6-J7) | BUG-2, BUG-3 | — | — |
| JS Edge Cases | 9 (J8-J16) | — | — | — |
| JS Script CRUD | 4 (J17-J20) | — | — | — |
| JS Batch | 1 (J21) | — | — | — |
| Divergent Behavior | — | — | — | 3 pairs (D1-D3) |
| **TOTAL** | **~49 new** | **4 bugs** | **3 verify** | **6 (3 pairs)** |

---

## Progress Tracker

- [ ] Wave 1: BUG-1 — Delete Triggers
- [ ] Wave 2: BUG-2/3 — Stream Rollback
- [ ] Wave 3: Registration & CRUD Edge Cases
- [ ] Wave 4: Pre-Trigger Edge Cases
- [ ] Wave 5: Post-Trigger Edge Cases
- [ ] Wave 6: Combined & Interaction Tests
- [ ] Wave 7: JS Edge Cases
- [ ] Wave 8: Divergent Behavior Documentation
- [ ] Wave 9: Full Regression Test
- [ ] Documentation Updates
- [ ] Version Bump, Tag & Push
