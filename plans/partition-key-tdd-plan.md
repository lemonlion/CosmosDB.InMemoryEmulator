# Partition Key TDD Plan

**Date**: April 1, 2026  
**Current Version**: 2.0.4 → will become **2.0.5**  
**Target File**: `tests/CosmosDB.InMemoryEmulator.Tests/PartitionKeyTests.cs`  
**Implementation File**: `src/CosmosDB.InMemoryEmulator/InMemoryContainer.cs`

---

## Existing Test Inventory (PartitionKeyTests.cs)

| # | Class | Test Name | Status |
|---|-------|-----------|--------|
| 1 | PartitionKeyGapTests4 | PartitionKey_CompositeKey_ThreePaths | ✅ Pass |
| 2 | PartitionKeyGapTests4 | PartitionKey_BooleanValue | ✅ Pass |
| 3 | PartitionKeyGapTests | PartitionKey_ExtractedFromItem_MatchesExplicitPk | ✅ Pass |
| 4 | PartitionKeyGapTests | PartitionKey_NumericValue | ✅ Pass |
| 5 | PartitionKeyGapTests | CrossPartition_Query_ReturnsAllPartitions | ✅ Pass |
| 6 | PartitionKeyGapTests3 | PartitionKey_None_ItemsStoredByDocumentPk | ✅ Pass |
| 7 | PartitionKeyGapTests3 | PartitionKey_MissingInItem_FallsBackToId | ⏭ Skipped (divergent) |
| 8 | PartitionKeyGapTests2 | PartitionKey_CompositeKey_TwoPaths | ✅ Pass |
| 9 | PartitionKeyGapTests2 | PartitionKey_NestedPath_Extraction | ✅ Pass |
| 10 | PartitionKeyGapTests2 | PartitionKey_NullValue | ✅ Pass |
| 11 | PartitionKeyFallbackDivergentBehaviorTests | PartitionKey_None_WithMissingPkField_Succeeds_InMemory | ✅ Pass |
| 12 | PartitionKeyNoneVsNullTests | PartitionKeyNone_ShouldNotMatchPartitionKeyNull | ⏭ Skipped (divergent) |
| 13 | PartitionKeyNoneVsNullTests | PartitionKeyNoneVsNull_EmulatorBehavior_TreatedIdentically | ✅ Pass |

**Also related PK tests in CrudTests.cs** (12 tests) — these are NOT being moved, just noted for overlap avoidance.

---

## Bugs Found During Analysis

### BUG 1: DeleteAllByPartitionKeyStreamAsync does NOT record change feed tombstones
**Severity**: Medium  
**Location**: `InMemoryContainer.cs` lines 1555-1568  
**Problem**: `DeleteAllItemsByPartitionKeyStreamAsync` removes items from `_items`, `_etags`, `_timestamps` but never calls `RecordDeleteTombstone()` or `RecordChangeFeed()`. This means change feed consumers will not see these deletions, which is inconsistent with real Cosmos DB behavior and with the single-item `DeleteItemAsync` which DOES record tombstones.  
**Fix**: Iterate items before removal, call `RecordDeleteTombstone(key.Id, pk)` for each item being deleted.  
**Test**: `DeleteAllByPartitionKey_ShouldRecordChangeFeedTombstones`

### BUG 2: Composite PK `|` delimiter collision
**Severity**: Low (edge case but real)  
**Location**: `InMemoryContainer.cs` lines 1598-1608 (`ExtractPartitionKeyValue`) and line 1615 (`PartitionKeyToString`)  
**Problem**: Composite partition key values are joined with `|`. If a PK value itself contains `|`, the resulting string is ambiguous. E.g., paths `/a` and `/b` with values `"x|y"` and `"z"` → stored as `"x|y|z"` which is indistinguishable from 3-path composite `"x"`, `"y"`, `"z"`. The `RecordDeleteTombstone` method splits on `|` (line 1665) which would produce wrong results.  
**Assessment**: This is a known limitation / acceptable trade-off for the emulator. Very unlikely in real-world usage. Document as a known limitation rather than fix. Won't fix but will add a test documenting the behavior.

### BUG 3: ExtractPartitionKeyValue drops null components from composite keys
**Severity**: High  
**Location**: `InMemoryContainer.cs` line 1600  
**Problem**: `var nonNull = parts.Where(p => p is not null).ToList();` filters out null PK components.  
For a 3-path composite key `/a`, `/b`, `/c` where `/b` is null → only non-null values are joined. This means:
  - Values `("x", null, "z")` → stored as `"x|z"` (2 components)
  - Values `("x", "z")` on a 2-path key → also stored as `"x|z"`
  - These would COLLIDE even though they are different documents in different composite key schemas.
  
In real Cosmos DB, null is a valid component value in hierarchical partition keys and is preserved.  
**Fix**: Include null components in the joined string, using a null-safe representation. Change `nonNull` logic to preserve positions.  
**Test**: `CompositeKey_WithNullComponent_PreservesPosition`

### BUG 4: ExtractPartitionKeyValue vs ExtractPartitionKeyValueFromJson inconsistency
**Severity**: Medium  
**Location**: `InMemoryContainer.cs` lines 1575-1610 vs 1380-1392  
**Problem**: Two different code paths extract PK from JSON:
  - `ExtractPartitionKeyValue` (CRUD path): filters out nulls, falls back to `id`
  - `ExtractPartitionKeyValueFromJson` (FeedRange/ChangeFeed path): keeps nulls as `""`, no fallback
  
This means the same document may get a different PK string depending on whether it's accessed via CRUD or via FeedRange/change feed filtering. Items could be "invisible" to change feed FeedRange filters.  
**Fix**: Unify the two methods or ensure they produce identical results for the same document.  
**Test**: `FeedRange_FilterConsistentWithCrudPartitionKey`

### BUG 5: Patch operations can silently modify the partition key field
**Severity**: Medium  
**Location**: `InMemoryContainer.cs` lines 639-700 (`PatchItemAsync`)  
**Problem**: `PatchItemAsync` extracts PK from the explicit `partitionKey` parameter via `PartitionKeyToString()`, then applies patches to the JObject via `ApplyPatchOperations()`. If a patch sets/replaces the partition key path (e.g., `/partitionKey`), the stored JSON will have a different PK value than the storage key. Subsequent reads by document body PK will fail. Real Cosmos DB rejects patches that modify the partition key field.  
**Assessment**: Implementing the rejection check would require comparing the PK path against patch operation paths. This is feasible. However, if too difficult, skip the ideal test and add a divergent behavior test.  
**Test**: `Patch_ModifyingPartitionKeyField_ShouldThrow` (may be skipped if complex to implement)

---

## New Tests to Write

All new tests go into `PartitionKeyTests.cs` in a clean class structure. TDD: write test FIRST (red), then implement fix (green), then refactor.

### Phase 1: PK Data Type Coverage

| # | Test Name | Description | Priority |
|---|-----------|-------------|----------|
| 1.1 | `PartitionKey_DoubleValue_StoredAndRetrievable` | Create/read item with `new PartitionKey(3.14)` | High |
| 1.2 | `PartitionKey_EmptyString_StoredAndRetrievable` | Create/read with `new PartitionKey("")` | High |
| 1.3 | `PartitionKey_LongString_StoredAndRetrievable` | PK value with 1000+ characters (within 2KB limit) | Medium |
| 1.4 | `PartitionKey_UnicodeCharacters_StoredAndRetrievable` | PK with emoji/CJK characters `"日本語🎉"` | Medium |
| 1.5 | `PartitionKey_SpecialJsonCharacters_StoredAndRetrievable` | PK with quotes, backslash: `"he said \"hello\""` | Medium |
| 1.6 | `PartitionKey_ContainingPipeCharacter_StoredAndRetrievable` | Single PK value `"a|b"` (not composite) — documents the delimiter risk | Medium |

### Phase 2: Composite/Hierarchical PK Edge Cases

| # | Test Name | Description | Priority |
|---|-----------|-------------|----------|
| 2.1 | `CompositeKey_WithNullComponent_PreservesPosition` | 3-path composite where middle is null → tests BUG 3 fix | **Critical** |
| 2.2 | `CompositeKey_MixedTypes_StringAndNumber` | Composite with string + numeric component | High |
| 2.3 | `CompositeKey_SameIdDifferentComposite_BothExist` | Same doc ID with different composite key values | High |
| 2.4 | `CompositeKey_FourPaths` | 4-path hierarchical key (stress test) | Medium |
| 2.5 | `CompositeKey_AllNullComponents` | All PK components are null | Medium |
| 2.6 | `CompositeKey_ReadMany_RetrievedCorrectly` | ReadMany with composite partition keys | High |
| 2.7 | `CompositeKey_DeleteAllByPartitionKey` | DeleteAll with composite key only deletes matching items | High |
| 2.8 | `CompositeKey_TransactionalBatch` | Batch operations scoped to composite PK | Medium |

### Phase 3: Bug Fix Tests (Red-Green)

| # | Test Name | Description | Bug | Priority |
|---|-----------|-------------|-----|----------|
| 3.1 | `DeleteAllByPartitionKey_ShouldRecordChangeFeedTombstones` | After DeleteAll, change feed should contain delete entries | BUG 1 | **Critical** |
| 3.2 | `FeedRange_FilterConsistentWithCrudPartitionKey` | Item stored via CRUD is visible in FeedRange-filtered change feed | BUG 4 | **Critical** |
| 3.3 | `Patch_ModifyingPartitionKeyField_ShouldThrow` | Patch `/partitionKey` op should throw BadRequest | BUG 5 | High (may skip) |
| 3.4 | `Patch_ModifyingPartitionKeyField_EmulatorBehavior_SilentlySucceeds` | Divergent sister test if 3.3 is too hard | BUG 5 | Conditional |

### Phase 4: PK Path Edge Cases

| # | Test Name | Description | Priority |
|---|-----------|-------------|----------|
| 4.1 | `PartitionKey_DeeplyNestedPath_ThreeLevels` | PK path `/a/b/c` with 3-level nesting | High |
| 4.2 | `PartitionKey_DefaultIdPath_WhenNoPathSpecified` | Container created with default `/id` path | Medium |
| 4.3 | `PartitionKey_PathCaseSensitivity_FieldNameMatching` | PK path `/Name` vs field `name` — test case sensitivity | Medium |

### Phase 5: PK with Operations (Cross-Cutting)

| # | Test Name | Description | Priority |
|---|-----------|-------------|----------|
| 5.1 | `Upsert_SameId_DifferentPartitionKey_CreatesTwoItems` | Upsert with different PKs creates separate items | High |
| 5.2 | `Replace_ExplicitPk_MismatchWithBody_UsesExplicitPk` | When explicit PK arg differs from body, which wins? | High |
| 5.3 | `Query_WithPartitionKeyFilter_ReturnsOnlyMatchingPartition` | `SELECT * FROM c WHERE c.partitionKey = "pk1"` only returns PK1 items | High |
| 5.4 | `Query_ParameterizedPkFilter_WorksCorrectly` | `QueryDefinition` with `@pk` parameter | Medium |
| 5.5 | `ReadMany_MixedExistingAndNonExisting_ReturnsOnlyExisting` | ReadMany with some valid, some invalid PK combos | Medium |
| 5.6 | `ReadMany_WithPartitionKeyNull_ReturnsNullPkItems` | ReadMany using `PartitionKey.Null` | Medium |
| 5.7 | `DeleteAllByPartitionKey_NonExistentPk_ReturnsOk` | DeleteAll with PK that has no items — should be no-op 200 OK | Medium |
| 5.8 | `DeleteAllByPartitionKey_WithNullPk_DeletesNullPkItems` | DeleteAll against `PartitionKey.Null` items | Medium |
| 5.9 | `Stream_CreateAndRead_ExplicitPk_WorksCorrectly` | Stream variant CRUD with explicit PK | Medium |

### Phase 6: PK and FeedRange / Change Feed

| # | Test Name | Description | Priority |
|---|-----------|-------------|----------|
| 6.1 | `SamePartitionKey_AlwaysInSameFeedRange` | All items with same PK hash to same FeedRange | High |
| 6.2 | `ChangeFeed_IncludesPartitionKeyFieldsInEntries` | Change feed entries have PK path values populated | Medium |
| 6.3 | `ChangeFeed_DeleteTombstone_HasCorrectPkFields` | Delete tombstone JSON has correct PK field values | Medium |

---

## Execution Order (TDD Cycle)

The implementation order follows dependency and risk priority:

### Step 1 — Phase 3 Bug Fixes (Highest Priority)
1. Write test `3.1` (DeleteAll change feed) → RED
2. Fix BUG 1 in `InMemoryContainer.cs` → GREEN
3. Write test `3.2` (FeedRange consistency) → RED
4. Fix BUG 4 in `InMemoryContainer.cs` (unify ExtractPK methods) → GREEN
5. Write test `2.1` (composite null component) → RED
6. Fix BUG 3 in `InMemoryContainer.cs` (preserve null components) → GREEN
7. Write test `3.3` (patch PK rejection) → RED → evaluate difficulty
   - If feasible: implement rejection in `PatchItemAsync` → GREEN
   - If too hard: mark `3.3` as Skipped, write `3.4` as sister divergent test

### Step 2 — Phase 1 Data Types (Should Mostly Pass Already)
8. Write all Phase 1 tests → expect GREEN (no implementation changes)
9. Any that fail → fix → GREEN

### Step 3 — Phase 2 Composite Key Edge Cases  
10. Write `2.2` through `2.8` → RED/GREEN as needed

### Step 4 — Phase 4 Path Edge Cases
11. Write `4.1` through `4.3` → RED/GREEN as needed

### Step 5 — Phase 5 Operations Cross-Cutting
12. Write `5.1` through `5.9` → RED/GREEN as needed

### Step 6 — Phase 6 FeedRange/ChangeFeed
13. Write `6.1` through `6.3` → RED/GREEN as needed

### Step 7 — Refactor
14. Clean up test class names (consolidate from GapTests1/2/3/4 into logical classes)
15. Ensure all test names follow consistent naming convention

---

## Documentation Updates Required

### Wiki Files to Update (c:\git\CosmosDB.InMemoryEmulator.wiki\)

1. **Known-Limitations.md**
   - Add: Composite PK `|` delimiter means values containing `|` in components may cause ambiguity
   - Add: Patch operations do not prevent modifying partition key fields (if skipped/divergent)
   - Update: Any existing PK-related limitations affected by bug fixes
   - Remove any limitations that are fixed by this work

2. **Features.md**
   - Update partition key section with enhanced composite/hierarchical PK support details
   - Note change feed tombstone recording for DeleteAllByPartitionKey (after BUG 1 fix)

3. **Feature-Comparison-With-Alternatives.md**
   - Update partition key handling row if any comparison claims change
   - Note enhanced composite PK fidelity

4. **SQL-Queries.md**
   - No changes expected unless query-related PK tests reveal issues

### Root Files

5. **README.md** (c:\git\CosmosDB.InMemoryEmulator\README.md)
   - Update test count (from ~1350 to new count after additions)
   - Minor: note hierarchical partition key improvements if significant

6. **Version bump**: `CosmosDB.InMemoryEmulator.csproj` → `2.0.5`

---

## Final Steps

1. Run full test suite: `dotnet test tests/CosmosDB.InMemoryEmulator.Tests --verbosity minimal`
2. Verify all new tests pass (or are correctly skipped with reasons)
3. Update this plan with final status of each test
4. Commit with message: `v2.0.5: Partition key bug fixes & comprehensive test coverage`
5. Tag: `git tag v2.0.5`
6. Push: `git push && git push --tags`
7. Update wiki: commit and push wiki changes

---

## Progress Tracker

| Phase | Status | Tests Written | Tests Passing | Tests Skipped |
|-------|--------|---------------|---------------|---------------|
| Phase 1: Data Types | ⬜ Not Started | 0/6 | 0 | 0 |
| Phase 2: Composite PKs | ⬜ Not Started | 0/8 | 0 | 0 |
| Phase 3: Bug Fixes | ⬜ Not Started | 0/4 | 0 | 0 |
| Phase 4: Path Edge Cases | ⬜ Not Started | 0/3 | 0 | 0 |
| Phase 5: Operations | ⬜ Not Started | 0/9 | 0 | 0 |
| Phase 6: FeedRange/CF | ⬜ Not Started | 0/3 | 0 | 0 |
| Documentation | ⬜ Not Started | — | — | — |
| Version/Tag/Push | ⬜ Not Started | — | — | — |
| **TOTAL** | ⬜ | **0/33** | **0** | **0** |
