# Patch Operations — Deep Dive Test Coverage & Bug Fix Plan

**Created:** 2026-04-01  
**Version:** Will increment from 2.0.4 → 2.0.5  
**Tag:** v2.0.5  
**Status:** PLANNING (not yet implemented)

---

## Sources

- [Partial Document Update in Azure Cosmos DB](https://learn.microsoft.com/en-us/azure/cosmos-db/partial-document-update)
- [Partial Document Update FAQ](https://learn.microsoft.com/en-us/azure/cosmos-db/partial-document-update-faq)
- Implementation: `src/CosmosDB.InMemoryEmulator/InMemoryContainer.cs` (lines 611-688, 926-972, 5207-5299)
- Existing tests: `tests/CosmosDB.InMemoryEmulator.Tests/PatchItemTests.cs`

---

## Existing Test Coverage Summary

The current `PatchItemTests.cs` has ~30 tests spread across several classes:
- `PatchItemTests` — basic CRUD: Set, Replace, Add, Remove, Increment, not-found, ETag match/stale, ETag update, multiple ops, nested
- `PatchPartitionKeyImmutabilityTests5` — PK immutability
- `PatchAtomicityTests5` — atomicity rollback
- `PatchGapTests3` — increment non-numeric, long type preservation, array append/insert, filter predicate, add new property, empty ops
- `PatchGapTests` — remove non-existent path, sequential ops, change feed, response content, deep nested, move
- `PatchGapTests2` — increment double, set creates missing property, remove /id, replace vs set (incomplete), not-found duplicate
- `PatchEnableContentResponseDivergentBehaviorTests` — content response suppression (weak)

Additional coverage in other files:
- `CrudTests.cs` — cancellation token, null id, null ops, EnableContentResponseOnWrite=false, unique key violation
- `FakeCosmosHandlerCrudTests.cs` — HTTP handler: set, multiple ops, not-found, filter predicate match/non-match
- `BulkOperationTests.cs` — bulk patch
- `ChangeFeedTests.cs` — change feed recording

---

## BUGS FOUND (Implementation vs Cosmos DB Specification)

### BUG 1: Replace treated identically to Set — should error on non-existent path
**Severity:** Medium  
**Location:** `InMemoryContainer.cs:5240-5247`  
**Issue:** `PatchOperationType.Replace` shares the same case as `PatchOperationType.Set`. Per Cosmos DB docs: _"Replace is similar to Set except it follows strict replace-only semantics. In case the target path specifies an element or an array that doesn't exist, it results in an error."_  
**Current behavior:** Replace silently creates the property (same as Set).  
**Expected behavior:** Replace should throw `BadRequest` if the target property doesn't exist.  
**Fix:** Split the `Set`/`Replace` case. For `Replace`, check existence before setting. For array paths, also validate.

### BUG 2: Remove on non-existent path succeeds — should error  
**Severity:** Medium  
**Location:** `InMemoryContainer.cs:5248-5253`  
**Issue:** Per Cosmos DB docs: _"If the target path specifies an element that doesn't exist, it results in an error."_ The current implementation just calls `parent.Remove(propertyName)` which is a silent no-op.  
**Current behavior:** `Patch_Remove_OnNonExistentPath_Succeeds` test explicitly asserts HTTP 200.  
**Expected behavior:** Should throw `BadRequest`.  
**Impact:** The existing test `Patch_Remove_OnNonExistentPath_Succeeds` asserts *wrong* behavior and must be updated.  
**Fix:** Check `parent.ContainsKey(propertyName)` before removing; throw if missing.

### BUG 3: Increment on non-existent field is silent no-op — should create it  
**Severity:** Medium  
**Location:** `InMemoryContainer.cs:5276-5299`  
**Issue:** Per Cosmos DB docs: _"If the field doesn't exist, it creates the field and sets it to the specified value."_ The `if (existingToken is not null && ...)` guard prevents this.  
**Current behavior:** Silently does nothing when the field doesn't exist.  
**Expected behavior:** Creates the field and sets it to the increment value.  
**Fix:** When `existingToken is null`, create the property with the increment value.

### BUG 4: Move from non-existent source is silent no-op — should error  
**Severity:** Low  
**Location:** `InMemoryContainer.cs:5254-5275`  
**Issue:** Per Cosmos DB docs: _'The "from" location MUST exist for the operation to be successful.'_ The code checks `if (sourceValue is not null)` and silently does nothing.  
**Current behavior:** No-op.  
**Expected behavior:** Should throw `BadRequest`.  
**Fix:** Throw when source property is null/missing.

### BUG 5: Set/Replace at array index doesn't work correctly  
**Severity:** Medium  
**Location:** `InMemoryContainer.cs:5240-5247`  
**Issue:** Per Cosmos DB docs: _"Set is similar to Add except with the array data type. If the target path is a valid array index, the existing element at that index is updated."_ The current implementation doesn't handle array parents for Set/Replace — it falls through to `(rawParent as JObject ?? jObj)` which misidentifies the parent.  
**Current behavior:** Likely writes to root JObject with numeric property name instead of updating array element.  
**Expected behavior:** Set at `/tags/0` should update the first element of the tags array.  
**Fix:** Add array index handling to the Set/Replace case, similar to Add.

### BUG 6: Remove at array index not handled  
**Severity:** Medium  
**Location:** `InMemoryContainer.cs:5248-5253`  
**Issue:** Per Cosmos DB docs: _"If the target path is an array index, it's deleted and any elements above the specified index are shifted back one position."_ The Remove case only handles JObject properties.  
**Current behavior:** Tries `parent.Remove(propertyName)` on JArray parent, which does nothing meaningful.  
**Expected behavior:** `JArray.RemoveAt(index)` with element shifting.  
**Fix:** Add array index handling similar to Add.

### BUG 7: Add at index > array length not validated  
**Severity:** Low  
**Location:** `InMemoryContainer.cs:5219-5239`  
**Issue:** Per Cosmos DB docs: _"Specifying an index greater than the array length results in an error."_ No bounds check on `insertIdx`.  
**Current behavior:** Likely throws `ArgumentOutOfRangeException` from `JArray.Insert()`, not a proper `CosmosException`.  
**Fix:** Validate index bounds and throw proper `BadRequest`.

### BUG 8: Remove at array index ≥ length not validated  
**Severity:** Low  
**Location:** `InMemoryContainer.cs:5248-5253`  
**Issue:** Per docs: _"Specifying an index equal to or greater than the array length would result in an error."_  
**Fix:** Validate after implementing array Remove.

### BUG 9: System-generated properties can be patched  
**Severity:** Medium  
**Location:** `InMemoryContainer.cs:611-688`  
**Issue:** Per Cosmos DB FAQ: _"We don't support partial document update for system-generated properties like `_id`, `_ts`, `_etag`, `_rid`."_ No validation exists.  
**Current behavior:** Can patch `_ts`, `_etag`, `_rid` — they get overwritten by `EnrichWithSystemProperties` afterwards, but this shouldn't be silently allowed.  
**Decision:** TBD — may skip if too difficult. Real Cosmos returns 400.

### BUG 10: 10 operations limit not enforced  
**Severity:** Low  
**Location:** `InMemoryContainer.cs:619-623`  
**Issue:** Per Cosmos DB FAQ: _"There's a limit of 10 patch operations that can be added in a single patch specification."_  
**Current behavior:** No upper limit.  
**Fix:** Add count validation alongside the existing empty check.

### BUG 11: Wiki says "five" operation types — should be six  
**Severity:** Cosmetic  
**Location:** `Features.md:7`  
**Issue:** Says "Supports all five Cosmos DB patch operation types" but there are 6: Set, Replace, Add, Remove, Increment, Move.  
**Fix:** Change to "six" and add Move to the operation types table.

---

## NEW TESTS TO WRITE

All new tests go in `PatchItemTests.cs` following existing patterns.

### Group A: Replace Strict Semantics (Bug 1)
- [ ] A1. `Replace_NonExistentProperty_ThrowsBadRequest` — Replace on a path that doesn't exist should throw
- [ ] A2. `Replace_ExistingProperty_UpdatesValue` — Replace on existing property works normally
- [ ] A3. `Replace_NonExistentNestedProperty_ThrowsBadRequest` — Replace on nested non-existent path should throw
- [ ] A4. `Replace_NonExistentArrayElement_ThrowsBadRequest` — Replace at out-of-bounds array index should throw

### Group B: Remove Strict Semantics (Bug 2)
- [ ] B1. `Remove_NonExistentProperty_ThrowsBadRequest` — Remove non-existent property should error **(flips existing test)**
- [ ] B2. `Remove_ExistingProperty_Succeeds` — Remove existing property works (already covered, verify)
- [ ] B3. `Remove_NonExistentNestedProperty_ThrowsBadRequest` — Nested path removal error
- [ ] B4. `Remove_ArrayElement_ByIndex_RemovesAndShifts` — Remove at array[1] shifts elements (Bug 6)
- [ ] B5. `Remove_ArrayElement_IndexOutOfBounds_ThrowsBadRequest` — Index ≥ length errors (Bug 8)

### Group C: Increment Creates Field (Bug 3)
- [ ] C1. `Increment_NonExistentField_CreatesWithValue` — Increment missing field creates it with the value
- [ ] C2. `Increment_NonExistentField_NegativeValue_CreatesNegative` — Creates field with negative value
- [ ] C3. `Increment_NonExistentNestedField_CreatesField` — Nested path creation on increment

### Group D: Move Validation (Bug 4)
- [ ] D1. `Move_NonExistentSource_ThrowsBadRequest` — Source must exist
- [ ] D2. `Move_PathIsChildOfFrom_ThrowsBadRequest` — `/a` → `/a/b` should error (per docs: "The 'path' attribute can't be a JSON child of the 'from' JSON location")
- [ ] D3. `Move_ToExistingPath_OverwritesTarget` — Move overwrites destination (per docs)
- [ ] D4. `Move_CreatesIntermediateObjects` — Move to deep path creates intermediate objects (per docs: "If the 'path' location suggests an object that doesn't exist, it creates the object")

### Group E: Array Operations (Bugs 5, 6, 7)
- [ ] E1. `Set_ArrayIndex_UpdatesExistingElement` — Set at `/tags/0` updates element 0 (Bug 5)
- [ ] E2. `Set_ArrayIndex_OutOfBounds_ThrowsBadRequest` — Set at index > length errors
- [ ] E3. `Add_ArrayIndex_BeyondLength_ThrowsBadRequest` — Add at index > array.length errors (Bug 7)
- [ ] E4. `Add_ArrayIndex_AtLength_AppendsElement` — Add at index == length appends (same as `-`)
- [ ] E5. `Remove_ArrayIndex_ValidIndex_RemovesElement` — Duplicate of B4 (consolidated)

### Group F: System Property Protection (Bug 9)
- [ ] F1. `Patch_SystemProperty_Ts_ThrowsBadRequest` — Cannot patch `_ts`
- [ ] F2. `Patch_SystemProperty_Etag_ThrowsBadRequest` — Cannot patch `_etag`
- [ ] F3. `Patch_SystemProperty_Rid_ThrowsBadRequest` — Cannot patch `_rid`
- [ ] F4. `Patch_SystemProperty_Self_ThrowsBadRequest` — Cannot patch `_self`

### Group G: Operations Limit (Bug 10)
- [ ] G1. `Patch_MoreThan10Operations_ThrowsBadRequest` — 11+ operations should error
- [ ] G2. `Patch_Exactly10Operations_Succeeds` — 10 operations at the limit works fine

### Group H: Stream Variant Coverage
- [ ] H1. `PatchItemStreamAsync_SetOperation_ReturnsOK` — Basic stream patch
- [ ] H2. `PatchItemStreamAsync_NonExistentItem_ReturnsNotFound` — Stream variant 404
- [ ] H3. `PatchItemStreamAsync_StaleETag_ReturnsPreconditionFailed` — Stream ETag check
- [ ] H4. `PatchItemStreamAsync_FilterPredicate_NonMatching_ReturnsPreconditionFailed` — Stream filter predicate
- [ ] H5. `PatchItemStreamAsync_EmptyOperations_ReturnsBadRequest` — Stream validation (NOTE: PatchItemStreamAsync doesn't currently validate empty ops — also a bug to fix)

### Group I: Edge Cases & Miscellaneous
- [ ] I1. `Patch_SetNullValue_SetsPropertyToNull` — Set with explicit null
- [ ] I2. `Patch_SetComplexObject_SetsNestedStructure` — Set with complex object value
- [ ] I3. `Patch_IncrementNegative_Decrements` — Negative increment
- [ ] I4. `Patch_IncrementDouble_ToLong_WhenWholeNumber` — 1.0 + 1.0 = 2 stored as long? (verify type behavior)
- [ ] I5. `Patch_WrongPartitionKey_ThrowsNotFound` — ID exists but wrong PK
- [ ] I6. `Patch_AfterDelete_ThrowsNotFound` — Patch after item deletion
- [ ] I7. `Patch_UpdatesTimestamp` — _ts is updated on patch
- [ ] I8. `Patch_WithIfNoneMatchETag_Behavior` — IfNoneMatch etag handling (if supported)
- [ ] I9. `Patch_Set_BooleanValue_Updates` — Boolean value patching
- [ ] I10. `Patch_Set_ArrayValue_ReplacesEntireArray` — Set entire array
- [ ] I11. `Patch_Add_ToNonExistentNestedPath_CreatesIntermediates` — Add to `/a/b/c` where `/a/b` doesn't exist
- [ ] I12. `Patch_CancellationToken_Respected` — Already covered in CrudTests, skip duplicate
- [ ] I13. `Patch_NullId_Throws` — Already covered in CrudTests, skip duplicate

### Group J: PatchItemStreamAsync — Missing Validation (Bug in stream path)
- [ ] J1. `PatchItemStreamAsync_NullOperations_ThrowsOrReturnsBadRequest` — Stream path doesn't validate null/empty ops 
- [ ] J2. `PatchItemStreamAsync_UniqueKeyViolation_Behavior` — Stream path doesn't lock on unique keys (see implementation comment)

---

## TESTS THAT MAY BE SKIPPED (Too Difficult / Behavioral Differences)

For each skipped test, a "sister" divergent-behavior test will be written to document the actual emulator behavior.

### Potential Skips:

1. **System property protection (F1-F4)** — If implementing validation for all system props is too complex, skip with reason: _"Real Cosmos DB rejects patches to system-generated properties (_ts, _etag, _rid, _self) with 400 Bad Request. The emulator's EnrichWithSystemProperties overwrites them after the patch making the mutation harmless but not spec-compliant."_ Sister test: `Patch_SystemProperty_Ts_EmulatorOverwrites_ButDoesNotReject`

2. **Path is child of from in Move (D2)** — If detecting JSON path ancestry is too complex, skip with reason: _"Real Cosmos DB rejects Move when path is a JSON child of from (e.g., Move from '/a' to '/a/b'). The emulator does not validate path ancestry."_ Sister test: `Move_PathIsChildOfFrom_EmulatorBehavior_SilentlySucceeds`

3. **Add/Set creating intermediate nested objects for non-existent ancestor paths (I11)** — If deep path auto-creation is complex, skip with reason: _"Real Cosmos DB auto-creates intermediate objects for deep paths. The emulator may throw when ancestor objects don't exist."_ Sister test: `Patch_Add_ToNonExistentNestedPath_EmulatorBehavior_ThrowsOrCreates`

---

## EXISTING TESTS TO FIX

1. **`Patch_Remove_OnNonExistentPath_Succeeds`** (in `PatchGapTests`) — This test asserts wrong behavior. After Bug 2 fix, change to assert `BadRequest` exception. Original behavior documented in a sister divergent test if the fix proves too complex.

2. **`Patch_Replace_VsSet_ReplaceRequiresExistingPath`** (in `PatchGapTests2`) — This test only tests the Set side. After Bug 1 fix, add an assertion that Replace on non-existent path throws.

3. **Wiki Features.md line 7** — Change "five" to "six" and add Move row to the operations table (Bug 11).

---

## IMPLEMENTATION ORDER (TDD: Red → Green → Refactor)

### Phase 1: Increment creates missing field (safest, non-breaking)
1. Write test C1 → red
2. Fix InMemoryContainer Increment case → green
3. Write tests C2, C3 → should pass

### Phase 2: Replace strict semantics
1. Write tests A1, A3, A4 → red
2. Split Set/Replace case in ApplyPatchOperations → green
3. Write test A2 → should pass
4. Array handling for Replace → A4 green

### Phase 3: Remove strict semantics + array support
1. Write test B1 → red (existing test flipped)
2. Fix Remove to check existence → green
3. Write B3 → should pass
4. Write B4 (array remove) → red
5. Implement array index removal → green
6. Write B5 (bounds check) → should pass

### Phase 4: Set/Replace at array index
1. Write E1 → red
2. Add array handling to Set case → green
3. Write E2 → should pass

### Phase 5: Add array bounds validation
1. Write E3 → red
2. Add bounds check to Add array insert → green
3. Write E4 → should pass

### Phase 6: Move validation
1. Write D1 → red
2. Fix Move source check → green
3. Write D2, D3, D4 (skip D2 if too complex)

### Phase 7: Operations limit
1. Write G1 → red
2. Add count check → green
3. Write G2 → should pass

### Phase 8: System property protection
1. Write F1-F4 → red
2. Add system property check → green (or skip with divergent tests)

### Phase 9: Stream variant
1. Write H1-H5 → determine which pass already
2. Fix J1 (missing empty ops validation in stream path) → green
3. Fix J2 or document as limitation

### Phase 10: Edge cases
1. Write I1-I11 → determine which pass, fix any that don't

### Phase 11: Existing test fixes
1. Fix `Patch_Remove_OnNonExistentPath_Succeeds`
2. Enhance `Patch_Replace_VsSet_ReplaceRequiresExistingPath`

---

## DOCUMENTATION UPDATES

### wiki/Features.md
- [ ] Change "five" → "six" operation types
- [ ] Add `Move` row to operation types table: `| Move | Move a property from one path to another | PatchOperation.Move("/from", "/to") |`
- [ ] Add note about 10-operation limit
- [ ] Add note about system property protection (or limitation)
- [ ] Add note about Replace strict semantics (fails on non-existent)
- [ ] Add note about Remove strict semantics (fails on non-existent)
- [ ] Add note about Increment auto-creating fields

### wiki/Known-Limitations.md  
- [ ] Add/update any divergent behaviors that are skipped (system properties, move ancestry, etc.)
- [ ] Remove any existing patch limitations that have been fixed

### wiki/Feature-Comparison-With-Alternatives.md
- [ ] Update patch row to mention Move support: "Set/Add/Replace/Remove/Increment/Move"
- [ ] Add any new rows for fixed behaviors

### README.md
- [ ] Update "all 5 types" → "all 6 types" in features bullet

### Version & Release
- [ ] Bump version in `CosmosDB.InMemoryEmulator.csproj`: 2.0.4 → 2.0.5
- [ ] `git add -A && git commit -m "v2.0.5: Patch operations spec compliance — Replace/Remove strict semantics, Increment auto-create, Move validation, array index ops, 10-op limit"`
- [ ] `git tag v2.0.5`
- [ ] `git push && git push --tags`
- [ ] Update wiki: `cd wiki && git add -A && git commit -m "v2.0.5: Patch operations — updated Features, Known Limitations, Comparison" && git push`

---

## PROGRESS TRACKER

| ID | Test/Task | Status |
|----|-----------|--------|
| A1 | Replace_NonExistentProperty_ThrowsBadRequest | ⬜ Not started |
| A2 | Replace_ExistingProperty_UpdatesValue | ⬜ Not started |
| A3 | Replace_NonExistentNestedProperty_ThrowsBadRequest | ⬜ Not started |
| A4 | Replace_NonExistentArrayElement_ThrowsBadRequest | ⬜ Not started |
| B1 | Remove_NonExistentProperty_ThrowsBadRequest | ⬜ Not started |
| B2 | Remove_ExistingProperty_Succeeds | ⬜ Not started |
| B3 | Remove_NonExistentNestedProperty_ThrowsBadRequest | ⬜ Not started |
| B4 | Remove_ArrayElement_ByIndex_RemovesAndShifts | ⬜ Not started |
| B5 | Remove_ArrayElement_IndexOutOfBounds_ThrowsBadRequest | ⬜ Not started |
| C1 | Increment_NonExistentField_CreatesWithValue | ⬜ Not started |
| C2 | Increment_NonExistentField_NegativeValue_CreatesNegative | ⬜ Not started |
| C3 | Increment_NonExistentNestedField_CreatesField | ⬜ Not started |
| D1 | Move_NonExistentSource_ThrowsBadRequest | ⬜ Not started |
| D2 | Move_PathIsChildOfFrom_ThrowsBadRequest | ⬜ Not started |
| D3 | Move_ToExistingPath_OverwritesTarget | ⬜ Not started |
| D4 | Move_CreatesIntermediateObjects | ⬜ Not started |
| E1 | Set_ArrayIndex_UpdatesExistingElement | ⬜ Not started |
| E2 | Set_ArrayIndex_OutOfBounds_ThrowsBadRequest | ⬜ Not started |
| E3 | Add_ArrayIndex_BeyondLength_ThrowsBadRequest | ⬜ Not started |
| E4 | Add_ArrayIndex_AtLength_AppendsElement | ⬜ Not started |
| F1 | Patch_SystemProperty_Ts_ThrowsBadRequest | ⬜ Not started |
| F2 | Patch_SystemProperty_Etag_ThrowsBadRequest | ⬜ Not started |
| F3 | Patch_SystemProperty_Rid_ThrowsBadRequest | ⬜ Not started |
| F4 | Patch_SystemProperty_Self_ThrowsBadRequest | ⬜ Not started |
| G1 | Patch_MoreThan10Operations_ThrowsBadRequest | ⬜ Not started |
| G2 | Patch_Exactly10Operations_Succeeds | ⬜ Not started |
| H1 | PatchItemStreamAsync_SetOperation_ReturnsOK | ⬜ Not started |
| H2 | PatchItemStreamAsync_NonExistentItem_ReturnsNotFound | ⬜ Not started |
| H3 | PatchItemStreamAsync_StaleETag_ReturnsPreconditionFailed | ⬜ Not started |
| H4 | PatchItemStreamAsync_FilterPredicate_NonMatching_ReturnsPreconditionFailed | ⬜ Not started |
| H5 | PatchItemStreamAsync_EmptyOperations_ReturnsBadRequest | ⬜ Not started |
| I1 | Patch_SetNullValue_SetsPropertyToNull | ⬜ Not started |
| I2 | Patch_SetComplexObject_SetsNestedStructure | ⬜ Not started |
| I3 | Patch_IncrementNegative_Decrements | ⬜ Not started |
| I4 | Patch_IncrementDouble_ToLong_WhenWholeNumber | ⬜ Not started |
| I5 | Patch_WrongPartitionKey_ThrowsNotFound | ⬜ Not started |
| I6 | Patch_AfterDelete_ThrowsNotFound | ⬜ Not started |
| I7 | Patch_UpdatesTimestamp | ⬜ Not started |
| I8 | Patch_WithIfNoneMatchETag_Behavior | ⬜ Not started |
| I9 | Patch_Set_BooleanValue_Updates | ⬜ Not started |
| I10 | Patch_Set_ArrayValue_ReplacesEntireArray | ⬜ Not started |
| I11 | Patch_Add_ToNonExistentNestedPath_CreatesIntermediates | ⬜ Not started |
| J1 | PatchItemStreamAsync_NullOperations_Behavior | ⬜ Not started |
| J2 | PatchItemStreamAsync_UniqueKeyViolation_Behavior | ⬜ Not started |
| BUG1 | Fix Replace strict semantics | ⬜ Not started |
| BUG2 | Fix Remove strict semantics | ⬜ Not started |
| BUG3 | Fix Increment auto-create | ⬜ Not started |
| BUG4 | Fix Move source validation | ⬜ Not started |
| BUG5 | Fix Set at array index | ⬜ Not started |
| BUG6 | Fix Remove at array index | ⬜ Not started |
| BUG7 | Fix Add array bounds check | ⬜ Not started |
| BUG8 | Fix Remove array bounds check | ⬜ Not started |
| BUG9 | Fix system property protection | ⬜ Not started |
| BUG10 | Fix 10 operations limit | ⬜ Not started |
| BUG11 | Fix wiki "five" → "six" | ⬜ Not started |
| DOC1 | Update wiki/Features.md | ⬜ Not started |
| DOC2 | Update wiki/Known-Limitations.md | ⬜ Not started |
| DOC3 | Update wiki/Feature-Comparison.md | ⬜ Not started |
| DOC4 | Update README.md | ⬜ Not started |
| REL1 | Bump version to 2.0.5 | ⬜ Not started |
| REL2 | Tag and push | ⬜ Not started |
| REL3 | Push wiki updates | ⬜ Not started |

---

## NOTES

- The `PatchItemStreamAsync` path (lines 926-972) is missing both the empty-ops validation AND the unique-key locking that the typed `PatchItemAsync` has. This is a secondary bug to address.
- Path escaping (`~0` for `~`, `~1` for `/`) per JSON Patch RFC is NOT tested and likely not implemented. Consider adding if trivial, otherwise document as limitation.
- TTL (`/ttl`) can be patched per Cosmos DB docs — verify this works (it should since there's no restriction in the implementation).
- The `id` field removal test (`Patch_Remove_IdField_DoesNotCorrupt`) documents a gray area — real Cosmos likely rejects this. Consider adding system field protection for `id` too.
