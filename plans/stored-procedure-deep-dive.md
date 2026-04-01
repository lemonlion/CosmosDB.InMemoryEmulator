# Stored Procedure Deep Dive — Test Coverage & Implementation Plan

**Created:** 2026-04-01  
**Version target:** 2.0.4 → 2.0.5  
**Approach:** TDD (Red-Green-Refactor)

---

## 1. Current State Analysis

### 1.1 What Exists in StoredProcedureTests.cs

| # | Test | Class | Status |
|---|------|-------|--------|
| 1 | `CreateStoredProcedure_ReturnsCreated` | `StoredProcedureTests` | ✅ Passing |
| 2 | `ExecuteStoredProcedure_ReturnsOk` | `StoredProcedureTests` | ✅ Passing |
| 3 | `ExecuteStoredProcedure_WithRegisteredHandler_ExecutesLogic` | `StoredProcedureTests` | ✅ Passing |
| 4 | `ExecuteStoredProcedure_WithRegisteredHandler_ReceivesArguments` | `StoredProcedureTests` | ✅ Passing |
| 5 | `ExecuteStoredProcedure_WithoutRegisteredHandler_ReturnsDefault` | `StoredProcedureTests` | ✅ Passing |
| 6 | `RegisterStoredProcedure_WithContainerAccess_CanReadItems` | `StoredProcedureTests` | ✅ Passing |
| 7 | `DeregisterStoredProcedure_RemovesHandler` | `StoredProcedureTests` | ✅ Passing |
| 8 | `StoredProc_WithPartitionKey_ExecutesInPartition` | `StoredProcGapTests3` | ✅ Passing |
| 9 | `StoredProc_Register_Execute_ReturnsResult` | `StoredProcGapTests` | ✅ Passing |
| 10 | `Udf_NotRegistered_ThrowsOnQuery` | `StoredProcGapTests` | ✅ Passing (misplaced — UDF test in sproc class) |

### 1.2 What's Implemented in InMemoryContainer.cs

- **`RegisterStoredProcedure(id, handler)`** — stores `Func<PartitionKey, dynamic[], string>` in `_storedProcedures` dictionary (ordinal case-sensitive)
- **`DeregisterStoredProcedure(id)`** — removes handler from dictionary
- **`Scripts.CreateStoredProcedureAsync()`** — NSubstitute mock, returns 201 Created + `StoredProcedureProperties` — but does NOT store the properties anywhere
- **`Scripts.ExecuteStoredProcedureAsync<string>()`** — NSubstitute mock, looks up handler by ID, returns handler result or empty string

### 1.3 What's NOT Implemented

| Method | Status | Trigger equivalent |
|--------|--------|--------------------|
| `ReadStoredProcedureAsync` | ❌ Not mocked | ✅ Trigger has `ReadTriggerAsync` |
| `ReplaceStoredProcedureAsync` | ❌ Not mocked | ✅ Trigger has `ReplaceTriggerAsync` |
| `DeleteStoredProcedureAsync` | ❌ Not mocked | ✅ Trigger has `DeleteTriggerAsync` |
| `GetStoredProcedureQueryIterator` | ❌ Not mocked | ❌ Trigger doesn't have this either |
| `_storedProcedureProperties` dictionary | ❌ Doesn't exist | ✅ Trigger has `_triggerProperties` |

**Key observation:** Triggers have full CRUD (Create/Read/Replace/Delete) with metadata storage in `_triggerProperties`. Stored procedures only have Create + Execute with NO metadata storage. This is the primary gap.

---

## 2. Bugs Found

### Bug 1: No metadata persistence for created stored procedures
**Severity:** Medium  
**Description:** `CreateStoredProcedureAsync` returns the `StoredProcedureProperties` from the call args but doesn't store them anywhere. Unlike triggers which maintain `_triggerProperties`, there's no `_storedProcedureProperties` dictionary. This means:
- You can't read back a created procedure
- You can't replace a created procedure
- You can't detect duplicate creation (real Cosmos returns 409 Conflict)
- You can't delete a created procedure

**Fix:** Add `_storedProcedureProperties` dictionary (same pattern as triggers) and store properties on create.

### Bug 2: Duplicate CreateStoredProcedureAsync doesn't throw 409 Conflict
**Severity:** Low  
**Description:** In real Cosmos DB, calling `CreateStoredProcedureAsync` with an ID that already exists returns HTTP 409 Conflict. The emulator silently succeeds every time since it doesn't track created procedures.

**Fix:** After adding properties storage, check for existing ID and throw `CosmosException` with 409.

### Bug 3: Misplaced test — `Udf_NotRegistered_ThrowsOnQuery` in `StoredProcGapTests`
**Severity:** Cosmetic  
**Description:** This test is about UDFs, not stored procedures, but lives in a class called `StoredProcGapTests`. Should be moved to a UDF test class.

**Fix:** Move to `UdfGapTests` class (already exists in same file).

---

## 3. Test Plan — New Tests to Write

All new tests go in `StoredProcedureTests.cs`. Tests are grouped by category. Each test follows TDD: write failing test → implement → verify green.

### Phase 1: Stored Procedure CRUD Metadata (requires `_storedProcedureProperties`)

| # | Test Name | Description | Requires Implementation |
|---|-----------|-------------|------------------------|
| T1 | `ReadStoredProcedure_AfterCreate_ReturnsProperties` | Create sproc, then ReadStoredProcedureAsync — should return 200 OK with matching Id and Body | Yes: Add `_storedProcedureProperties` dict, mock `ReadStoredProcedureAsync` |
| T2 | `ReadStoredProcedure_NotFound_Throws404` | Read a non-existent sproc — should throw `CosmosException` with 404 | Yes: same mock |
| T3 | `ReplaceStoredProcedure_UpdatesBody` | Create sproc, replace with new body, read back — should see updated body | Yes: Mock `ReplaceStoredProcedureAsync` |
| T4 | `ReplaceStoredProcedure_NotFound_Throws404` | Replace a non-existent sproc — should throw `CosmosException` with 404 | Yes: same mock |
| T5 | `DeleteStoredProcedure_RemovesMetadata` | Create sproc, delete it, then read — should throw 404 | Yes: Mock `DeleteStoredProcedureAsync` |
| T6 | `DeleteStoredProcedure_NotFound_Throws404` | Delete a non-existent sproc — should throw `CosmosException` with 404 | Yes: same mock |
| T7 | `CreateStoredProcedure_DuplicateId_Throws409` | Create two sprocs with same ID — second should throw `CosmosException` with 409 | Yes: Add conflict check in CreateStoredProcedureAsync mock |

### Phase 2: Registration & Deregistration Edge Cases

| # | Test Name | Description | Requires Implementation |
|---|-----------|-------------|------------------------|
| T8 | `RegisterStoredProcedure_SameIdTwice_OverwritesHandler` | Register "sp1" with handler A, then with handler B — executing should use handler B | No |
| T9 | `RegisterStoredProcedure_CaseSensitive_DifferentHandlers` | Register "myProc" and "MYPROC" as separate handlers — both should work independently | No |
| T10 | `DeregisterStoredProcedure_NonExistent_DoesNotThrow` | Deregister a sproc that was never registered — should not throw | No |
| T11 | `DeregisterStoredProcedure_ThenReRegister_Works` | Register → deregister → re-register → execute — should work | No |

### Phase 3: Execution Edge Cases

| # | Test Name | Description | Requires Implementation |
|---|-----------|-------------|------------------------|
| T12 | `ExecuteStoredProcedure_NullArguments_PassedToHandler` | Execute with `null` instead of `dynamic[]` — verify handler receives null | No |
| T13 | `ExecuteStoredProcedure_EmptyArguments_PassedToHandler` | Execute with `Array.Empty<dynamic>()` — verify handler receives empty array | No |
| T14 | `ExecuteStoredProcedure_ManyArguments_AllPassedToHandler` | Execute with 10+ arguments — verify all arrive | No |
| T15 | `ExecuteStoredProcedure_ComplexJsonArguments_Deserializable` | Pass JObject/complex objects as args — verify handler can work with them | No |
| T16 | `ExecuteStoredProcedure_HandlerReturnsNull_ResourceIsNull` | Handler returns null — verify response.Resource is null | No |
| T17 | `ExecuteStoredProcedure_HandlerReturnsComplexJson_Deserialized` | Handler returns `JsonConvert.SerializeObject(new { a = 1, b = "x" })` — verify deserialization | No |
| T18 | `ExecuteStoredProcedure_HandlerThrowsException_PropagatesException` | Handler throws InvalidOperationException — should propagate to caller | No |
| T19 | `ExecuteStoredProcedure_ReturnsRequestCharge` | Verify `response.RequestCharge` returns synthetic 1.0 RU | No |

### Phase 4: Handler Container Access Patterns

| # | Test Name | Description | Requires Implementation |
|---|-----------|-------------|------------------------|
| T20 | `RegisterStoredProcedure_HandlerCanCreateItems` | Handler calls `_container.CreateItemAsync()` — items should be persisted | No |
| T21 | `RegisterStoredProcedure_HandlerCanDeleteItems` | Handler calls `_container.DeleteItemAsync()` — items should be removed | No |
| T22 | `RegisterStoredProcedure_HandlerCanReplaceItems` | Handler calls `_container.ReplaceItemAsync()` — items should be updated | No |
| T23 | `RegisterStoredProcedure_HandlerCanQueryWithFilter` | Handler runs parameterized query `SELECT * FROM c WHERE c.partitionKey = @pk` | No |
| T24 | `RegisterStoredProcedure_BulkDeletePattern` | Handler queries and deletes all items in partition — common real-world pattern | No |

### Phase 5: Partition Key Behavior

| # | Test Name | Description | Requires Implementation |
|---|-----------|-------------|------------------------|
| T25 | `ExecuteStoredProcedure_PartitionKeyValue_PassedCorrectly` | Execute with `new PartitionKey("specific-pk")` — verify handler receives exactly that PK | No |
| T26 | `ExecuteStoredProcedure_PartitionKeyNone_PassedCorrectly` | Execute with `PartitionKey.None` — verify handler receives it | No |

### Phase 6: Divergent Behavior Documentation (Skipped tests with sister tests)

| # | Test Name | Description | Skip Reason | Sister Test |
|---|-----------|-------------|-------------|-------------|
| T27 | `ExecuteStoredProcedure_NotRegistered_ShouldThrow404` | **SKIP**: Real Cosmos throws 404 when sproc doesn't exist | Emulator returns 200 OK for unregistered sprocs to allow flexible testing without requiring registration for every sproc | `ExecuteStoredProcedure_NotRegistered_EmulatorReturns200` |
| T28 | `ExecuteStoredProcedure_JavaScriptBody_ShouldExecute` | **SKIP**: Real Cosmos executes JavaScript body server-side | Emulator uses C# handler pattern via `RegisterStoredProcedure()` — JavaScript bodies are stored via `CreateStoredProcedureAsync` but not interpreted. Use `CosmosDB.InMemoryEmulator.JsTriggers` for JS execution in triggers. | `ExecuteStoredProcedure_WithCSharpHandler_ExecutesLogicInstead` |
| T29 | `GetStoredProcedureQueryIterator_ShouldEnumerateProcedures` | **SKIP**: Real Cosmos supports `GetStoredProcedureQueryIterator()` to list all sprocs | Not implemented — `GetStoredProcedureQueryIterator` is not mocked on the NSubstitute Scripts proxy. Would require significant work to return a `FeedIterator<StoredProcedureProperties>`. | N/A (no sister test — simply not available) |
| T30 | `ExecuteStoredProcedure_NonStringGenericType_ShouldDeserialize` | **SKIP**: Real Cosmos supports `ExecuteStoredProcedureAsync<T>` for any serializable T | Only `ExecuteStoredProcedureAsync<string>` is mocked via NSubstitute. Adding mocks for all possible `<T>` types is not feasible with the current NSubstitute-based approach. Workaround: use `<string>` and deserialize the JSON result manually. | `ExecuteStoredProcedure_StringWithManualDeserialization_Workaround` |

---

## 4. Implementation Order (TDD Red-Green-Refactor)

### Step 1: Write ALL failing tests first (RED phase)
Write tests T1–T30 in `StoredProcedureTests.cs`. Tests T1–T7 will fail (require implementation). Tests T8–T26 should mostly pass already (verify existing behavior). Tests T27–T30 are skip/divergent tests.

### Step 2: Implement stored procedure metadata CRUD (GREEN phase)
In `InMemoryContainer.cs`:
1. Add `private readonly Dictionary<string, StoredProcedureProperties> _storedProcedureProperties = new(StringComparer.Ordinal);` (line ~72, alongside `_triggerProperties`)
2. Update `CreateStoredProcedureAsync` mock to store properties and check for duplicates (409)
3. Add `ReadStoredProcedureAsync` mock (same pattern as `ReadTriggerAsync`)
4. Add `ReplaceStoredProcedureAsync` mock (same pattern as `ReplaceTriggerAsync`)
5. Add `DeleteStoredProcedureAsync` mock (same pattern as `DeleteTriggerAsync`)

### Step 3: Fix bugs
1. Move `Udf_NotRegistered_ThrowsOnQuery` from `StoredProcGapTests` to `UdfGapTests`
2. Clean up redundant `StoredProcGapTests` and `StoredProcGapTests3` — consolidate into main `StoredProcedureTests` class

### Step 4: Verify all tests pass (REFACTOR phase)
Run full test suite. All non-skipped tests should be green. Skipped tests should have clear skip reasons.

---

## 5. Documentation Updates

### 5.1 Wiki — Known-Limitations.md
Add new section for stored procedure divergent behavior:
- **Stored Procedures Use C# Handlers (JavaScript Not Interpreted)** — same as trigger pattern
- **ExecuteStoredProcedureAsync Only Mocked for `<string>`** — workaround documented
- **GetStoredProcedureQueryIterator Not Implemented** — not available

### 5.2 Wiki — Features.md  
Update Stored Procedures section to document:
- Full CRUD lifecycle (Create/Read/Replace/Delete) now supported
- Duplicate creation detection (409 Conflict)
- C# handler pattern with container access
- `ExecuteStoredProcedureAsync<string>` limitation and workaround

### 5.3 Wiki — Feature-Comparison-With-Alternatives.md
Update stored procedure row to clarify:
- ✅ (C# handlers, full CRUD metadata) instead of just ✅ (C# handlers)

### 5.4 README.md
No change needed — already mentions "Stored Procedures" in features list.

### 5.5 Wiki — Known-Limitations.md — Behavioural Differences
Add sections:
- **Stored Procedures Use C# Handlers** — with skipped test references
- **ExecuteStoredProcedureAsync<string> Only** — with workaround
- **GetStoredProcedureQueryIterator Not Available** — with skip reference

---

## 6. Version Bump & Release

1. Increment version in `CosmosDB.InMemoryEmulator.csproj`: `2.0.4` → `2.0.5`
2. `git add -A`
3. `git commit -m "v2.0.5: Stored procedure CRUD metadata, comprehensive test coverage"`
4. `git tag v2.0.5`
5. `git push; git push --tags`
6. Push wiki changes separately

---

## 7. Test Execution Checklist

- [ ] Phase 1 tests written (T1–T7) — initially RED
- [ ] Phase 2 tests written (T8–T11) — should be GREEN immediately
- [ ] Phase 3 tests written (T12–T19) — most should be GREEN immediately
- [ ] Phase 4 tests written (T20–T24) — should be GREEN immediately
- [ ] Phase 5 tests written (T25–T26) — should be GREEN immediately
- [ ] Phase 6 tests written (T27–T30) — skipped with reasons + sister tests
- [ ] `_storedProcedureProperties` dictionary added
- [ ] `CreateStoredProcedureAsync` stores properties + 409 duplicate check
- [ ] `ReadStoredProcedureAsync` mocked
- [ ] `ReplaceStoredProcedureAsync` mocked
- [ ] `DeleteStoredProcedureAsync` mocked
- [ ] Bug fix: move misplaced UDF test
- [ ] Bug fix: consolidate test classes
- [ ] All tests green (except intentional skips)
- [ ] Wiki Known-Limitations.md updated
- [ ] Wiki Features.md updated
- [ ] Wiki Feature-Comparison-With-Alternatives.md updated
- [ ] Version bumped to 2.0.5
- [ ] Tagged and pushed
