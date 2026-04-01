# CosmosClient & Database TDD Plan — v2.0.5

## Summary

Deep-dive analysis of `CosmosClientAndDatabaseTests.cs` (2033 lines, ~80 tests) against the
`InMemoryCosmosClient` and `InMemoryDatabase` implementations. This plan covers:

- **1 confirmed bug** to fix (TDD: test first → red → fix → green)
- **~25 new tests** across 8 categories
- **~4 divergent behavior documentation tests** (skipped ideal + passing sister test)
- **Documentation updates** (wiki, README)
- **Version bump** to v2.0.5, tag, and push

Current version: **2.0.4**
Target version: **2.0.5**

---

## Status Key

| Symbol | Meaning |
|--------|---------|
| ⬜ | Not started |
| 🔵 | In progress |
| ✅ | Completed |
| ⏭️ | Skipped (with reason) |

---

## Phase 0: Bug Fixes (TDD — write failing test first)

### BUG-1: `Database.DeleteAsync` / `DeleteStreamAsync` don't clear `_users` dictionary ⬜

**Root cause:** `InMemoryDatabase.DeleteAsync()` and `DeleteStreamAsync()` both call
`_containers.Clear()` but do NOT call `_users.Clear()`. After deleting a database, if you
hold a reference to the old `InMemoryDatabase` instance and query users, they'll still appear.

**Impact:** If code holds a reference to a deleted database and enumerates users, it gets
stale data. In real Cosmos DB, any operation on a deleted database returns 404.

**Tests to write (RED):**
- `DeleteAsync_ClearsUsersFromDatabase` — create users, delete db, query users → expect empty
- `DeleteStreamAsync_ClearsUsersFromDatabase` — same via stream API

**Fix (GREEN):**
Add `_users.Clear();` before `_containers.Clear();` in both `DeleteAsync` and `DeleteStreamAsync`
in `InMemoryDatabase.cs` (lines 198-215).

**Implementation difficulty:** Trivial — one-line fix per method.

---

## Phase 1: Missing CosmosClient Tests ⬜

### 1.1 Concurrent Database Creation ⬜
Tests to verify thread-safety at the client level (parity with existing container concurrency tests).

| # | Test Name | Description |
|---|-----------|-------------|
| 1 | `ConcurrentCreateDatabaseAsync_DifferentIds_AllSucceed` | 20 parallel `CreateDatabaseAsync` with unique IDs — all should return 201 |
| 2 | `ConcurrentCreateDatabaseIfNotExistsAsync_SameId_OnlyOneCreated` | 20 parallel `CreateDatabaseIfNotExistsAsync` with same ID — exactly 1×201, 19×200 |

### 1.2 Database Query Iterator After Delete ⬜

| # | Test Name | Description |
|---|-----------|-------------|
| 3 | `GetDatabaseQueryIterator_AfterDatabaseDelete_NoLongerListed` | Create db1 & db2, delete db1, iterate → only db2 remains |

### 1.3 GetDatabase Same Instance ⬜

| # | Test Name | Description |
|---|-----------|-------------|
| 4 | `GetDatabase_CalledTwice_ReturnsSameInstance` | `client.GetDatabase("x")` twice → `ReferenceEquals(ref1, ref2)` |

### 1.4 CreateDatabaseStreamAsync with ThroughputProperties ⬜

| # | Test Name | Description |
|---|-----------|-------------|
| 5 | `CreateDatabaseStreamAsync_WithThroughputProperties_ReturnsCreated` | Verify the `ThroughputProperties` overload exists and returns 201 |

**Note:** The impl likely delegates to int? — need to check if this overload exists. If missing,
implement it following the same pattern as container stream.

### 1.5 GetDatabaseQueryStreamIterator QueryDefinition Overload ⬜

| # | Test Name | Description |
|---|-----------|-------------|
| 6 | `GetDatabaseQueryStreamIterator_WithQueryDefinition_ReturnsAllDatabases` | Verify the QueryDefinition overload works |

### 1.6 ReadAccountAsync Detailed Verification ⬜

| # | Test Name | Description |
|---|-----------|-------------|
| 7 | `ReadAccountAsync_ReturnsIdAsInMemoryEmulator` | `account.Id` should be `"in-memory-emulator"` |

---

## Phase 2: Missing Database Tests ⬜

### 2.1 CreateContainerAsync Input Validation ⬜

| # | Test Name | Description |
|---|-----------|-------------|
| 8 | `CreateContainerAsync_NullPartitionKeyPath_Throws` | `CreateContainerAsync("c1", null!)` → ArgumentNullException |
| 9 | `CreateContainerAsync_NullId_Throws` | `CreateContainerAsync(null!, "/pk")` → ArgumentNullException |
| 10 | `CreateContainerAsync_EmptyId_Throws` | `CreateContainerAsync("", "/pk")` → ArgumentException |

### 2.2 CreateContainerIfNotExistsAsync with null PK Fallback ⬜

| # | Test Name | Description |
|---|-----------|-------------|
| 11 | `CreateContainerIfNotExistsAsync_ContainerProperties_NullPkPath_DefaultsToId` | Verify null PK path falls back to `/id` |

### 2.3 Delete Then Re-use Same Reference ⬜

| # | Test Name | Description |
|---|-----------|-------------|
| 12 | `DeleteAsync_ThenReadAsync_OnSameReference_StillReturnsOk` | Delete db, then ReadAsync on same ref → 200 (divergent: real would 404) |
| 13 | `DeleteAsync_ThenCreateContainer_OnSameReference_ShouldWork` | Delete db, then create container on same ref → 201 (containers were cleared) |

### 2.4 Standalone Database (No Client) ⬜

| # | Test Name | Description |
|---|-----------|-------------|
| 14 | `DeleteAsync_StandaloneDatabase_NoClient_DoesNotThrow` | `new InMemoryDatabase("x").DeleteAsync()` → NoContent, no NRE |
| 15 | `StandaloneDatabase_Client_ReturnsNull` | `new InMemoryDatabase("x").Client` → null |

### 2.5 User Query Iterator on Empty Database ⬜

| # | Test Name | Description |
|---|-----------|-------------|
| 16 | `GetUserQueryIterator_EmptyDatabase_ReturnsEmpty` | No users created → iterator returns empty list |

### 2.6 Permission Error Handling ⬜

| # | Test Name | Description |
|---|-----------|-------------|
| 17 | `Permission_ReadAsync_NonExistent_ThrowsNotFound` | Get permission ref, read without creating → 404 |
| 18 | `Permission_ReplaceAsync_NonExistent_ThrowsNotFound` | Replace without creating → 404 |
| 19 | `Permission_DeleteAsync_NonExistent_ThrowsNotFound` | Delete without creating → 404 |

### 2.7 CreateDatabaseAsync Response Properties ⬜

| # | Test Name | Description |
|---|-----------|-------------|
| 20 | `CreateDatabaseAsync_ResponseDatabase_IsUsable` | `response.Database` should be non-null and same as `client.GetDatabase(id)` |

---

## Phase 3: Divergent Behavior Tests (Skip + Sister) ⬜

These test the *ideal* SDK behavior (real Cosmos DB). Since the emulator intentionally diverges,
the ideal test is skipped with a detailed reason, and a *sister test* demonstrates the actual
emulator behavior with inline commentary.

### 3.1 GetDatabase Proxy vs Auto-Create ⬜

Already covered by existing `DivergentBehavior_GetDatabase_AutoCreatesDatabase`. No action needed.

### 3.2 Dispose Then Use ⬜

| # | Test Name | Description |
|---|-----------|-------------|
| 21 | `Dispose_ThenCreateDatabase_ShouldThrow` (SKIP) | Real SDK: `ObjectDisposedException` after dispose. Skip reason: "InMemoryCosmosClient.Dispose is a no-op to avoid NullReferenceException from base class. Post-dispose operations continue to work. Real SDK throws ObjectDisposedException." |
| 22 | `DivergentBehavior_Dispose_ThenCreateDatabase_StillWorks` | Sister: emulator continues to function after dispose — documents this intentional divergence |

### 3.3 GetUser Auto-Creates ⬜

| # | Test Name | Description |
|---|-----------|-------------|
| 23 | `GetUser_NonExistent_ShouldThrowNotFound` (SKIP) | Real SDK: `GetUser` returns proxy, `ReadAsync` throws 404 if user doesn't exist. Skip reason: "InMemoryDatabase.GetUser auto-creates user entries for test convenience, mirroring the GetDatabase/GetContainer pattern. Real SDK returns a proxy that throws 404 on ReadAsync for non-existent users." |
| 24 | `DivergentBehavior_GetUser_AutoCreatesUser` | Sister: GetUser on non-existent user → returns valid User with correct Id, ReadAsync succeeds |

### 3.4 Throughput Replace Not Persisted ⬜

| # | Test Name | Description |
|---|-----------|-------------|
| 25 | `ReplaceThroughputAsync_ThenRead_ShouldReturnNewValue` (SKIP) | Real SDK: Replace 1000, Read → 1000. Skip reason: "InMemoryDatabase throughput is synthetic. ReplaceThroughputAsync returns success but doesn't persist the new value. ReadThroughputAsync always returns 400. This keeps the implementation simple since throughput has no behavioral impact on an in-memory store." |
| 26 | `DivergentBehavior_ReplaceThroughputAsync_ThenRead_StillReturns400` | Sister: Replace 1000, then ReadThroughputAsync → still 400, documenting the synthetic nature |

---

## Phase 4: Implementation Plan (TDD Execution Order)

Execute in this order for clean red-green-refactor:

### Step 1: Bug fix tests (Phase 0) — RED
1. Write `DeleteAsync_ClearsUsersFromDatabase` test → fails (users still present)
2. Write `DeleteStreamAsync_ClearsUsersFromDatabase` test → fails

### Step 2: Bug fix (Phase 0) — GREEN
3. Add `_users.Clear();` to `InMemoryDatabase.DeleteAsync` (line ~200)
4. Add `_users.Clear();` to `InMemoryDatabase.DeleteStreamAsync` (line ~210)
5. Both tests pass

### Step 3: CosmosClient tests (Phase 1) — RED → GREEN
6. Write tests 1-7 (all should pass immediately since the implementation already exists)
7. If test 5 (`CreateDatabaseStreamAsync_WithThroughputProperties`) fails because the overload
   is missing, implement it in `InMemoryCosmosClient.cs` following the existing pattern

### Step 4: Database tests (Phase 2) — RED → GREEN
8. Write tests 8-20 (most should pass since input validation and null guards already exist)
9. For permission tests 17-19: verify InMemoryPermission throws 404 — should pass

### Step 5: Divergent behavior tests (Phase 3)
10. Write skipped tests 21, 23, 25 with detailed skip reasons
11. Write sister tests 22, 24, 26 with inline comments

### Step 6: Run full suite
12. `dotnet test tests/CosmosDB.InMemoryEmulator.Tests --verbosity minimal`
13. All tests should pass

---

## Phase 5: Documentation Updates ⬜

### 5.1 Wiki Known-Limitations.md ⬜

Add/update these entries:

1. **Users/Permissions row in limitations table** — update to mention that `GetUser` auto-creates
   (similar to `GetDatabase` auto-create behavior, already noted)
2. **Add Behavioural Difference #16**: `Database.GetUser` auto-creates user entries for test
   convenience. Real SDK returns a proxy; `ReadAsync` throws 404 for non-existent users.
3. **Add Behavioural Difference #17**: `Dispose` is a no-op. Post-dispose operations continue
   to work. Real SDK throws `ObjectDisposedException`.
4. **Add Behavioural Difference #18**: `ReplaceThroughputAsync` succeeds but doesn't persist.
   `ReadThroughputAsync` always returns 400.
5. **Verify existing entry** on throughput being synthetic — may already cover #18.

### 5.2 Wiki Feature-Comparison-With-Alternatives.md ⬜

No changes expected — the comparison table already covers Users & Permissions, throughput, etc.

### 5.3 Wiki Features.md ⬜

Add under **Users & Permissions → Behavioural Differences**:
- `GetUser` auto-creates for test convenience (diverges from real SDK proxy semantics)

### 5.4 README.md ⬜

No changes expected unless new features are added. The bug fix is internal.

---

## Phase 6: Version Bump, Tag & Push ⬜

1. Update `src/CosmosDB.InMemoryEmulator/CosmosDB.InMemoryEmulator.csproj` version to `2.0.5`
2. `git add -A`
3. `git commit -m "v2.0.5: Fix DeleteAsync not clearing users, expand CosmosClient/Database test coverage"`
4. `git tag v2.0.5`
5. `git push`
6. `git push --tags`

---

## Appendix A: Existing Test Inventory (for reference)

Tests already in `CosmosClientAndDatabaseTests.cs` — DO NOT duplicate:

| Category | Count | Tests |
|----------|-------|-------|
| Foundation (Endpoint, Client, Dispose) | 4 | `Client_Property_*`, `Endpoint_*`, `Dispose_*` |
| CRUD Conflict/Delete | 6 | `CreateDatabaseAsync_Duplicate*`, `CreateContainerAsync_Duplicate*`, `DeleteAsync_*` |
| ThroughputProperties overloads | 6 | `Create*_WithThroughputProperties_*` |
| Stream APIs | 8 | `Create*StreamAsync_*`, `ReadStreamAsync_*`, `DeleteStreamAsync_*` |
| Query Iterators | 8 | `Get*QueryIterator_*` |
| ReadAsync | 1 | `ReadAsync_ReturnsCorrectDatabaseProperties` |
| Properties (ClientOptions, ResponseFactory, Account) | 3 | `ClientOptions_*`, `ResponseFactory_*`, `ReadAccountAsync_*` |
| Stream Iterators | 2 | `Get*QueryStreamIterator_*` |
| Throughput | 4 | `ReadThroughputAsync_*`, `ReplaceThroughputAsync_*` |
| DefineContainer | 1 | `DefineContainer_Creates*` |
| Users | 10 | `CreateUserAsync_*`, `UpsertUserAsync_*`, `GetUser_*`, `GetUserQueryIterator_*`, `User_*` |
| Permissions | 10 | `Permission_*`, `DivergentBehavior_PermissionTokens_*` |
| Encryption Keys | 3 | `GetClientEncryptionKey_*` (skipped), `DivergentBehavior_ClientEncryptionKeys_*` |
| CreateAndInitializeAsync | 3 | `CreateAndInitializeAsync_*` |
| Divergent Behaviors | 3 | `DivergentBehavior_GetDatabase_*`, `DivergentBehavior_PermissionTokens_*`, `DivergentBehavior_ClientEncryptionKeys_*` |
| Standalone classes | ~20 | Concurrency, input validation, edge cases, response properties, etc. |

### Tests in InMemoryCosmosClientTests.cs (overlapping — NOT touching):

13 tests covering basic create/get/query/dispose. Significant overlap with `CosmosClientAndDatabaseTests.cs`
but maintained as a separate simpler file.

---

## Appendix B: Files To Modify

| File | Changes |
|------|---------|
| `src/CosmosDB.InMemoryEmulator/InMemoryDatabase.cs` | Add `_users.Clear()` to `DeleteAsync` and `DeleteStreamAsync` |
| `tests/CosmosDB.InMemoryEmulator.Tests/CosmosClientAndDatabaseTests.cs` | Add ~26 new tests |
| `src/CosmosDB.InMemoryEmulator/CosmosDB.InMemoryEmulator.csproj` | Version → 2.0.5 |
| `c:\git\CosmosDB.InMemoryEmulator.wiki\Known-Limitations.md` | Add behavioural differences #16-#18 |
| `c:\git\CosmosDB.InMemoryEmulator.wiki\Features.md` | Update Users & Permissions section |

---

## Appendix C: Checklist for Each New Test

For each test, follow this discipline:

1. ⬜ Write test (should fail or be skipped)
2. ⬜ Implement fix if needed (minimum code to pass)
3. ⬜ Verify test passes (green)
4. ⬜ Run full suite to ensure no regressions
5. ⬜ Update plan status
