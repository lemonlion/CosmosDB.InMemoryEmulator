# Plan: FakeCosmosHandlerCrudTests — Deep Dive Coverage & Bug Fixes

**Status:** PLANNED — Not yet implemented  
**Current version:** 2.0.4 → will become **2.0.5** after implementation  
**Approach:** TDD — write failing test first, then implement fix/feature, then refactor  

---

## Scope

`FakeCosmosHandlerCrudTests.cs` exercises the real Cosmos SDK HTTP pipeline end-to-end:
**SDK → HTTP request → FakeCosmosHandler → InMemoryContainer → HTTP response → SDK**

This plan covers CRUD operations (Create, Read, Upsert, Replace, Delete, Patch) plus edge
cases specific to the handler layer (URL encoding, ETags, response headers, special characters,
partition key types, IfNoneMatch conditional reads, etc.).

Non-CRUD handler features (queries, ORDER BY, aggregates, pagination, cache, pkranges, routing)
are out of scope — those belong in `FakeCosmosHandlerTests.cs`.

---

## Analysis Summary

### Existing Tests (26 tests across 9 sections)

| Section | Tests | Coverage |
|---------|-------|----------|
| 1A. Create | 3 | Basic, duplicate conflict, create-then-query |
| 1B. Read | 3 | Basic, 404, partition key isolation |
| 1C. Upsert | 3 | New, existing, upsert-then-query |
| 1D. Replace | 3 | Basic, 404, stale ETag |
| 1E. Delete | 3 | Basic, 404, delete-then-read |
| 1F. Patch | 4 | Set, multi-op, 404, filter predicate match |
| 1G. Integration | 3 | LINQ round-trip, multi-container, request log |
| 1H. SDK Compat | 1 | Verify doesn't throw |
| 1I. Edge Cases | 4 | Fault injection 429, patch non-match filter, URL-encoded ID, composite PK |

### Identified Gaps

Gaps are categorised as:
- **Missing test** — the handler supports this but no test verifies it via the SDK pipeline
- **Bug** — incorrect behaviour found during code review
- **Edge case** — uncommon but valid scenario with no coverage

---

## New Tests to Write

### 2A. Create — Additional Coverage

| # | Test Name | Type | Description |
|---|-----------|------|-------------|
| 1 | `Handler_CreateItem_ReturnsETagInResponse` | Missing test | Create an item and verify `response.ETag` is non-null and non-empty. The handler's `ConvertToHttpResponse` copies the ETag header — this should be verified at the SDK level. |
| 2 | `Handler_CreateItem_ReturnsRequestCharge` | Missing test | Create an item and verify `response.RequestCharge` is > 0. The handler hardcodes `x-ms-request-charge: 1`. |
| 3 | `Handler_CreateItem_ReturnsActivityId` | Missing test | Create an item and verify `response.ActivityId` is non-null. |
| 4 | `Handler_CreateItem_WithSpecialCharactersInId_Succeeds` | Edge case | Create with ID containing `+`, `#`, `%`, `/`, `?` and verify round-trip read. Tests URL encoding in both create (POST body) and read (GET URL path) directions. |
| 5 | `Handler_CreateItem_WithUnicodeId_Succeeds` | Edge case | Create with ID containing unicode chars (e.g. `"café"`, `"日本語"`) and verify round-trip. |
| 6 | `Handler_CreateItem_WithEmptyStringPartitionKey_Succeeds` | Edge case | Create an item where the partition key is `""` (empty string) — this is valid in Cosmos DB. |
| 7 | `Handler_CreateItem_WithLargeDocument_Succeeds` | Edge case | Create a document with a large body (e.g. 100KB of data) to verify the handler doesn't truncate or corrupt. |

### 2B. Read — Additional Coverage

| # | Test Name | Type | Description |
|---|-----------|------|-------------|
| 8 | `Handler_ReadItem_ReturnsETag` | Missing test | Read an item and verify the response contains an ETag header. |
| 9 | `Handler_ReadItem_WithIfNoneMatch_CurrentETag_ThrowsNotModified` | Missing test | The handler's `BuildItemRequestOptions` wires `IfNoneMatch` — but no test verifies this. Read with the current ETag in `IfNoneMatchEtag`; expect 304. |
| 10 | `Handler_ReadItem_WithIfNoneMatch_StaleETag_ReturnsOk` | Missing test | Read with a stale ETag in `IfNoneMatchEtag`; expect 200 with the document. |
| 11 | `Handler_ReadItem_WithSpecialCharactersInId_Succeeds` | Edge case | Complements test #4 — verifies the read path specifically with `+`, `#`, `%`, `/` chars. The handler uses `Uri.UnescapeDataString` on the path. |
| 12 | `Handler_ReadItem_ReturnsSystemProperties` | Missing test | Read an item and verify `_ts`, `_etag`, `_rid` are present in the returned document JSON. |

### 2C. Upsert — Additional Coverage

| # | Test Name | Type | Description |
|---|-----------|------|-------------|
| 13 | `Handler_UpsertItem_WithIfMatchCurrentETag_Succeeds` | Missing test | Upsert with a valid ETag in `IfMatchEtag` option — the handler wires this through `BuildItemRequestOptions` but no test verifies it. |
| 14 | `Handler_UpsertItem_WithIfMatchStaleETag_ThrowsPreconditionFailed` | Missing test | Upsert with a stale ETag; expect 412. |
| 15 | `Handler_UpsertItem_ReturnsUpdatedETag` | Missing test | Verify the ETag changes after upserting an existing item. |

### 2D. Replace — Additional Coverage

| # | Test Name | Type | Description |
|---|-----------|------|-------------|
| 16 | `Handler_ReplaceItem_WithIfMatchCurrentETag_Succeeds` | Missing test | Replace with the current (valid) ETag; expect 200. Currently only stale ETag is tested. |
| 17 | `Handler_ReplaceItem_ReturnsUpdatedETag` | Missing test | Verify the ETag changes after replacing. |
| 18 | `Handler_ReplaceItem_PreservesPartitionKeyIsolation` | Edge case | Replace item in pkA, verify item in pkB with same ID is unaffected. |

### 2E. Delete — Additional Coverage

| # | Test Name | Type | Description |
|---|-----------|------|-------------|
| 19 | `Handler_DeleteItem_WithIfMatchCurrentETag_Succeeds` | Missing test | Delete with the current (valid) ETag; expect 204. |
| 20 | `Handler_DeleteItem_WithIfMatchStaleETag_ThrowsPreconditionFailed` | Missing test | Delete with stale ETag; expect 412. The handler wires IfMatch via `BuildItemRequestOptions`. |
| 21 | `Handler_DeleteItem_ThenRecreate_Succeeds` | Edge case | Delete an item, then re-create it. Verify the recreated item has a different ETag. |
| 22 | `Handler_DeleteItem_OnlyAffectsCorrectPartition` | Edge case | Create same ID in two partitions, delete from one, verify other still exists. |

### 2F. Patch — Additional Coverage

| # | Test Name | Type | Description |
|---|-----------|------|-------------|
| 23 | `Handler_PatchItem_AddOperation_AddsNewProperty` | Missing test | The handler's `ParsePatchBody` handles "add" — verify it works through the SDK pipeline. |
| 24 | `Handler_PatchItem_RemoveOperation_RemovesProperty` | Missing test | The handler's `ParsePatchBody` handles "remove" — verify it. |
| 25 | `Handler_PatchItem_WithIfMatchCurrentETag_Succeeds` | Missing test | Patch with valid ETag; expect 200. The handler wires IfMatch in `HandlePatchAsync`. |
| 26 | `Handler_PatchItem_WithIfMatchStaleETag_ThrowsPreconditionFailed` | Missing test | Patch with stale ETag; expect 412. |
| 27 | `Handler_PatchItem_NestedPropertySet_ReturnsUpdatedDocument` | Missing test | Patch a nested property (e.g. `/nested/description`) and verify the response. |

### 2G. Response Metadata (via Handler Pipeline)

| # | Test Name | Type | Description |
|---|-----------|------|-------------|
| 28 | `Handler_CrudResponses_ContainSessionToken` | Missing test | All CRUD responses should contain `x-ms-session-token` header. Verify for create, read, upsert, replace, delete. |
| 29 | `Handler_CreateResponse_ContainsResourceBody` | Missing test | Verify the create response body contains the full document (id, partitionKey, name, etc.). |

### 2H. Fault Injection — CRUD-specific

| # | Test Name | Type | Description |
|---|-----------|------|-------------|
| 30 | `Handler_FaultInjection_503OnRead_ThrowsServiceUnavailable` | Missing test | Fault inject 503 on a read operation. |
| 31 | `Handler_FaultInjection_SelectiveByMethod_OnlyFailsDeletes` | Missing test | Inject fault only for DELETE requests, verify creates/reads still work. |

### 2I. Edge Cases — Additional

| # | Test Name | Type | Description |
|---|-----------|------|-------------|
| 32 | `Handler_UnrecognisedRoute_Returns404` | Missing test | Send a PUT to a non-docs path — the handler's catch-all returns 404 but this is never tested at the SDK level. Will need raw HTTP for this. |
| 33 | `Handler_ReadFeed_ViaGetItemQueryIterator_ReturnsAllDocuments` | Missing test | Verify `GetItemQueryIterator<T>()` (no SQL) works through the handler. This hits `HandleReadFeedAsync`. Note: this overlaps with FakeCosmosHandlerTests — if already adequately covered there, mark as duplicate. |
| 34 | `Handler_Dispose_DoesNotThrowOnSubsequentDispose` | Edge case | Verify double-dispose doesn't throw. |
| 35 | `Handler_ConcurrentCrudOperations_AreThreadSafe` | Edge case | Run create/read/upsert/delete in parallel tasks to verify thread safety of the handler. |

---

## Potential Bugs Found During Code Review

### Bug 1: ExtractPartitionKey doesn't handle non-string PK types

**Location:** `FakeCosmosHandler.ExtractPartitionKey()` lines ~1070-1105  
**Issue:** The handler parses the `x-ms-documentdb-partitionkey` header and for single-element
arrays converts everything via `.Value<string>()` or `.ToString()`. For numeric partition keys,
the real SDK sends `[42]` (JSON number) in the header but `ExtractPartitionKey` always creates
`new PartitionKey(string)`, not `new PartitionKey(double)`. This means numeric PK values get
stringified incorrectly and won't match the InMemoryContainer's PK matching.

**Action:** Write a test proving this fails, then fix `ExtractPartitionKey` to handle
`JTokenType.Integer`, `JTokenType.Float`, `JTokenType.Boolean`, and `JTokenType.Null`
appropriately. If the fix is too complex, mark the test as skipped with a detailed reason
and add a divergent behaviour test.

### Bug 2: Composite PK extraction doesn't handle non-string components

**Location:** Same method, the `arr.Count > 1` branch.  
**Issue:** Same as Bug 1 but for composite partition keys — each component is always added as
a string via `builder.Add(string)`. Numeric or boolean components would be mishandled.

**Action:** Write a test, fix or skip+document.

### Bug 3: Patch "replace" operation mapped to PatchOperation.Set

**Location:** `FakeCosmosHandler.ParsePatchBody()` line ~403  
**Issue:** `"replace"` is mapped to `PatchOperation.Set()` — but in real Cosmos DB, "replace"
requires the path to already exist, while "set" creates it if missing. The InMemoryContainer
may or may not enforce this distinction, but the mapping is semantically incorrect.

**Action:** Investigate whether `PatchOperation.Replace()` exists in the SDK. If it does,
verify the InMemoryContainer handles it correctly. Write a test to expose the difference.
If fixing is non-trivial, skip + document divergent behaviour.

---

## Implementation Order (TDD Red-Green-Refactor)

### Phase 1: Quick Wins — Missing Response Metadata Tests (tests 1-3, 8, 12, 15, 17, 28-29)
These test existing correct behaviour that just lacks assertions. They should all pass green
immediately (no handler changes needed).

### Phase 2: ETag Conditional Operations (tests 9-10, 13-14, 16, 19-20, 25-26)
Test IfMatch/IfNoneMatch through the handler pipeline. The handler's `BuildItemRequestOptions`
already wires these headers — tests should pass green. If any fail, it indicates a handler bug.

### Phase 3: Edge Cases — Special Characters & IDs (tests 4-6, 11)
Tests for URL encoding, special characters, unicode. May expose bugs in `ExtractDocumentId`
or `HasDocumentId`.

### Phase 4: Bug Fixes (Bugs 1-3)
Write failing tests first. Implement fixes in `FakeCosmosHandler`. Verify green.
For any fix that's too complex: skip test with detailed reason + add divergent behaviour test.

### Phase 5: Additional Patch Operations (tests 23-24, 27)
Verify add/remove/nested operations through the handler pipeline.

### Phase 6: Delete & Replace Edge Cases (tests 18, 21-22)
Verify partition isolation and re-creation after delete.

### Phase 7: Fault Injection & Concurrency (tests 30-31, 34-35)
Additional fault injection patterns and thread safety.

### Phase 8: Route Edge Cases (tests 32-33)
Unrecognised route handling, read feed via handler.

---

## Documentation Updates Required

After all tests pass:

### 1. Wiki: Known-Limitations.md
- If Bug 1/2 (numeric partition keys in handler) is fixed: no change needed
- If Bug 1/2 is skipped: add limitation entry:
  `| FakeCosmosHandler partition keys | ⚠️ String only | Non-string partition keys (numeric, boolean) are stringified in the handler's HTTP extraction layer. Use string PKs when testing via FakeCosmosHandler. |`
- If Bug 3 (replace vs set) diverges: add behavioural difference entry

### 2. Wiki: Features.md
- No changes expected unless new features are added

### 3. Wiki: Feature-Comparison-With-Alternatives.md (Comparison)
- Update if any new feature rows are needed (unlikely for CRUD tests)

### 4. README.md
- No changes needed for test-only additions

### 5. CHANGELOG (commit message)
- List all new tests and any bug fixes in the commit message
- Tag as v2.0.5

---

## Version & Release Plan

1. Implement all tests and fixes
2. Run full test suite: `dotnet test tests/CosmosDB.InMemoryEmulator.Tests --verbosity minimal`
3. Update wiki documentation (if limitations discovered)
4. `git add -A`
5. `git commit -m "v2.0.5: FakeCosmosHandler CRUD test coverage - [N] new tests, [M] bug fixes"`
6. Update `<Version>` in `.csproj` to `2.0.5`
7. `git tag v2.0.5`
8. `git push; git push --tags`
9. Push wiki changes separately

---

## Tests That May Need Skip + Divergent Behaviour

These tests might be too difficult to implement cleanly through the handler pipeline:

| Test | Reason for potential skip |
|------|--------------------------|
| Bug 1 test (numeric PK) | SDK may not send numeric PKs via Gateway in the format we expect — needs investigation |
| Bug 3 test (replace vs set) | The SDK's PatchOperation.Replace may not exist as a separate type, making the distinction impossible to test |
| Test 32 (unrecognised route) | This requires raw HTTP, not SDK calls — may not belong in CRUD tests |
| Test 35 (concurrent ops) | May be flaky in CI; consider if it adds sufficient value |

---

## Progress Tracking

| Phase | Status | Tests Written | Tests Passing | Bugs Fixed |
|-------|--------|--------------|--------------|------------|
| Phase 1: Response metadata | ⬜ Not started | 0/9 | 0/9 | — |
| Phase 2: ETag conditionals | ⬜ Not started | 0/9 | 0/9 | — |
| Phase 3: Special chars | ⬜ Not started | 0/4 | 0/4 | — |
| Phase 4: Bug fixes | ⬜ Not started | 0/3 | 0/3 | 0/3 |
| Phase 5: Patch operations | ⬜ Not started | 0/3 | 0/3 | — |
| Phase 6: Delete/Replace edge | ⬜ Not started | 0/3 | 0/3 | — |
| Phase 7: Fault/Concurrency | ⬜ Not started | 0/4 | 0/4 | — |
| Phase 8: Route edge cases | ⬜ Not started | 0/2 | 0/2 | — |
| Documentation | ⬜ Not started | — | — | — |
| Version bump & tag | ⬜ Not started | — | — | — |
| **TOTAL** | | **0/37** | **0/37** | **0/3** |
