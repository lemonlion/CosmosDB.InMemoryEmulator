# FakeCosmosHandler Test Coverage Deep-Dive Plan

**Date:** 2026-04-01  
**Target Version:** 2.0.5  
**Approach:** TDD — Red-Green-Refactor  
**Files:**
- Tests: `tests/CosmosDB.InMemoryEmulator.Tests/FakeCosmosHandlerTests.cs`
- Tests: `tests/CosmosDB.InMemoryEmulator.Tests/FakeCosmosHandlerCrudTests.cs`
- Source: `src/CosmosDB.InMemoryEmulator/FakeCosmosHandler.cs`

---

## Current Coverage Inventory

### FakeCosmosHandlerTests.cs (3 test classes)

| # | Test | Class | Status |
|---|------|-------|--------|
| 1 | Handler_ReadFeed_ReturnsAllDocuments | GapTests | ✅ |
| 2 | Handler_AccountMetadata_ReturnsValidResponse | GapTests4 | ✅ |
| 3 | Handler_Query_PartitionKeyRange_FiltersToRange | GapTests4 | ✅ |
| 4 | Handler_CacheEviction_StaleEntries | GapTests4 | ✅ |
| 5 | Handler_CacheEviction_ExceedsMaxEntries | GapTests4 | ✅ |
| 6 | Handler_CollectionMetadata_ReturnsContainerProperties | GapTests4 | ✅ |
| 7 | Handler_MurmurHash_DistributesEvenly | GapTests4 | ✅ |
| 8 | Handler_BasicQuery_ReturnsAllItems | Tests | ✅ |
| 9 | Handler_OrderByQuery_ReturnsCorrectOrder | Tests | ✅ |
| 10 | Handler_Pagination_ContinuationTokenRoundtrip | Tests | ✅ |
| 11 | Handler_PartitionKeyRanges_ReturnsConfiguredCount | Tests | ✅ |
| 12 | Handler_PartitionKeyRanges_IfNoneMatch_Returns304 | Tests | ✅ |
| 13 | Handler_QueryLog_RecordsQueries | Tests | ✅ |
| 14 | Handler_RequestLog_RecordsRequests | Tests | ✅ |
| 15 | Handler_FilteredQuery_ReturnsCorrectResults | Tests | ✅ |
| 16 | Handler_VerifySdkCompatibility_DoesNotThrow | Tests | ✅ |
| 17 | Handler_MultiContainer_RouterDispatchesCorrectly | Tests | ✅ |
| 18 | Handler_CrossPartition_WithMultipleRanges_AllDataReturned | Tests | ✅ |
| 19 | Handler_Router_UnregisteredContainer_ThrowsDescriptiveError | Tests | ✅ |
| 20 | Handler_CountAsync_ReturnsCorrectCount | Tests | ✅ |

### FakeCosmosHandlerCrudTests.cs (1 test class, 24 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Handler_CreateItem_ReturnsCreated | ✅ |
| 2 | Handler_CreateItem_DuplicateId_ReturnsConflict | ✅ |
| 3 | Handler_CreateItem_ThenQuery_ReturnsItem | ✅ |
| 4 | Handler_ReadItem_ReturnsDocument | ✅ |
| 5 | Handler_ReadItem_NotFound_Throws404 | ✅ |
| 6 | Handler_ReadItem_WithPartitionKey_ReturnsCorrectItem | ✅ |
| 7 | Handler_UpsertItem_NewItem_ReturnsCreated | ✅ |
| 8 | Handler_UpsertItem_ExistingItem_ReturnsOk | ✅ |
| 9 | Handler_UpsertItem_ThenQuery_ReturnsUpdatedItem | ✅ |
| 10 | Handler_ReplaceItem_ReturnsOk | ✅ |
| 11 | Handler_ReplaceItem_NotFound_Throws404 | ✅ |
| 12 | Handler_ReplaceItem_WithETag_StaleETag_ThrowsPreconditionFailed | ✅ |
| 13 | Handler_DeleteItem_ReturnsNoContent | ✅ |
| 14 | Handler_DeleteItem_NotFound_Throws404 | ✅ |
| 15 | Handler_DeleteItem_ThenRead_Throws404 | ✅ |
| 16 | Handler_PatchItem_SetOperation_ReturnsOk | ✅ |
| 17 | Handler_PatchItem_MultipleOperations_AllApplied | ✅ |
| 18 | Handler_PatchItem_NotFound_Throws404 | ✅ |
| 19 | Handler_PatchItem_WithFilterPredicate_MatchingCondition_Succeeds | ✅ |
| 20 | Handler_CrudThenLinqQuery_RoundTrip | ✅ |
| 21 | Handler_MultiContainer_CrudIsolated | ✅ |
| 22 | Handler_RequestLog_RecordsCrudOperations | ✅ |
| 23 | Handler_VerifySdkCompatibility_IncludesCrudCheck | ✅ |
| 24 | Handler_FaultInjection_ThrottlesCreateRequest | ✅ |
| 25 | Handler_PatchItem_WithFilterPredicate_NonMatchingCondition_ThrowsPreconditionFailed | ✅ |
| 26 | Handler_ReadItem_WithUrlEncodedId_Succeeds | ✅ |
| 27 | Handler_Crud_WithCompositePartitionKey_RoundTrip | ✅ |

---

## Identified Gaps — New Tests to Write

### A. CRUD Edge Cases (FakeCosmosHandlerCrudTests.cs)

| ID | Test Name | Rationale | Difficulty |
|----|-----------|-----------|------------|
| A1 | `Handler_CreateItem_WithSpecialCharactersInId_Succeeds` | IDs with slashes (`a/b`), dots (`a.b`), unicode (`日本語`), plus (`a+b`). Real Cosmos accepts these. Tests URL encoding/decoding in `ExtractDocumentId`. | Low |
| A2 | `Handler_ReplaceItem_WithMatchingETag_Succeeds` | Only stale ETag tested. Need to verify the happy path: read ETag from create response, then replace with that ETag. | Low |
| A3 | `Handler_DeleteItem_WithETag_StaleETag_ThrowsPreconditionFailed` | Delete with if-match ETag is supported in `BuildItemRequestOptions` but never tested. | Low |
| A4 | `Handler_DeleteItem_WithMatchingETag_Succeeds` | Happy-path conditional delete. | Low |
| A5 | `Handler_PatchItem_RemoveOperation_Succeeds` | Only Set and Increment patch ops tested. Remove is a valid operation. | Low |
| A6 | `Handler_PatchItem_AddOperation_Succeeds` | Add is different from Set (adds new property vs replaces). | Low |
| A7 | `Handler_UpsertItem_ReturnsETagInResponse` | Verify that upsert response contains an ETag header that flows through the HTTP layer. | Low |
| A8 | `Handler_CreateItem_ReturnsETagInResponse` | Verify that create response contains an ETag header. | Low |
| A9 | `Handler_ReplaceItem_ReturnsUpdatedETag` | After replace, the returned ETag should differ from the original. | Low |
| A10 | `Handler_ReadItem_ReturnsETagInResponse` | Read response should include ETag. | Low |

### B. Partition Key Edge Cases (FakeCosmosHandlerCrudTests.cs)

| ID | Test Name | Rationale | Difficulty |
|----|-----------|-----------|------------|
| B1 | `Handler_CreateItem_WithNumericPartitionKey_Succeeds` | `ExtractPartitionKey` parses `["123"]` — verify numeric PK values work through SDK round-trip. The SDK sends numeric PKs as `[123]` (no quotes). Current code may coerce to string. | Medium |
| B2 | `Handler_CreateItem_WithBooleanPartitionKey_Succeeds` | SDK sends boolean PKs as `[true]` — verify this flows correctly. | Medium |
| B3 | `Handler_ReadItem_WithPartitionKeyNone_Succeeds` | `PartitionKey.None` is used when no PK header is sent. Verify CRUD works. | Low |

### C. Query Features via Handler (FakeCosmosHandlerTests.cs)

| ID | Test Name | Rationale | Difficulty |
|----|-----------|-----------|------------|
| C1 | `Handler_ParameterizedQuery_ReturnsCorrectResults` | SDK sends parameterized queries via `QueryDefinition.WithParameter()`. `BuildQueryDefinition` handles parameters but it's never tested end-to-end through the handler. | Low |
| C2 | `Handler_TopQuery_ReturnsLimitedResults` | TOP queries go through query plan → rewritten query. Not tested. | Medium |
| C3 | `Handler_OffsetLimitQuery_ReturnsPaginatedSlice` | OFFSET/LIMIT via query plan. Not tested. | Medium |
| C4 | `Handler_DistinctQuery_ReturnsUniqueValues` | DISTINCT goes through query plan with distinctType. Not tested. | Medium |
| C5 | `Handler_OrderByDescending_ReturnsReverseOrder` | Only ascending ORDER BY tested. Descending uses different query plan metadata. | Low |
| C6 | `Handler_OrderBy_MultipleFields_ReturnsCorrectOrder` | Multi-field ORDER BY generates multiple orderByItems. Not tested. | Medium |
| C7 | `Handler_SumAggregate_ReturnsCorrectSum` | Only COUNT aggregate tested. SUM uses different pipeline path. | Medium |
| C8 | `Handler_MinMaxAggregate_ReturnsCorrectValues` | MIN/MAX aggregates not tested. | Medium |
| C9 | `Handler_AvgAggregate_ReturnsCorrectAverage` | AVG has special `{sum, count}` wrapping logic in the handler. Not tested. | Hard |
| C10 | `Handler_EmptyContainer_QueryReturnsEmpty` | Zero-document edge case — empty Documents array, _count=0. | Low |
| C11 | `Handler_GroupByQuery_ReturnsGroupedResults` | GROUP BY sends groupByExpressions in query plan. Not tested. | Hard |
| C12 | `Handler_QueryWithPartitionKeyFilter_ReturnsFilteredResults` | Cross-partition vs single-partition query via PK header. | Low |

### D. Response Headers & Metadata (FakeCosmosHandlerTests.cs)

| ID | Test Name | Rationale | Difficulty |
|----|-----------|-----------|------------|
| D1 | `Handler_CrudResponse_ContainsRequestCharge` | All CRUD responses set `x-ms-request-charge` but it's never validated. | Low |
| D2 | `Handler_CrudResponse_ContainsActivityId` | `x-ms-activity-id` is set but never validated. | Low |
| D3 | `Handler_CrudResponse_ContainsSessionToken` | `x-ms-session-token` is set but never validated. | Low |
| D4 | `Handler_QueryResponse_ContainsItemCount` | `x-ms-item-count` header on query responses. | Low |
| D5 | `Handler_CollectionMetadata_ContainsIndexingPolicy` | Collection GET response includes indexingPolicy but only `id` and `partitionKey` are validated. | Low |
| D6 | `Handler_CollectionMetadata_WithCompositePartitionKey_ReturnsMultiplePaths` | Composite PK container should return multiple partition key paths. | Low |
| D7 | `Handler_AccountMetadata_ContainsQueryEngineConfiguration` | Account metadata includes queryEngineConfiguration but not validated. | Low |

### E. Router Edge Cases (FakeCosmosHandlerTests.cs)

| ID | Test Name | Rationale | Difficulty |
|----|-----------|-----------|------------|
| E1 | `Handler_Router_SingleContainer_WorksCorrectly` | Router with dictionary of 1 container. Validates default handler fallback. | Low |
| E2 | `Handler_Router_CrudThroughRouter_WorksEndToEnd` | Full CRUD cycle (create/read/update/delete) through the router, not just queries. | Low |
| E3 | `Handler_Router_FaultInjection_PerContainer` | Set FaultInjector on one handler in the router — only that container should be affected. | Medium |

### F. Cache Edge Cases (FakeCosmosHandlerTests.cs)

| ID | Test Name | Rationale | Difficulty |
|----|-----------|-----------|------------|
| F1 | `Handler_AbandonedIteration_CacheIsEventuallyEvicted` | Start paginated query, read only first page, let cache TTL expire. Verify no memory leak. | Low |
| F2 | `Handler_ConcurrentQueries_DoNotInterfere` | Two different queries running concurrently should not cross-contaminate cache entries. | Medium |

### G. Disposal & Lifecycle (FakeCosmosHandlerTests.cs)

| ID | Test Name | Rationale | Difficulty |
|----|-----------|-----------|------------|
| G1 | `Handler_Dispose_ClearsQueryCache` | Dispose calls `_queryResultCache.Clear()` — verify the cache is actually emptied. | Low |
| G2 | `Handler_DoubleDispose_DoesNotThrow` | Idempotent dispose is important for `using` patterns with router. | Low |

### H. Unrecognised Routes (FakeCosmosHandlerTests.cs)

| ID | Test Name | Rationale | Difficulty |
|----|-----------|-----------|------------|
| H1 | `Handler_UnrecognisedRoute_Returns404WithMessage` | The catch-all returns 404 with a descriptive message. Not tested. | Low |

---

## Identified Bugs / Issues

### BUG-1: `ExtractPartitionKey` — Numeric/Boolean PK values coerced to string

**Location:** `FakeCosmosHandler.cs` line ~1070  
**Issue:** The SDK sends numeric PK values as `[123]` (JTokenType.Integer) and boolean as `[true]`. The current code:
```csharp
if (arr.Count == 1)
{
    return arr[0].Type == JTokenType.String
        ? new PartitionKey(arr[0].Value<string>())
        : new PartitionKey(arr[0].ToString());
}
```
For numeric values, `arr[0].ToString()` returns `"123"` which creates `new PartitionKey("123")` (string), not `new PartitionKey(123.0)` (double). Real Cosmos has typed partition keys.

**Impact:** Medium — affects anyone using numeric or boolean partition keys via FakeCosmosHandler. The InMemoryContainer level handles this correctly; the bug is only in the HTTP header parsing.

**Fix:** Add type-switching:
```csharp
return arr[0].Type switch
{
    JTokenType.String => new PartitionKey(arr[0].Value<string>()),
    JTokenType.Integer or JTokenType.Float => new PartitionKey(arr[0].Value<double>()),
    JTokenType.Boolean => new PartitionKey(arr[0].Value<bool>()),
    JTokenType.Null => PartitionKey.Null,
    _ => new PartitionKey(arr[0].ToString())
};
```

Same fix needed for the composite PK builder branch.

**Tests:** B1, B2

### BUG-2: `ExtractPartitionKey` — Composite PK with non-string types

**Location:** `FakeCosmosHandler.cs` line ~1087  
**Issue:** In the composite PK branch:
```csharp
builder.Add(token.Type == JTokenType.String
    ? token.Value<string>()
    : token.ToString());
```
All non-string values are coerced to string via `ToString()`. `PartitionKeyBuilder.Add()` has typed overloads for double and bool. This causes the composite PK to contain `"123"` instead of `123.0`.

**Impact:** Low — composite PKs with mixed types are uncommon but valid.

**Fix:** Add the same type-switching as BUG-1.

**Tests:** B1 (extend to composite), B2 (extend to composite)

### BUG-3: `ParsePatchBody` — Missing "replace" operation type mapping

**Location:** `FakeCosmosHandler.cs` line ~430  
**Status:** NOT A BUG — `"replace"` correctly maps to `PatchOperation.Set()` because the SDK serializes both Replace and Set as the same wire format. The Cosmos REST API uses `"set"` for both. Verified.

### BUG-4: (Potential) `RoutingHandler` Dispose double-disposes handlers

**Location:** `FakeCosmosHandler.cs` line ~1440  
**Issue:** `RoutingHandler.Dispose(true)` iterates _handlers.Values and calls `handler.Dispose()` on each. But if the caller also wraps each handler in a `using` statement (as the tests do), the handlers get disposed twice. The current `FakeCosmosHandler.Dispose` only calls `_queryResultCache.Clear()` which is idempotent, so this is currently safe. But if Dispose logic changes, this could become a problem.

**Impact:** None currently (idempotent dispose). But G2 test will document this is safe.

**Status:** Not a bug — current behavior is correct. Add G2 test to prevent regression.

---

## Execution Plan

### Phase 1: Low-Complexity Tests (Quick Wins)

**Write all tests first (RED), then implement fixes (GREEN).**

| Order | IDs | Description | Est. Tests |
|-------|-----|-------------|------------|
| 1.1 | A1 | Special character IDs | 1 |
| 1.2 | A2 | Replace with matching ETag | 1 |
| 1.3 | A3, A4 | Delete with ETag (stale + matching) | 2 |
| 1.4 | A5, A6 | Patch remove + add operations | 2 |
| 1.5 | A7, A8, A9, A10 | ETag in CRUD responses | 4 |
| 1.6 | B3 | PartitionKey.None round-trip | 1 |
| 1.7 | C1 | Parameterized query | 1 |
| 1.8 | C5 | ORDER BY descending | 1 |
| 1.9 | C10 | Empty container query | 1 |
| 1.10 | C12 | Query with PK filter | 1 |
| 1.11 | D1, D2, D3, D4, D5, D6, D7 | Response header/metadata validation | 7 |
| 1.12 | E1, E2 | Router single container + CRUD | 2 |
| 1.13 | F1 | Abandoned iteration cache eviction | 1 |
| 1.14 | G1, G2 | Dispose + double-dispose | 2 |
| 1.15 | H1 | Unrecognised route 404 | 1 |

**Subtotal Phase 1: 28 tests**

### Phase 2: Medium-Complexity Tests

| Order | IDs | Description | Est. Tests |
|-------|-----|-------------|------------|
| 2.1 | B1, B2 | Numeric + boolean PK (requires BUG-1 fix) | 2 |
| 2.2 | C2 | TOP query via handler | 1 |
| 2.3 | C3 | OFFSET/LIMIT query via handler | 1 |
| 2.4 | C4 | DISTINCT query via handler | 1 |
| 2.5 | C6 | Multi-field ORDER BY | 1 |
| 2.6 | C7 | SUM aggregate | 1 |
| 2.7 | C8 | MIN/MAX aggregate | 1 |
| 2.8 | E3 | Per-container fault injection via router | 1 |
| 2.9 | F2 | Concurrent queries don't interfere | 1 |

**Subtotal Phase 2: 10 tests**

### Phase 3: Hard Tests (May Skip)

| Order | IDs | Description | Est. Tests | Notes |
|-------|-----|-------------|------------|-------|
| 3.1 | C9 | AVG aggregate | 1 | Has special `{sum, count}` wrapping. May need Skip if SDK pipeline is complex. |
| 3.2 | C11 | GROUP BY query | 1 | Depends on SDK GROUP BY pipeline which may be complex to satisfy via handler. |

**Subtotal Phase 3: 2 tests (may become skipped with divergent sister tests)**

### Phase 4: Bug Fixes

| Order | Bug | Description | Impact |
|-------|-----|-------------|--------|
| 4.1 | BUG-1 | Fix `ExtractPartitionKey` for numeric/boolean single values | Medium |
| 4.2 | BUG-2 | Fix `ExtractPartitionKey` for numeric/boolean composite values | Low |

Both bugs are addressed by the same code change. Tests B1 and B2 from Phase 2 will turn GREEN after these fixes.

---

## Skip Strategy

For any test where the SDK's internal pipeline makes it infeasible to produce correct results through FakeCosmosHandler:

1. **Mark the test as Skipped** with a `[Fact(Skip = "...")]` attribute containing:
   - The gap ID (e.g., `C9`, `C11`)
   - Clear description of why it's skipped
   - What real Cosmos behavior would be
   - What the emulator does instead
   - Why the fix is non-trivial

2. **Create a sister test** that demonstrates the actual (divergent) behavior, named with a `_Divergent` suffix. The sister test will:
   - Have inline comments explaining the expected vs actual behavior
   - Assert what the emulator actually returns (so it doesn't regress)
   - Cross-reference the skipped test

---

## Documentation Updates (Post-Implementation)

After all tests pass:

### Wiki Updates

| File | Change |
|------|--------|
| `Known-Limitations.md` | Add entry for any new Skip'd tests (numeric/boolean PK if not fixed, AVG/GROUP BY if skipped). Remove entries for anything now fixed. |
| `Features.md` | Add entry for FakeCosmosHandler response header fidelity if new assertions prove headers are correct. |
| `Feature-Comparison-With-Alternatives.md` | Update "Fault injection" row if E3 (per-container fault injection) passes. Update any corrected PK handling. |
| `API-Reference.md` | Update `ExtractPartitionKey` documentation if BUG-1/BUG-2 fixed (now handles numeric, boolean, null PK types). |

### Source Updates

| File | Change |
|------|--------|
| `README.md` | Update test count (currently "1350+") to reflect new total. |
| `CosmosDB.InMemoryEmulator.csproj` | Bump `<Version>` from `2.0.4` to `2.0.5`. |

### Git

```powershell
git add -A
git commit -m "v2.0.5: FakeCosmosHandler test coverage deep-dive — 40 new tests, PK type handling fixes"
git tag v2.0.5
git push
git push --tags
```

### Wiki Git

```powershell
Push-Location "c:\git\CosmosDB.InMemoryEmulator.wiki"
git add -A
git commit -m "v2.0.5: Update Known Limitations, Features, Comparison for FakeCosmosHandler improvements"
git push
Pop-Location
```

---

## Test Execution Order (TDD Flow)

```
For each test in Phase 1..3:
  1. Write the failing test                              [RED]
  2. Run: dotnet test --filter "TestName" --verbosity minimal
  3. Verify test FAILS for the right reason
  4. Implement/fix the minimum code to pass              [GREEN]
  5. Run: dotnet test --filter "TestName" --verbosity minimal
  6. Verify test PASSES
  7. Refactor if needed                                  [REFACTOR]
  8. Run full suite: dotnet test --verbosity minimal
  9. Verify no regressions
  10. Update this plan: mark test as ✅ DONE
```

---

## Progress Tracker

| Phase | Total | Done | Skipped | Remaining |
|-------|-------|------|---------|-----------|
| 1 — Low  | 28 | 0 | 0 | 28 |
| 2 — Medium | 10 | 0 | 0 | 10 |
| 3 — Hard | 2 | 0 | 0 | 2 |
| Bug Fixes | 2 | 0 | 0 | 2 |
| Docs | 6 | 0 | 0 | 6 |
| Git | 2 | 0 | 0 | 2 |
| **TOTAL** | **50** | **0** | **0** | **50** |
