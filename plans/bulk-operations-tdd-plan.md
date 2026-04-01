# Bulk Operations TDD Plan

**Version**: v2.0.5 (target)  
**File**: `tests/CosmosDB.InMemoryEmulator.Tests/BulkOperationTests.cs`  
**Date**: 2026-04-01  
**Approach**: TDD — Red-Green-Refactor. Write failing test first, implement fix, verify green. Skip tests whose behaviour is too hard to implement and add a sister divergent-behaviour test.

---

## Phase 0 — Bugs in Existing Tests

### Bug 1: `InMemoryCosmosClient_ClientOptions_AllowBulkExecution_CanBeSet` is async with no await
- **Problem**: Method is `async Task` but contains no `await`. Compiler warning CS1998. Harmless but sloppy.
- **Fix**: Remove `async` keyword, return `Task.CompletedTask` or make it `void`.

### Bug 2: `BulkCreate_ViaCosmosClient_WithAllowBulkExecution_AllSucceed` uses wrong partition key path
- **Problem**: `client.GetDatabase("test-db").GetContainer("test-container")` auto-creates the container with default partition key path `/id`. But the test creates `TestDocument` items with `PartitionKey = "pk1"` and passes `new PartitionKey("pk1")`. In real Cosmos DB, the PK value must match the document field at the PK path — this would fail against real Cosmos because the actual `/id` field values are `"0"`, `"1"`, etc., not `"pk1"`.
- **Fix**: Create the container explicitly with `/partitionKey` path before running the test, e.g. via `database.CreateContainerIfNotExistsAsync("test-container", "/partitionKey")`.

### Bug 3: `BulkMixedOperations_CreateUpsertDeleteReplace_AllSucceed` has weak assertions
- **Problem**: Only asserts `results.Should().NotBeEmpty()` and the final count. Does not verify individual operation status codes (Created, OK, NoContent). If an operation returned an unexpected status, the test would still pass.
- **Fix**: Assert that creates returned `HttpStatusCode.Created`, upserts returned `HttpStatusCode.Created`, deletes returned `HttpStatusCode.NoContent`, replaces returned `HttpStatusCode.OK`.

---

## Phase 1 — Stream Variant Coverage

Currently only `BulkCreate_StreamVariant_ManyItems_AllSucceed` exists. Need stream variants for all CRUD operations.

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| 1.1 | `BulkUpsert_StreamVariant_ManyItems_AllSucceed` | 200 concurrent `UpsertItemStreamAsync`, verify all 201; then re-upsert all, verify 200 | not-started |
| 1.2 | `BulkReplace_StreamVariant_ManyItems_AllSucceed` | Pre-create 200, concurrent `ReplaceItemStreamAsync`, verify all 200 | not-started |
| 1.3 | `BulkDelete_StreamVariant_ManyItems_AllSucceed` | Pre-create 200, concurrent `DeleteItemStreamAsync`, verify all 204 | not-started |
| 1.4 | `BulkPatch_StreamVariant_ManyItems_AllSucceed` | Pre-create 200, concurrent `PatchItemStreamAsync`, verify all 200 | not-started |
| 1.5 | `BulkRead_StreamVariant_ManyItems_AllSucceed` | Pre-create 200, concurrent `ReadItemStreamAsync`, verify all 200 and body content | not-started |

---

## Phase 2 — Error Handling / Negative Paths

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| 2.1 | `BulkReplace_NonExistentItems_AllReturn404` | Concurrent `ReplaceItemAsync` on 50 IDs that don't exist; each should throw `CosmosException` with 404 | not-started |
| 2.2 | `BulkDelete_NonExistentItems_AllReturn404` | Concurrent `DeleteItemAsync` on 50 IDs that don't exist; each should throw 404 | not-started |
| 2.3 | `BulkPatch_NonExistentItems_AllReturn404` | Concurrent `PatchItemAsync` on 50 IDs that don't exist; each should throw 404 | not-started |
| 2.4 | `BulkRead_NonExistentItems_AllReturn404` | Concurrent `ReadItemAsync` on 50 IDs that don't exist; each should throw 404 | not-started |
| 2.5 | `BulkCreate_OversizedDocuments_AllReturn413` | Concurrent `CreateItemAsync` with docs near/over 2MB limit; should throw 413 RequestEntityTooLarge | not-started |

---

## Phase 3 — RequestOptions and Conditional Operations

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| 3.1 | `BulkReplace_WithCorrectIfMatchEtag_AllSucceed` | Pre-create 100 items, collect ETags, concurrent `ReplaceItemAsync` with `IfMatchEtag` set correctly — all succeed | not-started |
| 3.2 | `BulkReplace_WithStaleIfMatchEtag_AllFail412` | Pre-create 100 items, mutate them so ETags change, concurrent `ReplaceItemAsync` with old ETags — all fail 412 PreconditionFailed | not-started |
| 3.3 | `BulkUpsert_WithIfMatchStar_AllSucceed` | Concurrent `UpsertItemAsync` with `IfMatchEtag = "*"` — all succeed (wildcard always matches) | not-started |
| 3.4 | `BulkCreate_WithEnableContentResponseOnWriteFalse_ResponseResourceIsNull` | Concurrent `CreateItemAsync` with `EnableContentResponseOnWrite = false` — verify `response.Resource` is null/default for all | not-started |
| 3.5 | `BulkOperations_CancellationToken_ThrowsWhenCancelled` | Start 200 concurrent creates with an already-cancelled token; all should throw `OperationCanceledException` | not-started |

---

## Phase 4 — Concurrency Race Conditions & Edge Cases

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| 4.1 | `BulkCreate_SameIdDifferentPartitionKeys_AllSucceed` | Concurrent `CreateItemAsync` with same ID but different PKs — all should succeed (IDs unique per partition) | not-started |
| 4.2 | `BulkUpsert_ConcurrentOnSameItem_LastWriterWins` | 50 concurrent upserts to the same (id, pk) — all return 200/201, final document reflects one of them, no corruption | not-started |
| 4.3 | `BulkReplace_ConcurrentOnSameItem_LastWriterWins` | Pre-create 1 item, 50 concurrent replaces — all succeed, final document is consistent, no partial state | not-started |
| 4.4 | `BulkDelete_ConcurrentOnSameItem_ExactlyOneSucceeds` | Pre-create 1 item, 50 concurrent deletes — exactly 1 returns 204, rest throw 404 | not-started |
| 4.5 | `BulkCreate_EmptyBatch_Succeeds` | `Task.WhenAll` of empty enumerable — should complete immediately with empty array | not-started |
| 4.6 | `BulkCreate_SingleItem_Succeeds` | `Task.WhenAll` of 1 task — basic sanity check | not-started |
| 4.7 | `BulkOperations_InterleavedWithTransactionalBatch_NoCorruption` | Concurrent bulk creates alongside a transactional batch on same container — no corruption or deadlock | not-started |

---

## Phase 5 — Data Integrity & Metadata

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| 5.1 | `BulkCreate_SystemMetadata_TsAndEtagSetOnAllItems` | Bulk create 100 items, read back all, verify `_ts` and `_etag` are set and `_ts` is a valid Unix timestamp | not-started |
| 5.2 | `BulkUpsert_ETags_ChangeOnUpdate` | Bulk create 100 items, record ETags, bulk upsert same items, verify all ETags changed | not-started |
| 5.3 | `BulkCreate_ResponseMetadata_RequestChargeAndActivityId` | Bulk create 100 items, verify every response has `RequestCharge > 0`, non-null `ActivityId`, non-null `Headers` | not-started |
| 5.4 | `BulkCreate_SpecialCharactersInIds_AllSucceed` | IDs with spaces, Unicode, URL-encoded chars, hyphens, dots — all should create successfully | not-started |
| 5.5 | `BulkCreate_UnicodeInPartitionKeys_AllSucceed` | Partition keys with emoji, CJK, accented characters — all should succeed and be queryable | not-started |

---

## Phase 6 — Change Feed Interactions

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| 6.1 | `BulkUpsert_ChangeFeed_RecordsAllUpdates` | Bulk upsert 100 items (new), then bulk upsert again (update) — change feed should see 200 entries (100 creates + 100 updates) | not-started |
| 6.2 | `BulkDelete_ChangeFeed_RecordsTombstones` | Bulk create 100 items, checkpoint, bulk delete all — change feed after checkpoint should contain delete tombstones | not-started |
| 6.3 | `BulkMixedOps_ChangeFeed_PreservesOperationOrder` | Bulk create, upsert, delete in sequence with checkpoints — change feed partitions changes correctly | not-started |

---

## Phase 7 — Partition Key Variants

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| 7.1 | `BulkCreate_WithPartitionKeyNone_ExtractsFromDocument` | Bulk create 100 items passing `PartitionKey.None` — PK should be extracted from document body at the configured PK path | not-started |
| 7.2 | `BulkCreate_HierarchicalPartitionKey_AllSucceed` | Container with composite PK paths, bulk create with `PartitionKeyBuilder` — all succeed. **May need to Skip if hierarchical PK support is incomplete; add divergent-behaviour sister test.** | not-started |

---

## Phase 8 — DeleteAllItemsByPartitionKey Integration

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| 8.1 | `DeleteAllItemsByPartitionKey_ConcurrentWithBulkCreate_NoCorruption` | Start concurrent bulk create on PK "A", and `DeleteAllItemsByPartitionKeyStreamAsync` on PK "B" — PK "A" items all persist, PK "B" wiped | not-started |
| 8.2 | `BulkCreate_ThenDeleteAllByPartitionKey_ContainerEmpty` | Bulk create 500 items on PK "pk1", then `DeleteAllItemsByPartitionKeyStreamAsync("pk1")` — container empty | not-started |

---

## Phase 9 — Post-Implementation Tasks

| # | Task | Status |
|---|------|--------|
| 9.1 | Update wiki **Known-Limitations.md** — add any new limitations discovered (e.g., hierarchical PK + bulk edge cases) | not-started |
| 9.2 | Update wiki **Features.md** — ensure bulk operations section reflects tested capabilities | not-started |
| 9.3 | Update wiki **Feature-Comparison-With-Alternatives.md** — update bulk operations row if needed | not-started |
| 9.4 | Update **README.md** — verify bulk operations description is accurate | not-started |
| 9.5 | Increment version to **2.0.5** in `src/CosmosDB.InMemoryEmulator/CosmosDB.InMemoryEmulator.csproj` | not-started |
| 9.6 | `git add -A && git commit -m "v2.0.5: ..."` | not-started |
| 9.7 | `git tag v2.0.5 && git push && git push --tags` | not-started |
| 9.8 | Update wiki and push: `cd wiki && git add -A && git commit && git push` | not-started |

---

## Summary Statistics

| Category | Count |
|----------|-------|
| Bugs to fix | 3 |
| New tests — Stream variants | 5 |
| New tests — Error handling | 5 |
| New tests — RequestOptions/Conditional | 5 |
| New tests — Concurrency edge cases | 7 |
| New tests — Data integrity/Metadata | 5 |
| New tests — Change feed | 3 |
| New tests — Partition key variants | 2 |
| New tests — DeleteAllByPK integration | 2 |
| Post-implementation tasks | 8 |
| **Total new tests** | **34** |
| **Total work items** | **45** |

---

## Execution Order

1. Fix Bug 1, Bug 2, Bug 3 (existing test corrections)
2. Phase 1 → Phase 8 (new tests, TDD red-green-refactor)
3. For each test:
   - Write failing test (RED)
   - Implement minimum code to pass (GREEN)
   - Refactor if needed
   - If behaviour is too hard to implement → Skip with detailed reason + add divergent-behaviour sister test
4. Phase 9 (documentation, version bump, tag, push)

---

## Skip / Divergent Behaviour Protocol

When a test's expected behaviour cannot be implemented:
1. Mark the test with `[Fact(Skip = "REASON: detailed explanation of why this diverges from real Cosmos DB")]`
2. Create a sister test named `{OriginalName}_DivergentBehaviour` with `[Fact]` (not skipped)
3. The sister test documents the *actual* emulator behaviour with heavy inline comments explaining:
   - What real Cosmos DB does
   - What the emulator does instead
   - Why the difference exists
   - Whether it matters in practice
