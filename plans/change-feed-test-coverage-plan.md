# Change Feed Test Coverage Deep Dive — Plan

**Created:** 2026-04-01  
**Current version:** 2.0.4 → will become **2.0.5** after implementation  
**Target file:** `tests/CosmosDB.InMemoryEmulator.Tests/ChangeFeedTests.cs`

---

## Table of Contents

1. [Existing Coverage Summary](#1-existing-coverage-summary)
2. [Identified Bugs](#2-identified-bugs)
3. [Missing Test Coverage](#3-missing-test-coverage)
4. [Tests That Will Be Skipped (Too Difficult / Divergent)](#4-tests-that-will-be-skipped)
5. [Implementation Order (TDD)](#5-implementation-order)
6. [Documentation Updates](#6-documentation-updates)
7. [Progress Tracker](#7-progress-tracker)

---

## 1. Existing Coverage Summary

The current `ChangeFeedTests.cs` (plus satellite classes in the same file) covers ~35 tests across 8 test classes:

### ChangeFeedTests (main class)
- ✅ Create → appears in change feed
- ✅ Upsert → latest version appears
- ✅ Multiple creates → all items appear
- ✅ Stream iterator → returns JSON with "Documents" envelope
- ✅ Empty container → returns OK with empty results (documented divergence)
- ✅ FromBeginning → all changes in order (incremental deduplication)
- ✅ FromNow (via checkpoint) → only subsequent changes
- ✅ Replace → records change
- ✅ Patch → records change
- ✅ Incremental: deleted item filtered out
- ✅ Processor: build/start/stop lifecycle
- ✅ Processor: builder methods return builder (chaining)
- ✅ Processor: invokes handler when items created
- ✅ Processor: multiple changes across multiple polls
- ✅ Processor: legacy ChangesHandler invoked
- ✅ Processor: context has lease token
- ✅ Processor: StopAsync stops polling
- ✅ GetFeedRanges → returns single range

### ChangeFeedStreamProcessorDivergentTests5
- ✅ Stream handler processor invokes handler

### ChangeFeedFeedRangeDivergentBehaviorTests4
- ✅ FeedRange scoping works (multi-range union = full dataset)

### ChangeFeedProcessorDivergentBehaviorTests
- ✅ WithLeaseContainer throws InvalidCastException (documented divergence)

### ChangeFeedManualCheckpointDivergentBehaviorTests4
- ✅ Manual checkpoint processor invokes handler

### ChangeFeedManualCheckpointStreamTests
- ✅ Manual checkpoint stream handler invokes and checkpoints
- ✅ Without calling checkpoint → redelivers changes

### ChangeFeedGapTests
- ✅ From checkpoint → only new changes
- ✅ Order preserved across writes (all-versions via checkpoint)
- ✅ Delete → tombstone via checkpoint (`_deleted: true`)
- ✅ Patch → recorded via checkpoint
- ✅ Stream iterator → JSON envelope with Documents array

### ChangeFeedAdvancedTests
- ✅ GetChangeFeedEstimator returns non-null
- ✅ GetChangeFeedEstimatorBuilder returns builder
- ✅ GetChangeFeedStreamIterator from beginning returns stream
- ✅ Skipped: Stream handler with WithLeaseContainer (documented divergence)

### ChangeFeedGapTests3
- ✅ Incremental mode → only latest version per item (3 upserts → 1 result)
- ✅ All versions via checkpoint (2 versions returned)
- ✅ Delete tombstone in full fidelity mode
- ✅ FromTimestamp → filters older items

### ChangeFeedGapTests4
- ✅ PageSizeHint limits page size
- ✅ FeedRange scoping (multi-range, proves subset)
- ✅ Manual checkpoint invokes handler

---

## 2. Identified Bugs

### Bug 1: `ChangeFeedStartFrom.Now()` in stream iterator uses wall-clock time, not captured timestamp
**File:** `InMemoryContainer.cs`, `FilterChangeFeedByStartFrom()` method  
**Problem:** The `GetChangeFeedStreamIterator` method calls `FilterChangeFeedByStartFrom()` which for `Now` uses `DateTimeOffset.UtcNow` at evaluation time. But unlike the typed `GetChangeFeedIterator<T>` (which captures the timestamp at creation time via lazy evaluation), the stream iterator eagerly evaluates. This means the stream iterator's "Now" filter evaluates at call time, not at creation time — items added between creation and ReadNextAsync would be missed if the internal implementation changes.  
**Current impact:** Low — the stream iterator evaluates eagerly anyway (all in one shot), so the time captured is essentially the evaluation time. But it's semantically inconsistent with the typed iterator.  
**Fix:** Capture the timestamp for "Now" and "Time" consistently in both paths.  
**Test:** `ChangeFeed_StreamIterator_FromNow_CapturesTimestampAtCreation`

### Bug 2: `InMemoryFeedIterator` `HasMoreResults` returns false for empty change feed, but doesn't return 304 NotModified
**File:** `InMemoryFeedIterator.cs`  
**Problem:** When the change feed has no new items, `HasMoreResults` returns `false` and the loop never enters. Real Cosmos DB returns a response with `HttpStatusCode.NotModified` (304). The current implementation makes `HasMoreResults = false` for empty feeds and `StatusCode = OK` for all responses. Tests work around this by checking for NotModified OR empty results, but no test verifies the specific status code behaviour.  
**Current impact:** Low — documented as behavioral difference already. Not a true bug, but the existing test `ChangeFeedIterator_EmptyContainer_ReturnsOkWithEmptyResults` should be clearer about this being divergent.  
**Action:** Not a fix, but add a comment referencing the divergence and add a sister test.

### Bug 3: `InMemoryStreamFeedIterator` doesn't support pagination
**File:** `InMemoryStreamFeedIterator.cs`  
**Problem:** The stream feed iterator always returns all items in a single page (`_hasMoreResults = false` after first read). No `PageSizeHint` is respected. This is fine for most use cases but diverges from real Cosmos DB which supports pagination for stream iterators too.  
**Current impact:** Low — stream iterators are less commonly paginated.  
**Action:** Document as limitation, add test showing the behavior.

### Bug 4: Processor `StartAsync` captures checkpoint *at start time*, not at "Now"
**File:** `InMemoryChangeFeedProcessor.cs`  
**Problem:** All processor variants call `_container.GetChangeFeedCheckpoint()` in `StartAsync()`. If items were inserted before the processor started, they get skipped because the checkpoint is set to the current feed length. This is actually *correct* behaviour (processors should only see new changes after starting), but it means items created between container creation and processor start are NOT seen. This matches real Cosmos DB behavior with a new lease.  
**Action:** Not a bug — add test to explicitly verify this expected behavior.

### Bug 5: `RecordDeleteTombstone` uses `|` as composite PK separator, but this could clash with PK values containing `|`
**File:** `InMemoryContainer.cs`, lines 1656-1665  
**Problem:** `RecordDeleteTombstone` splits `pk` by `|` to populate partition key fields in the tombstone. If a partition key value actually contains `|`, this splits incorrectly.  
**Current impact:** Edge case — composite PKs with `|` in values would produce malformed tombstones.  
**Action:** Add test to document the edge case. Fix if feasible.

---

## 3. Missing Test Coverage

### Category A: Iterator Lifecycle & Edge Cases

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| A1 | `ChangeFeed_FromBeginning_ThenAddMoreItems_LazyEval_DoesNotIncludeNewItems` | Verify Beginning() uses eager evaluation — items added after iterator creation should NOT appear | Not started |
| A2 | `ChangeFeed_FromNow_ThenAddItems_LazyEval_IncludesNewItems` | Verify Now() uses lazy evaluation — items added after iterator creation but before ReadNextAsync SHOULD appear | Not started |
| A3 | `ChangeFeed_ContinuationToken_ResumesFromLastPosition` | Read first page with PageSizeHint, then use continuation token to read next page | Not started |
| A4 | `ChangeFeed_ContinuationToken_NullWhenExhausted` | Verify continuation token is null on last page | Not started |
| A5 | `ChangeFeed_MultipleReadNext_PaginatesThroughAllItems` | Create many items, paginate through with PageSizeHint | Not started |
| A6 | `ChangeFeed_EmptyFeed_HasMoreResults_IsFalse` | Verify HasMoreResults is false for empty feed | Not started |
| A7 | `ChangeFeed_ReadNextAsync_CancellationToken_IsRespected` | Pass cancelled token, verify TaskCanceledException or proper handling | Not started |

### Category B: All CRUD Operations Record Change Feed

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| B1 | `ChangeFeed_CreateItemStream_RecordsChange` | Verify CreateItemStreamAsync records to change feed | Not started |
| B2 | `ChangeFeed_UpsertItemStream_RecordsChange` | Verify UpsertItemStreamAsync records to change feed | Not started |
| B3 | `ChangeFeed_ReplaceItemStream_RecordsChange` | Verify ReplaceItemStreamAsync records to change feed | Not started |
| B4 | `ChangeFeed_DeleteItemStream_RecordsTombstone` | Verify DeleteItemStreamAsync records tombstone | Not started |
| B5 | `ChangeFeed_PatchItemStream_RecordsChange` | Verify PatchItemStreamAsync records to change feed | Not started |
| B6 | `ChangeFeed_TransactionalBatch_RecordsAllChanges` | Verify transactional batch operations appear in change feed | Not started |
| B7 | `ChangeFeed_BulkCreate_RecordsAllChanges` | Verify bulk CreateItemAsync records all items (may overlap with BulkOperationTests) | Not started |

### Category C: Cross-Partition Key Scenarios

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| C1 | `ChangeFeed_MultiplePartitionKeys_ReturnsAllChanges` | Items across different PKs all appear in change feed | Not started |
| C2 | `ChangeFeed_CompositePartitionKey_RecordsCorrectly` | Container with composite PK records changes correctly | Not started |
| C3 | `ChangeFeed_HierarchicalPartitionKey_RecordsCorrectly` | Container with hierarchical PK records changes correctly | Not started |
| C4 | `ChangeFeed_PartitionKeyNone_RecordsChange` | Items with PartitionKey.None appear in change feed | Not started |
| C5 | `ChangeFeed_DeleteTombstone_CompositeKey_HasCorrectPkFields` | Tombstone for composite PK has all PK fields populated | Not started |

### Category D: Delete Tombstone Details

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| D1 | `ChangeFeed_DeleteTombstone_HasTimestamp` | Tombstone has `_ts` field with Unix epoch seconds | Not started |
| D2 | `ChangeFeed_DeleteTombstone_HasPartitionKeyField` | Tombstone has the partition key field populated | Not started |
| D3 | `ChangeFeed_DeleteTombstone_Incremental_ExcludesActiveThenDeletedItems` | Create item, delete it — incremental feed returns nothing | Not started |
| D4 | `ChangeFeed_DeleteTombstone_AllVersions_ShowsCreateThenDelete` | Create then delete — all-versions shows both entries | Not started |
| D5 | `ChangeFeed_DeleteNonexistentItem_ThrowsNotFound` | Attempting to delete an item that doesn't exist throws, nothing recorded | Not started |

### Category E: Processor Advanced Scenarios

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| E1 | `ChangeFeedProcessor_DoesNotSeeItemsCreatedBeforeStart` | Items created before StartAsync are NOT delivered | Not started |
| E2 | `ChangeFeedProcessor_HandlerException_DoesNotCrashProcessor` | If handler throws, processor continues polling | Not started |
| E3 | `ChangeFeedProcessor_MultipleStartStop_WorksCorrectly` | Start, stop, start again — no duplicate deliveries | Not started |
| E4 | `ChangeFeedProcessor_ConcurrentCreates_AllDelivered` | Multiple concurrent CreateItemAsync, all get delivered | Not started |
| E5 | `ChangeFeedProcessor_LargeNumberOfItems_AllDelivered` | Create 100+ items, verify all delivered eventually | Not started |
| E6 | `ChangeFeedProcessor_Context_HasFeedRange` | Verify context.FeedRange is not null | Not started |
| E7 | `ChangeFeedProcessor_Context_HasHeaders` | Verify context.Headers is not null | Not started |
| E8 | `ManualCheckpoint_PartialCheckpoint_OnlyCheckpointedItemsSkipped` | Process batch, checkpoint after first item — verify redelivery of remaining | Not started |

### Category F: Stream Iterator Specifics

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| F1 | `ChangeFeedStream_EmptyContainer_ReturnsEmptyDocumentsArray` | Stream iterator on empty container returns `{"Documents": []}` | Not started |
| F2 | `ChangeFeedStream_MultipleItems_AllInDocumentsArray` | Verify all items in the Documents array | Not started |
| F3 | `ChangeFeedStream_HasMoreResults_IsFalseAfterRead` | Stream always returns false after single read (single page) | Not started |
| F4 | `ChangeFeedStream_ResponseStatusCode_IsOK` | Verify StatusCode is 200 OK | Not started |
| F5 | `ChangeFeedStream_FromNow_ReturnsOnlyNewItems` | Verify Now() start position with stream iterator | Not started |
| F6 | `ChangeFeedStream_FromTime_FiltersCorrectly` | Verify Time() start position with stream iterator | Not started |

### Category G: `ChangeFeedStartFrom` Variants

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| G1 | `ChangeFeed_FromBeginning_WithNullFeedRange_ReturnsAllItems` | `Beginning()` without FeedRange returns everything | Not started |
| G2 | `ChangeFeed_FromNow_WithFeedRange_ScopesToRange` | `Now(feedRange)` scopes to specific range | Not started |
| G3 | `ChangeFeed_FromTime_WithFeedRange_FiltersBothTimeAndRange` | Combined time + range filtering | Not started |
| G4 | `ChangeFeed_FromTime_ExactTimestamp_IncludesItemAtThatTime` | `>=` semantics — item at exact time is included | Not started |
| G5 | `ChangeFeed_FromNow_NothingAdded_ReturnsEmpty` | Now() with no subsequent writes returns empty | Not started |
| G6 | `ChangeFeed_FromTime_FutureTimestamp_ReturnsEmpty` | Time far in future returns empty | Not started |
| G7 | `ChangeFeed_FromTime_PastTimestamp_ReturnsAll` | Time before any writes returns all | Not started |

### Category H: Concurrency & Thread Safety

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| H1 | `ChangeFeed_ConcurrentWritesAndReads_NoExceptions` | Parallel writes + change feed reads don't throw | Not started |
| H2 | `ChangeFeed_ConcurrentProcessors_BothReceiveChanges` | Two processors on same container both get all changes | Not started |
| H3 | `ChangeFeed_Checkpoint_ThreadSafe` | Multiple threads calling GetChangeFeedCheckpoint concurrently | Not started |

### Category I: ClearItems Interaction

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| I1 | `ChangeFeed_ClearItems_ResetsChangeFeed` | After ClearItems(), change feed is empty | Not started |
| I2 | `ChangeFeed_ClearItems_ThenAddItems_StartsFromScratch` | Clear then add — only new items in feed | Not started |
| I3 | `ChangeFeed_Checkpoint_AfterClearItems_IsZero` | Checkpoint resets to 0 after clear | Not started |

### Category J: Estimator (Stub Coverage)

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| J1 | `ChangeFeedEstimator_IsStub_DoesNotThrow` | Estimator can be called without exception | Not started |
| J2 | `ChangeFeedEstimatorBuilder_Build_ReturnsProcessor` | Builder.Build() returns a working processor | Not started |

---

## 4. Tests That Will Be Skipped (Too Difficult / Divergent)

### Skip 1: `ChangeFeed_AllVersionsAndDeletesMode_ViaSDKEnum`
**Reason:** `ChangeFeedMode.AllVersionsAndDeletes` is internal in older SDK versions. The emulator supports this via the checkpoint-based `GetChangeFeedIterator<T>(long)` API instead. Implementing the SDK enum path would require reflection into internal types.  
**Sister test:** `ChangeFeed_AllVersions_ViaCheckpoint` (already exists) — documents the alternative API.

### Skip 2: `ChangeFeedProcessor_WithLeaseContainer_RealLease`
**Reason:** Real SDK casts Container to internal ContainerInternal class. InMemoryContainer only extends public Container. Would require implementing internal SDK types.  
**Sister test:** `ChangeFeedProcessorBuilder_WithLeaseContainer_ThrowsInvalidCast` (already exists) — documents the divergence.

### Skip 3: `ChangeFeedStream_PageSizeHint_LimitsResults`
**Reason:** `InMemoryStreamFeedIterator` returns all items in a single response. Implementing pagination for stream iterators would require significant refactoring of the stream iterator infrastructure for limited benefit.  
**Sister test:** `ChangeFeedStream_HasMoreResults_IsFalseAfterSingleRead` (new F3) — documents the single-page behavior.

### Skip 4: `ChangeFeed_ContinuationToken_FromChangeFeedStartFrom`
**Reason:** `ChangeFeedStartFrom.ContinuationToken()` creates an internal `ChangeFeedStartFromContinuationAndFeedRange` type. The current implementation's `FilterChangeFeedByStartFrom` falls through to the default (returns all) for unrecognized types, and the typed iterator's `Now`/`Time` detection won't match. Supporting this would require parsing opaque continuation tokens.  
**Sister test:** New test `ChangeFeed_ContinuationAndFeedRange_FallsBackToAll` — documents the fallback behavior.

### Skip 5: `ChangeFeedProcessor_HandlerException_RetriesBatch`
**Reason:** Real Cosmos DB retries the batch if the handler throws. The in-memory processor's `PollAsync` catches `OperationCanceledException` (for stop) but lets other exceptions propagate, which kills the polling loop. Fixing this requires adding try/catch with retry logic.  
**Sister test:** E2 `ChangeFeedProcessor_HandlerException_DoesNotCrashProcessor` — will test current behavior (processor stops on exception) rather than ideal behavior.  
**Note:** E2 will be implemented to TEST the actual behavior, and if the processor crashes on handler exception, we'll fix it to continue polling (with the batch checkpoint not advancing, matching Cosmos DB retry semantics).

---

## 5. Implementation Order (TDD)

Phase 1 — Bug fixes & their tests (red-green-refactor):
1. ☐ E2: Handler exception resilience → fix processor to catch handler exceptions and retry batch
2. ☐ Bug 5 test: Composite PK tombstone with `|` in values → fix `RecordDeleteTombstone`

Phase 2 — Iterator lifecycle & edge cases:
3. ☐ A1: Beginning() eager evaluation verification
4. ☐ A2: Now() lazy evaluation verification  
5. ☐ A3: Continuation token resume
6. ☐ A4: Continuation token null when exhausted
7. ☐ A5: Multi-page pagination
8. ☐ A6: Empty feed HasMoreResults
9. ☐ A7: CancellationToken respected

Phase 3 — CRUD completeness:
10. ☐ B1-B5: Stream API variants record change feed
11. ☐ B6: Transactional batch
12. ☐ B7: Bulk operations (verify not duplicating BulkOperationTests)

Phase 4 — Partition key edge cases:
13. ☐ C1: Multiple partition keys
14. ☐ C2: Composite partition key
15. ☐ C3: Hierarchical partition key
16. ☐ C4: PartitionKey.None
17. ☐ C5: Composite PK tombstone fields

Phase 5 — Delete tombstone details:
18. ☐ D1-D5: Tombstone field verification

Phase 6 — Processor advanced scenarios:
19. ☐ E1: Items before start not delivered
20. ☐ E3: Multiple start/stop cycles
21. ☐ E4: Concurrent creates all delivered
22. ☐ E5: Large batch delivery
23. ☐ E6-E7: Context properties
24. ☐ E8: Partial manual checkpoint

Phase 7 — Stream iterator:
25. ☐ F1-F6: Stream iterator edge cases

Phase 8 — StartFrom variants:
26. ☐ G1-G7: All ChangeFeedStartFrom variants

Phase 9 — Concurrency:
27. ☐ H1-H3: Thread safety tests

Phase 10 — ClearItems interaction:
28. ☐ I1-I3: ClearItems resets change feed

Phase 11 — Estimator stubs:
29. ☐ J1-J2: Estimator coverage

Phase 12 — Skipped tests with sister tests:
30. ☐ Skip 3 + F3: Stream pagination skip + sister
31. ☐ Skip 4: ContinuationAndFeedRange fallback
32. ☐ Skip 5 + E2: Handler exception skip + sister (if behavior can't be fixed)

---

## 6. Documentation Updates

After all tests pass:

### 6.1 Wiki Known-Limitations.md
- **Review:** Ensure the `AllVersionsAndDeletes` mode entry is still accurate
- **Review:** Ensure the change feed stream processor entry is still accurate
- **Add:** If any new limitations discovered during testing, document them
- **Add:** If Bug 3 (stream pagination) remains unfixed, add as known limitation

### 6.2 Wiki Features.md
- **Update** change feed section if any new capabilities added (e.g., handler exception resilience)
- **Add** note about ClearItems() resetting change feed if not already mentioned

### 6.3 Wiki Feature-Comparison-With-Alternatives.md
- **Review** change feed row for accuracy after new tests

### 6.4 README.md
- **Review** features list if any change feed capabilities changed

### 6.5 Version Bump
- **Bump** `CosmosDB.InMemoryEmulator.csproj` version from `2.0.4` → `2.0.5`
- **Git tag:** `v2.0.5`
- **Git push** with tags

### 6.6 Commit Message Template
```
v2.0.5: Change feed test coverage deep dive — N new tests, M bug fixes

- Fix processor handler exception resilience (continues polling on error)
- Fix composite PK tombstone splitting for values containing '|'
- Add N tests covering: iterator lifecycle, stream CRUD, partition key edge cases,
  delete tombstone details, processor advanced scenarios, concurrency, ClearItems
- Document N skipped tests with sister tests showing divergent behavior
```

---

## 7. Progress Tracker

| Phase | Tests | Status |
|-------|-------|--------|
| Phase 1 — Bug fixes | E2, Bug 5 | ⬜ Not started |
| Phase 2 — Iterator lifecycle | A1-A7 | ⬜ Not started |
| Phase 3 — CRUD completeness | B1-B7 | ⬜ Not started |
| Phase 4 — Partition keys | C1-C5 | ⬜ Not started |
| Phase 5 — Tombstone details | D1-D5 | ⬜ Not started |
| Phase 6 — Processor advanced | E1, E3-E8 | ⬜ Not started |
| Phase 7 — Stream iterator | F1-F6 | ⬜ Not started |
| Phase 8 — StartFrom variants | G1-G7 | ⬜ Not started |
| Phase 9 — Concurrency | H1-H3 | ⬜ Not started |
| Phase 10 — ClearItems | I1-I3 | ⬜ Not started |
| Phase 11 — Estimator stubs | J1-J2 | ⬜ Not started |
| Phase 12 — Skipped + sisters | Skip 3-5 | ⬜ Not started |
| Documentation updates | Wiki, README | ⬜ Not started |
| Version bump + tag + push | v2.0.5 | ⬜ Not started |

**Total new tests:** ~55  
**Total bug fixes:** 2 (handler exception resilience, composite PK tombstone)  
**Total skipped tests:** 5 (with sister tests documenting divergent behavior)
