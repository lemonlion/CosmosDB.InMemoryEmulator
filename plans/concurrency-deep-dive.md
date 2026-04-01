# Concurrency Deep Dive Plan

**Status:** PLANNING (not yet implemented)  
**Current Version:** 2.0.4  
**Target Version:** 2.0.5  
**TDD Approach:** Red-Green-Refactor — write failing test first, then fix implementation  

---

## Part A: Bugs Found in InMemoryContainer.cs

### Bug 1: RestoreSnapshot Does Not Restore _timestamps
- **Location:** `RestoreSnapshot()` (~line 2018)
- **Impact:** When a transactional batch fails and rolls back via `RestoreSnapshot()`, `_items` and `_etags` are restored but `_timestamps` retains values from the failed operations. State is permanently inconsistent.
- **Fix:** Add `_timestamps` to snapshot/restore cycle — add `SnapshotTimestamps()` and restore them in `RestoreSnapshot()`.

### Bug 2: Delete Operations Have No Lock Protection (TOCTOU)
- **Location:** `DeleteItemAsync()` (~line 589), `DeleteItemStreamAsync()` (~line 901)
- **Impact:** Two concurrent deletes of the same item both pass `ContainsKey` check, then both call `TryRemove`. Real Cosmos would return 404 for the second delete in a serialised execution, but both succeed here (no 404 for the loser). Also, a concurrent delete + read can produce inconsistencies between `_items`, `_etags`, and `_timestamps` removal order.
- **Severity:** Low for in-memory emulator (ConcurrentDictionary.TryRemove is safe), but behavioral fidelity gap.
- **Decision:** Document as known limitation rather than fix — real Cosmos doesn't guarantee serialised delete ordering either. Write test to show current behaviour.

### Bug 3: Patch Operations Read Existing Item Outside Lock (Lost Update)
- **Location:** `PatchItemAsync()` (~line 611), `PatchItemStreamAsync()` (~line 926)
- **Impact:** Thread A reads `existingJson`, Thread B modifies item, Thread A applies patch on stale data and writes back — Thread B's update is lost. This is a classic read-modify-write race condition.
- **Real Cosmos Behaviour:** Real Cosmos serialises operations within a logical partition, so this race doesn't occur. Users can also use ETags for protection.
- **Decision:** This is actually correct emulator behaviour for the non-ETag case — real Cosmos has the same last-writer-wins semantic at the HTTP level. The ETag mechanism (`IfMatchEtag`) is the intended protection. Write tests to demonstrate both the unprotected and ETag-protected cases.

### Bug 4: Upsert/Replace/Create Without Unique Keys Have No Write Locking
- **Location:** All write methods' `else` branches when `HasUniqueKeys == false`
- **Impact:** When no unique key policy is configured, write operations (etag generation, timestamp assignment, item enrichment) are completely unprotected. Multiple concurrent writers can interleave these individual dictionary updates, leaving _items, _etags, and _timestamps temporarily inconsistent.
- **Real Cosmos Behaviour:** Real Cosmos serialises writes within a partition. Without unique keys, multiple concurrent upserts on different items are fine (ConcurrentDictionary handles this). For same-item writes, last-writer-wins is the expected semantic.
- **Decision:** This is an acceptable emulator trade-off. ConcurrentDictionary ensures no corruption. The brief inconsistency between _etags and _items for the same key is theoretical — in practice, the final state is always consistent. Write tests to verify.

### Bug 5: Change Feed Recording Happens After Item Mutation
- **Location:** `RecordChangeFeed()` called after `_items[key] = enrichedJson` in all write methods
- **Impact:** Brief window where item is visible via read/query but not yet in the change feed.
- **Decision:** Acceptable emulator trade-off — real Cosmos has similar eventual consistency between operations and change feed visibility. Write test to document behaviour.

---

## Part B: Missing Test Coverage

### Category 1: Concurrent Delete Scenarios
These are completely absent from the existing tests.

#### Test 1.1: ConcurrentDeletes_SameItem_OnlyOneSeesItem
- **What:** 50 threads all try to delete the same item simultaneously
- **Why:** Verifies thread safety of delete path (no crashes/corruption)
- **Expected:** All complete without exception (TryRemove is safe), item is gone
- **Difficulty:** Easy

#### Test 1.2: ConcurrentDeleteAndRead_ItemDisappearsCleanly
- **What:** Pre-create 100 items, 50 threads delete items while 50 threads read them
- **Why:** Ensures reads don't crash on items being deleted mid-operation
- **Expected:** Reads either succeed (200) or throw NotFound (404), no corruption
- **Difficulty:** Easy

#### Test 1.3: ConcurrentDeleteAndCreate_SameId_NoCorruption
- **What:** Rapidly delete and recreate the same item from multiple threads
- **Why:** Tests the create-delete-create cycle under concurrency
- **Expected:** No data corruption, item count is consistent after all operations settle
- **Difficulty:** Medium

#### Test 1.4: ConcurrentDeleteAndUpsert_SameItem_UpsertRevives
- **What:** One thread deletes item, another thread upserts it simultaneously
- **Why:** Tests the upsert-as-create-or-update under concurrent deletion
- **Expected:** Item either exists (upsert won) or doesn't (delete won after upsert), state is consistent
- **Difficulty:** Medium

### Category 2: Concurrent Patch Scenarios
Only covered indirectly — no dedicated concurrent patch tests exist.

#### Test 2.1: ConcurrentPatches_SameItem_AllSucceed_LastWriteWins
- **What:** 50 threads patch the same item's name field concurrently
- **Why:** Tests thread safety of patch-on-same-item path
- **Expected:** All succeed (200), final state reflects one of the patches
- **Difficulty:** Easy

#### Test 2.2: ConcurrentPatches_SameItem_WithETag_ExactlyOneSucceeds
- **What:** Read item to get ETag, then 50 threads all try to patch with that same ETag
- **Why:** Tests optimistic concurrency control under contention
- **Expected:** Exactly 1 succeeds, 49 get PreconditionFailed (412)
- **Difficulty:** Medium — may be hard to implement if ETag check isn't inside the lock. If it fails, skip with reason and add divergent behaviour test.

#### Test 2.3: ConcurrentPatchAndReplace_SameItem_NoCorruption
- **What:** Half threads patch, half threads replace the same item
- **Why:** Tests mixed write operations don't cause corruption
- **Expected:** All succeed, final state is valid JSON
- **Difficulty:** Easy

#### Test 2.4: ConcurrentPatch_DifferentItems_AllSucceed
- **What:** 100 threads each patch a different item
- **Why:** Verifies no cross-item interference
- **Expected:** All 100 patches succeed, each item has correct patched value
- **Difficulty:** Easy

#### Test 2.5: ConcurrentPatch_IncrementOperation_LostUpdates
- **What:** 100 threads each increment same counter field by 1
- **Why:** Demonstrates lost update problem without ETags (important educational test)
- **Expected:** Final value < 100 because of lost updates (last-writer-wins)
- **Difficulty:** Easy — skip if increment is somehow serialised, with explanatory sister test

### Category 3: Concurrent Replace Scenarios

#### Test 3.1: ConcurrentReplaces_SameItem_AllSucceed_LastWriteWins
- **What:** 50 threads replace the same item with different content
- **Why:** Tests thread safety of replace path
- **Expected:** All succeed (200), final item has content from one of the replacements
- **Difficulty:** Easy

#### Test 3.2: ConcurrentReplaces_SameItem_WithETag_ExactlyOneSucceeds
- **What:** Read item, then 50 threads all try to replace with the original ETag
- **Why:** Tests optimistic concurrency for replace operations under contention
- **Expected:** Exactly 1 succeeds, 49 get PreconditionFailed (412)
- **Difficulty:** Medium

#### Test 3.3: ConcurrentReplaces_DifferentItems_AllSucceed
- **What:** 100 threads each replace a different item
- **Why:** Verifies no cross-item interference during replaces
- **Expected:** All 100 succeed
- **Difficulty:** Easy

### Category 4: Concurrent Upsert Edge Cases

#### Test 4.1: ConcurrentUpserts_ItemDoesntExist_AllSucceed_OneCreates
- **What:** 50 threads upsert the same non-existent item simultaneously
- **Why:** Tests the create-or-update decision under race conditions
- **Expected:** All succeed — first one creates (201), rest update (200)
- **Difficulty:** Easy

#### Test 4.2: ConcurrentUpserts_WithETag_ExactlyOneSucceeds
- **What:** Read item to get ETag, then 50 threads all try to upsert with that ETag
- **Why:** Tests ETag-protected upsert under contention
- **Expected:** Exactly 1 succeeds, 49 get PreconditionFailed (412)
- **Difficulty:** Medium

#### Test 4.3: ConcurrentUpserts_DifferentPartitionKeys_AllSucceed
- **What:** 100 threads each upsert to a different partition key
- **Why:** Tests cross-partition-key independence
- **Expected:** All 100 succeed
- **Difficulty:** Easy

### Category 5: Transactional Batch Under Concurrency

#### Test 5.1: ConcurrentBatch_AndDirectCrud_NoCorruption
- **What:** Run a transactional batch while other threads do direct CRUD on same partition
- **Why:** Tests batch isolation under external concurrent operations
- **Expected:** Batch completes atomically (all-or-nothing). Direct CRUD operations either see pre-batch or post-batch state.
- **Difficulty:** Hard — snapshot/restore is not atomic. If batch isolation breaks, skip with detailed reason.

#### Test 5.2: ConcurrentBatches_SamePartition_BothSucceed
- **What:** Two transactional batches operating on different items within the same partition key
- **Why:** Tests batch-level concurrency
- **Expected:** Both batches succeed, all items created
- **Difficulty:** Medium

#### Test 5.3: ConcurrentBatch_RollbackPreservesState
- **What:** One batch designed to fail (e.g., duplicate create) while concurrent reads verify state
- **Why:** Tests that rollback doesn't leak intermediate state to readers
- **Expected:** Readers never see partially-applied batch state
- **Difficulty:** Hard — RestoreSnapshot isn't atomic (clear + rewrite has a window). May need to skip with detailed reason.

#### Test 5.4: TransactionalBatch_Rollback_RestoresTimestamps
- **What:** Create item, start batch that modifies it then fails, verify _ts is restored
- **Why:** BUG: RestoreSnapshot doesn't restore _timestamps — this test will FAIL (red phase)
- **Expected (before fix):** FAIL — _ts reflects the failed batch operation
- **Expected (after fix):** _ts is restored to pre-batch value
- **Difficulty:** Medium — requires reading _ts from the document after rollback

### Category 6: Concurrent Change Feed Operations

#### Test 6.1: ConcurrentChangeFeedRead_WhileWriting_NoMissedEntries
- **What:** 10 writers continuously create items while 5 readers poll the change feed
- **Why:** Tests change feed completeness under concurrent writes
- **Expected:** After writes complete and all readers finish, union of all read items covers all written items
- **Difficulty:** Medium

#### Test 6.2: ConcurrentChangeFeed_DeleteTombstones_WhileDeleting
- **What:** Create items, then concurrently delete them while reading change feed
- **Why:** Tests tombstone recording under concurrent deletes
- **Expected:** All deletes produce tombstones in the change feed
- **Difficulty:** Medium

#### Test 6.3: ConcurrentChangeFeedProcessors_SameContainer
- **What:** Start two change feed processors on the same container
- **Why:** Tests processor isolation — each should see all changes independently
- **Expected:** Both processors receive all changes
- **Difficulty:** Medium

### Category 7: Concurrent Operations Across Partitions

#### Test 7.1: ConcurrentOperations_DifferentPartitions_FullyIndependent
- **What:** 10 partition keys × 10 concurrent operations each (create, read, upsert, delete mix)
- **Why:** Tests that operations on different partitions don't interfere
- **Expected:** All operations succeed, each partition has correct final state
- **Difficulty:** Easy

#### Test 7.2: CrossPartitionQuery_DuringConcurrentWrites
- **What:** Run cross-partition queries while concurrent writes add items
- **Why:** Tests query snapshot consistency during concurrent modifications
- **Expected:** Query returns some consistent set (not necessarily all items if writes interleave)
- **Difficulty:** Easy

### Category 8: Unique Key Constraint Under Concurrency

#### Test 8.1: ConcurrentCreates_UniqueKeyViolation_ExactlyOneSucceeds
- **What:** Container with unique key on /name. 50 threads create items with different IDs but same /name
- **Why:** Tests that the unique key lock actually prevents violations
- **Expected:** Exactly 1 succeeds, 49 get Conflict (409)
- **Difficulty:** Medium

#### Test 8.2: ConcurrentUpserts_UniqueKeyViolation_Handled
- **What:** Container with unique key. 50 threads upsert items that would violate uniqueness
- **Why:** Tests upsert path's unique key protection under load
- **Expected:** Only non-violating upserts succeed
- **Difficulty:** Medium

### Category 9: Concurrent Stream API Operations

#### Test 9.1: ConcurrentCreateItemStream_AllSucceed
- **What:** 100 concurrent CreateItemStreamAsync with different IDs
- **Why:** Tests thread safety of stream API create path
- **Expected:** All return Created (201)
- **Difficulty:** Easy

#### Test 9.2: ConcurrentUpsertItemStream_SameItem
- **What:** 50 concurrent UpsertItemStreamAsync on same item
- **Why:** Tests thread safety of stream API upsert path
- **Expected:** All return OK (200)
- **Difficulty:** Easy

#### Test 9.3: ConcurrentPatchItemStream_SameItem
- **What:** 50 concurrent PatchItemStreamAsync on same item
- **Why:** Tests patch stream path (has ZERO lock protection — potential issue)
- **Expected:** All return OK (200), no corruption
- **Difficulty:** Easy

#### Test 9.4: ConcurrentDeleteItemStream_SameItem
- **What:** 50 concurrent DeleteItemStreamAsync on same item
- **Why:** Tests stream delete path thread safety
- **Expected:** Exactly 1 returns NoContent (204), rest return NotFound (404) — or all return 204 depending on emulator behaviour. Write to document actual behaviour.
- **Difficulty:** Easy

### Category 10: ReadMany Under Concurrency

#### Test 10.1: ConcurrentReadMany_WhileWriting
- **What:** Continuously ReadMany while other threads create/upsert items
- **Why:** ReadMany reads multiple items — tests consistency during concurrent writes
- **Expected:** ReadMany always returns valid items (may be stale but not corrupted)
- **Difficulty:** Easy

### Category 11: Container/Database Level Concurrency

#### Test 11.1: ConcurrentContainerCreation_SameDatabase
- **What:** 50 threads call GetContainer or CreateContainerAsync for different container names
- **Why:** Tests ConcurrentDictionary-based container management
- **Expected:** All containers created without error
- **Difficulty:** Easy

#### Test 11.2: ConcurrentDatabaseCreation
- **What:** 50 threads call GetDatabase or CreateDatabaseAsync for different database names
- **Why:** Tests ConcurrentDictionary-based database management
- **Expected:** All databases created without error
- **Difficulty:** Easy

### Category 12: Stress / Chaos Tests

#### Test 12.1: HighContention_MixedOperations_NoCorruption
- **What:** 200 threads doing random CRUD (create, read, upsert, replace, patch, delete) on a pool of 50 items
- **Why:** Ultimate thread-safety stress test
- **Expected:** No exceptions other than expected 404/409/412, final state is consistent (item count == items in _items, all items have valid ETags and timestamps)
- **Difficulty:** Medium

#### Test 12.2: HighContention_WithUniqueKeys_NoViolations
- **What:** Same as 12.1 but with unique key constraints
- **Why:** Tests that unique key enforcement holds under extreme concurrency
- **Expected:** No unique key violations in the final state
- **Difficulty:** Medium

---

## Part C: Implementation Order (TDD)

### Phase 1: Bug Fix — Timestamp Snapshot Restoration (RED-GREEN-REFACTOR)
1. [ ] Write Test 5.4: `TransactionalBatch_Rollback_RestoresTimestamps` — expect FAIL (RED)
2. [ ] Fix `SnapshotTimestamps()` and `RestoreSnapshot()` in InMemoryContainer.cs (GREEN)
3. [ ] Refactor if needed

### Phase 2: Easy Concurrency Tests (All expected to pass immediately — GREEN)
4. [ ] Test 1.1: `ConcurrentDeletes_SameItem_OnlyOneSeesItem`
5. [ ] Test 1.2: `ConcurrentDeleteAndRead_ItemDisappearsCleanly`
6. [ ] Test 2.1: `ConcurrentPatches_SameItem_AllSucceed_LastWriteWins`
7. [ ] Test 2.4: `ConcurrentPatch_DifferentItems_AllSucceed`
8. [ ] Test 3.1: `ConcurrentReplaces_SameItem_AllSucceed_LastWriteWins`
9. [ ] Test 3.3: `ConcurrentReplaces_DifferentItems_AllSucceed`
10. [ ] Test 4.1: `ConcurrentUpserts_ItemDoesntExist_AllSucceed_OneCreates`
11. [ ] Test 4.3: `ConcurrentUpserts_DifferentPartitionKeys_AllSucceed`
12. [ ] Test 7.1: `ConcurrentOperations_DifferentPartitions_FullyIndependent`
13. [ ] Test 7.2: `CrossPartitionQuery_DuringConcurrentWrites`
14. [ ] Test 9.1: `ConcurrentCreateItemStream_AllSucceed`
15. [ ] Test 9.2: `ConcurrentUpsertItemStream_SameItem`
16. [ ] Test 9.3: `ConcurrentPatchItemStream_SameItem`
17. [ ] Test 9.4: `ConcurrentDeleteItemStream_SameItem`
18. [ ] Test 10.1: `ConcurrentReadMany_WhileWriting`
19. [ ] Test 11.1: `ConcurrentContainerCreation_SameDatabase`
20. [ ] Test 11.2: `ConcurrentDatabaseCreation`

### Phase 3: Medium Difficulty Tests (Some may need implementation fixes)
21. [ ] Test 1.3: `ConcurrentDeleteAndCreate_SameId_NoCorruption`
22. [ ] Test 1.4: `ConcurrentDeleteAndUpsert_SameItem_UpsertRevives`
23. [ ] Test 2.3: `ConcurrentPatchAndReplace_SameItem_NoCorruption`
24. [ ] Test 2.5: `ConcurrentPatch_IncrementOperation_LostUpdates`
25. [ ] Test 4.2: `ConcurrentUpserts_WithETag_ExactlyOneSucceeds`
26. [ ] Test 2.2: `ConcurrentPatches_SameItem_WithETag_ExactlyOneSucceeds`
27. [ ] Test 3.2: `ConcurrentReplaces_SameItem_WithETag_ExactlyOneSucceeds`
28. [ ] Test 5.2: `ConcurrentBatches_SamePartition_BothSucceed`
29. [ ] Test 6.1: `ConcurrentChangeFeedRead_WhileWriting_NoMissedEntries`
30. [ ] Test 6.2: `ConcurrentChangeFeed_DeleteTombstones_WhileDeleting`
31. [ ] Test 6.3: `ConcurrentChangeFeedProcessors_SameContainer`
32. [ ] Test 8.1: `ConcurrentCreates_UniqueKeyViolation_ExactlyOneSucceeds`
33. [ ] Test 8.2: `ConcurrentUpserts_UniqueKeyViolation_Handled`

### Phase 4: Hard Tests (May need skipping with divergent behaviour tests)
34. [ ] Test 5.1: `ConcurrentBatch_AndDirectCrud_NoCorruption`
35. [ ] Test 5.3: `ConcurrentBatch_RollbackPreservesState`

### Phase 5: Stress Tests
36. [ ] Test 12.1: `HighContention_MixedOperations_NoCorruption`
37. [ ] Test 12.2: `HighContention_WithUniqueKeys_NoViolations`

### Phase 6: Documentation & Release
38. [ ] Update wiki Known-Limitations.md (if any new limitations discovered)
39. [ ] Update wiki Features.md (document concurrency guarantees)
40. [ ] Update wiki Feature-Comparison-With-Alternatives.md (concurrency row if relevant)
41. [ ] Update README.md test count
42. [ ] Increment version 2.0.4 → 2.0.5 in .csproj
43. [ ] Git commit, tag v2.0.5, push, push tags

---

## Part D: Skip Criteria

If a test's expected behaviour proves too difficult to implement (would require fundamental architectural changes to the locking strategy), it should be:

1. **Skipped** with `[Fact(Skip = "...detailed reason...")]`
2. **Sister test** created with `_DivergentBehaviour` suffix showing actual emulator behaviour
3. **Sister test** should have inline comments explaining:
   - What real Cosmos DB does
   - What the emulator does instead
   - Why the implementation is difficult
   - What the user should be aware of

### Likely Skip Candidates:
- **Test 5.3** (`ConcurrentBatch_RollbackPreservesState`): RestoreSnapshot() does Clear() + rewrite which is not atomic. Concurrent readers can see empty state during the window. Fixing this would require a completely different snapshot strategy (e.g., copy-on-write or a global read-write lock).
- **Test 2.2** (`ConcurrentPatches_SameItem_WithETag_ExactlyOneSucceeds`): ETag check happens before lock acquisition. Under extreme concurrency, multiple threads could pass ETag check before any thread writes. This won't cause exactly-one semantics. The sister test should show how many can succeed.
- **Test 5.1** (`ConcurrentBatch_AndDirectCrud_NoCorruption`): Batch execution isn't truly isolated from non-batch operations — no global lock prevents direct CRUD from interleaving with batch operations.

---

## Part E: Test File Organisation

All new tests go in: `tests/CosmosDB.InMemoryEmulator.Tests/ConcurrencyTests.cs`

New test classes to add (appended after existing classes):
- `ConcurrentDeleteTests` — Category 1
- `ConcurrentPatchTests` — Category 2
- `ConcurrentReplaceTests` — Category 3
- `ConcurrentUpsertEdgeCaseTests` — Category 4
- `ConcurrentTransactionalBatchTests` — Category 5
- `ConcurrentChangeFeedTests` — Category 6
- `ConcurrentCrossPartitionTests` — Category 7
- `ConcurrentUniqueKeyTests` — Category 8
- `ConcurrentStreamApiTests` — Category 9
- `ConcurrentReadManyTests` — Category 10
- `ConcurrentContainerDatabaseTests` — Category 11
- `ConcurrencyStressTests` — Category 12
