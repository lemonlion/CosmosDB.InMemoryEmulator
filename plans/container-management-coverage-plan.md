# Container Management Test Coverage & Bug Fix Plan

**Date:** 2026-04-01  
**Current version:** 2.0.4  
**Target version:** 2.0.5  
**Approach:** TDD — red-green-refactor. Write test first, watch it fail, fix code, verify green.

---

## Summary

Deep dive into `ContainerManagementTests.cs` and all related container management code
(`InMemoryContainer.cs`, `InMemoryDatabase.cs`) revealed **4 bugs** and **~25 missing tests**
across container lifecycle, property preservation, throughput, change feed clearing,
delete-all-by-partition-key, and database-level container querying.

---

## Bugs Found

### BUG-1: DeleteContainerAsync does not clear change feed ⭐ HIGH PRIORITY

**Location:** `InMemoryContainer.cs` L1493-1514 (both typed and stream variants)  
**Problem:** `DeleteContainerAsync` clears `_items`, `_etags`, `_timestamps` and invokes
`OnDeleted`, but does NOT clear `_changeFeed`. Compare with `ClearItems()` at L176-178
which correctly clears `_changeFeed` under lock.  
**Impact:** If a container is deleted and then items are re-added, the change feed still
contains entries from the pre-deletion era, contaminating the new container's history.  
**Fix:** Add `lock (_changeFeedLock) { _changeFeed.Clear(); }` to both `DeleteContainerAsync`
and `DeleteContainerStreamAsync`, matching the pattern in `ClearItems()`.  
**Status:** [ ] Not started

### BUG-2: CreateContainerAsync(ContainerProperties) loses advanced properties

**Location:** `InMemoryDatabase.cs` L114-120  
**Problem:** The `ContainerProperties`-based overload of `CreateContainerAsync` extracts only
`Id` and `PartitionKeyPath`, then delegates to the string-based overload which creates
`new InMemoryContainer(id, pkPath)`. This discards:
- `UniqueKeyPolicy`
- `ComputedProperties`  
- `IndexingPolicy` customizations
- `DefaultTimeToLive`
- `ConflictResolutionPolicy`
- `ChangeFeedPolicy`

**Fix:** Use `new InMemoryContainer(containerProperties)` constructor instead.  
**Status:** [ ] Not started

### BUG-3: CreateContainerIfNotExistsAsync(ContainerProperties) loses advanced properties

**Location:** `InMemoryDatabase.cs` L79-92  
**Problem:** Same issue as BUG-2 — both `ContainerProperties` and `ThroughputProperties`
overloads delegate to the string-based version, losing all advanced properties.  
**Fix:** When the container is new, use `new InMemoryContainer(containerProperties)`.  
**Status:** [ ] Not started

### BUG-4: CreateContainerStreamAsync(ContainerProperties) loses advanced properties

**Location:** `InMemoryDatabase.cs` L132-146  
**Problem:** Same property loss as BUG-2/3.  
**Fix:** Use `new InMemoryContainer(containerProperties)` constructor.  
**Status:** [ ] Not started

---

## Missing Test Coverage

### Category A: Delete Container — Change Feed Clearing

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| A1 | `DeleteContainerAsync_ClearsChangeFeed` | Create items, verify change feed has entries, delete container, verify change feed is empty | [ ] |
| A2 | `DeleteContainerStreamAsync_ClearsChangeFeed` | Same as A1 but with stream variant | [ ] |
| A3 | `DeleteContainer_ThenAddItems_ChangeFeedOnlyHasNewEntries` | Delete container, add new items, verify change feed checkpoint starts fresh | [ ] |

### Category B: Database-Level Container Creation with Properties

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| B1 | `CreateContainerAsync_WithContainerProperties_PreservesUniqueKeyPolicy` | Create container via `db.CreateContainerAsync(props)` with UniqueKeyPolicy, verify enforcement works | [ ] |
| B2 | `CreateContainerAsync_WithContainerProperties_PreservesDefaultTtl` | Create container via `db.CreateContainerAsync(props)` with DefaultTimeToLive set, verify it's preserved | [ ] |
| B3 | `CreateContainerIfNotExistsAsync_WithContainerProperties_PreservesUniqueKeyPolicy` | Same as B1 but via IfNotExists variant | [ ] |
| B4 | `CreateContainerStreamAsync_WithContainerProperties_PreservesUniqueKeyPolicy` | Same as B1 but via Stream variant | [ ] |
| B5 | `CreateContainerAsync_DuplicateId_ThrowsConflict` | Create same container twice, verify 409 Conflict | [ ] |
| B6 | `CreateContainerIfNotExistsAsync_DuplicateId_ReturnsOk` | Create same container twice via IfNotExists, verify OK (not Created) | [ ] |
| B7 | `CreateContainerStreamAsync_DuplicateId_ReturnsConflict` | Create same container twice via stream, verify 409 ResponseMessage | [ ] |

### Category C: Replace Container — Property Mutation Edge Cases

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| C1 | `ReplaceContainerAsync_UpdatesDefaultTtl_ReadReflectsChange` | Replace with new TTL, read back, verify TTL changed | [ ] |
| C2 | `ReplaceContainerAsync_UpdatesIndexingPolicy_ReadReflectsChange` | Replace with new indexing policy, read back, verify persisted | [ ] |
| C3 | `ReplaceContainerAsync_ClearsComputedPropertiesCache` | Set computed properties, replace container with new ones, verify queries use new definitions | [ ] |
| C4 | `ReplaceContainerStreamAsync_PersistsMultiplePropertyChanges` | Replace via stream with TTL + indexing changes, read back, verify both updated | [ ] |
| C5 | `ReplaceContainerAsync_CannotChangePartitionKeyPath` | Replace with different PK path. Skip this test — real Cosmos returns 400 BadRequest but emulator silently accepts. Document as divergent behavior. | [ ] |

### Category D: Delete Container — Lifecycle & Database Integration

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| D1 | `DeleteContainerAsync_RemovesFromDatabase` | Create container via database, delete it, verify `GetContainerQueryIterator` no longer lists it | [ ] |
| D2 | `DeleteContainerStreamAsync_RemovesFromDatabase` | Same as D1 via stream variant | [ ] |
| D3 | `DeleteContainer_ThenRecreate_SameId_Succeeds` | Delete container, recreate with same name, verify fresh container | [ ] |
| D4 | `DeleteContainer_ClearsAllItems_VerifyViaItemCount` | Existing test but with change feed verification added | [ ] |

### Category E: Container Querying (Database-Level)

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| E1 | `GetContainerQueryIterator_ReturnsAllContainers` | Create multiple containers, verify iterator returns all of them | [ ] |
| E2 | `GetContainerQueryIterator_EmptyDatabase_ReturnsEmpty` | Query containers on empty database, verify empty result | [ ] |
| E3 | `GetContainerQueryStreamIterator_ReturnsContainers` | Same as E1 via stream variant | [ ] |
| E4 | `GetContainerQueryIterator_AfterDelete_ExcludesDeleted` | Create 3 containers, delete 1, verify iterator returns 2 | [ ] |

### Category F: Throughput — Edge Cases

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| F1 | `ReadThroughputAsync_WithRequestOptions_ReturnsThroughputProperties` | Verify `Resource` has correct throughput value | [ ] |
| F2 | `ReplaceThroughputAsync_WithThroughputProperties_AutoscaleAccepted` | Create autoscale throughput, replace, verify accepted | [ ] |
| F3 | `Database_ReadThroughputAsync_ReturnsSynthetic400` | Verify database-level throughput returns sensible default | [ ] |
| F4 | `Database_ReplaceThroughputAsync_Succeeds` | Verify database-level throughput replace doesn't throw | [ ] |

### Category G: DeleteAllItemsByPartitionKeyStreamAsync — Edge Cases

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| G1 | `DeleteAllByPK_DoesNotRecordChangeFeedTombstones` | Verify no delete tombstones in change feed (real Cosmos does record them — skip if behavior is too hard to implement, mark as divergent) | [ ] |
| G2 | `DeleteAllByPK_WithCompositePartitionKey_RemovesCorrectItems` | Use hierarchical PK, delete by PK, verify only matching items removed | [ ] |
| G3 | `DeleteAllByPK_MultipleTimes_Idempotent` | Delete same PK twice, verify second call returns OK with no error | [ ] |

### Category H: Container Properties & Metadata

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| H1 | `ReadContainerAsync_ReturnsPartitionKeyPath_Composite` | Create container with composite PK, read container, verify paths returned | [ ] |
| H2 | `ReadContainerStreamAsync_ReturnsJsonWithContainerProperties` | Read stream, deserialize JSON, verify contains id and partitionKey | [ ] |
| H3 | `Container_Scripts_Property_ReturnsNonNull` | Verify Scripts property is accessible | [ ] |
| H4 | `Container_Database_Property_ReturnsNewMockEachCall` | Verify Database property doesn't throw (existing behavior — mock per call). Add divergent behavior note: real Cosmos returns same parent reference. | [ ] |

### Category I: DefineContainer (Fluent Builder)

| # | Test Name | Description | Status |
|---|-----------|-------------|--------|
| I1 | `DefineContainer_ReturnsContainerBuilder` | Verify DefineContainer returns non-null ContainerBuilder | [ ] |

---

## Skipped Tests (Divergent Behavior Documentation)

Each skipped test should have:
1. A `[Fact(Skip = "...")]` with a clear, detailed skip reason
2. A sister `[Fact]` test showing the emulator's actual behavior with inline comments

### SKIP-1: ReplaceContainerAsync Cannot Change PartitionKeyPath (C5)

**Skip reason:** "Real Cosmos DB returns 400 BadRequest when attempting to change the partition
key path via ReplaceContainerAsync. The in-memory emulator updates _containerProperties but
the internal PartitionKeyPaths field is read-only (set in constructor), creating an
inconsistency. Fixing this would require either throwing on PK path changes or making
PartitionKeyPaths mutable. This is a known divergent behavior."

**Sister test:** `ReplaceContainer_EmulatorBehavior_AcceptsPartitionKeyPathChange_ButInternal
PathUnchanged` — shows that _containerProperties is updated but actual item routing still
uses the original PK path.

### SKIP-2: DeleteAllByPK Does Not Record Change Feed Tombstones (G1)

**Skip reason:** "Real Cosmos DB records delete tombstones in the change feed for items
deleted by DeleteAllItemsByPartitionKeyStreamAsync, visible via AllVersionsAndDeletes mode.
The in-memory emulator removes items without recording tombstones. Implementing this would
require iterating all affected items before removal and calling RecordDeleteTombstone for
each. Marking as known gap."

**Sister test:** `DeleteAllByPK_EmulatorBehavior_NoChangeFeedTombstonesRecorded` — shows that
change feed checkpoint does not advance after DeleteAllByPK.

---

## Implementation Order

TDD approach: write the test, run it red, fix the code, run it green.

### Phase 1: Bug Fixes (highest value)

1. **A1** → Write test for BUG-1 (DeleteContainerAsync doesn't clear change feed) → Red
2. Fix BUG-1 in `InMemoryContainer.cs` → Green
3. **A2** → Stream variant test → Should now also be green
4. **A3** → Post-delete fresh change feed test → Green
5. **B1** → Write test for BUG-2 (CreateContainerAsync loses UniqueKeyPolicy) → Red
6. Fix BUG-2 in `InMemoryDatabase.cs` → Green
7. **B3** → Test for BUG-3 (IfNotExists loses properties) → Red
8. Fix BUG-3 → Green
9. **B4** → Test for BUG-4 (Stream loses properties) → Red
10. Fix BUG-4 → Green
11. **B2** → TTL preservation test → Should be green given fixes

### Phase 2: Container Lifecycle (D-series)

12. **D1** → Delete removes from database
13. **D2** → Stream variant
14. **D3** → Delete then recreate
15. **D4** → Verify items + change feed cleared

### Phase 3: Container Querying (E-series)

16. **E1** → Query iterator returns all containers
17. **E2** → Empty database
18. **E3** → Stream iterator
19. **E4** → After delete excludes deleted

### Phase 4: Replace Edge Cases (C-series)

20. **C1** → TTL update persists
21. **C2** → IndexingPolicy persists
22. **C3** → Computed properties cache cleared
23. **C4** → Stream multi-property update
24. **C5/SKIP-1** → PK path change divergence (skip + sister test)

### Phase 5: Container Creation Edge Cases (B-series remaining)

25. **B5** → Duplicate ID conflict
26. **B6** → IfNotExists returns OK
27. **B7** → Stream duplicate conflict

### Phase 6: Throughput (F-series)

28. **F1** → RequestOptions throughput
29. **F2** → Autoscale throughput
30. **F3** → Database-level read throughput
31. **F4** → Database-level replace throughput

### Phase 7: DeleteAllByPK Edge Cases (G-series)

32. **G1/SKIP-2** → Change feed tombstones (skip + sister test)
33. **G2** → Composite PK delete
34. **G3** → Idempotent delete

### Phase 8: Properties & Metadata (H, I series)

35. **H1** → Composite PK in ReadContainer
36. **H2** → Stream JSON deserialization
37. **H3** → Scripts property
38. **H4** → Database property divergence
39. **I1** → DefineContainer builder

---

## Documentation Updates (After All Tests Pass)

### Wiki Updates

1. **Known-Limitations.md** — If any new divergent behaviors are found (SKIP-1, SKIP-2), add them
   to the behavioural differences section with test references.

2. **Features.md** — Update "Container Management" section to note:
   - Delete now properly clears change feed
   - CreateContainerAsync with ContainerProperties now preserves all properties
   - DeleteAllItemsByPartitionKeyStreamAsync does not record change feed tombstones (limitation)

3. **Feature-Comparison-With-Alternatives.md** — No changes expected (container management row
   already shows ✅ for all CRUD operations).

### README.md

- No changes expected unless a major new capability is added.

### Version & Release

1. Increment version in `src/CosmosDB.InMemoryEmulator/CosmosDB.InMemoryEmulator.csproj`:
   `2.0.4` → `2.0.5`
2. `git add -A`
3. `git commit -m "v2.0.5: Fix container management bugs — change feed clearing, property preservation on create"`
4. `git tag v2.0.5`
5. `git push`
6. `git push --tags`
7. Wiki: `git add -A && git commit -m "v2.0.5: Container management bug fixes — update Known Limitations and Features" && git push`

---

## Files Modified

| File | Changes |
|------|---------|
| `tests/.../ContainerManagementTests.cs` | ~25 new tests, 2 skipped tests with sister divergent behavior tests |
| `src/.../InMemoryContainer.cs` | BUG-1 fix: clear `_changeFeed` in DeleteContainerAsync/StreamAsync |
| `src/.../InMemoryDatabase.cs` | BUG-2/3/4 fix: use `InMemoryContainer(ContainerProperties)` constructor in all overloads |
| `Known-Limitations.md` (wiki) | Add entries for SKIP-1 (PK path change) and SKIP-2 (delete-all tombstones) |
| `Features.md` (wiki) | Update container management notes |
| `.csproj` | Version bump 2.0.4 → 2.0.5 |

---

## Progress Tracker

- [ ] Phase 1: Bug Fixes (tests A1-A3, B1-B4)
- [ ] Phase 2: Container Lifecycle (D1-D4)
- [ ] Phase 3: Container Querying (E1-E4)
- [ ] Phase 4: Replace Edge Cases (C1-C5)
- [ ] Phase 5: Creation Edge Cases (B5-B7)
- [ ] Phase 6: Throughput (F1-F4)
- [ ] Phase 7: DeleteAllByPK Edge Cases (G1-G3)
- [ ] Phase 8: Properties & Metadata (H1-H4, I1)
- [ ] Documentation updates
- [ ] Version bump, tag, push
