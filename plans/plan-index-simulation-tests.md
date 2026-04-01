# IndexSimulationTests.cs — Deep Dive Plan

## Current State

**File:** `tests/CosmosDB.InMemoryEmulator.Tests/IndexSimulationTests.cs`  
**Current version:** 2.0.4  
**Target version:** 2.0.5

### Existing Tests (10)

| # | Test Name | Status | What it covers |
|---|-----------|--------|----------------|
| 1 | `DefaultIndexingPolicy_HasAutomaticIndexing` | ✅ Pass | Default policy: Automatic=true, Mode=Consistent |
| 2 | `SetIndexingPolicy_UpdatesPolicy` | ✅ Pass | Can set Automatic=false, Mode=None |
| 3 | `IndexingPolicy_DefaultIncludesAllPaths` | ✅ Pass | Default IncludedPaths has `/*` |
| 4 | `ExcludedPath_StillAllowsPointReads` | ✅ Pass | Excluded path doesn't block point reads |
| 5 | `ExcludedPath_QueriesStillWorkButLogWarning` | ✅ Pass | Queries on excluded paths still return results |
| 6 | `CompositeIndexes_CanBeConfigured` | ✅ Pass | Can add composite index with 2 paths |
| 7 | `OrderBy_WithCompositeIndex_ReturnsCorrectOrder` | ✅ Pass | Multi-field ORDER BY works with composite index |
| 8 | `SpatialIndexes_CanBeConfigured` | ✅ Pass | Can add spatial path |
| 9 | `ExcludedPath_QueryReturnsHigherRequestCharge` | ⏭️ Skip | RU model not implemented |
| 10 | `BehavioralDifference_RequestChargeIsAlwaysSynthetic` | ✅ Pass | Sister: RU is always 1.0 |

---

## Bugs Found

### BUG-1: InMemoryDatabase.CreateContainerAsync(ContainerProperties) discards IndexingPolicy

**Location:** `src/CosmosDB.InMemoryEmulator/InMemoryDatabase.cs` lines 116-122

**Problem:** When `CreateContainerAsync(ContainerProperties)` is called, only `Id` and `PartitionKeyPath` are extracted. The full `ContainerProperties` object (including IndexingPolicy, UniqueKeyPolicy, ComputedProperties, ConflictResolutionPolicy, DefaultTimeToLive, etc.) is discarded. The `InMemoryContainer(ContainerProperties)` constructor exists but is never called from the database path.

**Same bug in:** `CreateContainerIfNotExistsAsync(ContainerProperties)` lines 85-91.

**Fix:** Change both methods to use `new InMemoryContainer(containerProperties)` instead of extracting id/pkPath and calling the string overload.

**Severity:** Medium — Affects anyone who creates containers via Database with custom ContainerProperties. UniqueKeyPolicy, ComputedProperties, IndexingPolicy, DefaultTimeToLive, ConflictResolutionPolicy all silently lost.

**Note:** This bug already partially manifests. The existing test `CreateContainerCustomIndexingTests.CreateContainerAsync_WithCustomIndexingPolicy_CreatesSuccessfully` in `CosmosClientAndDatabaseTests.cs` only checks `StatusCode` and `Id` — it never verifies that the IndexingPolicy was actually preserved on the container. Similarly, `ContainerManagementTests.ConflictResolution_EmulatorBehavior_PolicyStoredButNotEnforced` works only because it uses `ReplaceContainerAsync` after creation to store the policy.

### BUG-2: BuildContainerResponse creates new ContainerProperties, losing settings

**Location:** `src/CosmosDB.InMemoryEmulator/InMemoryDatabase.cs` lines 223-230

**Problem:** `BuildContainerResponse` creates `new ContainerProperties(container.Id, partitionKeyPath)` for `response.Resource`, which discards all custom settings. The response should reflect the actual container's properties.

**Fix:** When the container is an `InMemoryContainer`, read its actual `_containerProperties` (or expose via a method/property) to return in the response.

### BUG-3: FakeCosmosHandler returns hardcoded indexing metadata

**Location:** `src/CosmosDB.InMemoryEmulator/FakeCosmosHandler.cs` line 1201

**Problem:** The collection metadata always returns a hardcoded indexing policy (`consistent`, `automatic=true`, `includedPaths=[/*]`, `excludedPaths=[/_etag/?]`) rather than reflecting the actual container's IndexingPolicy.

**Fix:** Read the container's actual IndexingPolicy when building the metadata response. This is lower priority since FakeCosmosHandler is a thin HTTP wrapper, but mismatches could confuse tests that inspect metadata.

---

## Missing Test Coverage

### Category A: Index Policy Roundtrip & Persistence

| # | Test Name | Description | Difficulty |
|---|-----------|-------------|------------|
| A1 | `IndexingPolicy_SurvivesReadContainerAsync` | Set custom policy, call ReadContainerAsync, verify policy in response | Easy |
| A2 | `IndexingPolicy_PreservedWhenCreatedViaDatabase` | Create container via `db.CreateContainerAsync(ContainerProperties)` with custom IndexingPolicy. Verify the IndexingPolicy is preserved on the container. **Exposes BUG-1.** | Easy (after fix) |
| A3 | `IndexingPolicy_PreservedWhenCreatedViaDatabase_IfNotExists` | Same as A2 but via `CreateContainerIfNotExistsAsync`. **Exposes BUG-1.** | Easy (after fix) |
| A4 | `IndexingPolicy_UpdatedViaReplaceContainerAsync` | Replace container with new policy, verify IndexingPolicy is updated and readable. (Partially covered in `ContainerManagementTests` but not in IndexSimulationTests — add here for completeness.) | Easy |
| A5 | `IndexingPolicy_DefaultExcludedPaths_Empty` | Verify default ExcludedPaths collection is empty (real Cosmos defaults to excluding `/_etag/?`, our stub doesn't — document divergence if applicable) | Easy |
| A6 | `BuildContainerResponse_ReflectsActualContainerProperties` | After creating via database, verify `response.Resource` IndexingPolicy matches what was passed in. **Exposes BUG-2.** | Easy (after fix) |

### Category B: IndexingMode Behavioral Differences

| # | Test Name | Description | Difficulty |
|---|-----------|-------------|------------|
| B1 | `IndexingMode_None_QueriesStillWork_Skipped` | SKIP: In real Cosmos DB, IndexingMode.None means queries fail unless `EnableScanInQuery=true` in FeedOptions. Our emulator ignores IndexingMode entirely. | Skip + sister |
| B1s | `BehavioralDifference_IndexingMode_None_QueriesStillWork` | Sister: demonstrate that queries work regardless of IndexingMode.None setting | Easy |
| B2 | `IndexingMode_Lazy_IsAccepted` | Verify IndexingMode.Lazy can be set and read back | Easy |
| B3 | `IndexingMode_Lazy_QueriesReturnResults_Skipped` | SKIP: In real Cosmos DB, Lazy mode indexes asynchronously and may have stale results. Our emulator returns results immediately. | Skip + sister |
| B3s | `BehavioralDifference_IndexingMode_Lazy_QueriesAreImmediate` | Sister: demonstrate that queries always return all matching results regardless of Lazy mode | Easy |

### Category C: Included/Excluded Path Edge Cases

| # | Test Name | Description | Difficulty |
|---|-----------|-------------|------------|
| C1 | `ExcludedPath_EtagPath_CanBeAdded` | Add the real Cosmos default `/_etag/?` exclusion, verify queries still work | Easy |
| C2 | `ExcludedPath_NestedPath_QueriesStillWork` | Exclude `/nested/deep/*`, query on `c.nested.deep.value`, verify results returned | Easy |
| C3 | `ExcludedPath_AllPaths_QueriesStillWork_Skipped` | SKIP: In real Cosmos, excluding `/*` with no included paths means no queries work (only point reads). Our emulator ignores this. | Skip + sister |
| C3s | `BehavioralDifference_ExcludedAllPaths_QueriesStillWork` | Sister: exclude `/*`, verify queries still return results | Easy |
| C4 | `IncludedPaths_CanBeCleared` | Clear included paths, verify policy reflects empty collection | Easy |
| C5 | `IncludedPaths_SpecificPath_CanBeSet` | Set included paths to only `/name/?`, verify stored correctly | Easy |
| C6 | `MultipleExcludedPaths_AllStoredCorrectly` | Add 3 excluded paths, verify all are stored | Easy |
| C7 | `ExcludedPath_WildcardVariations_Stored` | Test `/*`, `/name/?`, `/name/*`, `/name/[]/?` path formats are stored | Easy |
| C8 | `ExcludedPath_PointReadsWorkRegardless` | Exclude all paths, point reads still work (already partially covered, extend to Mode=None) | Easy |

### Category D: Composite Index Edge Cases

| # | Test Name | Description | Difficulty |
|---|-----------|-------------|------------|
| D1 | `CompositeIndex_ThreePaths_CanBeConfigured` | Add composite index with 3 paths | Easy |
| D2 | `CompositeIndex_MultipleSets_CanBeConfigured` | Add 2 separate composite index configurations | Easy |
| D3 | `OrderBy_WithoutCompositeIndex_StillWorks_Skipped` | SKIP: In real Cosmos DB, multi-field ORDER BY without a composite index fails with an error. Our emulator doesn't enforce this. | Skip + sister |
| D3s | `BehavioralDifference_OrderBy_MultiField_WorksWithoutCompositeIndex` | Sister: multi-field ORDER BY works without composite index | Easy |
| D4 | `OrderBy_CompositeIndex_MixedSortOrders` | Test ASC/DESC on different fields with composite index | Easy |
| D5 | `CompositeIndex_AllDescending` | Add composite with all paths Descending, verify ORDER BY DESC, DESC works | Easy |

### Category E: Spatial Index Edge Cases

| # | Test Name | Description | Difficulty |
|---|-----------|-------------|------------|
| E1 | `SpatialIndex_WithSpatialTypes_CanBeConfigured` | Add SpatialPath with specific SpatialTypes (Point, Polygon, etc.) | Easy |
| E2 | `SpatialIndex_MultiplePaths_CanBeConfigured` | Add spatial indexes on multiple paths | Easy |
| E3 | `SpatialQueries_WorkWithoutSpatialIndex_Skipped` | SKIP: In real Cosmos, spatial queries require spatial index. Our emulator doesn't enforce this. | Skip + sister |
| E3s | `BehavioralDifference_SpatialQueries_WorkWithoutSpatialIndex` | Sister: spatial queries function regardless of index config | Easy |

### Category F: Unique Key Policy Interaction

| # | Test Name | Description | Difficulty |
|---|-----------|-------------|------------|
| F1 | `UniqueKeyPolicy_PreservedWhenCreatedViaDatabase` | Create container via database with UniqueKeyPolicy. Verify enforcement. **Exposes BUG-1.** | Easy (after fix) |
| F2 | `UniqueKeyPolicy_AndIndexingPolicy_BothPreserved` | Set both policies, verify both are preserved after database creation | Easy (after fix) |

### Category G: FakeCosmosHandler Index Metadata

| # | Test Name | Description | Difficulty |
|---|-----------|-------------|------------|
| G1 | `FakeCosmosHandler_ReturnsIndexingPolicyInMetadata` | Read collection metadata via handler, verify indexing policy shape | Already exists in FakeCosmosHandlerTests (line 999) — skip |
| G2 | `FakeCosmosHandler_IndexMetadata_ReflectsContainerPolicy_Skipped` | SKIP: FakeCosmosHandler hardcodes index metadata rather than reading from container. Fixing requires significant refactoring. | Skip + sister |
| G2s | `BehavioralDifference_FakeCosmosHandler_IndexMetadataIsHardcoded` | Sister: verify the hardcoded metadata shape | Easy |

---

## Implementation Plan (TDD: Red → Green → Refactor)

### Phase 1: Write failing tests that expose BUG-1 and BUG-2

1. **A2** — `IndexingPolicy_PreservedWhenCreatedViaDatabase` (RED — will fail due to BUG-1)
2. **A3** — `IndexingPolicy_PreservedWhenCreatedViaDatabase_IfNotExists` (RED — will fail due to BUG-1)
3. **A6** — `BuildContainerResponse_ReflectsActualContainerProperties` (RED — will fail due to BUG-2)
4. **F1** — `UniqueKeyPolicy_PreservedWhenCreatedViaDatabase` (RED — will fail due to BUG-1)

### Phase 2: Fix BUG-1 and BUG-2

5. Fix `InMemoryDatabase.CreateContainerAsync(ContainerProperties)` — use `new InMemoryContainer(containerProperties)` 
6. Fix `InMemoryDatabase.CreateContainerIfNotExistsAsync(ContainerProperties)` — same approach
7. Fix `BuildContainerResponse` — read actual ContainerProperties from InMemoryContainer
8. Run tests A2, A3, A6, F1 → GREEN

### Phase 3: Write remaining roundtrip/persistence tests

9. **A1** — `IndexingPolicy_SurvivesReadContainerAsync`
10. **A4** — `IndexingPolicy_UpdatedViaReplaceContainerAsync`
11. **A5** — `IndexingPolicy_DefaultExcludedPaths_Empty`
12. **F2** — `UniqueKeyPolicy_AndIndexingPolicy_BothPreserved`

### Phase 4: Write IndexingMode behavioral difference tests

13. **B1** (skipped) + **B1s** (sister) — IndexingMode.None queries
14. **B2** — IndexingMode.Lazy acceptance
15. **B3** (skipped) + **B3s** (sister) — IndexingMode.Lazy stale results

### Phase 5: Write included/excluded path edge case tests

16. **C1** — `/_etag/?` exclusion
17. **C2** — Nested path exclusion
18. **C3** (skipped) + **C3s** (sister) — Exclude all paths
19. **C4** — Clear included paths
20. **C5** — Specific included path
21. **C6** — Multiple excluded paths
22. **C7** — Wildcard path variations
23. **C8** — Point reads with Mode=None

### Phase 6: Write composite index edge case tests

24. **D1** — Three-path composite index
25. **D2** — Multiple composite index sets
26. **D3** (skipped) + **D3s** (sister) — ORDER BY without composite index
27. **D4** — Mixed sort orders
28. **D5** — All descending composite

### Phase 7: Write spatial index edge case tests

29. **E1** — Spatial types configuration
30. **E2** — Multiple spatial paths
31. **E3** (skipped) + **E3s** (sister) — Spatial queries without spatial index

### Phase 8: FakeCosmosHandler

32. **G2** (skipped) + **G2s** (sister) — Hardcoded metadata divergence
33. Decide whether to fix BUG-3 (FakeCosmosHandler hardcoded metadata) or leave as known limitation

### Phase 9: Documentation updates

34. Update `Known-Limitations.md`:
    - Update "IndexingPolicy | ⚠️ Stub" row with more detail about what's stored vs enforced
    - Add behavioral difference entries for IndexingMode.None and IndexingMode.Lazy not affecting queries
    - Add behavioral difference entry for composite index not required for multi-field ORDER BY
    - Add behavioral difference entry for spatial index not required for spatial queries

35. Update `Features.md`:
    - Add/update IndexingPolicy section describing what's stored
    - Note that composite indexes, spatial indexes, included/excluded paths are accepted but not enforced

36. Update `Feature-Comparison-With-Alternatives.md`:
    - Update row 87 (Custom index policy) with detail about BUG-1 fix
    - Update row 120 (IndexingPolicy enforcement) if applicable
    - Note that composite index enforcement is different

37. Update `README.md` if any index-related features are mentioned (currently none — may add a line)

### Phase 10: Version, tag, push

38. Bump version from `2.0.4` to `2.0.5` in `CosmosDB.InMemoryEmulator.csproj`
39. `git add -A`
40. `git commit -m "v2.0.5: Fix ContainerProperties preservation in Database.CreateContainerAsync, expand IndexSimulationTests"`
41. `git tag v2.0.5`
42. `git push; git push --tags`
43. Push wiki changes: `cd wiki; git add -A; git commit -m "v2.0.5: ..."; git push`

---

## Test Count Summary

| Category | New Tests | Skipped | Sisters | Total |
|----------|-----------|---------|---------|-------|
| A: Roundtrip & Persistence | 6 | 0 | 0 | 6 |
| B: IndexingMode Behavior | 1 | 2 | 2 | 5 |
| C: Included/Excluded Paths | 7 | 1 | 1 | 9 |
| D: Composite Index | 4 | 1 | 1 | 6 |
| E: Spatial Index | 2 | 1 | 1 | 4 |
| F: UniqueKey Interaction | 2 | 0 | 0 | 2 |
| G: FakeCosmosHandler | 0 | 1 | 1 | 2 |
| **Total** | **22** | **6** | **6** | **34** |

Combined with existing 10 tests → **44 tests total** in IndexSimulationTests.cs.

---

## Bug Fix Summary

| Bug | Files Changed | Risk |
|-----|---------------|------|
| BUG-1: ContainerProperties discarded | `InMemoryDatabase.cs` (2 methods) | Low — adds a code path, existing string-based creation unchanged |
| BUG-2: BuildContainerResponse loses settings | `InMemoryDatabase.cs` (1 method) | Low — response now reflects real state |
| BUG-3: FakeCosmosHandler hardcoded metadata | `FakeCosmosHandler.cs` (1 method) | Medium — may break existing FakeCosmosHandlerTests that assert hardcoded values. Defer if risky; skip test + document. |

---

## Decision Log

- **BUG-3 (FakeCosmosHandler):** Will assess difficulty during Phase 8. If the fix breaks existing FakeCosmosHandler tests or requires significant refactoring, skip the test with detailed reason and document as known limitation instead.
- **Request charge / RU model:** Already covered by existing skip+sister pair. No additional work needed.
- **Vector index policy:** Out of scope for IndexSimulationTests — covered by FullTextSearchTests and VectorSearchTests.
- **Full-text index policy:** Out of scope — covered by FullTextSearchTests.
