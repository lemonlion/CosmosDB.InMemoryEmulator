# DivergentBehaviorTests.cs — Deep Dive Plan

> **Status:** PLAN ONLY — nothing implemented yet  
> **Current version:** 2.0.4  
> **Target version:** 2.0.5  
> **Approach:** TDD — red-green-refactor. If behaviour is too difficult to implement, skip test with detailed reason + sister test showing actual emulator behaviour.

---

## Table of Contents

1. [Existing Coverage Audit](#1-existing-coverage-audit)
2. [Bugs / Issues Found](#2-bugs--issues-found)
3. [Missing Test Coverage](#3-missing-test-coverage)
4. [Implementation Plan](#4-implementation-plan)
5. [Documentation Updates](#5-documentation-updates)
6. [Release Steps](#6-release-steps)

---

## 1. Existing Coverage Audit

DivergentBehaviorTests.cs currently contains **6 test classes** (5 skipped, 1 resolved):

| ID | Class | Status | Gap |
|----|-------|--------|-----|
| M7 | `CrossPartitionAggregateTests` | ⏭️ Skipped | Aggregates multiplied when PartitionKeyRangeCount > 1 |
| M9 | `SubqueryOrderByTests` | ✅ Resolved | Subquery ORDER BY + OFFSET/LIMIT works |
| L2 | `ArrayFunctionLiteralTests` | ⏭️ Skipped | Array functions reject literal arrays |
| L3 | `GetCurrentDateTimeConsistencyTests` | ⏭️ Skipped | GetCurrentDateTime() per-row not per-query |
| L4 | `LinqQueryableOptionsTests` | ⏭️ Skipped | linqSerializerOptions/continuationToken ignored |
| L6 | `UndefinedNullOrderByTests` | ⏭️ Skipped | undefined vs null not distinguished in ORDER BY |

### Relationship to BehavioralDifferenceTests.cs

- `BehavioralDifferenceTests.cs` documents **actual divergent behaviour** with passing tests that assert the emulator's current behaviour.
- `DivergentBehaviorTests.cs` documents **ideal Cosmos behaviour** that the emulator can't yet match, using **skipped tests** + sister tests.
- There are also `*DivergentBehaviorTests` classes scattered across other test files (ChangeFeedTests, FeedRangeTests, FullTextSearchTests, etc.).

**Finding:** DivergentBehaviorTests.cs is meant to be the **centralised** divergent behavior registry, but many divergences are documented elsewhere. This plan will:
1. Add MISSING divergence coverage to DivergentBehaviorTests.cs
2. Not move existing tests from other files (that would be scope creep)
3. Cross-reference the wiki Known Limitations for consistency

---

## 2. Bugs / Issues Found

### Bug 1: M9 (SubqueryOrderByTests) marked "RESOLVED" but no sister test
- **Issue:** The resolved test `Subquery_WithOrderByAndLimit_ShouldReturnOrderedSubset` is a passing green test, which is correct. But other gap IDs follow the pattern of having a **skipped test** (expected Cosmos behaviour) plus a **sister test** (actual emulator behaviour). M9 broke this pattern when it was resolved — it only has the passing test now, which is fine, but it should be clearly marked differently from the still-open gaps.
- **Fix:** No code fix needed. Just update the section comment from "RESOLVED" to include the resolution date/version.
- **Severity:** Cosmetic/documentation only

### Bug 2: Missing `gap-fix-tdd-plan.md` reference
- **Issue:** The file header comment says "See gap-fix-tdd-plan.md for details on each gap ID" but that file doesn't exist.
- **Fix:** Update the comment to reference this plan file (`divergent-behavior-deep-dive-plan.md`).
- **Severity:** Documentation only

### Bug 3: No sister tests for skipped gap tests
- **Issue:** The skipped tests (M7, L2, L3, L4, L6) describe the expected real Cosmos behaviour, but none have a sister "green" test showing the actual emulator behaviour. The file comment says "Each test documents a gap... with a sister skipped test showing the expected real Cosmos DB behavior" — meaning the skipped test IS the Cosmos-behaviour test, and there should be a passing test next to it showing what the emulator actually does.
- **Fix:** Add passing sister tests for each skipped gap showing actual emulator behaviour. These are the inline-commented "divergent" tests.
- **Severity:** Medium — this is a core pattern of the file that's incomplete

---

## 3. Missing Test Coverage

### 3.1 Gaps already in DivergentBehaviorTests.cs that need sister tests

| ID | Missing Sister Test | Description |
|----|-------------------|-------------|
| M7 | `CrossPartition_Count_EmulatorBehavior_MultipliesResults` | Show that COUNT returns N × rangeCount |
| L2 | `ArrayContains_WithLiteralArray_EmulatorBehavior_Throws` | Show that ARRAY_CONTAINS with literal array fails/returns unexpected results |
| L3 | `GetCurrentDateTime_EmulatorBehavior_EvaluatesPerRow` | Show that different rows may get different timestamps |
| L4 | `GetItemLinqQueryable_EmulatorBehavior_IgnoresOptions` | Show that linqSerializerOptions are ignored |
| L6 | `OrderBy_EmulatorBehavior_UndefinedAndNullSortTogether` | Show that undefined and null are not distinguished |

### 3.2 Divergences documented in Known Limitations but NOT in DivergentBehaviorTests.cs

| New ID | Divergence | Known Limitation # | Covered Elsewhere? |
|--------|-----------|-------------------|-------------------|
| D1 | Consistency levels ignored (all ops immediate) | "Consistency Levels" section | No dedicated test |
| D2 | Request charge always 1.0 RU synthetic | "Request Charges" section | ResponseMetadataTests verifies > 0 but not 1.0 specifically |
| D3 | Continuation tokens are plain integers not opaque base64 | KL #5 | SkippedBehaviorTests has pagination test but no format divergence test |
| D4 | _ts is present but _self, _rid, _attachments format differs | KL "Resource IDs" | Partially in CrudTests |
| D5 | IndexingPolicy stored but not enforced (no query perf impact) | KL table | SkippedBehaviorTests stores it but no enforcement divergence test |
| D6 | TTL eviction is lazy (on next read) not proactive | KL table | TtlTests tests lazy eviction but not as a divergence test |
| D7 | Analytical store (Synapse) not simulated | KL table | No test |
| D8 | Client encryption keys not implemented | KL "Out of Scope" | No test |

### 3.3 Divergences NOT in Known Limitations AND NOT in DivergentBehaviorTests.cs

| New ID | Divergence | Severity | Notes |
|--------|-----------|----------|-------|
| D9 | LINQ-to-Objects accepts ops that real Cosmos SQL rejects (e.g. String.Format, regex) | Low | BehavioralDifferenceTests mentions this, not in DivergentBehaviorTests |
| D10 | Database.Id returns empty string (NSubstitute mock) | Low | BehavioralDifferenceTests has test |
| D11 | Container remains usable after DeleteContainerAsync | Low | BehavioralDifferenceTests has test |
| D12 | ChangeFeedProcessorBuilder returns stub (no real processing) | Low | BehavioralDifferenceTests has test |
| D13 | GetFeedRanges returns single mock FeedRange by default | Low | FeedRangeDivergentBehaviorTests has test |
| D14 | Geospatial function precision differences | Low | Known Limitation but no divergent test |
| D15 | Full-text search: no BM25, no stemming, no indexing policy requirement | Medium | FullTextSearchDivergentBehaviorTests has tests |

### 3.4 Edge cases within existing gaps that need testing

| Existing ID | Edge Case | Description |
|-------------|-----------|-------------|
| M7 | PartitionKeyRangeCount=1 (default) | Verify aggregates work correctly with default config |
| M7 | SUM, AVG, MIN, MAX | Verify multiplication affects all aggregate types |
| L2 | ARRAY_LENGTH with literal | `ARRAY_LENGTH([1,2,3])` should return 3 |
| L2 | ARRAY_SLICE with literal | `ARRAY_SLICE([1,2,3,4], 1, 2)` |
| L3 | Single-document query | GetCurrentDateTime should be consistent trivially |
| L6 | Mixed type ORDER BY | Sort numbers, strings, booleans, nulls, undefined together |
| L6 | ORDER BY DESC with undefined | Reverse ordering |

---

## 4. Implementation Plan

### Phase 1: Fix bugs and add sister tests to existing gaps

All work in `DivergentBehaviorTests.cs`.

#### Task 1.1: Fix file header comment (Bug 2)
- [ ] Update reference from `gap-fix-tdd-plan.md` to `divergent-behavior-deep-dive-plan.md`

#### Task 1.2: Add M7 sister test + edge cases
- [ ] **RED:** Write `CrossPartition_Count_EmulatorBehavior_MultipliesResults` — expects COUNT returns 3 × rangeCount
- [ ] **GREEN:** Verify it passes with current emulator
- [ ] **RED:** Write `CrossPartition_Count_WithDefaultRange_ReturnsCorrectCount` — expects correct result with PartitionKeyRangeCount=1
- [ ] **GREEN:** Verify it passes
- [ ] **RED:** Write edge case tests for SUM, AVG with PartitionKeyRangeCount > 1

#### Task 1.3: Add L2 sister test + edge cases
- [ ] **RED:** Write `ArrayContains_WithLiteralArray_EmulatorBehavior_ReturnsNoResults` — show actual behaviour
- [ ] **GREEN:** Verify it passes
- [ ] **RED:** Write `ArrayLength_WithLiteralArray_EmulatorBehavior` 
- [ ] **RED:** Write `ArraySlice_WithLiteralArray_EmulatorBehavior`

#### Task 1.4: Add L3 sister test
- [ ] **RED:** Write `GetCurrentDateTime_EmulatorBehavior_ReturnsValueForEachRow` — show timestamps may differ
- [ ] **GREEN:** Verify it passes (likely need to insert many rows and query, check timestamps are close but possibly not identical)
- [ ] Note: this is hard to assert deterministically. May need to just verify the function works and add a comment about per-row evaluation.

#### Task 1.5: Add L4 sister test
- [ ] **RED:** Write `GetItemLinqQueryable_EmulatorBehavior_IgnoresSerializerOptions` — pass custom options, show they're ignored
- [ ] **GREEN:** Verify it passes
- [ ] **RED:** Write `GetItemLinqQueryable_EmulatorBehavior_IgnoresContinuationToken` — pass continuation token, show it's ignored

#### Task 1.6: Add L6 sister test + edge cases
- [ ] **RED:** Write `OrderBy_EmulatorBehavior_UndefinedAndNullSortTogether` — create docs with missing field, null field, valued field, ORDER BY, assert undefined and null are interleaved
- [ ] **GREEN:** Verify it passes
- [ ] **RED:** Write `OrderBy_EmulatorBehavior_MixedTypes_NoTypeOrdering` — show numbers/strings/bools are not type-ordered

### Phase 2: Add new divergence coverage

#### Task 2.1: D1 — Consistency levels ignored
- [ ] **RED:** Write skipped test `ConsistencyLevel_ShouldAffectReadBehavior` with detailed skip reason
- [ ] **RED:** Write sister test `ConsistencyLevel_EmulatorBehavior_AllLevelsReturnSameResult` — set different consistency levels, show all return same result immediately
- [ ] **GREEN:** Verify sister test passes
- **Difficulty assessment:** Too difficult to implement — real consistency levels require distributed state tracking, session tokens, bounded staleness clocks. SKIP.

#### Task 2.2: D2 — Request charge always 1.0 RU
- [ ] **RED:** Write skipped test `RequestCharge_ShouldVaryByOperationComplexity` with detailed skip reason
- [ ] **RED:** Write sister test `RequestCharge_EmulatorBehavior_AlwaysReturns1RU` — create, read, query, delete all return 1.0
- [ ] **GREEN:** Verify sister test passes
- **Difficulty assessment:** Simulating real RU costs requires a full cost model. SKIP.

#### Task 2.3: D3 — Continuation tokens are plain integers
- [ ] **RED:** Write skipped test `ContinuationToken_ShouldBeOpaqueBase64EncodedJson` with detailed skip reason
- [ ] **RED:** Write sister test `ContinuationToken_EmulatorBehavior_IsPlainIntegerOffset` — verify token is a simple integer string
- [ ] **GREEN:** Verify sister test passes
- **Difficulty assessment:** Could implement base64-encoded tokens but it would break existing consumer code that may parse them. SKIP.

#### Task 2.4: D4 — System properties format
- [ ] **RED:** Write skipped test `SystemProperties_ShouldMatchCosmosFormat` with detailed skip reason about _rid, _self, _attachments format
- [ ] **RED:** Write sister test `SystemProperties_EmulatorBehavior_SyntheticFormat` — verify _ts present, _rid is synthetic GUID-like, _self may be empty/different
- [ ] **GREEN:** Verify sister test passes

#### Task 2.5: D5 — IndexingPolicy not enforced
- [ ] **RED:** Write skipped test `IndexingPolicy_ShouldAffectQueryPerformance` with skip reason
- [ ] **RED:** Write sister test `IndexingPolicy_EmulatorBehavior_StoredButNotEnforced` — store an excluding indexing policy, show queries still work
- [ ] **GREEN:** Verify sister test passes
- **Difficulty assessment:** Simulating index selectivity would require a full query optimizer. SKIP.

#### Task 2.6: D6 — TTL eviction is lazy
- [ ] **RED:** Write skipped test `TTL_ShouldProactivelyEvictExpiredItems` with skip reason
- [ ] **RED:** Write sister test `TTL_EmulatorBehavior_LazyEvictionOnRead` — set TTL, wait, show item still in internal store until read triggers eviction
- [ ] **GREEN:** Verify sister test passes
- **Difficulty assessment:** Background eviction would require a timer/background service. Not worth the complexity. SKIP.

#### Task 2.7: D7 — Analytical store not simulated
- [ ] **RED:** Write skipped test `AnalyticalStore_ShouldBeAvailable` with skip reason
- [ ] No sister test needed — there's no analytical store API surface to call
- **Difficulty assessment:** Out of scope. SKIP.

#### Task 2.8: D8 — Client encryption keys not implemented
- [ ] **RED:** Write skipped test `ClientEncryptionKeys_ShouldSupportCreateAndRead` with skip reason
- [ ] No sister test needed — out of scope
- **Difficulty assessment:** Requires Azure Key Vault integration. Out of scope. SKIP.

#### Task 2.9: D9 — LINQ accepts operations real Cosmos rejects
- [ ] **RED:** Write skipped test `Linq_ShouldRejectUnsupportedOperators` with detailed skip reason listing which LINQ ops Cosmos doesn't support
- [ ] **RED:** Write sister test `Linq_EmulatorBehavior_AcceptsAllLinqToObjectsOperators` — use String.Contains, regex, custom comparers, show they all work
- [ ] **GREEN:** Verify sister test passes
- **Difficulty assessment:** Implementing Cosmos LINQ-to-SQL restrictions would require maintaining a whitelist of supported operators. SKIP.

#### Task 2.10: D14 — Geospatial precision differences
- [ ] **RED:** Write skipped test `Geospatial_ShouldMatchCosmosExactValues` with skip reason about calculation method differences
- [ ] **RED:** Write sister test `Geospatial_EmulatorBehavior_GeometricallyCorrectButMayDiffer` — compute ST_DISTANCE, show the result is "close" but may not be bit-exact with Cosmos
- [ ] **GREEN:** Verify sister test passes
- **Difficulty assessment:** Getting bit-exact results would require reverse-engineering Cosmos's exact geodesic implementation. SKIP.

### Phase 3: Edge case tests within existing divergences

#### Task 3.1: M7 aggregate variant tests
- [ ] **RED:** Write `CrossPartition_Sum_EmulatorBehavior_MultipliesResults` 
- [ ] **RED:** Write `CrossPartition_Avg_EmulatorBehavior_AffectedByDuplication`
- [ ] **GREEN:** Verify they pass or determine exact behaviour and adjust

#### Task 3.2: L6 mixed type ordering
- [ ] **RED:** Write `OrderBy_EmulatorBehavior_NumbersAndStrings_NoTypeOrdering` — mix numeric and string values in same ORDER BY field
- [ ] **GREEN:** Verify behaviour and document it

---

## 5. Documentation Updates

### 5.1 Wiki: Known-Limitations.md
After implementation, update Known Limitations to:
- [ ] Add entries for any NEW divergences discovered during testing (D1, D2, D3, D5, D6, D9 if not already there as prose sections)
- [ ] Verify all 15 existing entries still accurately describe behaviour
- [ ] Add test references for new divergent behavior tests
- [ ] Ensure "Consistency Levels" and "Request Charges" sections reference the new tests

### 5.2 Wiki: Feature-Comparison-With-Alternatives.md
- [ ] Verify the comparison table rows align with all divergences. Check that columns for the in-memory emulator accurately reflect the current state for:
  - Consistency levels
  - Request charges
  - Continuation token format
  - System properties
  - IndexingPolicy enforcement
  - TTL eviction model
  - LINQ operator support

### 5.3 Wiki: Features.md
- [ ] Verify features list is consistent with new divergent behavior test findings
- [ ] No new features added — only documentation accuracy

### 5.4 README.md (repo root)
- [ ] Update version badge/reference if applicable
- [ ] No feature changes to document

### 5.5 DivergentBehaviorTests.cs file header
- [ ] Update reference from `gap-fix-tdd-plan.md` → `divergent-behavior-deep-dive-plan.md`
- [ ] Update the gap ID listing to include new D-series IDs

---

## 6. Release Steps

After all tests pass:

- [ ] Increment `<Version>` in `src/CosmosDB.InMemoryEmulator/CosmosDB.InMemoryEmulator.csproj` from `2.0.4` → `2.0.5`
- [ ] `git add -A`
- [ ] `git commit -m "v2.0.5: Deep dive divergent behavior tests — add sister tests, new gap coverage, documentation updates"`
- [ ] `git tag v2.0.5`
- [ ] `git push`
- [ ] `git push --tags`
- [ ] Push wiki updates:
  - `cd c:\git\CosmosDB.InMemoryEmulator.wiki`
  - `git add -A`
  - `git commit -m "v2.0.5: Update Known Limitations and Comparison for divergent behavior deep dive"`
  - `git push`

---

## Progress Tracker

| Task | Status | Notes |
|------|--------|-------|
| 1.1 Fix header comment | ⬜ Not started | |
| 1.2 M7 sister tests | ⬜ Not started | |
| 1.3 L2 sister tests | ⬜ Not started | |
| 1.4 L3 sister test | ⬜ Not started | |
| 1.5 L4 sister tests | ⬜ Not started | |
| 1.6 L6 sister tests | ⬜ Not started | |
| 2.1 D1 Consistency levels | ⬜ Not started | |
| 2.2 D2 Request charge 1.0 RU | ⬜ Not started | |
| 2.3 D3 Continuation tokens | ⬜ Not started | |
| 2.4 D4 System properties | ⬜ Not started | |
| 2.5 D5 IndexingPolicy | ⬜ Not started | |
| 2.6 D6 TTL lazy eviction | ⬜ Not started | |
| 2.7 D7 Analytical store | ⬜ Not started | |
| 2.8 D8 Client encryption | ⬜ Not started | |
| 2.9 D9 LINQ permissiveness | ⬜ Not started | |
| 2.10 D14 Geospatial precision | ⬜ Not started | |
| 3.1 M7 aggregate variants | ⬜ Not started | |
| 3.2 L6 mixed type ordering | ⬜ Not started | |
| 5.1 Wiki: Known Limitations | ⬜ Not started | |
| 5.2 Wiki: Comparison | ⬜ Not started | |
| 5.3 Wiki: Features | ⬜ Not started | |
| 5.4 README | ⬜ Not started | |
| 5.5 File header | ⬜ Not started | |
| 6.x Release (version, tag, push) | ⬜ Not started | |

---

## Summary

- **3 bugs found** (all documentation/pattern issues, no code bugs)
- **5 existing gaps need sister tests** (M7, L2, L3, L4, L6)
- **10 new divergence areas to add** (D1–D9, D14)
- **~7 edge case tests** within existing gaps
- **Total new tests: ~30–35**
- **Tests requiring implementation changes: 0** (all are skipped or document existing behaviour)
- **Wiki pages to update: 3** (Known Limitations, Comparison, Features)
