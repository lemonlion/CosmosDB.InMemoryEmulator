# Behavioral Differences TDD Plan

**Version**: 2.0.4 → 2.0.5  
**Target file**: `tests/CosmosDB.InMemoryEmulator.Tests/BehavioralDifferenceTests.cs`  
**Date**: 2026-04-01  
**Status**: PLAN ONLY — not yet implemented  

---

## Summary

Deep-dive audit of `BehavioralDifferenceTests.cs` against the real Cosmos DB SDK contract.  
Goal: fill missing test coverage, fix bugs, document divergent behaviours with  
skipped + sister tests, and update all documentation.

---

## Existing Coverage in BehavioralDifferenceTests.cs

The file currently has **two test classes** with **16 tests total**:

### Class: `BehavioralDifferenceTests` (10 tests)
| # | Test | Status |
|---|------|--------|
| 1 | `ChangeFeed_ReturnsAllCurrentItems_NotIncrementalChanges` | ✅ Passes |
| 2 | `DeleteContainer_ContainerRemainsUsable_UnlikeRealCosmos` | ✅ Passes |
| 3 | `ReadThroughput_AlwaysReturns400_RegardlessOfReplace` | ✅ Passes |
| 4 | `ETag_IsQuotedGuid_NotOpaqueTimestamp` | ✅ Passes |
| 5 | `LinqQuery_SupportsAllLinqToObjectsOperators` | ✅ Passes |
| 6 | `PartitionKey_ExtractsFromConfiguredPath_WhenNotSupplied` | ✅ Passes |
| 7 | `StreamReplace_ChecksIfMatch_LikeRealCosmos` | ✅ Passes |
| 8 | `Database_ReturnsSubstituteMock_NotRealDatabase` | ✅ Passes |
| 9 | `ReplaceContainer_PersistsPropertyChanges` | ✅ Passes |
| 10 | `GetFeedRanges_ReturnsSingleMockRange` | ✅ Passes |
| 11 | `ChangeFeedProcessorBuilder_ReturnsMock_NoRealProcessing` | ✅ Passes |

### Class: `BehavioralDifferenceGapTests` (5 tests)
| # | Test | Status |
|---|------|--------|
| 12 | `ChangeFeed_EmptyContainer_ReturnsOk_NotNotModified` | ✅ Passes |
| 13 | `ChangeFeed_DeletesRecordedAsTombstone` | ✅ Passes |
| 14 | `DeleteContainer_RemainsUsable_UnlikeRealCosmos` | ✅ Passes (DUPLICATE of #2) |
| 15 | `ETag_Format_IsQuotedGuid_NotOpaqueTimestamp` | ✅ Passes (DUPLICATE of #4) |
| 16 | `Throughput_AlwaysReturns400_IgnoresReplace` | ✅ Passes (DUPLICATE of #3) |
| 17 | `Aggregate_Count_WithoutGroupBy_ReturnsCount` | ✅ Passes |
| 18 | `Query_NullCoalesce_ProducesScalarResult_NotJObject` | ✅ Passes |

---

## Bugs Found

### BUG-1: Duplicate Tests Across Classes
`BehavioralDifferenceGapTests` duplicates three tests already in `BehavioralDifferenceTests`:
- `DeleteContainer_RemainsUsable_UnlikeRealCosmos` ≡ `DeleteContainer_ContainerRemainsUsable_UnlikeRealCosmos`
- `ETag_Format_IsQuotedGuid_NotOpaqueTimestamp` ≡ `ETag_IsQuotedGuid_NotOpaqueTimestamp`
- `Throughput_AlwaysReturns400_IgnoresReplace` ≡ `ReadThroughput_AlwaysReturns400_RegardlessOfReplace`

**Fix**: Remove duplicates from `BehavioralDifferenceGapTests`. Keep unique tests (`ChangeFeed_EmptyContainer_ReturnsOk_NotNotModified`, `ChangeFeed_DeletesRecordedAsTombstone`, `Aggregate_Count_WithoutGroupBy_ReturnsCount`, `Query_NullCoalesce_ProducesScalarResult_NotJObject`) and move them into the main `BehavioralDifferenceTests` class.

### BUG-2: Weak Aggregate Count Assertion  
`Aggregate_Count_WithoutGroupBy_ReturnsCount` asserts only `results.Should().NotBeEmpty()` — it doesn't verify the actual count value. This should assert `results.Should().ContainSingle()` and the value should be `3`.

### BUG-3: `ChangeFeedProcessorBuilder_ReturnsMock_NoRealProcessing` — Misleading Summary
The summary says "NSubstitute cannot proxy it" but then the test asserts `builder.Should().NotBeNull()` and passes. The actual implementation uses `InMemoryChangeFeedProcessor`, not an NSubstitute mock. The XML doc is stale/incorrect.

### BUG-4: `Database_ReturnsSubstituteMock_NotRealDatabase` — New Instance Each Access
`Database` property creates a `new Substitute.For<Database>()` on each access. This means `_container.Database != _container.Database` which is unusual. Not tested.

---

## Missing Test Coverage

### Category A: System Properties & Response Metadata

#### A1: `_rid` Not Present on Documents
**Real Cosmos**: Every document has `_rid` (resource ID — short base64 string).  
**Emulator**: `_rid` is never injected into documents by `EnrichWithSystemProperties`.  
**Test**: Assert `_rid` is absent from response resource JSON.  
**Sister (skipped)**: Show that real Cosmos always includes `_rid`.  
**Difficulty**: Easy — just read and assert.

#### A2: `_self` Not Present on Documents
**Real Cosmos**: Every document has `_self` (e.g. `dbs/db1/colls/col1/docs/doc1`).  
**Emulator**: Never set.  
**Test**: Assert `_self` is absent.  
**Sister (skipped)**: Show expected `_self` format.  
**Difficulty**: Easy.

#### A3: `_attachments` Not Present on Documents
**Real Cosmos**: Every document has `_attachments` (e.g. `attachments/`).  
**Emulator**: Never set.  
**Test**: Assert `_attachments` is absent.  
**Sister (skipped)**: Show expected format.  
**Difficulty**: Easy.

#### A4: Request Charge Always Returns 1.0 RU
**Real Cosmos**: Varies per operation (reads ~1 RU, writes ~5-10 RU, queries vary).  
**Emulator**: Hardcoded `SyntheticRequestCharge = 1.0` for ALL operations.  
**Test**: Assert request charge is exactly `1.0` for create, read, query, delete.  
**Sister (skipped)**: Document that real Cosmos returns varying values.  
**Difficulty**: Easy.

#### A5: Session Token Format Is Synthetic
**Real Cosmos**: Session tokens encode partition key range IDs and logical sequence numbers (e.g. `0:-1#12345`).  
**Emulator**: Hardcoded format `$"0:{Guid.NewGuid():N}"`.  
**Test**: Assert session token starts with `"0:"` and contains a hex GUID.  
**Sister (skipped)**: Document real format.  
**Difficulty**: Easy.

#### A6: Diagnostics Object Is a Mock
**Real Cosmos**: `CosmosDiagnostics` contains detailed timing, request latency, endpoint info.  
**Emulator**: Returns `Substitute.For<CosmosDiagnostics>()` with empty `ToString()`.  
**Test**: Assert diagnostics is not null but `ToString()` returns empty/default.  
**Sister (skipped)**: Document real diagnostics structure.  
**Difficulty**: Easy.

### Category B: Consistency & Session Handling

#### B1: Consistency Level Is Ignored (Always Strong)
**Real Cosmos**: `ConsistencyLevel.Eventual`, `.Session`, `.BoundedStaleness` all produce different read behaviour.  
**Emulator**: All operations are immediately consistent regardless of `ItemRequestOptions.ConsistencyLevel`.  
**Test**: Create item, read with `ConsistencyLevel.Eventual` — still sees latest data immediately.  
**Sister (skipped)**: Document that real Cosmos Eventual might return stale data.  
**Difficulty**: Easy.

### Category C: Container Lifecycle

#### C1: DeleteContainer — Items Cleared But Container Reusable
Already tested. ✅ (Keep existing test.)

#### C2: DeleteContainer — ReadContainerAsync Still Works After Delete
**Real Cosmos**: `ReadContainerAsync` would throw 404 after deletion.  
**Emulator**: Returns the container properties even after deletion.  
**Test**: Delete container, then call `ReadContainerAsync()` — should succeed (emulator) vs would fail (real).  
**Difficulty**: Easy.

#### C3: ReplaceContainer — IndexingPolicy Stored But Not Enforced
**Real Cosmos**: Changing IndexingPolicy affects query execution plans.  
**Emulator**: Stores the policy but query behaviour unchanged.  
**Test**: Set an exclusion path, verify it's stored but queries still return excluded items.  
**Sister (skipped)**: Document that real Cosmos would respect the exclusion.  
**Difficulty**: Easy — store a policy, query, verify results unfiltered.

#### C4: Conflicts Property Returns Mock
**Real Cosmos**: `Container.Conflicts` provides access to conflict resolution.  
**Emulator**: Returns `Substitute.For<Conflicts>()`.  
**Test**: Assert `Conflicts` is not null and is a mock with no real methods.  
**Difficulty**: Easy.

### Category D: Change Feed Edge Cases

#### D1: Change Feed — Empty Container Returns HasMoreResults=false Immediately
Already partially tested in `ChangeFeed_EmptyContainer_ReturnsOk_NotNotModified`. Review whether the assertion fully captures the behavior.

#### D2: Change Feed — Updates Replace Previous Version (Incremental Mode)
**Real Cosmos**: Incremental mode returns only the latest version of each document.  
**Emulator**: Same behaviour (correct) but never tested in BehavioralDifferenceTests.  
**Test**: Create item, update it twice, read change feed — should see only latest version.  
**Difficulty**: Easy.

#### D3: Change Feed — FeedRange Scoping Filters by Partition Key Hash
**Real Cosmos**: FeedRange limits change feed to a specific partition key range.  
**Emulator**: Uses MurmurHash3 routing.  
**Test**: Create items in different partitions, read change feed with a specific FeedRange — only items in that range appear.  
**Difficulty**: Medium — requires setting `FeedRangeCount > 1` and understanding hash routing. Likely too complex; mark as SKIP if implementation isn't straightforward.

### Category E: Error Formatting

#### E1: CosmosException SubStatusCode Always 0
**Real Cosmos**: Sub-status codes provide fine-grained error classification (e.g., 1001 for timeout, 1003 for request rate too large).  
**Emulator**: All `CosmosException` instances have `SubStatusCode = 0`.  
**Test**: Trigger a known error (e.g., 409 Conflict on duplicate id), assert `SubStatusCode == 0`.  
**Sister (skipped)**: Document real sub-status codes.  
**Difficulty**: Easy.

#### E2: CosmosException Message Format Differs
**Real Cosmos**: Error messages include activity ID, request URI, status and substatus.  
**Emulator**: Minimal messages.  
**Test**: Trigger error, verify message content is simpler than real Cosmos.  
**Difficulty**: Easy.

### Category F: Continuation Tokens

#### F1: Continuation Token Is Plain Integer
Already documented in DivergentBehaviorTests but NOT in BehavioralDifferenceTests.  
**Test**: Query with `MaxItemCount=1`, verify continuation token is an integer string.  
**Difficulty**: Easy.

#### F2: Invalid Continuation Token Handling
**Real Cosmos**: Invalid continuation token returns 400 BadRequest.  
**Emulator**: Falls back to offset 0 (silently ignores invalid tokens).  
**Test**: Pass garbage string as continuation token, verify it doesn't throw.  
**Sister (skipped)**: Document that real Cosmos would fail with 400.  
**Difficulty**: Easy.

### Category G: LINQ Specific

#### G1: LINQ Supports Operators That Real Cosmos Would Reject
Already partially tested. Enhance to show specific operators like `GroupJoin`, `Aggregate`, `TakeWhile` that work in-memory but would fail server-side.  
**Difficulty**: Easy.

#### G2: LINQ ContinuationToken Ignored
Documented in DivergentBehaviorTests (L4 skip). Add sister test to BehavioralDifferenceTests showing the token is silently ignored.  
**Difficulty**: Easy.

### Category H: Partition Key Edge Cases

#### H1: PartitionKey.None vs PartitionKey.Null Treated Identically
Documented in Known-Limitations (#7) but NOT tested in BehavioralDifferenceTests. Tests exist in PartitionKeyTests.cs.  
**Test**: Create with `PartitionKey.None`, read with `PartitionKey.Null` — should succeed identically.  
**Sister (skipped)**: Document that real Cosmos treats them differently.  
**Difficulty**: Easy.

#### H2: Hierarchical Partition Keys — Only First Level Used
**Real Cosmos**: Supports multi-level hierarchical partition keys.  
**Emulator**: May only use the first level of a hierarchical partition key.  
**Test**: Check if hierarchical PK behavior matches or diverges.  
**Difficulty**: Medium — need to investigate current implementation first. May need SKIP.

### Category I: Database-Level Differences

#### I1: Database Property Creates New Mock Each Access
**Test**: Assert `container.Database != container.Database` (referential inequality).  
**Real Cosmos**: Returns the same Database instance.  
**Sister (skipped)**: Document that real Cosmos returns a consistent reference.  
**Difficulty**: Easy.

---

## Execution Plan (TDD: Red → Green → Refactor)

### Phase 0: Cleanup & Bug Fixes
- [ ] 0.1 Remove duplicate tests from `BehavioralDifferenceGapTests`
- [ ] 0.2 Move unique `BehavioralDifferenceGapTests` tests into `BehavioralDifferenceTests`
- [ ] 0.3 Delete `BehavioralDifferenceGapTests` class
- [ ] 0.4 Fix `Aggregate_Count_WithoutGroupBy_ReturnsCount` weak assertion (BUG-2)
- [ ] 0.5 Fix `ChangeFeedProcessorBuilder_ReturnsMock_NoRealProcessing` XML doc (BUG-3)
- [ ] 0.6 Verify all existing tests still pass (GREEN baseline)

### Phase 1: System Properties & Response Metadata
- [ ] 1.1 RED: Write `SystemProperties_RidNotPresent_OnDocuments` → assert `_rid` missing
- [ ] 1.2 GREEN: Verify test passes (emulator never sets `_rid`)
- [ ] 1.3 RED: Write skipped sister `SystemProperties_RidShouldBePresent_RealCosmos`
- [ ] 1.4 RED: Write `SystemProperties_SelfNotPresent_OnDocuments` → assert `_self` missing
- [ ] 1.5 GREEN: Verify pass
- [ ] 1.6 RED: Write skipped sister `SystemProperties_SelfShouldBePresent_RealCosmos`
- [ ] 1.7 RED: Write `SystemProperties_AttachmentsNotPresent_OnDocuments` → assert `_attachments` missing
- [ ] 1.8 GREEN: Verify pass
- [ ] 1.9 RED: Write skipped sister `SystemProperties_AttachmentsShouldBePresent_RealCosmos`
- [ ] 1.10 RED: Write `RequestCharge_AlwaysReturns1RU_ForAllOperations`
- [ ] 1.11 GREEN: Verify pass
- [ ] 1.12 RED: Write skipped sister `RequestCharge_ShouldVaryByOperation_RealCosmos`
- [ ] 1.13 RED: Write `SessionToken_IsSyntheticGuidFormat`
- [ ] 1.14 GREEN: Verify pass
- [ ] 1.15 RED: Write skipped sister `SessionToken_ShouldContainPartitionAndLSN_RealCosmos`
- [ ] 1.16 RED: Write `Diagnostics_ReturnsMock_EmptyToString`
- [ ] 1.17 GREEN: Verify pass
- [ ] 1.18 RED: Write skipped sister `Diagnostics_ShouldContainTimingInfo_RealCosmos`

### Phase 2: Consistency & Session
- [ ] 2.1 RED: Write `ConsistencyLevel_Ignored_AlwaysStrongSemantics`
- [ ] 2.2 GREEN: Verify pass
- [ ] 2.3 RED: Write skipped sister `ConsistencyLevel_ShouldAffectReadBehavior_RealCosmos`

### Phase 3: Container Lifecycle
- [ ] 3.1 RED: Write `DeleteContainer_ReadContainerAsync_StillWorks_UnlikeRealCosmos`
- [ ] 3.2 GREEN: Verify pass (or implement if needed)
- [ ] 3.3 RED: Write `ReplaceContainer_IndexingPolicy_StoredButNotEnforced`
- [ ] 3.4 GREEN: Verify pass (or implement if needed — may need to store the IndexingPolicy)
- [ ] 3.5 RED: Write skipped sister `ReplaceContainer_IndexingPolicyShouldAffectQueries_RealCosmos`
- [ ] 3.6 RED: Write `Conflicts_ReturnsMock_NoRealConflictResolution`
- [ ] 3.7 GREEN: Verify pass

### Phase 4: Change Feed
- [ ] 4.1 RED: Write `ChangeFeed_IncrementalMode_UpdatesReturnOnlyLatestVersion`
- [ ] 4.2 GREEN: Verify pass
- [ ] 4.3 Assess FeedRange-scoped change feed — implement or SKIP with detailed reason

### Phase 5: Error Formatting
- [ ] 5.1 RED: Write `CosmosException_SubStatusCode_AlwaysZero`
- [ ] 5.2 GREEN: Verify pass
- [ ] 5.3 RED: Write skipped sister `CosmosException_SubStatusCodeShouldBeSpecific_RealCosmos`
- [ ] 5.4 RED: Write `CosmosException_MessageFormat_SimplerThanRealCosmos`
- [ ] 5.5 GREEN: Verify pass

### Phase 6: Continuation Tokens
- [ ] 6.1 RED: Write `ContinuationToken_IsPlainInteger_NotOpaqueBase64`
- [ ] 6.2 GREEN: Verify pass
- [ ] 6.3 RED: Write `ContinuationToken_Invalid_SilentlyFallsToStart`
- [ ] 6.4 GREEN: Verify pass (or implement graceful fallback)
- [ ] 6.5 RED: Write skipped sister `ContinuationToken_InvalidShouldReturn400_RealCosmos`

### Phase 7: LINQ Enhancements
- [ ] 7.1 RED: Write `LinqQuery_UnsupportedOperators_SucceedInMemory_WouldFailRealCosmos`
- [ ] 7.2 GREEN: Verify pass
- [ ] 7.3 RED: Write skipped sister `LinqQuery_UnsupportedOperators_ShouldThrow_RealCosmos`

### Phase 8: Partition Key Edge Cases
- [ ] 8.1 RED: Write `PartitionKey_NoneVsNull_TreatedIdentically`
- [ ] 8.2 GREEN: Verify pass
- [ ] 8.3 RED: Write skipped sister `PartitionKey_NoneVsNull_ShouldDiffer_RealCosmos`

### Phase 9: Database Property
- [ ] 9.1 RED: Write `Database_NewMockInstanceOnEachAccess_UnlikeRealCosmos`
- [ ] 9.2 GREEN: Verify pass (each access returns a different mock instance)
- [ ] 9.3 RED: Write skipped sister `Database_ShouldReturnSameInstance_RealCosmos`

### Phase 10: Final Validation
- [ ] 10.1 Run full test suite — all tests GREEN
- [ ] 10.2 Verify no regressions in other test files

### Phase 11: Documentation Updates
- [ ] 11.1 Update `Known-Limitations.md` — add new behavioral differences for:
  - System properties (_rid, _self, _attachments missing)
  - Request charge always 1.0 RU
  - Session token synthetic format
  - Diagnostics object is a mock
  - Consistency level ignored
  - Continuation token plain integer (move from just DivergentBehaviorTests reference)
  - Error sub-status always 0
  - PartitionKey.None vs .Null (already there, just verify test reference updated)
  - Database property returns new mock each access
- [ ] 11.2 Update `Feature-Comparison-With-Alternatives.md` — ensure new behavioral differences reflected in comparison table rows for diagnostics, session tokens, consistency
- [ ] 11.3 Update `Features.md` — add notes about system properties coverage 
- [ ] 11.4 Update `README.md` — no changes expected unless new high-level features added
- [ ] 11.5 Commit and push wiki changes

### Phase 12: Version & Release
- [ ] 12.1 Increment version in `CosmosDB.InMemoryEmulator.csproj`: `2.0.4` → `2.0.5`
- [ ] 12.2 `git add -A`
- [ ] 12.3 `git commit -m "v2.0.5: Comprehensive behavioral difference test coverage — system properties, session tokens, consistency, error formatting, continuation tokens, partition key edge cases"`
- [ ] 12.4 `git tag v2.0.5`
- [ ] 12.5 `git push && git push --tags`

---

## Tests That Will Be Skipped (Too Difficult / Out of Scope)

### SKIP-1: FeedRange-Scoped Change Feed Filtering
**Reason**: Requires deep knowledge of MurmurHash3 boundary behaviour and may  
need FeedRangeCount > 1 configuration. Already covered in FeedRangeTests.cs.  
Will add a cross-reference comment to FeedRangeTests.cs instead.

### SKIP-2: Hierarchical Partition Key Support
**Reason**: The emulator's hierarchical partition key support status is unclear.  
Need to investigate `PartitionKeyBuilder` support first. If not supported, add  
a skipped test documenting the gap. May already be covered in PartitionKeyTests.cs.

### SKIP-3: ChangeFeed AllVersionsAndDeletes via SDK Mode
**Reason**: `ChangeFeedMode.AllVersionsAndDeletes` is an internal/preview API in some  
SDK versions. The emulator supports this via the checkpoint-based API which is a  
different code path. Already documented in Known-Limitations #7.

---

## Test Naming Convention

All new tests follow this pattern:  
```
{Feature}_{Behavior}_{EmulatorContext}
```

Skipped "real Cosmos" sister tests follow:  
```
{Feature}_{ExpectedRealBehavior}_RealCosmos  (Skip = "Detailed explanation...")
```

Divergent behaviour sister tests (unskipped, inline-commented):  
```
{Feature}_EmulatorBehavior_{WhatItDoes}
```

---

## File Structure After Implementation

```
BehavioralDifferenceTests.cs
├── class BehavioralDifferenceTests
│   ├── // ── Existing (kept) ──
│   │   ├── ChangeFeed_ReturnsAllCurrentItems_NotIncrementalChanges
│   │   ├── DeleteContainer_ContainerRemainsUsable_UnlikeRealCosmos
│   │   ├── ReadThroughput_AlwaysReturns400_RegardlessOfReplace
│   │   ├── ETag_IsQuotedGuid_NotOpaqueTimestamp
│   │   ├── LinqQuery_SupportsAllLinqToObjectsOperators
│   │   ├── PartitionKey_ExtractsFromConfiguredPath_WhenNotSupplied
│   │   ├── StreamReplace_ChecksIfMatch_LikeRealCosmos
│   │   ├── Database_ReturnsSubstituteMock_NotRealDatabase
│   │   ├── ReplaceContainer_PersistsPropertyChanges
│   │   ├── GetFeedRanges_ReturnsSingleMockRange
│   │   ├── ChangeFeedProcessorBuilder_ReturnsMock_NoRealProcessing (fix XML doc)
│   │   ├── // ── Moved from BehavioralDifferenceGapTests ──
│   │   ├── ChangeFeed_EmptyContainer_ReturnsOk_NotNotModified (was Gap)
│   │   ├── ChangeFeed_DeletesRecordedAsTombstone (was Gap)
│   │   ├── Aggregate_Count_WithoutGroupBy_ReturnsCount (was Gap, FIX assertion)
│   │   ├── Query_NullCoalesce_ProducesScalarResult_NotJObject (was Gap)
│   │   ├── // ── New: System Properties ──
│   │   ├── SystemProperties_RidNotPresent_OnDocuments
│   │   ├── SystemProperties_SelfNotPresent_OnDocuments
│   │   ├── SystemProperties_AttachmentsNotPresent_OnDocuments
│   │   ├── RequestCharge_AlwaysReturns1RU_ForAllOperations
│   │   ├── SessionToken_IsSyntheticGuidFormat
│   │   ├── Diagnostics_ReturnsMock_EmptyToString
│   │   ├── // ── New: Consistency ──
│   │   ├── ConsistencyLevel_Ignored_AlwaysStrongSemantics
│   │   ├── // ── New: Container Lifecycle ──
│   │   ├── DeleteContainer_ReadContainerAsync_StillWorks_UnlikeRealCosmos
│   │   ├── ReplaceContainer_IndexingPolicy_StoredButNotEnforced
│   │   ├── Conflicts_ReturnsMock_NoRealConflictResolution
│   │   ├── // ── New: Change Feed ──
│   │   ├── ChangeFeed_IncrementalMode_UpdatesReturnOnlyLatestVersion
│   │   ├── // ── New: Error Formatting ──
│   │   ├── CosmosException_SubStatusCode_AlwaysZero
│   │   ├── CosmosException_MessageFormat_SimplerThanRealCosmos
│   │   ├── // ── New: Continuation Tokens ──
│   │   ├── ContinuationToken_IsPlainInteger_NotOpaqueBase64
│   │   ├── ContinuationToken_Invalid_SilentlyFallsToStart
│   │   ├── // ── New: LINQ ──
│   │   ├── LinqQuery_UnsupportedOperators_SucceedInMemory_WouldFailRealCosmos
│   │   ├── // ── New: Partition Key ──
│   │   ├── PartitionKey_NoneVsNull_TreatedIdentically
│   │   ├── // ── New: Database Property ──
│   │   ├── Database_NewMockInstanceOnEachAccess_UnlikeRealCosmos
│   │   │
│   │   └── // ── Skipped Sister Tests (Real Cosmos Behaviour) ──
│   │       ├── SystemProperties_RidShouldBePresent_RealCosmos (SKIP)
│   │       ├── SystemProperties_SelfShouldBePresent_RealCosmos (SKIP)
│   │       ├── SystemProperties_AttachmentsShouldBePresent_RealCosmos (SKIP)
│   │       ├── RequestCharge_ShouldVaryByOperation_RealCosmos (SKIP)
│   │       ├── SessionToken_ShouldContainPartitionAndLSN_RealCosmos (SKIP)
│   │       ├── Diagnostics_ShouldContainTimingInfo_RealCosmos (SKIP)
│   │       ├── ConsistencyLevel_ShouldAffectReadBehavior_RealCosmos (SKIP)
│   │       ├── ReplaceContainer_IndexingPolicyShouldAffectQueries_RealCosmos (SKIP)
│   │       ├── CosmosException_SubStatusCodeShouldBeSpecific_RealCosmos (SKIP)
│   │       ├── ContinuationToken_InvalidShouldReturn400_RealCosmos (SKIP)
│   │       ├── LinqQuery_UnsupportedOperators_ShouldThrow_RealCosmos (SKIP)
│   │       ├── PartitionKey_NoneVsNull_ShouldDiffer_RealCosmos (SKIP)
│   │       └── Database_ShouldReturnSameInstance_RealCosmos (SKIP)
│
└── (BehavioralDifferenceGapTests class DELETED — contents merged above)
```

---

## Estimated Test Count

| Category | Active Tests | Skipped Sister Tests |
|----------|:---:|:---:|
| Existing (kept) | 11 | 0 |
| Moved from GapTests | 4 | 0 |
| New: System Properties | 4 | 4 |
| New: Session/Diagnostics | 2 | 2 |
| New: Consistency | 1 | 1 |
| New: Container Lifecycle | 3 | 1 |
| New: Change Feed | 1 | 0 |
| New: Error Formatting | 2 | 1 |
| New: Continuation Tokens | 2 | 1 |
| New: LINQ | 1 | 1 |
| New: Partition Key | 1 | 1 |
| New: Database Property | 1 | 1 |
| **Total** | **33** | **13** |

Grand total: **46 tests** (33 active + 13 skipped)

---

## Documentation Changes Summary

### Known-Limitations.md
Add new sections:
- **16. System Properties Missing (_rid, _self, _attachments)** — Impact: Low
- **17. Request Charge Always 1.0 RU** — Impact: Low (was in "Request Charges" section, now gets a numbered entry with test reference)
- **18. Session Token Format Is Synthetic** — Impact: Low
- **19. Diagnostics Object Is a Mock** — Impact: Low  
- **20. Consistency Level Not Simulated** — Impact: Medium (was in "Consistency Levels" section, now numbered)
- **21. Error Sub-Status Code Always 0** — Impact: Low
- **22. Database Property Returns New Mock Each Access** — Impact: Low

Update existing test references for entries that now have tests in BehavioralDifferenceTests.

### Feature-Comparison-With-Alternatives.md
Update CRUD & Data Operations table:
- Add row for "System properties (_rid, _self, _attachments)" — mark emulator as ⚠️
- Add row for "CosmosDiagnostics" — mark emulator as ⚠️ Stub
- Verify "Session tokens" row exists, or add with ⚠️ Synthetic

### Features.md
- Add note under system properties section re: `_rid`, `_self`, `_attachments` not included
- Add note under diagnostics section re: stub implementation

### README.md
- No changes expected unless the new coverage reveals high-impact gaps

---

## Progress Tracking

Use this section to track actual implementation progress:

| Phase | Status | Notes |
|-------|--------|-------|
| Phase 0: Cleanup | ⬜ Not started | |
| Phase 1: System Properties | ⬜ Not started | |
| Phase 2: Consistency | ⬜ Not started | |
| Phase 3: Container Lifecycle | ⬜ Not started | |
| Phase 4: Change Feed | ⬜ Not started | |
| Phase 5: Error Formatting | ⬜ Not started | |
| Phase 6: Continuation Tokens | ⬜ Not started | |
| Phase 7: LINQ | ⬜ Not started | |
| Phase 8: Partition Key | ⬜ Not started | |
| Phase 9: Database Property | ⬜ Not started | |
| Phase 10: Final Validation | ⬜ Not started | |
| Phase 11: Documentation | ⬜ Not started | |
| Phase 12: Version & Release | ⬜ Not started | |
