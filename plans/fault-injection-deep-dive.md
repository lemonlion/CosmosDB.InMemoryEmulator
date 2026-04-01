# Fault Injection Deep Dive — Test Coverage & Bug Fix Plan

## Current State

### Existing Tests (10 total across 3 files)

| # | File | Class | Test Name | What It Covers |
|---|------|-------|-----------|---------------|
| 1 | FaultInjectionTests.cs | FaultInjectionGapTests4 | `FaultInjection_Timeout_408` | 408 status via GetItemQueryIterator |
| 2 | FaultInjectionTests.cs | FaultInjectionGapTests4 | `FaultInjection_SelectiveByPath_OnlyFailsWrites` | Conditional fault by HTTP method (raw HttpClient) |
| 3 | FaultInjectionTests.cs | FaultInjectionTests | `FaultInjection_429_ClientReceivesThrottle` | 429 via LINQ ToFeedIterator |
| 4 | FaultInjectionTests.cs | FaultInjectionTests | `FaultInjection_503_ClientReceivesServiceUnavailable` | 503 via LINQ ToFeedIterator |
| 5 | FaultInjectionTests.cs | FaultInjectionTests | `FaultInjection_SkipsMetadata_ByDefault` | Fault injector doesn't fire for metadata |
| 6 | FaultInjectionTests.cs | FaultInjectionTests | `FaultInjection_IncludesMetadata_WhenEnabled` | FaultInjectorIncludesMetadata=true |
| 7 | FaultInjectionTests.cs | FaultInjectionTests | `FaultInjection_Intermittent_EventuallySucceeds` | 429 fails twice then succeeds with retries |
| 8 | FakeCosmosHandlerCrudTests.cs | — | `Handler_FaultInjection_ThrottlesCreateRequest` | 429 on CreateItemAsync via SDK |
| 9 | RealToFeedIteratorTests.cs | — | `ToFeedIterator_WithFaultInjector_Returns503` | 503 through feed iterator |
| 10 | RealToFeedIteratorTests.cs | — | `ToFeedIterator_WithMetadataFaultInjection_FailsOnCollectionFetch` | Metadata fault on pkranges/colls |

### Current Implementation (FakeCosmosHandler.cs)

- **FaultInjector**: `Func<HttpRequestMessage, HttpResponseMessage?>?` — sync-only delegate
- **FaultInjectorIncludesMetadata**: `bool` (default `false`) — controls whether metadata routes are intercepted
- **Two injection points**: Early (before metadata, if `FaultInjectorIncludesMetadata=true`) and Late (after metadata, if `FaultInjectorIncludesMetadata=false`)
- **No exception handling** around FaultInjector invocation — exceptions propagate raw
- **CreateRouter** does NOT propagate FaultInjector to child handlers (by design — each handler configured independently)
- **FaultInjector is sync-only** — no `Func<HttpRequestMessage, Task<HttpResponseMessage?>>` overload

---

## Bugs Found

### Bug 1: `FaultInjectionGapTests4` — Stale class name
**File:** FaultInjectionTests.cs, line 13  
**Issue:** Class named `FaultInjectionGapTests4` — the suffix "4" is a leftover from iterative gap-filling. Should be consolidated into the main `FaultInjectionTests` class or renamed to something meaningful.  
**Fix:** Merge both tests into `FaultInjectionTests` class.

### Bug 2: `FaultInjection_SkipsMetadata_ByDefault` — Unused counter variable
**File:** FaultInjectionTests.cs, line 173  
**Issue:** `queryCallCount` is incremented via `Interlocked.Increment` but never asserted on. The test name suggests it verifies metadata skipping, but the only assertion is that queries throw. It doesn't actually prove metadata requests were NOT intercepted.  
**Fix:** Assert that `queryCallCount` is > 0 (fault injector was called) but also verify that the SDK was able to initialise successfully up to the point of the data query (which it could only do if metadata was NOT faulted).

### Bug 3: `FaultInjection_IncludesMetadata_WhenEnabled` — Overly broad exception assertion
**File:** FaultInjectionTests.cs, line 223  
**Issue:** Asserts `ThrowAsync<Exception>()` instead of a more specific type. When metadata is faulted, the SDK might throw various types (CosmosException, HttpRequestException, InvalidOperationException). The broad `Exception` assertion could pass for entirely wrong reasons (e.g. NullReferenceException).  
**Fix:** Investigate what the SDK actually throws and assert on the specific type, or at minimum assert on the exception message/properties.

### Bug 4: `FaultInjection_SelectiveByPath_OnlyFailsWrites` — Uses raw HttpClient, not SDK
**File:** FaultInjectionTests.cs, line 51  
**Issue:** This test bypasses the CosmosClient SDK entirely and uses raw `HttpClient` to send requests. While it proves the FakeCosmosHandler routing works, it doesn't prove anything about how the SDK consumers would experience selective fault injection. A user configuring fault injection would always go through the SDK.  
**Fix:** Create a companion test that exercises the same selective pattern through the actual CosmosClient SDK (e.g., read succeeds, write fails).

---

## Missing Test Coverage

### Category A: Operation-Specific Fault Injection

Currently, fault injection is only tested on **queries** (GetItemQueryIterator, LINQ ToFeedIterator) and **create** (one test in FakeCosmosHandlerCrudTests). Real applications issue many operation types.

| # | Test Name | Operation | Why Needed |
|---|-----------|-----------|-----------|
| A1 | `FaultInjection_429_OnPointRead` | ReadItemAsync | Most common Cosmos operation; retry behaviour may differ from queries |
| A2 | `FaultInjection_429_OnReplace` | ReplaceItemAsync | Write ops have different retry policies in real Cosmos |
| A3 | `FaultInjection_429_OnUpsert` | UpsertItemAsync | Frequently used in event-sourcing patterns |
| A4 | `FaultInjection_429_OnDelete` | DeleteItemAsync | Delete has its own code path in FakeCosmosHandler |
| A5 | `FaultInjection_429_OnPatch` | PatchItemAsync | Patch is a distinct HTTP PATCH verb |
| A6 | `FaultInjection_503_OnReadMany` | ReadManyItemsAsync¹ | ReadMany fans out internally; fault behaviour is complex |
| A7 | `FaultInjection_503_OnTransactionalBatch` | TransactionalBatch¹ | Batch goes through different pipeline in SDK |
| A8 | `FaultInjection_503_OnChangeFeed` | GetChangeFeedIterator¹ | Change feed processing is critical for event-driven architectures |

> ¹ These operations go through `InMemoryContainer` directly, not `FakeCosmosHandler`. They may be **out of scope** for FakeCosmosHandler fault injection since the handler only intercepts REST/HTTP. Mark as skipped if they can't be tested through the handler path. Create sister tests showing the divergent behaviour.

### Category B: HTTP Status Code Coverage

Currently tested: 408, 429, 503. Real Cosmos DB returns many other error codes.

| # | Test Name | Status Code | Why Needed |
|---|-----------|-------------|-----------|
| B1 | `FaultInjection_500_InternalServerError` | 500 | Common transient failure |
| B2 | `FaultInjection_410_Gone_PartitionMoved` | 410 | Partition split/move; SDK has special handling |
| B3 | `FaultInjection_449_RetryWith` | 449 | Write conflicts during partition split |
| B4 | `FaultInjection_403_Forbidden` | 403 | Quota exceeded, firewall, CORS |
| B5 | `FaultInjection_404_NotFound_ViaFaultInjection` | 404 | Distinct from natural 404 — simulates infrastructure errors |
| B6 | `FaultInjection_400_BadRequest` | 400 | Simulates service-side validation rejections |

### Category C: Fault Response Fidelity & Headers

Real Cosmos DB error responses include specific headers the SDK depends on.

| # | Test Name | What It Tests |
|---|-----------|---------------|
| C1 | `FaultInjection_429_WithRetryAfterMs_SDKRespectsDelay` | Verify x-ms-retry-after-ms header controls retry timing |
| C2 | `FaultInjection_429_WithSubstatus_3200_Throttled` | x-ms-substatus: 3200 (collection throttle) |
| C3 | `FaultInjection_429_WithSubstatus_3088_PartitionThrottle` | x-ms-substatus: 3088 (partition-level) |
| C4 | `FaultInjection_Response_WithEmptyContent` | Fault response has empty string Content |
| C5 | `FaultInjection_Response_WithNullContent` | Fault response has null Content |
| C6 | `FaultInjection_Response_WithActivityId` | Custom x-ms-activity-id in fault response is preserved |

### Category D: Dynamic / Stateful Fault Injection Patterns

| # | Test Name | What It Tests |
|---|-----------|---------------|
| D1 | `FaultInjection_SetToNull_DisablesMidway` | Set FaultInjector to null after initial faults — operations resume |
| D2 | `FaultInjection_ToggleOnOff_DynamicBehavior` | Enable, disable, re-enable fault injection |
| D3 | `FaultInjection_CountBased_FailsFirstNThenSucceeds` | Explicit N-failure pattern (more targeted than Intermittent test) |
| D4 | `FaultInjection_PartitionKeyBased_OnlyFailsSpecificPK` | Route-aware faults: only fail requests for a specific partition |
| D5 | `FaultInjection_DocumentIdBased_OnlyFailsSpecificDoc` | Fail only point reads for a specific document ID |

### Category E: Infrastructure / Edge Cases

| # | Test Name | What It Tests |
|---|-----------|---------------|
| E1 | `FaultInjection_DelegateThrows_ExceptionPropagates` | FaultInjector delegate throws — verify it propagates cleanly |
| E2 | `FaultInjection_RequestLogStillRecords_FaultedRequests` | Verify RequestLog captures the METHOD + PATH even for faulted requests |
| E3 | `FaultInjection_QueryLogNotPopulated_WhenFaulted` | QueryLog should NOT contain the SQL when fault fires before query execution |
| E4 | `FaultInjection_ConcurrentRequests_ThreadSafe` | Multiple parallel faulted requests don't corrupt state |
| E5 | `FaultInjection_WithCreateRouter_IndependentPerContainer` | Multi-container: fault one container, other is healthy |
| E6 | `FaultInjection_DirectContainerOps_BypassFaultInjection` | Direct InMemoryContainer calls are NOT affected by FaultInjector |

### Category F: SDK Retry Behaviour Verification

| # | Test Name | What It Tests |
|---|-----------|---------------|
| F1 | `FaultInjection_429_NoRetries_WhenRetryDisabled` | MaxRetryAttemptsOnRateLimitedRequests=0 means no retry |
| F2 | `FaultInjection_429_MaxRetriesExhausted_StillThrows` | Set retries=2, fail 5 times — verify it throws after exhausting retries |
| F3 | `FaultInjection_503_WithRetries_SdkRetries` | 503 with retries enabled — verify SDK retries transient errors |
| F4 | `FaultInjection_408_WithRetries_SdkRetries` | 408 timeout with retries — verify retry behaviour |

---

## Implementation Difficulty Assessment

### Tests Expected to Be Straightforward (Implement Normally)
A1-A5, B1-B6, C1-C6, D1-D5, E1-E4, E6, F1

### Tests That May Need Skip + Divergent Behaviour Sister Test

| Test | Difficulty | Reason | Approach |
|------|-----------|--------|----------|
| A6 (ReadMany) | Hard | ReadMany goes through SDK internal batching, may not route through FakeCosmosHandler in a way that's interceptable | Skip with reason, sister test showing ReadMany doesn't go through HTTP handler |
| A7 (TransactionalBatch) | Hard | TransactionalBatch uses a different SDK pipeline; may bypass the handler entirely or use a different URL pattern | Skip with reason, sister test showing batch routing |
| A8 (ChangeFeed) | Medium-Hard | Change feed uses different HTTP paths; need to verify the handler routes them through the fault injector | Attempt implementation; skip if change feed has dedicated bypass |
| E5 (CreateRouter) | Medium | Multi-container test requires wiring up the router with fault on one handler | Should be feasible but verify router delegates to correct handler |
| F3-F4 (503/408 retries) | Medium | SDK retry behaviour for non-429 codes depends on SDK version and config | Can be flaky; may need skip if SDK doesn't retry 503/408 by default through Gateway mode |

---

## Execution Plan (TDD: Red-Green-Refactor)

### Phase 0: Bug Fixes & Cleanup
- [ ] 0.1 — Merge `FaultInjectionGapTests4` into `FaultInjectionTests`, remove stale class name
- [ ] 0.2 — Fix `FaultInjection_SkipsMetadata_ByDefault`: assert on `queryCallCount`
- [ ] 0.3 — Fix `FaultInjection_IncludesMetadata_WhenEnabled`: use specific exception type
- [ ] 0.4 — Add SDK-based companion for `FaultInjection_SelectiveByPath_OnlyFailsWrites`

### Phase 1: Operation-Specific Tests (A1-A8)
For each: write failing test → implement/verify → green → refactor
- [ ] 1.1 — A1: Point read fault injection
- [ ] 1.2 — A2: Replace fault injection
- [ ] 1.3 — A3: Upsert fault injection
- [ ] 1.4 — A4: Delete fault injection
- [ ] 1.5 — A5: Patch fault injection
- [ ] 1.6 — A6: ReadMany fault injection (likely skip + sister test)
- [ ] 1.7 — A7: TransactionalBatch fault injection (likely skip + sister test)
- [ ] 1.8 — A8: ChangeFeed fault injection (attempt, may skip)

### Phase 2: Status Code Coverage (B1-B6)
- [ ] 2.1 — B1: 500 Internal Server Error
- [ ] 2.2 — B2: 410 Gone
- [ ] 2.3 — B3: 449 Retry With
- [ ] 2.4 — B4: 403 Forbidden
- [ ] 2.5 — B5: 404 via fault injection
- [ ] 2.6 — B6: 400 Bad Request

### Phase 3: Response Fidelity (C1-C6)
- [ ] 3.1 — C1: Retry-After header respected
- [ ] 3.2 — C2: Substatus 3200
- [ ] 3.3 — C3: Substatus 3088
- [ ] 3.4 — C4: Empty content response
- [ ] 3.5 — C5: Null content response
- [ ] 3.6 — C6: Activity ID preserved

### Phase 4: Dynamic Patterns (D1-D5)
- [ ] 4.1 — D1: Disable fault injection mid-flight
- [ ] 4.2 — D2: Toggle on/off
- [ ] 4.3 — D3: Count-based N failures
- [ ] 4.4 — D4: Partition-key-based faults
- [ ] 4.5 — D5: Document-ID-based faults

### Phase 5: Infrastructure & Edge Cases (E1-E6)
- [ ] 5.1 — E1: Delegate throws
- [ ] 5.2 — E2: Request log records faulted requests
- [ ] 5.3 — E3: Query log NOT populated when faulted
- [ ] 5.4 — E4: Concurrent thread safety
- [ ] 5.5 — E5: CreateRouter independent fault injection
- [ ] 5.6 — E6: Direct container ops bypass

### Phase 6: SDK Retry Verification (F1-F4)
- [ ] 6.1 — F1: No retries when disabled
- [ ] 6.2 — F2: Max retries exhausted
- [ ] 6.3 — F3: 503 with retries (may skip)
- [ ] 6.4 — F4: 408 with retries (may skip)

### Phase 7: Documentation & Release
- [ ] 7.1 — Update wiki Known-Limitations.md (if any new limitations discovered)
- [ ] 7.2 — Update wiki Features.md (add Fault Injection section if not present)
- [ ] 7.3 — Update wiki Feature-Comparison-With-Alternatives.md (fault injection row already exists; add detail)
- [ ] 7.4 — Update wiki API-Reference.md (FaultInjector, FaultInjectorIncludesMetadata docs)
- [ ] 7.5 — Update README.md (if fault injection is underrepresented)
- [ ] 7.6 — Increment version to 2.0.5 in .csproj
- [ ] 7.7 — `git add -A && git commit && git tag v2.0.5 && git push && git push --tags`
- [ ] 7.8 — Push wiki changes

---

## Test Structure Notes

### Skip Pattern
For tests that can't be implemented, use:
```csharp
[Fact(Skip = "Detailed reason explaining why this can't be tested through FakeCosmosHandler")]
public async Task FaultInjection_ReadMany_429()
{
    // Full test body written as if it would work — preserved for future implementation
}

/// <summary>
/// Sister test for FaultInjection_ReadMany_429 — demonstrates the divergent behaviour.
/// ReadMany goes through the SDK's internal batch pipeline which may not route through
/// FakeCosmosHandler's SendAsync in a way that's interceptable by FaultInjector.
/// This test documents what actually happens when fault injection is configured and
/// ReadMany is called.
/// </summary>
[Fact]
public async Task FaultInjection_ReadMany_429_DivergentBehavior_BypassesFaultInjection()
{
    // Detailed inline comments explaining the divergence
}
```

### Test Helper
Consider extracting a shared helper to reduce boilerplate:
```csharp
private static (FakeCosmosHandler handler, CosmosClient client, Container container) 
    CreateFaultableSetup(InMemoryContainer backing, 
        Func<HttpRequestMessage, HttpResponseMessage?>? injector = null,
        bool includeMetadata = false,
        int maxRetries = 0);
```

---

## Estimated Test Count
- **New tests:** ~35-40
- **Bug fixes:** 4
- **Skipped with sister tests:** ~3-5 (exact count depends on implementation feasibility)
- **Total after completion:** ~45-50 fault injection tests

---

## Progress Tracking

| Phase | Status | Notes |
|-------|--------|-------|
| Phase 0: Bug Fixes | ⬜ Not Started | |
| Phase 1: Operation Tests | ⬜ Not Started | |
| Phase 2: Status Codes | ⬜ Not Started | |
| Phase 3: Response Fidelity | ⬜ Not Started | |
| Phase 4: Dynamic Patterns | ⬜ Not Started | |
| Phase 5: Infrastructure | ⬜ Not Started | |
| Phase 6: SDK Retry | ⬜ Not Started | |
| Phase 7: Docs & Release | ⬜ Not Started | |
