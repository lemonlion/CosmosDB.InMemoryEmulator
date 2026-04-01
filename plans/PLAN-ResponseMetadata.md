# Response Metadata Tests — Deep Dive Plan

**Target file:** `tests/CosmosDB.InMemoryEmulator.Tests/ResponseMetadataTests.cs`  
**Approach:** TDD (Red-Green-Refactor). Write test → see it fail → fix source → see it pass.  
**Version bump:** 2.0.4 → 2.0.5 after all changes.

---

## Current State

The file has 3 test classes (`ResponseMetadataGapTests`, `GapTests3`, `GapTests4`) with **6 total tests** covering only:

| # | Test | What it checks |
|---|------|----------------|
| 1 | `Response_RequestCharge_PositiveOnWrite` | RequestCharge > 0 on Create (typed) |
| 2 | `Response_ETag_SetOnAllWriteOperations` | ETag present on Create/Upsert/Replace/Read (typed) |
| 3 | `Response_Diagnostics_NotNull` | Diagnostics not null on Create (typed) |
| 4 | `StreamResponse_Headers_ContainETag_AfterWrite` | ETag header on stream Create |
| 5 | `Response_ActivityId_NotNull` | ActivityId is valid GUID on Create (typed) |
| 6 | `Response_Headers_ContainStandardCosmosHeaders` | ETag, x-ms-activity-id, x-ms-request-charge in stream Create headers |

**Problems with current tests:**
- All assertions are only on **Create** operations
- No coverage for Read, Upsert, Replace, Delete, Patch, Query, Batch, Database, Container
- No coverage for **error** response metadata (404, 409, 412, 304)
- No coverage for **stream API** beyond Create
- Test class naming is inconsistent (GapTests, GapTests3, GapTests4)
- Tests 2 and 4 overlap in asserting ETag on create

---

## Bugs Discovered

### BUG-1: InMemoryStreamFeedIterator creates ResponseMessage without any headers

**File:** `src/CosmosDB.InMemoryEmulator/InMemoryStreamFeedIterator.cs`  
**Issue:** `ReadNextAsync` creates `new ResponseMessage(HttpStatusCode.OK)` directly instead of using `CreateResponseMessage()`. Result: no `x-ms-activity-id`, no `x-ms-request-charge` headers.  
**Fix:** Add standard headers to the ResponseMessage construction.

### BUG-2: InMemoryFeedResponse.ActivityId returns empty string instead of GUID

**File:** `src/CosmosDB.InMemoryEmulator/InMemoryFeedIterator.cs` (Lines ~113-124)  
**Also:** `src/CosmosDB.InMemoryEmulator/InMemoryContainer.cs` (InMemoryFeedResponse class)  
**Issue:** `ActivityId => string.Empty` — real Cosmos SDK returns a GUID.  
**Fix:** Return `Guid.NewGuid().ToString()` or a cached GUID per response.

### BUG-3: InMemoryFeedResponse.Headers is empty — no metadata headers

**File:** Same as BUG-2.  
**Issue:** `Headers { get; } = new()` — no `x-ms-request-charge`, no `x-ms-activity-id`. Code that reads `feedResponse.Headers["x-ms-request-charge"]` will get null even though `feedResponse.RequestCharge` returns 1.0.  
**Fix:** Populate the Headers collection to match the property values.

### BUG-4: ReadItemStreamAsync 304 NotModified does not include ETag in headers

**File:** `src/CosmosDB.InMemoryEmulator/InMemoryContainer.cs` (Line ~758)  
**Issue:** `CreateResponseMessage(HttpStatusCode.NotModified)` is called without the ETag. Real Cosmos returns the current ETag in 304 responses so clients know which version they already have.  
**Fix:** Pass the current ETag to `CreateResponseMessage`.

### BUG-5: DatabaseResponse / ContainerResponse missing standard metadata

**File:** `src/CosmosDB.InMemoryEmulator/InMemoryDatabase.cs`  
**Issue:** `BuildContainerResponse` and database Read/Delete construct substitutes with only StatusCode and Resource — no Headers, RequestCharge, ActivityId, or Diagnostics.  
**Fix:** Add metadata stubs matching the pattern used by CreateItemResponse.

### BUG-6: Direct `new ResponseMessage(Conflict)` in InMemoryCosmosClient and InMemoryDatabase

**File:** `src/CosmosDB.InMemoryEmulator/InMemoryCosmosClient.cs` (Line ~127), `InMemoryDatabase.cs` (Lines ~142, ~273)  
**Issue:** These conflict responses are constructed without headers — no activity-id, no request-charge.  
**Fix:** Set standard headers on these ResponseMessages.

---

## Plan — Tests to Write

All tests go in a single reorganized `ResponseMetadataTests.cs`. The file will be restructured into clearly named test classes.

### Class 1: `TypedResponseStatusCodeTests`

Tests that each typed CRUD operation returns the correct HttpStatusCode.

| # | Test Name | Assertion | Needs Source Fix? |
|---|-----------|-----------|-------------------|
| T01 | `CreateItemAsync_Returns_Created` | StatusCode == 201 | No |
| T02 | `ReadItemAsync_Returns_OK` | StatusCode == 200 | No |
| T03 | `UpsertItemAsync_NewItem_Returns_Created` | StatusCode == 201 | No |
| T04 | `UpsertItemAsync_ExistingItem_Returns_OK` | StatusCode == 200 | No |
| T05 | `ReplaceItemAsync_Returns_OK` | StatusCode == 200 | No |
| T06 | `DeleteItemAsync_Returns_NoContent` | StatusCode == 204 | No |
| T07 | `PatchItemAsync_Returns_OK` | StatusCode == 200 | No |

### Class 2: `TypedResponseMetadataTests`

Tests that every typed CRUD operation populates standard metadata properties.

| # | Test Name | Assertion | Needs Source Fix? |
|---|-----------|-----------|-------------------|
| T08 | `CreateItemAsync_HasRequestCharge_ActivityId_Diagnostics_ETag_Headers` | All 5 metadata props present | No |
| T09 | `ReadItemAsync_HasRequestCharge_ActivityId_Diagnostics_ETag_Headers` | All 5 metadata props present | No |
| T10 | `UpsertItemAsync_HasRequestCharge_ActivityId_Diagnostics_ETag_Headers` | All 5 metadata props present | No |
| T11 | `ReplaceItemAsync_HasRequestCharge_ActivityId_Diagnostics_ETag_Headers` | All 5 metadata props present | No |
| T12 | `DeleteItemAsync_HasRequestCharge_ActivityId_Diagnostics_Headers` | Charge, ActivityId, Diagnostics, Headers present | No |
| T13 | `PatchItemAsync_HasRequestCharge_ActivityId_Diagnostics_ETag_Headers` | All 5 metadata props present | No |
| T14 | `ActivityId_IsUnique_PerOperation` | Two consecutive creates have different ActivityIds | No |
| T15 | `ETag_IsQuotedString` | ETag matches `"..."` pattern (HTTP quoted-string) | No |
| T16 | `Diagnostics_GetClientElapsedTime_ReturnsTimeSpan` | Diagnostics.GetClientElapsedTime() == TimeSpan.Zero | No |
| T17 | `Headers_ContainSessionToken` | Headers["x-ms-session-token"] is not null | No |

### Class 3: `TypedResponseContentSuppressionTests`

Tests for `enableContentResponseOnWrite` / `ItemRequestOptions.EnableContentResponseOnWrite = false`.

| # | Test Name | Assertion | Needs Source Fix? |
|---|-----------|-----------|-------------------|
| T18 | `CreateItemAsync_SuppressContent_ResourceIsDefault` | response.Resource == null when suppressed | No |
| T19 | `UpsertItemAsync_SuppressContent_ResourceIsDefault` | Same for upsert | No |
| T20 | `ReplaceItemAsync_SuppressContent_ResourceIsDefault` | Same for replace | No |
| T21 | `PatchItemAsync_SuppressContent_ResourceIsDefault` | Same for patch | No |
| T22 | `DeleteItemAsync_ResourceIsDefault` | Delete always returns default(T) | No |

### Class 4: `StreamResponseStatusCodeTests`

Tests that stream CRUD operations return correct status codes.

| # | Test Name | Assertion | Needs Source Fix? |
|---|-----------|-----------|-------------------|
| T23 | `CreateItemStreamAsync_Returns_Created` | StatusCode == 201 | No |
| T24 | `ReadItemStreamAsync_Returns_OK` | StatusCode == 200 | No |
| T25 | `UpsertItemStreamAsync_NewItem_Returns_Created` | StatusCode == 201 | No |
| T26 | `UpsertItemStreamAsync_ExistingItem_Returns_OK` | StatusCode == 200 | No |
| T27 | `ReplaceItemStreamAsync_Returns_OK` | StatusCode == 200 | No |
| T28 | `DeleteItemStreamAsync_Returns_NoContent` | StatusCode == 204 | No |
| T29 | `PatchItemStreamAsync_Returns_OK` | StatusCode == 200 | No |

### Class 5: `StreamResponseHeaderTests`

Tests that stream responses include standard Cosmos headers.

| # | Test Name | Assertion | Needs Source Fix? |
|---|-----------|-----------|-------------------|
| T30 | `StreamCreate_HasActivityId_RequestCharge_ETag` | 3 headers present | No |
| T31 | `StreamRead_HasActivityId_RequestCharge_ETag` | 3 headers present | No |
| T32 | `StreamUpsert_HasActivityId_RequestCharge_ETag` | 3 headers present | No |
| T33 | `StreamReplace_HasActivityId_RequestCharge_ETag` | 3 headers present | No |
| T34 | `StreamDelete_HasActivityId_RequestCharge` | No ETag on delete (correct) | No |
| T35 | `StreamPatch_HasActivityId_RequestCharge_ETag` | 3 headers present | No |

### Class 6: `ErrorResponseStatusCodeTests`

Tests that error conditions return correct HTTP status codes.

| # | Test Name | Assertion | Needs Source Fix? |
|---|-----------|-----------|-------------------|
| T36 | `CreateItemAsync_Duplicate_Throws_Conflict_409` | CosmosException.StatusCode == 409 | No |
| T37 | `ReadItemAsync_NotFound_Throws_404` | CosmosException.StatusCode == 404 | No |
| T38 | `ReplaceItemAsync_NotFound_Throws_404` | CosmosException.StatusCode == 404 | No |
| T39 | `DeleteItemAsync_NotFound_Throws_404` | CosmosException.StatusCode == 404 | No |
| T40 | `PatchItemAsync_NotFound_Throws_404` | CosmosException.StatusCode == 404 | No |
| T41 | `ReplaceItemAsync_StaleETag_Throws_PreconditionFailed_412` | CosmosException.StatusCode == 412 | No |
| T42 | `PatchItemAsync_FilterPredicate_Throws_PreconditionFailed_412` | CosmosException.StatusCode == 412 | No |
| T43 | `ReadItemAsync_IfNoneMatch_Returns_NotModified_304` | CosmosException.StatusCode == 304 | No |

### Class 7: `StreamErrorResponseHeaderTests`

Tests that error stream responses still include standard headers.

| # | Test Name | Assertion | Needs Source Fix? |
|---|-----------|-----------|-------------------|
| T44 | `Stream_NotFound_HasActivityId_RequestCharge` | 404 response has headers | No |
| T45 | `Stream_Conflict_HasActivityId_RequestCharge` | 409 response has headers | No |
| T46 | `Stream_PreconditionFailed_HasActivityId_RequestCharge` | 412 response has headers | No |
| T47 | `Stream_NotModified_HasActivityId_RequestCharge_ETag` | 304 response has headers + ETag | **YES — BUG-4** |

### Class 8: `FeedResponseMetadataTests`

Tests for query/feed iterator response metadata.

| # | Test Name | Assertion | Needs Source Fix? |
|---|-----------|-----------|-------------------|
| T48 | `FeedResponse_HasRequestCharge` | RequestCharge > 0 | No |
| T49 | `FeedResponse_HasDiagnostics` | Diagnostics not null | No |
| T50 | `FeedResponse_HasStatusCode_OK` | StatusCode == 200 | No |
| T51 | `FeedResponse_ActivityId_IsValidGuid` | ActivityId is GUID format | **YES — BUG-2** |
| T52 | `FeedResponse_Headers_ContainRequestCharge` | Headers["x-ms-request-charge"] is set | **YES — BUG-3** |
| T53 | `StreamFeedIterator_HasActivityId_RequestCharge` | Stream feed iterator headers | **YES — BUG-1** |

### Class 9: `BatchResponseMetadataTests`

Tests for transactional batch response metadata.

| # | Test Name | Assertion | Needs Source Fix? |
|---|-----------|-----------|-------------------|
| T54 | `BatchResponse_HasStatusCode` | StatusCode is set | No |
| T55 | `BatchResponse_HasRequestCharge` | RequestCharge > 0 | No |
| T56 | `BatchResponse_HasCount` | Count matches number of operations | No |
| T57 | `BatchResponse_IsSuccessStatusCode_TrueOnSuccess` | IsSuccessStatusCode == true | No |
| T58 | `BatchResponse_PerOperation_HasStatusCode` | Each op result has StatusCode | No |

### Class 10: `DatabaseContainerResponseMetadataTests`

Tests for database and container management response metadata.

| # | Test Name | Assertion | Needs Source Fix? |
|---|-----------|-----------|-------------------|
| T59 | `CreateDatabaseAsync_Returns_StatusCode_Created` | StatusCode == 201 | No (need to check) |
| T60 | `ReadDatabaseAsync_Returns_StatusCode_OK` | StatusCode == 200 | No |
| T61 | `DeleteDatabaseAsync_Returns_StatusCode_NoContent` | StatusCode == 204 | No |
| T62 | `CreateContainerAsync_Returns_StatusCode_Created` | StatusCode == 201 | No |
| T63 | `CreateContainerAsync_HasResource` | Resource has container properties | No |
| T64 | `DatabaseResponse_HasRequestCharge_ActivityId` | Metadata is populated | **YES — BUG-5** (SKIP candidate) |
| T65 | `ContainerResponse_HasRequestCharge_ActivityId` | Metadata is populated | **YES — BUG-5** (SKIP candidate) |

### Class 11: `ResponseMetadataDivergentBehaviorTests`

Tests that document known divergent behavior with detailed skip reasons and matching "emulator behavior" tests.

| # | Test Name | Skip? | Divergent Reason |
|---|-----------|-------|------------------|
| T66 | `RequestCharge_ShouldVaryByOperationType` | **SKIP** | Real Cosmos: writes ~6-10 RU, reads ~1 RU, queries vary by complexity. Emulator: always 1.0 RU. |
| T67 | `RequestCharge_EmulatorBehavior_AlwaysSynthetic1RU` | No (sister test) | Documents that all ops return 1.0 |
| T68 | `Diagnostics_ShouldContainQueryPlanAndTimings` | **SKIP** | Real Cosmos: Diagnostics contain query plan, execution time, retries, contact regions. Emulator: stub returns TimeSpan.Zero. |
| T69 | `Diagnostics_EmulatorBehavior_StubReturnsZeroElapsed` | No (sister test) | Documents the stub behavior |
| T70 | `SessionToken_ShouldBeCumulative` | **SKIP** | Real Cosmos: session tokens are cumulative LSN-based (e.g. "0:0#12345"). Emulator: random GUID per response with no cumulative state. |
| T71 | `SessionToken_EmulatorBehavior_RandomPerResponse` | No (sister test) | Documents the random behavior |

---

## Implementation Order (TDD Sequence)

### Phase 1: Restructure & Existing Coverage (no source changes)
1. Refactor file into organized test classes
2. Write T01–T07 (Typed status codes) — should all pass GREEN immediately
3. Write T08–T17 (Typed metadata) — should all pass GREEN immediately
4. Write T18–T22 (Content suppression) — should all pass GREEN immediately
5. Write T23–T29 (Stream status codes) — should all pass GREEN immediately
6. Write T30–T35 (Stream headers) — should all pass GREEN immediately
7. Write T36–T43 (Error status codes) — should all pass GREEN immediately
8. Write T44–T46 (Stream error headers) — should all pass GREEN immediately
9. Write T54–T58 (Batch metadata) — should all pass GREEN immediately
10. Write T59–T63 (Database/container status codes) — should all pass GREEN immediately

### Phase 2: Bug Fix Tests (RED → fix → GREEN)
11. Write T47 (Stream 304 ETag) — **RED**. Fix BUG-4 → GREEN
12. Write T51 (FeedResponse ActivityId) — **RED**. Fix BUG-2 → GREEN
13. Write T52 (FeedResponse headers) — **RED**. Fix BUG-3 → GREEN
14. Write T53 (StreamFeedIterator headers) — **RED**. Fix BUG-1 → GREEN

### Phase 3: Skipped Tests + Divergent Behavior
15. Write T64, T65 — evaluate difficulty. If too hard, **SKIP** with reason + write sister tests documenting current behavior. Fix BUG-5 if feasible, otherwise SKIP.
16. Write T66–T71 (divergent behavior pairs)

### Phase 4: Bug Fix for External ResponseMessages
17. Fix BUG-6 (add headers to direct `new ResponseMessage(Conflict)` in InMemoryCosmosClient/InMemoryDatabase)
18. Write tests for BUG-6 if not already covered by T44–T46

### Phase 5: Finalize
19. Run full test suite — all GREEN (except intentionally skipped)
20. Update wiki Known-Limitations.md if any new limitations discovered
21. Update wiki Features.md if any new capabilities added
22. Update wiki Feature-Comparison-With-Alternatives.md if comparison impacts
23. Update README.md if necessary
24. Bump version 2.0.4 → 2.0.5 in `CosmosDB.InMemoryEmulator.csproj`
25. `git add -A && git commit && git tag v2.0.5 && git push && git push --tags`
26. Update wiki with commit referencing v2.0.5

---

## Documentation Updates Needed

### Known-Limitations.md
- Add or update entry on **session tokens being random** (if not already covered under "Consistency Levels")
- Add entry on **Diagnostics being a stub** (TimeSpan.Zero, no query plan) if not already present
- Reference new divergent behavior tests T66–T71

### Features.md
- Add section on **Response Metadata** documenting what's populated on each operation type
- Document that all responses include ActivityId, RequestCharge, Headers, Diagnostics

### Feature-Comparison-With-Alternatives.md  
- Add row for "Response Metadata Fidelity" comparing emulator vs real vs Docker emulator
- Add row for "CosmosDiagnostics" 

### README.md
- May need minor update if Features section references metadata

---

## Files That Will Be Modified

### Test files:
- `tests/CosmosDB.InMemoryEmulator.Tests/ResponseMetadataTests.cs` — full rewrite with 71 tests

### Source files (bug fixes):
- `src/CosmosDB.InMemoryEmulator/InMemoryContainer.cs` — BUG-4 (304 ETag)
- `src/CosmosDB.InMemoryEmulator/InMemoryFeedIterator.cs` — BUG-2 (ActivityId), BUG-3 (Headers)
- `src/CosmosDB.InMemoryEmulator/InMemoryStreamFeedIterator.cs` — BUG-1 (headers)
- `src/CosmosDB.InMemoryEmulator/InMemoryDatabase.cs` — BUG-5 (metadata), BUG-6 (conflict headers)
- `src/CosmosDB.InMemoryEmulator/InMemoryCosmosClient.cs` — BUG-6 (conflict headers)
- `src/CosmosDB.InMemoryEmulator/CosmosDB.InMemoryEmulator.csproj` — version bump

### Wiki files:
- `c:\git\CosmosDB.InMemoryEmulator.wiki\Known-Limitations.md`
- `c:\git\CosmosDB.InMemoryEmulator.wiki\Features.md`
- `c:\git\CosmosDB.InMemoryEmulator.wiki\Feature-Comparison-With-Alternatives.md`

### Other:
- `c:\git\CosmosDB.InMemoryEmulator\README.md` (if needed)

---

## Status Tracker

| ID | Test/Task | Status |
|----|-----------|--------|
| T01–T07 | Typed status code tests | ⬜ Not started |
| T08–T17 | Typed metadata tests | ⬜ Not started |
| T18–T22 | Content suppression tests | ⬜ Not started |
| T23–T29 | Stream status code tests | ⬜ Not started |
| T30–T35 | Stream header tests | ⬜ Not started |
| T36–T43 | Error status code tests | ⬜ Not started |
| T44–T47 | Stream error header tests | ⬜ Not started |
| T48–T53 | Feed response metadata tests | ⬜ Not started |
| T54–T58 | Batch response metadata tests | ⬜ Not started |
| T59–T65 | Database/container response tests | ⬜ Not started |
| T66–T71 | Divergent behavior tests | ⬜ Not started |
| BUG-1 | StreamFeedIterator missing headers | ⬜ Not started |
| BUG-2 | FeedResponse ActivityId empty | ⬜ Not started |
| BUG-3 | FeedResponse Headers empty | ⬜ Not started |
| BUG-4 | 304 NotModified missing ETag | ⬜ Not started |
| BUG-5 | Database/Container response metadata | ⬜ Not started |
| BUG-6 | Direct ResponseMessage missing headers | ⬜ Not started |
| DOCS | Wiki + README updates | ⬜ Not started |
| RELEASE | Version bump + tag + push | ⬜ Not started |
