# Stream CRUD Deep-Dive — TDD Plan

**Date:** April 1, 2026  
**Current version:** 2.0.4  
**Target version:** 2.0.5  
**File:** `tests/CosmosDB.InMemoryEmulator.Tests/StreamCrudTests.cs`  
**Implementation:** `src/CosmosDB.InMemoryEmulator/InMemoryContainer.cs`

---

## Executive Summary

StreamCrudTests.cs has ~33 tests across 7 classes covering the stream CRUD API surface. While basic happy paths and error status codes are tested, there are **3 bugs**, **~40 missing tests**, and **3 divergent behaviours** to document. The stream API contract ("only throws on client-side exceptions") is violated in two places.

---

## A) Bugs to Fix (Red → Green → Refactor)

### BUG-1: InvalidJSON throws raw JsonReaderException instead of returning 400 BadRequest
- **Location:** `CreateItemStreamAsync` (L702), `UpsertItemStreamAsync` (L791), `ReplaceItemStreamAsync` (L874)
- **Problem:** `JsonParseHelpers.ParseJson(json)` throws `Newtonsoft.Json.JsonReaderException` when the stream contains invalid JSON. The real Cosmos SDK stream API returns `ResponseMessage` with `400 BadRequest` status — it never throws for request errors.
- **Impact:** Any caller passing malformed JSON gets an unhandled exception instead of a status code response.
- **Fix:** Wrap `ParseJson` in try-catch; on `JsonException`, return `CreateResponseMessage(HttpStatusCode.BadRequest)`.
- **Test (RED):** `CreateStream_InvalidJson_Returns400BadRequest` — pass `"{{not json}"`, assert `StatusCode == 400`, assert no exception thrown.
- **Test (RED):** `UpsertStream_InvalidJson_Returns400BadRequest`
- **Test (RED):** `ReplaceStream_InvalidJson_Returns400BadRequest`
- [ ] Tests written
- [ ] Tests RED
- [ ] Bug fixed
- [ ] Tests GREEN

### BUG-2: ValidateDocumentSize throws CosmosException instead of returning 413 in stream methods
- **Location:** `CreateItemStreamAsync` (L700), `UpsertItemStreamAsync` (L789), `ReplaceItemStreamAsync` (L872), `PatchItemStreamAsync` (L999)
- **Problem:** `ValidateDocumentSize(json)` throws a `CosmosException` with 413 status. Stream methods should return `ResponseMessage` with `413 RequestEntityTooLarge`, not throw.
- **Impact:** Callers of stream API get an unexpected exception for oversized documents.
- **Fix:** Create a `ValidateDocumentSizeStream()` that returns a bool, or wrap in try-catch.
- **Test (RED):** `CreateStream_OversizedDocument_Returns413_DoesNotThrow`
- **Test (RED):** `UpsertStream_OversizedDocument_Returns413_DoesNotThrow`
- **Test (RED):** `ReplaceStream_OversizedDocument_Returns413_DoesNotThrow`
- **Test (RED):** `PatchStream_OversizedDocument_Returns413_DoesNotThrow` (patch that causes doc to exceed 2MB)
- [ ] Tests written
- [ ] Tests RED
- [ ] Bug fixed
- [ ] Tests GREEN

### BUG-3: EnableContentResponseOnWrite not respected in stream methods
- **Location:** All stream write methods — `CreateItemStreamAsync`, `UpsertItemStreamAsync`, `ReplaceItemStreamAsync`, `PatchItemStreamAsync`
- **Problem:** When `requestOptions.EnableContentResponseOnWrite == false`, the typed API (`CreateItemAsync<T>` etc.) correctly suppresses the response body. The stream variants always return the full body in `Content`.
- **Impact:** Callers optimizing bandwidth by suppressing write responses still receive the full body.
- **Fix:** Check `requestOptions?.EnableContentResponseOnWrite == false` and pass `null` for the json parameter to `CreateResponseMessage` when suppressed.
- **Test (RED):** `CreateStream_EnableContentResponseOnWrite_False_ContentIsNull`
- **Test (RED):** `UpsertStream_EnableContentResponseOnWrite_False_ContentIsNull`
- **Test (RED):** `ReplaceStream_EnableContentResponseOnWrite_False_ContentIsNull`
- **Test (RED):** `PatchStream_EnableContentResponseOnWrite_False_ContentIsNull`
- **Test (RED):** `CreateStream_EnableContentResponseOnWrite_True_ContentPopulated` (confirm default is populated)
- [ ] Tests written
- [ ] Tests RED
- [ ] Bug fixed
- [ ] Tests GREEN

---

## B) Missing Test Coverage — Response Body Validation

These tests verify the stream response `Content` contains correct and complete document data.

### B1: Create response contains created document
- **Test:** `CreateStream_ResponseContent_ContainsCreatedDocument`
- Parse response `Content` stream → verify all input fields present + `id` field set.
- [ ] Written | [ ] GREEN

### B2: Upsert-create response contains created document
- **Test:** `UpsertStream_NewItem_ResponseContent_ContainsDocument`
- [ ] Written | [ ] GREEN

### B3: Upsert-replace response contains updated document
- **Test:** `UpsertStream_ExistingItem_ResponseContent_ContainsUpdatedDocument`
- Verify old values NOT present, new values present.
- [ ] Written | [ ] GREEN

### B4: Replace response contains replaced document
- **Test:** `ReplaceStream_ResponseContent_ContainsReplacedDocument`
- [ ] Written | [ ] GREEN

### B5: Patch response contains patched document
- **Test:** `PatchStream_ResponseContent_ContainsPatchedDocument`
- [ ] Written | [ ] GREEN

### B6: Delete response Content is null
- **Test:** `DeleteStream_Success_ContentIsNull`
- Real Cosmos delete returns no body. Verify `response.Content == null`.
- [ ] Written | [ ] GREEN

### B7: Read response content round-trips through stream create
- **Test:** `ReadStream_AfterCreateStream_ReturnsCompleteDocument`
- Create via stream → Read via stream → verify data matches.
- [ ] Written | [ ] GREEN

---

## C) Missing Test Coverage — System Properties in Stream Responses

### C1: _etag present in stream response body
- **Test:** `CreateStream_ResponseBody_ContainsEtagSystemProperty`
- Parse body JSON → assert `_etag` property exists and matches `Headers.ETag`.
- [ ] Written | [ ] GREEN

### C2: _ts present in stream response body
- **Test:** `CreateStream_ResponseBody_ContainsTsSystemProperty`
- Parse body JSON → assert `_ts` is a valid Unix timestamp.
- [ ] Written | [ ] GREEN

### C3: System properties updated on upsert-replace
- **Test:** `UpsertStream_ExistingItem_UpdatesEtagAndTs`
- Create → capture _etag/_ts → Upsert → verify both changed.
- [ ] Written | [ ] GREEN

### C4: System properties updated on replace
- **Test:** `ReplaceStream_UpdatesEtagAndTs`
- [ ] Written | [ ] GREEN

### C5: System properties updated on patch
- **Test:** `PatchStream_UpdatesEtagAndTs`
- [ ] Written | [ ] GREEN

---

## D) Missing Test Coverage — IsSuccessStatusCode & EnsureSuccessStatusCode

### D1: Success responses have IsSuccessStatusCode = true
- **Test:** `Stream_SuccessResponses_HaveIsSuccessStatusCode_True`
- Create (201), Read (200), Upsert (201/200), Replace (200), Delete (204) — all should have `IsSuccessStatusCode == true`.
- [ ] Written | [ ] GREEN

### D2: Error responses have IsSuccessStatusCode = false
- **Test:** `Stream_ErrorResponses_HaveIsSuccessStatusCode_False`
- Read miss (404), Create duplicate (409), Replace miss (404), Delete miss (404) — all should have `IsSuccessStatusCode == false`.
- [ ] Written | [ ] GREEN

### D3: EnsureSuccessStatusCode on success returns self
- **Test:** `Stream_EnsureSuccessStatusCode_OnSuccess_ReturnsSelf`
- [ ] Written | [ ] GREEN

### D4: EnsureSuccessStatusCode on failure throws CosmosException
- **Test:** `Stream_EnsureSuccessStatusCode_OnFailure_ThrowsCosmosException`
- Read miss → `EnsureSuccessStatusCode()` → should throw `CosmosException` with StatusCode 404.
- [ ] Written | [ ] GREEN

---

## E) Missing Test Coverage — Response Headers

### E1: RequestCharge header on all stream responses
- **Test:** `Stream_AllCrudResponses_ContainRequestChargeHeader`
- Create, Read, Upsert, Replace, Delete, Patch — all should have `x-ms-request-charge`.
- [ ] Written | [ ] GREEN

### E2: ActivityId header on all stream responses
- **Test:** `Stream_AllCrudResponses_ContainActivityIdHeader`
- All responses should have `x-ms-activity-id` set to a non-empty GUID-like value.
- [ ] Written | [ ] GREEN

### E3: ETag header on write responses only
- **Test:** `Stream_WriteResponses_ContainETagHeader`
- Create, Upsert, Replace, Patch responses should have `ETag` header set.
- [ ] Written | [ ] GREEN

### E4: ETag header NOT on error responses
- **Test:** `Stream_ErrorResponses_DoNotContainETagHeader`
- 404, 409, 412 responses should NOT have an ETag header.
- [ ] Written | [ ] GREEN

---

## F) Missing Test Coverage — ETag Lifecycle in Stream API

### F1: ETag changes after stream upsert
- **Test:** `UpsertStream_ChangesETag`
- Create → record etag → Upsert → verify etag changed.
- [ ] Written | [ ] GREEN

### F2: ETag changes after stream replace
- **Test:** `ReplaceStream_ChangesETag`
- [ ] Written | [ ] GREEN

### F3: ETag consistent across consecutive reads
- **Test:** `ReadStream_ConsecutiveReads_ETagConsistent`
- Two reads without writes → ETag should be identical.
- [ ] Written | [ ] GREEN

### F4: IfMatch wildcard "*" always succeeds on write
- **Test:** `UpsertStream_WithIfMatch_Wildcard_AlwaysSucceeds`
- **Test:** `ReplaceStream_WithIfMatch_Wildcard_AlwaysSucceeds`
- **Test:** `DeleteStream_WithIfMatch_Wildcard_AlwaysSucceeds`
- [ ] Written | [ ] GREEN

### F5: IfNoneMatch wildcard on read returns 304 if item exists
- **Test:** `ReadStream_IfNoneMatch_Wildcard_Returns304`
- [ ] Written | [ ] GREEN

### F6: IfNoneMatch with stale etag on read returns 200
- **Test:** `ReadStream_IfNoneMatch_StaleEtag_Returns200WithContent`
- [ ] Written | [ ] GREEN

### F7: IfMatch on upsert of non-existent item
- **Test:** `UpsertStream_IfMatch_OnNonExistentItem_Returns412`
- When IfMatchEtag is set but no item exists, the etag can't possibly match → 412.
- NOTE: This may need to check actual behavior. If no item exists and IfMatchEtag is set, CheckIfMatchStream looks up key in _etags — it won't find it, so stored etag is null, which != the requested etag → returns false → 412. This is correct.
- [ ] Written | [ ] GREEN

### F8: Delete with IfMatch current etag succeeds
- **Test:** `DeleteStream_WithIfMatch_CurrentETag_Succeeds`
- [ ] Written | [ ] GREEN

---

## G) Missing Test Coverage — Data Integrity

### G1: Stream create → typed read returns same data
- **Test:** `CreateStream_ThenTypedRead_DataRoundTrips`
- Create via stream → Read via `ReadItemAsync<T>` → verify properties match.
- [ ] Written | [ ] GREEN

### G2: Typed create → stream read returns correct data
- **Test:** `TypedCreate_ThenStreamRead_DataRoundTrips`
- [ ] Written | [ ] GREEN

### G3: Stream upsert replaces entire document, not merge
- **Test:** `UpsertStream_ReplacesEntireDocument_NotMerge`
- Create with fields A+B → Upsert with only A → Read → verify B is missing.
- [ ] Written | [ ] GREEN

### G4: Stream delete → read returns 404
- **Test:** `DeleteStream_ThenRead_Returns404`
- [ ] Written | [ ] GREEN

### G5: Stream delete → can recreate same id
- **Test:** `DeleteStream_ThenCreate_SameId_Succeeds`
- [ ] Written | [ ] GREEN

### G6: Stream replace → data is fully replaced
- **Test:** `ReplaceStream_FullyReplacesDocument`
- Create with fields A+B → Replace with only A → Read → verify B is missing.
- [ ] Written | [ ] GREEN

---

## H) Missing Test Coverage — Edge Cases

### H1: Empty stream input
- **Test:** `CreateStream_EmptyStream_Returns400OrThrows`
- Pass `new MemoryStream()` → should return 400 BadRequest (or throw? — check behaviour).
- This is related to BUG-1. Empty JSON string will fail parsing.
- [ ] Written | [ ] GREEN

### H2: PartitionKey.None with stream methods
- **Test:** `CreateStream_WithPartitionKeyNone_ExtractsFromDocument`
- Pass PartitionKey.None → document has pk field → should extract pk from body.
- [ ] Written | [ ] GREEN

### H3: Composite partition key with stream methods
- **Test:** `CreateStream_CompositePartitionKey_ExtractsCorrectly`
- Container with composite pk (`/field1`, `/field2`) → stream create → verify both pk components extracted.
- [ ] Written | [ ] GREEN

### H4: Special characters in id via stream
- **Test:** `CreateStream_SpecialCharactersInId_Succeeds`
- Theory with: `"item/1"`, `"item#1"`, `"item 1"`, `"item?1"`
- [ ] Written | [ ] GREEN

### H5: Unicode content in stream
- **Test:** `CreateStream_UnicodeContent_PreservedInResponse`
- Create with Japanese/emoji content → Read → verify exact match.
- [ ] Written | [ ] GREEN

### H6: Stream create records in change feed
- **Test:** `CreateStream_RecordsInChangeFeed`
- Create via stream → Read change feed → verify item appears.
- [ ] Written | [ ] GREEN

### H7: Stream delete records tombstone in change feed
- **Test:** `DeleteStream_RecordsTombstoneInChangeFeed`
- Create → Delete via stream → Read change feed (all versions) → verify tombstone.
- [ ] Written | [ ] GREEN

### H8: CancellationToken respected in all stream methods
- **Test:** `AllStreamMethods_WithCancelledToken_ThrowOperationCancelled`
- Verify all 6 CRUD stream methods throw `OperationCanceledException` with a pre-cancelled token.
- NOTE: `CreateItemStream_WithCancelledToken_ThrowsOperationCancelled` already exists in `CancellationTokenTests5`. Add Read, Upsert, Replace, Delete, Patch.
- [ ] Written | [ ] GREEN

---

## I) Divergent Behaviours (Skip + Sister Tests)

### DIV-1: ReplaceStream — id parameter vs body id mismatch
- **Real Cosmos DB:** Returns `400 BadRequest` when the `id` in the request body doesn't match the `id` URL parameter.
- **InMemoryContainer:** Uses only the `id` parameter for key lookup; body `id` is ignored. Implementing full validation requires parsing the body to compare ids, and deciding what to do with the body's id field; this is complex given the stream API should be lightweight.
- **Skipped Test:** `ReplaceStream_IdMismatch_Returns400` — skip reason: "Real Cosmos DB returns 400 when body id differs from parameter id. InMemoryContainer uses parameter id for lookup; body id is not validated. Implementing this requires parsing stream to extract body id and comparing, which adds overhead to the stream path."
- **Sister Test (passing):** `ReplaceStream_IdMismatch_UsesParameterId_InMemory` — document that the emulator uses the parameter id regardless of body id. Heavy inline comments explaining the divergence.
- [ ] Skipped test written
- [ ] Sister test written and GREEN

### DIV-2: Missing _rid, _self, _attachments system properties
- **Real Cosmos DB:** Response body includes `_rid`, `_self`, `_attachments` fields.
- **InMemoryContainer:** Only enriches with `_etag` and `_ts`. The other system properties are omitted.
- **Skipped Test:** `Stream_ResponseBody_ContainsAllSystemProperties` — skip reason: "Real Cosmos DB includes _rid, _self, and _attachments system properties in response bodies. InMemoryContainer only enriches with _etag and _ts. Adding the remaining properties would require generating synthetic RID-style identifiers and URI-based _self references which are not meaningful for in-memory testing."
- **Sister Test (passing):** `Stream_ResponseBody_ContainsOnlyEtagAndTs_InMemory` — document that only _etag/_ts are present.
- [ ] Skipped test written
- [ ] Sister test written and GREEN

### DIV-3: No ErrorMessage on failure ResponseMessages
- **Real Cosmos DB:** Failure `ResponseMessage` objects have `ErrorMessage` set with a human-readable error string, and `Content` may contain a JSON error body.
- **InMemoryContainer:** `CreateResponseMessage` for errors passes no body and sets no `ErrorMessage`.
- **Skipped Test:** `Stream_ErrorResponse_ContainsErrorMessage` — skip reason: "Real Cosmos DB sets ErrorMessage on failure ResponseMessages with a human-readable description. InMemoryContainer's CreateResponseMessage helper doesn't set ErrorMessage for error status codes. Adding synthetic error messages is low priority since callers typically switch on StatusCode."
- **Sister Test (passing):** `Stream_ErrorResponse_ErrorMessageIsNull_InMemory` — document that ErrorMessage is null for error responses.
- [ ] Skipped test written
- [ ] Sister test written and GREEN

---

## J) Test Execution Order

TDD sequence — test first, red, green, refactor:

### Phase 1: Write ALL new tests (RED)
1. Write BUG-1 tests (invalid JSON → 400) — will be RED
2. Write BUG-2 tests (oversized document → 413 without throw) — will be RED
3. Write BUG-3 tests (EnableContentResponseOnWrite) — will be RED
4. Write B1–B7 (response body validation) — should be GREEN already
5. Write C1–C5 (system properties) — should be GREEN already
6. Write D1–D4 (IsSuccessStatusCode + EnsureSuccessStatusCode) — should be GREEN already
7. Write E1–E4 (response headers) — should be GREEN already
8. Write F1–F8 (ETag lifecycle) — should be GREEN already
9. Write G1–G6 (data integrity) — should be GREEN already
10. Write H1–H8 (edge cases) — H1 will be RED (related to BUG-1), rest should be GREEN
11. Write I (DIV-1, DIV-2, DIV-3) — skipped + sister tests

### Phase 2: Fix bugs (GREEN)
1. Fix BUG-1: Wrap `ParseJson` in try-catch in stream methods → return 400
2. Fix BUG-2: Wrap `ValidateDocumentSize` in try-catch in stream methods → return 413
3. Fix BUG-3: Check `EnableContentResponseOnWrite` in stream methods
4. Run ALL tests → confirm GREEN

### Phase 3: Refactor
1. Consider extracting common try-catch pattern for stream methods
2. No over-engineering — only if duplication is excessive

### Phase 4: Update Documentation
- **Known-Limitations.md:** Add DIV-1 (Replace id mismatch), DIV-2 (system properties), DIV-3 (error messages) — OR check if these are already covered
- **Features.md:** Update Stream API section to mention EnableContentResponseOnWrite support, error handling contract
- **Feature-Comparison-With-Alternatives.md:** Verify stream API rows are still accurate
- **README.md:** No changes expected (stream API already mentioned)
- **Known-Limitations.md behavioural differences:** Add new numbered items for each divergent behaviour

### Phase 5: Version, Tag, Push
1. Bump `<Version>2.0.4</Version>` → `<Version>2.0.5</Version>` in `CosmosDB.InMemoryEmulator.csproj`
2. `git add -A`
3. `git commit -m "v2.0.5: Stream API hardening — fix 3 bugs, add ~40 tests, document 3 divergent behaviours"`
4. `git tag v2.0.5`
5. `git push; git push --tags`
6. Wiki: `git add -A; git commit -m "v2.0.5: Stream API — update Known Limitations, Features, Comparison"; git push`

---

## Test Count Summary

| Category | New Tests | Currently RED |
|----------|-----------|--------------|
| A) BUG-1: Invalid JSON | 3 | 3 |
| A) BUG-2: Oversized doc | 4 | 4 |
| A) BUG-3: EnableContentResponseOnWrite | 5 | 4 |
| B) Response body validation | 7 | 0 |
| C) System properties | 5 | 0 |
| D) IsSuccessStatusCode | 4 | 0 |
| E) Response headers | 4 | 0 |
| F) ETag lifecycle | 10 | 0–1 |
| G) Data integrity | 6 | 0 |
| H) Edge cases | 8 | 1 |
| I) Divergent behaviours | 6 (3 skipped + 3 sister) | 0 |
| **TOTAL** | **~62** | **~12** |

---

## Files Modified

| File | Changes |
|------|---------|
| `tests/.../StreamCrudTests.cs` | Add ~62 new tests across new test classes |
| `src/.../InMemoryContainer.cs` | Fix BUG-1 (invalid JSON), BUG-2 (doc size), BUG-3 (EnableContentResponseOnWrite) |
| `src/.../CosmosDB.InMemoryEmulator.csproj` | Version bump 2.0.4 → 2.0.5 |
| `wiki/Known-Limitations.md` | Add 3 new divergent behaviours (DIV-1, DIV-2, DIV-3) |
| `wiki/Features.md` | Update Stream API section |
| `wiki/Feature-Comparison-With-Alternatives.md` | Verify/update stream rows |
