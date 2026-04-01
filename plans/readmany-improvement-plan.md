# ReadMany Deep Dive — Test Coverage & Bug Fix Plan

**Date**: 2026-04-01  
**Current Version**: 2.0.4 → **Target Version**: 2.0.5  
**Scope**: `ReadManyItemsAsync<T>`, `ReadManyItemsStreamAsync`  
**File**: `tests/CosmosDB.InMemoryEmulator.Tests/ReadManyTests.cs`  
**Implementation**: `src/CosmosDB.InMemoryEmulator/InMemoryContainer.cs` lines 974–1013

---

## 1. Current State Analysis

### 1.1 Implementation (InMemoryContainer.cs:974–1013)

```csharp
// ReadManyItemsAsync<T>: loops items, TryGetValue from ConcurrentDictionary, deserializes with JsonSettings
// ReadManyItemsStreamAsync: same loop but builds JObject { Documents: JArray } envelope
```

**Observations**:
- `ReadManyRequestOptions` parameter is accepted but **completely ignored**
- No null-check on `items` parameter — will throw `NullReferenceException` (real SDK throws `ArgumentNullException`)
- Stream envelope only has `"Documents"` — real Cosmos also returns `"_count"` and `"_rid"`
- CancellationToken parameter is accepted but never checked

### 1.2 Existing Test Classes (6 classes, ~18 tests)

| Class | Tests | Notes |
|---|---|---|
| `ReadManyTests` | 6 | Core happy path + stream |
| `ReadManyGapTests` | 3 | **Duplicates** empty list, some missing, all missing |
| `ReadManyEdgeCaseTests` | 3 | Stream, duplicates, null list |
| `ReadManyStreamEdgeCaseTests5` | 2 | Stream empty + stream all missing |
| `ReadManyGapTests3` | 1 | Response.Count matches |
| `ReadManyGapTests2` | 3 | Weak duplicate assertion, large list 100+, stream |

### 1.3 Duplicate / Overlapping Tests

These pairs test the same thing:
1. `ReadManyTests.ReadManyItemsAsync_EmptyList_ReturnsEmpty` ≡ `ReadManyGapTests.ReadMany_EmptyList_ReturnsEmptyResponse`
2. `ReadManyTests.ReadManyItemsAsync_SomeNotExist_ReturnsOnlyExisting` ≡ `ReadManyGapTests.ReadMany_SomeItemsMissing_ReturnsOnlyExisting`
3. `ReadManyTests.ReadManyItemsAsync_NoneExist_ReturnsEmpty` ≡ `ReadManyGapTests.ReadMany_AllMissing_ReturnsEmpty`
4. `ReadManyEdgeCaseTests.ReadManyItemsAsync_DuplicateItems_InList_ReturnsDuplicates` ≡ `ReadManyGapTests2.ReadMany_DuplicateIds_InList` (weak version)
5. `ReadManyTests.ReadManyItemsStreamAsync_ReturnsStreamWithDocuments` overlaps with `ReadManyEdgeCaseTests.ReadManyItemsStreamAsync_AllExist_ReturnsOkStream` and `ReadManyGapTests2.ReadMany_StreamVariant_ReturnsResponse`

---

## 2. Bugs to Fix

### BUG-1: Null items list throws NullReferenceException instead of ArgumentNullException
- **Location**: `InMemoryContainer.cs` ReadManyItemsAsync (line ~984) and ReadManyItemsStreamAsync (line ~1004)
- **Current**: `foreach` on null → `NullReferenceException`
- **Expected**: Real Cosmos SDK throws `ArgumentNullException("items")`
- **Fix**: Add `ArgumentNullException.ThrowIfNull(items)` at top of both methods
- **Test**: Update existing `ReadManyEdgeCaseTests.ReadManyItemsAsync_NullList_Throws` to assert `ArgumentNullException` specifically (currently asserts generic `Exception`)

### BUG-2: Stream response envelope missing `_count` field  
- **Location**: `InMemoryContainer.cs` ReadManyItemsStreamAsync (line ~1011)
- **Current**: `{ "Documents": [...] }`
- **Expected**: `{ "Documents": [...], "_count": N }`
- **Fix**: Add `["_count"] = results.Count` to the JObject envelope
- **Test**: New test verifying `_count` in stream response

### BUG-3: Weak duplicate assertion in ReadManyGapTests2
- **Location**: `ReadManyTests.cs` `ReadManyGapTests2.ReadMany_DuplicateIds_InList`
- **Current**: `response.Resource.Should().HaveCountGreaterThanOrEqualTo(1)` — this assertion accepts 1 OR 2 items
- **Fix**: This is a duplicate test — will be removed during consolidation (the correct assertion already exists in `ReadManyEdgeCaseTests`)

---

## 3. Test Consolidation Plan

**Goal**: Merge 6 fragmented classes into 1 well-organized class.

**New structure** — single class `ReadManyTests` with regions:

```
ReadManyTests
├── #region Happy Path (typed)
│   ├── ReadMany_AllExist_ReturnsAll
│   ├── ReadMany_SingleItem_ReturnsIt
│   └── ReadMany_MixedPartitionKeys_ReturnsAll
├── #region Missing Items (typed)
│   ├── ReadMany_SomeNotExist_ReturnsOnlyExisting
│   ├── ReadMany_NoneExist_ReturnsEmpty
│   ├── ReadMany_WrongPartitionKey_DoesNotReturn
│   └── ReadMany_EmptyList_ReturnsEmpty
├── #region Duplicate Handling
│   ├── ReadMany_DuplicateIdsInList_ReturnsDuplicates
│   └── ReadMany_DuplicateIdsInList_Stream_ReturnsDuplicates
├── #region Stream Variant
│   ├── ReadManyStream_AllExist_ReturnsOkWithDocuments
│   ├── ReadManyStream_EmptyList_ReturnsOkWithEmptyDocuments
│   ├── ReadManyStream_AllMissing_ReturnsOkWithEmptyDocuments
│   └── ReadManyStream_ContainsCountField
├── #region Response Metadata
│   ├── ReadMany_StatusCode_IsOk
│   ├── ReadMany_Count_MatchesFoundItems
│   ├── ReadMany_RequestCharge_IsPositive
│   └── ReadMany_ActivityId_IsPresent
├── #region Error Handling
│   ├── ReadMany_NullList_ThrowsArgumentNullException
│   └── ReadManyStream_NullList_ThrowsArgumentNullException
├── #region Scale
│   └── ReadMany_LargeList_100Items_ReturnsAll
├── #region After Mutations
│   ├── ReadMany_AfterItemUpdate_ReturnsUpdatedVersion
│   ├── ReadMany_AfterItemDelete_ExcludesDeletedItem
│   └── ReadMany_AfterItemReplace_ReturnsReplacedVersion
├── #region Partition Key Edge Cases
│   ├── ReadMany_HierarchicalPartitionKey_ReturnsItems
│   ├── ReadMany_PartitionKeyNone_ReturnsItems
│   └── ReadMany_EmptyStringPartitionKey_ReturnsItems
├── #region ID Edge Cases
│   ├── ReadMany_CaseSensitiveIds_TreatedDistinct
│   ├── ReadMany_SpecialCharactersInId_ReturnsItem
│   └── ReadMany_UnicodeId_ReturnsItem
├── #region System Properties
│   ├── ReadMany_ResultsIncludeETag
│   ├── ReadMany_ResultsIncludeTimestamp
│   └── ReadManyStream_ResultsIncludeSystemProperties
└── #region Concurrency
    └── ReadMany_ConcurrentCalls_AllSucceed
```

---

## 4. New Tests to Write

### 4.1 Happy Path — Typed

| # | Test | Status | Notes |
|---|---|---|---|
| T01 | `ReadMany_AllExist_ReturnsAll` | KEEP | Existing, keep as-is |
| T02 | `ReadMany_SingleItem_ReturnsIt` | NEW | Verify single-item list works |
| T03 | `ReadMany_MixedPartitionKeys_ReturnsAll` | KEEP | Existing in ReadManyGapTests (keep stronger version) |

### 4.2 Missing Items — Typed

| # | Test | Status | Notes |
|---|---|---|---|
| T04 | `ReadMany_SomeNotExist_ReturnsOnlyExisting` | KEEP | Existing, remove duplicate |
| T05 | `ReadMany_NoneExist_ReturnsEmpty` | KEEP | Existing, remove duplicate |
| T06 | `ReadMany_WrongPartitionKey_DoesNotReturn` | KEEP | Existing |
| T07 | `ReadMany_EmptyList_ReturnsEmpty` | KEEP | Existing, remove duplicates |

### 4.3 Duplicate Handling

| # | Test | Status | Notes |
|---|---|---|---|
| T08 | `ReadMany_DuplicateIdsInList_ReturnsDuplicates` | KEEP | Keep strong assertion version (HaveCount(2)) |
| T09 | `ReadMany_DuplicateIdsInList_Stream_ReturnsDuplicates` | NEW | Stream variant of duplicate test |

### 4.4 Stream Variant

| # | Test | Status | Notes |
|---|---|---|---|
| T10 | `ReadManyStream_AllExist_ReturnsOkWithDocuments` | KEEP | Existing, merge best parts |
| T11 | `ReadManyStream_EmptyList_ReturnsOkWithEmptyDocuments` | KEEP | Existing |
| T12 | `ReadManyStream_AllMissing_ReturnsOkWithEmptyDocuments` | KEEP | Existing |
| T13 | `ReadManyStream_ContainsCountField` | NEW | Verifies `_count` field in JSON envelope (BUG-2 fix) |

### 4.5 Response Metadata

| # | Test | Status | Notes |
|---|---|---|---|
| T14 | `ReadMany_StatusCode_IsOk` | KEEP | Part of existing tests, make explicit |
| T15 | `ReadMany_Count_MatchesFoundItems` | KEEP | Existing in ReadManyGapTests3 |
| T16 | `ReadMany_RequestCharge_IsPositive` | NEW | Verify synthetic RequestCharge > 0 |
| T17 | `ReadMany_ActivityId_IsPresent` | NEW | Verify non-null ActivityId in headers |

### 4.6 Error Handling

| # | Test | Status | Notes |
|---|---|---|---|
| T18 | `ReadMany_NullList_ThrowsArgumentNullException` | FIX | Existing but asserts generic `Exception`; tighten to `ArgumentNullException` |
| T19 | `ReadManyStream_NullList_ThrowsArgumentNullException` | NEW | Stream variant null check |

### 4.7 Scale

| # | Test | Status | Notes |
|---|---|---|---|
| T20 | `ReadMany_LargeList_100Items_ReturnsAll` | KEEP | Existing 110-item test |

### 4.8 After Mutations

| # | Test | Status | Notes |
|---|---|---|---|
| T21 | `ReadMany_AfterItemUpdate_ReturnsUpdatedVersion` | NEW | Upsert then ReadMany, verify latest version |
| T22 | `ReadMany_AfterItemDelete_ExcludesDeletedItem` | NEW | Delete item then ReadMany, verify it's excluded |
| T23 | `ReadMany_AfterItemReplace_ReturnsReplacedVersion` | NEW | Replace then ReadMany, verify replaced content |

### 4.9 Partition Key Edge Cases

| # | Test | Status | Notes |
|---|---|---|---|
| T24 | `ReadMany_HierarchicalPartitionKey_ReturnsItems` | NEW | Use PartitionKeyBuilder with 2+ levels. May need Skip if hierarchical keys aren't wired through ReadMany's PartitionKeyToString. |
| T25 | `ReadMany_PartitionKeyNone_ReturnsItems` | NEW | Container with `/id` path, use `PartitionKey.None` |
| T26 | `ReadMany_EmptyStringPartitionKey_ReturnsItems` | NEW | `new PartitionKey("")` |

### 4.10 ID Edge Cases

| # | Test | Status | Notes |
|---|---|---|---|
| T27 | `ReadMany_CaseSensitiveIds_TreatsDistinct` | NEW | "ABC" vs "abc" are different documents |
| T28 | `ReadMany_SpecialCharactersInId_ReturnsItem` | NEW | IDs with spaces, slashes, dots, etc. |
| T29 | `ReadMany_UnicodeId_ReturnsItem` | NEW | IDs with emoji/unicode characters |

### 4.11 System Properties

| # | Test | Status | Notes |
|---|---|---|---|
| T30 | `ReadMany_ResultsInclude_ETag` | NEW | Deserialize to JObject, check `_etag` property present |
| T31 | `ReadMany_ResultsInclude_Timestamp` | NEW | Check `_ts` present in results |
| T32 | `ReadManyStream_DocumentsIncludeSystemProperties` | NEW | Check `_etag`, `_ts`, `_rid` in stream JSON |

### 4.12 Concurrency

| # | Test | Status | Notes |
|---|---|---|---|
| T33 | `ReadMany_ConcurrentCalls_AllSucceed` | NEW | Fire 10 ReadMany calls in parallel, all should return correct results. ConcurrentDictionary should handle this, but worth verifying. |

---

## 5. Potentially Difficult / Divergent Behavior Tests

These tests may reveal behavior that differs from real Cosmos DB and might be hard to replicate perfectly. If so, they should be **skipped with a clear reason** and a **sister test showing the divergent behavior**.

### T24: Hierarchical Partition Key with ReadMany
- **Risk**: `PartitionKeyToString` may not correctly handle composite partition keys from `PartitionKeyBuilder` in the ReadMany path
- **Approach**: Write the test first. If it fails and the fix is complex (e.g., requires deep changes to key resolution), skip it with reason: `"ReadMany with hierarchical partition keys requires PartitionKeyBuilder serialization in PartitionKeyToString which is not yet wired for composite keys in the ReadMany path"` and write sister test showing what currently happens.

### T30/T31/T32: System Properties in ReadMany Results
- **Risk**: The emulator stores JSON with system properties injected during writes, but deserialization to `TestDocument` won't include `_etag`/`_ts`. Stream variant may or may not include them depending on how `_items` stores the JSON.
- **Approach**: First check what `_items` actually stores (does it include `_etag`, `_ts`?). If system properties aren't in stored JSON, these tests need implementation changes. If too complex for the ReadMany scope, skip with reason `"System properties (_etag, _ts) are maintained in separate dictionaries and not embedded in stored JSON returned by ReadMany"` and write sister test documenting what IS returned.

---

## 6. Implementation Changes Required

### IMPL-1: Null argument guard (BUG-1)
**File**: `src/CosmosDB.InMemoryEmulator/InMemoryContainer.cs`  
**Both methods** (ReadManyItemsAsync ~line 980, ReadManyItemsStreamAsync ~line 1001):
```csharp
ArgumentNullException.ThrowIfNull(items);
```

### IMPL-2: Add _count to stream envelope (BUG-2)
**File**: `src/CosmosDB.InMemoryEmulator/InMemoryContainer.cs`  
**ReadManyItemsStreamAsync** (~line 1011):
```csharp
// Before:
var envelope = new JObject { ["Documents"] = results };
// After:
var envelope = new JObject { ["Documents"] = results, ["_count"] = results.Count };
```

### IMPL-3: Any implementation needed to support hierarchical PK in ReadMany (if T24 fails)
- Investigate and decide at test time

---

## 7. TDD Execution Order

Red-Green-Refactor workflow. Write test → see it fail → implement fix → see it pass.

### Phase 1: Bug Fixes (test first)
1. [ ] Write T18 (tightened ArgumentNullException) → RED
2. [ ] Write T19 (stream null check) → RED
3. [ ] Fix IMPL-1 (null guard) → GREEN
4. [ ] Write T13 (stream _count field) → RED
5. [ ] Fix IMPL-2 (add _count) → GREEN

### Phase 2: Consolidate Existing Tests
6. [ ] Create new unified `ReadManyTests` class with all existing tests reorganized
7. [ ] Remove old Gap/EdgeCase classes
8. [ ] Run all tests → GREEN (no behavior change, just reorganization)

### Phase 3: New Coverage — Happy Path & Missing Items
9. [ ] Write T02 (single item) → should be GREEN immediately (no impl needed)
10. [ ] Write T09 (duplicate stream) → should be GREEN immediately

### Phase 4: New Coverage — Response Metadata
11. [ ] Write T16 (RequestCharge positive) → should be GREEN (already returns 1.0)
12. [ ] Write T17 (ActivityId present) → check if GREEN or needs fix

### Phase 5: New Coverage — After Mutations
13. [ ] Write T21 (after update) → should be GREEN (reads from _items which is latest)
14. [ ] Write T22 (after delete) → should be GREEN
15. [ ] Write T23 (after replace) → should be GREEN

### Phase 6: New Coverage — Partition Key Edge Cases
16. [ ] Write T24 (hierarchical PK) → may be RED — evaluate complexity, skip if needed
17. [ ] Write T25 (PartitionKey.None) → should be GREEN
18. [ ] Write T26 (empty string PK) → should be GREEN

### Phase 7: New Coverage — ID Edge Cases
19. [ ] Write T27 (case sensitive IDs) → should be GREEN (dictionary key is case-sensitive)
20. [ ] Write T28 (special chars in ID) → should be GREEN
21. [ ] Write T29 (unicode ID) → should be GREEN

### Phase 8: New Coverage — System Properties
22. [ ] Write T30 (ETag in results) → evaluate, may need skip + sister
23. [ ] Write T31 (timestamp in results) → evaluate, may need skip + sister
24. [ ] Write T32 (stream system props) → evaluate, may need skip + sister

### Phase 9: New Coverage — Concurrency
25. [ ] Write T33 (concurrent ReadMany) → should be GREEN (ConcurrentDictionary)

### Phase 10: Final Validation
26. [ ] Run full test suite → all GREEN
27. [ ] Review skipped tests have clear reasons & sister tests

---

## 8. Post-Implementation Steps

### 8.1 Documentation Updates

| File | Action |
|---|---|
| `Known-Limitations.md` (wiki) | Add any new limitations discovered (e.g., ReadManyRequestOptions ignored, system properties if skipped) |
| `Feature-Comparison-With-Alternatives.md` (wiki) | Update ReadMany row if any nuances found |
| `Features.md` (wiki) | Update ReadMany section with any new details (e.g., `_count` in stream response) |
| `README.md` (repo root) | No changes expected unless major feature added |

### 8.2 Version Bump & Release

1. Bump version: `2.0.4` → `2.0.5` in `src/CosmosDB.InMemoryEmulator/CosmosDB.InMemoryEmulator.csproj`
2. `git add -A`
3. `git commit -m "v2.0.5: ReadMany improvements — null guards, stream _count, expanded test coverage"`
4. `git tag v2.0.5`
5. `git push && git push --tags`

### 8.3 Wiki Updates Push

1. `cd c:\git\CosmosDB.InMemoryEmulator.wiki`
2. `git add -A`
3. `git commit -m "v2.0.5: Update ReadMany documentation — stream _count, known limitations"`
4. `git push`

---

## 9. Summary Statistics

| Category | Count |
|---|---|
| Existing tests to keep/improve | 12 |
| Duplicate tests to remove | 6 |
| New tests to write | 17 |
| Bugs to fix | 3 |
| Implementation changes | 2–3 |
| **Total tests in final file** | **~29** |
| Potentially skipped (with sister tests) | 0–4 |

---

## 10. Progress Tracker

- [ ] Phase 1: Bug fix tests (T13, T18, T19) + implementation (IMPL-1, IMPL-2)
- [ ] Phase 2: Test consolidation
- [ ] Phase 3: Happy path new tests (T02, T09)
- [ ] Phase 4: Response metadata tests (T16, T17)
- [ ] Phase 5: After mutations tests (T21, T22, T23)
- [ ] Phase 6: Partition key edge cases (T24, T25, T26)
- [ ] Phase 7: ID edge cases (T27, T28, T29)
- [ ] Phase 8: System properties tests (T30, T31, T32)
- [ ] Phase 9: Concurrency test (T33)
- [ ] Phase 10: Final validation & all green
- [ ] Documentation updates (wiki + README)
- [ ] Version bump, tag, push
