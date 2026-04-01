# WebApplicationFactoryIntegrationTests — Deep Dive Plan

**Date:** 2026-04-01
**Current version:** 2.0.4 → **Target version:** 2.0.5
**File:** `tests/CosmosDB.InMemoryEmulator.Tests/WebApplicationFactoryIntegrationTests.cs`
**Approach:** TDD — Red-Green-Refactor. Difficult behaviours → Skip with reason + sister divergent-behaviour test.

---

## 1. Current State Analysis

### Existing Tests (11 tests)

| # | Test Name | What it covers |
|---|-----------|---------------|
| 1 | `CreateAndReadItem` | POST/GET round-trip via HTTP |
| 2 | `ReadNotFound` | 404 on missing item |
| 3 | `ListItems` | POST 3 items → GET /items → count 3 |
| 4 | `LinqQueryWorks` | POST 2 items → GET /items → count 2 (no LINQ filtering) |
| 5 | `ClientIsAccessible` | Resolve CosmosClient from DI, verify container name |
| 6 | `IsolatedPerFactory` | Two TestAppHost instances → data not shared |
| 7 | `CalledFromConfigureTestServices_FullRoundTrip` | Full CRUD cycle (Create → Read → List) via HTTP |
| 8 | `ConstructorResolvedContainer_Pattern3` | client.GetContainer() in repo constructor |
| 9 | `ZeroConfig_JustWorks` | UseInMemoryCosmosDB() with zero explicit config |
| 10 | `ZeroConfig_AutoDetect_ContainerNameMatchesProduction` | Auto-detect preserves production container name |
| 11 | `UpsertAndQuery` | Upsert via direct container + list via HTTP |

### Helper Infrastructure

- `CosmosTestItem` — test record with Id, PartitionKey, Name
- `TestRepository` — DI-resolved Container (Pattern 1)
- `ClientResolvedRepository` — DI-resolved CosmosClient, calls `client.GetContainer()` (Pattern 3)
- `TestAppHost` — IAsyncDisposable, builds IHost with TestServer, configurable base/test services and endpoints

---

## 2. Identified Gaps

### 2.1 Missing CRUD Operations via HTTP

| Gap | Priority | Notes |
|-----|----------|-------|
| **G01** Delete item via HTTP endpoint | High | No DELETE endpoint exists in TestAppHost; no test for delete round-trip |
| **G02** Replace/Update item via HTTP endpoint | High | No PUT endpoint; replace is a core Cosmos operation |
| **G03** Upsert via HTTP endpoint | Medium | `UpsertAndQuery` uses direct container, not HTTP |
| **G04** Patch item via HTTP endpoint | Medium | Patch is a key SDK feature (Set, Add, Remove, Increment) |
| **G05** Conflict on duplicate create via HTTP | High | POST same ID twice — should surface 409 |

### 2.2 Missing DI Integration Patterns

| Gap | Priority | Notes |
|-----|----------|-------|
| **G06** `UseInMemoryCosmosContainers` via WAF | Medium | Only `UseInMemoryCosmosDB` is tested through WAF |
| **G07** Typed client pattern (`UseInMemoryCosmosDB<TClient>`) via WAF | Medium | Not tested in integration context |
| **G08** Multiple containers in same WAF app | High | Not tested — common real-world pattern (e.g. orders + products) |
| **G09** OnClientCreated callback surfaced in WAF | Low | Tested in unit tests but not integration |
| **G10** OnHandlerCreated + fault injection via WAF | Medium | Not tested — common pattern for testing retries |
| **G11** HttpMessageHandlerWrapper via WAF | Medium | Tested in unit tests but not via full HTTP pipeline |
| **G12** Custom database name in non-Pattern3 tests | Low | Only tested via Pattern3 (DatabaseName = "TestDb") |
| **G13** Scoped container lifetime via WAF | Medium | All tests use Singleton; Scoped is common in real apps |

### 2.3 Missing Query & Data Access Patterns

| Gap | Priority | Notes |
|-----|----------|-------|
| **G14** SQL query via HTTP endpoint | Medium | No endpoint exercises GetItemQueryIterator |
| **G15** Filtered LINQ query with WHERE clause | Medium | `LinqQueryWorks` has no predicate — just counts all |
| **G16** Pagination through HTTP | Low | No continuation token / paging tested |
| **G17** Cross-partition query via WAF | Low | Not tested; all items share partition key pattern |
| **G18** Empty container listing returns empty array | Low | All list tests pre-seed data |

### 2.4 Missing Error & Edge Cases

| Gap | Priority | Notes |
|-----|----------|-------|
| **G19** Concurrent requests from same HttpClient | Medium | Not tested; real apps have parallel calls |
| **G20** Dispose TestAppHost then attempt use | Low | IAsyncDisposable correctness after dispose |
| **G21** Query with partition key targeting specific partition | Medium | GET /items/{id} assumes partitionKey == id; no test for different PK |
| **G22** Item with special characters in ID | Medium | URL encoding edge case through HTTP |
| **G23** Large payload through HTTP pipeline | Low | Document size limits through WAF |
| **G24** ETag / optimistic concurrency through WAF | Medium | No If-Match / If-None-Match testing via HTTP |

### 2.5 Potential Bugs

| Bug | Severity | Description |
|-----|----------|-------------|
| **B01** `LinqQueryWorks` is functionally identical to `ListItems` | Low | It doesn't test any LINQ-specific behavior (no Where, Select, OrderBy). Rename or enhance. |
| **B02** `TestRepository.GetByIdAsync` assumes partitionKey == id in HTTP endpoint | Low | The GET /items/{id} endpoint hardcodes `await repo.GetByIdAsync(id, id)` — items where PK ≠ ID will never be found via HTTP. This is a design constraint, not a bug, but it's undocumented. |
| **B03** `TestRepository.GetAllAsync` uses `.ToFeedIteratorOverridable()` but UseInMemoryCosmosDB shouldn't need it | Medium | The FakeCosmosHandler supports native `.ToFeedIterator()`. Using `.ToFeedIteratorOverridable()` works but is misleading — it implies the override is necessary when it isn't for this DI pattern. |
| **B04** `ClientResolvedRepository` incomplete — no `GetAllAsync` or `UpsertAsync` | Low | Compared to `TestRepository`, this repo is minimal. May need expansion for comprehensive Pattern3 tests. |

---

## 3. Test Plan

### Phase A: Fix Existing Issues (B01, B03)

#### A1. Rename and enhance `LinqQueryWorks`
- **Action:** Rename to `LinqQueryWithFilter_ReturnsMatchingItems`
- **Red:** Add WHERE filter: `repo.GetFilteredAsync("Alice")` → expect 1 item
- **Green:** Add `GetFilteredAsync` method to `TestRepository`, add `/items/search?name=X` endpoint
- **Verify:** Two items seeded, only one returned by filtered query

#### A2. Add `ToFeedIterator` sister test
- **Action:** Add new test `LinqWithNativeToFeedIterator_WorksWithUseInMemoryCosmosDB` demonstrating that `.ToFeedIterator()` (not `.ToFeedIteratorOverridable()`) works fine with UseInMemoryCosmosDB
- **This documents the important difference between the two DI patterns**

### Phase B: Missing CRUD Operations via HTTP

#### B1. `DeleteItem_ViaHttp_RemovesItem` (G01)
- **Red:** Add DELETE /items/{id} endpoint to TestAppHost defaults; test POST → DELETE → GET returns 404
- **Green:** Add `DeleteAsync` to `TestRepository`, wire endpoint
- **Refactor:** Ensure consistent error handling

#### B2. `DeleteItem_ViaHttp_NotFound_Returns404` (G01)
- **Red:** DELETE a non-existent item
- **Green:** Endpoint returns 404 on CosmosException NotFound

#### B3. `ReplaceItem_ViaHttp_UpdatesItem` (G02)
- **Red:** Add PUT /items/{id} endpoint; POST then PUT with new name → GET returns updated
- **Green:** Add `ReplaceAsync` to `TestRepository`, wire endpoint

#### B4. `UpsertItem_ViaHttp_CreatesOrUpdates` (G03)
- **Red:** Add PUT /items (upsert) endpoint; upsert new → 200, upsert existing → 200 with updated data
- **Green:** Wire `UpsertAsync` (already on TestRepository)

#### B5. `PatchItem_ViaHttp_PartialUpdate` (G04)
- **Red:** Add PATCH /items/{id} endpoint; Create then Patch name → verify only name changed
- **Green:** Add `PatchNameAsync` to `TestRepository`, wire endpoint

#### B6. `CreateDuplicateItem_ViaHttp_Returns409` (G05)
- **Red:** POST same item twice → second POST returns 409 Conflict
- **Green:** Endpoint catches CosmosException with StatusCode.Conflict and returns 409

### Phase C: Missing DI Integration Patterns

#### C1. `UseInMemoryCosmosContainers_WorksThroughWaf` (G06)
- **Red:** Create TestAppHost using `UseInMemoryCosmosContainers` instead of `UseInMemoryCosmosDB`
- **Green:** POST/GET round-trip works via HTTP
- **Note:** This requires `.ToFeedIteratorOverridable()` in the repo — demonstrate the constraint

#### C2. `TypedClient_WorksThroughWaf` (G07)
- **Red:** Register a test-only `InMemoryCosmosClient` subclass via `UseInMemoryCosmosDB<TestCosmosClient>()`
- **Green:** Repo resolves typed client, CRUD works through HTTP
- **Note:** May need a `TestCosmosClient : InMemoryCosmosClient` class

#### C3. `MultipleContainers_SameApp_IsolatedData` (G08)
- **Red:** Register two containers ("orders" + "products") via AddContainer; write to one, verify other is empty
- **Green:** Two endpoints `/orders` and `/products`, each backed by a different container
- **Note:** Requires adding a mechanism to resolve named containers (or multiple repos)

#### C4. `OnHandlerCreated_FaultInjection_ViaWaf` (G10)
- **Red:** Use OnHandlerCreated to capture FakeCosmosHandler, set FaultInjector to return 503 on reads
- **Green:** GET /items/{id} returns 500 (Internal Server Error) because the repository throws
- **Note:** Testing that WAF surfaces fault injection correctly

#### C5. `HttpMessageHandlerWrapper_RequestCounting_ViaWaf` (G11)
- **Red:** Wire a CountingHandler via WithHttpMessageHandlerWrapper; do CRUD; verify count > 0
- **Green:** CountingHandler incremented on every SDK HTTP call within the app

#### C6. `ScopedContainerLifetime_ViaWaf` (G13)
- **Red:** Register Container as Scoped in base services; UseInMemoryCosmosDB should preserve lifetime
- **Green:** Two requests resolve different Container instances (scoped isolation)
- **LIKELY SKIP — FakeCosmosHandler creates a single Container instance; scoped resolution returns the same Container regardless. Add sister test documenting this: `ScopedLifetime_ActualBehavior_ReturnsSameContainerInstance`**

### Phase D: Missing Query & Data Patterns

#### D1. `SqlQuery_ViaHttpEndpoint_ReturnsFilteredResults` (G14)
- **Red:** Add `/items/query?sql=...` endpoint that uses `GetItemQueryIterator<T>`; verify filtering
- **Green:** Wire endpoint, execute parameterized SQL

#### D2. `FilteredLinqQuery_ViaHttpEndpoint` (G15)
- **Red:** Add `/items/search?name=X` endpoint with LINQ Where clause
- **Green:** Two items seeded, filter returns only matching ones
- **Note:** This overlaps with A1 — may merge into same test

#### D3. `EmptyContainer_ListReturnsEmptyArray` (G18)
- **Red:** GET /items on empty container → returns empty []
- **Green:** Should already work; this is a regression guard

### Phase E: Error & Edge Cases

#### E1. `ConcurrentRequests_AllSucceed` (G19)
- **Red:** POST 10 items in parallel via Task.WhenAll; GET /items → count 10
- **Green:** Thread safety of FakeCosmosHandler under concurrent load

#### E2. `ItemWithSpecialCharactersInId_RoundTrips` (G22)
- **Red:** Create item with ID = "item/with+special chars&more" → GET round-trips correctly
- **Green:** URL encoding handled properly by TestServer + FakeCosmosHandler

#### E3. `ETagConcurrency_ViaDirectContainer_InWaf` (G24)
- **Red:** Create item, read with ETag, upsert with stale ETag → expect PreconditionFailed
- **Green:** Uses direct Container from DI (not HTTP, since HTTP endpoints don't wire headers)
- **Note:** HTTP-level ETag testing would require custom endpoint; test via direct container access in WAF context

#### E4. `ItemWithDifferentIdAndPartitionKey_ViaHttp` (G21)
- **Red:** Create item with ID="abc" PK="xyz"; GET /items/abc returns 404 (because endpoint hardcodes PK=ID)
- **Green:** This is expected behavior — document that the GET endpoint assumes PK == ID
- **This is a documentation test — it demonstrates the TestAppHost's design constraint**

### Phase F: Difficult Behaviours (Skip + Sister Tests)

#### F1. `ScopedLifetime_PreservesPerRequestIsolation` (G13)
- **SKIP** — Reason: "UseInMemoryCosmosDB creates a single FakeCosmosHandler and CosmosClient at registration time. The Container resolved from client.GetContainer() returns the same in-memory backing store regardless of scope. Per-request data isolation would require a scoped FakeCosmosHandler factory, which is not supported. See sister test: ScopedLifetime_ActualBehavior_ReturnsSameDataAcrossScopes."
- **Sister test:** `ScopedLifetime_ActualBehavior_ReturnsSameDataAcrossScopes`
  - // DIVERGENT BEHAVIOR: Even when the base service registers Container as Scoped,
  - // UseInMemoryCosmosDB replaces it with a Singleton CosmosClient. The Container
  - // resolved from client.GetContainer() shares the same in-memory store. Data written
  - // in one scope is visible in all other scopes. This matches real CosmosClient behavior
  - // (the SDK client is typically a singleton), but differs from what "Scoped" lifetime
  - // might imply for DI-resolved Container instances.

#### F2. `Pagination_ContinuationToken_ViaHttp` (G16)
- **SKIP** — Reason: "HTTP pagination requires exposing continuation tokens through the HTTP API layer. The FakeCosmosHandler supports pagination internally (query results are paged), but surfacing this through a minimal API endpoint would require custom response headers and request parameters that are beyond the scope of the integration pattern being tested. The underlying pagination is tested in FakeCosmosHandlerTests."
- **Sister test:** `Pagination_WorksViaDirectContainerAccess_InWafContext`
  - // DIVERGENT BEHAVIOR: Pagination via FeedIterator works correctly at the Container
  - // level (tested extensively in other test files). However, when accessed through
  - // HTTP endpoints in a WAF context, pagination requires the API layer to thread
  - // continuation tokens through request/response headers — this is an application
  - // concern, not an emulator concern. This test demonstrates pagination works fine
  - // at the Container layer even when running inside a WAF-hosted app.

---

## 4. TestAppHost Enhancements Required

To support the new tests, `TestAppHost`'s default endpoints and `TestRepository` need expansion:

### TestRepository additions:
- `DeleteAsync(string id, string partitionKey)` — for G01
- `ReplaceAsync(CosmosTestItem item)` — for G02
- `PatchNameAsync(string id, string partitionKey, string newName)` — for G04
- `GetFilteredByNameAsync(string name)` — for G14/G15
- `QuerySqlAsync(string sql)` — for G14

### Default endpoint additions:
- `DELETE /items/{id}` → calls `repo.DeleteAsync(id, id)`
- `PUT /items/{id}` → calls `repo.ReplaceAsync(item)`
- `PUT /items` (upsert) → calls `repo.UpsertAsync(item)`
- `PATCH /items/{id}` → calls `repo.PatchNameAsync(id, id, newName)`
- `GET /items/search?name=X` → calls `repo.GetFilteredByNameAsync(name)`
- `POST /items/query` → calls `repo.QuerySqlAsync(sql)`

### ClientResolvedRepository additions:
No changes needed — existing two methods sufficient for Pattern3 testing.

### New helper class:
- `TestCosmosClient : InMemoryCosmosClient` — test-only typed client for C2

---

## 5. Implementation Order (TDD sequence)

```
1.  A1 — Fix LinqQueryWorks → LinqQueryWithFilter
2.  A2 — Add ToFeedIterator sister test
3.  B6 — CreateDuplicate → 409
4.  B1 — Delete via HTTP
5.  B2 — Delete NotFound via HTTP
6.  B3 — Replace via HTTP
7.  B4 — Upsert via HTTP
8.  B5 — Patch via HTTP
9.  D3 — Empty container list
10. E1 — Concurrent requests
11. E2 — Special characters in ID
12. C1 — UseInMemoryCosmosContainers via WAF
13. C2 — Typed client via WAF
14. C3 — Multiple containers same app
15. C4 — Fault injection via WAF
16. C5 — HttpMessageHandlerWrapper via WAF
17. D1 — SQL query via HTTP
18. D2 — Filtered LINQ via HTTP (merge with A1 if overlapping)
19. E3 — ETag concurrency via WAF
20. E4 — Different ID/PK via HTTP (documentation test)
21. F1 — Scoped lifetime (SKIP + sister)
22. F2 — Pagination (SKIP + sister)
23. C6 — Scoped container lifetime (likely merged into F1)
```

---

## 6. Documentation Updates

### 6.1 Wiki — Known-Limitations.md
- No new limitations expected (all behaviors are consistent with existing documented limitations)
- If F1 reveals anything unexpected about scoped Container resolution, document it

### 6.2 Wiki — Features.md
- Add mention of WebApplicationFactory integration testing support in the DI section
- Add note that all three DI patterns (UseInMemoryCosmosDB, UseInMemoryCosmosContainers, UseInMemoryCosmosDB<T>) are tested through WAF integration

### 6.3 Wiki — Feature-Comparison-With-Alternatives.md
- Update integration testing row if it exists; verify WAF support is called out
- No substantive changes expected unless a new feature is implemented

### 6.4 README.md
- No changes expected unless new DI patterns are added

### 6.5 Wiki — Other pages
- Check if any testing guide references need updating with new patterns (e.g. fault injection through WAF)

---

## 7. Release Steps

```powershell
# After all tests pass:
# 1. Bump version in CosmosDB.InMemoryEmulator.csproj: 2.0.4 → 2.0.5
# 2. Update wiki docs (Known-Limitations, Features, Comparison)
# 3. Update README if needed
# 4. Commit, tag, push:
git add -A
git commit -m "v2.0.5: Comprehensive WAF integration test coverage - CRUD endpoints, DI patterns, edge cases, fault injection"
git tag v2.0.5
git push
git push --tags
# 5. Push wiki updates:
Push-Location "c:\git\CosmosDB.InMemoryEmulator.wiki"
git add -A
git commit -m "v2.0.5: Update Features for WAF integration test coverage"
git push
Pop-Location
```

---

## 8. Test Count Summary

| Category | New Tests | Skipped (with sister) |
|----------|-----------|----------------------|
| Phase A: Fix existing | 2 | 0 |
| Phase B: CRUD via HTTP | 6 | 0 |
| Phase C: DI patterns | 5 | 1 (F1) |
| Phase D: Query patterns | 3 | 0 |
| Phase E: Edge cases | 4 | 0 |
| Phase F: Difficult behaviours | 0 | 2 (skipped + 2 sisters) |
| **Total** | **20 new** | **2 skipped + 2 sister tests** |

Final expected test count: 11 existing + 20 new + 2 skipped + 2 sisters = **35 tests**

---

## 9. Progress Tracking

| Step | Test | Status |
|------|------|--------|
| A1 | LinqQueryWithFilter_ReturnsMatchingItems | ⬜ Not started |
| A2 | LinqWithNativeToFeedIterator_WorksWithUseInMemoryCosmosDB | ⬜ Not started |
| B1 | DeleteItem_ViaHttp_RemovesItem | ⬜ Not started |
| B2 | DeleteItem_ViaHttp_NotFound_Returns404 | ⬜ Not started |
| B3 | ReplaceItem_ViaHttp_UpdatesItem | ⬜ Not started |
| B4 | UpsertItem_ViaHttp_CreatesOrUpdates | ⬜ Not started |
| B5 | PatchItem_ViaHttp_PartialUpdate | ⬜ Not started |
| B6 | CreateDuplicateItem_ViaHttp_Returns409 | ⬜ Not started |
| C1 | UseInMemoryCosmosContainers_WorksThroughWaf | ⬜ Not started |
| C2 | TypedClient_WorksThroughWaf | ⬜ Not started |
| C3 | MultipleContainers_SameApp_IsolatedData | ⬜ Not started |
| C4 | OnHandlerCreated_FaultInjection_ViaWaf | ⬜ Not started |
| C5 | HttpMessageHandlerWrapper_RequestCounting_ViaWaf | ⬜ Not started |
| C6/F1 | ScopedLifetime (SKIP + sister) | ⬜ Not started |
| D1 | SqlQuery_ViaHttpEndpoint_ReturnsFilteredResults | ⬜ Not started |
| D2 | FilteredLinqQuery_ViaHttpEndpoint | ⬜ Not started |
| D3 | EmptyContainer_ListReturnsEmptyArray | ⬜ Not started |
| E1 | ConcurrentRequests_AllSucceed | ⬜ Not started |
| E2 | ItemWithSpecialCharactersInId_RoundTrips | ⬜ Not started |
| E3 | ETagConcurrency_ViaDirectContainer_InWaf | ⬜ Not started |
| E4 | ItemWithDifferentIdAndPartitionKey_ViaHttp | ⬜ Not started |
| F2 | Pagination (SKIP + sister) | ⬜ Not started |
