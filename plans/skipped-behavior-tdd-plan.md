# SkippedBehaviorTests.cs — Deep Dive TDD Plan

**Version**: 2.0.4 → 2.0.5  
**Date**: 2026-04-01  
**Target file**: `tests/CosmosDB.InMemoryEmulator.Tests/SkippedBehaviorTests.cs`

---

## Executive Summary

SkippedBehaviorTests.cs contains 17 tests across 16 domains — **all currently passing** despite the file name suggesting otherwise. Deep analysis reveals:

- **3 bugs**: Tests that pass but don't actually verify the intended behaviour
- **~25 missing edge-case tests** within the domains already touched
- **Significant overlap** with dedicated test files (ResponseMetadataTests, DocumentSizeLimitTests, TtlTests, StoredProcedureTests, TriggerTests, TransactionalBatchTests, PartitionKeyTests, QueryTests)
- **Several gap areas** not covered anywhere in the test suite

### Approach

1. **Fix 3 bugs** — tests that pass vacuously or test wrong things
2. **Add missing edge-case tests** — grouped by domain, TDD red-green-refactor
3. **Mark too-hard tests as skipped** with detailed skip reasons + sister divergent-behavior tests
4. **Update documentation** — wiki Known-Limitations, Features, Comparison, README
5. **Version bump** — 2.0.4 → 2.0.5, tag, push

---

## Phase 1: Bug Fixes (3 tests)

### BUG-1: `StoredProcedure_ShouldExecuteServerSideLogic`

**Problem**: The test calls `ExecuteStoredProcedureAsync("sprocId", ...)` without registering a handler via `_container.RegisterStoredProcedure()`. The emulator returns `HttpStatusCode.OK` with a default/null result for unregistered procs, so the test passes without validating any server-side logic.

**Fix**:
1. Write failing test first: register a handler that returns `"hello"`, execute it, assert response contains `"hello"`
2. The implementation already supports this (see StoredProcedureTests.cs for the pattern)
3. Fix the test to call `RegisterStoredProcedure` before execution

**Test (updated)**:
```csharp
[Fact]
public async Task StoredProcedure_ShouldExecuteServerSideLogic()
{
    _container.RegisterStoredProcedure("sprocId", (pk, args) => "executed");
    var response = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
        "sprocId", new PartitionKey("pk1"), Array.Empty<dynamic>());
    response.StatusCode.Should().Be(HttpStatusCode.OK);
    response.Resource.Should().Be("executed");
}
```

---

### BUG-2: `PreTrigger_ShouldFireOnCreate`

**Problem**: The test creates a trigger definition via `CreateTriggerAsync` but never creates an item with `ItemRequestOptions.PreTriggers`. It only asserts the trigger *definition* was stored (HTTP 201), not that the trigger *fires*. The JS body `"function() { /* validation logic */ }"` is also never executed.

**Fix**:
1. Write failing test: register a C# pre-trigger that adds a `"triggered": true` field, create an item with PreTriggers option, read item back, assert `"triggered"` field exists
2. Implementation supports this (see TriggerTests.cs `PreTrigger_ModifiesDocument_OnCreate`)

**Test (updated)**:
```csharp
[Fact]
public async Task PreTrigger_ShouldFireOnCreate()
{
    _container.RegisterTrigger("validateInsert", TriggerType.Pre, TriggerOperation.Create,
        doc => { doc["triggered"] = true; return doc; });

    var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" };
    var options = new ItemRequestOptions { PreTriggers = new List<string> { "validateInsert" } };
    await _container.CreateItemAsync(doc, new PartitionKey("pk1"), options);

    var result = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
    result.Resource["triggered"]!.Value<bool>().Should().BeTrue();
}
```

---

### BUG-3: `UserDefinedFunction_ShouldBeCallableInQuery`

**Problem**: `CreateUserDefinedFunctionAsync` stores the UDF metadata but registers a stub handler that returns `null`. The test asserts `response.Count.Should().Be(1)` which passes because one row is returned, but the `taxAmount` field would be `null`, not `20.0`. The test doesn't verify the UDF result value.

**Fix**:
1. Write failing test: register a real UDF handler, query using it, assert correct calculation
2. Use `_container.RegisterUdf("tax", args => (double)args[0] * 0.2)` before creating the UDF definition

**Test (updated)**:
```csharp
[Fact]
public async Task UserDefinedFunction_ShouldBeCallableInQuery()
{
    _container.RegisterUdf("tax", args => (double)(long)args[0] * 0.2);

    await _container.CreateItemAsync(
        new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 100 },
        new PartitionKey("pk1"));

    var query = new QueryDefinition("SELECT udf.tax(c.value) AS taxAmount FROM c");
    var iterator = _container.GetItemQueryIterator<JObject>(query);
    var response = await iterator.ReadNextAsync();
    response.Should().ContainSingle();
    response.First()["taxAmount"]!.Value<double>().Should().Be(20.0);
}
```

---

## Phase 2: Missing Edge-Case Tests (by Domain)

### 2.1 — Request Charge Edge Cases

**Existing coverage**: ResponseMetadataTests (2 tests) + SkippedBehaviorTests (1 test)  
**Gap**: Only tested on Create. Not tested across all operation types.

| # | Test Name | Description | Expected | Difficulty |
|---|-----------|-------------|----------|------------|
| RC-1 | `RequestCharge_ShouldBe1_OnRead` | Read returns synthetic 1.0 RU | 1.0 | Easy |
| RC-2 | `RequestCharge_ShouldBe1_OnReplace` | Replace returns 1.0 RU | 1.0 | Easy |
| RC-3 | `RequestCharge_ShouldBe1_OnDelete` | Delete returns 1.0 RU | 1.0 | Easy |
| RC-4 | `RequestCharge_ShouldBe1_OnPatch` | Patch returns 1.0 RU | 1.0 | Easy |
| RC-5 | `RequestCharge_ShouldBe1_OnQuery` | Query FeedResponse returns 1.0 RU | 1.0 | Easy |

**Implementation**: No code changes needed — just verify existing behavior.

---

### 2.2 — Continuation Token Edge Cases

**Existing coverage**: QueryTests (3 tests) + SkippedBehaviorTests (1 test)  
**Gap**: No test for ORDER BY + pagination, empty result pagination, or full round-trip collection.

| # | Test Name | Description | Expected | Difficulty |
|---|-----------|-------------|----------|------------|
| CT-1 | `ContinuationToken_WithOrderBy_ShouldPreserveOrder` | Paginate through ORDER BY results | Items in correct order across pages | Easy |
| CT-2 | `ContinuationToken_EmptyResults_ShouldReturnNull` | Query matching nothing returns null token | Null continuation | Easy |
| CT-3 | `ContinuationToken_FullIteration_CollectsAllItems` | Iterate all pages, verify all items collected | All items present | Easy |
| CT-4 | `ContinuationToken_LastPage_ShouldReturnNull` | After last page, continuation is null | Null | Easy |

**Implementation**: No code changes needed.

---

### 2.3 — TTL Edge Cases

**Existing coverage**: TtlTests (8 tests)  
**Gap**: TTL=-1 (item-level override to disable), interaction with change feed

| # | Test Name | Description | Expected | Difficulty |
|---|-----------|-------------|----------|------------|
| TTL-1 | `ItemTtl_MinusOne_OverridesContainerDefault_NoExpiration` | Per-item `_ttl: -1` disables expiration | Item still readable | Medium |
| TTL-2 | `Ttl_ExpiredItems_NotReturnedByChangeFeed` | Expired items excluded from change feed | Empty/excluded | Medium |
| TTL-3 | `Ttl_ZeroContainerDefault_NoExpiration` | Container TTL=0 means off | Items don't expire | Easy |

**Implementation**: TTL-1 may need implementation fix if `_ttl: -1` is not handled. The current `IsExpired()` checks `if (DefaultTimeToLive is null or <= 0) return false;` but per-item `_ttl` uses `elapsed >= perItemTtl` which would be `elapsed >= -1` → always true. **This is a bug — _ttl: -1 should disable expiration.**

**Action for TTL-1**: TDD — write failing test, then fix `IsExpired()` to check `if (perItemTtl == -1) return false;`

---

### 2.4 — LIKE Operator Edge Cases

**Existing coverage**: QueryTests (1 LIKE % test) + SkippedBehaviorTests (1 LIKE _ test)  
**Gap**: NOT LIKE, ESCAPE clause, combined patterns, null handling, special regex chars, case sensitivity

| # | Test Name | Description | Expected | Difficulty |
|---|-----------|-------------|----------|------------|
| LK-1 | `NotLike_ShouldExcludeMatches` | `WHERE c.name NOT LIKE 'A%'` | Items not starting with A | Easy |
| LK-2 | `Like_CombinedWildcards_PercentAndUnderscore` | `LIKE 'a_%'` matches "ab", "abc", not "a" | Correct filtering | Easy |
| LK-3 | `Like_EscapeClause_ShouldTreatWildcardAsLiteral` | `LIKE 'a\%b' ESCAPE '\'` matches "a%b" only | Exact match | Easy |
| LK-4 | `Like_NullValue_ShouldReturnFalse` | `WHERE null LIKE 'a%'` | No results | Easy |
| LK-5 | `Like_SpecialRegexChars_ShouldBeTreatedAsLiterals` | `LIKE 'a.b'` matches only "a.b", not "axb" | Literal dot match | Easy |
| LK-6 | `Like_EmptyPattern_ShouldMatchEmptyString` | `LIKE ''` matches only empty string | Single match | Easy |
| LK-7 | `Like_CaseSensitive_ShouldNotMatchDifferentCase` | `LIKE 'Alice'` doesn't match "alice" | No match | Easy |
| LK-8 | `Like_PercentOnly_ShouldMatchAll` | `LIKE '%'` matches everything | All items | Easy |

**Implementation**: All should work with current implementation. If LK-5 fails, the `Regex.Escape` in `ConvertLikeToRegex` should already handle it. Tests only.

---

### 2.5 — Indexing Policy Edge Cases

**Existing coverage**: ContainerManagementTests (2 tests) + IndexSimulationTests (5 tests) + SkippedBehaviorTests (1 test)  
**Gap**: Composite index persistence, spatial index persistence, excluded/included paths persistence

| # | Test Name | Description | Expected | Difficulty |
|---|-----------|-------------|----------|------------|
| IX-1 | `IndexingPolicy_CompositeIndex_ShouldPersistOnReplace` | Set composite indexes, read back | Composite indexes present | Easy |
| IX-2 | `IndexingPolicy_ExcludedPaths_ShouldPersistOnReplace` | Set excluded paths, read back | Excluded paths present | Easy |
| IX-3 | `IndexingPolicy_LazyMode_ShouldBeStoredButNotEnforced` | Set lazy indexing mode | Stored but no effect | Easy |

**Implementation**: No code changes needed — just verify ContainerProperties persistence.

---

### 2.6 — Session Token Edge Cases

**Existing coverage**: ResponseMetadataTests (4 tests) + SkippedBehaviorTests (1 test)  
**Gap**: Token presence on error responses, token on stream responses

| # | Test Name | Description | Expected | Difficulty |
|---|-----------|-------------|----------|------------|
| ST-1 | `SessionToken_ShouldBePresent_OnReadResponse` | Session token on ReadItemAsync | Non-empty | Easy |
| ST-2 | `SessionToken_ShouldBePresent_OnStreamResponse` | Session token on ReadItemStreamAsync | Non-empty header | Easy |

**Implementation**: No code changes needed.

---

### 2.7 — MaxItemCount Edge Cases

**Existing coverage**: QueryTests (2 tests) + SkippedBehaviorTests (1 test)  
**Gap**: MaxItemCount=1, interaction with ORDER BY, unset MaxItemCount

| # | Test Name | Description | Expected | Difficulty |
|---|-----------|-------------|----------|------------|
| MI-1 | `MaxItemCount_One_ShouldReturnOnePerPage` | MaxItemCount=1 returns exactly 1 per page | Count=1 per page | Easy |
| MI-2 | `MaxItemCount_GreaterThanTotal_ShouldReturnAll` | MaxItemCount=100 with 3 items | All in one page | Easy |
| MI-3 | `MaxItemCount_WithOrderBy_ShouldPaginateCorrectly` | ORDER BY + MaxItemCount=2 | Pages in order | Easy |

**Implementation**: No code changes needed.

---

### 2.8 — Hierarchical Partition Key Edge Cases

**Existing coverage**: PartitionKeyTests (9 tests) + SkippedBehaviorTests (1 test)  
**Gap**: Query filtering by prefix of hierarchical key, point read with wrong sub-key

| # | Test Name | Description | Expected | Difficulty |
|---|-----------|-------------|----------|------------|
| HP-1 | `HierarchicalPK_QueryByFirstLevelOnly_ShouldFilterCorrectly` | Query filtering by first level of composite key | Subset of items | Medium |
| HP-2 | `HierarchicalPK_PointReadWithWrongSubKey_ShouldReturn404` | Read with correct id but wrong sub-key | CosmosException 404 | Easy |
| HP-3 | `HierarchicalPK_ThreeLevels_ShouldWork` | 3-level hierarchical key | CRUD works | Easy |

**Implementation**: HP-1 may need investigation — cross-partition query with partial PK filter. If this doesn't work, mark as skipped with divergent behavior sister test.

---

### 2.9 — Stream Response Header Edge Cases

**Existing coverage**: ResponseMetadataTests (2 tests) + SkippedBehaviorTests (1 test)  
**Gap**: Headers on error responses

| # | Test Name | Description | Expected | Difficulty |
|---|-----------|-------------|----------|------------|
| SH-1 | `StreamHeaders_OnNotFound_ShouldContainActivityId` | 404 response has activity-id | Non-empty | Easy |
| SH-2 | `StreamHeaders_OnConflict_ShouldContainActivityId` | 409 response has activity-id | Non-empty | Easy |

**Implementation**: May need code changes if error responses don't include activity-id headers. If they don't, mark as skipped + divergent behavior.

---

### 2.10 — Cross-Partition ORDER BY Edge Cases

**Existing coverage**: QueryTests (5+ tests) + SkippedBehaviorTests (1 test)  
**Gap**: ORDER BY with null values, ORDER BY DESC cross-partition

| # | Test Name | Description | Expected | Difficulty |
|---|-----------|-------------|----------|------------|
| CO-1 | `CrossPartitionOrderBy_DESC_ShouldSortCorrectly` | ORDER BY DESC across partitions | Correct descending order | Easy |
| CO-2 | `CrossPartitionOrderBy_WithNulls_ShouldSortNullsFirst` | Null values sort before non-null in ASC | Nulls first | Medium |

**Implementation**: CO-2 may reveal a gap in null handling. If it fails and is too hard to fix, mark as skipped.

---

### 2.11 — Conflict Resolution Edge Cases

**Existing coverage**: ContainerManagementTests (2 tests) + SkippedBehaviorTests (1 test)  
**Gap**: Custom stored procedure mode storage

| # | Test Name | Description | Expected | Difficulty |
|---|-----------|-------------|----------|------------|
| CR-1 | `ConflictResolution_CustomMode_ShouldStoreSprocLink` | Custom mode with sproc link is stored | Policy persisted | Easy |

**Implementation**: No code changes needed — just verify ContainerProperties persistence.

---

## Phase 3: Skipped Tests with Divergent Behavior Sisters

These tests document behavior that's too hard to implement correctly or is a known limitation.

### SKIP-1: `ConflictResolution_ShouldResolveConflicts_AtRuntime` (Too Hard)

**Skip reason**: "Conflict resolution policy is stored but not enforced. The in-memory emulator is single-instance and single-region, so no write conflicts can arise. Implementing conflict resolution would require simulating multi-region replication with concurrent writes, which is fundamentally incompatible with a unit-testing mock."

**Sister test**: `ConflictResolution_EmulatorBehavior_PolicyStoredButNotEnforced` (already exists in ContainerManagementTests.cs)

---

### SKIP-2: `RequestCharge_ShouldReflectActualRUConsumption` (Too Hard)

**Skip reason**: "Request charges are always synthetic (1.0 RU). Real Cosmos DB computes RU based on document size, index utilization, and query complexity. Implementing RU estimation would require replicating the Cosmos DB cost model, which is proprietary and version-specific. Use real Cosmos DB for RU consumption testing."

**Sister test**: `RequestCharge_IsAlwaysSynthetic_1RU` — verify all ops return exactly 1.0

---

### SKIP-3: `TTL_ProactiveEviction_ShouldDeleteWithoutRead` (Too Hard)

**Skip reason**: "TTL eviction is lazy — expired items are only removed when a read or query attempts to access them. Real Cosmos DB proactively evicts expired items via a background process. Implementing proactive eviction would require a background timer thread, which adds complexity and non-determinism to a testing mock. Lazy eviction is functionally equivalent for all test scenarios."

**Sister test**: `TTL_LazyEviction_OnlyRemovesOnAccess` (already exists in TtlTests.cs)

---

### SKIP-4: `ContinuationToken_ShouldBeOpaqueBase64` (Too Hard)

**Skip reason**: "Continuation tokens in the emulator are simple integer offsets (e.g. '3', '10'). Real Cosmos DB uses opaque base64-encoded JSON strings containing internal cursor state, partition information, and version metadata. Implementing opaque tokens would add complexity without functional benefit since pagination works correctly with integer offsets."

**Sister test**: `ContinuationToken_IsPlainIntegerOffset` (already exists in DivergentBehaviorTests.cs as `ContinuationTokenFormatTests`)

---

## Phase 4: Implementation Bug Fix

### BUG-4: `_ttl: -1` Does Not Disable Per-Item Expiration

**Location**: `InMemoryContainer.cs` → `IsExpired()` method (~line 2003)

**Problem**: The `IsExpired()` method checks per-item `_ttl` as `elapsed >= perItemTtl`. When `_ttl = -1`, this evaluates to `elapsed >= -1` which is always `true`, meaning the item is always "expired". In real Cosmos DB, `_ttl: -1` means "this item never expires, regardless of container default."

**Current code** (~line 2015):
```csharp
var itemTtl = jObj["_ttl"];
if (itemTtl is not null && int.TryParse(itemTtl.ToString(), out var perItemTtl))
{
    return elapsed >= perItemTtl;  // BUG: -1 → always expired
}
```

**Fix**:
```csharp
var itemTtl = jObj["_ttl"];
if (itemTtl is not null && int.TryParse(itemTtl.ToString(), out var perItemTtl))
{
    if (perItemTtl == -1) return false;  // -1 means never expire
    return elapsed >= perItemTtl;
}
```

**TDD**:
1. RED: Write `ItemTtl_MinusOne_OverridesContainerDefault_NoExpiration` — set container TTL=1, create item with `_ttl: -1`, wait 2s, read → should succeed
2. GREEN: Add the `if (perItemTtl == -1) return false;` guard
3. REFACTOR: None needed

---

## Phase 5: Documentation Updates

### 5.1 Wiki — Known-Limitations.md

**Changes**:
- Add TTL `-1` per-item override note: "Per-item `_ttl: -1` now correctly overrides the container default to prevent expiration (fixed in v2.0.5)"
- No other limitation changes (all found issues were in tests, not implementation)

### 5.2 Wiki — Features.md

**Changes**:
- Under TTL section: add mention that per-item `_ttl: -1` correctly disables expiration
- Verify stored procedures, triggers, UDF documentation is accurate

### 5.3 Wiki — Feature-Comparison-With-Alternatives.md

**Changes**:
- No changes needed (TTL was already listed as supported)

### 5.4 README.md

**Changes**:
- No changes needed (high-level feature list is accurate)

---

## Phase 6: Version Bump, Tag & Push

1. Bump `src/CosmosDB.InMemoryEmulator/CosmosDB.InMemoryEmulator.csproj` → `<Version>2.0.5</Version>`
2. `git add -A`
3. `git commit -m "v2.0.5: Fix SkippedBehaviorTests bugs, add edge-case coverage, fix _ttl:-1 expiration"`
4. `git tag v2.0.5`
5. `git push`
6. `git push --tags`
7. Wiki: `git add -A && git commit -m "v2.0.5: TTL _ttl:-1 override now supported" && git push`

---

## Test Execution Order (TDD)

### Round 1: Bug Fixes (3 tests)
- [ ] BUG-1: Fix `StoredProcedure_ShouldExecuteServerSideLogic`
- [ ] BUG-2: Fix `PreTrigger_ShouldFireOnCreate`
- [ ] BUG-3: Fix `UserDefinedFunction_ShouldBeCallableInQuery`

### Round 2: Implementation Bug Fix (1 code change)
- [ ] BUG-4: Write `ItemTtl_MinusOne_OverridesContainerDefault_NoExpiration` (RED)
- [ ] BUG-4: Fix `IsExpired()` in InMemoryContainer.cs (GREEN)
- [ ] BUG-4: Verify all TTL tests still pass (REFACTOR)

### Round 3: Request Charge Tests (5 tests)
- [ ] RC-1 through RC-5

### Round 4: Continuation Token Tests (4 tests)
- [ ] CT-1 through CT-4

### Round 5: TTL Edge Cases (2 tests — TTL-2, TTL-3)
- [ ] TTL-2, TTL-3

### Round 6: LIKE Operator Tests (8 tests)
- [ ] LK-1 through LK-8

### Round 7: Indexing Policy Tests (3 tests)
- [ ] IX-1 through IX-3

### Round 8: Session Token Tests (2 tests)
- [ ] ST-1, ST-2

### Round 9: MaxItemCount Tests (3 tests)
- [ ] MI-1 through MI-3

### Round 10: Hierarchical PK Tests (3 tests)
- [ ] HP-1 through HP-3

### Round 11: Stream Header Tests (2 tests)
- [ ] SH-1, SH-2

### Round 12: Cross-Partition ORDER BY Tests (2 tests)
- [ ] CO-1, CO-2

### Round 13: Conflict Resolution Test (1 test)
- [ ] CR-1

### Round 14: Skipped Tests with Sisters (4 skipped tests)
- [ ] SKIP-1 through SKIP-4

### Round 15: Documentation & Version
- [ ] Update wiki Known-Limitations.md
- [ ] Update wiki Features.md
- [ ] Bump version to 2.0.5
- [ ] Git tag and push
- [ ] Wiki commit and push

---

## Summary

| Category | Count |
|----------|-------|
| Bug fixes in existing tests | 3 |
| Implementation bug fix (_ttl:-1) | 1 |
| New edge-case tests | ~35 |
| Skipped tests with sisters | 4 |
| Documentation updates | 3 files |
| Total new/modified tests | ~42 |
