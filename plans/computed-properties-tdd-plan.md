# Computed Properties — TDD Deep-Dive Plan

**Date**: April 1, 2026  
**Current version**: 2.0.4  
**Target version**: 2.0.5  
**File**: `tests/CosmosDB.InMemoryEmulator.Tests/ComputedPropertyTests.cs`

---

## Executive Summary

The existing `ComputedPropertyTests.cs` has **15 tests** covering the basics: SELECT/WHERE/ORDER BY/GROUP BY projection, null handling, multiple CPs, ReplaceContainer, arithmetic, concatenation, partition-key scoping, zero-overhead baseline, container-read round-trip, and point-read exclusion.

Deep-dive analysis identified **1 bug in existing tests**, **3 implementation bugs**, and **~25 missing test scenarios** across edge cases, query clause combinations, expression types, and divergent behaviours.

---

## Phase 0 — Bugs in Existing Tests

### Bug 0.1: `ComputedProperty_EvaluatesToNull_WhenSourceMissing` — incorrect assertion

**File**: `ComputedPropertyTests.cs`, test `ComputedProperty_EvaluatesToNull_WhenSourceMissing`  
**Problem**: The test asserts `doc["cp_lowerName"]!.Type.Should().Be(JTokenType.Null)`, i.e. the computed property is present with a null value. In real Cosmos DB, `LOWER(undefined)` evaluates to `undefined`, meaning the property would be **absent** from the projected result, not present-but-null.  
**Root cause**: `UndefinedValue.ToString()` returns `null`, so `LOWER(UndefinedValue)` collapses to C# `null` instead of propagating `UndefinedValue`. Then `AugmentWithComputedProperties` adds the property as `JValue.CreateNull()`.  
**Decision**: This is a deep-rooted issue — `UndefinedValue` propagation through 120+ SQL functions would be a massive change. **Mark the existing test as `Skip` with a detailed reason. Write a sister divergent-behaviour test documenting the emulator's actual behaviour and the real Cosmos behaviour.**

**Status**: [ ] Not started

---

## Phase 1 — Implementation Bugs to Fix

### Bug 1.1: `SELECT c.*` includes computed properties (should strip them)

**Location**: `InMemoryContainer.cs` — `FilterItemsByQuery`  
**Problem**: `SELECT c.*` is parsed with `Expression == "c.*"` which does NOT match the `IsSelectAll` check (`fields[0].Expression == "*"`). Therefore `StripComputedProperties` is never called, and computed properties **leak** into `SELECT c.*` results.  
**Real Cosmos behaviour**: `SELECT c.*` is a wildcard projection and should NOT include computed properties, same as `SELECT *`.  
**Fix**: Either update the `IsSelectAll` check in `CosmosSqlParser.Parse()` to also recognise `c.*` / `<alias>.*`, or add a separate `isAliasedSelectAll` flag and strip computed properties in that case too.  
**TDD**: Write failing test `ComputedProperty_NotIncludedInSelectAliasStar` → fix → green.

**Status**: [ ] Not started

### Bug 1.2: `CONCAT(undefined, ...)` returns empty string instead of undefined

**Location**: `InMemoryContainer.cs` line ~3771  
**Problem**: `CONCAT` uses `a?.ToString() ?? ""` which turns `UndefinedValue` into `""` because `UndefinedValue.ToString()` returns `null`, then `?? ""` kicks in. Real Cosmos: if **any** CONCAT arg is undefined, the entire result is undefined.  
**Scope**: This is a general function bug, not specific to computed properties. However it manifests clearly in computed properties using CONCAT.  
**Decision**: Fix CONCAT to check for `UndefinedValue` arguments and return `UndefinedValue.Instance` if any arg is undefined. This is a targeted fix (one line) unlike fixing all 120+ functions.  
**TDD**: Write failing test `ComputedProperty_ConcatWithUndefinedArg_ReturnsUndefined` → fix → green.

**Status**: [ ] Not started

### Bug 1.3: `ComputedProperty_UpdatedViaReplaceContainer` doesn't verify old CP is removed

**Location**: `ComputedPropertyTests.cs`, test `ComputedProperty_UpdatedViaReplaceContainer`  
**Problem**: The test replaces `cp_lowerName` with `cp_upperName` and verifies the new one works, but never asserts that `cp_lowerName` is gone. This should verify the old computed property no longer resolves.  
**Fix**: Add assertion that querying `SELECT c.cp_lowerName FROM c` after replace returns null/absent.

**Status**: [ ] Not started

---

## Phase 2 — Missing Test Coverage: Query Clause Combinations

### Test 2.1: `ComputedProperty_UsedWithDistinct`
**Query**: `SELECT DISTINCT c.cp_category FROM c`  
**Seeds**: 3 items with categories "Books", "books", "Electronics" → CP `LOWER(c.category)` → expect 2 distinct values  
**Status**: [ ] Not started

### Test 2.2: `ComputedProperty_UsedWithTop`
**Query**: `SELECT TOP 2 c.cp_lowerName FROM c ORDER BY c.cp_lowerName`  
**Seeds**: 3 items → expect first 2 alphabetically  
**Status**: [ ] Not started

### Test 2.3: `ComputedProperty_UsedWithOffsetLimit`
**Query**: `SELECT c.cp_lowerName FROM c ORDER BY c.cp_lowerName OFFSET 1 LIMIT 1`  
**Seeds**: 3 items → expect middle one  
**Status**: [ ] Not started

### Test 2.4: `ComputedProperty_UsedWithValueSelect`
**Query**: `SELECT VALUE c.cp_lowerName FROM c`  
**Seeds**: 2 items → expect raw string values, not wrapped in objects  
**Status**: [ ] Not started

### Test 2.5: `ComputedProperty_UsedInAggregateFunction`
**Query**: `SELECT SUM(c.cp_discountedPrice) FROM c`  
**CP**: `SELECT VALUE c.price * 0.8 FROM c`  
**Seeds**: items with price 100, 200 → expect sum 240  
**Status**: [ ] Not started

### Test 2.6: `ComputedProperty_UsedWithAlias`
**Query**: `SELECT c.cp_lowerName AS lowered FROM c`  
**Seeds**: 1 item → expect `{"lowered": "alice"}`  
**Status**: [ ] Not started

### Test 2.7: `ComputedProperty_UsedInWhereWithComparison`
**Query**: `SELECT c.id FROM c WHERE c.cp_discountedPrice < 100`  
**CP**: `SELECT VALUE c.price * 0.8 FROM c`  
**Seeds**: items with price 100 (80) and 200 (160) → expect only the first  
**Status**: [ ] Not started

### Test 2.8: `ComputedProperty_UsedInWhereWithBooleanLogic`
**Query**: `SELECT c.id FROM c WHERE c.cp_lowerName = 'alice' OR c.cp_lowerName = 'bob'`  
**Seeds**: 3 items → expect 2  
**Status**: [ ] Not started

### Test 2.9: `ComputedProperty_UsedInExpressionInSelect`
**Query**: `SELECT c.price - c.cp_discountedPrice AS savings FROM c`  
**CP**: `SELECT VALUE c.price * 0.8 FROM c` → savings = price * 0.2  
**Seeds**: price 100 → savings 20  
**Status**: [ ] Not started

---

## Phase 3 — Missing Test Coverage: Expression Types

### Test 3.1: `ComputedProperty_NestedPropertyAccess`
**CP**: `SELECT VALUE c.address.city FROM c`  
**Seeds**: `{ address: { city: "London" } }` → expect "London"  
**Status**: [ ] Not started

### Test 3.2: `ComputedProperty_MathFunctions`
**CP**: `SELECT VALUE ROUND(c.price, 0) FROM c`  
**Seeds**: `{ price: 19.99 }` → expect 20  
**Status**: [ ] Not started

### Test 3.3: `ComputedProperty_StringLengthFunction`
**CP**: `SELECT VALUE LENGTH(c.name) FROM c`  
**Seeds**: `{ name: "Alice" }` → expect 5  
**Status**: [ ] Not started

### Test 3.4: `ComputedProperty_SubstringFunction`
**CP**: `SELECT VALUE SUBSTRING(c.categoryName, 0, INDEX_OF(c.categoryName, ',')) FROM c`  
(From the Cosmos DB docs example)  
**Seeds**: `{ categoryName: "Bikes, Touring Bikes" }` → expect "Bikes"  
**Status**: [ ] Not started

### Test 3.5: `ComputedProperty_BooleanExpression`
**CP**: `SELECT VALUE CONTAINS(c.name, 'ali', true) FROM c`  
**Seeds**: `{ name: "Alice" }` → expect `true`  
**Status**: [ ] Not started

### Test 3.6: `ComputedProperty_ConditionalIIF`
**CP**: `SELECT VALUE IIF(c.age >= 18, "adult", "minor") FROM c`  
**Seeds**: `{ age: 25 }` → "adult", `{ age: 10 }` → "minor"  
**Status**: [ ] Not started

### Test 3.7: `ComputedProperty_TypeCheckFunction`
**CP**: `SELECT VALUE IS_STRING(c.name) FROM c`  
**Seeds**: `{ name: "Alice" }` → true, `{ name: 42 }` → false  
**Status**: [ ] Not started

### Test 3.8: `ComputedProperty_CoalesceExpression`
**CP**: `SELECT VALUE c.nickname ?? c.name FROM c`  
**Seeds**: `{ name: "Alice" }` (no nickname) → "Alice", `{ name: "Alice", nickname: "Ali" }` → "Ali"  
**Status**: [ ] Not started

### Test 3.9: `ComputedProperty_ArrayFunction`
**CP**: `SELECT VALUE ARRAY_LENGTH(c.tags) FROM c`  
**Seeds**: `{ tags: ["a", "b", "c"] }` → expect 3  
**Status**: [ ] Not started

---

## Phase 4 — Missing Test Coverage: Edge Cases & Lifecycle

### Test 4.1: `ComputedProperty_EmptyComputedPropertiesCollection`
Tests that an empty `Collection<ComputedProperty>()` (not null) behaves the same as no CPs.  
**Status**: [ ] Not started

### Test 4.2: `ComputedProperty_DifferentFromAlias`
**CP**: `SELECT VALUE LOWER(root.name) FROM root`  
Tests that a non-`c` alias in the computed property definition works correctly.  
**Status**: [ ] Not started

### Test 4.3: `ComputedProperty_EvaluatesPerItem`
**CP**: `SELECT VALUE c.price * 0.9 FROM c`  
**Seeds**: 3 items with different prices → each gets its own computed value  
**Status**: [ ] Not started

### Test 4.4: `ComputedProperty_ReEvaluatesAfterDocumentUpdate`
1. Seed item with name "Alice"  
2. Query → cp_lowerName = "alice"  
3. UpsertItem with name "Bob"  
4. Query again → cp_lowerName = "bob"  
Verifies CPs are not cached per-document.  
**Status**: [ ] Not started

### Test 4.5: `ComputedProperty_ReadItemStreamAsync_ExcludesComputedProperties`
Stream variant of existing `ComputedProperty_DoesNotPersistOnDocument`.  
Verifies `ReadItemStreamAsync` response body doesn't contain computed property names.  
**Status**: [ ] Not started

### Test 4.6: `ComputedProperty_CrossPartitionQuery`
Query without partition key, CPs from items across multiple partitions are all evaluated correctly.  
**Status**: [ ] Not started

### Test 4.7: `ComputedProperty_NameCollisionWithPersistedProperty`
**CP**: `cp_name` → `SELECT VALUE LOWER(c.name) FROM c`  
**Document**: has a persisted field `cp_name` = "Persisted"  
**Real Cosmos behaviour**: Queries use computed value; SELECT * returns persisted value.  
**Decision**: This is complex and nuanced. Write as a behavioural documentation test — if behaviour is too hard to match exactly, **skip with detailed reason and add sister test**.  
**Status**: [ ] Not started

### Test 4.8: `ComputedProperty_ReplaceContainerStreamAsync_InvalidatesCache`
Verifies that `ReplaceContainerStreamAsync` (stream variant) also invalidates the CP cache.  
**Status**: [ ] Not started

### Test 4.9: `ComputedProperty_OldCPRemovedAfterReplace`
After `ReplaceContainerAsync` changes CPs, querying with the old CP name returns null/absent.  
(This extends the fix from Bug 1.3)  
**Status**: [ ] Not started

---

## Phase 5 — Divergent Behaviour Tests (Skip + Sister Pattern)

These tests document behaviour where the emulator intentionally or unavoidably diverges from real Cosmos DB. Each pair consists of:
- A **skipped** test with `[Fact(Skip = "...")]` showing real Cosmos DB behaviour
- A **sister test** (inline-commented) showing the emulator's **actual** behaviour

### Test 5.1a: `ComputedProperty_UndefinedPropagation_RealCosmos` (SKIP)
**Skip reason**: "Emulator returns null instead of undefined for functions with missing-property inputs. LOWER(undefined) should evaluate to undefined (property absent), but emulator evaluates to null (property present with null value). Fixing this requires UndefinedValue propagation through all 120+ SQL function implementations — tracked as a known limitation."  
**Expected (real)**: `doc["cp_lowerName"]` is absent (C# null from JObject indexer)

### Test 5.1b: `ComputedProperty_UndefinedPropagation_EmulatorBehaviour`
**Actual (emulator)**: `doc["cp_lowerName"]` is present with `JTokenType.Null`  
**Inline comment**: Full explanation of the root cause (UndefinedValue.ToString() → null → collapses to null in function chains)

**Status**: [ ] Not started  
**Note**: Retire existing `ComputedProperty_EvaluatesToNull_WhenSourceMissing` — it's replaced by this pair.

### Test 5.2a: `ComputedProperty_ConcatAllUndefined_RealCosmos` (SKIP) — if fix from Bug 1.2 is too invasive
Only needed if Bug 1.2 fix proves too complex. Otherwise Bug 1.2 is fixed and this becomes a normal green test.  
**Status**: [ ] Conditional — depends on Bug 1.2 outcome

---

## Phase 6 — Documentation Updates

### 6.1: Wiki — Known-Limitations.md
Add behavioural difference entry:
> **Computed properties: undefined propagation** — When a computed property's underlying expression references a missing field, real Cosmos DB evaluates the entire expression to `undefined` (the property is absent from query results). The emulator evaluates it to `null` (the property is present with a null value). This applies to all SQL functions (LOWER, UPPER, CONCAT, arithmetic, etc.) when given undefined inputs.

**Status**: [ ] Not started

### 6.2: Wiki — Features.md
Update the Computed Properties section to mention:
- `SELECT c.*` wildcard also excludes computed properties (after Bug 1.1 fix)  
- Computed properties work with DISTINCT, TOP, OFFSET LIMIT, VALUE, aggregates  
- Computed properties work with nested property paths, IIF, type-check functions, coalesce  
- Cross-reference known limitation re: undefined propagation

**Status**: [ ] Not started

### 6.3: Wiki — Feature-Comparison-With-Alternatives.md
No change needed — computed properties already listed as ✅ for InMemoryEmulator.

**Status**: [x] No action needed

### 6.4: README.md
No change needed — computed properties already listed in features.

**Status**: [x] No action needed

---

## Phase 7 — Version Bump, Tag & Push

1. Bump `src/CosmosDB.InMemoryEmulator/CosmosDB.InMemoryEmulator.csproj` → `2.0.5`
2. `git add -A`
3. `git commit -m "v2.0.5: Computed properties — fix SELECT c.* stripping, CONCAT undefined handling, 25+ new tests"`
4. `git tag v2.0.5`
5. `git push; git push --tags`
6. Push wiki changes separately

**Status**: [ ] Not started

---

## Execution Order (TDD Red-Green-Refactor)

```
Phase 0: Fix existing test bug
  0.1  Retire incorrect test → write skip + sister pair (Phase 5.1a/b)

Phase 1: Implementation bugs (red → green)
  1.1  Write failing test for SELECT c.* → fix CosmosSqlParser/InMemoryContainer → green
  1.2  Write failing test for CONCAT(undefined) → fix CONCAT handler → green
  1.3  Extend existing ReplaceContainer test → add old-CP-removed assertion

Phase 2: Query clause combinations (red → green)
  2.1 through 2.9 — write each test, verify green (most should pass already)

Phase 3: Expression types (red → green)
  3.1 through 3.9 — write each test, verify green (most should pass already)

Phase 4: Edge cases & lifecycle (red → green)
  4.1 through 4.9 — write each test, verify green or fix

Phase 5: Divergent behaviour documentation tests
  5.1a/5.1b  (already done in Phase 0)
  5.2a       (conditional on Phase 1 outcome)

Phase 6: Documentation updates
  6.1  Known-Limitations.md
  6.2  Features.md

Phase 7: Version bump, tag, push
```

---

## Test Count Summary

| Category | Count |
|----------|-------|
| Existing tests (kept as-is) | 14 |
| Existing tests retired/replaced | 1 |
| Bug fix tests (new) | 3 |
| Query clause tests (new) | 9 |
| Expression type tests (new) | 9 |
| Edge case / lifecycle tests (new) | 9 |
| Divergent behaviour pairs (new) | 2–3 |
| **Total after plan** | **~47–48** |

---

## Progress Tracker

- [ ] Phase 0 — Bug in existing test
- [ ] Phase 1 — Implementation bugs (3)  
- [ ] Phase 2 — Query clause tests (9)
- [ ] Phase 3 — Expression type tests (9)
- [ ] Phase 4 — Edge case tests (9)
- [ ] Phase 5 — Divergent behaviour tests (2–3)
- [ ] Phase 6 — Documentation updates (2)
- [ ] Phase 7 — Version bump & push
