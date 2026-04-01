# Extended Array Functions — Deep Dive Test Coverage & Bug Fix Plan

**Scope:** `ExtendedArrayFunctionTests.cs` — covers `ARRAY_CONTAINS_ANY`, `ARRAY_CONTAINS_ALL`, `SetIntersect`, `SetUnion`
**Current version:** 2.0.4 → Target: 2.0.5
**Date:** 2026-04-01
**Approach:** TDD — Red-Green-Refactor. Tests first, then implementation fixes.

---

## Table of Contents

1. [Bug Analysis](#1-bug-analysis)
2. [Missing Coverage Analysis](#2-missing-coverage-analysis)
3. [Test Plan](#3-test-plan)
4. [Implementation Tasks](#4-implementation-tasks)
5. [Documentation Updates](#5-documentation-updates)
6. [Release Checklist](#6-release-checklist)
7. [Progress Tracking](#7-progress-tracking)

---

## 1. Bug Analysis

### BUG-1: ARRAY_CONTAINS_ANY / ARRAY_CONTAINS_ALL Signature Mismatch (MAJOR)

**Real Cosmos DB syntax** (from official docs):
```sql
ARRAY_CONTAINS_ANY(<array_expr>, <expr> [, exprN])   -- variadic
ARRAY_CONTAINS_ALL(<array_expr>, <expr> [, exprN])   -- variadic
```
Examples from docs:
```sql
ARRAY_CONTAINS_ANY([1, true, "3", [1,2,3]], 1, true, "3", [1,2,3])  -- true
ARRAY_CONTAINS_ALL([1, 2, 3, 4], 2, 3, 4, 5)                        -- false
```

**Emulator current syntax** (second argument is an **array**, not variadic):
```sql
ARRAY_CONTAINS_ANY(c.tags, ['a', 'z'])    -- emulator-specific syntax
ARRAY_CONTAINS_ALL(c.tags, ['a', 'b'])    -- emulator-specific syntax
```

**Impact:** Any query written using real Cosmos DB variadic syntax will silently misbehave.
Real-world queries like `ARRAY_CONTAINS_ANY(c.tags, 'a', 'z')` would not resolve properly
because the emulator only looks at the second argument and calls `ResolveJArray` on it.

**Fix approach:** Support BOTH forms:
1. If the second argument resolves to a JArray → use existing behaviour (iterate its elements)
2. Otherwise → collect all remaining arguments (args[1..N]) as individual values to match against

**Difficulty:** Medium. Need to change argument resolution in both ARRAY_CONTAINS_ANY and
ARRAY_CONTAINS_ALL case branches. The parser already captures all arguments into `func.Arguments`,
so we just need to check if the second arg is an array or scalar and handle accordingly.

### BUG-2: ToString() Comparison is Type-Insensitive (MEDIUM)

**Location:** All four functions — `ARRAY_CONTAINS_ANY`, `ARRAY_CONTAINS_ALL`, `SetIntersect`, `SetUnion`

**Problem:** The implementation converts all array elements to strings via `element.ToString()` for
HashSet comparison. This means:
- `1` (number) matches `"1"` (string) — **WRONG**: Real Cosmos DB is type-sensitive
- `true` (bool) matches `"True"` (string) — **WRONG**: Different types never match
- `null` may cause `NullReferenceException` or match `"null"` string
- Objects/arrays use their JSON string representation which may not be stable for deep equality

**Real Cosmos DB behaviour:**
```sql
ARRAY_CONTAINS_ANY([1, 2, 3], "1")  -- false (number ≠ string)
SetIntersect([1, 2, "A", "B"], ["A", 1])  -- ["A", 1] (type-aware matching)
```
The docs example `unorderedIntersect: SETINTERSECT([1, 2, "A", "B"], ["A", 1])` returns `["A", 1]`
which confirms type-aware matching IS used but different types are correctly distinguished.

**Fix approach:** Replace `ToString()` comparison with `JToken.DeepEquals()` or a type-aware
comparison that considers JTokenType. Use a custom `IEqualityComparer<JToken>` for the HashSet.

**Difficulty:** Medium. Need a JToken-aware equality comparer that handles:
- Primitive type+value comparison (number vs string vs bool vs null)
- Deep structural equality for objects and arrays

### BUG-3: SetIntersect/SetUnion Return Empty Array for Non-Array/Undefined Input (MINOR)

**Problem:** When the source property doesn't exist (e.g., `c.nonExistentProp`), `ResolveJArray`
returns null. The current code:
- SetIntersect: Returns `new JArray()` (empty array)
- SetUnion: Gracefully skips null arrays, returns whatever the other array has

**Real Cosmos DB behaviour:** When given undefined inputs, these functions likely return undefined
(omitted from output, not an empty array).

**Fix approach:** Return `UndefinedValue.Instance` when both/either input is null/undefined,
depending on the function semantics. Need to verify exact Cosmos DB behaviour for each case:
- Both undefined → undefined
- One undefined, one valid → undefined (likely)

**Difficulty:** Low-Medium. Straightforward null→undefined mapping, but need to verify exact
Cosmos DB semantics. Mark as skipped+divergent if behaviour is hard to confirm.

### BUG-4: ARRAY_CONTAINS_ALL Empty Source Array Behaviour (MINOR)

**Current:** If source is null (non-existent property), returns false. If source is empty `[]` with
non-empty search, the HashSet check `searchArray.All(t => sourceValues.Contains(...))` returns false
(correct). But the docs show:
```sql
ARRAY_CONTAINS_ALL([], 1, 2, 3)  -- false (confirmed)
```
The emulator doesn't have this direct test (empty source + non-empty search via `[]` literal).
Currently an indirect test via `c.id = '4'` (item with empty tags). Need explicit test.

---

## 2. Missing Coverage Analysis

### Existing test count per function:

| Function | Tests | Gaps |
|----------|-------|------|
| ARRAY_CONTAINS_ANY | 5 | 16 |
| ARRAY_CONTAINS_ALL | 4 | 14 |
| SetIntersect | 3 | 14 |
| SetUnion | 3 | 13 |
| **Total** | **15** | **57** |

### Gap categories:

**Type handling (affects all 4 functions):**
- Numeric element matching
- Boolean element matching
- Mixed-type arrays (numbers + strings + bools)
- Null element handling
- Object elements
- Nested array elements
- Type-sensitive comparison (number ≠ string) — BUG-2

**Edge cases (affects all 4 functions):**
- Non-existent property path → should return undefined/false
- Non-array property (scalar) → should return undefined/false
- Nested property paths (`c.nested.items`)
- Duplicate elements in source/search arrays
- Both empty arrays
- Empty second array

**API / syntax (ARRAY_CONTAINS_ANY/ALL only):**
- Variadic argument form (real Cosmos DB syntax) — BUG-1
- Both arrays from document properties

**Integration / composition:**
- In projection (`SELECT func(...) AS result FROM c`)
- Nested in other functions (`ARRAY_LENGTH(SetIntersect(...))`)
- Combined with other WHERE conditions (AND/OR)
- With parameterized queries (`@param`)
- In subqueries

---

## 3. Test Plan

### Test naming convention: `Function_Scenario_ExpectedResult`

### Seed data enhancement

Need additional seed items with diverse types:

```csharp
var items = new[]
{
    // Existing items (string tags: a,b,c / b,c,d / x,y,z / empty)
    new { id = "1", partitionKey = "pk1", name = "Alice",   tags = new[] { "a", "b", "c" } },
    new { id = "2", partitionKey = "pk1", name = "Bob",     tags = new[] { "b", "c", "d" } },
    new { id = "3", partitionKey = "pk1", name = "Charlie", tags = new[] { "x", "y", "z" } },
    new { id = "4", partitionKey = "pk1", name = "Diana",   tags = new string[0] },

    // New items for type-diverse testing (raw JObject insertion):
    // id=5: numeric tags [1, 2, 3]
    // id=6: mixed types [1, "a", true, null]
    // id=7: object elements [{name:"x"}, {name:"y"}]
    // id=8: nested array property (nested.items = ["p","q"])
    // id=9: no tags property at all (test undefined behaviour)
    // id=10: tags is a string scalar, not array (test non-array)
    // id=11: tags with duplicates ["a", "a", "b"]
};
```

For items 5–11, use raw JObject insertion since TestDocument only has `string[] Tags`.

---

### 3.1 ARRAY_CONTAINS_ANY New Tests

| # | Test Name | Status | Notes |
|---|-----------|--------|-------|
| A1 | `ArrayContainsAny_WithNumericElements_ReturnsTrue` | TODO | Source=[1,2,3], search=[2,4] → true (item 5) |
| A2 | `ArrayContainsAny_WithMixedTypes_MatchesSameTypeOnly` | TODO | Source=[1,"a",true,null], search=["a"] → true (item 6). **Tests BUG-2** |
| A3 | `ArrayContainsAny_NumberDoesNotMatchString_TypeSensitive` | TODO | Source=[1,2,3], search=["1"] → false. **Validates BUG-2 fix** |
| A4 | `ArrayContainsAny_BoolDoesNotMatchString_TypeSensitive` | TODO | Source=[1,"a",true,null], search=["true"] → false. **Validates BUG-2 fix** |
| A5 | `ArrayContainsAny_WithNullElement_MatchesNull` | TODO | Source=[1,"a",true,null], search=[null] → true (item 6) |
| A6 | `ArrayContainsAny_NonExistentProperty_ReturnsFalse` | TODO | Query `c.nonExistent` for item 9 → false |
| A7 | `ArrayContainsAny_NonArrayProperty_ReturnsFalse` | TODO | Query `c.name` (string scalar) → false |
| A8 | `ArrayContainsAny_InProjection_ReturnsBoolean` | TODO | `SELECT ARRAY_CONTAINS_ANY(c.tags, ['a']) AS result FROM c WHERE c.id = '1'` → {result: true} |
| A9 | `ArrayContainsAny_WithDuplicatesInSource_StillMatches` | TODO | Source=["a","a","b"], search=["a"] → true (item 11) |
| A10 | `ArrayContainsAny_WithDuplicatesInSearch_StillMatches` | TODO | Source=["a","b","c"], search=["a","a"] → true |
| A11 | `ArrayContainsAny_BothArraysFromDocument_Works` | TODO | Need two array props: `ARRAY_CONTAINS_ANY(c.tags, c.otherTags)`. Create item with tags=["a","b"] and otherTags=["b","c"] |
| A12 | `ArrayContainsAny_NestedPropertyPath_Works` | TODO | `ARRAY_CONTAINS_ANY(c.nested.items, ['p'])` for item 8 → true |
| A13 | `ArrayContainsAny_WithObjectElements_MatchesDeepEqual` | TODO | May be SKIP — real Cosmos deep-compares objects in arrays. Source=[{name:"x"},{name:"y"}], search=[{name:"x"}] |
| A14 | `ArrayContainsAny_VariadicForm_Works` | TODO | `ARRAY_CONTAINS_ANY(c.tags, 'a', 'z')` — **real Cosmos syntax, tests BUG-1 fix** |
| A15 | `ArrayContainsAny_VariadicForm_SingleArg_Works` | TODO | `ARRAY_CONTAINS_ANY(c.tags, 'a')` — single variadic arg |
| A16 | `ArrayContainsAny_SingleElementMatch_ReturnsTrue` | TODO | Source=["a"], search=["a"] → true |

### 3.2 ARRAY_CONTAINS_ALL New Tests

| # | Test Name | Status | Notes |
|---|-----------|--------|-------|
| B1 | `ArrayContainsAll_SourceEqualsSearch_ReturnsTrue` | TODO | Source=["a","b","c"], search=["a","b","c"] → true |
| B2 | `ArrayContainsAll_SourceIsSupersetOfSearch_ReturnsTrue` | TODO | Source=["a","b","c"], search=["a","b"] → true |
| B3 | `ArrayContainsAll_EmptySourceWithNonEmptySearch_ReturnsFalse` | TODO | Source=[], search=["a"] → false (item 4) |
| B4 | `ArrayContainsAll_WithNumericElements_ReturnsTrue` | TODO | Source=[1,2,3], search=[1,3] → true (item 5) |
| B5 | `ArrayContainsAll_NumberDoesNotMatchString_TypeSensitive` | TODO | Source=[1,2,3], search=["1","2"] → false. **Validates BUG-2 fix** |
| B6 | `ArrayContainsAll_WithNullElement_MatchesNull` | TODO | Source=[1,"a",true,null], search=[null] → true |
| B7 | `ArrayContainsAll_NonExistentProperty_ReturnsFalse` | TODO | `c.nonExistent` → false |
| B8 | `ArrayContainsAll_NonArrayProperty_ReturnsFalse` | TODO | `c.name` → false |
| B9 | `ArrayContainsAll_DuplicatesInSearch_StillWorks` | TODO | Source=["a","b"], search=["a","a"] → true (element exists, duplicates don't matter) |
| B10 | `ArrayContainsAll_InProjection_ReturnsBoolean` | TODO | `SELECT ARRAY_CONTAINS_ALL(c.tags, ['a']) AS result FROM c WHERE c.id = '1'` |
| B11 | `ArrayContainsAll_BothEmptyArrays_ReturnsTrue` | TODO | Source=[], search=[] → true (vacuously true, already partially tested) |
| B12 | `ArrayContainsAll_VariadicForm_Works` | TODO | `ARRAY_CONTAINS_ALL(c.tags, 'a', 'b', 'c')` — **tests BUG-1 fix** |
| B13 | `ArrayContainsAll_VariadicForm_SingleMissing_ReturnsFalse` | TODO | `ARRAY_CONTAINS_ALL(c.tags, 'a', 'q')` → false |
| B14 | `ArrayContainsAll_NestedPropertyPath_Works` | TODO | `c.nested.items` for item 8 |

### 3.3 SetIntersect New Tests

| # | Test Name | Status | Notes |
|---|-----------|--------|-------|
| C1 | `SetIntersect_BothEmpty_ReturnsEmptyArray` | TODO | Source=[], literal=[] → [] |
| C2 | `SetIntersect_EmptySecondArray_ReturnsEmptyArray` | TODO | Source=["a","b"], literal=[] → [] |
| C3 | `SetIntersect_CompleteOverlap_ReturnsSameElements` | TODO | `SetIntersect(c.tags, c.tags)` → ["a","b","c"] |
| C4 | `SetIntersect_DuplicatesInInput_DedupedInResult` | TODO | Source=["a","a","b"], literal=["a","b"] → ["a","b"] (no dup) |
| C5 | `SetIntersect_WithNumericElements_Works` | TODO | Source=[1,2,3], literal=[2,4] → [2] (item 5) |
| C6 | `SetIntersect_TypeSensitive_NumberDoesNotMatchString` | TODO | Source=[1,2,3], literal=["1","2"] → [] **BUG-2** |
| C7 | `SetIntersect_OrderPreserved_FromFirstArray` | TODO | Source=["c","a","b"], literal=["b","a"] → ["a","b"]? Verify order is from first array's iteration |
| C8 | `SetIntersect_NonExistentProperty_ReturnsUndefined` | TODO | `c.nonExistent` → undefined. **BUG-3** — may need SKIP+divergent if returns empty array instead |
| C9 | `SetIntersect_NonArrayProperty_ReturnsUndefined` | TODO | `c.name` (string) → undefined. **BUG-3** |
| C10 | `SetIntersect_WithNullElements_Works` | TODO | Source=[1,null,"a"], literal=[null,"a"] → [null,"a"] |
| C11 | `SetIntersect_NestedInArrayLength_Works` | TODO | `SELECT ARRAY_LENGTH(SetIntersect(c.tags, ['a','b','x'])) AS count` |
| C12 | `SetIntersect_InProjection_ReturnsArray` | TODO | `SELECT SetIntersect(c.tags, ['a','b','x']) AS common FROM c WHERE c.id = '1'` |
| C13 | `SetIntersect_BothFromDocument_Works` | TODO | `SetIntersect(c.tags, c.otherTags)` |
| C14 | `SetIntersect_WithMixedTypes_OnlyMatchesSameType` | TODO | Source=[1,"a",true], literal=[1,"b",true] → [1,true] |

### 3.4 SetUnion New Tests

| # | Test Name | Status | Notes |
|---|-----------|--------|-------|
| D1 | `SetUnion_BothEmpty_ReturnsEmptyArray` | TODO | Source=[], literal=[] → [] |
| D2 | `SetUnion_EmptySecondArray_ReturnsFirstArray` | TODO | Source=["a","b"], literal=[] → ["a","b"] |
| D3 | `SetUnion_DuplicatesWithinSingleArray_Deduped` | TODO | Source=["a","a","b"], literal=["c"] → ["a","b","c"] |
| D4 | `SetUnion_WithNumericElements_Works` | TODO | Source=[1,2], literal=[3,4] → [1,2,3,4] (item 5) |
| D5 | `SetUnion_TypeSensitive_NumberAndStringBothKept` | TODO | Source=[1,2], literal=["1","2"] → [1,2,"1","2"] **BUG-2** |
| D6 | `SetUnion_OrderPreserved_FirstThenSecond` | TODO | Source=["c","a"], literal=["b","d"] → ["c","a","b","d"] |
| D7 | `SetUnion_NonExistentProperty_ReturnsUndefined` | TODO | **BUG-3** — may need SKIP+divergent |
| D8 | `SetUnion_NonArrayProperty_ReturnsUndefined` | TODO | **BUG-3** |
| D9 | `SetUnion_WithNullElements_Works` | TODO | Source=[1,null], literal=["a",null] → [1,null,"a"] (null deduped) |
| D10 | `SetUnion_NestedInArrayLength_Works` | TODO | `ARRAY_LENGTH(SetUnion(c.tags, ['x','y']))` |
| D11 | `SetUnion_BothFromDocument_Works` | TODO | `SetUnion(c.tags, c.otherTags)` |
| D12 | `SetUnion_InProjection_ReturnsArray` | TODO | `SELECT SetUnion(c.tags, ['x']) AS combined FROM c WHERE c.id = '1'` |
| D13 | `SetUnion_WithMixedTypes_AllKept` | TODO | Source=[1,"a",true], literal=[2,"b",false] → [1,"a",true,2,"b",false] |

### 3.5 Cross-Cutting / Integration Tests

| # | Test Name | Status | Notes |
|---|-----------|--------|-------|
| X1 | `ExtendedArrayFunctions_WithParameterizedQuery_Works` | TODO | Use `@searchValues` with QueryDefinition.WithParameter |
| X2 | `ExtendedArrayFunctions_CombinedInWhereClause_Works` | TODO | `WHERE ARRAY_CONTAINS_ANY(c.tags, ['a']) AND ARRAY_CONTAINS_ALL(c.tags, ['a','b'])` |
| X3 | `SetIntersect_ResultUsedInWhere_WithArrayLength` | TODO | `WHERE ARRAY_LENGTH(SetIntersect(c.tags, ['a','b'])) > 0` |

---

## 4. Implementation Tasks

### Task order (TDD cycle per bug):

#### Phase 1: BUG-2 — Type-Sensitive Comparison
1. **Write failing tests:** A3, A4, A5, B5, B6, C6, C10, C14, D5, D9, D13
2. **Implement fix:** Create `JTokenDeepEqualityComparer : IEqualityComparer<JToken>` that:
   - Compares by `JTokenType` first (different types → not equal)
   - For same types: uses `JToken.DeepEquals` for value comparison
   - For HashSet: implements `GetHashCode` based on type + value
3. **Replace** all `HashSet<string>(array.Select(t => t.ToString()))` with `HashSet<JToken>(array, new JTokenDeepEqualityComparer())`
4. **Run tests, verify green**

#### Phase 2: BUG-1 — Variadic Argument Support
1. **Write failing tests:** A14, A15, B12, B13
2. **Implement fix:** In ARRAY_CONTAINS_ANY and ARRAY_CONTAINS_ALL cases:
   ```csharp
   // If second arg is already a JArray (array literal or identifier), use it directly
   // Otherwise, collect remaining args as individual search values
   JArray searchArray;
   var secondArg = ResolveJArray(func.Arguments[1], item, fromAlias, parameters);
   if (secondArg is not null)
   {
       searchArray = secondArg;
   }
   else
   {
       searchArray = new JArray();
       for (int i = 1; i < func.Arguments.Length; i++)
       {
           var val = EvaluateSqlExpression(func.Arguments[i], item, fromAlias, parameters);
           if (val is not null && val is not UndefinedValue) searchArray.Add(JToken.FromObject(val));
       }
   }
   ```
3. **Run tests, verify green**

#### Phase 3: BUG-3 — Undefined Return for Non-Array Inputs
1. **Write tests:** C8, C9, D7, D8 — these may need to be SKIPPED with divergent sister tests
   if the fix is too complex (returning undefined vs empty array affects the entire expression
   evaluation chain)
2. **Assess difficulty:**
   - If returning `UndefinedValue.Instance` from SetIntersect/SetUnion just works in the
     expression pipeline → implement
   - If it causes cascading issues in SELECT/WHERE evaluation → SKIP with detailed reason,
     create divergent sister tests
3. **Run tests**

#### Phase 4: Remaining Coverage Tests
1. Write all remaining tests from sections 3.1–3.5 (edge cases, projections, composition, etc.)
2. These should mostly pass on the existing (now fixed) implementation
3. Any failures → new bugs to investigate and fix

---

## 5. Documentation Updates

### 5.1 Known-Limitations.md

**If BUG-1 is fully fixed:** No new limitation needed — variadic form now supported.
**If BUG-1 is partially fixed (array form kept, variadic added):** Note both forms supported.

**If BUG-3 is NOT fixed (SKIP'd):** Add new limitation entry:
```markdown
### N. SetIntersect/SetUnion Return Empty Array for Undefined Inputs

**Real Cosmos DB:** `SetIntersect(undefined, [...])` and `SetUnion(undefined, [...])` return
undefined (the property is omitted from the result).

**InMemoryContainer:** Returns an empty array `[]` instead of undefined when either input
is undefined (e.g., property doesn't exist).

**Impact:** Low. Only affects queries on documents where the array property is missing entirely.

**Test:** `SetIntersect_NonExistentProperty_EmulatorReturnsEmptyArray` (divergent)
```

**If BUG-2 is partially fixed:** Document any remaining type comparison edge cases.

### 5.2 Feature-Comparison-With-Alternatives.md

Update the SQL query function rows to note:
- Extended array functions fully support variadic AND array argument forms (if BUG-1 fixed)
- Type-sensitive element comparison (if BUG-2 fixed)

### 5.3 SQL-Queries.md

Verify `ARRAY_CONTAINS_ANY`, `ARRAY_CONTAINS_ALL`, `SetIntersect`, `SetUnion` are listed in the
array functions section. If not, add them with syntax examples showing both variadic and array forms.

### 5.4 README.md

No change needed — README already mentions "100+ built-in functions" and links to wiki.
Update test count if it changes significantly (currently "1350+ tests").

### 5.5 Features.md

No change expected unless new functionality is added beyond bug fixes.

---

## 6. Release Checklist

- [ ] All new tests written and passing (or explicitly SKIP'd with reasons)
- [ ] All bugs fixed or documented as known limitations
- [ ] Wiki Known-Limitations.md updated
- [ ] Wiki Feature-Comparison-With-Alternatives.md updated (if applicable)
- [ ] Wiki SQL-Queries.md verified
- [ ] README.md test count updated (if needed)
- [ ] Version bumped: `<Version>2.0.5</Version>` in `CosmosDB.InMemoryEmulator.csproj`
- [ ] `dotnet test` all green
- [ ] `git add -A && git commit -m "v2.0.5: Extended array function coverage — type-sensitive comparison, variadic syntax, edge cases"`
- [ ] `git tag v2.0.5`
- [ ] `git push && git push --tags`
- [ ] Wiki committed and pushed separately

---

## 7. Progress Tracking

### Phase 1: BUG-2 — Type-Sensitive Comparison
- [ ] Write failing tests (A3, A4, A5, B5, B6, C6, C10, C14, D5, D9, D13)
- [ ] Implement JTokenDeepEqualityComparer
- [ ] Replace ToString() comparisons in all 4 functions
- [ ] All tests green

### Phase 2: BUG-1 — Variadic Argument Support
- [ ] Write failing tests (A14, A15, B12, B13)
- [ ] Implement variadic fallback in ARRAY_CONTAINS_ANY
- [ ] Implement variadic fallback in ARRAY_CONTAINS_ALL
- [ ] All tests green

### Phase 3: BUG-3 — Undefined Return for Non-Array Inputs
- [ ] Write tests (C8, C9, D7, D8)
- [ ] Assess fix difficulty
- [ ] Implement or SKIP+divergent
- [ ] All tests green or explicitly skipped

### Phase 4: Remaining Coverage
- [ ] Seed data enhancement (items 5-11 with diverse types)
- [ ] ARRAY_CONTAINS_ANY edge cases (A1, A2, A6-A13, A16)
- [ ] ARRAY_CONTAINS_ALL edge cases (B1-B4, B7-B11, B14)
- [ ] SetIntersect edge cases (C1-C5, C7, C11-C13)
- [ ] SetUnion edge cases (D1-D4, D6, D10-D12)
- [ ] Cross-cutting tests (X1-X3)
- [ ] All tests green

### Phase 5: Documentation & Release
- [ ] Update Known-Limitations.md
- [ ] Update Feature-Comparison-With-Alternatives.md
- [ ] Verify SQL-Queries.md
- [ ] Update README.md test count
- [ ] Bump version to 2.0.5
- [ ] Tag and push
- [ ] Wiki push
