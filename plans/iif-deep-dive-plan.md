# IIF Function Deep Dive — TDD Plan

**Date:** 2026-04-01
**Current version:** 2.0.4
**Target version:** 2.0.5

---

## Executive Summary

Deep analysis of the `IIF` function revealed a **critical correctness bug** and significant **missing test coverage**.

### Bug Found

The emulator uses JavaScript-like truthiness (`IsTruthy()`) to evaluate the IIF condition, but real Cosmos DB **only treats boolean `true` as truthy**. All non-boolean values (numbers, strings, arrays, objects) cause IIF to return the **false branch** in real Cosmos DB.

**Evidence from official Microsoft documentation:**
```sql
SELECT VALUE {
  evalTrue: IIF(true, 123, 456),           -- 123
  evalFalse: IIF(false, 123, 456),         -- 456
  evalNumberNotTrue: IIF(123, 123, 456),   -- 456  ← emulator returns 123 (BUG)
  evalStringNotTrue: IIF("ABC", 123, 456), -- 456  ← emulator returns 123 (BUG)
  evalArrayNotTrue: IIF([1,2,3], 123, 456),-- 456  ← emulator returns 123 (BUG)
  evalObjectNotTrue: IIF({...}, 123, 456)  -- 456  ← emulator returns 123 (BUG)
}
```
Source: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/iif

### Root Cause

In `InMemoryContainer.cs` line ~4276:
```csharp
case "IIF":
    if (args.Length < 3) return null;
    return IsTruthy(args[0]) ? args[1] : args[2];  // ← BUG: should be `args[0] is true`
```

`IsTruthy()` treats `long != 0` as true, `string.Length > 0` as true, and everything else (including arrays/objects) as true via the `_ => true` default case. Real Cosmos DB IIF only returns the true branch when the condition is **exactly boolean `true`**.

### Fix

Replace `IsTruthy(args[0])` with `args[0] is true` in the IIF case only. This does NOT affect `IsTruthy` usage elsewhere (AND/OR/NOT/WHERE) — those are separate concerns.

### Impact on Existing Tests

None. All 6 existing tests use boolean conditions (field references to `bool` properties or comparison expressions), which evaluate to `true`/`false` and work correctly with both `IsTruthy(args[0])` and `args[0] is true`.

---

## Test Coverage Analysis

### Currently Covered (6 tests)
- [x] True boolean condition (`c.isActive` = true)
- [x] False boolean condition (`c.isActive` = false)
- [x] Comparison expression (`c.value > 15`)
- [x] Numeric return values
- [x] IIF in WHERE clause
- [x] Nested IIF (2 levels)

### Missing Coverage (29 new tests planned)

| # | Category | Gap |
|---|----------|-----|
| 1–8 | Non-boolean conditions | Numbers, strings, arrays, objects as IIF condition — the main bug |
| 9–10 | Null/undefined conditions | Null literal and missing properties |
| 11–17 | Complex boolean expressions | AND, OR, NOT, equality, IS_DEFINED, CONTAINS, ARRAY_LENGTH |
| 18–23 | Return value variations | Mixed types, null branches, expressions, functions, booleans |
| 24–26 | Nesting & composition | Triple nesting, inside other functions, multiple in SELECT |
| 27–29 | Usage contexts | ORDER BY, parameterized queries, VALUE select |
| 30 | Edge cases | Function name case insensitivity |

---

## Detailed Test Plan

### Phase 1: Write Tests (RED)

All tests go in `tests/CosmosDB.InMemoryEmulator.Tests/IifFunctionTests.cs`.

---

#### Group 1: Non-Boolean Condition Bug Fix (8 tests)

These tests verify the main bug fix. Currently 6 of 8 will FAIL (red), 2 pass accidentally.

**T1: `Iif_NumericNonZeroCondition_ReturnsFalseBranch`**
```sql
SELECT IIF(42, 'yes', 'no') AS result FROM c WHERE c.id = '1'
```
- Expected: `'no'` (42 is not boolean true)
- Current emulator: `'yes'` (BUG — IsTruthy(42L) = true)
- Status: [ ] Write test → [ ] Confirm RED → [ ] Fix → [ ] Confirm GREEN

**T2: `Iif_NumericZeroCondition_ReturnsFalseBranch`**
```sql
SELECT IIF(0, 'yes', 'no') AS result FROM c WHERE c.id = '1'
```
- Expected: `'no'` (0 is not boolean true)
- Current emulator: `'no'` (passes accidentally — IsTruthy(0) = false)
- Status: [ ] Write test → [ ] Confirm GREEN (accidental) → [ ] Stays GREEN after fix

**T3: `Iif_StringCondition_ReturnsFalseBranch`**
```sql
SELECT IIF('hello', 'yes', 'no') AS result FROM c WHERE c.id = '1'
```
- Expected: `'no'` ('hello' is not boolean true)
- Current emulator: `'yes'` (BUG — IsTruthy("hello") = true)
- Status: [ ] Write test → [ ] Confirm RED → [ ] Fix → [ ] Confirm GREEN

**T4: `Iif_EmptyStringCondition_ReturnsFalseBranch`**
```sql
SELECT IIF('', 'yes', 'no') AS result FROM c WHERE c.id = '1'
```
- Expected: `'no'` ('' is not boolean true)
- Current emulator: `'no'` (passes accidentally — IsTruthy("") = false)
- Status: [ ] Write test → [ ] Confirm GREEN (accidental) → [ ] Stays GREEN after fix

**T5: `Iif_NumericFieldAsCondition_ReturnsFalseBranch`**
```sql
SELECT IIF(c.value, 'yes', 'no') AS result FROM c ORDER BY c.id
```
- Expected: all 3 items return `'no'` (numeric field is never boolean)
- Current emulator: `'no'` for value=0, `'yes'` for value=10,20 (BUG)
- Status: [ ] Write test → [ ] Confirm RED → [ ] Fix → [ ] Confirm GREEN

**T6: `Iif_StringFieldAsCondition_ReturnsFalseBranch`**
```sql
SELECT IIF(c.name, 'yes', 'no') AS result FROM c ORDER BY c.id
```
- Expected: all 3 items return `'no'` (string field is never boolean)
- Current emulator: `'yes'` for all (BUG — all names are non-empty)
- Status: [ ] Write test → [ ] Confirm RED → [ ] Fix → [ ] Confirm GREEN

**T7: `Iif_ArrayFieldAsCondition_ReturnsFalseBranch`**
```sql
SELECT IIF(c.tags, 'yes', 'no') AS result FROM c ORDER BY c.id
```
- Expected: all 3 items return `'no'` (array field is never boolean)
- Current emulator: `'yes'` for all (BUG — IsTruthy(_ => true) for JArray)
- Note: Item 3 has empty array `[]` — still returns `'no'` in real Cosmos
- Status: [ ] Write test → [ ] Confirm RED → [ ] Fix → [ ] Confirm GREEN

**T8: `Iif_ObjectFieldAsCondition_ReturnsFalseBranch`**
```sql
SELECT IIF(c.nested, 'yes', 'no') AS result FROM c WHERE c.id = '1'
```
- Expected: `'no'` (object field is never boolean)
- Note: Need to seed item with nested object. Item 1 has `Nested = null` by default.
  May need to add a seeded item with a non-null nested object for this test.
- Status: [ ] Write test → [ ] Confirm RED → [ ] Fix → [ ] Confirm GREEN

---

#### Group 2: Null and Undefined Conditions (2 tests)

These should pass both before and after the fix.

**T9: `Iif_NullCondition_ReturnsFalseBranch`**
```sql
SELECT IIF(null, 'yes', 'no') AS result FROM c WHERE c.id = '1'
```
- Expected: `'no'` (null is not boolean true)
- Current emulator: `'no'` (IsTruthy(null) = false → correct)
- Status: [ ] Write test → [ ] Confirm GREEN

**T10: `Iif_UndefinedPropertyCondition_ReturnsFalseBranch`**
```sql
SELECT IIF(c.nonExistentField, 'yes', 'no') AS result FROM c WHERE c.id = '1'
```
- Expected: `'no'` (undefined is not boolean true)
- Current emulator: `'no'` (IsTruthy(UndefinedValue) = false → correct)
- Status: [ ] Write test → [ ] Confirm GREEN

---

#### Group 3: Complex Boolean Expressions in Condition (7 tests)

These test various boolean-producing expressions. Should all pass before and after fix.

**T11: `Iif_WithAndCondition_EvaluatesCorrectly`**
```sql
SELECT IIF(c.isActive AND c.value > 5, 'yes', 'no') AS result FROM c ORDER BY c.id
```
- Expected: 'yes' (id=1: active AND 10>5), 'no' (id=2: not active), 'no' (id=3: active AND NOT 0>5)
- Status: [ ] Write test → [ ] Confirm GREEN

**T12: `Iif_WithOrCondition_EvaluatesCorrectly`**
```sql
SELECT IIF(c.isActive OR c.value > 15, 'yes', 'no') AS result FROM c ORDER BY c.id
```
- Expected: 'yes' (id=1: active), 'yes' (id=2: 20>15), 'yes' (id=3: active)
- Status: [ ] Write test → [ ] Confirm GREEN

**T13: `Iif_WithNotCondition_EvaluatesCorrectly`**
```sql
SELECT IIF(NOT c.isActive, 'inactive', 'active') AS status FROM c ORDER BY c.id
```
- Expected: 'active' (id=1), 'inactive' (id=2), 'active' (id=3)
- Status: [ ] Write test → [ ] Confirm GREEN

**T14: `Iif_WithEqualityCondition_EvaluatesCorrectly`**
```sql
SELECT IIF(c.name = 'Alice', 'found', 'not found') AS result FROM c ORDER BY c.id
```
- Expected: 'found' (id=1), 'not found' (id=2), 'not found' (id=3)
- Status: [ ] Write test → [ ] Confirm GREEN

**T15: `Iif_WithIsDefinedCondition_EvaluatesCorrectly`**
```sql
SELECT IIF(IS_DEFINED(c.nested), 'has nested', 'no nested') AS result FROM c WHERE c.id = '1'
```
- Expected: depends on seed data — item 1 has Nested = null, so IS_DEFINED may still return true
  (the property exists in JSON even if null). Need to seed one item WITHOUT nested property.
- Status: [ ] Write test → [ ] Confirm GREEN

**T16: `Iif_WithContainsFunctionCondition_EvaluatesCorrectly`**
```sql
SELECT IIF(CONTAINS(c.name, 'li'), 'match', 'no match') AS result FROM c ORDER BY c.id
```
- Expected: 'match' (Alice has 'li'), 'no match' (Bob), 'no match' (Charlie — 'li' not in 'arlie')
  Wait: 'Charlie' DOES contain 'li' → 'match'. Let me use 'Ali' instead.
```sql
SELECT IIF(CONTAINS(c.name, 'Ali'), 'match', 'no match') AS result FROM c ORDER BY c.id
```
- Expected: 'match' (Alice→Ali), 'no match' (Bob), 'no match' (Charlie)
- Status: [ ] Write test → [ ] Confirm GREEN

**T17: `Iif_WithArrayLengthComparisonCondition_EvaluatesCorrectly`**
```sql
SELECT IIF(ARRAY_LENGTH(c.tags) > 0, 'tagged', 'untagged') AS result FROM c ORDER BY c.id
```
- Expected: 'tagged' (id=1: 2 tags), 'tagged' (id=2: 1 tag), 'untagged' (id=3: 0 tags)
- Status: [ ] Write test → [ ] Confirm GREEN

---

#### Group 4: Return Value Variations (6 tests)

**T18: `Iif_WithMixedReturnTypes_ReturnsCorrectType`**
```sql
SELECT IIF(c.isActive, 42, 'inactive') AS result FROM c WHERE c.id = '1'
```
- Expected: 42 (number, not string)
```sql
SELECT IIF(c.isActive, 42, 'inactive') AS result FROM c WHERE c.id = '2'
```
- Expected: 'inactive' (string)
- Status: [ ] Write test → [ ] Confirm GREEN

**T19: `Iif_WithNullTrueBranch_ReturnsNull`**
```sql
SELECT IIF(true, null, 'fallback') AS result FROM c WHERE c.id = '1'
```
- Expected: result is null (or property absent in JSON)
- Status: [ ] Write test → [ ] Confirm GREEN

**T20: `Iif_WithNullFalseBranch_ReturnsNull`**
```sql
SELECT IIF(false, 'value', null) AS result FROM c WHERE c.id = '1'
```
- Expected: result is null
- Status: [ ] Write test → [ ] Confirm GREEN

**T21: `Iif_WithExpressionReturnValues_EvaluatesExpressions`**
```sql
SELECT IIF(c.isActive, c.value * 2, c.value) AS computed FROM c ORDER BY c.id
```
- Expected: 20 (id=1: active, 10*2), 20 (id=2: not active, raw value 20), 0 (id=3: active, 0*2)
- Status: [ ] Write test → [ ] Confirm GREEN

**T22: `Iif_WithFunctionCallReturnValues_EvaluatesFunctions`**
```sql
SELECT IIF(c.isActive, UPPER(c.name), LOWER(c.name)) AS result FROM c ORDER BY c.id
```
- Expected: 'ALICE' (id=1: active→UPPER), 'bob' (id=2: not active→LOWER), 'CHARLIE' (id=3: active→UPPER)
- Status: [ ] Write test → [ ] Confirm GREEN

**T23: `Iif_WithBooleanReturnValues_ReturnsBooleans`**
```sql
SELECT IIF(c.value > 10, true, false) AS overTen FROM c ORDER BY c.id
```
- Expected: false (id=1: 10 NOT > 10), true (id=2: 20>10), false (id=3: 0)
- Status: [ ] Write test → [ ] Confirm GREEN

---

#### Group 5: Advanced Nesting and Composition (3 tests)

**T24: `Iif_TripleNested_EvaluatesCorrectly`**
```sql
SELECT IIF(c.value > 15, 'high', IIF(c.value > 5, 'medium', IIF(c.value > 0, 'low', 'zero'))) AS tier FROM c ORDER BY c.id
```
- Expected: 'medium' (id=1: 10), 'high' (id=2: 20), 'zero' (id=3: 0)
- Status: [ ] Write test → [ ] Confirm GREEN

**T25: `Iif_InsideConcat_EvaluatesCorrectly`**
```sql
SELECT CONCAT(IIF(c.isActive, 'Active', 'Inactive'), ': ', c.name) AS label FROM c ORDER BY c.id
```
- Expected: 'Active: Alice', 'Inactive: Bob', 'Active: Charlie'
- Status: [ ] Write test → [ ] Confirm GREEN

**T26: `Iif_MultipleInSameSelect_EvaluatesIndependently`**
```sql
SELECT IIF(c.isActive, 'active', 'inactive') AS status, IIF(c.value > 10, 'high', 'low') AS level FROM c ORDER BY c.id
```
- Expected: (active, low), (inactive, high), (active, low)
- Status: [ ] Write test → [ ] Confirm GREEN

---

#### Group 6: Usage Contexts (3 tests)

**T27: `Iif_InOrderByClause_SortsCorrectly`**
```sql
SELECT c.name FROM c ORDER BY IIF(c.isActive, 0, 1), c.name
```
- Expected: Active items first (sorted by name: Alice, Charlie), then inactive (Bob)
- Note: If ORDER BY with function calls is not supported, mark as SKIP with divergent sister test.
- Status: [ ] Write test → [ ] Assess difficulty → [ ] GREEN or SKIP

**T28: `Iif_WithParameterizedQuery_UsesParameterValue`**
```sql
SELECT IIF(c.value > @threshold, 'high', 'low') AS level FROM c ORDER BY c.id
```
With `@threshold = 15`.
- Expected: 'low' (id=1), 'high' (id=2), 'low' (id=3)
- Status: [ ] Write test → [ ] Confirm GREEN

**T29: `Iif_InValueSelect_ReturnsScalar`**
```sql
SELECT VALUE IIF(c.isActive, 'yes', 'no') FROM c WHERE c.id = '1'
```
- Expected: scalar `'yes'` (not wrapped in object)
- Status: [ ] Write test → [ ] Confirm GREEN

---

#### Group 7: Edge Cases (1 test)

**T30: `Iif_FunctionNameCaseInsensitive_Works`**
```sql
SELECT iif(c.isActive, 'yes', 'no') AS r1, Iif(c.isActive, 'yes', 'no') AS r2 FROM c WHERE c.id = '1'
```
- Expected: r1='yes', r2='yes'
- Note: Parser calls `name.ToUpperInvariant()` so this should work. Verify.
- Status: [ ] Write test → [ ] Confirm GREEN

---

### Phase 2: Fix Implementation (GREEN)

**Step 1: Fix IIF condition evaluation**
- File: `src/CosmosDB.InMemoryEmulator/InMemoryContainer.cs`
- Line: ~4276
- Change:
  ```csharp
  // BEFORE:
  return IsTruthy(args[0]) ? args[1] : args[2];

  // AFTER:
  return args[0] is true ? args[1] : args[2];
  ```
- Rationale: Real Cosmos DB only returns the true branch when condition is exactly boolean `true`.
  All other types (numbers, strings, arrays, objects, null, undefined) return the false branch.
- Risk: LOW — IsTruthy is still used for AND/OR/NOT/WHERE which are unaffected.
- Breaking change: Only for users who relied on JavaScript-like truthiness in IIF conditions.
  This is a bug fix, not a behavior change — the previous behavior was incorrect.

**Step 2: Run all existing tests to confirm no regressions**
```
dotnet test tests/CosmosDB.InMemoryEmulator.Tests --verbosity minimal
```

**Step 3: Run new IIF tests to confirm all GREEN**

---

### Phase 3: Handle Skip Cases

Any test that proves too difficult to implement correctly should be:
1. Marked with `[Fact(Skip = "...")]` with a detailed skip reason
2. Given a sister test that documents the actual divergent behavior

Based on analysis, likely skip candidates:
- **T27 (IIF in ORDER BY)** — May require parser changes if ORDER BY doesn't support function call expressions. If this fails, skip with reason and add a sister test showing that IIF works in SELECT but not ORDER BY.

---

### Phase 4: Update Documentation

#### 4a. Wiki Known-Limitations.md
- No new limitation entry needed (this is a bug FIX, not a new limitation)
- If T27 (ORDER BY) needs to be skipped, add a new behavioral difference entry

#### 4b. Wiki Features.md
- No changes needed (IIF is already covered under "Conditional functions")

#### 4c. Wiki Feature-Comparison-With-Alternatives.md
- No changes needed (already shows ✅ for "Conditional functions (IIF, COALESCE)")

#### 4d. README.md
- No changes needed

#### 4e. Wiki SQL-Queries.md
- Review if IIF has an entry. If not, consider adding one with examples.

#### 4f. Test count update
- Update "1350+ tests" in README.md if test count crosses a new milestone

---

### Phase 5: Version Bump, Tag, Push

1. Update version in `src/CosmosDB.InMemoryEmulator/CosmosDB.InMemoryEmulator.csproj`:
   `2.0.4` → `2.0.5`
2. Git add, commit:
   ```
   git add -A
   git commit -m "v2.0.5: Fix IIF non-boolean condition handling, add 29 IIF tests"
   ```
3. Tag: `git tag v2.0.5`
4. Push: `git push && git push --tags`
5. Wiki commit (if any wiki changes were made):
   ```
   cd c:\git\CosmosDB.InMemoryEmulator.wiki
   git add -A
   git commit -m "v2.0.5: IIF bug fix documentation"
   git push
   ```

---

## Execution Order (TDD Red-Green-Refactor)

```
1. Write ALL 29 new tests in IifFunctionTests.cs
2. Run tests → confirm ~8 RED (Group 1 bug tests), ~21 GREEN (other groups)
3. Apply one-line fix in InMemoryContainer.cs (IsTruthy → is true)
4. Run tests → confirm ALL GREEN
5. Run full test suite → confirm no regressions
6. Handle any skip cases (if T27 ORDER BY fails)
7. Update documentation (wiki, README test count if needed)
8. Version bump, tag, push
```

---

## Risk Assessment

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Fix breaks AND/OR/NOT evaluation | Very Low | IsTruthy is unchanged; only IIF case is modified |
| Fix breaks existing 6 IIF tests | None | All use boolean conditions, verified by analysis |
| ORDER BY with IIF not supported | Medium | Parser may not support function calls in ORDER BY; skip if needed |
| Undiscovered IIF edge cases | Low | 29 new tests cover comprehensive matrix |
| Wiki merge conflicts | Low | Push immediately after code changes |
