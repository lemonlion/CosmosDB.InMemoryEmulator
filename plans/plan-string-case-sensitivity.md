# StringCaseSensitivityTests.cs — Deep Dive Plan

**Version:** 2.0.4 → **2.0.5** (patch bump)
**File:** `tests/CosmosDB.InMemoryEmulator.Tests/StringCaseSensitivityTests.cs`
**Source:** `src/CosmosDB.InMemoryEmulator/InMemoryContainer.cs`, `CosmosSqlParser.cs`

---

## Current Coverage (8 tests)

| # | Section | Test | Status |
|---|---------|------|--------|
| 1 | C1 | `Query_WhereEquals_IsCaseSensitive` | ✅ |
| 2 | C1 | `Query_WhereNotEquals_IsCaseSensitive` | ✅ |
| 3 | C1 | `Query_WhereIn_IsCaseSensitive` | ✅ |
| 4 | C2 | `Query_OrderBy_String_IsCaseSensitive` | ✅ |
| 5 | C2 | `Query_WhereGreaterThan_String_IsCaseSensitive` | ✅ |
| 6 | C3 | `Query_Like_IsCaseSensitive` | ✅ |
| 7 | C3 | `Query_Like_LowercasePattern_IsCaseSensitive` | ✅ |
| 8 | C4 | `Query_Like_WithSquareBrackets_TreatedAsLiteral` | ✅ |
| 9 | C4 | `Query_Like_WithDot_TreatedAsLiteral` | ✅ |

---

## Analysis of Missing Coverage

### A. Core Comparison Operators Not Tested

The file tests `=`, `!=`, `>`, `IN`, and `ORDER BY ASC` but is missing:

- **`<` (less than):** Should use ordinal comparison. "BOB" < "alice" because 'B' (66) < 'a' (97).
- **`<=` (less than or equal):** Same ordinal boundary — "Alice" <= "Alice" true, "alice" <= "Alice" false.
- **`>=` (greater than or equal):** "alice" >= "BOB" should be true.
- **`NOT IN`:** Parser wraps `IN` in `UnaryExpression(Not, InExpression)`. Needs case-sensitive verification.
- **`BETWEEN` with strings:** Uses `CompareValues` which is ordinal — `BETWEEN 'A' AND 'Z'` should include only uppercase-starting strings.
- **`ORDER BY DESC`:** Reverse ordinal — "bob", "alice", "BOB", "Alice".

### B. LIKE Gaps

Only prefix matching (`A%`, `b%`) and metacharacter literals (`[1]`, `.`) are tested. Missing:

- **`_` (single-char wildcard) case sensitivity:** `LIKE '_lice'` should match "Alice" but not "alice" if we write `LIKE 'A____'` or similar.
- **`NOT LIKE` case sensitivity:** `NOT LIKE 'A%'` should exclude only "Alice", not "alice".
- **`LIKE` with `ESCAPE` clause:** `LIKE '10\%' ESCAPE '\'` should match literal "10%", case-sensitively.
- **`LIKE` exact match (no wildcards):** `LIKE 'Alice'` should match "Alice" only, not "alice".
- **`LIKE` with `%` in middle:** `LIKE 'A%e'` should match "Alice" but not "alice".
- **`LIKE` with multiple `%` wildcards:** `LIKE '%li%'` should match "Alice" and "alice" (both contain "li") but not "BOB".
- **Other regex metacharacters as literals:** `+`, `*`, `^`, `$`, `(`, `)`, `{`, `}`, `|`, `\`, `?` — all should be treated as literal characters, not regex operators.
- **`LIKE` with empty pattern:** `LIKE ''` should match only empty strings.

### C. String Functions with Case-Sensitivity Semantics

These functions have explicit case-sensitivity behavior that belongs in this test suite:

- **`CONTAINS(str, sub)` default (2 args):** Case-sensitive. `CONTAINS(c.name, 'ali')` should not match "Alice".
- **`CONTAINS(str, sub, true)` (3 args):** Case-insensitive. `CONTAINS(c.name, 'ali', true)` should match both "Alice" and "alice".
- **`CONTAINS(str, sub, false)` (3 args, explicit false):** Should behave as case-sensitive default.
- **`STARTSWITH(str, prefix)` default:** Case-sensitive. `STARTSWITH(c.name, 'a')` should not match "Alice".
- **`STARTSWITH(str, prefix, true)` (3 args):** Case-insensitive. Should match regardless of case.
- **`ENDSWITH(str, suffix)` default:** Case-sensitive. `ENDSWITH(c.name, 'CE')` should not match "Alice".
- **`ENDSWITH(str, suffix, true)` (3 args):** Case-insensitive.
- **`STRING_EQUALS(s1, s2)` default:** Case-sensitive. `STRING_EQUALS(c.name, 'alice')` should not match "Alice".
- **`STRING_EQUALS(s1, s2, true)` (3 args):** Case-insensitive.
- **`INDEX_OF(str, sub)` :** Always case-sensitive (no 3rd arg option). `INDEX_OF(c.name, 'A')` should return 0 for "Alice", -1 for "alice".
- **`REPLACE(str, find, rep)`:** Always case-sensitive. `REPLACE(c.name, 'a', 'X')` should change "alice" to "Xlice" but leave "Alice" as "Alice".
- **`REGEXMATCH(str, pattern)` default:** Case-sensitive. `REGEXMATCH(c.name, '^a')` matches "alice" not "Alice".
- **`REGEXMATCH(str, pattern, 'i')` (3 args):** Case-insensitive via 'i' modifier.

### D. Case Transformations Composing with Comparisons

- **`LOWER()` + equality:** `WHERE LOWER(c.name) = 'alice'` should match both "Alice" and "alice".
- **`UPPER()` + equality:** `WHERE UPPER(c.name) = 'BOB'` should match both "BOB" and "bob".

### E. Query Clauses with String Case-Sensitivity

- **`DISTINCT`:** `SELECT DISTINCT c.name` where data has "Alice" and "alice" — should return both as separate values. (Implementation uses `JToken.Distinct()` which compares by value.)
- **`GROUP BY`:** `SELECT c.name, COUNT(1) FROM c GROUP BY c.name` — "Alice" and "alice" should be separate groups.
- **`MIN()` / `MAX()` over strings:** `SELECT MIN(c.name)` should return "Alice" (ordinal: 'A' < 'a'). `SELECT MAX(c.name)` should return "bob".

### F. Edge Cases

- **Empty string equality:** `c.name = ''` — should be case-sensitive (trivially, but documents behaviour).
- **Null vs string comparison:** `c.name > null` — documents null ordering.
- **Unicode case sensitivity:** Non-ASCII characters like `é` vs `É`, `ü` vs `Ü`. Ordinal means code-point comparison (É=201, é=233, so É < é). This is likely hard to get right vs real Cosmos DB — may need to be a divergent behavior test.
- **Property name case sensitivity:** JSON property names lookup via `JObject.SelectToken()` is case-sensitive by default in Newtonsoft.Json. `WHERE c.Name = 'Alice'` should NOT match if the JSON has `"name": "Alice"`.
- **Parameterized query with case-sensitive string:** `WHERE c.name = @name` with `@name = "Alice"` should be case-sensitive.

---

## Potential Bugs Found

### Bug 1: NOT LIKE Without ESCAPE Uses BinaryExpression Instead of LikeExpression

**Location:** `CosmosSqlParser.cs` line ~654

```csharp
// NOT LIKE pattern (without ESCAPE) — line 654
select (Func<SqlExpression, SqlExpression>)(l =>
    new UnaryExpression(UnaryOp.Not,
        new BinaryExpression(l, BinaryOp.Like, pattern)))  // ← Uses BinaryExpression
```

Compare with NOT LIKE *with* ESCAPE (line 647):
```csharp
select (Func<SqlExpression, SqlExpression>)(l =>
    new UnaryExpression(UnaryOp.Not,
        new LikeExpression(l, pattern, escChar...)))  // ← Uses LikeExpression
```

This inconsistency means `NOT LIKE` without ESCAPE goes through `BinaryExpression` evaluation path (which calls `EvaluateLike` without an escape char), while `LIKE` without ESCAPE *also* goes through the same `BinaryExpression` path. The behaviour is actually correct — both call `EvaluateLike(left, right)` with no escape char. So this is **not a bug**, just an asymmetry in AST representation. The behaviour is identical.

**Verdict:** NOT A BUG — but the asymmetry deserves a test to lock down the behaviour.

### Bug 2 (Investigate): DISTINCT on JToken Equality

**Location:** `InMemoryContainer.cs` line ~2335

```csharp
items = items.Distinct().ToList();
```

This uses `JToken.Equals()` for deduplication. Need to verify that `JToken.Equals()` for string values is case-sensitive. Newtonsoft's `JToken.Equals` does ordinal string comparison for `JValue<string>`, so this should be correct. But a test should lock it down.

**Verdict:** LIKELY CORRECT — needs test confirmation.

### Bug 3 (Investigate): GROUP BY Key Comparison

The GROUP BY implementation groups by serialized key representation. Need to verify that "Alice" and "alice" produce different group keys.

**Verdict:** LIKELY CORRECT — needs test confirmation.

---

## Test Plan (New Tests to Write)

TDD approach: write test → RED → implement/verify → GREEN → refactor.
Tests marked 🟡 may be skipped with divergent behavior sister test if implementation is too difficult.

### Section C5: Remaining Comparison Operators (5 tests)

| # | Test Name | Query | Expected | Priority |
|---|-----------|-------|----------|----------|
| T01 | `Query_WhereLessThan_String_IsCaseSensitive` | `WHERE c.name < 'a'` | "Alice", "BOB" (uppercase < lowercase in ordinal) | HIGH |
| T02 | `Query_WhereLessThanOrEqual_String_IsCaseSensitive` | `WHERE c.name <= 'BOB'` | "Alice", "BOB" | HIGH |
| T03 | `Query_WhereGreaterThanOrEqual_String_IsCaseSensitive` | `WHERE c.name >= 'alice'` | "alice", "bob" | HIGH |
| T04 | `Query_NotIn_IsCaseSensitive` | `WHERE c.name NOT IN ('Alice', 'BOB')` | "alice", "bob" | HIGH |
| T05 | `Query_Between_String_IsCaseSensitive` | `WHERE c.name BETWEEN 'A' AND 'Z'` | "Alice", "BOB" (only uppercase-starting) | HIGH |

### Section C6: ORDER BY Completeness (1 test)

| # | Test Name | Query | Expected | Priority |
|---|-----------|-------|----------|----------|
| T06 | `Query_OrderByDesc_String_IsCaseSensitive` | `ORDER BY c.name DESC` | "bob", "alice", "BOB", "Alice" | MEDIUM |

### Section C7: LIKE Extended Coverage (8 tests)

| # | Test Name | Query | Expected | Priority |
|---|-----------|-------|----------|----------|
| T07 | `Query_Like_ExactMatch_IsCaseSensitive` | `LIKE 'Alice'` | Only "Alice", not "alice" | HIGH |
| T08 | `Query_Like_UnderscoreWildcard_IsCaseSensitive` | `LIKE '_lice'` | "Alice" and "alice" (both have 1 char then "lice") | HIGH |
| T09 | `Query_NotLike_IsCaseSensitive` | `NOT LIKE 'A%'` | "alice", "BOB", "bob" (NOT "Alice") | HIGH |
| T10 | `Query_Like_MiddlePercent_IsCaseSensitive` | `LIKE 'A%e'` | "Alice" only | MEDIUM |
| T11 | `Query_Like_SubstringPercent_IsCaseSensitive` | `LIKE '%li%'` | "Alice", "alice" (both contain "li"), not "BOB"/"bob" | MEDIUM |
| T12 | `Query_Like_WithEscape_IsCaseSensitive` | `LIKE '10\%' ESCAPE '\'` on data with "10%" and "10x" | Only "10%" | MEDIUM |
| T13 | `Query_Like_EmptyPattern_MatchesOnlyEmpty` | `LIKE ''` | Only items with empty name | LOW |
| T14 | `Query_Like_RegexMetachars_AllTreatedAsLiteral` | Data with `+*^$(){}|\?` chars, `LIKE` patterns for each | Literal matching only | MEDIUM |

### Section C8: String Functions Case Sensitivity (14 tests)

| # | Test Name | Query | Expected | Priority |
|---|-----------|-------|----------|----------|
| T15 | `Query_Contains_Default_IsCaseSensitive` | `CONTAINS(c.name, 'ali')` | Only "alice" (not "Alice" — "Ali" has uppercase A) | HIGH |
| T16 | `Query_Contains_ThirdArgTrue_IsCaseInsensitive` | `CONTAINS(c.name, 'ALI', true)` | "Alice" and "alice" | HIGH |
| T17 | `Query_Contains_ThirdArgFalse_IsCaseSensitive` | `CONTAINS(c.name, 'ali', false)` | Only "alice" | MEDIUM |
| T18 | `Query_StartsWith_Default_IsCaseSensitive` | `STARTSWITH(c.name, 'a')` | Only "alice" | HIGH |
| T19 | `Query_StartsWith_ThirdArgTrue_IsCaseInsensitive` | `STARTSWITH(c.name, 'a', true)` | "Alice" and "alice" | HIGH |
| T20 | `Query_EndsWith_Default_IsCaseSensitive` | `ENDSWITH(c.name, 'CE')` | None (no name ends with uppercase "CE") | HIGH |
| T21 | `Query_EndsWith_ThirdArgTrue_IsCaseInsensitive` | `ENDSWITH(c.name, 'CE', true)` | "Alice" and "alice" (end with "ce"/"ce") | HIGH |
| T22 | `Query_StringEquals_Default_IsCaseSensitive` | `STRING_EQUALS(c.name, 'alice')` | Only "alice" (id=2) | HIGH |
| T23 | `Query_StringEquals_ThirdArgTrue_IsCaseInsensitive` | `STRING_EQUALS(c.name, 'alice', true)` | "Alice" and "alice" | HIGH |
| T24 | `Query_IndexOf_IsCaseSensitive` | `INDEX_OF(c.name, 'a')` | 0 for "alice", -1 for "Alice" (has 'A'), 2 for "bob"→-1, etc. | MEDIUM |
| T25 | `Query_Replace_IsCaseSensitive` | `REPLACE(c.name, 'a', 'X')` | "Alice"→"Alice", "alice"→"Xlice" | MEDIUM |
| T26 | `Query_RegexMatch_Default_IsCaseSensitive` | `REGEXMATCH(c.name, '^a')` | "alice" only | HIGH |
| T27 | `Query_RegexMatch_WithIgnoreCaseModifier` | `REGEXMATCH(c.name, '^a', 'i')` | "Alice" and "alice" | HIGH |
| T28 | `Query_Contains_ThirdArgFalse_SameAsDefault` | `CONTAINS(c.name, 'ali', false)` returns same as 2-arg | Confirms false == default | LOW |

### Section C9: Case Transformations in Comparisons (2 tests)

| # | Test Name | Query | Expected | Priority |
|---|-----------|-------|----------|----------|
| T29 | `Query_Lower_EnablesCaseInsensitiveEquality` | `WHERE LOWER(c.name) = 'alice'` | "Alice" and "alice" | MEDIUM |
| T30 | `Query_Upper_EnablesCaseInsensitiveEquality` | `WHERE UPPER(c.name) = 'BOB'` | "BOB" and "bob" | MEDIUM |

### Section C10: DISTINCT and GROUP BY (3 tests)

| # | Test Name | Query | Expected | Priority |
|---|-----------|-------|----------|----------|
| T31 | `Query_Distinct_PreservesCaseDifferences` | `SELECT DISTINCT c.name` | 4 separate values | HIGH |
| T32 | `Query_GroupBy_TreatsCaseDifferencesAsSeparateGroups` | `GROUP BY c.name` + COUNT | 4 groups, 1 each | HIGH |
| T33 | `Query_MinMax_String_UsesOrdinalComparison` | `MIN(c.name)` / `MAX(c.name)` | min="Alice", max="bob" | MEDIUM |

### Section C11: Edge Cases (5 tests)

| # | Test Name | Query | Expected | Priority |
|---|-----------|-------|----------|----------|
| T34 | `Query_EmptyStringEquality_IsCaseSensitive` | Seed `""`, query `= ''` | Matches empty | LOW |
| T35 | `Query_ParameterizedQuery_StringIsCaseSensitive` | `WHERE c.name = @n` with `@n = "Alice"` | Only "Alice" | MEDIUM |
| T36 | `Query_PropertyNameLookup_IsCaseSensitive` | `WHERE c.Name = 'Alice'` vs `c.name` | `c.Name` returns nothing (property is "name") | MEDIUM |
| T37 | 🟡 `Query_UnicodeCase_IsOrdinal` | Data with "Über"/"über", query `= 'Über'` | Ordinal: matches exact only | LOW |
| T38 | `Query_NullStringComparison_ReturnsExpected` | `WHERE c.name > null` + data with null name | Documents null-ordering behaviour | LOW |

---

## Summary

| Category | New Tests | Coverage Gap |
|----------|-----------|-------------|
| C5: Remaining comparisons (`<`, `<=`, `>=`, `NOT IN`, `BETWEEN`) | 5 | HIGH |
| C6: ORDER BY DESC | 1 | MEDIUM |
| C7: LIKE extended | 8 | HIGH |
| C8: String functions | 14 | HIGH |
| C9: Case transforms | 2 | MEDIUM |
| C10: DISTINCT / GROUP BY / MIN / MAX | 3 | HIGH |
| C11: Edge cases | 5 | MEDIUM |
| **TOTAL** | **38** | |

---

## Implementation Order (TDD Red-Green-Refactor)

1. **Phase 1 — Core comparisons (T01–T06):** Write all 6 tests. Expect GREEN (implementation exists using `CompareValues`/`ValuesEqual` with ordinal). Run. If any RED, fix.
2. **Phase 2 — LIKE (T07–T14):** Write 8 tests. Expect GREEN for most; T09 (NOT LIKE) and T12 (ESCAPE) need verification. If RED, diagnose and fix.
3. **Phase 3 — String functions (T15–T28):** Write 14 tests. Expect GREEN (implementations already use Ordinal/OrdinalIgnoreCase). Some may overlap with SqlFunctionTests.cs but these are focused on the case-sensitivity contract.
4. **Phase 4 — LOWER/UPPER (T29–T30):** Write 2 tests. Expect GREEN.
5. **Phase 5 — DISTINCT/GROUP BY/aggregates (T31–T33):** Write 3 tests. These verify JToken equality and group key behavior. Expect GREEN but need confirmation.
6. **Phase 6 — Edge cases (T34–T38):** Write 5 tests. T37 (Unicode) may need to be 🟡 skipped if real Cosmos DB behavior differs from .NET ordinal for non-ASCII. T38 (null) documents behavior.

---

## Skip/Divergent Behavior Protocol

For any test where real Cosmos DB behavior is too difficult to emulate:
1. Mark the test as `[Fact(Skip = "...detailed reason...")]`
2. Create a sister test named `*_DivergentBehavior` that:
   - Passes GREEN
   - Has inline comments explaining the real Cosmos DB behavior
   - Has inline comments explaining the emulator's behavior
   - Has inline comments explaining why the divergence exists

---

## Documentation Updates Required

After implementation:

1. **Wiki: Known-Limitations.md** — Add any new divergent behaviors discovered (e.g., Unicode case sensitivity if applicable).
2. **Wiki: Feature-Comparison-With-Alternatives.md** — No changes expected (string case sensitivity is the same across all emulators).
3. **Wiki: Features.md** — No changes expected (string functions already documented).
4. **Wiki: SQL-Queries.md** — Verify case-sensitivity note is present for `LIKE`, `CONTAINS`, `STARTSWITH`, `ENDSWITH`, `STRING_EQUALS`, `REGEXMATCH`. Add if missing.
5. **README.md** — No changes expected (high-level; case sensitivity is an implementation detail).
6. **Version bump:** `CosmosDB.InMemoryEmulator.csproj` Version `2.0.4` → `2.0.5`.

---

## Post-Implementation Checklist

- [ ] All 38 new tests written
- [ ] All tests GREEN (or 🟡 skipped with sister tests)
- [ ] No existing tests broken
- [ ] Documentation updated (wiki, README if needed)
- [ ] Version bumped to 2.0.5
- [ ] Git tag `v2.0.5` created
- [ ] `git push` + `git push --tags`

---

## Progress Tracker

| Phase | Status | Notes |
|-------|--------|-------|
| Phase 1: Comparisons | ⬜ Not started | |
| Phase 2: LIKE | ⬜ Not started | |
| Phase 3: String functions | ⬜ Not started | |
| Phase 4: LOWER/UPPER | ⬜ Not started | |
| Phase 5: DISTINCT/GROUP BY | ⬜ Not started | |
| Phase 6: Edge cases | ⬜ Not started | |
| Documentation | ⬜ Not started | |
| Version/tag/push | ⬜ Not started | |
