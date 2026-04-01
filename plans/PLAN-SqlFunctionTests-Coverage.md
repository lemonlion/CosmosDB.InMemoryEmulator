# SqlFunctionTests.cs — Deep Dive Coverage & Bug Fix Plan

**Date**: 2026-04-01
**Current Version**: 2.0.4 → **Target Version**: 2.0.5
**Approach**: TDD — Red-Green-Refactor. Test first, then implement. Hard cases get `Skip` + divergent-behaviour sister test.

---

## Table of Contents

1. [Summary of Current Coverage](#1-summary-of-current-coverage)
2. [Identified Bugs in Implementation](#2-identified-bugs-in-implementation)
3. [Missing Test Coverage — Functions Implemented but Untested in SqlFunctionTests](#3-missing-test-coverage--functions-implemented-but-untested-in-sqlfunctiontests)
4. [Missing Test Coverage — Edge Cases for Existing Functions](#4-missing-test-coverage--edge-cases-for-existing-functions)
5. [Test Execution Plan](#5-test-execution-plan)
6. [Documentation Updates](#6-documentation-updates)
7. [Version Bump & Release](#7-version-bump--release)
8. [Status Tracker](#8-status-tracker)

---

## 1. Summary of Current Coverage

SqlFunctionTests.cs currently has **4 test classes** with a total of **~108 tests** covering:

### Main SqlFunctionTests class (~89 tests)
- **String**: STARTSWITH, ENDSWITH, CONTAINS, CONCAT, LOWER, UPPER, TRIM, LTRIM, RTRIM, LEFT, RIGHT, LENGTH, SUBSTRING, INDEX_OF, REPLACE, REVERSE, REPLICATE, STRING_EQUALS, StringToArray, StringToBoolean, StringToNumber, StringToObject, ToString, REGEXMATCH
- **Math**: ABS, FLOOR, CEILING, ROUND, SQRT, SQUARE, POWER, EXP, LOG, LOG10, SIGN, TRUNC, PI, SIN, COS, TAN, ASIN, ACOS, ATAN, ATN2, DEGREES, RADIANS, RAND, NumberBin
- **Integer**: IntAdd, IntSub, IntMul, IntDiv (incl. div-by-zero), IntMod, IntBitAnd, IntBitOr, IntBitXor, IntBitNot, IntBitLeftShift, IntBitRightShift
- **Type checking**: IS_ARRAY, IS_BOOL, IS_NUMBER, IS_STRING, IS_OBJECT, IS_PRIMITIVE, IS_DEFINED, IS_NULL, IS_FINITE_NUMBER, IS_INTEGER, ToNumber, ToBoolean
- **Array**: ARRAY_CONTAINS, ARRAY_LENGTH, ARRAY_SLICE, ARRAY_CONCAT
- **Aggregates**: SUM, AVG, MIN, MAX, GROUP BY, HAVING
- **Operators**: IN, NOT IN, BETWEEN
- **Spatial**: ST_DISTANCE, ST_WITHIN, ST_INTERSECTS, ST_ISVALID, ST_ISVALIDDETAILED (15 tests)
- **UDF**: Register, Where clause, unregistered throws, multi-arg (4 tests)

### Gap test classes (~19 tests)
- SqlFunctionGapTests: CONTAINS case, STARTSWITH case, ARRAY_CONTAINS partial, IS_DEFINED, INDEX_OF not-found, SUBSTRING basic, REPLACE multi
- SqlFunctionGapTests2: GetCurrentDateTime, IS_INTEGER float distinction, null args to string funcs
- SqlFunctionGapTests3: GetCurrentTimestamp, SUBSTRING out-of-bounds, Math with null
- SqlFunctionGapTests4: COALESCE, IS_PRIMITIVE for obj/array, type functions on undefined

---

## 2. Identified Bugs in Implementation

### Bug 1: `LOG()` does not support custom base (2-arg form)
- **Location**: `InMemoryContainer.cs:4169`
- **Current**: `case "LOG": return args.Length > 0 ? MathOp(args[0], Math.Log) : null;`
- **Cosmos DB**: `LOG(value [, base])` — second arg is optional base. `LOG(8, 2)` should return `3`.
- **Fix**: Check `args.Length >= 2` and call `Math.Log(value, base)`.
- **Difficulty**: Easy

### Bug 2: `ROUND()` does not support precision parameter (2-arg form)  
- **Location**: `InMemoryContainer.cs:4164`
- **Current**: `case "ROUND": return args.Length > 0 ? MathOp(args[0], Math.Round) : null;`
- **Cosmos DB**: `ROUND(value [, precision])` — `ROUND(3.14159, 2)` should return `3.14`.
- **Fix**: Check `args.Length >= 2` and use `Math.Round(value, precision)`.
- **Difficulty**: Easy

### Bug 3: `INDEX_OF()` does not support optional start position (3-arg form)
- **Location**: `InMemoryContainer.cs:3826-3835`
- **Current**: Only takes 2 args (`string`, `substring`).
- **Cosmos DB**: `INDEX_OF(string, substring [, start_position])` — third arg is search start.
- **Fix**: Check `args.Length >= 3` and pass start position to `String.IndexOf`.
- **Difficulty**: Easy

### Bug 4: `CONCAT()` with null args — should return undefined per Cosmos DB
- **Location**: `InMemoryContainer.cs:3771`
- **Current**: `string.Concat(args.Select(a => a?.ToString() ?? ""))` — treats null as empty string.
- **Cosmos DB**: If any arg is undefined/not a string, `CONCAT` returns `undefined`.
- **Investigation needed**: Cosmos DB treats null differently from undefined. Null args → "null" string? Or undefined? Need to validate exact behavior. Mark as needs-investigation.
- **Difficulty**: Medium (behavior nuances between null/undefined)

### Bug 5: `LENGTH()` on non-string input should return undefined
- **Location**: `InMemoryContainer.cs:3772`
- **Current**: `args[0]?.ToString()?.Length` — calls ToString() on any type, so `LENGTH(123)` returns 3.
- **Cosmos DB**: `LENGTH` only operates on strings. Non-string → undefined.
- **Difficulty**: Easy but potentially breaking

### Bug 6: `REPLACE()` should be case-sensitive only (matches Cosmos DB)
- **Current implementation uses `String.Replace` which is ordinal** — this is CORRECT.
- **Status**: NOT A BUG. Verified correct.

### Bug 7: `REPLICATE()` negative count should return undefined, not error
- **Location**: `InMemoryContainer.cs:3867-3882`
- **Current**: Needs verification — does it handle negative count?
- **Cosmos DB**: `REPLICATE("abc", -1)` → undefined.
- **Difficulty**: Easy

### Bug 8: `StringToNumber` — `"NaN"`, `"Infinity"`, `"-Infinity"` should return undefined
- **Location**: `InMemoryContainer.cs:3948-3972`
- **Current**: `double.TryParse` succeeds for these values, returning them as doubles.
- **Cosmos DB**: These should return `undefined`.
- **Difficulty**: Easy

---

## 3. Missing Test Coverage — Functions Implemented but Untested in SqlFunctionTests

These functions exist in `EvaluateSqlFunction()` but have NO tests in SqlFunctionTests.cs:

### 3.1 `COT` — Cotangent
- **Impl**: `InMemoryContainer.cs:4177` — `1.0 / Math.Tan(v)`
- **Tests needed**:
  - [ ] `Cot_ReturnsCorrectValue` — `COT(1)` ≈ `1/tan(1)` ≈ `0.6421`
  - [ ] `Cot_Zero_ReturnsInfinity` — `COT(0)` → division by zero of tan(0)=0, returns Infinity (divergent?)

### 3.2 `CHOOSE` — Select by 1-based index
- **Impl**: `InMemoryContainer.cs:4381-4387`
- **Tests needed**:
  - [ ] `Choose_ReturnsCorrectElement` — `CHOOSE(2, 'a', 'b', 'c')` → `'b'`
  - [ ] `Choose_OutOfRange_ReturnsNull` — `CHOOSE(5, 'a', 'b')` → null
  - [ ] `Choose_ZeroIndex_ReturnsNull` — 1-based, so 0 is invalid

### 3.3 `OBJECTTOARRAY` — Convert object to `[{k,v}]` pairs
- **Impl**: `InMemoryContainer.cs:4388-4401`
- **Tests needed**:
  - [ ] `ObjectToArray_ReturnsKeyValuePairs`
  - [ ] `ObjectToArray_OnNonObject_ReturnsNull`

### 3.4 `ARRAYTOOBJECT` — Convert `[{k,v}]` pairs to object
- **Impl**: `InMemoryContainer.cs:4402-4424`
- **Tests needed**:
  - [ ] `ArrayToObject_ReturnsObject`
  - [ ] `ArrayToObject_InvalidShape_ReturnsUndefined`

### 3.5 `STRINGJOIN` — Join array elements with separator
- **Impl**: `InMemoryContainer.cs:4425-4435`
- **Tests needed**:
  - [ ] `StringJoin_JoinsArrayWithSeparator`
  - [ ] `StringJoin_EmptyArray_ReturnsEmptyString`

### 3.6 `STRINGSPLIT` — Split string by delimiter
- **Impl**: `InMemoryContainer.cs:4436-4446`
- **Tests needed**:
  - [ ] `StringSplit_SplitsStringByDelimiter`
  - [ ] `StringSplit_NoMatchDelimiter_ReturnsSingleElement`

### 3.7 `STRINGTONULL` — Parse "null" string to null
- **Impl**: `InMemoryContainer.cs:3939-3947`
- **Tests needed**:
  - [ ] `StringToNull_ParsesNullLiteral`
  - [ ] `StringToNull_InvalidInput_ReturnsUndefined`

### 3.8 `DOCUMENTID` — Return document's _rid or id
- **Impl**: `InMemoryContainer.cs:4447-4455`
- **Tests needed**:
  - [ ] `DocumentId_ReturnsDocumentId`

### 3.9 `DATETIMEDIFF` — Difference between two datetimes
- **Impl**: `InMemoryContainer.cs:4611-4632`
- **Tests needed**:
  - [ ] `DateTimeDiff_ReturnsDayDifference`
  - [ ] `DateTimeDiff_ReturnsHourDifference`
  - [ ] NOTE: Covered in DateHandlingTests.cs — just ensure SqlFunctionTests has basic smoke test

### 3.10 `DATETIMEFROMPARTS` — Build datetime from parts
- **Impl**: `InMemoryContainer.cs:4633-4647`
- **Tests needed**:
  - [ ] `DateTimeFromParts_BuildsCorrectDateTime`
  - [ ] NOTE: May be covered in DateHandlingTests.cs — check and add smoke test if not

### 3.11 `COUNT` aggregate — Missing from SqlFunctionTests main class
- Only tested indirectly via GROUP BY
- **Tests needed**:
  - [ ] `CountAggregate_ReturnsCorrectCount`

### 3.12 `ENDSWITH` case-insensitive (3-arg form)
- Implemented but not tested in main SqlFunctionTests
- **Tests needed**:
  - [ ] `EndsWith_CaseInsensitive`

### 3.13 `ST_AREA` — Spatial area calculation
- **Impl**: `InMemoryContainer.cs:4533` — exists in dispatch
- **Tests needed**:
  - [ ] `StArea_ReturnsAreaOfPolygon` (or skip if not meaningful in emulator)

---

## 4. Missing Test Coverage — Edge Cases for Existing Functions

### 4.1 String Function Edge Cases

| # | Test Name | Description | Priority |
|---|-----------|-------------|----------|
| 1 | `Concat_WithNullArg_BehavesCorrectly` | Cosmos DB behavior for null inside CONCAT | High |
| 2 | `Concat_NoArgs_ReturnsEmpty` | `CONCAT()` with zero args | Medium |
| 3 | `Contains_EmptySubstring_ReturnsTrue` | `CONTAINS("hello", "")` → true | Medium |
| 4 | `StartsWith_EmptyPrefix_ReturnsTrue` | `STARTSWITH("hello", "")` → true | Medium |
| 5 | `EndsWith_EmptyPrefix_ReturnsTrue` | `ENDSWITH("hello", "")` → true | Medium |
| 6 | `Length_OnNull_ReturnsNull` | `LENGTH(null)` → undefined | High |
| 7 | `Length_OnNonString_ReturnsUndefined` | `LENGTH(123)` should be undefined in Cosmos DB | High |
| 8 | `Left_NegativeCount_ReturnsEmpty` | `LEFT("hello", -1)` behavior | Medium |
| 9 | `Right_NegativeCount_ReturnsEmpty` | `RIGHT("hello", -1)` behavior | Medium |
| 10 | `Left_CountExceedsLength_ReturnsFullString` | `LEFT("hi", 100)` → `"hi"` | Medium |
| 11 | `Right_CountExceedsLength_ReturnsFullString` | `RIGHT("hi", 100)` → `"hi"` | Medium |
| 12 | `Substring_NegativeStart_BehavesCorrectly` | Edge case for negative start index | Medium |
| 13 | `IndexOf_WithStartPosition_ReturnsCorrectIndex` | Bug #3 — 3-arg INDEX_OF | High |
| 14 | `IndexOf_EmptySubstring_ReturnsZero` | `INDEX_OF("hello", "")` → 0 | Medium |
| 15 | `Replace_EmptyTarget_BehavesCorrectly` | `REPLACE("abc", "", "x")` | Medium |
| 16 | `Replicate_NegativeCount_ReturnsUndefined` | Bug #7 | High |
| 17 | `Replicate_LargeCount_ClampedAt10000` | Current impl caps at 10000 | Low |
| 18 | `Reverse_EmptyString_ReturnsEmpty` | `REVERSE("")` → `""` | Low |
| 19 | `Reverse_NullArg_ReturnsNull` | `REVERSE(null)` | Medium |
| 20 | `Upper_NullArg_ReturnsNull` | Verify null passthrough | Medium |
| 21 | `Lower_NullArg_ReturnsNull` | Verify null passthrough | Medium |
| 22 | `Trim_NullArg_ReturnsNull` | Verify null passthrough | Medium |
| 23 | `StringToArray_InvalidJson_ReturnsUndefined` | `StringToArray("not json")` → undefined | Medium |
| 24 | `StringToArray_ObjectInput_ReturnsUndefined` | `StringToArray('{"a":1}')` → undefined (not array) | Medium |
| 25 | `StringToBoolean_MixedCase_ReturnsUndefined` | `StringToBoolean("True")` → undefined (case-sensitive) | High |
| 26 | `StringToNumber_NaN_ReturnsUndefined` | Bug #8 — `StringToNumber("NaN")` | High |
| 27 | `StringToNumber_Infinity_ReturnsUndefined` | Bug #8 — `StringToNumber("Infinity")` | High |
| 28 | `StringToObject_ArrayInput_ReturnsUndefined` | `StringToObject('[1,2]')` → undefined (not obj) | Medium |
| 29 | `StringEquals_NullArg_ReturnsNull` | `STRING_EQUALS(null, "x")` → null | Medium |
| 30 | `RegexMatch_NullInput_ReturnsFalse` | Verify null handling | Medium |
| 31 | `RegexMatch_InvalidPattern_HandleGracefully` | Bad regex pattern | Medium |
| 32 | `RegexMatch_MultilineFlag` | `RegexMatch(str, pattern, 'm')` | Low |
| 33 | `ToString_BoolInput_ReturnsCorrectString` | `ToString(true)` → `"True"` or `"true"`? | Medium |
| 34 | `ToString_NullInput_ReturnsNull` | `ToString(null)` → null | Medium |

### 4.2 Math Function Edge Cases

| # | Test Name | Description | Priority |
|---|-----------|-------------|----------|
| 35 | `Log_WithBase_ReturnsCustomBaseLog` | Bug #1 — `LOG(8, 2)` → `3` | High |
| 36 | `Log_NegativeInput_ReturnsNaN` | `LOG(-1)` behavior | Medium |
| 37 | `Log_ZeroInput_ReturnsNegativeInfinity` | `LOG(0)` behavior | Medium |
| 38 | `Round_WithPrecision_RoundsToDecimalPlaces` | Bug #2 — `ROUND(3.14159, 2)` → `3.14` | High |
| 39 | `Round_NegativePrecision_BehavesCorrectly` | `ROUND(12345, -2)` → `12300` | Medium |
| 40 | `Sqrt_NegativeInput_ReturnsNaN` | `SQRT(-1)` behavior | Medium |
| 41 | `Power_ZeroExponent_ReturnsOne` | `POWER(5, 0)` → `1` | Low |
| 42 | `Power_NegativeExponent_ReturnsFraction` | `POWER(2, -1)` → `0.5` | Low |
| 43 | `Abs_NullInput_ReturnsNull` | Already in GapTests3, but explicit | Low |
| 44 | `Sign_Zero_ReturnsZero` | `SIGN(0)` → `0` | Low |
| 45 | `Sign_Positive_ReturnsOne` | `SIGN(42)` → `1` | Low |
| 46 | `Trunc_NegativeDecimal_TruncatesTowardZero` | `TRUNC(-3.7)` → `-3` | Medium |
| 47 | `Exp_Zero_ReturnsOne` | `EXP(0)` → `1` | Low |
| 48 | `NumberBin_NegativeValue_RoundsCorrectly` | `NumberBin(-15, 7)` | Medium |
| 49 | `NumberBin_ZeroBinSize_ReturnsNull` | Edge case: bin size 0 | Medium |

### 4.3 Integer Math Edge Cases

| # | Test Name | Description | Priority |
|---|-----------|-------------|----------|
| 50 | `IntAdd_Overflow_BehavesCorrectly` | Max values | Low |
| 51 | `IntMod_ByZero_ReturnsNull` | Like IntDiv | High |
| 52 | `IntAdd_WithNonInteger_ReturnsNull` | `IntAdd(3.5, 2)` → null | Medium |
| 53 | `IntBitLeftShift_NegativeShift_BehavesCorrectly` | Edge case | Low |

### 4.4 Type Checking Edge Cases

| # | Test Name | Description | Priority |
|---|-----------|-------------|----------|
| 54 | `IsArray_ReturnsFalseForNonArray` | `IS_ARRAY("not array")` → false | Medium |
| 55 | `IsBool_ReturnsFalseForNumber` | `IS_BOOL(1)` → false | Medium |
| 56 | `IsNumber_ReturnsFalseForBooleanTrue` | In Cosmos DB, `true` is not a number | Medium |
| 57 | `IsString_ReturnsFalseForNumber` | `IS_STRING(42)` → false | Medium |
| 58 | `IsObject_ReturnsFalseForArray` | `IS_OBJECT([1,2])` → false | Medium |
| 59 | `IsObject_ReturnsFalseForNull` | `IS_OBJECT(null)` → false | Medium |
| 60 | `IsNull_ReturnsFalseForUndefined` | Undefined vs null distinction | High |
| 61 | `IsDefined_TrueForNullProperty` | `IS_DEFINED(c.nullProp)` → true (exists but null) | High |
| 62 | `IsPrimitive_TrueForNull` | In Cosmos DB, null IS a primitive | High |
| 63 | `IsPrimitive_TrueForNumber` | Already covered but worth explicit test | Low |
| 64 | `IsFiniteNumber_NaN_ReturnsFalse` | Need to test NaN handling | Medium |
| 65 | `IsInteger_ReturnsTrueForNegativeInt` | Sanity check | Low |

### 4.5 Array Function Edge Cases

| # | Test Name | Description | Priority |
|---|-----------|-------------|----------|
| 66 | `ArrayContains_NullElement_MatchesNull` | null element matching | Medium |
| 67 | `ArraySlice_NegativeStart_FromEnd` | `ARRAY_SLICE([1,2,3,4], -2)` → `[3, 4]` | High |
| 68 | `ArraySlice_StartBeyondLength_ReturnsEmpty` | Out of bounds | Medium |
| 69 | `ArraySlice_LengthExceedsRemaining_ReturnsRemainder` | Partial slice | Medium |
| 70 | `ArrayConcat_EmptyArrays_ReturnsEmpty` | Edge case | Low |
| 71 | `ArrayConcat_NullArg_ReturnsNull` | Null handling | Medium |
| 72 | `ArrayLength_OnNull_ReturnsNull` | `ARRAY_LENGTH(null)` | Medium |
| 73 | `ArrayLength_OnNonArray_ReturnsUndefined` | `ARRAY_LENGTH("string")` | Medium |

### 4.6 Aggregate Edge Cases

| # | Test Name | Description | Priority |
|---|-----------|-------------|----------|
| 74 | `CountAggregate_ReturnsCorrectCount` | Basic COUNT test | High |
| 75 | `CountAggregate_WithFilter_ReturnsFilteredCount` | `COUNT(1)` with WHERE | Medium |
| 76 | `SumAggregate_EmptyResult_ReturnsZero` | SUM with no matching docs | Medium |
| 77 | `AvgAggregate_EmptyResult_ReturnsUndefined` | AVG with no matching docs | Medium |
| 78 | `MinAggregate_EmptyResult_ReturnsUndefined` | MIN with no matching docs | Medium |
| 79 | `MaxAggregate_WithStrings_ReturnsLexMax` | MAX on string property | Medium |
| 80 | `CountAggregate_DistinctValues` | `COUNT(DISTINCT c.field)` if supported | Low |

### 4.7 Spatial Edge Cases (lower priority as spatial has good coverage)

| # | Test Name | Description | Priority |
|---|-----------|-------------|----------|
| 81 | `StArea_ReturnsPolygonArea` | New function, untested | Medium |
| 82 | `StDistance_NullLocation_ReturnsNull` | Null geometry | Low |

### 4.8 Conversion Function Edge Cases

| # | Test Name | Description | Priority |
|---|-----------|-------------|----------|
| 83 | `ToNumber_BoolInput_ConvertsCorrectly` | `ToNumber(true)` → 1? | Medium |
| 84 | `ToNumber_NullInput_ReturnsNull` | `ToNumber(null)` → null | Medium |
| 85 | `ToNumber_InvalidString_ReturnsUndefined` | `ToNumber("abc")` → undefined | Medium |
| 86 | `ToBoolean_NumberInput_ConvertsCorrectly` | `ToBoolean(1)` → true? | Medium |
| 87 | `ToBoolean_NullInput_ReturnsNull` | `ToBoolean(null)` → null | Medium |
| 88 | `ToString_ObjectInput_ReturnsJsonString` | `ToString({a:1})` behavior | Medium |
| 89 | `ToString_ArrayInput_ReturnsJsonString` | `ToString([1,2])` behavior | Medium |

---

## 5. Test Execution Plan

All new tests go in `SqlFunctionTests.cs` as new methods in the appropriate class or a new `SqlFunctionDeepDiveTests` class.

### Phase 1: Bug Fix Tests (Red → Investigate → Green/Skip)

**Order**: Write failing test first, then fix implementation.

| Order | Bug | Test(s) | Expected Action |
|-------|-----|---------|-----------------|
| 1.1 | Bug #1 — LOG custom base | `Log_WithBase_ReturnsCustomBaseLog` | Fix: add 2-arg LOG support |
| 1.2 | Bug #2 — ROUND precision | `Round_WithPrecision_RoundsToDecimalPlaces` | Fix: add 2-arg ROUND support |
| 1.3 | Bug #3 — INDEX_OF start pos | `IndexOf_WithStartPosition_ReturnsCorrectIndex` | Fix: add 3-arg INDEX_OF support |
| 1.4 | Bug #5 — LENGTH on non-string | `Length_OnNonString_ReturnsUndefined` | Investigate: may skip if too breaking |
| 1.5 | Bug #7 — REPLICATE negative | `Replicate_NegativeCount_ReturnsUndefined` | Fix: add check |
| 1.6 | Bug #8 — StringToNumber NaN | `StringToNumber_NaN_ReturnsUndefined`, `StringToNumber_Infinity_ReturnsUndefined` | Fix: add NaN/Infinity checks |

### Phase 2: Missing Function Tests

| Order | Function | Test(s) |
|-------|----------|---------|
| 2.1 | COT | `Cot_ReturnsCorrectValue` |
| 2.2 | CHOOSE | `Choose_ReturnsCorrectElement`, `Choose_OutOfRange_ReturnsNull` |
| 2.3 | OBJECTTOARRAY | `ObjectToArray_ReturnsKeyValuePairs` |
| 2.4 | ARRAYTOOBJECT | `ArrayToObject_ReturnsObject` |
| 2.5 | STRINGJOIN | `StringJoin_JoinsArrayWithSeparator` |
| 2.6 | STRINGSPLIT | `StringSplit_SplitsStringByDelimiter` |
| 2.7 | STRINGTONULL | `StringToNull_ParsesNullLiteral`, `StringToNull_InvalidInput_ReturnsUndefined` |
| 2.8 | DOCUMENTID | `DocumentId_ReturnsDocumentId` |
| 2.9 | ENDSWITH case-insensitive | `EndsWith_CaseInsensitive` |
| 2.10 | COUNT aggregate | `CountAggregate_ReturnsCorrectCount` |
| 2.11 | DateTimeDiff smoke | `DateTimeDiff_ReturnsDayDifference` (basic smoke, detailed coverage in DateHandlingTests) |
| 2.12 | DateTimeFromParts smoke | `DateTimeFromParts_BuildsCorrectDateTime` (basic smoke) |
| 2.13 | ST_AREA | `StArea_ReturnsPolygonArea` (or skip if too complex) |

### Phase 3: Edge Case Tests (grouped by function category)

Execute in table order from Section 4, prioritizing High → Medium → Low.

### Phase 4: Consolidation
- Review all `Gap` test classes (SqlFunctionGapTests, GapTests2, GapTests3, GapTests4)
- Merge any tests that are duplicated between main class and gap classes into a unified structure
- Ensure no test is testing the exact same thing in two places
- Final test run — all green

---

## 6. Documentation Updates

After implementation:

### 6.1 Wiki: Known-Limitations.md
- **Remove** any function limitation entries that are now fixed (LOG 2-arg, ROUND 2-arg, INDEX_OF 3-arg)
- **Add** any newly-discovered limitations from Skip-annotated tests with clear descriptions

### 6.2 Wiki: Features.md
- **Add** mention of newly-tested functions: COT, CHOOSE, OBJECTTOARRAY, ARRAYTOOBJECT, STRINGJOIN, STRINGSPLIT, STRINGTONULL, DOCUMENTID, ST_AREA
- Update SQL function count if applicable

### 6.3 Wiki: Feature-Comparison-With-Alternatives.md
- Update any rows related to SQL function support
- If LOG/ROUND/INDEX_OF multi-arg were previously noted as gaps, mark them as supported

### 6.4 Wiki: SQL-Queries.md
- Add any newly-supported function variants to examples if relevant

### 6.5 Root README.md
- Update function count if it changed (currently "100+ built-in SQL functions")

### 6.6 Wiki: Home.md
- Update if version is mentioned

---

## 7. Version Bump & Release

1. Bump `src/CosmosDB.InMemoryEmulator/CosmosDB.InMemoryEmulator.csproj` → `<Version>2.0.5</Version>`
2. `git add -A`
3. `git commit -m "v2.0.5: SqlFunction deep-dive — fix LOG/ROUND/INDEX_OF multi-arg, add COT/CHOOSE/OBJECTTOARRAY/ARRAYTOOBJECT/STRINGJOIN/STRINGSPLIT/STRINGTONULL/DOCUMENTID tests, comprehensive edge cases"`
4. `git tag v2.0.5`
5. `git push; git push --tags`
6. Push wiki updates separately

---

## 8. Status Tracker

### Bugs
| ID | Description | Status |
|----|-------------|--------|
| B1 | LOG() custom base 2-arg | 🔴 NOT STARTED |
| B2 | ROUND() precision 2-arg | 🔴 NOT STARTED |
| B3 | INDEX_OF() start position 3-arg | 🔴 NOT STARTED |
| B4 | CONCAT() null behavior investigation | 🔴 NOT STARTED |
| B5 | LENGTH() on non-string | 🔴 NOT STARTED |
| B7 | REPLICATE() negative count | 🔴 NOT STARTED |
| B8 | StringToNumber NaN/Infinity | 🔴 NOT STARTED |

### Missing Function Tests
| ID | Function | Status |
|----|----------|--------|
| F1 | COT | 🔴 NOT STARTED |
| F2 | CHOOSE | 🔴 NOT STARTED |
| F3 | OBJECTTOARRAY | 🔴 NOT STARTED |
| F4 | ARRAYTOOBJECT | 🔴 NOT STARTED |
| F5 | STRINGJOIN | 🔴 NOT STARTED |
| F6 | STRINGSPLIT | 🔴 NOT STARTED |
| F7 | STRINGTONULL | 🔴 NOT STARTED |
| F8 | DOCUMENTID | 🔴 NOT STARTED |
| F9 | ENDSWITH case-insensitive | 🔴 NOT STARTED |
| F10 | COUNT aggregate | 🔴 NOT STARTED |
| F11 | DateTimeDiff smoke | 🔴 NOT STARTED |
| F12 | DateTimeFromParts smoke | 🔴 NOT STARTED |
| F13 | ST_AREA | 🔴 NOT STARTED |

### Edge Cases (by priority)
| Priority | Count | Status |
|----------|-------|--------|
| High | ~15 tests | 🔴 NOT STARTED |
| Medium | ~45 tests | 🔴 NOT STARTED |
| Low | ~15 tests | 🔴 NOT STARTED |

### Documentation
| ID | Document | Status |
|----|----------|--------|
| D1 | Known-Limitations.md | 🔴 NOT STARTED |
| D2 | Features.md | 🔴 NOT STARTED |
| D3 | Feature-Comparison-With-Alternatives.md | 🔴 NOT STARTED |
| D4 | SQL-Queries.md | 🔴 NOT STARTED |
| D5 | README.md | 🔴 NOT STARTED |

### Release
| ID | Step | Status |
|----|------|--------|
| R1 | Version bump to 2.0.5 | 🔴 NOT STARTED |
| R2 | Git tag and push | 🔴 NOT STARTED |
| R3 | Wiki push | 🔴 NOT STARTED |

---

## TDD Protocol Reminder

For each item:
1. **RED**: Write the failing test first. Run it. Confirm it fails for the right reason.
2. **GREEN**: Implement the minimum fix. Run the test. Confirm it passes.
3. **REFACTOR**: Clean up if needed. Run full suite. Confirm no regressions.
4. **SKIP PROTOCOL**: If implementation is too complex or would introduce breaking changes:
   - Mark the test with `[Fact(Skip = "...detailed reason...")]`
   - Create a sister test `[Fact] public async Task FuncName_DivergentBehavior_EmulatorReturnsX()` that documents what the emulator actually does, with inline comments explaining the difference
   - Add to Known-Limitations.md
5. **Update this plan**: Mark the status tracker after each item completes.
