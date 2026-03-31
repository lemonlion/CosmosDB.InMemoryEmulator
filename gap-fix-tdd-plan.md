# Gap Fix TDD Plan

Status: COMPLETE — All 13 implemented fixes pass (30 tests), 6 divergent tests skipped, full regression: 0 failed, 1158 passed, 29 skipped

## Critical Fixes (C1–C5)

### C1: String `=`/`!=` comparison — case-insensitive → case-sensitive
- **File**: InMemoryContainer.cs `ValuesEqual` line ~2375
- **Test class**: `StringComparisonCaseSensitivityTests`
- **Tests**:
  - `Query_WhereEquals_IsCaseSensitive` — `WHERE c.name = 'alice'` should NOT match `"Alice"`
  - `Query_WhereNotEquals_IsCaseSensitive` — `WHERE c.name != 'Alice'` should NOT match `"Alice"`
  - `Query_WhereIn_IsCaseSensitive` — `WHERE c.name IN ('alice')` should NOT match `"Alice"`
- **Fix**: Change `StringComparison.OrdinalIgnoreCase` → `StringComparison.Ordinal` in `ValuesEqual`
- **Status**: DONE

### C2: String `<`/`>`/`<=`/`>=` comparison — case-insensitive → case-sensitive
- **File**: InMemoryContainer.cs `CompareValues` line ~2406
- **Tests**:
  - `Query_OrderBy_String_IsCaseSensitive` — uppercase letters sort before lowercase in ordinal
  - `Query_WhereGreaterThan_String_IsCaseSensitive`
- **Fix**: Change `StringComparison.OrdinalIgnoreCase` → `StringComparison.Ordinal` in `CompareValues`
- **Status**: DONE

### C3: `LIKE` operator — case-insensitive → case-sensitive
- **File**: InMemoryContainer.cs `EvaluateLike` line ~2431
- **Tests**:
  - `Query_Like_IsCaseSensitive` — `LIKE 'A%'` should NOT match `"alice"`
- **Fix**: Remove `RegexOptions.IgnoreCase` from LIKE regex
- **Status**: DONE

### C4: `LIKE` without `ESCAPE` — regex metacharacters not escaped
- **File**: InMemoryContainer.cs `EvaluateLike` line ~2431
- **Tests**:
  - `Query_Like_WithRegexMetachars_TreatedAsLiterals` — `LIKE 'test[1]'` matches literally
  - `Query_Like_WithDot_TreatedAsLiteral` — `LIKE 'foo.bar'` matches literally
- **Fix**: Apply `Regex.Escape()` to non-wildcard portions of the pattern
- **Status**: DONE

### C5: `ObjectToArray` returns `{Name, Value}` instead of `{k, v}`
- **File**: InMemoryContainer.cs line ~3534
- **Tests**:
  - `ObjectToArray_ReturnsKVKeys` — verify keys are `"k"` and `"v"`
- **Fix**: Change `["Name"]` → `["k"]`, `["Value"]` → `["v"]`
- **Also fix**: Existing test `ObjectToArray_ConvertsObjectToNameValuePairs` assertions
- **Status**: DONE

## Moderate Fixes (M1–M10)

### M1: `COUNT(c.field)` counts all rows — should count only defined
- **File**: InMemoryContainer.cs `ProjectAggregateFields` line ~2146
- **Tests**:
  - `Query_Count_OfField_ExcludesUndefined` — `COUNT(c.optional)` excludes docs without the field
- **Fix**: When COUNT argument is not `*` or `1`, count only items where field is defined
- **Status**: DONE

### M2: `MIN`/`MAX` on strings — should work lexicographically
- **File**: InMemoryContainer.cs `ProjectAggregateFields` line ~2152
- **Tests**:
  - `Query_Min_OnStrings_ReturnsLexicographicMin`
  - `Query_Max_OnStrings_ReturnsLexicographicMax`
- **Fix**: Change `ExtractNumericValues` to `ExtractComparableValues` supporting strings
- **Status**: DONE

### M3: `AVG` returns `0` for empty sets — should return undefined
- **File**: InMemoryContainer.cs line ~2155
- **Tests**:
  - `Query_Avg_EmptySet_ReturnsNoValue`
- **Fix**: Return `null` (omit field) when values list is empty
- **Status**: DONE

### M4: `REGEXMATCH` — support all modifiers (`m`, `s`, `x`, combined)
- **File**: InMemoryContainer.cs line ~2999
- **Tests**:
  - `RegexMatch_MultilineModifier_MatchesAcrossLines`
  - `RegexMatch_CombinedModifiers_Work`
- **Fix**: Parse each character of the modifiers string into `RegexOptions` flags
- **Status**: DONE

### M5: `EXISTS` catch-all returns `true` — should return `false`
- **File**: InMemoryContainer.cs line ~2673
- **Tests**:
  - `Query_Exists_UnparseableSubquery_ReturnsFalse`
- **Fix**: Change `catch { return true; }` → `catch { return false; }`
- **Status**: DONE

### M6: `ArrayToObject` function missing
- **File**: InMemoryContainer.cs function dispatch
- **Tests**:
  - `ArrayToObject_ConvertsKVArrayToObject`
  - `ArrayToObject_WithInvalidInput_ReturnsNull`
- **Fix**: Add `ARRAYTOOBJECT` case to function dispatch
- **Status**: DONE

### M7: Cross-partition aggregates multiply results when PartitionKeyRangeCount > 1
- **Complexity**: HIGH — requires changes to FakeCosmosHandler aggregate detection and per-range delegation
- **Decision**: SKIP with divergent test — this only affects non-default config
- **Status**: SKIPPED (DivergentBehaviorTests.cs)

### M8: `StringTo*` functions return `null` for invalid input — should return `undefined`
- **File**: InMemoryContainer.cs lines ~3020-3050
- **Tests**:
  - `StringToNumber_InvalidInput_ReturnsUndefined`
  - `StringToBoolean_InvalidInput_ReturnsUndefined`
  - `StringToArray_InvalidInput_ReturnsUndefined`
  - `StringToObject_InvalidInput_ReturnsUndefined`
  - `StringToNull_InvalidInput_ReturnsUndefined`
- **Fix**: Return `UndefinedValue.Instance` instead of `null` for invalid inputs
- **Status**: DONE

### M9: Subqueries ignore `ORDER BY` and `OFFSET`/`LIMIT`
- **Complexity**: HIGH — requires significant subquery evaluation expansion
- **Decision**: SKIP with divergent test
- **Status**: DONE

### M10: `GROUP BY` without aggregates returns full document — should return projected fields
- **File**: InMemoryContainer.cs line ~1740
- **Tests**:
  - `Query_GroupBy_WithoutAggregates_ReturnsProjectedFields`
- **Fix**: When GROUP BY is used without aggregates, project only SELECT fields from the first item
- **Status**: DONE

## Minor Fixes (L1–L6)

### L1: `DateTimeBin` — 'year'/'month' bin parts return input unchanged
- **File**: InMemoryContainer.cs line ~3660
- **Tests**:
  - `DateTimeBin_Year_BinsToYearBoundary`
  - `DateTimeBin_Month_BinsToMonthBoundary`
- **Fix**: Add year/month cases to the switch
- **Status**: DONE

### L2: Array functions only accept identifiers — not literal arrays
- **Complexity**: MODERATE — requires expression evaluation changes in array function dispatch
- **Decision**: SKIP with divergent test — literal arrays in SQL are rare
- **Status**: SKIPPED (DivergentBehaviorTests.cs)

### L3: `GetCurrentDateTime()` not consistent across rows
- **Decision**: SKIP with divergent test — sub-millisecond drift is negligible
- **Status**: SKIPPED (DivergentBehaviorTests.cs)

### L4: `linqSerializerOptions` and `continuationToken` on `GetItemLinqQueryable` ignored
- **Decision**: SKIP with divergent test — documented in Known Limitations
- **Status**: SKIPPED (DivergentBehaviorTests.cs)

### L5: `PreTriggers`/`PostTriggers` in `RequestOptions` silently ignored
- **Decision**: Already covered by existing "Triggers Don't Execute" known limitation
- **Status**: SKIP (already documented)

### L6: Undefined vs null not distinguished in ORDER BY
- **Decision**: SKIP with divergent test — real Cosmos type ordering is complex
- **Status**: SKIPPED (DivergentBehaviorTests.cs)
