# QueryPlanTests Deep-Dive Plan

## Overview

The `QueryPlanTests.cs` file tests `HandleQueryPlanAsync()` in `FakeCosmosHandler.cs` — the gateway
query plan endpoint the Cosmos SDK uses on non-Windows platforms (where the native ServiceInterop
DLL is unavailable). The query plan tells the SDK how to build its execution pipeline (merge sort
for ORDER BY, aggregate accumulation for COUNT/SUM, deduplication for DISTINCT, etc.).

**Current version:** 2.0.4 → will become **2.0.5** after implementation.

---

## Current Coverage (17 tests)

| # | Test | Fields Verified |
|---|------|-----------------|
| 1 | `SimpleSelect_HasNoSpecialFlags` | distinctType, orderBy, aggregates, groupBy, hasSelectValue, top, offset, limit |
| 2 | `OrderByAscending_SetsOrderByMetadata` | orderBy[0]=Ascending, orderByExpressions[0], hasNonStreamingOrderBy |
| 3 | `OrderByDescending_SetsDescendingFlag` | orderBy[0]=Descending |
| 4 | `MultipleOrderBy_SetsAllFields` | orderBy count=2, orderByExpressions count=2 |
| 5 | `Top_SetsTopField` | top=10 |
| 6 | `OffsetLimit_SetsBothFields` | offset=5, limit=10 |
| 7 | `Distinct_SetsDistinctTypeUnordered` | distinctType=Unordered |
| 8 | `DistinctWithOrderBy_SetsDistinctTypeOrdered` | distinctType=Ordered |
| 9 | `CountAggregate_DetectsCount` | aggregates contains Count |
| 10 | `SumAggregate_DetectsSum` | aggregates contains Sum |
| 11 | `MinMaxAvg_DetectsAll` | aggregates contains Min/Max/Average, alias mapping |
| 12 | `GroupBy_SetsGroupByExpressions` | groupByExpressions count=1 |
| 13 | `SelectValue_SetsHasSelectValue` | hasSelectValue=true |
| 14 | `WhereClause_DoesNotAffectFlags` | distinctType=None, empty orderBy/aggregates |
| 15 | `QueryRanges_CoversFullRange` | queryRanges min/max/inclusive |
| 16 | `ComplexQuery_SetsAllRelevantFlags` | Combined DISTINCT+TOP+ORDER BY+GROUP BY+aggregates |
| 17 | `UnparsableQuery_StillReturnsValidPlan` | Graceful fallback |

---

## Gap Analysis

### A. ORDER BY Gaps

| ID | Gap | Category | Difficulty |
|----|-----|----------|------------|
| A1 | ORDER BY without explicit ASC/DESC (defaults to Ascending) | Edge case | Easy |
| A2 | ORDER BY on nested property (e.g. `c.address.city`) | Edge case | Easy |
| A3 | ORDER BY with function expression (e.g. `LOWER(c.name)`) — `field.Field` may be null | Edge case / Bug | Medium |
| A4 | `hasNonStreamingOrderBy` is false when no ORDER BY | Missing assertion | Easy |
| A5 | Rewritten query format for single ORDER BY field | Missing coverage | Easy |
| A6 | Rewritten query format for multiple ORDER BY fields | Missing coverage | Easy |
| A7 | Rewritten query includes WHERE clause | Missing coverage | Medium |
| A8 | Rewritten query preserves FROM alias when not "c" | Missing coverage | Medium |

### B. DISTINCT Gaps

| ID | Gap | Category | Difficulty |
|----|-----|----------|------------|
| B1 | `SELECT DISTINCT VALUE c.name` — both DISTINCT and VALUE flags | Edge case | Easy |

### C. Aggregate Gaps

| ID | Gap | Category | Difficulty |
|----|-----|----------|------------|
| C1 | Aggregate in arithmetic expression: `SUM(c.a) + SUM(c.b)` — currently only detects aggregates at top-level FunctionCallExpression, not when wrapped in BinaryExpression alias | Bug | Medium |
| C2 | Duplicate aggregate type dedup: `SUM(c.a) AS s1, SUM(c.b) AS s2` — aggregates array should contain "Sum" only once | Edge case | Easy |
| C3 | COUNT without alias — no alias mapping entry, just aggregates array entry | Edge case | Easy |
| C4 | Aggregate function case insensitivity: `count(1)`, `Count(1)`, `COUNT(1)` | Edge case | Easy |
| C5 | Non-aggregate function in SELECT doesn't pollute aggregates array (e.g. `UPPER(c.name)`) | Negative test | Easy |

### D. GROUP BY Gaps

| ID | Gap | Category | Difficulty |
|----|-----|----------|------------|
| D1 | Multiple GROUP BY fields | Edge case | Easy |
| D2 | `groupByAliases` field initialized but never populated — potential SDK issue? | Observation / Bug | Needs research |

### E. SELECT VALUE Gaps

| ID | Gap | Category | Difficulty |
|----|-----|----------|------------|
| E1 | `SELECT VALUE COUNT(1)` — both hasSelectValue + aggregate | Combination | Easy |

### F. Rewritten Query Gaps

| ID | Gap | Category | Difficulty |
|----|-----|----------|------------|
| F1 | Non-ORDER BY query: rewrittenQuery = original SQL verbatim | Missing coverage | Easy |
| F2 | Unparsable query: rewrittenQuery = original SQL verbatim | Already tested (partial) | Easy |
| F3 | ORDER BY rewritten query structure: `SELECT alias._rid, [...] AS orderByItems, alias AS payload` | Missing coverage | Easy |

### G. Response Structure Gaps

| ID | Gap | Category | Difficulty |
|----|-----|----------|------------|
| G1 | `partitionedQueryExecutionInfoVersion` = 2 | Missing assertion | Easy |
| G2 | HTTP response status code is 200 OK | Missing assertion | Easy |
| G3 | All expected default fields present in a simple plan (complete schema check) | Missing coverage | Easy |

### H. Edge Cases

| ID | Gap | Category | Difficulty |
|----|-----|----------|------------|
| H1 | Query with parameters: `SELECT * FROM c WHERE c.name = @name` | Edge case | Easy |
| H2 | OFFSET without LIMIT (and vice versa) — are they independent? | Edge case | Easy |
| H3 | TOP 0 — edge case for zero results | Edge case | Easy |
| H4 | `SELECT *` vs `SELECT c.field1, c.field2` — no aggregate detection for wildcard | Edge case | Easy |
| H5 | Query with JOIN — no query plan flags affected | Edge case | Medium |

---

## Bugs Found

### Bug 1: `DetectAggregates` doesn't detect aggregates wrapped in binary expressions WITH aliases

**Location:** `FakeCosmosHandler.cs` line ~581-617

When a SELECT field like `SUM(c.a) * 2 AS total` is detected:
- The `field.SqlExpr` is a `BinaryExpression` (not a `FunctionCallExpression`)
- `DetectAggregates` recurses into `BinaryExpression.Left` and finds `SUM` → adds "Sum" to aggregates
- BUT the alias `"total"` is only passed on the top-level call, and when recursing into binary children, alias is set to `null`
- **Result:** The `groupByAliasToAgg` mapping won't contain `total → Sum` because the alias is lost during recursion

**Impact:** GROUP BY queries with aliased arithmetic aggregate expressions won't get correct alias mapping.

**Fix:** Track the top-level alias through the recursion, or detect the alias at the top level when an aggregate is found anywhere in the expression tree.

**Test plan:** Write test C1 to demonstrate this bug, then fix the recursion to propagate the alias to the first aggregate found.

### Bug 2: ORDER BY expression fields — `field.Field` can be null

**Location:** `FakeCosmosHandler.cs` line ~486-491 and `BuildOrderByRewrittenQuery` line ~813

When ORDER BY uses a function expression like `ORDER BY LOWER(c.name)`, the parser creates `OrderByField(null, true, Expression)` where `Field` is null but `Expression` is set. The query plan code unconditionally adds `field.Field` to `orderByExpressions`, resulting in a null entry.

Similarly, `BuildOrderByRewrittenQuery` uses `field.Field` in the template string, producing `{"item": }` which is invalid JSON.

**Impact:** ORDER BY with function expressions produces malformed query plans.

**Fix:** When `field.Field` is null, fall back to `CosmosSqlParser.ExprToString(field.Expression)`.

**Test plan:** Write test A3. This may be too difficult to fix cleanly — if so, skip with reason and write divergent behavior test.

### Bug 3: `groupByAliases` initialized but never populated

**Location:** `FakeCosmosHandler.cs` line 467

The `groupByAliases` JArray is initialized but no code ever populates it.

**Impact:** Likely benign — the SDK may not use this field, or it may expect it empty. Research needed.

**Decision:** Add observation test D2 that asserts current behavior (always empty). If real Cosmos populates this, skip with divergence note.

---

## Implementation Plan

### Phase 1: New Tests (Red)

Write all new tests first. Each test targets a specific gap from the analysis above.
All tests go into `QueryPlanTests.cs`.

```
Test # | Gap ID | Test Name                                                          | Expected Result
-------|--------|--------------------------------------------------------------------|-----------------
 1     | A1     | QueryPlan_OrderByWithoutDirection_DefaultsToAscending              | orderBy[0]=Ascending
 2     | A2     | QueryPlan_OrderByNestedProperty_SetsExpression                     | orderByExpressions[0]="c.address.city"
 3     | A3     | QueryPlan_OrderByFunctionExpression_SetsExpression                 | orderByExpressions[0]="LOWER(c.name)" or similar — MAY SKIP if too hard
 4     | A4     | QueryPlan_NoOrderBy_HasNonStreamingOrderByIsFalse                  | hasNonStreamingOrderBy=false
 5     | A5     | QueryPlan_OrderBy_RewrittenQueryHasCorrectStructure                | rewrittenQuery contains _rid, orderByItems, payload
 6     | A6     | QueryPlan_MultipleOrderBy_RewrittenQueryHasAllFields               | rewrittenQuery has both orderByItems entries
 7     | A7     | QueryPlan_OrderByWithWhere_RewrittenQueryIncludesWhere             | rewrittenQuery contains WHERE clause
 8     | A8     | QueryPlan_OrderByWithCustomAlias_RewrittenQueryUsesAlias           | rewrittenQuery uses alias not "c" — MAY SKIP if parser doesn't support FROM x
 9     | B1     | QueryPlan_DistinctValue_SetsBothFlags                              | distinctType ≠ None, hasSelectValue=true
10     | C1     | QueryPlan_AggregateInArithmeticExpression_DetectsAggregate         | aggregates contains "Sum" — alias mapping may be missing (BUG)
11     | C2     | QueryPlan_DuplicateAggregateType_DeduplicatesInArray               | aggregates has "Sum" once, alias mapping has both
12     | C3     | QueryPlan_CountWithoutAlias_StillDetectsAggregate                  | aggregates contains "Count", aliasMap empty or no entry
13     | C4     | QueryPlan_AggregateFunctionCaseInsensitive_Detected                | count(1) as lowercase detected
14     | C5     | QueryPlan_NonAggregateFunction_NotInAggregates                     | UPPER() not in aggregates
15     | D1     | QueryPlan_MultipleGroupByFields_SetsAll                            | groupByExpressions count=2+
16     | E1     | QueryPlan_SelectValueWithAggregate_SetsBothFlags                   | hasSelectValue=true + aggregates contains "Count"
17     | F1     | QueryPlan_NonOrderByQuery_RewrittenQueryIsOriginalSql              | rewrittenQuery = original SQL
18     | G1     | QueryPlan_ResponseVersion_IsTwo                                    | partitionedQueryExecutionInfoVersion=2
19     | G2     | QueryPlan_ResponseStatusCode_Is200                                 | HTTP 200 OK
20     | G3     | QueryPlan_SimpleSelect_AllDefaultFieldsPresent                     | All expected keys exist in queryInfo
21     | H1     | QueryPlan_QueryWithParameters_ReturnsValidPlan                     | Plan parses correctly, no crash
22     | H2     | QueryPlan_OffsetWithoutLimit_SetsOnlyOffset                        | offset set, limit null — MAY SKIP if Cosmos requires both
23     | H3     | QueryPlan_TopZero_SetsTopToZero                                    | top=0
24     | H4     | QueryPlan_SelectSpecificFields_NoAggregates                        | aggregates empty, like SELECT c.name, c.age
25     | H5     | QueryPlan_JoinQuery_DoesNotAffectOrderByOrAggregates               | Standard flags unchanged
```

### Phase 2: Bug Fixes (Green)

1. **Bug 1 (C1):** Fix `DetectAggregates` to propagate alias through recursion — when a `BinaryExpression` contains an aggregate and the caller provided an alias, the alias should be applied to the first aggregate found.

2. **Bug 2 (A3):** Fix ORDER BY expression handling — when `field.Field` is null, use `CosmosSqlParser.ExprToString(field.Expression)` for both `orderByExpressions` and `BuildOrderByRewrittenQuery`. If this is too complex, skip test A3 with a clear reason and create a divergent behavior test showing what currently happens.

3. **Bug 3 (D2):** Research-only — add assertion that `groupByAliases` is always empty. If this turns out to be a real gap, note it in known limitations.

### Phase 3: Refactor

No significant refactoring anticipated. Minor cleanup only if test patterns suggest shared helpers.

### Phase 4: Tests Likely Needing Skip

| Test | Skip Reason |
|------|-------------|
| A3 (ORDER BY function expression) | If `ExprToString` doesn't round-trip function expressions stored in OrderByField.Expression, or if the parser doesn't populate Expression for complex ORDER BY — skip with reason "ORDER BY function expressions produce null Field in OrderByField; fixing requires parser changes to ExprToString round-tripping and BuildOrderByRewrittenQuery refactor" |
| A8 (custom FROM alias) | If `FROM x IN c.items` or `FROM x` doesn't work at query plan level — skip with reason "Parser supports FROM alias but query plan BuildOrderByRewrittenQuery may not use it correctly for non-standard aliases" |
| H2 (OFFSET without LIMIT) | Real Cosmos requires OFFSET+LIMIT together. If parser rejects OFFSET alone — skip with reason "Real Cosmos DB requires OFFSET and LIMIT together; standalone OFFSET is a syntax error" |

### Phase 5: Divergent Behavior Sister Tests

For each skipped test, create a sister test showing current behavior:

```
Skipped Test                              | Sister Test Name                                              | Purpose
------------------------------------------|---------------------------------------------------------------|--------
A3 (if skipped)                           | DivergentBehavior_OrderByFunctionExpr_FieldIsNull             | Shows field.Field is null for ORDER BY LOWER(c.name)
A8 (if skipped)                           | DivergentBehavior_CustomFromAlias_MayNotPropagate             | Shows alias handling gap in rewritten query
H2 (if skipped)                           | DivergentBehavior_OffsetWithoutLimit_ParserBehavior           | Shows what happens with OFFSET alone
```

### Phase 6: Documentation Updates

1. **Wiki Known-Limitations.md** — Add any newly discovered permanent limitations (e.g., ORDER BY function expressions in query plans if skipped)
2. **Wiki Feature-Comparison-With-Alternatives.md** — Update query plan row if comparison matrix exists
3. **Wiki Features.md** — Ensure query plan section reflects actual coverage
4. **README.md** — No changes expected unless a major feature gap is found
5. **CHANGELOG or commit message** — Summarize all fixes and new coverage

### Phase 7: Version Bump, Tag, Push

1. Bump `Version` in `CosmosDB.InMemoryEmulator.csproj`: `2.0.4` → `2.0.5`
2. `git add -A`
3. `git commit -m "v2.0.5: QueryPlan test coverage deep-dive — N new tests, M bug fixes"`
4. `git tag v2.0.5`
5. `git push && git push --tags`

---

## Execution Order

1. ☐ Write all new test methods (Phase 1) — they should all fail (Red)
2. ☐ Fix Bug 2 (ORDER BY null field) — some A-tests go green
3. ☐ Fix Bug 1 (aggregate alias propagation) — C1 goes green
4. ☐ Evaluate skipped tests — mark with `[Fact(Skip = "...")]` and write sister tests
5. ☐ Run full test suite — all should pass
6. ☐ Update wiki Known-Limitations.md
7. ☐ Update wiki Features.md / Comparison if needed
8. ☐ Version bump, commit, tag, push
9. ☐ Update this plan with final status

---

## Status Tracker

| Step | Status | Notes |
|------|--------|-------|
| Phase 1: Write tests | ☐ Not started | |
| Phase 2: Fix bugs | ☐ Not started | |
| Phase 3: Refactor | ☐ Not started | |
| Phase 4: Skip decisions | ☐ Not started | |
| Phase 5: Sister tests | ☐ Not started | |
| Phase 6: Documentation | ☐ Not started | |
| Phase 7: Version+Push | ☐ Not started | |
