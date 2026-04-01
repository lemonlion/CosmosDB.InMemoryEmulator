# Full-Text Search — Deep Dive Test Coverage & Bug Fix Plan

**Target version:** 2.0.5  
**Approach:** TDD — red-green-refactor. Write failing test → implement fix → verify green.  
**File:** `tests/CosmosDB.InMemoryEmulator.Tests/FullTextSearchTests.cs`  
**Implementation:** `src/CosmosDB.InMemoryEmulator/InMemoryContainer.cs` (lines ~4456–4530)  
**Parser:** `src/CosmosDB.InMemoryEmulator/CosmosSqlParser.cs` (`ORDER BY RANK` support)  

---

## Current State (28 tests)

| Class | Tests | Status |
|-------|:-----:|--------|
| `FullTextContainsTests` | 6 | ✅ All pass |
| `FullTextContainsAllTests` | 4 | ✅ All pass |
| `FullTextContainsAnyTests` | 5 | ✅ All pass |
| `FullTextScoreTests2` | 3 | ✅ All pass |
| `OrderByRankTests` | 4 | ✅ All pass |
| `FullTextCombinedTests` | 2 | ✅ All pass |
| `FullTextSearchDivergentBehaviorTests` | 3 | ✅ All pass (documenting divergence) |
| `FullTextSearchSkippedTests` | 1 | ⏭️ Skipped (known limitation) |

---

## Bugs Found

### BUG-1: Class name `FullTextScoreTests2` has spurious "2" suffix  
- **Severity:** Cosmetic  
- **Action:** Rename to `FullTextScoreTests`  
- **Status:** [ ] Not started  

### BUG-2: `FULLTEXTSCORE` allowed in `SELECT` projection — undocumented divergence  
- **Severity:** Medium (documentation gap)  
- **Details:** Per [Microsoft docs](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/fulltextscore), `FULLTEXTSCORE` **cannot** be part of a projection. The exact remark: *"This function can't be part of a projection (for example, `SELECT FullTextScore(c.text, "keyword") AS Score FROM c` is invalid)."* The emulator **allows** this and several existing tests (`FullTextScoreTests2.FullTextScore_ReturnsNumericValue`, `MoreMatchingTerms_HigherScore`, `NoMatchingTerms_ReturnsZero`, `OrderByRankTests.WithSelectScore_ReturnsSortedWithScores`) depend on it.  
- **Decision:** Keep the emulator behaviour as-is (it's useful for debugging), but document it as a divergent behaviour with a skipped test showing the real Cosmos would reject it, and a passing sister test showing the emulator accepts it.  
- **Status:** [ ] Not started  

### BUG-3: Substring matching not documented as divergent from word-boundary tokenization  
- **Severity:** Medium (documentation gap)  
- **Details:** The emulator uses `string.Contains()` (substring matching), so `FullTextContains(c.text, 'data')` matches a document containing "database". Real Cosmos DB tokenizes at word boundaries, so "data" would NOT match "database" (different tokens). Existing tests mention "no stemming" but don't cover substring-vs-word-boundary.  
- **Action:** Add explicit divergent behaviour test + documentation.  
- **Status:** [ ] Not started  

### BUG-4: `RRF` (Reciprocal Rank Fusion) function not implemented — not in Known Limitations  
- **Severity:** Medium (missing feature awareness)  
- **Details:** Cosmos DB has an `RRF()` function for hybrid search (combining `FullTextScore` + `VectorDistance`). It's not implemented in the emulator and not mentioned in Known Limitations. Anyone trying `ORDER BY RANK RRF(...)` will get a parse error with no guidance.  
- **Action:** Add to Known Limitations. Add skipped test documenting the gap. Consider implementing basic RRF in future.  
- **Status:** [ ] Not started  

---

## Missing Test Coverage

### A) Parity gaps — FullTextContainsAll (missing tests that FullTextContains has)

| # | Test | Rationale | Difficulty |
|---|------|-----------|:----------:|
| A1 | `FullTextContainsAll_NullField_ReturnsEmpty` | `FullTextContains` has a null-field test; `ContainsAll` doesn't | Easy |
| A2 | `FullTextContainsAll_WithPartitionKey_RespectsFilter` | Parity with `FullTextContains` PK test | Easy |

### B) Parity gaps — FullTextContainsAny

| # | Test | Rationale | Difficulty |
|---|------|-----------|:----------:|
| B1 | `FullTextContainsAny_NullField_ReturnsEmpty` | Missing null-field guard test | Easy |
| B2 | `FullTextContainsAny_IsCaseInsensitive` | ContainsAll has this, ContainsAny doesn't | Easy |
| B3 | `FullTextContainsAny_SingleTerm_WorksLikeContains` | Parity with ContainsAll single-term test | Easy |
| B4 | `FullTextContainsAny_WithPartitionKey_RespectsFilter` | Parity with FullTextContains PK test | Easy |

### C) FullTextScore edge cases

| # | Test | Rationale | Difficulty |
|---|------|-----------|:----------:|
| C1 | `FullTextScore_NullField_ReturnsZero` | Confirm null-safety of scoring | Easy |
| C2 | `FullTextScore_SingleSearchTerm_ReturnsCount` | Only multi-term tested currently | Easy |
| C3 | `FullTextScore_IsCaseInsensitive` | Scoring correctness with case variation | Easy |

### D) Edge cases — all functions

| # | Test | Rationale | Difficulty |
|---|------|-----------|:----------:|
| D1 | `FullTextContains_EmptyStringTerm_MatchesEverything` | Edge: `"".Contains("")` is true in .NET; confirm behaviour | Easy |
| D2 | `FullTextContains_EmptyStringField_ReturnsEmpty` | Field is `""`, term is a real word | Easy |
| D3 | `FullTextContains_NestedPropertyPath` | e.g., `FullTextContains(c.metadata.description, 'test')` | Easy |
| D4 | `FullTextContains_MultiWordPhrase` | `FullTextContains(c.text, 'search phrase')` — phrase matching | Easy |
| D5 | `FullTextContains_SpecialCharacters` | Punctuation, accented characters in text/term | Easy |
| D6 | `FullTextContains_WithNotOperator` | `NOT FullTextContains(c.text, 'term')` | Easy |
| D7 | `FullTextContains_WithOrOperator` | `FullTextContains(c.text, 'a') OR FullTextContains(c.text, 'b')` | Easy |
| D8 | `FullTextContainsAll_ManyTerms` | 5+ terms — confirms variadic args handling | Easy |
| D9 | `FullTextContainsAny_ManyTerms` | 5+ terms — confirms variadic args handling | Easy |

### E) ORDER BY RANK edge cases

| # | Test | Rationale | Difficulty |
|---|------|-----------|:----------:|
| E1 | `OrderByRank_WithOffsetLimit_ReturnsPage` | `ORDER BY RANK ... OFFSET 1 LIMIT 2` | Medium |
| E2 | `OrderByRank_TiedScores_ReturnsAllDocuments` | Same score — stability check | Easy |
| E3 | `OrderByRank_EmptyContainer_ReturnsEmpty` | Zero-document edge case | Easy |
| E4 | `OrderByRank_SingleDocument_ReturnsThatDocument` | Single-document edge case | Easy |
| E5 | `OrderByRank_WithDistinct_ReturnsUniqueResults` | `SELECT DISTINCT c.category ... ORDER BY RANK ...` | Medium |

### F) Parameterized queries

| # | Test | Rationale | Difficulty |
|---|------|-----------|:----------:|
| F1 | `FullTextContains_WithParameterizedTerm` | `FullTextContains(c.text, @term)` with `QueryDefinition` | Medium |
| F2 | `FullTextScore_WithParameterizedTerms` | Parameterized scoring | Medium |

### G) Divergent behaviour — FULLTEXTSCORE in projection (BUG-2)

| # | Test | Rationale | Difficulty |
|---|------|-----------|:----------:|
| G1 | `FullTextScore_InProjection_ShouldBeInvalid` | **SKIPPED** — real Cosmos rejects `SELECT FullTextScore(...) AS score`. Skip reason documents the real behaviour. | N/A |
| G2 | `FullTextScore_InProjection_WorksInEmulator` | **SISTER** — passing test showing emulator allows it. Heavily commented explaining the divergence. | Easy |

### H) Divergent behaviour — Substring vs word-boundary matching (BUG-3)

| # | Test | Rationale | Difficulty |
|---|------|-----------|:----------:|
| H1 | `FullTextContains_SubstringMatchesMidWord_DivergentFromRealCosmos` | `"data"` matches `"database"` in emulator but not in real Cosmos (tokenization). Passing test documenting the divergence. | Easy |

### I) RRF not implemented (BUG-4)

| # | Test | Rationale | Difficulty |
|---|------|-----------|:----------:|
| I1 | `RRF_BasicFusion_ShouldCombineScores` | **SKIPPED** — `ORDER BY RANK RRF(FullTextScore(...), FullTextScore(...))` not implemented. Clear skip reason explaining what RRF is. | N/A |
| I2 | `RRF_NotSupported_ParsesButThrows` | **SISTER** — shows what actually happens when you try RRF (parse error or unrecognized function). | Medium |

### J) Non-string field types

| # | Test | Rationale | Difficulty |
|---|------|-----------|:----------:|
| J1 | `FullTextContains_NumericField_Behaviour` | What happens when field is a number? `ToString()` in impl means it would convert to string. | Easy |
| J2 | `FullTextContains_BooleanField_Behaviour` | Boolean field — `ToString()` conversion | Easy |
| J3 | `FullTextContains_ArrayField_Behaviour` | Array field — `ToString()` would produce JSON array string | Easy |

---

## Implementation Order (TDD)

### Phase 1: Bug fixes + rename
1. [ ] **BUG-1** — Rename `FullTextScoreTests2` → `FullTextScoreTests`
2. [ ] **BUG-2** — Add G1 (skipped) + G2 (sister) for FULLTEXTSCORE-in-projection divergence
3. [ ] **BUG-3** — Add H1 for substring-vs-word-boundary divergence
4. [ ] **BUG-4** — Add I1 (skipped) + I2 (sister) for RRF gap

### Phase 2: Parity tests (should all pass immediately — these test existing behaviour)
5. [ ] A1, A2 — FullTextContainsAll parity
6. [ ] B1–B4 — FullTextContainsAny parity
7. [ ] C1–C3 — FullTextScore edge cases

### Phase 3: Edge case tests (most should pass; may surface bugs)
8.  [ ] D1–D9 — Cross-function edge cases
9.  [ ] E1–E5 — ORDER BY RANK edge cases
10. [ ] F1–F2 — Parameterized queries
11. [ ] J1–J3 — Non-string field types

### Phase 4: Documentation
12. [ ] Update `Known-Limitations.md`:
    - Add RRF (Reciprocal Rank Fusion) as not implemented
    - Add FULLTEXTSCORE-in-projection as divergent behaviour
    - Add substring-vs-word-boundary matching to existing FTS section
13. [ ] Update `Features.md`:
    - Ensure full-text search section mentions all tested functions
14. [ ] Update `Feature-Comparison-With-Alternatives.md`:
    - Add row for RRF (not supported)
    - Note FULLTEXTSCORE projection divergence
15. [ ] Update `README.md`:
    - Update test count if changed
16. [ ] Update `Known-Limitations.md` § 13 to reference new divergent tests

### Phase 5: Version, tag, push
17. [ ] Bump version 2.0.4 → 2.0.5 in `CosmosDB.InMemoryEmulator.csproj`
18. [ ] `git add -A && git commit -m "v2.0.5: Full-text search deep dive — 30+ new tests, RRF gap documented, divergence tests"` (adjust message based on actual changes)
19. [ ] `git tag v2.0.5`
20. [ ] `git push && git push --tags`
21. [ ] Push wiki changes: `cd wiki && git add -A && git commit && git push`

---

## Test Count Estimate

| Category | New Tests |
|----------|:---------:|
| Parity (A1–B4) | 6 |
| Score edge cases (C1–C3) | 3 |
| Edge cases (D1–D9) | 9 |
| ORDER BY RANK (E1–E5) | 5 |
| Parameterized (F1–F2) | 2 |
| Divergent/Skipped (G1–G2, H1, I1–I2) | 5 |
| Non-string types (J1–J3) | 3 |
| **Total new** | **33** |
| **Existing** | **28** |
| **Grand total** | **~61** |

---

## Notes

- Tests that should be trivially "green" immediately (existing behaviour already works): A1–A2, B1–B4, C1–C3, D1–D7, D8–D9, E2–E4, G2, H1, J1–J3
- Tests that may need implementation work: E1 (OFFSET+LIMIT with RANK), E5 (DISTINCT with RANK), F1–F2 (parameterized queries with FTS functions), I2 (RRF parse behaviour)
- Skipped tests (will NOT need implementation): G1, I1
- If any "should-be-green" test fails, that surfaces a new bug — fix it in the red-green-refactor cycle
