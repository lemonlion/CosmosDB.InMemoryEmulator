# Vector Search TDD Plan

> Deep-dive test coverage expansion for `VectorSearchTests.cs` and the `VECTORDISTANCE` implementation.
> TDD: test first, red-green-refactor. Skipped tests for behaviour too hard to implement.
> After all work: increment patch version to **2.0.5**, tag `v2.0.5`, push.

## Current State

### Existing Tests (15 in VectorSearchTests.cs + 1 in QueryTests.cs)

| # | Test | Distance Fn | What it covers |
|---|------|-------------|----------------|
| 1 | `VectorDistance_Cosine_IdenticalVectors_ReturnsOne` | cosine | Score = 1.0 |
| 2 | `VectorDistance_Cosine_OrthogonalVectors_ReturnsZero` | cosine | Score = 0.0 |
| 3 | `VectorDistance_Cosine_OppositeVectors_ReturnsNegativeOne` | cosine | Score = -1.0 |
| 4 | `VectorDistance_DotProduct_ReturnsCorrectValue` | dotproduct | 2·1 + 3·5 + 4·2 = 25 |
| 5 | `VectorDistance_Euclidean_ReturnsCorrectValue` | euclidean | √2 |
| 6 | `VectorDistance_InSelectProjection_ReturnsSimilarityScore` | cosine | Scores computed for all docs |
| 7 | `VectorDistance_InOrderBy_SortsByScore` | cosine | DESC ordering |
| 8 | `VectorDistance_TopN_WithOrderBy_ReturnsClosest` | cosine | TOP 2 + DESC |
| 9 | `VectorDistance_WithWhereClause_FiltersAndScores` | cosine | WHERE + ORDER BY |
| 10 | `VectorDistance_MismatchedDimensions_ReturnsNull` | cosine | 2D doc vs 3D query → null |
| 11 | `VectorDistance_MissingEmbedding_ReturnsNull` | cosine | No embedding prop → null |
| 12 | `VectorDistance_DistanceFunctionOverride_UsesEuclidean` | euclidean | 4th arg override |
| 13 | `VectorDistance_BoolBruteForceParam_AcceptedAndIgnored` | cosine | 3rd arg = true |
| 14 | `VectorDistance_HighDimensional_1536Dimensions` | cosine | 1536-dim unit vector |
| 15 | `VectorDistance_DefaultDistanceFunction_IsCosine` | cosine | No 4th arg defaults to cosine |
| 16 | (QueryTests) `VectorDistance_ShouldComputeCosineSimilarity` | cosine | Basic ORDER BY ASC |

### Implementation Code (InMemoryContainer.cs lines 5375-5470)

- `VectorDistanceFunc(args)` — dispatches to cosine/dotproduct/euclidean
- `ToDoubleArray(object)` — converts JArray or string to double[]
- `CosineSimilarity(a, b)` — dot / (|a| × |b|), returns 0 for zero-magnitude
- `DotProduct(a, b)` — Σ(a[i] × b[i])
- `EuclideanDistance(a, b)` — √Σ(a[i] - b[i])²

---

## Identified Bugs

### BUG-1: Zero vector cosine returns 0 instead of matching real Cosmos DB

**Current code:** `return denominator == 0 ? 0 : dot / denominator;`

**Issue:** When either vector is all zeros (zero magnitude), cosine similarity is mathematically undefined. Real Cosmos DB returns `null` (no SimilarityScore in output) for zero-magnitude vectors. The emulator returns `0.0`, which is a valid cosine similarity value (it means orthogonal), so it's silently misleading.

**Fix:** Return `null` when denominator is 0.

**Severity:** Low — zero vectors are rare in real embeddings, but the incorrect return value could mask bugs in user code that produces zero embeddings.

### BUG-2: Options object with `dataType` property is silently ignored

**Current code:** Only reads `distanceFunction` from the 4th argument options object. The `dataType`, `searchListSizeMultiplier`, `quantizedVectorListMultiplier`, and `filterPriority` properties are accepted (not rejected) but silently ignored.

**Assessment:** This is intentional and correct for the emulator (these relate to indexing, not computation). Not a bug — but we should have a test proving they're accepted without error.

### BUG-3: Unknown distance function silently falls back to cosine

**Current code:** The default arm `_ => CosineSimilarity(vec1, vec2)` means a typo like `{distanceFunction:'cossine'}` silently uses cosine instead of returning an error.

**Assessment:** Real Cosmos DB rejects unknown distance functions. However, since the emulator is for testing convenience, this is arguably acceptable. We should document it and add a test. If the user passes a completely bogus value, they might want to know, but this is low priority.

---

## Coverage Gaps & New Tests to Write

### Category A: Distance Function Correctness (Mathematical Edge Cases)

| ID | Test | Why | Expected |
|----|------|-----|----------|
| A1 | `VectorDistance_Cosine_ZeroVector_ReturnsNull` | BUG-1: zero-magnitude vector | null (match real Cosmos) |
| A2 | `VectorDistance_Cosine_NonUnitVectors_NormalizesCorrectly` | Verify normalization with non-unit vectors e.g. [3,4] vs [6,8] | 1.0 (parallel) |
| A3 | `VectorDistance_Cosine_NegativeComponents_ComputesCorrectly` | Mixed positive/negative components | Exact expected value |
| A4 | `VectorDistance_DotProduct_OrthogonalVectors_ReturnsZero` | Orthogonal dot product | 0.0 |
| A5 | `VectorDistance_DotProduct_OppositeVectors_ReturnsNegative` | [1,0] · [-1,0] = -1 | -1.0 |
| A6 | `VectorDistance_Euclidean_IdenticalVectors_ReturnsZero` | Distance to self = 0 | 0.0 |
| A7 | `VectorDistance_Euclidean_KnownTriangle_345` | [0,0] vs [3,4] = 5.0 | 5.0 |
| A8 | `VectorDistance_Cosine_SingleDimension_HandlesCorrectly` | 1D vectors [5] vs [3] | 1.0 (both positive = parallel) |
| A9 | `VectorDistance_DotProduct_ZeroVector_ReturnsZero` | Dot product with zero vector | 0.0 |
| A10 | `VectorDistance_Euclidean_ZeroVector_ReturnsVectorMagnitude` | Euclidean([3,4,0], [0,0,0]) = 5 | 5.0 |

### Category B: Parameter Handling & Overloads

| ID | Test | Why | Expected |
|----|------|-----|----------|
| B1 | `VectorDistance_TwoArgsOnly_DefaultsToCosine` | Minimal 2-arg call | Cosine result |
| B2 | `VectorDistance_ThreeArgs_BruteForce_False` | 3 args with false | Same as without |
| B3 | `VectorDistance_FourArgs_DotProduct` | Full 4-arg dotproduct | Dot product result |
| B4 | `VectorDistance_FourArgs_Cosine_Explicit` | Explicitly specify cosine in 4th arg | Same as default |
| B5 | `VectorDistance_OptionsWithDataType_AcceptedSilently` | `{distanceFunction:'cosine', dataType:'Float32'}` | Cosine result (dataType ignored) |
| B6 | `VectorDistance_OptionsWithSearchListSizeMultiplier_AcceptedSilently` | `{distanceFunction:'cosine', searchListSizeMultiplier:10}` | Cosine result |
| B7 | `VectorDistance_OptionsWithFilterPriority_AcceptedSilently` | `{distanceFunction:'cosine', filterPriority:0.5}` | Cosine result |
| B8 | `VectorDistance_UnknownDistanceFunction_FallsToCosine` | `{distanceFunction:'manhattan'}` — fallback behaviour | Cosine result |
| B9 | `VectorDistance_CaseInsensitiveDistanceFunction` | `{distanceFunction:'COSINE'}` vs `'Cosine'` vs `'cosine'` | All produce same result |
| B10 | `VectorDistance_OneArg_ReturnsNull` | Only 1 vector arg | null |

### Category C: Data Type & Input Edge Cases

| ID | Test | Why | Expected |
|----|------|-----|----------|
| C1 | `VectorDistance_IntegerVectorValues_WorkCorrectly` | Vector with integer-only values [1, 2, 3] | Valid score |
| C2 | `VectorDistance_MixedIntAndFloatValues_WorkCorrectly` | [1, 2.5, 3] | Valid score |
| C3 | `VectorDistance_EmptyVector_ReturnsNull` | [] vs [] — both empty | null (length 0 check) |
| C4 | `VectorDistance_VeryLargeValues_NoOverflow` | [1e38, 1e38] — near float max | Valid or null, shouldn't throw |
| C5 | `VectorDistance_VerySmallValues_NoPrecisionLoss` | [1e-38, 1e-38] | Valid score near 1.0 |
| C6 | `VectorDistance_NullVectorProperty_ReturnsNull` | Doc has `embedding: null` | null |
| C7 | `VectorDistance_NonArrayVectorProperty_ReturnsNull` | Doc has `embedding: "not-a-vector"` | null |
| C8 | `VectorDistance_NestedProperty_WorksCorrectly` | `c.metadata.embedding` path | Valid score |
| C9 | `VectorDistance_VectorWithNonNumericElement_ReturnsNull` | `[1.0, "abc", 3.0]` | null |
| C10 | `VectorDistance_TwoDimensional_WorksCorrectly` | 2D vectors [1,0] vs [0,1] | 0.0 cosine |

### Category D: SQL Integration (SELECT, WHERE, ORDER BY, GROUP BY)

| ID | Test | Why | Expected |
|----|------|-----|----------|
| D1 | `VectorDistance_InWhereClause_FiltersBySimilarityThreshold` | `WHERE VectorDistance(...) > 0.5` | Only similar docs |
| D2 | `VectorDistance_InWhereAndSelect_DifferentVectors` | WHERE uses one query vec, SELECT another | Different scores |
| D3 | `VectorDistance_OrderByAsc_LeastSimilarFirst` | ORDER BY ... ASC | Ascending scores |
| D4 | `VectorDistance_WithOffsetLimit_Paginated` | OFFSET 1 LIMIT 2 with ORDER BY | Middle 2 results |
| D5 | `VectorDistance_InSubquery_Works` | Subquery with VectorDistance | Valid results |
| D6 | `VectorDistance_AliasedInOrderBy_Works` | `SELECT ... AS score ... ORDER BY score` — tests alias in ORDER BY | Correct ordering |
| D7 | `VectorDistance_MultipleCallsInSameQuery_Works` | Two VectorDistance calls with different vectors | Both scores computed |
| D8 | `VectorDistance_WithDistinct_WorksCorrectly` | `SELECT DISTINCT VectorDistance(...)` | Unique scores only |
| D9 | `VectorDistance_CrossPartition_WithoutPartitionKey` | Query without PK filter | Results from all partitions |
| D10 | `VectorDistance_WithGroupBy_AggregatesCorrectly` | `GROUP BY c.category` + AVG(VectorDistance) | Group aggregates |

### Category E: Multi-Document / Ranking Scenarios

| ID | Test | Why | Expected |
|----|------|-----|----------|
| E1 | `VectorDistance_KNN_Top5_ReturnsCorrectNearest` | Classic k-NN use case with 20 docs | Top 5 nearest |
| E2 | `VectorDistance_TiedScores_ReturnedStably` | Multiple docs with same similarity | All returned, stable |
| E3 | `VectorDistance_AllDocsHaveSameEmbedding_SameScore` | Degenerate case: all identical | All scores equal |
| E4 | `VectorDistance_LargeDataset_100Docs_OrderByCorrect` | Scaling test | Correct top-K |
| E5 | `VectorDistance_MixOfValidAndMissingEmbeddings_NullsHandled` | Some docs have embedding, some don't | Nulls for missing, scores for present |

### Category F: Divergent Behaviour Tests (Skip + Sister)

These test real Cosmos DB behaviour that is too complex or not meaningful to implement in the emulator.

| ID | Test (Skipped) | Sister Test (Passing) | Why skipped |
|----|----------------|----------------------|-------------|
| F1 | `VectorDistance_MultiDimensionalArray_RealCosmosReturnsNoScore` | `VectorDistance_MultiDimensionalArray_EmulatorReturnsNull` | MS docs: "multi-dimensional array → no SimilarityScore, no error". Emulator returns null which is close enough but not exactly the same (Cosmos omits the property entirely). |
| F2 | `VectorDistance_RequiresVectorPolicy_InRealCosmos` | `VectorDistance_EmulatorDoesNotRequireVectorPolicy` | Real Cosmos requires `vectorEmbeddings` container policy. Emulator has no such concept — always brute-force. |
| F3 | `VectorDistance_FlatIndexMax505Dimensions_InRealCosmos` | `VectorDistance_EmulatorSupportsAnyDimensionality` | Flat index limited to 505 dims. Emulator has no index limits. |
| F4 | `VectorDistance_RequiresTopNWithOrderBy_InRealCosmos` | `VectorDistance_EmulatorAllowsOrderByWithoutTopN` | MS docs: "Always use TOP N in SELECT with vector ORDER BY". Emulator doesn't enforce this. |

---

## Implementation Plan (TDD Order)

### Phase 1: Bug Fixes (red-green-refactor)

1. **Write test A1** (`Cosine_ZeroVector_ReturnsNull`) — should fail (returns 0 not null)
2. **Fix BUG-1** in `CosineSimilarity` — return null when denominator == 0
   - Need to change return type or use a sentinel; since `VectorDistanceFunc` returns `object`, change `CosineSimilarity` to return `object` or use `double?`, and return null for zero denominator
3. **Verify test A1 passes**
4. **Run all existing tests** — verify no regressions (tests 1-15 all use non-zero vectors, should be fine)

### Phase 2: Mathematical Edge Cases (A2-A10)

Write each test first, verify it passes (these should mostly be green already since the math is correct), add any that fail to the fix queue.

### Phase 3: Parameter Handling (B1-B10)

Write each test, verify passing. B8 (unknown distance function) and B10 (one arg) should pass based on current implementation.

### Phase 4: Data Type Edge Cases (C1-C10)

Write tests. C4 (very large values) may reveal overflow issues. C8 (nested property) depends on how property paths are resolved in the query engine.

### Phase 5: SQL Integration (D1-D10)

Write tests exercising VectorDistance in various SQL contexts. These may reveal parser or evaluation limitations.

### Phase 6: Multi-Document Ranking (E1-E5)

Write scaling and ranking tests.

### Phase 7: Divergent Behaviour Tests (F1-F4)

Write skipped tests with detailed skip reasons documenting real Cosmos behaviour, plus sister tests showing emulator behaviour.

### Phase 8: Documentation Updates

1. **Wiki Known-Limitations.md** — No new limitations expected (vector search is already documented as brute-force only)
2. **Wiki Features.md** — Update vector search section if any new capabilities are added
3. **Wiki Feature-Comparison-With-Alternatives.md** — Already shows ✅ for vector search; update if coverage changes
4. **Wiki SQL-Queries.md** — Update vector functions table if behaviour changes (e.g., zero-vector → null)
5. **README.md** — Already mentions vector search; update if scope changes
6. **Wiki Known-Limitations.md** — Add BUG-1 zero-vector fix note if behaviour changes

### Phase 9: Version, Tag, Push

1. Increment version in `src/CosmosDB.InMemoryEmulator/CosmosDB.InMemoryEmulator.csproj` from `2.0.4` to `2.0.5`
2. `git add -A`
3. `git commit -m "v2.0.5: Expand vector search test coverage, fix zero-vector cosine bug"`
4. `git tag v2.0.5`
5. `git push; git push --tags`
6. Wiki: `git add -A; git commit -m "v2.0.5: Vector search documentation updates"; git push`

---

## Test Count Summary

| Category | New Tests | Bug Fixes |
|----------|-----------|-----------|
| A: Math edge cases | 10 | 1 (zero-vector) |
| B: Parameter handling | 10 | 0 |
| C: Data type edge cases | 10 | 0 (possibly overflow) |
| D: SQL integration | 10 | 0 (possibly parser) |
| E: Multi-doc ranking | 5 | 0 |
| F: Divergent (skip + sister) | 8 (4 skipped + 4 passing) | 0 |
| **Total** | **53** | **1** |

---

## Progress Tracking

| Phase | Status |
|-------|--------|
| Phase 1: Bug fixes | ✅ Done — BUG-1 fixed (zero vector → null), parser scientific notation fixed |
| Phase 2: Math edge cases | ✅ Done — all 10 tests pass |
| Phase 3: Parameter handling | ✅ Done — all 10 tests pass (incl. 4 Theory variants) |
| Phase 4: Data type edge cases | ✅ Done — all 10 tests pass |
| Phase 5: SQL integration | ✅ Done — 9 pass, D10 (GROUP BY + AVG(VectorDistance)) skipped with sister |
| Phase 6: Multi-doc ranking | ✅ Done — all 5 tests pass |
| Phase 7: Divergent behaviour | ✅ Done — 4 skipped + 5 sister tests pass |
| Phase 8: Documentation | ⬜ In progress |
| Phase 9: Version/tag/push | ⬜ Not started |

### Additional Bugs Found & Fixed During Implementation

**BUG-2 (NEW): SQL parser doesn't support scientific notation (1e38, 1e-10)**
- `Numerics.Decimal` only parses basic decimals, not `1e38` or `1e-10`
- Fixed by extending `NumberToken` to support optional `[eE][+-]?digits` suffix
- Tests C4 and C5 now pass

**D10 → Skipped**: GROUP BY with AVG(VectorDistance(...)) throws because `ExtractNumericValues` 
uses function call text as a JSON path. This is a general GROUP BY limitation, not vector-specific.

### Final Test Count: 71 total (66 passed, 5 skipped)
