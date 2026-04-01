# Computed Properties — TDD Implementation Plan

## Feature Summary
Cosmos DB computed properties are virtual top-level properties defined on a container with a `Name` and a `Query` (e.g., `SELECT VALUE LOWER(c.name) FROM c`). They are not persisted on documents — instead, queries can reference them like regular properties in SELECT, WHERE, ORDER BY, and GROUP BY. `SELECT *` does NOT include computed properties.

## SDK Support
`ContainerProperties.ComputedProperties` is available in Microsoft.Azure.Cosmos 3.58.0 (already referenced).

## Approach
- Hook into `FilterItemsByQuery` right after `GetAllItemsForPartition()`.
- When `_containerProperties.ComputedProperties?.Count > 0`, augment each item's JSON string with the computed property values. This is a single-pass `List<string>` → `List<string>` transform.
- Cache parsed computed property queries (`ConcurrentDictionary<string, (string FromAlias, SqlExpression Expr)>`) to avoid re-parsing on every query.
- Invalidate cache on `ReplaceContainerAsync`.
- For users without computed properties: zero overhead (branch never entered).

## Test Plan

### ✅ Tests (Expected to pass after implementation)

| # | Test Name | Description |
|---|-----------|-------------|
| 1 | `ComputedProperty_ProjectedInSelect` | Define `cp_lowerName = SELECT VALUE LOWER(c.name) FROM c`, query `SELECT c.cp_lowerName FROM c`, verify lowercase result |
| 2 | `ComputedProperty_UsedInWhereClause` | Filter `WHERE c.cp_lowerName = "alice"` matches item with name "Alice" |
| 3 | `ComputedProperty_UsedInOrderBy` | ORDER BY computed property sorts correctly |
| 4 | `ComputedProperty_UsedInGroupBy` | GROUP BY computed property groups correctly |
| 5 | `ComputedProperty_NotIncludedInSelectStar` | `SELECT * FROM c` does NOT include computed properties |
| 6 | `ComputedProperty_ExplicitSelectAlongsideStar` | `SELECT *, c.cp_lowerName FROM c` includes both persisted and computed |
| 7 | `ComputedProperty_EvaluatesToNull_WhenSourceMissing` | Computed property based on missing field evaluates to null/undefined |
| 8 | `ComputedProperty_MultipleOnSameContainer` | Two computed properties on same container both work |
| 9 | `ComputedProperty_UpdatedViaReplaceContainer` | Update computed properties via ReplaceContainerAsync, new definitions take effect |
| 10 | `ComputedProperty_ArithmeticExpression` | `SELECT VALUE c.price * 0.8 FROM c` computes discount |
| 11 | `ComputedProperty_ConcatExpression` | `SELECT VALUE CONCAT(c.first, ' ', c.last) FROM c` |
| 12 | `ComputedProperty_WithPartitionKeyFilter` | Works correctly with partition-scoped queries |
| 13 | `ComputedProperty_NoComputedProperties_ZeroOverhead` | Baseline — container without computed properties works as before (no regression) |
| 14 | `ComputedProperty_StoredAndReturnedOnContainerRead` | `ReadContainerAsync` returns the computed properties that were set |
| 15 | `ComputedProperty_DoesNotPersistOnDocument` | After creating computed props and items, reading item directly doesn't include computed property |

### ⏭️ Skipped Tests (with divergent behaviour sisters)

| # | Skip Reason | Sister Test |
|---|-------------|-------------|
| — | None anticipated — computed property evaluation uses existing SQL expression engine which already handles all the function types | — |

## Implementation Steps

1. [x] Write TDD plan (this file)
2. [x] Write all RED tests
3. [x] Run tests — confirm RED (11 failed, 4 passed)
4. [x] Implement: add computed property augmentation in `FilterItemsByQuery`
5. [x] Implement: cache parsed computed property queries
6. [x] Implement: invalidate cache on `ReplaceContainerAsync`
7. [x] Run tests — confirm GREEN (15 passed, 0 failed)
8. [x] Refactor — no changes needed, implementation is clean
9. [x] Update wiki Known-Limitations (no change needed — wasn't listed as limitation)
10. [x] Update wiki Features page
11. [x] Update wiki Feature-Comparison-With-Alternatives
12. [x] Update README features list
13. [x] Version bump to 2.0.2, tag, push

## Key Files
- `src/CosmosDB.InMemoryEmulator/InMemoryContainer.cs` — main implementation
- `tests/CosmosDB.InMemoryEmulator.Tests/ComputedPropertyTests.cs` — new test file
- Wiki: Known-Limitations.md, Features.md, Feature-Comparison-With-Alternatives.md
- README.md
- src/CosmosDB.InMemoryEmulator/CosmosDB.InMemoryEmulator.csproj (version bump)
