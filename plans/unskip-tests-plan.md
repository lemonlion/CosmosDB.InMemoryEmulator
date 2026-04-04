# Unskip Tests Execution Plan

## Goal
Get as many skipped tests passing as possible by fixing the InMemoryEmulator to correctly emulate real Cosmos DB behavior. Make the emulator as close to real Cosmos DB as possible.

## Instructions (for the executing agent)
1. Work through batches in order. Each batch targets ~20 unskipped tests.
2. For each test: read the test body, understand what real Cosmos does, fix the emulator source, remove the `Skip`, run the test.
3. If a test is too hard to fix, don't delete it — update the Skip reason with extra notes on why it was too difficult.
4. **Don't delete any sister divergent behavior tests** until AFTER the relevant previously-skipped test is passing successfully.
5. After each batch (~20 tests unskipped), increment the patch version, commit, tag, push, and **update the wiki Known-Limitations page** (remove limitations that are now fixed, add any new notes).
6. **Update this plan file** as you go: mark batches as COMPLETED with test counts and version numbers.
7. Run the full test suite on both net8.0 and net10.0 before each push.
8. Current version: **2.0.52**. Current passing: ~4260 passed, ~232 skipped.

## Exclusions (not worth emulating)
These categories are inherently impossible or meaningless to emulate in-memory:
- **Authentication/encryption**: Azure Key Vault, client encryption keys, HMAC-signed permission tokens
- **User management**: Already excluded per user request
- **RU charge calculation**: Would need Cosmos cost model (always 1.0 is fine)
- **Session token LSN format**: Real format is `0:-1#12345` — synthetic is acceptable
- **Diagnostics timing**: Real diagnostics have latency/retries/endpoints — mock is acceptable
- **Consistency levels**: Would need multi-replica simulation
- **Analytical store (Synapse Link)**: Entirely separate subsystem
- **Continuation token format**: Real is opaque base64 JSON — integer offset is acceptable

---

## Batch 1: Easy Validation & Error Handling (~20 tests) — COMPLETED
**Version: v2.0.53 — 17 unskipped, 3 kept skipped (NaN vector, GetDatabase 404, GetContainer 404)**

| # | File | Line | Test | Fix |
|---|------|------|------|-----|
| 1 | PartitionKeyTests | 1018 | Create_ExplicitPK_DiffersFromBody_Returns400 | Add PK body vs explicit comparison in CreateItemAsync |
| 2 | PartitionKeyTests | 1045 | Upsert_ExplicitPK_DiffersFromBody_Returns400 | Same for UpsertItemAsync |
| 3 | PartitionKeyTests | 1070 | Patch_ModifiesPK_Returns400 | Check if patch targets PK path |
| 4 | PatchItemTests | 1749 | Patch_TargetsId_Returns400 | Reject /id in PatchItemAsync |
| 5 | TtlTests | 1901 | DefaultTimeToLive_Zero_Returns400 | Validate TTL=0 → 400 |
| 6 | TtlTests | 1923 | ItemTtl_Zero_Returns400 | Validate _ttl=0 → 400 |
| 7 | SkippedBehaviorTests | 987 | MaxItemCount_Zero_Returns400 | Validate MaxItemCount=0 → 400 |
| 8 | ContainerManagementTests | 999 | Replace_ChangesPK_Returns400 | Reject PK change in ReplaceContainerAsync |
| 9 | ContainerManagementTests | 1520 | Replace_ChangesUniqueKeyPolicy_Returns400 | Reject UKP change |
| 10 | TransactionalBatchTests | 2067 | Batch_PKMismatch_Returns400 | Add PK mismatch check in batch |
| 11 | StoredProcedureTests | 875 | Execute_NonExistentSproc_Returns404 | Check sproc existence before execute |
| 12 | ComputedPropertyTests | 1677 | Max20ComputedProperties_Enforced | Add CP count validation |
| 13 | ComputedPropertyTests | 1696 | RejectSystemPropertyCPNames | Validate CP name against reserved names |
| 14 | VectorSearchTests | 1774 | VectorDistance_TooManyArgs_Rejects | Validate arg count |
| 15 | VectorSearchTests | 1791 | VectorDistance_UnknownDistanceFunction_Rejects | Validate distance function name |
| 16 | VectorSearchTests | 1875 | NaN_InVectorField_Rejected | Validate vector values |
| 17 | InMemoryCosmosClientTests | 487 | Dispose_ThenOperate_ThrowsObjectDisposed | Track disposal state |
| 18 | InMemoryCosmosClientTests | 624 | GetDatabase_Read_NonExistent_Returns404 | Check existence, not auto-create |
| 19 | InMemoryCosmosClientTests | 649 | GetContainer_Read_NonExistent_Returns404 | Check existence |
| 20 | ContainerManagementTests | 1691 | DeleteContainer_CascadesTriggers | Clean up triggers/UDFs on delete |

## Batch 2: More Validation + Query/Parser Fixes (~20 tests) — NOT STARTED
**Target: v2.0.54**

| # | File | Line | Test | Fix |
|---|------|------|------|-----|
| 1 | ComputedPropertyTests | 1719 | CP_MustUseSelectValue | Parse/validate CP query |
| 2 | ComputedPropertyTests | 1743 | CP_RejectWhereOrderBy | Validate CP query clauses |
| 3 | ComputedPropertyTests | 1770 | CP_CantReferenceOtherCP | Validate CP dependencies |
| 4 | ComputedPropertyTests | 923 | Patch_TargetingCP_Rejected | Validate patch paths vs CP |
| 5 | QueryTests | 4591 | CountStar_Parsed | Add COUNT(*) parsing |
| 6 | QueryTests | 4562 | TypeOrdering_InOrderBy | Implement type rank in ORDER BY |
| 7 | SkippedBehaviorTests | 1011 | TypeOrdering_UndefinedNullBoolNumStr | Same type rank in ORDER BY |
| 8 | DivergentBehaviorTests | 132 | Undefined_Null_Distinguished_OrderBy | Separate undefined vs null sorting |
| 9 | IndexSimulationTests | 937 | TypeRank_InSorting | Same type rank enforcement |
| 10 | StringCaseSensitivityTests | 1059 | ArrayContains_CaseSensitive | Fix ARRAY_CONTAINS comparison |
| 11 | StringCaseSensitivityTests | 1158 | GroupBy_FunctionExpression | Add function-in-GROUP-BY support |
| 12 | FullTextSearchTests | 630 | FT_IndexPolicy_Validation | Validate FT index config |
| 13 | FullTextSearchTests | 666 | FullTextScore_InvalidInProjection | Validate FTS usage context |
| 14 | ReadManyTests | 682 | IfNoneMatchEtag_Honored | Add ETag matching |
| 15 | ReadManyTests | 779 | ResponseHeaders_Populated | Populate headers |
| 16 | PartitionKeyTests | 1203 | DeleteTombstone_PipeInPK | Fix pipe delimiter bug |
| 17 | PartitionKeyTests | 1241 | NullPKValue_NotFallback | Return null not empty string |
| 18 | DivergentBehaviorTests | 544 | UnsupportedSQL_ThrowsCosmosException | Wrap in CosmosException(400) |
| 19 | DivergentBehaviorTests | 575 | ParseFailure_ThrowsCosmosException | Wrap in CosmosException(400) |
| 20 | ContainerManagementTests | 2037 | Container_Database_ReturnsSameInstance | Cache Database instance |

## Batch 3: Feature Completeness (~20 tests) — NOT STARTED
**Target: v2.0.55**

| # | File | Line | Test | Fix |
|---|------|------|------|-----|
| 1 | ComputedPropertyTests | 770 | Undefined_PropagatesThrough_Functions | UndefinedValue in LOWER/UPPER |
| 2 | ComputedPropertyTests | 844 | SelectCStar_IdenticalToSelectStar | `c.*` → same as `*` in parser |
| 3 | ComputedPropertyTests | 884 | CONCAT_UndefinedArg_ReturnsUndefined | Undefined propagation in CONCAT |
| 4 | ComputedPropertyTests | 1498 | ArrayContains_OnComputedProperty | CP returning arrays |
| 5 | ExtendedArrayFunctionTests | 1728 | SetIntersect_NonArray_ReturnsUndefined | Type check inputs |
| 6 | ExtendedArrayFunctionTests | 1752 | SetUnion_NonArray_ReturnsUndefined | Type check inputs |
| 7 | ExtendedArrayFunctionTests | 1775 | SetDifference_NonArray_ReturnsUndefined | Type check inputs |
| 8 | ExtendedArrayFunctionTests | 1702 | ArrayContainsAny_SecondArgScalar | Strict arg validation |
| 9 | DivergentBehaviorTests | 95 | GetCurrentDateTime_PerQuery | Evaluate once per query |
| 10 | ReadManyTests | 656 | CancellationToken_Honored | Add cancellation support |
| 11 | StreamCrudTests | 1569 | ErrorMessage_OnFailure | Set ErrorMessage property |
| 12 | QueryTests | 4616 | GroupBy_OrderBy_AggAlias | Parser support |
| 13 | DocumentSizeLimitTests | 327 | SizeValidation_PreTrigger | Validate after trigger |
| 14 | DocumentSizeLimitTests | 1323 | SizeValidation_PostTrigger | Validate after post-trigger |
| 15 | TtlTests | 1945 | Queries_EvictExpiredItems | Eviction on query |
| 16 | SkippedBehaviorTests | 1043 | TTL_Eviction_Timing | Same eviction behavior |
| 17 | ChangeFeedTests | 3758 | TTL_Eviction_ChangeFeed | TTL + change feed |
| 18 | SkippedBehaviorTests | 896 | ConflictResolution_Enforced | Basic conflict resolution |
| 19 | TtlTests | 946 | TTL_AllVersionsDeletes_ChangeFeed | TTL in AVAD mode |
| 20 | PointInTimeRestoreTests | 675 | ChangeFeed_RollbackOnFailedBatch | Failed batch change feed |

## Batch 4: LINQ, WAF, and Advanced Features (~20 tests) — NOT STARTED
**Target: v2.0.56**

| # | File | Line | Test | Fix |
|---|------|------|------|-----|
| 1 | LinqToFeedIteratorTests | 1269 | AllowSyncQuery_Enforcement | Check flag |
| 2 | LinqToFeedIteratorTests | 1288 | ContinuationToken_OnLinq | Pass through token |
| 3 | LinqToFeedIteratorTests | 1308 | Linq_ParameterPassing | LINQ setup params |
| 4 | DivergentBehaviorTests | 114 | LinqSerializerOptions_Honored | Pass through option |
| 5 | VectorSearchTests | 1812 | Parameterized_QueryVectors | @param support |
| 6 | VectorSearchTests | 872 | GroupBy_Avg_VectorDistance | GROUP BY + AVG |
| 7 | VectorSearchTests | 1044 | SimilarityScore_MultiDim_Omitted | Multi-dim score behavior |
| 8 | VectorSearchTests | 1081 | VectorEmbeddings_PolicyRequired | Policy validation |
| 9 | VectorSearchTests | 1114 | FlatIndex_505DimLimit | Dimension validation |
| 10 | VectorSearchTests | 1150 | TopN_WithOrderBy_Warning | Suggest TOP N |
| 11 | TriggerTests | 1103 | Triggers_FireOnPatch | Wire triggers into PatchItemAsync |
| 12 | TriggerTests | 1115 | ChangeFeed_PostTrigger_Rollback | Rollback trigger logic |
| 13 | JsTriggerTests | 2874 | JsTriggers_FireOnPatch | Same for JS |
| 14 | JsTriggerTests | 2924 | ChangeFeed_CommittedOnly | Change feed only after commit |
| 15 | WebApplicationFactoryIntegrationTests | 1811 | FakeCosmosHandler_DatabasePlusContainer | Route by db+container |
| 16 | WebApplicationFactoryIntegrationTests | 1873 | AddContainer_ContainerProperties | Expose properties |
| 17 | WebApplicationFactoryIntegrationTests | 1878 | DeleteContainer_RemovesFromRouting | Clean up routing |
| 18 | ContainerManagementTests | 1980 | Management_CreatesIfMissing | Proper 404 handling |
| 19 | FakeCosmosHandlerTests | 924 | GroupBy_NonWindows | Fix platform issue |
| 20 | DivergentBehaviorTests | 75 | ArrayFunctions_LiteralArrays | Parser support |

## Batch 5: Remaining Achievable Tests (~20 tests) — NOT STARTED
**Target: v2.0.57**

| # | File | Line | Test | Fix |
|---|------|------|------|-----|
| 1 | CosmosClientAndDatabaseTests | 2623+ | Database name validation (multiple tests) | Add char validation |
| 2-15 | CosmosClientAndDatabaseTests | 2623-3185 | ~14 database stream/naming tests | Validate + implement |
| 16 | DocumentSizeLimitTests | 495 | DeleteMetadata_CountsTowardBatch | Better size accounting |
| 17 | DocumentSizeLimitTests | 526 | ReadMetadata_CountsTowardBatch | Better size accounting |
| 18 | FeedRangeFilteringTests | 1401 | FeedRange_FromPartitionKey | PK-to-range mapping |
| 19 | FeedRangeFilteringTests | 1473 | ChangeFeedStream_EagerEval | Lazy evaluation |
| 20 | FeedRangeTests | 967 | StreamIterator_ItemsAfterCreation | Same lazy eval |

## Tests to Leave Skipped (with updated notes)
These ~100 tests should remain skipped with enhanced explanations:

### Authentication/Encryption (~4 tests)
- CosmosClientAndDatabaseTests: 1182, 1191, 2014 — Azure Key Vault; not meaningful for in-memory
- DivergentBehaviorTests: 630 — HMAC-signed permission tokens

### RU/Diagnostics/Session Tokens (~20 tests)
- All RequestCharge/Diagnostics/SessionToken tests across BehavioralDifferenceTests, ResponseMetadataTests, SkippedBehaviorTests, SdkCompatibilityTests, TransactionalBatchTests
- Inherently synthetic; real cost model not emulatable

### Continuation Token Format (~5 tests)
- DivergentBehaviorTests: 342, QueryTests: 3266, SkippedBehaviorTests: 959, ResponseMetadataTests: 1696, CosmosClientAndDatabaseTests: 2956
- Integer offsets are acceptable; opaque base64 would be cosmetic

### System Properties _rid/_self/_attachments (~8 tests)
- BehavioralDifferenceTests: 379, 396, 413; CrudTests: 2346, 2365; StreamCrudTests: 1037
- Design decision: not generating these is acceptable

### Indexing Policy Enforcement (~6 tests)
- IndexSimulationTests: 355, 493, 624, 1274, 1317, 1386; DivergentBehaviorTests: 401
- Full index enforcement would need B-tree/inverted index simulation

### JavaScript Engine Limitations (~12 tests)
- StoredProcedureTests: 906, 948, 963, 994, 1006, 1018, 1054, 1066, 1077, 1092
- JsTriggerTests: 1422, 1488, 1548, 1621, 1677, 2648, 2670, 2827, 2847, 2999
- Would need full Jint integration; C# handlers are the supported pattern

### Change Feed Architecture (~5 tests)
- ChangeFeedTests: 872, 2104, 2274, 3666, 3703, 1931
- ContainerInternal casting, internal APIs, all-versions-and-deletes typed API

### WAF/Integration Architecture (~6 tests)
- WebApplicationFactoryIntegrationTests: 1174, 1227, 1773, 1862, 1867, 1883, 1888, 1892
- Deep integration patterns not worth reworking

### Other (~15 tests)
- BulkOperationTests: 1397, 1513 — synchronous architecture
- ConcurrencyTests: 1589, 1637 — fundamental isolation model
- DivergentBehaviorTests: 276, 308, 481, 492, 524, 790 — inherent emulator limitations
- FakeCosmosHandlerTests: 1993, 2026 — undocumented SDK wrapping format
- QueryPlanTests: 806, 1066 — query plan metadata
- FullTextSearchTests: 701, 1931 — RRF algorithm, text analysis
- RealToFeedIteratorTests: 1375, 2030, 2134, 2639 — FakeCosmosHandler parser limits
- LinqToFeedIteratorTests: 1192, 1213, 1231, 1251 — LINQ operators Cosmos SDK doesn't translate

---

## Progress Log
| Batch | Version | Tests Unskipped | Tests Passed | Tests Skipped | Status |
|-------|---------|-----------------|--------------|---------------|--------|
| 1 | 2.0.53 | 17 | 4255 | 215 | COMPLETED |
| 2 | 2.0.54 | | | | NOT STARTED |
| 3 | 2.0.55 | | | | NOT STARTED |
| 4 | 2.0.56 | | | | NOT STARTED |
| 5 | 2.0.57 | | | | NOT STARTED |
