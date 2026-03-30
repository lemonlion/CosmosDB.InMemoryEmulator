# Known Limitations & Behavioural Differences

This document lists all known areas where the in-memory emulator differs from real Azure Cosmos DB. These are documented so you can make informed decisions about which behaviours matter for your tests.

## Limitations

| Area | Status | Notes |
|------|--------|-------|
| Deletes in change feed | ❌ Not recorded | Deletions don't appear in the change feed |
| Spatial functions | ⚠️ Stub | `ST_DISTANCE`, `ST_WITHIN`, etc. return synthetic values |
| Cross-partition transactions | ❌ Not supported | Batches are single-partition only (matches real Cosmos DB) |
| Analytical store (Synapse) | ❌ Not simulated | OLAP context not available |
| Continuous backup / PITR | ❌ Not simulated | Point-in-time restore not available |
| IndexingPolicy | ⚠️ Stub | Accepted and stored but doesn't affect query performance |
| TTL eviction | ⚠️ Lazy | Expired items removed on next read, not proactively |
| Resource IDs | ⚠️ Synthetic | Valid format but doesn't match real Cosmos RIDs |
| Throughput (RU/s) | ⚠️ Synthetic | Returns 400 RU/s; doesn't affect behaviour |
| `FeedRange` filtering | ⚠️ Not implemented | Accepted but currently ignored |
| `AllVersionsAndDeletes` mode | ❌ Not supported | Only `Incremental` mode is supported |
| ChangeFeed stream handler | ⚠️ NoOp | `ChangeFeedStreamHandler` variant builds but never invokes the handler |
| `ReplaceContainerStreamAsync` | ⚠️ Does not persist | Returns OK but does not update internal container state |
| Users / permissions | ❌ Not implemented | Throws `NotImplementedException` |
| Client encryption keys | ❌ Not implemented | Throws `NotImplementedException` |

---

## Behavioural Differences

These are areas where the emulator produces different results from real Cosmos DB. Each has a corresponding test documenting the difference.

### 1. Undefined vs Null Fields

**Real Cosmos DB:** A missing field is `undefined`, which is NOT equal to `null`. `WHERE c.status = null` does NOT match a document without a `status` field.

**InMemoryContainer:** Missing fields are treated as `null`. `WHERE c.status = null` WILL match a document without a `status` field.

**Impact:** Low for most applications. Only affects queries that use `= null` on fields that may be absent.

**Test:** `QueryUndefinedFieldDivergentBehaviorTests.Query_MissingField_MatchesNull_InMemory`

---

### 2. Batch ReadItem Result Resource

**Real Cosmos DB:** `GetOperationResultAtIndex<T>().Resource` returns the document data after a batch `ReadItem`.

**InMemoryContainer:** Returns `null` for `Resource` on batch read results. Status code is `0` instead of `200`.

**Impact:** Medium if your code reads items via transactional batch. Use individual `ReadItemAsync` instead.

**Test:** `BatchReadResultDivergentBehaviorTests.Batch_ReadResult_HasNullResource_InMemory`

---

### 3. Partition Key Fallback (Missing Field)

**Real Cosmos DB:** When `PartitionKey.None` is used and the partition key field is missing from the document, the document is stored with a system-defined fallback PK.

**InMemoryContainer:** Falls back to the `id` field as the PK value when the PK path field is missing.

**Impact:** Low. Only affects documents missing their partition key field entirely.

**Test:** `PartitionKeyFallbackDivergentBehaviorTests.PartitionKey_None_WithMissingPkField_Succeeds_InMemory`

---

### 4. Change Feed Delete Tombstones

**Real Cosmos DB:** In `AllVersionsAndDeletes` mode, deletes appear as tombstone entries in the change feed.

**InMemoryContainer:** Deletes are never recorded in the change feed regardless of mode.

**Impact:** Medium if your code processes delete events from the change feed. Use real Cosmos DB or the official emulator for testing delete-aware change feed processing.

**Test:** `ChangeFeedModeDivergentBehaviorTests.ChangeFeed_DeletesNotInFeed_EvenInFullFidelity`

---

### 5. FeedRange Count

**Real Cosmos DB:** Returns multiple `FeedRange` instances based on physical partition distribution.

**InMemoryContainer:** Always returns exactly 1 `FeedRange` regardless of data volume.

**Impact:** Low. Only affects code that explicitly parallelises across feed ranges. For multi-range simulation, use `FakeCosmosHandler` with `PartitionKeyRangeCount > 1`.

**Test:** `FeedRangeDivergentBehaviorTests.GetFeedRanges_AlwaysReturnsSingle_RegardlessOfData`

---

### 6. Incremental Change Feed (Multiple Updates)

**Real Cosmos DB (Incremental mode):** Returns only the latest version of each item, but the timing and batching of "latest" can vary based on physical partition distribution.

**InMemoryContainer:** Strictly returns only the latest version per item across all updates.

**Impact:** None for most applications. The in-memory behaviour is actually more predictable.

---

### 7. ChangeFeed Stream Handler

**Real Cosmos DB:** `GetChangeFeedProcessorBuilder` with a `ChangeFeedStreamHandler` delegate invokes the handler with raw `Stream` data when changes are detected, enabling low-level change processing without deserialization.

**InMemoryContainer:** The `ChangeFeedStreamHandler` overload uses a `NoOpChangeFeedProcessor` internally. The processor can be built and started/stopped but the handler is never invoked. Use the typed `ChangeFeedHandler<T>` overload for functional in-memory change feed processing.

**Impact:** Medium if your code uses raw stream processing for the change feed. Use the typed handler overload instead.

**Test:** `ChangeFeedStreamProcessorDivergentTests5.ChangeFeedStreamHandler_IsNoOp_InMemory`

---

### 8. ReplaceContainerStreamAsync Does Not Persist

**Real Cosmos DB:** `ReplaceContainerStreamAsync` updates the container's actual properties (partition key, indexing policy, etc.) and subsequent `ReadContainerAsync` returns the new values.

**InMemoryContainer:** Returns OK with the supplied properties in the response body but does not persist changes internally. Subsequent `ReadContainerAsync` returns the original properties. Same behaviour as the non-stream `ReplaceContainerAsync`.

**Impact:** Low. Only affects tests that modify then re-read container properties.

**Test:** `ContainerStreamDivergentBehaviorTests5.ReplaceContainerStream_DoesNotPersistChanges`

---

## Geospatial Functions

The emulator implements `ST_DISTANCE`, `ST_WITHIN`, `ST_INTERSECTS`, `ST_ISVALID`, and `ST_ISVALIDDETAILED` as stubs:

- `ST_DISTANCE` uses a simplified haversine calculation
- Other functions return synthetic boolean results

For precision-critical geospatial testing, use the real Cosmos DB or the official emulator.

---

## Request Charges (RU)

All operations return a synthetic request charge of `1.0 RU`. There is no attempt to simulate real RU consumption. If your code makes decisions based on RU consumption, test with the real Cosmos DB.

---

## Consistency Levels

All operations execute with immediate consistency (equivalent to `Strong` in real Cosmos DB). There is no simulation of eventual consistency, session consistency, or stale reads. If your code is sensitive to consistency level behaviour, test with the real Cosmos DB.
