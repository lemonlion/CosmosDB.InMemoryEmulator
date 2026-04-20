# Azure Cosmos DB Troubleshooting Docs → InMemoryEmulator Validation

## Scope
Validated the InMemoryEmulator against 9 Azure Cosmos DB troubleshooting pages covering HTTP status codes 400, 401, 403, 404, 408, 409, 429, 503, and query performance.

---

## 1. Troubleshoot Bad Request (400)

**Source:** [Troubleshoot Azure Cosmos DB bad request exceptions](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-bad-request)

### Documented Behaviours
| # | Behaviour | Doc Reference | Emulator Status | Test Coverage |
|---|-----------|---------------|----------------|---------------|
| 1 | Missing `id` property → 400 `"The input content is invalid because the required properties - 'id; ' - are missing"` | [§ Missing the ID property](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-bad-request#missing-the-id-property) — *"A response with this error means the JSON document that is being sent to the service lacks the required ID property."* | ✅ Implemented — empty string id throws 400 | ✅ Unit + Integration |
| 2 | Invalid partition key type → 400 `"Partition key ... is invalid"` | [§ Invalid partition key type](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-bad-request#invalid-partition-key-type) — *"The value of the partition key should be a string or a number."* | ❌ NOT implemented — accepts any type without validation | ❌ No tests |
| 3 | Partition key mismatch (header vs body) → 400 SubStatus 1001 | [§ Wrong partition key value](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-bad-request#wrong-partition-key-value) — *"Response status code does not indicate success: BadRequest (400); Substatus: 1001" / "PartitionKey extracted from document doesn't match the one specified in the header"* | ✅ Implemented — `ValidatePartitionKeyConsistency()` | ⚠️ Partial (tests exist but SubStatus 1001 not verified) |
| 4 | Numeric PK precision loss → 400 `"out of key range, possibly because of loss of precision"` | [§ Numeric partition key value precision loss](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-bad-request#numeric-partition-key-value-precision-loss) — *"A response with this error is likely caused by an operation on a document with a numeric partition key whose value is outside what Azure Cosmos DB supports."* Precision limits defined in [Per-item limits](https://learn.microsoft.com/en-us/azure/cosmos-db/concepts-limits#per-item-limits). | ❌ NOT implemented | ❌ No tests |

### Gaps to Address
- **ID with missing property**: The SDK throws `InvalidOperationException` when `id` is absent, but the emulator doesn't produce the exact Cosmos 400 message. The _empty string_ case does return 400.
- **Invalid PK type**: No type validation — real Cosmos rejects e.g. arrays/objects as PK values.
- **SubStatus 1001**: The PK mismatch error returns SubStatus 0 instead of 1001.
- **Numeric precision loss**: No validation for oversized numeric PK values.

---

## 2. Troubleshoot Not Found (404)

**Source:** [Troubleshoot Azure Cosmos DB not found exceptions](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-not-found)

### Documented Behaviours
| # | Behaviour | Doc Reference | Emulator Status | Test Coverage |
|---|-----------|---------------|----------------|---------------|
| 1 | Item doesn't exist → 404 | [§ Invalid partition key and ID combination](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-not-found#invalid-partition-key-and-id-combination) — *"The partition key and ID combination aren't valid."* | ✅ Implemented | ✅ Unit + Integration |
| 2 | TTL expired → 404 | [§ Time to Live purge](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-not-found#time-to-live-purge) — *"The item had the Time to Live (TTL) property set. The item was purged because the TTL property expired."* | ✅ Implemented — `IsExpired()` check on read/replace/delete/patch | ✅ Unit + Integration |
| 3 | Wrong partition key on read → 404 | [§ Invalid partition key and ID combination](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-not-found#invalid-partition-key-and-id-combination) | ✅ Implemented | ✅ Integration |
| 4 | Invalid characters in ID (`/`, `#`, `?`, `\`) | [§ Invalid character in an item ID](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-not-found#invalid-character-in-an-item-id) — *"An item is inserted into Azure Cosmos DB with an invalid character in the item ID."* Restricted chars defined in [Resource.Id Remarks](https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.documents.resource.id#remarks) — *"The following characters are restricted and cannot be used in the Id property: '/', '\\', '?', '#'"* | ❌ NOT validated — accepts any characters | ❌ No tests |
| 5 | Parent resource (database/container) deleted → 404 | [§ Parent resource deleted](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-not-found#parent-resource-deleted) — *"The database or container that the item exists in was deleted."* | ✅ Implemented — container/database not found returns 404 | ✅ Integration |
| 6 | Container/collection names are case-sensitive | [§ Container/Collection names are case-sensitive](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-not-found#containercollection-names-are-case-sensitive) — *"Container/Collection names are case-sensitive in Azure Cosmos DB."* | ✅ Implemented — dictionary is case-sensitive | ⚠️ No explicit test |
| 7 | Session token stale read → 404 then retry | [§ The read session isn't available for the input session token](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-not-found#the-read-session-isnt-available-for-the-input-session-token) | ❌ NOT implemented — no session token tracking | ❌ No tests |

### Gaps to Address
- **Invalid ID characters**: Real Cosmos rejects `id` values containing `/`, `#`, `?`, `\`. The emulator accepts them silently.
- **Container name case sensitivity**: Implicitly works (C# dictionary) but no test proves it.
- **Session consistency**: No session token mechanism.

---

## 3. Troubleshoot Conflict (409)

**Source:** [Troubleshoot Azure Cosmos DB conflict exceptions](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-conflict)

### Documented Behaviours
| # | Behaviour | Doc Reference | Emulator Status | Test Coverage |
|---|-----------|---------------|----------------|---------------|
| 1 | Duplicate id (same partition) on Create → 409 | Implicit in 409 status code semantics; duplicate id returns *"Entity with the specified id already exists"* | ✅ Implemented | ✅ Unit + Integration |
| 2 | Unique key constraint violation → 409 | Implicit in 409 status code semantics; returns *"Unique index constraint violation."* | ✅ Implemented | ✅ Unit + Integration |
| 3 | Partition key collision (hash of first 101 bytes) → 409 | [§ Partition Key Collision](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-conflict#partition-key-collision) — *"All Azure Cosmos DB containers created before May 3, 2019 use a hash function that computes hash based on the first 101 bytes of the partition key."* | ❌ NOT implemented — legacy issue, not applicable for emulator | N/A |
| 4 | Duplicate container creation → 409 | Implicit in 409 status code semantics | ✅ Implemented | ✅ Integration |

### Assessment
409 handling is **well-implemented**. Partition key collision is a legacy issue (pre-May 2019) and is correctly out of scope for an emulator.

---

## 4. Troubleshoot Unauthorized (401)

**Source:** [Troubleshoot Azure Cosmos DB unauthorized exceptions](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-unauthorized)

### Documented Behaviours
| # | Behaviour | Doc Reference | Emulator Status | Test Coverage |
|---|-----------|---------------|----------------|---------------|
| 1 | MAC signature mismatch → 401 | [§ Cause](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-unauthorized#cause) — *"This error occurs when the authentication signature (MAC) in your request doesn't match what Azure Cosmos DB expects."* | ❌ NOT implemented — no auth in emulator | ❌ No tests |
| 2 | Misconfigured keys → 401 | [§ Solution: Fix misconfigured keys](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-unauthorized#solution-fix-misconfigured-keys) — *"This scenario usually means the key is incorrect or incomplete in your application configuration."* | ❌ NOT implemented | ❌ No tests |
| 3 | Read-only key for writes → 401 | [§ Solution: Use read/write keys for write operations](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-unauthorized#solution-use-readwrite-keys-for-write-operations) — *"This scenario indicates the application is using read-only keys for write actions."* | ❌ NOT implemented | ❌ No tests |

### Assessment
**Correctly out of scope.** The emulator is designed for in-memory testing without authentication. Real auth testing requires the actual Cosmos emulator or service. No action needed.

---

## 5. Troubleshoot Forbidden (403)

**Source:** [Troubleshoot Azure Cosmos DB forbidden exceptions](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-forbidden)

### Documented Behaviours
| # | Behaviour | Doc Reference | Emulator Status | Test Coverage |
|---|-----------|---------------|----------------|---------------|
| 1 | Firewall blocking requests → 403 | [§ Firewall blocking requests](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-forbidden#firewall-blocking-requests) — *"Data plane requests can come to Azure Cosmos DB via the following three paths."* | ❌ NOT implemented — no networking | N/A |
| 2 | Partition key exceeding storage (SubStatus 1014) → 403 | [§ Partition key exceeding storage](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-forbidden#partition-key-exceeding-storage) — *"Response status code does not indicate success: Forbidden (403); Substatus: 1014" / "Partition key reached maximum size of {...} GB"* | ❌ NOT implemented — no storage limits | ❌ No tests |
| 3 | Non-data operations via Entra → 403 (SubStatus 5300) | [§ Nondata operations aren't allowed](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-forbidden#nondata-operations-arent-allowed) — *"Forbidden (403); Substatus: 5300; The given request [PUT ...] cannot be authorized by AAD token in data plane."* | ❌ NOT implemented — no auth | N/A |

### Assessment
Firewall and Entra auth are **correctly out of scope**. However, **partition key storage quota** (SubStatus 1014) could be useful to implement — it's the most common 403 in data plane operations and apps should handle it.

---

## 6. Troubleshoot Request Timeout (408)

**Source:** [Troubleshoot Azure Cosmos DB request timeout exceptions](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-request-time-out)

### Documented Behaviours
| # | Behaviour | Doc Reference | Emulator Status | Test Coverage |
|---|-----------|---------------|----------------|---------------|
| 1 | Transient timeout → 408, app should retry | [§ Solution 1](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-request-time-out#solution-1-it-didnt-violate-the-azure-cosmos-db-for-nosql-sla) — *"The application should handle this scenario and retry on these transient failures."* | ⚠️ Via FaultInjector only | ✅ 1 unit test |
| 2 | Hot partition causing timeouts | [§ Hot partition key](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-request-time-out#hot-partition-key) — *"When there's a hot partition, one or more logical partition keys on a physical partition are consuming all the physical partition's Request Units per second (RU/s)."* | ❌ NOT implemented — no partitioning simulation | ❌ No tests |

### Assessment
The FaultInjector mechanism allows users to inject 408 responses for retry testing. This is **adequate** for an emulator — real timeouts are network-layer concerns.

---

## 7. Troubleshoot Request Rate Too Large (429)

**Source:** [Troubleshoot Azure Cosmos DB request rate too large exceptions](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-request-rate-too-large)

### Documented Behaviours
| # | Behaviour | Doc Reference | Emulator Status | Test Coverage |
|---|-----------|---------------|----------------|---------------|
| 1 | RU consumption exceeds provisioned → 429 | [§ Request rate is large](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-request-rate-too-large#request-rate-is-large) — *"It occurs when the RUs consumed by operations on data exceed the provisioned number of RU/s."* | ❌ NOT natively implemented — no RU tracking | ❌ No native tests |
| 2 | Hot partition throttling → 429 | [§ Step 2: Determine if there's a hot partition](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-request-rate-too-large#step-2-determine-if-theres-a-hot-partition) — *"A hot partition arises when one or a few logical partition keys consume a disproportionate amount of the total RU/s."* | ❌ NOT implemented | ❌ No tests |
| 3 | Metadata rate limiting → 429 | [§ Rate limiting on metadata requests](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-request-rate-too-large#rate-limiting-on-metadata-requests) — *"Metadata rate limiting can occur when you're performing a high volume of metadata operations on databases and/or containers."* | ❌ NOT implemented | ❌ No tests |
| 4 | Transient service error → 429 | [§ Rate limiting due to transient service error](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-request-rate-too-large#rate-limiting-due-to-transient-service-error) — *"This 429 error is returned when the request encounters a transient service error."* | ⚠️ Via FaultInjector only | ✅ ~12 unit tests |
| 5 | SDK auto-retry on 429 (up to 9 times) | [§ Step 1: Check the metrics](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-request-rate-too-large#step-1-check-the-metrics-to-determine-the-percentage-of-requests-with-429-error) — *"By default, the Azure Cosmos DB client SDKs … automatically retry requests on 429s. They retry typically up to nine times."* | ⚠️ Via FaultInjector — users can test retry behaviour | ✅ Some tests |
| 6 | Retry-After header on 429 | [§ Step 1: Check the metrics](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-request-rate-too-large#step-1-check-the-metrics-to-determine-the-percentage-of-requests-with-429-error) — SDK retries internally imply Retry-After header presence. See also [CosmosClientOptions.MaxRetryAttemptsOnRateLimitedRequests](https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.cosmosclientoptions.maxretryattemptsonratelimitedrequests). | ❌ NOT verified in FaultInjector responses | ❌ No tests |

### Assessment
FaultInjector provides **good coverage** for testing app-level retry logic against 429. Native RU tracking is likely out of scope for an in-memory emulator. The **Retry-After header** is worth validating.

---

## 8. Troubleshoot Query Performance

**Source:** [Troubleshoot query performance for Azure Cosmos DB](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-query-performance)

### Documented Behaviours
| # | Behaviour | Doc Reference | Emulator Status | Test Coverage |
|---|-----------|---------------|----------------|---------------|
| 1 | Retrieved vs Output Document Count metrics | [§ Get query metrics](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-query-performance#get-query-metrics) — *"compare the Retrieved Document Count with the Output Document Count for your query"* | ❌ No query metrics tracking | N/A |
| 2 | Index utilization | [§ Include necessary paths in the indexing policy](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-query-performance#include-necessary-paths-in-the-indexing-policy) — *"Your indexing policy should cover any properties included in WHERE clauses, ORDER BY clauses, JOIN, and most system functions."* | ❌ No indexing simulation | N/A |
| 3 | Cross-partition query fan-out | [§ Minimize cross partition queries](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-query-performance#minimize-cross-partition-queries) — *"If your query has an equality filter that matches your container's partition key, you need to check only the relevant partition's index."* | ❌ No physical partition simulation | N/A |
| 4 | Composite indexes | [§ Optimize queries that have filters on multiple properties](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-query-performance#optimize-queries-that-have-filters-on-multiple-properties) | ❌ No index simulation | N/A |
| 5 | System functions that don't use the index | [§ Understand which system functions use the index](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-query-performance#understand-which-system-functions-use-the-index) — *"Following are some common system functions that don't use the index and must load each document: Upper/Lower, GetCurrentDateTime…"* | ❌ No index simulation | N/A |
| 6 | Query returning empty pages with continuation | [§ Common SDK issues](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-query-performance#common-sdk-issues) — *"Sometimes queries may have empty pages even when there are results on a future page."* | ⚠️ Continuation tokens implemented | ✅ Some tests |

### Assessment
Query performance optimizations are **correctly out of scope** — the emulator focuses on functional correctness, not performance simulation. The continuation token mechanism is implemented, which is the key testable behaviour.

---

## 9. Troubleshoot SDK Availability

**Source:** [Troubleshoot Azure Cosmos DB SDK availability](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-sdk-availability)

### Documented Behaviours
| # | Behaviour | Doc Reference | Emulator Status | Test Coverage |
|---|-----------|---------------|----------------|---------------|
| 1 | Region failover / preferred regions | [§ Fail over the write region in a single write region account](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-sdk-availability#fail-over-the-write-region-in-a-single-write-region-account) — *"the next write request will fail with a known backend response. When this response is detected, the client will query the account to learn the new write region"* | ❌ NOT implemented — single-region emulator | N/A |
| 2 | Removing/adding regions | [§ Removing a region from the account](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-sdk-availability#removing-a-region-from-the-account) — *"The client then marks the regional endpoint as unavailable. The client retries the current operation and all the future operations are permanently routed to the next region."* | ❌ NOT implemented | N/A |
| 3 | Transient TCP issues → retry (503, timeouts) | [§ Transient connectivity issues on TCP protocol](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-sdk-availability#transient-connectivity-issues-on-tcp-protocol) — *"These temporary network conditions can surface as TCP timeouts and Service Unavailable (HTTP 503) errors."* | ⚠️ Via FaultInjector (503, timeouts) | ✅ Some tests |
| 4 | Session consistency across regions | [§ Session consistency guarantees](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-sdk-availability#session-consistency-guarantees) — *"the client needs to guarantee that it can read its own writes."* | ❌ NOT implemented | ❌ No tests |

### Assessment
Multi-region and availability scenarios are **correctly out of scope** for an in-memory emulator. FaultInjector adequately covers transient failure testing.

---

## Summary of Actionable Gaps

### Worth Implementing (data-plane correctness)
| Priority | Gap | Status Code | Doc Reference | Effort |
|----------|-----|-------------|---------------|--------|
| **HIGH** | Invalid ID character validation (`/`, `#`, `?`, `\`) | 400 | [Resource.Id Remarks](https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.documents.resource.id#remarks) — *"The following characters are restricted and cannot be used in the Id property: '/', '\\', '?', '#'"* | Low |
| **HIGH** | Partition key mismatch SubStatus 1001 | 400 | [§ Wrong partition key value](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-bad-request#wrong-partition-key-value) — *"BadRequest (400); Substatus: 1001"* | Low |
| **MEDIUM** | Container name case-sensitivity test | 404 | [§ Container/Collection names are case-sensitive](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-not-found#containercollection-names-are-case-sensitive) | Low (test only) |
| **MEDIUM** | Invalid partition key type validation | 400 | [§ Invalid partition key type](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-bad-request#invalid-partition-key-type) — *"The value of the partition key should be a string or a number."* | Medium |
| **LOW** | Numeric PK precision loss validation | 400 | [§ Numeric partition key value precision loss](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-bad-request#numeric-partition-key-value-precision-loss) + [Per-item limits](https://learn.microsoft.com/en-us/azure/cosmos-db/concepts-limits#per-item-limits) | Medium |
| **LOW** | Partition key storage quota (SubStatus 1014) | 403 | [§ Partition key exceeding storage](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-forbidden#partition-key-exceeding-storage) — *"Forbidden (403); Substatus: 1014"* | High |
| **LOW** | Retry-After header validation on fault-injected 429 | 429 | [§ Step 1](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-request-rate-too-large#step-1-check-the-metrics-to-determine-the-percentage-of-requests-with-429-error) — SDK auto-retries imply Retry-After header | Low (test only) |

### Correctly Out of Scope
| Area | Reason | Doc Reference |
|------|--------|---------------|
| 401 Unauthorized | No auth in emulator | [troubleshoot-unauthorized](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-unauthorized) |
| 403 Firewall/Entra | No networking layer | [troubleshoot-forbidden § Firewall](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-forbidden#firewall-blocking-requests) |
| 408 Native timeouts | Network layer concern | [troubleshoot-request-time-out](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-request-time-out) |
| 429 Native RU tracking | Performance simulation | [troubleshoot-request-rate-too-large § Request rate is large](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-request-rate-too-large#request-rate-is-large) |
| Query performance metrics/indexing | Performance simulation | [troubleshoot-query-performance](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-query-performance) |
| Multi-region availability/failover | Single-region emulator | [troubleshoot-sdk-availability](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-sdk-availability) |
| Session token consistency | No session tracking | [troubleshoot-not-found § Session token](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-not-found#the-read-session-isnt-available-for-the-input-session-token) |
| Partition key collision (legacy) | Pre-May 2019 issue | [troubleshoot-conflict § Partition Key Collision](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-conflict#partition-key-collision) |

---

## Todos

1. **invalid-id-chars** — Add validation for invalid characters (`/`, `#`, `?`, `\`) in item IDs, returning 400 Bad Request. Per [Resource.Id Remarks](https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.documents.resource.id#remarks). Add unit + integration tests.
2. **pk-mismatch-substatus** — Update PK mismatch error to use SubStatus 1001 instead of 0. Per [§ Wrong partition key value](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-bad-request#wrong-partition-key-value). Add test verifying SubStatus.
3. **container-case-sensitivity-test** — Add integration test proving container names are case-sensitive. Per [§ Container/Collection names are case-sensitive](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-not-found#containercollection-names-are-case-sensitive). No code change needed.
4. **invalid-pk-type** — Add validation rejecting non-string/non-number/non-bool/non-null partition key values. Per [§ Invalid partition key type](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-bad-request#invalid-partition-key-type). Add tests.
5. **numeric-pk-precision** — Add validation for numeric PK values exceeding Cosmos DB's precision limits. Per [§ Numeric partition key value precision loss](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-bad-request#numeric-partition-key-value-precision-loss) + [Per-item limits](https://learn.microsoft.com/en-us/azure/cosmos-db/concepts-limits#per-item-limits).
6. **retry-after-header-test** — Add test verifying FaultInjector 429 responses include Retry-After header. Per [SDK auto-retry docs](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-request-rate-too-large#step-1-check-the-metrics-to-determine-the-percentage-of-requests-with-429-error).
