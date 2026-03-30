# Comparison with Alternatives

## vs Official Cosmos DB Emulator

| Feature | CosmosDB.InMemoryEmulator | [Official Emulator](https://learn.microsoft.com/en-us/azure/cosmos-db/emulator) | Real Azure Cosmos DB |
|---------|:---:|:---:|:---:|
| Pricing | ✅ Free | ✅ Free | ❌ Pay-per-RU + storage |
| In-process (no external deps) | ✅ | ❌ | ❌ Requires network |
| SQL query parser | ✅ (40+ functions) | ✅ | ✅ Full |
| `GROUP BY` / `HAVING` | ✅ | ✅ | ✅ |
| Subqueries / `EXISTS` | ✅ | ✅ | ✅ |
| `JOIN` (array expansion) | ✅ Unlimited | ⚠️ Max 5 per query | ✅ |
| Transactional batches | ✅ | ✅ | ✅ |
| Change feed | ✅ | ✅ | ✅ |
| Patch operations | ✅ | ✅ | ✅ |
| ETag concurrency | ✅ | ✅ | ✅ |
| TTL / expiration | ✅ | ✅ | ✅ |
| Stored procedures / UDFs | ✅ (C# handlers) | ✅ (JavaScript) | ✅ (JavaScript) |
| Fault injection | ✅ | ❌ | ❌ |
| Stream API | ✅ | ✅ | ✅ |
| LINQ integration | ✅ | ✅ | ✅ |
| ReadMany | ✅ | ✅ | ✅ |
| Composite partition keys | ✅ | ✅ | ✅ |
| State export/import | ✅ | ❌ | ❌ |
| Multi-container routing | ✅ | ✅ | ✅ |
| Unlimited containers | ✅ | ⚠️ Degrades past 10 fixed / 5 unlimited | ✅ |
| All consistency levels | ✅ (simulated) | ⚠️ Session & Strong only | ✅ |
| Serverless throughput mode | ✅ (no RU enforcement) | ❌ Provisioned only | ✅ |
| Multiple instances / parallel | ✅ One per test | ❌ Single instance only | ⚠️ Shared state; needs cleanup |
| Custom auth keys | ✅ Any / none | ⚠️ Well-known key; restart to change | ✅ |
| Fast startup | ✅ Instant | ❌ 10-30s | ❌ Provisioning minutes |
| CI-friendly | ✅ | ⚠️ Flaky | ⚠️ Needs secrets, network, costs |
| Test isolation | ✅ New instance per test | ⚠️ Shared state | ❌ Shared; risk of data leakage |
| Stable under load | ✅ No sockets/network | ⚠️ Socket exhaustion, 407s, hangs | ✅ (at cost) |
| No CPU/corruption issues | ✅ Pure in-memory | ⚠️ Can spike CPU, enter corrupted state | ✅ |
| Linux / Docker reliable | ✅ Any platform | ⚠️ Docker image fails on some CPUs | ✅ Any platform |
| No special reset needed | ✅ Dispose and recreate | ⚠️ May need "Reset Data" or `/DisableRIO` | ⚠️ Manual cleanup |
| Works offline | ✅ | ✅ | ❌ Requires internet |

## vs Community Alternatives

| Feature | CosmosDB.InMemoryEmulator | [FakeCosmosDb](https://github.com/timabell/FakeCosmosDb) | [FakeCosmosEasy](https://github.com/rentready/fake-cosmos-easy) |
|---------|:---:|:---:|:---:|
| SQL query parser | ✅ (40+ functions) | ⚠️ Basic | ⚠️ Basic |
| `GROUP BY` / `HAVING` | ✅ | ❌ | ❌ |
| Subqueries / `EXISTS` | ✅ | ❌ | ❌ |
| `JOIN` (array expansion) | ✅ Unlimited | ❌ | ❌ |
| Transactional batches | ✅ | ❌ | ❌ |
| Change feed | ✅ | ❌ | ❌ |
| Patch operations | ✅ | ❌ | ❌ |
| ETag concurrency | ✅ | ❌ | ❌ |
| TTL / expiration | ✅ | ❌ | ❌ |
| Stored procedures / UDFs | ✅ (C# handlers) | ❌ | ❌ |
| Fault injection | ✅ | ❌ | ❌ |
| Stream API | ✅ | ❌ | ❌ |
| LINQ integration | ✅ | ❌ | ❌ |
| ReadMany | ✅ | ❌ | ❌ |
| Composite partition keys | ✅ | ❌ | ❌ |
| State export/import | ✅ | ❌ | ❌ |
| Actively maintained | ✅ | ✅ | ❌ (3 years stale) |
