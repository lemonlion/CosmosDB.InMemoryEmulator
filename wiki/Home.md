# CosmosDB.InMemoryEmulator

A fully featured, in-process fake for the Azure Cosmos DB SDK for .NET — purpose-built for fast, reliable component and integration testing.

No external processes, no Docker, no network — just add the NuGet package and go:

```csharp
// DI (WebApplicationFactory / integration tests)
serviceCollection.UseInMemoryCosmosDB();

// Direct instantiation (unit tests)
var container = new InMemoryContainer();
```

Full support for CRUD, [SQL querying](SQL-Queries) (120+ functions), [LINQ](Features#linq-support), [change feed](Features#change-feed), [transactional batches](Features#transactional-batches), [patch operations](Features#patch-operations), [ETags](Features#etag--optimistic-concurrency), [TTL](Features#ttl--expiration), [stored procedures & triggers](Features#stored-procedures), [full-text search](Features#full-text-search-approximate), [vector search](Features#vector-search), and [more](Features).

## Documentation

| Guide | Description |
|-------|-------------|
| **[Getting Started](Getting-Started)** | Installation, quick start with DI, first test |
| **[Unit Testing](Unit-Testing)** | Direct `InMemoryContainer` usage without DI |
| **[Integration Approaches](Integration-Approaches)** | Detailed comparison of all three approaches with pros/cons |
| **[Dependency Injection](Dependency-Injection)** | Step-by-step DI setup for all patterns (including custom factory interfaces) |
| **[Feed Iterator Usage](Feed-Iterator-Usage-Guide)** | Making `.ToFeedIterator()` work — `FakeCosmosHandler` vs `ToFeedIteratorOverridable()` |
| **[SQL Queries](SQL-Queries)** | Full SQL reference — clauses, operators, 120+ built-in functions |
| **[Features](Features)** | Patch, batches, change feed, ETags, TTL, stored procs, full-text & vector search, and more |
| **[API Reference](API-Reference)** | Complete class and method reference |
| **[Feature Comparison](Feature-Comparison-With-Alternatives)** | vs Official Emulator, vs Real Azure, vs community alternatives |
| **[Known Limitations](Known-Limitations)** | Limitations, behavioural differences, and intentionally out-of-scope areas |
| **[Troubleshooting](Troubleshooting)** | Common errors and how to fix them |
| **[Seeding Data](Seeding-Data)** | DI callbacks, SDK methods, snapshots, bulk seeding |
| **[State Persistence](State-Persistence)** | Export/import, automatic persistence between runs, point-in-time restore |
