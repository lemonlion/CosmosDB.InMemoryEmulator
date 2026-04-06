**[Home](Home)**

---

**Getting Started**
- [Getting Started](Getting-Started)
  - [Installation](Getting-Started#installation)
  - [Quick Start — DI](Getting-Started#quick-start--integration-tests-with-di)
  - [Quick Start — No DI](Getting-Started#quick-start--unit-tests-no-di)
- [Unit Testing](Unit-Testing)
  - [Basic Setup](Unit-Testing#basic-setup)
  - [CRUD Operations](Unit-Testing#crud-operations)
  - [SQL & LINQ Queries](Unit-Testing#sql-queries)
  - [Patch Operations](Unit-Testing#patch-operations)
  - [Test Isolation](Unit-Testing#test-isolation)

**Integration & DI**
- [Integration Approaches](Integration-Approaches)
  - [At a Glance](Integration-Approaches#at-a-glance)
  - [DI Extensions](Integration-Approaches#di-extensions-recommended)
  - [FakeCosmosHandler](Integration-Approaches#cosmosclient--fakecosmoshandler-high-fidelity)
  - [Decision Flowchart](Integration-Approaches#decision-flowchart)
- [Dependency Injection](Dependency-Injection)
  - [Pattern 1: Singleton](Dependency-Injection#pattern-1-singleton-client--singleton-container)
  - [Pattern 2: Typed Client](Dependency-Injection#pattern-2-typed-cosmosclient-subclasses)
  - [Pattern 3: GetContainer()](Dependency-Injection#pattern-3-singleton-client-repos-call-getcontainer)
  - [Pattern 4: Container-Only](Dependency-Injection#pattern-4-container-only-replacement)
  - [Pattern 5 & 6: Custom Factory](Dependency-Injection#pattern-5-custom-factory-interface--direct-inmemorycosmosclient)
  - [Configuration Options](Dependency-Injection#configuration-options)
- [Feed Iterator Usage](Feed-Iterator-Usage-Guide)

**Data Management**
- [Seeding Data](Seeding-Data)
  - [DI Callbacks](Seeding-Data#seeding-via-di-callbacks)
  - [SDK Methods](Seeding-Data#seeding-via-sdk-methods)
  - [File / Snapshot](Seeding-Data#seeding-from-a-file-or-snapshot)
  - [Resetting Between Tests](Seeding-Data#resetting-data-between-tests)
- [State Persistence](State-Persistence)
  - [Export / Import](State-Persistence#manual-exportimport)
  - [Auto-Persist Between Runs](State-Persistence#automatic-persistence-between-test-runs)
  - [Point-in-Time Restore](State-Persistence#point-in-time-restore)

**Reference**
- [Features](Features)
  - [Change Feed](Features#change-feed)
  - [Patch Operations](Features#patch-operations)
  - [Batches & Bulk](Features#transactional-batches)
  - [ETag Concurrency](Features#etag--optimistic-concurrency)
  - [TTL / Expiration](Features#ttl--expiration)
  - [Stored Procedures](Features#stored-procedures)
  - [Triggers](Features#triggers)
  - [Partition Keys](Features#partition-keys)
  - [Vector & Full-Text Search](Features#vector-search)
- [SQL Queries](SQL-Queries)
  - [Clauses & Operators](SQL-Queries#clauses)
  - [Built-in Functions](SQL-Queries#built-in-functions)
  - [Subqueries & UDFs](SQL-Queries#subqueries)
- [API Reference](API-Reference)
- [Feature Comparison](Feature-Comparison-With-Alternatives)

**Help**
- [Known Limitations](Known-Limitations)
- [Troubleshooting](Troubleshooting)
