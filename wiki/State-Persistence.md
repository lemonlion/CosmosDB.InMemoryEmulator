# State Persistence

Save, restore, and automatically persist container state between test runs.

## Overview

The emulator provides three levels of state persistence:

| Feature | What it does | Use case |
|---|---|---|
| [Manual Export/Import](#manual-exportimport) | Save/load container state as JSON on demand | Snapshots, seed data files, debugging |
| [Automatic Persistence](#automatic-persistence-between-test-runs) | Auto-save on disposal, auto-load on creation | Preserving data between test runs |
| [Point-in-Time Restore](#point-in-time-restore) | Roll back to any previous state via change feed replay | Test isolation, debugging, simulating PITR |

---

## Manual Export/Import

Export the current container state as JSON and re-import it later.

### API

```csharp
// Export to string
string json = container.ExportState();

// Export to file
container.ExportStateToFile("test-data/snapshot.json");

// Import from string (replaces all data)
container.ImportState(json);

// Import from file
container.ImportStateFromFile("test-data/snapshot.json");

// Utilities
int count = container.ItemCount;
container.ClearItems(); // Remove everything
```

### JSON Format

The exported format is a simple JSON object with an `items` array:

```json
{
  "items": [
    { "id": "1", "partitionKey": "pk1", "name": "Alice", "_etag": "\"guid\"", "_ts": 1234567890 },
    { "id": "2", "partitionKey": "pk2", "name": "Bob", "_etag": "\"guid\"", "_ts": 1234567890 }
  ]
}
```

### Behaviour

- **`ImportState` clears all existing data first** — it's a full replacement, not a merge.
- **New ETags and timestamps** are generated on import. The original `_etag` and `_ts` values in the
  JSON are overwritten with fresh values.
- **Change feed is not exported or imported.** The change feed starts fresh after import. Writes
  after import do appear in the change feed.
- **Unique key constraints are validated** on import — items violating the container's unique key
  policy will throw.

### Seeding from a File

Check seed files into source control for repeatable test data:

```csharp
// Direct instantiation
var container = new InMemoryContainer("orders", "/customerId");
container.ImportStateFromFile("TestData/orders-seed.json");

// Via DI callback
services.UseInMemoryCosmosDB(options =>
{
    options.AddContainer("orders", "/customerId");
    options.OnHandlerCreated = (name, handler) =>
    {
        handler.BackingContainer.ImportStateFromFile("TestData/orders-seed.json");
    };
});
```

For more seeding patterns, see [Seeding Data](Seeding-Data).

---

## Automatic Persistence Between Test Runs

**Added in v2.0.70.** Automatically save and restore container state between test runs with zero
manual `ExportState`/`ImportState` calls.

### How It Works

| Step | What happens |
|---|---|
| **Startup** | If a state file exists for the container, `ImportState` loads it automatically |
| **First run** | No file exists — container starts empty (no error) |
| **Disposal** | Container saves its state via `ExportState` to the file |
| **Next run** | Container loads the previously saved state |

### Via DI Options — `StatePersistenceDirectory`

Set `StatePersistenceDirectory` on the DI options. Each container gets its own file.

#### `UseInMemoryCosmosDB` (Pattern 1)

```csharp
builder.ConfigureTestServices(services =>
{
    services.UseInMemoryCosmosDB(options =>
    {
        options.AddContainer("orders", "/customerId");
        options.AddContainer("customers", "/id");
        options.StatePersistenceDirectory = "./test-state";
    });
});
```

Files: `test-state/in-memory-db_orders.json`, `test-state/in-memory-db_customers.json`

#### `UseInMemoryCosmosContainers` (Pattern 3/4)

```csharp
builder.ConfigureTestServices(services =>
{
    services.UseInMemoryCosmosContainers(options =>
    {
        options.AddContainer("orders", "/customerId");
        options.StatePersistenceDirectory = "./test-state";
    });
});
```

Files: `test-state/orders.json`

#### `UseInMemoryCosmosDB<TClient>` (Pattern 2)

```csharp
builder.ConfigureTestServices(services =>
{
    services.UseInMemoryCosmosDB<EmployeeCosmosClient>(options =>
    {
        options.AddContainer("employees", "/departmentId");
        options.StatePersistenceDirectory = "./test-state";
    });
});
```

Files: `test-state/in-memory-db_employees.json`

### Via Direct Instantiation — `StateFilePath`

For unit tests without DI, set `StateFilePath` and call `LoadPersistedState()` directly:

```csharp
public class OrderRepositoryTests : IDisposable
{
    private readonly InMemoryContainer _container;

    public OrderRepositoryTests()
    {
        _container = new InMemoryContainer("orders", "/customerId");
        _container.StateFilePath = "./test-state/orders.json";
        _container.LoadPersistedState(); // Loads if file exists, no-op otherwise
    }

    [Fact]
    public async Task CreateOrder_PersistsBetweenRuns()
    {
        await _container.CreateItemAsync(
            new Order { Id = "order-1", CustomerId = "cust-1" },
            new PartitionKey("cust-1"));
    }

    public void Dispose()
    {
        _container.Dispose(); // Saves state to ./test-state/orders.json
    }
}
```

### File Naming Convention

| DI Extension | File name pattern | Example |
|---|---|---|
| `UseInMemoryCosmosDB` | `{DatabaseName}_{ContainerName}.json` | `in-memory-db_orders.json` |
| `UseInMemoryCosmosDB<T>` | `{DatabaseName}_{ContainerName}.json` | `in-memory-db_employees.json` |
| `UseInMemoryCosmosContainers` | `{ContainerName}.json` | `orders.json` |
| Direct (`StateFilePath`) | Whatever you set | `./my-state/orders.json` |

### Behaviour Details

- **Directory creation:** The directory is created automatically on save if it doesn't exist.
- **ETags and timestamps:** `ImportState` generates new ETags and timestamps on load. Items get
  fresh system properties each run.
- **Change feed:** Not persisted. The change feed starts fresh each run.
- **Multiple containers:** Each container gets its own file. Multiple containers in the same
  persistence directory coexist without conflict.
- **Disposal cascade:** When using `InMemoryCosmosClient`, disposing the client cascades `Dispose`
  to all containers. When using `FakeCosmosHandler`, disposing the handler cascades to the
  backing container.

### `.gitignore`

If you don't want state files committed to source control:

```gitignore
# In-memory emulator persisted state
test-state/
```

If you _do_ want to commit them (e.g. as seed data that evolves over time), that works too — the
files are standard JSON in the same `{"items":[...]}` format used by `ExportState`.

---

## Point-in-Time Restore

Restore a container to its state at any previous point in time. This replays the internal change
feed up to the specified timestamp, reconstructing the exact state of every item.

```csharp
// Create and modify data over time
await container.CreateItemAsync(item1, new PartitionKey("pk"));
var restorePoint = DateTimeOffset.UtcNow;

await container.CreateItemAsync(item2, new PartitionKey("pk"));
await container.DeleteItemAsync<MyDoc>("item1", new PartitionKey("pk"));

// Roll back to the restore point — item1 reappears, item2 is gone
container.RestoreToPointInTime(restorePoint);
```

### Use Cases

- **Debugging** — inspect container state at a specific moment
- **Test isolation** — restore to a known baseline between test cases
- **Simulating Cosmos DB continuous backup** — test PITR-dependent recovery logic

### Limitations

- The restore replays the change feed, so it only works for changes made during the lifetime of
  the container instance.
- The change feed is not persisted across `ExportState` / `ImportState` calls or between test runs.
- `ClearItems()` resets the change feed — PITR is not available after clearing.

---

## API Summary

| Method / Property | Description |
|---|---|
| `ExportState()` | Returns all items as formatted JSON string |
| `ImportState(json)` | Replaces all data from JSON string |
| `ExportStateToFile(path)` | Saves state to file |
| `ImportStateFromFile(path)` | Loads state from file |
| `StateFilePath` | When set, `Dispose()` auto-saves and `LoadPersistedState()` auto-loads |
| `LoadPersistedState()` | Loads from `StateFilePath` if file exists (no-op if missing) |
| `Dispose()` | Saves to `StateFilePath` if set |
| `RestoreToPointInTime(time)` | Replays change feed to restore to a point in time |
| `ClearItems()` | Removes all items, ETags, timestamps, and change feed |
| `ItemCount` | Number of items currently stored |
| `StatePersistenceDirectory` | DI option — sets `StateFilePath` on each container automatically |

---

## See Also

- [Seeding Data](Seeding-Data) — all data seeding patterns including DI callbacks and bulk loading
- [Features](Features) — full feature reference
- [API Reference](API-Reference) — detailed method signatures
- [Unit Testing](Unit-Testing) — using InMemoryContainer directly
- [Dependency Injection](Dependency-Injection) — DI extension patterns
