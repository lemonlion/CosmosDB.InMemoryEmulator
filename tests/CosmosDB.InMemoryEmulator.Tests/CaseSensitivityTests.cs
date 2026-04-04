using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Cosmos DB is case-sensitive for UDF names, stored procedure IDs, trigger IDs,
/// and container names in routing. These tests verify the emulator matches that behaviour.
/// </summary>
public class CaseSensitivityTests
{
    // ═══════════════════════════════════════════════════════════════════════════
    //  UDF case sensitivity
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Udf_RegisteredWithCasing_IsNotCallableWithDifferentCasing()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        container.RegisterUdf("myFunc", args => Convert.ToDouble(args[0]) * 2);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", partitionKey = "pk1", value = 10 }),
            new PartitionKey("pk1"));

        // Query with WRONG casing — should throw because "MYFUNC" is not registered, only "myFunc"
        var query = new QueryDefinition("SELECT VALUE udf.MYFUNC(c.value) FROM c");

        var act = async () =>
        {
            var iter = container.GetItemQueryIterator<double>(query);
            while (iter.HasMoreResults) await iter.ReadNextAsync();
        };

        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task Udf_RegisteredWithCasing_IsCallableWithExactCasing()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        container.RegisterUdf("myFunc", args => Convert.ToDouble(args[0]) * 2);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", partitionKey = "pk1", value = 10 }),
            new PartitionKey("pk1"));

        // Query with CORRECT casing — should work
        var query = new QueryDefinition("SELECT VALUE udf.myFunc(c.value) FROM c");
        var iter = container.GetItemQueryIterator<double>(query);
        var results = new List<double>();
        while (iter.HasMoreResults)
        {
            var page = await iter.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle().Which.Should().Be(20);
    }

    [Fact]
    public async Task Udf_TwoUdfs_DifferingOnlyByCasing_AreSeparate()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        container.RegisterUdf("myFunc", args => "lower");
        container.RegisterUdf("MYFUNC", args => "upper");

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", partitionKey = "pk1" }),
            new PartitionKey("pk1"));

        var queryLower = new QueryDefinition("SELECT VALUE udf.myFunc(c.id) FROM c");
        var queryUpper = new QueryDefinition("SELECT VALUE udf.MYFUNC(c.id) FROM c");

        var iterLower = container.GetItemQueryIterator<string>(queryLower);
        var resultsLower = new List<string>();
        while (iterLower.HasMoreResults) resultsLower.AddRange(await iterLower.ReadNextAsync());

        var iterUpper = container.GetItemQueryIterator<string>(queryUpper);
        var resultsUpper = new List<string>();
        while (iterUpper.HasMoreResults) resultsUpper.AddRange(await iterUpper.ReadNextAsync());

        resultsLower.Should().ContainSingle().Which.Should().Be("lower");
        resultsUpper.Should().ContainSingle().Which.Should().Be("upper");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Stored Procedure case sensitivity
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task StoredProcedure_RegisteredWithCasing_IsNotCallableWithDifferentCasing()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        container.RegisterStoredProcedure("myProc", (pk, args) => "result");

        var scripts = container.Scripts;

        // Call with WRONG casing — should not find the handler (throws 404)
        var act = () => scripts.ExecuteStoredProcedureAsync<string>(
            "MYPROC",
            new PartitionKey("pk1"),
            Array.Empty<dynamic>());

        (await act.Should().ThrowAsync<CosmosException>())
            .Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task StoredProcedure_TwoProcedures_DifferingOnlyByCasing_AreSeparate()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        container.RegisterStoredProcedure("myProc", (pk, args) => "lower");
        container.RegisterStoredProcedure("MYPROC", (pk, args) => "upper");

        var scripts = container.Scripts;

        var responseLower = await scripts.ExecuteStoredProcedureAsync<string>(
            "myProc", new PartitionKey("pk1"), Array.Empty<dynamic>());
        var responseUpper = await scripts.ExecuteStoredProcedureAsync<string>(
            "MYPROC", new PartitionKey("pk1"), Array.Empty<dynamic>());

        responseLower.Resource.Should().Be("lower");
        responseUpper.Resource.Should().Be("upper");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Trigger case sensitivity
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Trigger_RegisteredWithCasing_DoesNotFireWithDifferentCasing()
    {
        var container = new InMemoryContainer("test", "/pk");
        container.RegisterTrigger("myTrigger", TriggerType.Pre, TriggerOperation.Create,
            (Func<JObject, JObject>)(doc =>
            {
                doc["triggered"] = true;
                return doc;
            }));

        // Reference trigger with WRONG casing — should throw because "MYTRIGGER" is not registered
        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"),
            new ItemRequestOptions { PreTriggers = new List<string> { "MYTRIGGER" } });

        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task Trigger_TwoTriggers_DifferingOnlyByCasing_AreSeparate()
    {
        var container = new InMemoryContainer("test", "/pk");
        container.RegisterTrigger("myTrigger", TriggerType.Pre, TriggerOperation.Create,
            (Func<JObject, JObject>)(doc =>
            {
                doc["source"] = "lower";
                return doc;
            }));
        container.RegisterTrigger("MYTRIGGER", TriggerType.Pre, TriggerOperation.Create,
            (Func<JObject, JObject>)(doc =>
            {
                doc["source"] = "upper";
                return doc;
            }));

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"),
            new ItemRequestOptions { PreTriggers = new List<string> { "myTrigger" } });

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a" }),
            new PartitionKey("a"),
            new ItemRequestOptions { PreTriggers = new List<string> { "MYTRIGGER" } });

        // Read back the items to verify each trigger applied its own value
        var readLower = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
        var readUpper = await container.ReadItemAsync<JObject>("2", new PartitionKey("a"));

        readLower.Resource["source"]!.Value<string>().Should().Be("lower");
        readUpper.Resource["source"]!.Value<string>().Should().Be("upper");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Container routing case sensitivity (FakeCosmosHandler.CreateRouter)
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Router_ContainerNames_AreCaseSensitive()
    {
        var container1 = new InMemoryContainer("MyData", "/partitionKey");
        var container2 = new InMemoryContainer("mydata", "/partitionKey");

        await container1.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "UpperCase" },
            new PartitionKey("pk1"));
        await container2.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "LowerCase" },
            new PartitionKey("pk1"));

        using var handler1 = new FakeCosmosHandler(container1);
        using var handler2 = new FakeCosmosHandler(container2);

        // Register both containers — they differ only in casing and should be separate
        using var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
        {
            ["MyData"] = handler1,
            ["mydata"] = handler2,
        });

        using var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                HttpClientFactory = () => new HttpClient(router) { Timeout = TimeSpan.FromSeconds(10) }
            });

        var c1 = client.GetContainer("db", "MyData");
        var c2 = client.GetContainer("db", "mydata");

        var results1 = new List<TestDocument>();
        var iter1 = c1.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
        while (iter1.HasMoreResults) results1.AddRange(await iter1.ReadNextAsync());

        var results2 = new List<TestDocument>();
        var iter2 = c2.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
        while (iter2.HasMoreResults) results2.AddRange(await iter2.ReadNextAsync());

        results1.Should().ContainSingle().Which.Name.Should().Be("UpperCase");
        results2.Should().ContainSingle().Which.Name.Should().Be("LowerCase");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Deep-Dive Section C: Item ID Case Sensitivity
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ItemId_DifferentCasings_AreSeparateDocuments()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "Item1", partitionKey = "pk1", name = "Upper" }),
            new PartitionKey("pk1"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "item1", partitionKey = "pk1", name = "Lower" }),
            new PartitionKey("pk1"));

        container.ItemCount.Should().Be(2);
    }

    [Fact]
    public async Task ItemId_ReadItem_RequiresExactCasing()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "Item1", partitionKey = "pk1", name = "Upper" }),
            new PartitionKey("pk1"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "item1", partitionKey = "pk1", name = "Lower" }),
            new PartitionKey("pk1"));

        var readUpper = await container.ReadItemAsync<JObject>("Item1", new PartitionKey("pk1"));
        var readLower = await container.ReadItemAsync<JObject>("item1", new PartitionKey("pk1"));

        readUpper.Resource["name"]!.ToString().Should().Be("Upper");
        readLower.Resource["name"]!.ToString().Should().Be("Lower");
    }

    [Fact]
    public async Task ItemId_DeleteItem_OnlyAffectsExactCasing()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "Item1", partitionKey = "pk1", name = "Upper" }),
            new PartitionKey("pk1"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "item1", partitionKey = "pk1", name = "Lower" }),
            new PartitionKey("pk1"));

        await container.DeleteItemAsync<JObject>("Item1", new PartitionKey("pk1"));

        container.ItemCount.Should().Be(1);
        var remaining = await container.ReadItemAsync<JObject>("item1", new PartitionKey("pk1"));
        remaining.Resource["name"]!.ToString().Should().Be("Lower");
    }

    [Fact]
    public async Task ItemId_ReplaceItem_OnlyAffectsExactCasing()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "Item1", partitionKey = "pk1", name = "Upper" }),
            new PartitionKey("pk1"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "item1", partitionKey = "pk1", name = "Lower" }),
            new PartitionKey("pk1"));

        await container.ReplaceItemAsync(
            JObject.FromObject(new { id = "Item1", partitionKey = "pk1", name = "Replaced" }),
            "Item1", new PartitionKey("pk1"));

        var readUpper = await container.ReadItemAsync<JObject>("Item1", new PartitionKey("pk1"));
        var readLower = await container.ReadItemAsync<JObject>("item1", new PartitionKey("pk1"));

        readUpper.Resource["name"]!.ToString().Should().Be("Replaced");
        readLower.Resource["name"]!.ToString().Should().Be("Lower");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Deep-Dive Section D: Partition Key Value Case Sensitivity
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task PartitionKey_DifferentCasings_TreatedAsSeparatePartitions()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "doc1", partitionKey = "pkA", name = "Upper" }),
            new PartitionKey("pkA"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "doc1", partitionKey = "pka", name = "Lower" }),
            new PartitionKey("pka"));

        container.ItemCount.Should().Be(2);
    }

    [Fact]
    public async Task PartitionKey_ReadItem_RequiresExactCasing()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "doc1", partitionKey = "pkA", name = "Upper" }),
            new PartitionKey("pkA"));

        var read = await container.ReadItemAsync<JObject>("doc1", new PartitionKey("pkA"));
        read.Resource["name"]!.ToString().Should().Be("Upper");

        var act = () => container.ReadItemAsync<JObject>("doc1", new PartitionKey("pka"));
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task PartitionKey_QueryWithPartitionKey_OnlyReturnsExactMatch()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", partitionKey = "pkA", name = "Upper" }),
            new PartitionKey("pkA"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "2", partitionKey = "pka", name = "Lower" }),
            new PartitionKey("pka"));

        var iter = container.GetItemQueryIterator<JObject>(
            new QueryDefinition("SELECT * FROM c"),
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pkA") });

        var results = new List<JObject>();
        while (iter.HasMoreResults)
        {
            var page = await iter.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle().Which["name"]!.ToString().Should().Be("Upper");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Deep-Dive Section A: Database Name Case Sensitivity
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Database_NamesAreCaseSensitive_SeparateInstances()
    {
        var client = new InMemoryCosmosClient();

        await client.CreateDatabaseAsync("MyDB");
        await client.CreateDatabaseAsync("mydb");

        var db1 = client.GetDatabase("MyDB");
        var db2 = client.GetDatabase("mydb");

        db1.Id.Should().Be("MyDB");
        db2.Id.Should().Be("mydb");
    }

    [Fact]
    public async Task Database_CreateAsync_DifferentCasings_NoConflict()
    {
        var client = new InMemoryCosmosClient();

        var r1 = await client.CreateDatabaseAsync("MyDB");
        var r2 = await client.CreateDatabaseAsync("mydb");

        r1.StatusCode.Should().Be(System.Net.HttpStatusCode.Created);
        r2.StatusCode.Should().Be(System.Net.HttpStatusCode.Created);
    }

    [Fact]
    public async Task Database_CreateIfNotExists_DifferentCasings_BothCreated()
    {
        var client = new InMemoryCosmosClient();

        var r1 = await client.CreateDatabaseIfNotExistsAsync("MyDB");
        var r2 = await client.CreateDatabaseIfNotExistsAsync("mydb");

        r1.StatusCode.Should().Be(System.Net.HttpStatusCode.Created);
        r2.StatusCode.Should().Be(System.Net.HttpStatusCode.Created);
    }

    [Fact]
    public async Task Database_GetDatabaseQueryIterator_ListsBothCasings()
    {
        var client = new InMemoryCosmosClient();

        await client.CreateDatabaseAsync("MyDB");
        await client.CreateDatabaseAsync("mydb");

        var iter = client.GetDatabaseQueryIterator<DatabaseProperties>();
        var databases = new List<DatabaseProperties>();
        while (iter.HasMoreResults)
        {
            var page = await iter.ReadNextAsync();
            databases.AddRange(page);
        }

        databases.Select(d => d.Id).Should().Contain("MyDB").And.Contain("mydb");
    }

    [Fact]
    public async Task Database_CreateIfNotExists_ExactCasing_ReturnsOk()
    {
        var client = new InMemoryCosmosClient();

        await client.CreateDatabaseAsync("MyDB");
        var r2 = await client.CreateDatabaseIfNotExistsAsync("MyDB");

        r2.StatusCode.Should().Be(System.Net.HttpStatusCode.OK);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Deep-Dive Section B: Container Name Case Sensitivity via InMemoryDatabase
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Container_NamesAreCaseSensitive_SeparateInstances()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("testdb")).Database;

        await db.CreateContainerAsync("MyContainer", "/pk");
        await db.CreateContainerAsync("mycontainer", "/pk");

        var c1 = db.GetContainer("MyContainer");
        var c2 = db.GetContainer("mycontainer");

        c1.Id.Should().Be("MyContainer");
        c2.Id.Should().Be("mycontainer");
    }

    [Fact]
    public async Task Container_CreateAsync_DifferentCasings_NoConflict()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("testdb")).Database;

        var r1 = await db.CreateContainerAsync("Foo", "/pk");
        var r2 = await db.CreateContainerAsync("foo", "/pk");

        r1.StatusCode.Should().Be(System.Net.HttpStatusCode.Created);
        r2.StatusCode.Should().Be(System.Net.HttpStatusCode.Created);
    }

    [Fact]
    public async Task Container_CreateIfNotExists_DifferentCasings_BothCreated()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("testdb")).Database;

        var r1 = await db.CreateContainerIfNotExistsAsync("Foo", "/pk");
        var r2 = await db.CreateContainerIfNotExistsAsync("foo", "/pk");

        r1.StatusCode.Should().Be(System.Net.HttpStatusCode.Created);
        r2.StatusCode.Should().Be(System.Net.HttpStatusCode.Created);
    }

    [Fact]
    public async Task Container_GetContainerQueryIterator_ListsBothCasings()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("testdb")).Database;

        await db.CreateContainerAsync("Foo", "/pk");
        await db.CreateContainerAsync("foo", "/pk");

        var iter = db.GetContainerQueryIterator<ContainerProperties>();
        var containers = new List<ContainerProperties>();
        while (iter.HasMoreResults)
        {
            var page = await iter.ReadNextAsync();
            containers.AddRange(page);
        }

        containers.Select(c => c.Id).Should().Contain("Foo").And.Contain("foo");
    }

    [Fact]
    public async Task Container_DifferentCasings_StoreDataSeparately()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("testdb")).Database;

        await db.CreateContainerAsync("Foo", "/pk");
        await db.CreateContainerAsync("foo", "/pk");

        var c1 = db.GetContainer("Foo");
        var c2 = db.GetContainer("foo");

        await c1.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "a", Name = "UpperContainer" },
            new PartitionKey("a"));
        await c2.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "a", Name = "LowerContainer" },
            new PartitionKey("a"));

        var read1 = await c1.ReadItemAsync<TestDocument>("1", new PartitionKey("a"));
        var read2 = await c2.ReadItemAsync<TestDocument>("1", new PartitionKey("a"));

        read1.Resource.Name.Should().Be("UpperContainer");
        read2.Resource.Name.Should().Be("LowerContainer");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Deep-Dive Section E: User Name Case Sensitivity
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task User_NamesAreCaseSensitive_SeparateInstances()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("testdb")).Database;

        var r1 = await db.CreateUserAsync("Admin");
        var r2 = await db.CreateUserAsync("admin");

        r1.Resource.Id.Should().Be("Admin");
        r2.Resource.Id.Should().Be("admin");
    }

    [Fact]
    public async Task User_GetUser_RequiresExactCasing()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("testdb")).Database;

        await db.CreateUserAsync("Admin");
        await db.CreateUserAsync("admin");

        var u1 = db.GetUser("Admin");
        var u2 = db.GetUser("admin");

        u1.Id.Should().Be("Admin");
        u2.Id.Should().Be("admin");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Deep-Dive Section F: Permission ID Case Sensitivity
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Permission_IdsAreCaseSensitive_SeparateEntries()
    {
        var client = new InMemoryCosmosClient();
        var db = (await client.CreateDatabaseAsync("testdb")).Database;
        await db.CreateUserAsync("user1");
        var user = db.GetUser("user1");

        var r1 = await user.CreatePermissionAsync(
            new PermissionProperties("ReadAll", PermissionMode.Read, db.GetContainer("c")));
        var r2 = await user.CreatePermissionAsync(
            new PermissionProperties("readall", PermissionMode.Read, db.GetContainer("c")));

        r1.Resource.Id.Should().Be("ReadAll");
        r2.Resource.Id.Should().Be("readall");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Deep-Dive Section G: Scripts API Sproc CRUD Case Sensitivity
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Sproc_Scripts_CreateAndRead_CaseSensitive()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var scripts = container.Scripts;

        await scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
        {
            Id = "MyProc",
            Body = "function() { return true; }"
        });

        var read = await scripts.ReadStoredProcedureAsync("MyProc");
        read.Resource.Id.Should().Be("MyProc");

        var act = () => scripts.ReadStoredProcedureAsync("myproc");
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task Sproc_Scripts_Delete_CaseSensitive()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var scripts = container.Scripts;

        await scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
        {
            Id = "MyProc",
            Body = "function() { return true; }"
        });

        var act = () => scripts.DeleteStoredProcedureAsync("myproc");
        await act.Should().ThrowAsync<CosmosException>();

        // Exact casing should work
        await scripts.DeleteStoredProcedureAsync("MyProc");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Deep-Dive Section H: Scripts API Trigger CRUD Case Sensitivity
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Trigger_Scripts_CreateAndRead_CaseSensitive()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var scripts = container.Scripts;

        await scripts.CreateTriggerAsync(new TriggerProperties
        {
            Id = "MyTrig",
            Body = "function() {}",
            TriggerType = TriggerType.Pre,
            TriggerOperation = TriggerOperation.Create
        });

        var read = await scripts.ReadTriggerAsync("MyTrig");
        read.Resource.Id.Should().Be("MyTrig");

        var act = () => scripts.ReadTriggerAsync("mytrig");
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task Trigger_Scripts_Delete_CaseSensitive()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var scripts = container.Scripts;

        await scripts.CreateTriggerAsync(new TriggerProperties
        {
            Id = "MyTrig",
            Body = "function() {}",
            TriggerType = TriggerType.Pre,
            TriggerOperation = TriggerOperation.Create
        });

        var act = () => scripts.DeleteTriggerAsync("mytrig");
        await act.Should().ThrowAsync<CosmosException>();

        await scripts.DeleteTriggerAsync("MyTrig");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Deep-Dive Section I: Scripts API UDF CRUD Case Sensitivity
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Udf_Scripts_CreateTwoDifferentCasings_NoConflict()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var scripts = container.Scripts;

        var r1 = await scripts.CreateUserDefinedFunctionAsync(
            new UserDefinedFunctionProperties { Id = "MyFunc", Body = "function(x) { return x; }" });
        var r2 = await scripts.CreateUserDefinedFunctionAsync(
            new UserDefinedFunctionProperties { Id = "MYFUNC", Body = "function(x) { return x; }" });

        r1.Resource.Id.Should().Be("MyFunc");
        r2.Resource.Id.Should().Be("MYFUNC");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Deep-Dive Section J: SQL Keyword Case Insensitivity
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SqlKeywords_AllUppercase_Works()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }),
            new PartitionKey("pk1"));

        var iter = container.GetItemQueryIterator<JObject>(
            new QueryDefinition("SELECT * FROM c WHERE c.name = 'Alice'"));
        var results = new List<JObject>();
        while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

        results.Should().ContainSingle();
    }

    [Fact]
    public async Task SqlKeywords_AllLowercase_Works()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }),
            new PartitionKey("pk1"));

        var iter = container.GetItemQueryIterator<JObject>(
            new QueryDefinition("select * from c where c.name = 'Alice'"));
        var results = new List<JObject>();
        while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

        results.Should().ContainSingle();
    }

    [Fact]
    public async Task SqlKeywords_MixedCase_Works()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }),
            new PartitionKey("pk1"));

        var iter = container.GetItemQueryIterator<JObject>(
            new QueryDefinition("SeLeCt * FrOm c WhErE c.name = 'Alice'"));
        var results = new List<JObject>();
        while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

        results.Should().ContainSingle();
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Deep-Dive Section K: SQL Function Name Case Insensitivity
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SqlFunction_MixedCasing_Works()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Hello World" }),
            new PartitionKey("pk1"));

        // Test CONTAINS with various casings
        var casings = new[] { "CONTAINS", "contains", "Contains" };
        foreach (var fn in casings)
        {
            var iter = container.GetItemQueryIterator<JObject>(
                new QueryDefinition($"SELECT * FROM c WHERE {fn}(c.name, 'Hello')"));
            var results = new List<JObject>();
            while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
            results.Should().ContainSingle($"{fn} should find the item");
        }
    }

    [Fact]
    public async Task SqlFunction_Aggregate_MixedCasing_Works()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        for (var i = 0; i < 5; i++)
            await container.CreateItemAsync(
                JObject.FromObject(new { id = $"{i}", partitionKey = "pk1", value = i }),
                new PartitionKey("pk1"));

        var casings = new[] { "COUNT", "count", "Count" };
        foreach (var fn in casings)
        {
            var iter = container.GetItemQueryIterator<int>(
                new QueryDefinition($"SELECT VALUE {fn}(1) FROM c"));
            var results = new List<int>();
            while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
            results.Should().ContainSingle().Which.Should().Be(5, $"{fn}(1) should return 5");
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Deep-Dive Section L: SQL Property Path Case Sensitivity
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SqlProperty_WrongCasing_ReturnsNoResults()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }),
            new PartitionKey("pk1"));

        // JSON has "name" (lowercase), query uses "Name" (PascalCase) — should not match
        var iter = container.GetItemQueryIterator<JObject>(
            new QueryDefinition("SELECT * FROM c WHERE c.Name = 'Alice'"));
        var results = new List<JObject>();
        while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task SqlProperty_SelectWrongCasing_ReturnsUndefined()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }),
            new PartitionKey("pk1"));

        // SELECT c.Name when JSON has "name" — should return object without "Name" value
        var iter = container.GetItemQueryIterator<JObject>(
            new QueryDefinition("SELECT c.Name FROM c"));
        var results = new List<JObject>();
        while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

        results.Should().ContainSingle();
        var doc = results[0];
        // "Name" should be missing/null since JSON property is "name" (lowercase)
        (doc["Name"] == null || doc["Name"]!.Type == Newtonsoft.Json.Linq.JTokenType.Null)
            .Should().BeTrue();
    }

    [Fact]
    public async Task SqlProperty_NestedPath_CaseSensitive()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", partitionKey = "pk1", address = new { city = "London" } }),
            new PartitionKey("pk1"));

        // Correct casing
        var iterCorrect = container.GetItemQueryIterator<JObject>(
            new QueryDefinition("SELECT * FROM c WHERE c.address.city = 'London'"));
        var correct = new List<JObject>();
        while (iterCorrect.HasMoreResults) correct.AddRange(await iterCorrect.ReadNextAsync());
        correct.Should().ContainSingle();

        // Wrong casing
        var iterWrong = container.GetItemQueryIterator<JObject>(
            new QueryDefinition("SELECT * FROM c WHERE c.address.City = 'London'"));
        var wrong = new List<JObject>();
        while (iterWrong.HasMoreResults) wrong.AddRange(await iterWrong.ReadNextAsync());
        wrong.Should().BeEmpty();
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Deep-Dive Section M: SQL FROM Alias Case Insensitivity
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SqlAlias_UppercaseAlias_Works()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }),
            new PartitionKey("pk1"));

        // Default alias is "c", use uppercase "C" in SELECT
        var iter = container.GetItemQueryIterator<JObject>(
            new QueryDefinition("SELECT C.name FROM C"));
        var results = new List<JObject>();
        while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

        results.Should().ContainSingle();
        results[0]["name"]!.ToString().Should().Be("Alice");
    }

    [Fact]
    public async Task SqlAlias_MixedCaseRef_Works()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }),
            new PartitionKey("pk1"));

        // FROM c AS x, reference as X
        var iter = container.GetItemQueryIterator<JObject>(
            new QueryDefinition("SELECT X.name FROM c AS x WHERE X.name = 'Alice'"));
        var results = new List<JObject>();
        while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

        results.Should().ContainSingle();
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Deep-Dive Section N: UDF SQL Prefix Normalization
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Udf_SqlPrefix_Lowercase_udf_ResolvesCorrectly()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        container.RegisterUdf("myFunc", args => Convert.ToDouble(args[0]) * 10);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", partitionKey = "pk1", value = 5 }),
            new PartitionKey("pk1"));

        var iter = container.GetItemQueryIterator<double>(
            new QueryDefinition("SELECT VALUE udf.myFunc(c.value) FROM c"));
        var results = new List<double>();
        while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

        results.Should().ContainSingle().Which.Should().Be(50);
    }

    [Fact]
    public async Task Udf_SqlPrefix_Uppercase_UDF_ResolvesCorrectly()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        container.RegisterUdf("myFunc", args => Convert.ToDouble(args[0]) * 10);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", partitionKey = "pk1", value = 5 }),
            new PartitionKey("pk1"));

        var iter = container.GetItemQueryIterator<double>(
            new QueryDefinition("SELECT VALUE UDF.myFunc(c.value) FROM c"));
        var results = new List<double>();
        while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

        results.Should().ContainSingle().Which.Should().Be(50);
    }

    [Fact]
    public async Task Udf_SqlPrefix_MixedCase_Udf_ResolvesCorrectly()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        container.RegisterUdf("myFunc", args => Convert.ToDouble(args[0]) * 10);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", partitionKey = "pk1", value = 5 }),
            new PartitionKey("pk1"));

        var iter = container.GetItemQueryIterator<double>(
            new QueryDefinition("SELECT VALUE Udf.myFunc(c.value) FROM c"));
        var results = new List<double>();
        while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

        results.Should().ContainSingle().Which.Should().Be(50);
    }
}
