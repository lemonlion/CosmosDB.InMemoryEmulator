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

        await act.Should().ThrowAsync<NotSupportedException>();
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

        // Call with WRONG casing — should not find the handler
        var response = await scripts.ExecuteStoredProcedureAsync<string>(
            "MYPROC",
            new PartitionKey("pk1"),
            Array.Empty<dynamic>());

        // In real Cosmos DB, a sproc with different casing is a different sproc.
        // The registered handler for "myProc" should NOT execute for "MYPROC".
        // The default behaviour (no handler found) returns OK with empty body,
        // so we verify the handler's return value was NOT used.
        response.Resource.Should().NotBe("result");
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
}
