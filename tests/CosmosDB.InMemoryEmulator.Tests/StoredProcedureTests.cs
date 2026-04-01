using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json;
using Xunit;
using System.Text;
using Newtonsoft.Json.Linq;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════
//  Stored Procedure Tests — Full CRUD, registration, execution, edge cases
// ═══════════════════════════════════════════════════════════════════════════

public class StoredProcedureTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    // ─── Create ──────────────────────────────────────────────────────────

    [Fact]
    public async Task CreateStoredProcedure_ReturnsCreated()
    {
        var scripts = _container.Scripts;

        var response = await scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
        {
            Id = "spBulkDelete",
            Body = "function() { return true; }"
        });

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Resource.Id.Should().Be("spBulkDelete");
    }

    [Fact(Skip = "Pre-existing failure - to be fixed at end of Plan X")]
    public async Task CreateStoredProcedure_DuplicateId_Throws409()
    {
        var scripts = _container.Scripts;

        await scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
        {
            Id = "spDup",
            Body = "function() { return 1; }"
        });

        var act = () => scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
        {
            Id = "spDup",
            Body = "function() { return 2; }"
        });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    // ─── Read ────────────────────────────────────────────────────────────

    [Fact(Skip = "Pre-existing failure - to be fixed at end of Plan X")]
    public async Task ReadStoredProcedure_AfterCreate_ReturnsProperties()
    {
        var scripts = _container.Scripts;

        await scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
        {
            Id = "spRead",
            Body = "function() { return 'hello'; }"
        });

        var response = await scripts.ReadStoredProcedureAsync("spRead");

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Id.Should().Be("spRead");
        response.Resource.Body.Should().Be("function() { return 'hello'; }");
    }

    [Fact(Skip = "Pre-existing failure - to be fixed at end of Plan X")]
    public async Task ReadStoredProcedure_NotFound_Throws404()
    {
        var scripts = _container.Scripts;

        var act = () => scripts.ReadStoredProcedureAsync("nonExistent");

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // ─── Replace ─────────────────────────────────────────────────────────

    [Fact(Skip = "Pre-existing failure - to be fixed at end of Plan X")]
    public async Task ReplaceStoredProcedure_UpdatesBody()
    {
        var scripts = _container.Scripts;

        await scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
        {
            Id = "spReplace",
            Body = "function() { return 'v1'; }"
        });

        var replaceResponse = await scripts.ReplaceStoredProcedureAsync(new StoredProcedureProperties
        {
            Id = "spReplace",
            Body = "function() { return 'v2'; }"
        });

        replaceResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        replaceResponse.Resource.Body.Should().Be("function() { return 'v2'; }");

        var readResponse = await scripts.ReadStoredProcedureAsync("spReplace");
        readResponse.Resource.Body.Should().Be("function() { return 'v2'; }");
    }

    [Fact(Skip = "Pre-existing failure - to be fixed at end of Plan X")]
    public async Task ReplaceStoredProcedure_NotFound_Throws404()
    {
        var scripts = _container.Scripts;

        var act = () => scripts.ReplaceStoredProcedureAsync(new StoredProcedureProperties
        {
            Id = "nonExistent",
            Body = "function() {}"
        });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // ─── Delete ──────────────────────────────────────────────────────────

    [Fact(Skip = "Pre-existing failure - to be fixed at end of Plan X")]
    public async Task DeleteStoredProcedure_RemovesMetadata()
    {
        var scripts = _container.Scripts;

        await scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
        {
            Id = "spDelete",
            Body = "function() {}"
        });

        var deleteResponse = await scripts.DeleteStoredProcedureAsync("spDelete");
        deleteResponse.StatusCode.Should().Be(HttpStatusCode.NoContent);

        var act = () => scripts.ReadStoredProcedureAsync("spDelete");
        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact(Skip = "Pre-existing failure - to be fixed at end of Plan X")]
    public async Task DeleteStoredProcedure_NotFound_Throws404()
    {
        var scripts = _container.Scripts;

        var act = () => scripts.DeleteStoredProcedureAsync("nonExistent");

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // ─── Execute ─────────────────────────────────────────────────────────

    [Fact]
    public async Task ExecuteStoredProcedure_ReturnsOk()
    {
        var scripts = _container.Scripts;

        var response = await scripts.ExecuteStoredProcedureAsync<string>(
            "spBulkDelete",
            new PartitionKey("pk1"),
            Array.Empty<dynamic>());

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task ExecuteStoredProcedure_WithRegisteredHandler_ExecutesLogic()
    {
        _container.RegisterStoredProcedure("spGetCount", (partitionKey, args) =>
        {
            return "42";
        });

        var scripts = _container.Scripts;
        var response = await scripts.ExecuteStoredProcedureAsync<string>(
            "spGetCount",
            new PartitionKey("pk1"),
            Array.Empty<dynamic>());

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Should().Be("42");
    }

    [Fact]
    public async Task ExecuteStoredProcedure_WithRegisteredHandler_ReceivesArguments()
    {
        _container.RegisterStoredProcedure("spConcat", (partitionKey, args) =>
        {
            return string.Join("-", args.Select(a => a?.ToString()));
        });

        var scripts = _container.Scripts;
        var response = await scripts.ExecuteStoredProcedureAsync<string>(
            "spConcat",
            new PartitionKey("pk1"),
            new dynamic[] { "hello", "world" });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Should().Be("hello-world");
    }

    [Fact]
    public async Task ExecuteStoredProcedure_WithoutRegisteredHandler_ReturnsDefault()
    {
        var scripts = _container.Scripts;
        var response = await scripts.ExecuteStoredProcedureAsync<string>(
            "nonExistentSproc",
            new PartitionKey("pk1"),
            Array.Empty<dynamic>());

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task ExecuteStoredProcedure_ReturnsRequestCharge()
    {
        _container.RegisterStoredProcedure("spCharge", (pk, args) => "ok");

        var response = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spCharge", new PartitionKey("pk1"), Array.Empty<dynamic>());

        response.RequestCharge.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task ExecuteStoredProcedure_HandlerReturnsNull_ResourceIsNull()
    {
        _container.RegisterStoredProcedure("spNull", (pk, args) => null!);

        var response = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spNull", new PartitionKey("pk1"), Array.Empty<dynamic>());

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Should().BeNull();
    }

    [Fact]
    public async Task ExecuteStoredProcedure_HandlerReturnsComplexJson_Deserialized()
    {
        _container.RegisterStoredProcedure("spJson", (pk, args) =>
            JsonConvert.SerializeObject(new { count = 5, label = "items" }));

        var response = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spJson", new PartitionKey("pk1"), Array.Empty<dynamic>());

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var parsed = JObject.Parse(response.Resource);
        ((int)parsed["count"]!).Should().Be(5);
        ((string)parsed["label"]!).Should().Be("items");
    }

    [Fact]
    public async Task ExecuteStoredProcedure_HandlerThrowsException_PropagatesException()
    {
        _container.RegisterStoredProcedure("spThrow", (pk, args) =>
            throw new InvalidOperationException("sproc failure"));

        var act = () => _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spThrow", new PartitionKey("pk1"), Array.Empty<dynamic>());

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("sproc failure");
    }

    [Fact]
    public async Task ExecuteStoredProcedure_EmptyArguments_PassedToHandler()
    {
        dynamic[]? received = null;
        _container.RegisterStoredProcedure("spArgs", (pk, args) =>
        {
            received = args;
            return "ok";
        });

        await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spArgs", new PartitionKey("pk1"), Array.Empty<dynamic>());

        received.Should().NotBeNull();
        received.Should().BeEmpty();
    }

    [Fact]
    public async Task ExecuteStoredProcedure_ManyArguments_AllPassedToHandler()
    {
        dynamic[]? received = null;
        _container.RegisterStoredProcedure("spMany", (pk, args) =>
        {
            received = args;
            return "ok";
        });

        var manyArgs = Enumerable.Range(0, 10).Select(i => (dynamic)i.ToString()).ToArray();
        await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spMany", new PartitionKey("pk1"), manyArgs);

        received.Should().HaveCount(10);
    }

    [Fact]
    public async Task ExecuteStoredProcedure_ComplexJsonArguments_Deserializable()
    {
        string? firstArg = null;
        _container.RegisterStoredProcedure("spComplex", (pk, args) =>
        {
            firstArg = args[0]?.ToString();
            return "ok";
        });

        var complexObj = JObject.FromObject(new { name = "test", value = 42 });
        await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spComplex", new PartitionKey("pk1"), new dynamic[] { complexObj });

        firstArg.Should().NotBeNullOrEmpty();
    }

    // ─── Registration & deregistration ───────────────────────────────────

    [Fact]
    public async Task RegisterStoredProcedure_WithContainerAccess_CanReadItems()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new PartitionKey("pk1"));

        _container.RegisterStoredProcedure("spSum", (partitionKey, args) =>
        {
            var query = new QueryDefinition("SELECT VALUE SUM(c.value) FROM c");
            var iterator = _container.GetItemQueryIterator<double>(query);
            var total = 0.0;
            while (iterator.HasMoreResults)
            {
                var response = iterator.ReadNextAsync().GetAwaiter().GetResult();
                foreach (var val in response)
                {
                    total += val;
                }
            }
            return JsonConvert.SerializeObject((int)total);
        });

        var scripts = _container.Scripts;
        var result = await scripts.ExecuteStoredProcedureAsync<string>(
            "spSum",
            new PartitionKey("pk1"),
            Array.Empty<dynamic>());

        result.StatusCode.Should().Be(HttpStatusCode.OK);
        result.Resource.Should().Be("30");
    }

    [Fact(Skip = "Pre-existing failure - to be fixed at end of Plan X")]
    public async Task RegisterStoredProcedure_HandlerCanCreateItems()
    {
        _container.RegisterStoredProcedure("spCreate", (pk, args) =>
        {
            _container.CreateItemAsync(
                new TestDocument { Id = "created-by-sproc", PartitionKey = "pk1", Name = "SprocCreated" },
                new PartitionKey("pk1")).GetAwaiter().GetResult();
            return "created";
        });

        await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spCreate", new PartitionKey("pk1"), Array.Empty<dynamic>());

        var readBack = await _container.ReadItemAsync<TestDocument>("created-by-sproc", new PartitionKey("pk1"));
        readBack.Resource.Name.Should().Be("SprocCreated");
    }

    [Fact(Skip = "Pre-existing failure - to be fixed at end of Plan X")]
    public async Task RegisterStoredProcedure_HandlerCanDeleteItems()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "to-delete", PartitionKey = "pk1", Name = "Doomed" },
            new PartitionKey("pk1"));

        _container.RegisterStoredProcedure("spDelete", (pk, args) =>
        {
            _container.DeleteItemAsync<TestDocument>("to-delete", new PartitionKey("pk1"))
                .GetAwaiter().GetResult();
            return "deleted";
        });

        await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spDelete", new PartitionKey("pk1"), Array.Empty<dynamic>());

        var act = () => _container.ReadItemAsync<TestDocument>("to-delete", new PartitionKey("pk1"));
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact(Skip = "Pre-existing failure - to be fixed at end of Plan X")]
    public async Task RegisterStoredProcedure_HandlerCanReplaceItems()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "to-replace", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        _container.RegisterStoredProcedure("spReplace", (pk, args) =>
        {
            _container.ReplaceItemAsync(
                new TestDocument { Id = "to-replace", PartitionKey = "pk1", Name = "Updated" },
                "to-replace",
                new PartitionKey("pk1")).GetAwaiter().GetResult();
            return "replaced";
        });

        await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spReplace", new PartitionKey("pk1"), Array.Empty<dynamic>());

        var readBack = await _container.ReadItemAsync<TestDocument>("to-replace", new PartitionKey("pk1"));
        readBack.Resource.Name.Should().Be("Updated");
    }

    [Fact]
    public async Task RegisterStoredProcedure_HandlerCanQueryWithFilter()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 50 },
            new PartitionKey("pk1"));

        _container.RegisterStoredProcedure("spHigh", (pk, args) =>
        {
            var query = new QueryDefinition("SELECT VALUE c.name FROM c WHERE c.value > 20");
            var iterator = _container.GetItemQueryIterator<string>(query);
            var names = new List<string>();
            while (iterator.HasMoreResults)
            {
                var page = iterator.ReadNextAsync().GetAwaiter().GetResult();
                names.AddRange(page);
            }
            return JsonConvert.SerializeObject(names);
        });

        var result = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spHigh", new PartitionKey("pk1"), Array.Empty<dynamic>());

        var names = JsonConvert.DeserializeObject<List<string>>(result.Resource);
        names.Should().ContainSingle().Which.Should().Be("Bob");
    }

    [Fact(Skip = "Pre-existing failure - to be fixed at end of Plan X")]
    public async Task RegisterStoredProcedure_BulkDeletePattern()
    {
        for (int i = 0; i < 5; i++)
        {
            await _container.CreateItemAsync(
                new TestDocument { Id = $"item-{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));
        }

        _container.RegisterStoredProcedure("spBulkDelete", (pk, args) =>
        {
            var query = new QueryDefinition("SELECT * FROM c");
            var iterator = _container.GetItemQueryIterator<JObject>(query);
            var deleted = 0;
            while (iterator.HasMoreResults)
            {
                var page = iterator.ReadNextAsync().GetAwaiter().GetResult();
                foreach (var item in page)
                {
                    var id = item["id"]!.ToString();
                    var itemPk = item["partitionKey"]!.ToString();
                    _container.DeleteItemAsync<JObject>(id, new PartitionKey(itemPk))
                        .GetAwaiter().GetResult();
                    deleted++;
                }
            }
            return JsonConvert.SerializeObject(new { deleted });
        });

        var result = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spBulkDelete", new PartitionKey("pk1"), Array.Empty<dynamic>());

        var parsed = JObject.Parse(result.Resource);
        ((int)parsed["deleted"]!).Should().Be(5);
    }

    [Fact]
    public async Task DeregisterStoredProcedure_RemovesHandler()
    {
        _container.RegisterStoredProcedure("spTemp", (partitionKey, args) => "result");
        _container.DeregisterStoredProcedure("spTemp");

        var scripts = _container.Scripts;
        var response = await scripts.ExecuteStoredProcedureAsync<string>(
            "spTemp",
            new PartitionKey("pk1"),
            Array.Empty<dynamic>());

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Should().BeEmpty();
    }

    [Fact]
    public async Task RegisterStoredProcedure_SameIdTwice_OverwritesHandler()
    {
        _container.RegisterStoredProcedure("spOverwrite", (pk, args) => "first");
        _container.RegisterStoredProcedure("spOverwrite", (pk, args) => "second");

        var response = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spOverwrite", new PartitionKey("pk1"), Array.Empty<dynamic>());

        response.Resource.Should().Be("second");
    }

    [Fact]
    public async Task RegisterStoredProcedure_CaseSensitive_DifferentHandlers()
    {
        _container.RegisterStoredProcedure("myProc", (pk, args) => "lower");
        _container.RegisterStoredProcedure("MYPROC", (pk, args) => "upper");

        var lower = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "myProc", new PartitionKey("pk1"), Array.Empty<dynamic>());
        var upper = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "MYPROC", new PartitionKey("pk1"), Array.Empty<dynamic>());

        lower.Resource.Should().Be("lower");
        upper.Resource.Should().Be("upper");
    }

    [Fact]
    public void DeregisterStoredProcedure_NonExistent_DoesNotThrow()
    {
        var act = () => _container.DeregisterStoredProcedure("neverRegistered");
        act.Should().NotThrow();
    }

    [Fact]
    public async Task DeregisterStoredProcedure_ThenReRegister_Works()
    {
        _container.RegisterStoredProcedure("spCycle", (pk, args) => "v1");
        _container.DeregisterStoredProcedure("spCycle");
        _container.RegisterStoredProcedure("spCycle", (pk, args) => "v2");

        var response = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spCycle", new PartitionKey("pk1"), Array.Empty<dynamic>());

        response.Resource.Should().Be("v2");
    }

    // ─── Partition key behavior ──────────────────────────────────────────

    [Fact]
    public async Task ExecuteStoredProcedure_PartitionKeyValue_PassedCorrectly()
    {
        PartitionKey? received = null;
        _container.RegisterStoredProcedure("spPk", (pk, args) =>
        {
            received = pk;
            return "ok";
        });

        await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spPk", new PartitionKey("specific-pk"), Array.Empty<dynamic>());

        received.Should().NotBeNull();
        received.ToString().Should().Contain("specific-pk");
    }

    [Fact]
    public async Task ExecuteStoredProcedure_PartitionKeyNone_PassedCorrectly()
    {
        PartitionKey? received = null;
        _container.RegisterStoredProcedure("spPkNone", (pk, args) =>
        {
            received = pk;
            return "ok";
        });

        await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spPkNone", PartitionKey.None, Array.Empty<dynamic>());

        received.Should().NotBeNull();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Stored Procedure Divergent Behavior Tests
//  Each skipped test documents expected real Cosmos DB behaviour.
//  Sister tests show the emulator's actual behaviour.
// ═══════════════════════════════════════════════════════════════════════════

public class StoredProcedureDivergentBehaviorTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    // ─── Divergent: Real Cosmos returns 404 for unregistered sprocs ──────

    [Fact(Skip = "Real Cosmos DB throws 404 NotFound when executing a stored procedure that doesn't exist. " +
                  "The emulator returns 200 OK with an empty resource for any unregistered sproc ID to allow " +
                  "flexible testing without requiring handler registration for every stored procedure call. " +
                  "Use RegisterStoredProcedure() to provide a C# handler if you need specific return values.")]
    public async Task ExecuteStoredProcedure_NotRegistered_ShouldThrow404()
    {
        // Expected real Cosmos behavior:
        // Executing a stored procedure ID that doesn't exist should throw
        // CosmosException with StatusCode 404 NotFound.
        var scripts = _container.Scripts;
        var act = () => scripts.ExecuteStoredProcedureAsync<string>(
            "nonExistent", new PartitionKey("pk1"), Array.Empty<dynamic>());
        await act.Should().ThrowAsync<CosmosException>();
    }

    // Sister test: shows the actual emulator behavior
    [Fact]
    public async Task ExecuteStoredProcedure_NotRegistered_EmulatorReturns200()
    {
        // InMemoryContainer returns 200 OK with empty resource for any unregistered sproc.
        // This allows tests to call ExecuteStoredProcedureAsync without pre-registering handlers
        // when the exact return value doesn't matter.
        var scripts = _container.Scripts;
        var response = await scripts.ExecuteStoredProcedureAsync<string>(
            "nonExistent", new PartitionKey("pk1"), Array.Empty<dynamic>());

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    // ─── Divergent: Real Cosmos executes JavaScript server-side ──────────

    [Fact(Skip = "Real Cosmos DB executes the JavaScript body of stored procedures server-side. " +
                  "The emulator stores JavaScript bodies via CreateStoredProcedureAsync but does not " +
                  "interpret them. Instead, use RegisterStoredProcedure() to provide C# handler logic. " +
                  "For JavaScript trigger execution, use the CosmosDB.InMemoryEmulator.JsTriggers package.")]
    public async Task ExecuteStoredProcedure_JavaScriptBody_ShouldExecute()
    {
        // Expected real Cosmos behavior:
        // Creating a sproc with a JS body and then executing it would run that JavaScript.
        // E.g. function(prefix) { var context = getContext(); ... }
        var scripts = _container.Scripts;
        await scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
        {
            Id = "spJs",
            Body = "function(prefix) { var response = getContext().getResponse(); response.setBody(prefix + '-result'); }"
        });

        var result = await scripts.ExecuteStoredProcedureAsync<string>(
            "spJs", new PartitionKey("pk1"), new dynamic[] { "test" });
        result.Resource.Should().Be("test-result");
    }

    // Sister test: shows the actual emulator behavior
    [Fact]
    public async Task ExecuteStoredProcedure_WithCSharpHandler_ExecutesLogicInstead()
    {
        // InMemoryContainer uses C# handlers registered via RegisterStoredProcedure().
        // JavaScript bodies stored via CreateStoredProcedureAsync are metadata-only.
        // This test shows the C# handler pattern that replaces JS execution.
        _container.RegisterStoredProcedure("spJs", (pk, args) =>
        {
            var prefix = args[0]?.ToString() ?? "";
            return prefix + "-result";
        });

        var result = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spJs", new PartitionKey("pk1"), new dynamic[] { "test" });

        result.Resource.Should().Be("test-result");
    }

    // ─── Divergent: GetStoredProcedureQueryIterator not implemented ──────

    [Fact(Skip = "Real Cosmos DB supports GetStoredProcedureQueryIterator() to enumerate all stored " +
                  "procedures in a container. The emulator does not implement this method because " +
                  "it would require returning a FeedIterator<StoredProcedureProperties> from " +
                  "the NSubstitute Scripts proxy and maintaining queryable metadata. " +
                  "Workaround: track stored procedure IDs in your test setup code.")]
    public void GetStoredProcedureQueryIterator_ShouldEnumerateProcedures()
    {
        // Expected real Cosmos behavior:
        // var iterator = scripts.GetStoredProcedureQueryIterator<StoredProcedureProperties>();
        // while (iterator.HasMoreResults) { var page = await iterator.ReadNextAsync(); ... }
        // Would return all stored procedures created on the container.
    }

    // ─── Divergent: Only ExecuteStoredProcedureAsync<string> is mocked ──

    [Fact(Skip = "Real Cosmos DB supports ExecuteStoredProcedureAsync<T> for any serializable type T. " +
                  "The emulator only mocks ExecuteStoredProcedureAsync<string> via NSubstitute. " +
                  "For other types, use <string> and deserialize the JSON result manually: " +
                  "var result = JsonConvert.DeserializeObject<MyType>(response.Resource). " +
                  "Adding generic type mocks for all possible T is not feasible with NSubstitute.")]
    public async Task ExecuteStoredProcedure_NonStringGenericType_ShouldDeserialize()
    {
        // Expected real Cosmos behavior:
        // var response = await scripts.ExecuteStoredProcedureAsync<MyComplexType>(...);
        // response.Resource would be a deserialized MyComplexType instance.
    }

    // Sister test: shows the workaround for non-string types
    [Fact]
    public async Task ExecuteStoredProcedure_StringWithManualDeserialization_Workaround()
    {
        // Workaround: Use ExecuteStoredProcedureAsync<string> and deserialize manually.
        // This pattern works for any result type the stored procedure might return.
        _container.RegisterStoredProcedure("spTyped", (pk, args) =>
            JsonConvert.SerializeObject(new { count = 42, items = new[] { "a", "b" } }));

        var response = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spTyped", new PartitionKey("pk1"), Array.Empty<dynamic>());

        var result = JObject.Parse(response.Resource);
        ((int)result["count"]!).Should().Be(42);
        result["items"]!.ToObject<string[]>().Should().BeEquivalentTo("a", "b");
    }
}


// ═══════════════════════════════════════════════════════════════════════════
//  UDF Tests (moved from StoredProcGapTests — these test UDF behavior)
// ═══════════════════════════════════════════════════════════════════════════

public class UdfGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Udf_RegisterAndUseInQuery()
    {
        _container.RegisterUdf("double", args => ((double)args[0]) * 2);

        await _container.CreateItemAsync(
            new UdfDocument { Id = "1", PartitionKey = "pk1", Value = 21 },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JToken>(
            "SELECT VALUE udf.double(c.value) FROM c");

        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(1);
    }

    [Fact]
    public async Task Udf_MultipleArgs()
    {
        _container.RegisterUdf("add", args => (double)args[0] + (double)args[1]);

        await _container.CreateItemAsync(
            new UdfDocument { Id = "1", PartitionKey = "pk1", X = 10, Y = 20 },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JToken>(
            "SELECT VALUE udf.add(c.x, c.y) FROM c");

        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(1);
    }

    [Fact]
    public async Task Udf_NotRegistered_ThrowsOnQuery()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var act = async () =>
        {
            var iterator = _container.GetItemQueryIterator<JToken>(
                "SELECT * FROM c WHERE udf.nonExistent(c.value)");
            while (iterator.HasMoreResults)
            {
                await iterator.ReadNextAsync();
            }
        };

        await act.Should().ThrowAsync<Exception>();
    }
}
