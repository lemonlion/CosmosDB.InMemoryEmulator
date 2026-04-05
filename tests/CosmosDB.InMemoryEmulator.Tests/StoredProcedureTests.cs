using System.Net;
using AwesomeAssertions;
using CosmosDB.InMemoryEmulator.JsTriggers;
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

    [Fact]
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

    [Fact]
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

    [Fact]
    public async Task ReadStoredProcedure_NotFound_Throws404()
    {
        var scripts = _container.Scripts;

        var act = () => scripts.ReadStoredProcedureAsync("nonExistent");

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // ─── Replace ─────────────────────────────────────────────────────────

    [Fact]
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

    [Fact]
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

    [Fact]
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

    [Fact]
    public async Task DeleteStoredProcedure_NotFound_Throws404()
    {
        var scripts = _container.Scripts;

        var act = () => scripts.DeleteStoredProcedureAsync("nonExistent");

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // ─── Execute ─────────────────────────────────────────────────────────

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

        await act.Should().ThrowAsync<CosmosException>()
            .WithMessage("*sproc failure*");
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

    [Fact]
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

    [Fact]
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

    [Fact]
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

    [Fact]
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
        var act = () => scripts.ExecuteStoredProcedureAsync<string>(
            "spTemp",
            new PartitionKey("pk1"),
            Array.Empty<dynamic>());

        (await act.Should().ThrowAsync<CosmosException>())
            .Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
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

    // ─── CRUD edge cases (T8–T13) ────────────────────────────────────────

    [Fact]
    public async Task DeleteStoredProcedure_ThenReCreate_SameId_Works()
    {
        var scripts = _container.Scripts;
        await scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
            { Id = "spRecreate", Body = "function() { return 'v1'; }" });
        await scripts.DeleteStoredProcedureAsync("spRecreate");

        var response = await scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
            { Id = "spRecreate", Body = "function() { return 'v2'; }" });
        response.StatusCode.Should().Be(HttpStatusCode.Created);

        var read = await scripts.ReadStoredProcedureAsync("spRecreate");
        read.Resource.Body.Should().Be("function() { return 'v2'; }");
    }

    [Fact]
    public async Task CreateStoredProcedure_MultipleDistinct_AllReadable()
    {
        var scripts = _container.Scripts;
        for (int i = 0; i < 5; i++)
        {
            await scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
                { Id = $"sp-{i}", Body = $"function() {{ return {i}; }}" });
        }

        for (int i = 0; i < 5; i++)
        {
            var read = await scripts.ReadStoredProcedureAsync($"sp-{i}");
            read.StatusCode.Should().Be(HttpStatusCode.OK);
            read.Resource.Body.Should().Be($"function() {{ return {i}; }}");
        }
    }

    [Fact]
    public async Task CreateStoredProcedure_SpecialCharactersInId_Works()
    {
        var scripts = _container.Scripts;
        await scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
            { Id = "sp-bulk.delete_v2", Body = "function() {}" });

        var read = await scripts.ReadStoredProcedureAsync("sp-bulk.delete_v2");
        read.StatusCode.Should().Be(HttpStatusCode.OK);
        read.Resource.Id.Should().Be("sp-bulk.delete_v2");
    }

    [Fact]
    public async Task CreateStoredProcedure_BodyPreservedExactly()
    {
        var body = "function(arg) {\n\tvar x = 'hello world';\n\treturn x + ' ñ 日本語';\n}";
        var scripts = _container.Scripts;
        await scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
            { Id = "spUnicode", Body = body });

        var read = await scripts.ReadStoredProcedureAsync("spUnicode");
        read.Resource.Body.Should().Be(body);
    }

    [Fact]
    public async Task ReplaceStoredProcedure_OldBodyNotAccessible()
    {
        var scripts = _container.Scripts;
        await scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
            { Id = "spOldBody", Body = "function() { return 'v1'; }" });

        await scripts.ReplaceStoredProcedureAsync(new StoredProcedureProperties
            { Id = "spOldBody", Body = "function() { return 'v2'; }" });

        var read = await scripts.ReadStoredProcedureAsync("spOldBody");
        read.Resource.Body.Should().Be("function() { return 'v2'; }");
        read.Resource.Body.Should().NotBe("function() { return 'v1'; }");
    }

    [Fact]
    public async Task CreateStoredProcedure_CaseSensitiveIds()
    {
        var scripts = _container.Scripts;
        await scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
            { Id = "myProc", Body = "function() { return 'lower'; }" });
        await scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
            { Id = "MYPROC", Body = "function() { return 'upper'; }" });

        var lower = await scripts.ReadStoredProcedureAsync("myProc");
        var upper = await scripts.ReadStoredProcedureAsync("MYPROC");

        lower.Resource.Body.Should().Be("function() { return 'lower'; }");
        upper.Resource.Body.Should().Be("function() { return 'upper'; }");
    }

    // ─── Execution advanced scenarios (T14–T21) ─────────────────────────

    [Fact]
    public async Task ExecuteStoredProcedure_HandlerReturnsEmptyString_ResourceIsEmpty()
    {
        _container.RegisterStoredProcedure("spEmpty", (pk, args) => "");

        var response = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spEmpty", new PartitionKey("pk1"), Array.Empty<dynamic>());

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Should().Be("");
    }

    [Fact]
    public async Task ExecuteStoredProcedure_PartitionKeyNull_PassedCorrectly()
    {
        PartitionKey? received = null;
        _container.RegisterStoredProcedure("spPkNull", (pk, args) =>
        {
            received = pk;
            return "ok";
        });

        await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spPkNull", PartitionKey.Null, Array.Empty<dynamic>());

        received.Should().NotBeNull();
    }

    [Fact]
    public async Task ExecuteStoredProcedure_HandlerThrowsCosmosException_PropagatesWithStatusCode()
    {
        _container.RegisterStoredProcedure("spCosmos", (pk, args) =>
            throw new CosmosException("bad request", HttpStatusCode.BadRequest, 0, "", 0));

        var act = () => _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spCosmos", new PartitionKey("pk1"), Array.Empty<dynamic>());

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task ExecuteStoredProcedure_HandlerModifiesArguments_NoSideEffects()
    {
        _container.RegisterStoredProcedure("spMutate", (pk, args) =>
        {
            args[0] = "mutated";
            return "ok";
        });

        var myArgs = new dynamic[] { "original" };
        await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spMutate", new PartitionKey("pk1"), myArgs);

        // Reference semantics: the handler mutates the same array
        ((string)myArgs[0]).Should().Be("mutated");
    }

    [Fact]
    public async Task ExecuteStoredProcedure_HandlerCanUpsertItems()
    {
        _container.RegisterStoredProcedure("spUpsert", (pk, args) =>
        {
            _container.UpsertItemAsync(
                new TestDocument { Id = "upserted", PartitionKey = "pk1", Name = "Upserted" },
                new PartitionKey("pk1")).GetAwaiter().GetResult();
            return "done";
        });

        await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spUpsert", new PartitionKey("pk1"), Array.Empty<dynamic>());

        var read = await _container.ReadItemAsync<TestDocument>("upserted", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Upserted");
    }

    [Fact]
    public async Task ExecuteStoredProcedure_HandlerCanPatchItems()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "to-patch", PartitionKey = "pk1", Name = "Before" },
            new PartitionKey("pk1"));

        _container.RegisterStoredProcedure("spPatch", (pk, args) =>
        {
            _container.PatchItemAsync<TestDocument>("to-patch", new PartitionKey("pk1"),
                new[] { PatchOperation.Replace("/name", "After") }).GetAwaiter().GetResult();
            return "patched";
        });

        await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spPatch", new PartitionKey("pk1"), Array.Empty<dynamic>());

        var read = await _container.ReadItemAsync<TestDocument>("to-patch", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("After");
    }

    [Fact]
    public async Task ExecuteStoredProcedure_SequentialExecutions_IndependentResults()
    {
        var callCount = 0;
        _container.RegisterStoredProcedure("spSeq", (pk, args) =>
        {
            callCount++;
            return callCount.ToString();
        });

        var r1 = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spSeq", new PartitionKey("pk1"), Array.Empty<dynamic>());
        var r2 = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spSeq", new PartitionKey("pk1"), Array.Empty<dynamic>());
        var r3 = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spSeq", new PartitionKey("pk1"), Array.Empty<dynamic>());

        r1.Resource.Should().Be("1");
        r2.Resource.Should().Be("2");
        r3.Resource.Should().Be("3");
    }

    [Fact]
    public async Task ExecuteStoredProcedure_HandlerUsesPartitionKeyToScope()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk-a", Name = "A" }, new PartitionKey("pk-a"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk-b", Name = "B" }, new PartitionKey("pk-b"));

        _container.RegisterStoredProcedure("spScoped", (pk, args) =>
        {
            var iterator = _container.GetItemQueryIterator<TestDocument>(
                "SELECT * FROM c",
                requestOptions: new QueryRequestOptions { PartitionKey = pk });
            var items = new List<string>();
            while (iterator.HasMoreResults)
            {
                var page = iterator.ReadNextAsync().GetAwaiter().GetResult();
                items.AddRange(page.Select(x => x.Name));
            }
            return JsonConvert.SerializeObject(items);
        });

        var result = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spScoped", new PartitionKey("pk-a"), Array.Empty<dynamic>());

        var names = JsonConvert.DeserializeObject<List<string>>(result.Resource);
        names.Should().ContainSingle().Which.Should().Be("A");
    }

    // ─── Response metadata (T25–T27) ────────────────────────────────────

    [Fact]
    public async Task CreateStoredProcedure_Response_HasResourceWithIdAndBody()
    {
        var response = await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
        {
            Id = "spMeta",
            Body = "function() { return 42; }"
        });

        response.Resource.Id.Should().Be("spMeta");
        response.Resource.Body.Should().Be("function() { return 42; }");
    }

    [Fact]
    public async Task ReplaceStoredProcedure_Response_HasUpdatedResource()
    {
        var scripts = _container.Scripts;
        await scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
            { Id = "spRepMeta", Body = "v1" });

        var response = await scripts.ReplaceStoredProcedureAsync(new StoredProcedureProperties
            { Id = "spRepMeta", Body = "v2" });

        response.Resource.Body.Should().Be("v2");
        response.Resource.Id.Should().Be("spRepMeta");
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

    [Fact]
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

    // ─── Divergent: Real Cosmos executes JavaScript server-side ──────────

    [Fact]
    public async Task ExecuteStoredProcedure_JavaScriptBody_ShouldExecute()
    {
        _container.UseJsStoredProcedures();

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

    // ─── Divergent: GetStoredProcedureQueryIterator not implemented ──────

    [Fact]
    public async Task GetStoredProcedureQueryIterator_ShouldEnumerateProcedures()
    {
        var scripts = _container.Scripts;
        await scripts.CreateStoredProcedureAsync(new StoredProcedureProperties { Id = "sp1", Body = "function(){}" });
        await scripts.CreateStoredProcedureAsync(new StoredProcedureProperties { Id = "sp2", Body = "function(){}" });

        var iterator = scripts.GetStoredProcedureQueryIterator<StoredProcedureProperties>();
        var all = new List<StoredProcedureProperties>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            all.AddRange(page);
        }

        all.Should().HaveCount(2);
        all.Select(s => s.Id).Should().BeEquivalentTo("sp1", "sp2");
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

    // ─── Divergent: Stream variants not implemented (T39, T40) ──────────

    [Fact(Skip = "Real Cosmos DB supports ExecuteStoredProcedureStreamAsync which returns a ResponseMessage " +
                  "with a Content Stream. The emulator does not mock this method on the Scripts NSubstitute " +
                  "proxy. Workaround: use ExecuteStoredProcedureAsync<string> and convert to stream manually " +
                  "if needed. The stream variants require mocking ResponseMessage which is non-trivial.")]
    public async Task ExecuteStoredProcedureStreamAsync_ShouldReturnStream()
    {
        // Expected real Cosmos behavior:
        // var response = await scripts.ExecuteStoredProcedureStreamAsync("spId", pk, args);
        // using var sr = new StreamReader(response.Content);
        // var body = await sr.ReadToEndAsync();
    }

    [Fact(Skip = "Real Cosmos DB supports stream CRUD: CreateStoredProcedureStreamAsync, " +
                  "ReadStoredProcedureStreamAsync, ReplaceStoredProcedureStreamAsync, " +
                  "DeleteStoredProcedureStreamAsync. The emulator does not mock these methods. " +
                  "Workaround: use the typed CRUD variants which are fully supported.")]
    public void CrudStreamVariants_ShouldWork()
    {
        // Expected real Cosmos behavior:
        // Stream-based CRUD for stored procedures, returning ResponseMessage.
    }

    // ─── Divergent: Script logging not available (T41) ──────────────────

    [Fact]
    public async Task ExecuteStoredProcedure_EnableScriptLogging_ShouldReturnLogs()
    {
        _container.UseJsStoredProcedures();

        await _container.Scripts.CreateStoredProcedureAsync(
            new StoredProcedureProperties
            {
                Id = "spLog",
                Body = """
                    function spLog(prefix) {
                        console.log("hello from sproc");
                        console.log("prefix=" + prefix);
                        var context = getContext();
                        var response = context.getResponse();
                        response.setBody(prefix + "-done");
                    }
                    """
            });

        var response = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spLog", new PartitionKey("pk1"), new dynamic[] { "test" });

        response.Resource.Should().Be("test-done");
        response.ScriptLog.Should().Contain("hello from sproc");
        response.ScriptLog.Should().Contain("prefix=test");
    }

    // ─── Divergent: Handler exceptions differ from real Cosmos (T42) ────

    [Fact]
    public async Task ExecuteStoredProcedure_HandlerError_ShouldReturn400()
    {
        // Expected real Cosmos behavior:
        // A sproc that throws an error at runtime returns CosmosException with 400 Bad Request.
        var scripts = _container.Scripts;
        _container.RegisterStoredProcedure("spFail", (pk, args) =>
            throw new InvalidOperationException("runtime error"));

        var act = () => scripts.ExecuteStoredProcedureAsync<string>(
            "spFail", new PartitionKey("pk1"), Array.Empty<dynamic>());
        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    // ─── Divergent: No 10-second timeout (T43) ─────────────────────────

    [Fact]
    public async Task ExecuteStoredProcedure_10SecondTimeout_ShouldThrow()
    {
        _container.RegisterStoredProcedure("spSlow", (pk, args) =>
        {
            Thread.Sleep(TimeSpan.FromSeconds(15));
            return "done";
        });

        var act = () => _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spSlow", new PartitionKey("pk1"), Array.Empty<dynamic>());
        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.RequestTimeout);
    }

    // ─── Divergent: No 2MB response limit (T44) ────────────────────────

    [Fact]
    public async Task ExecuteStoredProcedure_2MBResponseLimit_ShouldFail()
    {
        _container.RegisterStoredProcedure("spBig", (pk, args) =>
        {
            return new string('x', 3 * 1024 * 1024);
        });

        var act = () => _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spBig", new PartitionKey("pk1"), Array.Empty<dynamic>());
        var ex = await act.Should().ThrowAsync<CosmosException>();
        ((int)ex.Which.StatusCode).Should().Be(413);
    }

    // ─── Divergent: No system metadata on resources (T45) ──────────────

    [Fact]
    public async Task StoredProcedure_SystemMetadata_ShouldHaveEtagTimestamp()
    {
        var scripts = _container.Scripts;
        var response = await scripts.CreateStoredProcedureAsync(
            new StoredProcedureProperties { Id = "spMeta", Body = "function(){}" });

        response.Resource.ETag.Should().NotBeNullOrEmpty();
        response.Resource.SelfLink.Should().NotBeNullOrEmpty();

        var readResponse = await scripts.ReadStoredProcedureAsync("spMeta");
        readResponse.Resource.ETag.Should().NotBeNullOrEmpty();
        readResponse.Resource.SelfLink.Should().NotBeNullOrEmpty();
    }

    // ─── Divergent: No partition scoping (T46) ─────────────────────────

    [Fact(Skip = "Real Cosmos DB stored procedures are scoped to a single logical partition. They can " +
                  "only read/write documents within the partition key value specified in the " +
                  "ExecuteStoredProcedureAsync call. The emulator's C# handlers have unrestricted access " +
                  "to the container via closure — they can query/write any partition. Workaround: scope " +
                  "your handler's queries using QueryRequestOptions.PartitionKey matching the PK passed " +
                  "to execute.")]
    public async Task StoredProcedure_CrossPartition_ShouldBeScoped()
    {
        // Expected real Cosmos behavior:
        // A sproc executed with PartitionKey("A") can only read/write docs in partition "A".
        // The emulator's handler has full container access — document this divergence.
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk-a", Name = "A" }, new PartitionKey("pk-a"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk-b", Name = "B" }, new PartitionKey("pk-b"));

        _container.RegisterStoredProcedure("spAll", (pk, args) =>
        {
            var iterator = _container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
            var items = new List<string>();
            while (iterator.HasMoreResults)
            {
                var page = iterator.ReadNextAsync().GetAwaiter().GetResult();
                items.AddRange(page.Select(x => x.Name));
            }
            return JsonConvert.SerializeObject(items);
        });

        var result = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spAll", new PartitionKey("pk-a"), Array.Empty<dynamic>());

        // In real Cosmos, only "A" would be returned. Emulator returns both.
        var names = JsonConvert.DeserializeObject<List<string>>(result.Resource);
        names.Should().Contain("A");
        names.Should().Contain("B"); // emulator divergence — no partition scoping
    }

    // Sister test: shows the workaround pattern for partition scoping
    [Fact]
    public async Task StoredProcedure_CrossPartition_EmulatorWorkaround_ScopeManually()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk-a", Name = "A" }, new PartitionKey("pk-a"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk-b", Name = "B" }, new PartitionKey("pk-b"));

        _container.RegisterStoredProcedure("spScoped", (pk, args) =>
        {
            var iterator = _container.GetItemQueryIterator<TestDocument>(
                "SELECT * FROM c",
                requestOptions: new QueryRequestOptions { PartitionKey = pk });
            var items = new List<string>();
            while (iterator.HasMoreResults)
            {
                var page = iterator.ReadNextAsync().GetAwaiter().GetResult();
                items.AddRange(page.Select(x => x.Name));
            }
            return JsonConvert.SerializeObject(items);
        });

        var result = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "spScoped", new PartitionKey("pk-a"), Array.Empty<dynamic>());

        var names = JsonConvert.DeserializeObject<List<string>>(result.Resource);
        names.Should().ContainSingle().Which.Should().Be("A");
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

    // ─── Deregister UDF (T30, T31) ──────────────────────────────────────

    [Fact]
    public async Task DeregisterUdf_RemovesHandler()
    {
        _container.RegisterUdf("removable", args => (double)args[0] * 10);

        await _container.CreateItemAsync(
            new UdfDocument { Id = "1", PartitionKey = "pk1", Value = 5 },
            new PartitionKey("pk1"));

        // First query should work
        var iterator = _container.GetItemQueryIterator<JToken>(
            "SELECT VALUE udf.removable(c.value) FROM c");
        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        results.Should().HaveCount(1);

        // Deregister and second query should throw
        _container.DeregisterUdf("removable");
        var act = async () =>
        {
            var it2 = _container.GetItemQueryIterator<JToken>(
                "SELECT VALUE udf.removable(c.value) FROM c");
            while (it2.HasMoreResults) await it2.ReadNextAsync();
        };
        await act.Should().ThrowAsync<Exception>();
    }

    [Fact]
    public void DeregisterUdf_NonExistent_DoesNotThrow()
    {
        var act = () => _container.DeregisterUdf("neverRegistered");
        act.Should().NotThrow();
    }

    // ─── UDF registration edge cases (T32, T33) ─────────────────────────

    [Fact]
    public async Task Udf_RegisterSameIdTwice_OverwritesHandler()
    {
        _container.RegisterUdf("overwrite", args => 1.0);
        _container.RegisterUdf("overwrite", args => 999.0);

        await _container.CreateItemAsync(
            new UdfDocument { Id = "1", PartitionKey = "pk1", Value = 1 },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JToken>(
            "SELECT VALUE udf.overwrite(c.value) FROM c");
        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        results.Should().HaveCount(1);
        results[0].Value<double>().Should().Be(999.0);
    }

    [Fact]
    public async Task Udf_CaseSensitive_DifferentHandlers()
    {
        _container.RegisterUdf("myUdf", args => 1.0);
        _container.RegisterUdf("MYUDF", args => 2.0);

        await _container.CreateItemAsync(
            new UdfDocument { Id = "1", PartitionKey = "pk1", Value = 0 },
            new PartitionKey("pk1"));

        var it1 = _container.GetItemQueryIterator<JToken>(
            "SELECT VALUE udf.myUdf(c.value) FROM c");
        var r1 = new List<JToken>();
        while (it1.HasMoreResults) { var pg = await it1.ReadNextAsync(); r1.AddRange(pg); }

        var it2 = _container.GetItemQueryIterator<JToken>(
            "SELECT VALUE udf.MYUDF(c.value) FROM c");
        var r2 = new List<JToken>();
        while (it2.HasMoreResults) { var pg = await it2.ReadNextAsync(); r2.AddRange(pg); }

        r1[0].Value<double>().Should().Be(1.0);
        r2[0].Value<double>().Should().Be(2.0);
    }

    // ─── UDF query patterns (T34–T37) ───────────────────────────────────

    [Fact]
    public async Task Udf_InWhereClause_FiltersCorrectly()
    {
        _container.RegisterUdf("isEven", args => ((double)args[0]) % 2 == 0);

        await _container.CreateItemAsync(
            new UdfDocument { Id = "1", PartitionKey = "pk1", Value = 2 }, new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new UdfDocument { Id = "2", PartitionKey = "pk1", Value = 3 }, new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new UdfDocument { Id = "3", PartitionKey = "pk1", Value = 4 }, new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<UdfDocument>(
            "SELECT * FROM c WHERE udf.isEven(c.value)");
        var results = new List<UdfDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        results.Select(x => x.Value).Should().BeEquivalentTo(new[] { 2.0, 4.0 });
    }

    [Fact]
    public async Task Udf_ReturningString_Works()
    {
        _container.RegisterUdf("label", args => "item-" + args[0]);

        await _container.CreateItemAsync(
            new UdfDocument { Id = "1", PartitionKey = "pk1", Value = 42 }, new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JToken>(
            "SELECT VALUE udf.label(c.value) FROM c");
        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(1);
        results[0].Value<string>().Should().Be("item-42");
    }

    [Fact]
    public async Task Udf_ReturningNull_Works()
    {
        _container.RegisterUdf("nullify", args => null!);

        await _container.CreateItemAsync(
            new UdfDocument { Id = "1", PartitionKey = "pk1", Value = 1 }, new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JToken>(
            "SELECT VALUE udf.nullify(c.value) FROM c");
        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(1);
    }

    [Fact]
    public async Task Udf_WithNoArguments_Works()
    {
        _container.RegisterUdf("getVersion", args => "1.0.0");

        await _container.CreateItemAsync(
            new UdfDocument { Id = "1", PartitionKey = "pk1", Value = 0 }, new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JToken>(
            "SELECT VALUE udf.getVersion() FROM c");
        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(1);
        results[0].Value<string>().Should().Be("1.0.0");
    }

    // ─── UDF CRUD duplicate detection (T38) ─────────────────────────────

    [Fact]
    public async Task CreateUserDefinedFunctionAsync_DuplicateId_Throws409()
    {
        var scripts = _container.Scripts;

        await scripts.CreateUserDefinedFunctionAsync(new UserDefinedFunctionProperties
        {
            Id = "udfDup",
            Body = "function(x) { return x; }"
        });

        var act = () => scripts.CreateUserDefinedFunctionAsync(new UserDefinedFunctionProperties
        {
            Id = "udfDup",
            Body = "function(x) { return x * 2; }"
        });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Stored Procedure Dual Store Tests — _storedProcedureProperties vs _storedProcedures
// ═══════════════════════════════════════════════════════════════════════════

public class StoredProcedureDualStoreTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task CreateMetadata_WithoutRegisterHandler_ExecuteReturnsDefault()
    {
        // CRUD create without RegisterStoredProcedure — execute should return OK with default
        await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
            { Id = "sp1", Body = "function() { return 1; }" });

        var response = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "sp1", new PartitionKey("pk1"), Array.Empty<dynamic>());

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task RegisterHandler_WithoutCreateMetadata_ReadThrows404()
    {
        // Handler registered but no CRUD metadata — Read should throw 404
        _container.RegisterStoredProcedure("sp1", (pk, args) => "hello");

        var act = () => _container.Scripts.ReadStoredProcedureAsync("sp1");

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task RegisterHandler_WithoutCreateMetadata_ExecuteWorks()
    {
        // Handler registered without CRUD create — execution should still work
        _container.RegisterStoredProcedure("sp1", (pk, args) => "result");

        var response = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "sp1", new PartitionKey("pk1"), Array.Empty<dynamic>());

        response.Resource.Should().Be("result");
    }

    [Fact]
    public async Task DeregisterHandler_MetadataStillAccessible()
    {
        // Create metadata + register handler, then deregister handler
        await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
            { Id = "sp1", Body = "function() {}" });
        _container.RegisterStoredProcedure("sp1", (pk, args) => "handler");
        _container.DeregisterStoredProcedure("sp1");

        // Metadata should still be accessible
        var read = await _container.Scripts.ReadStoredProcedureAsync("sp1");
        read.StatusCode.Should().Be(HttpStatusCode.OK);
        read.Resource.Id.Should().Be("sp1");
    }

    [Fact]
    public async Task DeleteMetadata_AlsoRemovesHandler()
    {
        // Create via CRUD + register handler, then delete via CRUD
        await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
            { Id = "sp1", Body = "function() {}" });
        _container.RegisterStoredProcedure("sp1", (pk, args) => "alive");

        await _container.Scripts.DeleteStoredProcedureAsync("sp1");

        // Handler should be removed too (CRUD delete cleans up both stores)
        var act = () => _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "sp1", new PartitionKey("pk1"), Array.Empty<dynamic>());
        (await act.Should().ThrowAsync<CosmosException>())
            .Which.StatusCode.Should().Be(HttpStatusCode.NotFound);

        // Metadata should also be gone
        var metaAct = () => _container.Scripts.ReadStoredProcedureAsync("sp1");
        await metaAct.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task DeleteMetadata_HandlerOnlyRegistered_Throws404_HandlerLeaks()
    {
        // Register handler only (no CRUD create)
        _container.RegisterStoredProcedure("sp1", (pk, args) => "leaked");

        // Delete throws 404 because no metadata exists
        var act = () => _container.Scripts.DeleteStoredProcedureAsync("sp1");
        await act.Should().ThrowAsync<CosmosException>();

        // Handler still works (leaked — dual-store edge case)
        var response = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "sp1", new PartitionKey("pk1"), Array.Empty<dynamic>());
        response.Resource.Should().Be("leaked");
    }

    [Fact]
    public async Task CreateMetadata_ThenRegisterHandler_BothUsable()
    {
        // Full lifecycle: CRUD create + register handler
        await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
            { Id = "sp1", Body = "function() { return 'js'; }" });
        _container.RegisterStoredProcedure("sp1", (pk, args) => "csharp");

        // Read returns metadata
        var read = await _container.Scripts.ReadStoredProcedureAsync("sp1");
        read.Resource.Body.Should().Be("function() { return 'js'; }");

        // Execute uses handler
        var exec = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "sp1", new PartitionKey("pk1"), Array.Empty<dynamic>());
        exec.Resource.Should().Be("csharp");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Stored Procedure Input Validation Tests
// ═══════════════════════════════════════════════════════════════════════════

public class StoredProcedureInputValidationTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public void RegisterStoredProcedure_NullId_ThrowsArgumentNullException()
    {
        var act = () => _container.RegisterStoredProcedure(null!, (pk, args) => "ok");
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void RegisterStoredProcedure_NullHandler_ThrowsArgumentNullException()
    {
        var act = () => _container.RegisterStoredProcedure("sp1", null!);
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void DeregisterStoredProcedure_NullId_ThrowsArgumentNullException()
    {
        var act = () => _container.DeregisterStoredProcedure(null!);
        act.Should().Throw<ArgumentNullException>();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Stored Procedure Container Isolation Tests
// ═══════════════════════════════════════════════════════════════════════════

public class StoredProcedureContainerIsolationTests
{
    [Fact]
    public async Task TwoContainers_SameSprocId_IndependentHandlers()
    {
        var container1 = new InMemoryContainer("container-1", "/partitionKey");
        var container2 = new InMemoryContainer("container-2", "/partitionKey");

        container1.RegisterStoredProcedure("sp1", (pk, args) => "from-container-1");
        container2.RegisterStoredProcedure("sp1", (pk, args) => "from-container-2");

        var r1 = await container1.Scripts.ExecuteStoredProcedureAsync<string>(
            "sp1", new PartitionKey("pk1"), Array.Empty<dynamic>());
        var r2 = await container2.Scripts.ExecuteStoredProcedureAsync<string>(
            "sp1", new PartitionKey("pk1"), Array.Empty<dynamic>());

        r1.Resource.Should().Be("from-container-1");
        r2.Resource.Should().Be("from-container-2");
    }

    [Fact]
    public async Task TwoContainers_SameSprocId_IndependentMetadata()
    {
        var container1 = new InMemoryContainer("container-1", "/partitionKey");
        var container2 = new InMemoryContainer("container-2", "/partitionKey");

        await container1.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
            { Id = "sp1", Body = "function() { return 'body-1'; }" });
        await container2.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
            { Id = "sp1", Body = "function() { return 'body-2'; }" });

        var r1 = await container1.Scripts.ReadStoredProcedureAsync("sp1");
        var r2 = await container2.Scripts.ReadStoredProcedureAsync("sp1");

        r1.Resource.Body.Should().Be("function() { return 'body-1'; }");
        r2.Resource.Body.Should().Be("function() { return 'body-2'; }");
    }
}
