using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════
//  Trigger Tests — TDD for C# trigger handler execution
// ═══════════════════════════════════════════════════════════════════════════

// ─── Phase 1: Trigger Storage & Registration ────────────────────────────

public class TriggerRegistrationTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public void RegisterTrigger_PreTrigger_StoresHandler()
    {
        _container.RegisterTrigger("addField", TriggerType.Pre, TriggerOperation.Create,
            (Func<JObject, JObject>)(doc =>
            {
                doc["added"] = true;
                return doc;
            }));

        // Trigger is registered — we verify it fires in Phase 2 tests
        // For now, just verify no exception + deregister works
        _container.DeregisterTrigger("addField");
    }

    [Fact]
    public void DeregisterTrigger_RemovesTrigger()
    {
        _container.RegisterTrigger("myTrigger", TriggerType.Pre, TriggerOperation.All,
            (Func<JObject, JObject>)(doc => doc));

        _container.DeregisterTrigger("myTrigger");

        // Using a deregistered trigger should fail
        var act = () => _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"),
            new ItemRequestOptions { PreTriggers = new List<string> { "myTrigger" } });
        act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task CreateTriggerAsync_StoresTriggerProperties()
    {
        await _container.Scripts.CreateTriggerAsync(new TriggerProperties
        {
            Id = "myTrigger",
            TriggerType = TriggerType.Pre,
            TriggerOperation = TriggerOperation.Create,
            Body = "function() { /* JS body */ }"
        });

        // Register a C# handler for this trigger
        _container.RegisterTrigger("myTrigger", TriggerType.Pre, TriggerOperation.Create,
            (Func<JObject, JObject>)(doc =>
            {
                doc["modified"] = true;
                return doc;
            }));

        // Now the trigger should fire
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"),
            new ItemRequestOptions { PreTriggers = new List<string> { "myTrigger" } });

        var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
        item["modified"]!.Value<bool>().Should().BeTrue();
    }

    [Fact]
    public async Task ReadTriggerAsync_ReturnsStoredTrigger()
    {
        await _container.Scripts.CreateTriggerAsync(new TriggerProperties
        {
            Id = "myTrigger",
            TriggerType = TriggerType.Pre,
            TriggerOperation = TriggerOperation.Create,
            Body = "function() {}"
        });

        var response = await _container.Scripts.ReadTriggerAsync("myTrigger");
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Id.Should().Be("myTrigger");
        response.Resource.TriggerType.Should().Be(TriggerType.Pre);
    }

    [Fact]
    public async Task ReadTriggerAsync_NotFound_Throws()
    {
        var act = () => _container.Scripts.ReadTriggerAsync("nonexistent");
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task DeleteTriggerAsync_RemovesTrigger()
    {
        await _container.Scripts.CreateTriggerAsync(new TriggerProperties
        {
            Id = "myTrigger",
            TriggerType = TriggerType.Pre,
            TriggerOperation = TriggerOperation.Create,
            Body = "function() {}"
        });

        var response = await _container.Scripts.DeleteTriggerAsync("myTrigger");
        response.StatusCode.Should().Be(HttpStatusCode.NoContent);

        var act = () => _container.Scripts.ReadTriggerAsync("myTrigger");
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task ReplaceTriggerAsync_UpdatesTrigger()
    {
        await _container.Scripts.CreateTriggerAsync(new TriggerProperties
        {
            Id = "myTrigger",
            TriggerType = TriggerType.Pre,
            TriggerOperation = TriggerOperation.Create,
            Body = "function() {}"
        });

        var response = await _container.Scripts.ReplaceTriggerAsync(new TriggerProperties
        {
            Id = "myTrigger",
            TriggerType = TriggerType.Post,
            TriggerOperation = TriggerOperation.All,
            Body = "function() { /* updated */ }"
        });

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var read = await _container.Scripts.ReadTriggerAsync("myTrigger");
        read.Resource.TriggerType.Should().Be(TriggerType.Post);
        read.Resource.TriggerOperation.Should().Be(TriggerOperation.All);
    }
}

// ─── Phase 2: Pre-Trigger Execution ─────────────────────────────────────

public class PreTriggerExecutionTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task PreTrigger_ModifiesDocument_OnCreate()
    {
        _container.RegisterTrigger("addCreatedBy", TriggerType.Pre, TriggerOperation.Create,
            (Func<JObject, JObject>)(doc =>
            {
                doc["createdBy"] = "trigger";
                return doc;
            }));

        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"),
            new ItemRequestOptions { PreTriggers = new List<string> { "addCreatedBy" } });

        var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
        item["createdBy"]!.Value<string>().Should().Be("trigger");
    }

    [Fact]
    public async Task PreTrigger_ModifiesDocument_OnUpsert()
    {
        _container.RegisterTrigger("stamp", TriggerType.Pre, TriggerOperation.All,
            (Func<JObject, JObject>)(doc =>
            {
                doc["stamped"] = true;
                return doc;
            }));

        await _container.UpsertItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"),
            new ItemRequestOptions { PreTriggers = new List<string> { "stamp" } });

        var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
        item["stamped"]!.Value<bool>().Should().BeTrue();
    }

    [Fact]
    public async Task PreTrigger_ModifiesDocument_OnReplace()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", name = "original" }),
            new PartitionKey("a"));

        _container.RegisterTrigger("addVersion", TriggerType.Pre, TriggerOperation.Replace,
            (Func<JObject, JObject>)(doc =>
            {
                doc["version"] = 2;
                return doc;
            }));

        await _container.ReplaceItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", name = "updated" }),
            "1", new PartitionKey("a"),
            new ItemRequestOptions { PreTriggers = new List<string> { "addVersion" } });

        var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
        item["version"]!.Value<int>().Should().Be(2);
        item["name"]!.Value<string>().Should().Be("updated");
    }

    [Fact]
    public async Task PreTrigger_ModifiesDocument_OnCreateStream()
    {
        _container.RegisterTrigger("addField", TriggerType.Pre, TriggerOperation.Create,
            (Func<JObject, JObject>)(doc =>
            {
                doc["streamModified"] = true;
                return doc;
            }));

        var json = """{"id":"1","pk":"a"}""";
        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
        await _container.CreateItemStreamAsync(stream, new PartitionKey("a"),
            new ItemRequestOptions { PreTriggers = new List<string> { "addField" } });

        var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
        item["streamModified"]!.Value<bool>().Should().BeTrue();
    }

    [Fact]
    public async Task PreTrigger_ModifiesDocument_OnUpsertStream()
    {
        _container.RegisterTrigger("addField", TriggerType.Pre, TriggerOperation.All,
            (Func<JObject, JObject>)(doc =>
            {
                doc["streamModified"] = true;
                return doc;
            }));

        var json = """{"id":"1","pk":"a"}""";
        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
        await _container.UpsertItemStreamAsync(stream, new PartitionKey("a"),
            new ItemRequestOptions { PreTriggers = new List<string> { "addField" } });

        var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
        item["streamModified"]!.Value<bool>().Should().BeTrue();
    }

    [Fact]
    public async Task PreTrigger_ModifiesDocument_OnReplaceStream()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"));

        _container.RegisterTrigger("addField", TriggerType.Pre, TriggerOperation.Replace,
            (Func<JObject, JObject>)(doc =>
            {
                doc["streamModified"] = true;
                return doc;
            }));

        var json = """{"id":"1","pk":"a","name":"replaced"}""";
        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
        await _container.ReplaceItemStreamAsync(stream, "1", new PartitionKey("a"),
            new ItemRequestOptions { PreTriggers = new List<string> { "addField" } });

        var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
        item["streamModified"]!.Value<bool>().Should().BeTrue();
    }

    [Fact]
    public async Task PreTrigger_NonExistentTrigger_ThrowsBadRequest()
    {
        var act = () => _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"),
            new ItemRequestOptions { PreTriggers = new List<string> { "doesNotExist" } });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task PreTrigger_OperationMismatch_TriggerNotFired()
    {
        // Register trigger for Create only
        _container.RegisterTrigger("createOnly", TriggerType.Pre, TriggerOperation.Create,
            (Func<JObject, JObject>)(doc =>
            {
                doc["triggered"] = true;
                return doc;
            }));

        // Create item first (without trigger)
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"));

        // Replace with trigger — should NOT fire because trigger is for Create, not Replace
        await _container.ReplaceItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", name = "replaced" }),
            "1", new PartitionKey("a"),
            new ItemRequestOptions { PreTriggers = new List<string> { "createOnly" } });

        var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
        item["triggered"].Should().BeNull();
    }

    [Fact]
    public async Task PreTrigger_MultipleTriggers_ChainInOrder()
    {
        _container.RegisterTrigger("first", TriggerType.Pre, TriggerOperation.Create,
            (Func<JObject, JObject>)(doc =>
            {
                doc["first"] = true;
                doc["order"] = "1";
                return doc;
            }));

        _container.RegisterTrigger("second", TriggerType.Pre, TriggerOperation.Create,
            (Func<JObject, JObject>)(doc =>
            {
                doc["second"] = true;
                doc["order"] = doc["order"]!.Value<string>() + ",2";
                return doc;
            }));

        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"),
            new ItemRequestOptions { PreTriggers = new List<string> { "first", "second" } });

        var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
        item["first"]!.Value<bool>().Should().BeTrue();
        item["second"]!.Value<bool>().Should().BeTrue();
        item["order"]!.Value<string>().Should().Be("1,2");
    }

    [Fact]
    public async Task PreTrigger_TriggerOperationAll_FiresOnAnyOperation()
    {
        _container.RegisterTrigger("allOps", TriggerType.Pre, TriggerOperation.All,
            (Func<JObject, JObject>)(doc =>
            {
                doc["triggered"] = true;
                return doc;
            }));

        var options = new ItemRequestOptions { PreTriggers = new List<string> { "allOps" } };

        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"), options);

        var item1 = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
        item1["triggered"]!.Value<bool>().Should().BeTrue();

        // Replace — trigger should fire again
        await _container.ReplaceItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", name = "replaced" }),
            "1", new PartitionKey("a"), options);

        var item2 = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
        item2["triggered"]!.Value<bool>().Should().BeTrue();
        item2["name"]!.Value<string>().Should().Be("replaced");

        // Upsert — trigger should fire again
        await _container.UpsertItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", name = "upserted" }),
            new PartitionKey("a"), options);

        var item3 = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
        item3["triggered"]!.Value<bool>().Should().BeTrue();
        item3["name"]!.Value<string>().Should().Be("upserted");
    }

    [Fact]
    public async Task PreTrigger_NoPreTriggersSpecified_TriggerNotFired()
    {
        _container.RegisterTrigger("addField", TriggerType.Pre, TriggerOperation.Create,
            (Func<JObject, JObject>)(doc =>
            {
                doc["triggered"] = true;
                return doc;
            }));

        // Create without specifying PreTriggers
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"));

        var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
        item["triggered"].Should().BeNull();
    }
}

// ─── Phase 3: Post-Trigger Execution ────────────────────────────────────

public class PostTriggerExecutionTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task PostTrigger_FiresAfterCreate()
    {
        _container.RegisterTrigger("audit", TriggerType.Post, TriggerOperation.Create,
            (Action<JObject>)(doc =>
            {
                // Post-trigger creates an audit entry
                var auditDoc = new JObject
                {
                    ["id"] = "audit-" + doc["id"]!.Value<string>(),
                    ["pk"] = doc["pk"]!.Value<string>(),
                    ["action"] = "created"
                };
                _container.CreateItemAsync(auditDoc, new PartitionKey(doc["pk"]!.Value<string>())).GetAwaiter().GetResult();
            }));

        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"),
            new ItemRequestOptions { PostTriggers = new List<string> { "audit" } });

        var audit = (await _container.ReadItemAsync<JObject>("audit-1", new PartitionKey("a"))).Resource;
        audit["action"]!.Value<string>().Should().Be("created");
    }

    [Fact]
    public async Task PostTrigger_ExceptionRollsBackWrite()
    {
        _container.RegisterTrigger("failingTrigger", TriggerType.Post, TriggerOperation.Create,
            (Action<JObject>)(_ => throw new InvalidOperationException("Post-trigger failed!")));

        var act = () => _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"),
            new ItemRequestOptions { PostTriggers = new List<string> { "failingTrigger" } });

        await act.Should().ThrowAsync<CosmosException>();

        // Item should NOT exist — the write was rolled back
        var readAct = () => _container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
        await readAct.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task PostTrigger_FiresAfterUpsert()
    {
        var postTriggered = false;
        _container.RegisterTrigger("flag", TriggerType.Post, TriggerOperation.All,
            (Action<JObject>)(_ => postTriggered = true));

        await _container.UpsertItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"),
            new ItemRequestOptions { PostTriggers = new List<string> { "flag" } });

        postTriggered.Should().BeTrue();
    }

    [Fact]
    public async Task PostTrigger_FiresAfterReplace()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"));

        var postTriggered = false;
        _container.RegisterTrigger("flag", TriggerType.Post, TriggerOperation.Replace,
            (Action<JObject>)(_ => postTriggered = true));

        await _container.ReplaceItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", name = "updated" }),
            "1", new PartitionKey("a"),
            new ItemRequestOptions { PostTriggers = new List<string> { "flag" } });

        postTriggered.Should().BeTrue();
    }

    [Fact]
    public async Task PostTrigger_NonExistentTrigger_ThrowsBadRequest()
    {
        var act = () => _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"),
            new ItemRequestOptions { PostTriggers = new List<string> { "nope" } });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task PostTrigger_OnStream_FiresAfterCreate()
    {
        var postTriggered = false;
        _container.RegisterTrigger("flag", TriggerType.Post, TriggerOperation.Create,
            (Action<JObject>)(_ => postTriggered = true));

        var json = """{"id":"1","pk":"a"}""";
        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
        await _container.CreateItemStreamAsync(stream, new PartitionKey("a"),
            new ItemRequestOptions { PostTriggers = new List<string> { "flag" } });

        postTriggered.Should().BeTrue();
    }

    [Fact]
    public async Task PostTrigger_OperationMismatch_NotFired()
    {
        var postTriggered = false;
        _container.RegisterTrigger("createOnly", TriggerType.Post, TriggerOperation.Create,
            (Action<JObject>)(_ => postTriggered = true));

        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"));

        // Replace with a Create-only post-trigger — should not fire
        await _container.ReplaceItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", name = "updated" }),
            "1", new PartitionKey("a"),
            new ItemRequestOptions { PostTriggers = new List<string> { "createOnly" } });

        postTriggered.Should().BeFalse();
    }
}
