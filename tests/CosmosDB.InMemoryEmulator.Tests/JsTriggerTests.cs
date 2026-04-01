using System.Net;
using AwesomeAssertions;
using CosmosDB.InMemoryEmulator.JsTriggers;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// TDD tests for JavaScript trigger body interpretation.
/// These tests use CreateTriggerAsync (which stores TriggerProperties with a JS body)
/// and UseJsTriggers() to enable the Jint-based JS engine.
/// </summary>
public class JsTriggerTests
{
    // ─── Pre-Trigger JS Execution ────────────────────────────────────────

    public class PreTriggerJsTests
    {
        private readonly InMemoryContainer _container = new("test-container", "/pk");

        private async Task RegisterJsPreTrigger(string id, string jsBody,
            TriggerOperation op = TriggerOperation.All)
        {
            var scripts = _container.Scripts;
            await scripts.CreateTriggerAsync(new TriggerProperties
            {
                Id = id,
                TriggerType = TriggerType.Pre,
                TriggerOperation = op,
                Body = jsBody
            });
        }

        [Fact]
        public async Task PreTrigger_Js_SetsTimestamp()
        {
            _container.UseJsTriggers();

            await RegisterJsPreTrigger("addTimestamp", """
                function addTimestamp() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    doc["timestamp"] = "2024-01-01T00:00:00Z";
                    request.setBody(doc);
                }
                """);

            await _container.CreateItemAsync(
                JObject.FromObject(new { id = "1", pk = "a" }),
                new PartitionKey("a"),
                new ItemRequestOptions { PreTriggers = new List<string> { "addTimestamp" } });

            var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
            item["timestamp"]!.Value<string>().Should().Be("2024-01-01T00:00:00Z");
        }

        [Fact]
        public async Task PreTrigger_Js_ModifiesExistingField()
        {
            _container.UseJsTriggers();

            await RegisterJsPreTrigger("upperName", """
                function upperName() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    doc["name"] = doc["name"].toUpperCase();
                    request.setBody(doc);
                }
                """);

            await _container.CreateItemAsync(
                JObject.FromObject(new { id = "1", pk = "a", name = "hello" }),
                new PartitionKey("a"),
                new ItemRequestOptions { PreTriggers = new List<string> { "upperName" } });

            var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
            item["name"]!.Value<string>().Should().Be("HELLO");
        }

        [Fact]
        public async Task PreTrigger_Js_AddsMultipleFields()
        {
            _container.UseJsTriggers();

            await RegisterJsPreTrigger("enrich", """
                function enrich() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    doc["createdAt"] = "2024-01-01";
                    doc["version"] = 1;
                    request.setBody(doc);
                }
                """);

            await _container.CreateItemAsync(
                JObject.FromObject(new { id = "1", pk = "a" }),
                new PartitionKey("a"),
                new ItemRequestOptions { PreTriggers = new List<string> { "enrich" } });

            var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
            item["createdAt"]!.Value<string>().Should().Be("2024-01-01");
            item["version"]!.Value<int>().Should().Be(1);
        }

        [Fact]
        public async Task PreTrigger_Js_WithoutSetBody_DocumentUnchanged()
        {
            _container.UseJsTriggers();

            await RegisterJsPreTrigger("noop", """
                function noop() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    // does NOT call request.setBody(doc)
                }
                """);

            await _container.CreateItemAsync(
                JObject.FromObject(new { id = "1", pk = "a", name = "original" }),
                new PartitionKey("a"),
                new ItemRequestOptions { PreTriggers = new List<string> { "noop" } });

            var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
            item["name"]!.Value<string>().Should().Be("original");
        }

        [Fact]
        public async Task PreTrigger_Js_OperationMismatch_NotFired()
        {
            _container.UseJsTriggers();

            await RegisterJsPreTrigger("createOnly", """
                function createOnly() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    doc["triggered"] = true;
                    request.setBody(doc);
                }
                """, TriggerOperation.Create);

            await _container.CreateItemAsync(
                JObject.FromObject(new { id = "1", pk = "a" }),
                new PartitionKey("a"));

            // Replace with a Create-only trigger — should NOT fire
            await _container.ReplaceItemAsync(
                JObject.FromObject(new { id = "1", pk = "a", name = "updated" }),
                "1", new PartitionKey("a"),
                new ItemRequestOptions { PreTriggers = new List<string> { "createOnly" } });

            var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
            item.ContainsKey("triggered").Should().BeFalse();
        }

        [Fact]
        public async Task PreTrigger_Js_FiresOnUpsert()
        {
            _container.UseJsTriggers();

            await RegisterJsPreTrigger("stamp", """
                function stamp() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    doc["stamped"] = true;
                    request.setBody(doc);
                }
                """);

            await _container.UpsertItemAsync(
                JObject.FromObject(new { id = "1", pk = "a" }),
                new PartitionKey("a"),
                new ItemRequestOptions { PreTriggers = new List<string> { "stamp" } });

            var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
            item["stamped"]!.Value<bool>().Should().BeTrue();
        }

        [Fact]
        public async Task PreTrigger_Js_StreamApi_ModifiesDocument()
        {
            _container.UseJsTriggers();

            await RegisterJsPreTrigger("addField", """
                function addField() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    doc["added"] = "yes";
                    request.setBody(doc);
                }
                """);

            var json = """{"id":"1","pk":"a"}""";
            using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
            await _container.CreateItemStreamAsync(stream, new PartitionKey("a"),
                new ItemRequestOptions { PreTriggers = new List<string> { "addField" } });

            var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
            item["added"]!.Value<string>().Should().Be("yes");
        }

        [Fact]
        public async Task PreTrigger_MultipleChainedJsTriggers()
        {
            _container.UseJsTriggers();

            await RegisterJsPreTrigger("first", """
                function first() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    doc["step1"] = true;
                    request.setBody(doc);
                }
                """);

            await RegisterJsPreTrigger("second", """
                function second() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    doc["step2"] = true;
                    request.setBody(doc);
                }
                """);

            await _container.CreateItemAsync(
                JObject.FromObject(new { id = "1", pk = "a" }),
                new PartitionKey("a"),
                new ItemRequestOptions { PreTriggers = new List<string> { "first", "second" } });

            var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
            item["step1"]!.Value<bool>().Should().BeTrue();
            item["step2"]!.Value<bool>().Should().BeTrue();
        }

        [Fact]
        public async Task PreTrigger_Js_ThrowingTrigger_ThrowsCosmosException()
        {
            _container.UseJsTriggers();

            await RegisterJsPreTrigger("fail", """
                function fail() {
                    throw new Error("trigger validation failed");
                }
                """);

            var act = () => _container.CreateItemAsync(
                JObject.FromObject(new { id = "1", pk = "a" }),
                new PartitionKey("a"),
                new ItemRequestOptions { PreTriggers = new List<string> { "fail" } });

            await act.Should().ThrowAsync<CosmosException>();
        }

        [Fact]
        public async Task PreTrigger_Js_WithoutUseJsTriggers_ThrowsBadRequest()
        {
            // NOTE: No UseJsTriggers() call here — the engine is not configured

            var scripts = _container.Scripts;
            await scripts.CreateTriggerAsync(new TriggerProperties
            {
                Id = "myTrigger",
                TriggerType = TriggerType.Pre,
                TriggerOperation = TriggerOperation.All,
                Body = "function myTrigger() { }"
            });

            var act = () => _container.CreateItemAsync(
                JObject.FromObject(new { id = "1", pk = "a" }),
                new PartitionKey("a"),
                new ItemRequestOptions { PreTriggers = new List<string> { "myTrigger" } });

            var ex = await act.Should().ThrowAsync<CosmosException>();
            ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
            ex.Which.Message.Should().Contain("UseJsTriggers");
        }
    }

    // ─── Post-Trigger JS Execution ───────────────────────────────────────

    public class PostTriggerJsTests
    {
        private readonly InMemoryContainer _container = new("test-container", "/pk");

        private async Task RegisterJsPostTrigger(string id, string jsBody,
            TriggerOperation op = TriggerOperation.All)
        {
            var scripts = _container.Scripts;
            await scripts.CreateTriggerAsync(new TriggerProperties
            {
                Id = id,
                TriggerType = TriggerType.Post,
                TriggerOperation = op,
                Body = jsBody
            });
        }

        [Fact]
        public async Task PostTrigger_Js_CanReadCommittedDocument()
        {
            _container.UseJsTriggers();

            await RegisterJsPostTrigger("createAudit", """
                function createAudit() {
                    var context = getContext();
                    var response = context.getResponse();
                    var doc = response.getBody();
                }
                """);

            await _container.CreateItemAsync(
                JObject.FromObject(new { id = "1", pk = "a" }),
                new PartitionKey("a"),
                new ItemRequestOptions { PostTriggers = new List<string> { "createAudit" } });

            // Verify the item was created successfully (post-trigger ran without error)
            var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
            item["id"]!.Value<string>().Should().Be("1");
        }

        [Fact]
        public async Task PostTrigger_Js_ThrowingTrigger_RollsBackWrite()
        {
            _container.UseJsTriggers();

            await RegisterJsPostTrigger("failPost", """
                function failPost() {
                    throw new Error("post-trigger failed");
                }
                """);

            var act = () => _container.CreateItemAsync(
                JObject.FromObject(new { id = "1", pk = "a" }),
                new PartitionKey("a"),
                new ItemRequestOptions { PostTriggers = new List<string> { "failPost" } });

            await act.Should().ThrowAsync<CosmosException>();

            // Item should NOT exist — write rolled back
            var readAct = () => _container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
            await readAct.Should().ThrowAsync<CosmosException>();
        }

        [Fact]
        public async Task PostTrigger_Js_FiresAfterUpsert()
        {
            _container.UseJsTriggers();

            await RegisterJsPostTrigger("readDoc", """
                function readDoc() {
                    var context = getContext();
                    var response = context.getResponse();
                    var doc = response.getBody();
                }
                """);

            // Should not throw — verifies post-trigger executed successfully
            await _container.UpsertItemAsync(
                JObject.FromObject(new { id = "1", pk = "a" }),
                new PartitionKey("a"),
                new ItemRequestOptions { PostTriggers = new List<string> { "readDoc" } });
        }

        [Fact]
        public async Task PostTrigger_Js_FiresAfterReplace()
        {
            _container.UseJsTriggers();

            await _container.CreateItemAsync(
                JObject.FromObject(new { id = "1", pk = "a" }),
                new PartitionKey("a"));

            await RegisterJsPostTrigger("readDoc", """
                function readDoc() {
                    var context = getContext();
                    var response = context.getResponse();
                    var doc = response.getBody();
                }
                """);

            // Should not throw
            await _container.ReplaceItemAsync(
                JObject.FromObject(new { id = "1", pk = "a", name = "updated" }),
                "1", new PartitionKey("a"),
                new ItemRequestOptions { PostTriggers = new List<string> { "readDoc" } });
        }

        [Fact]
        public async Task PostTrigger_Js_OperationMismatch_NotFired()
        {
            _container.UseJsTriggers();

            await RegisterJsPostTrigger("createOnly", """
                function createOnly() {
                    throw new Error("should not run");
                }
                """, TriggerOperation.Create);

            await _container.CreateItemAsync(
                JObject.FromObject(new { id = "1", pk = "a" }),
                new PartitionKey("a"));

            // Replace with Create-only post-trigger — should not fire (and not throw)
            await _container.ReplaceItemAsync(
                JObject.FromObject(new { id = "1", pk = "a", name = "updated" }),
                "1", new PartitionKey("a"),
                new ItemRequestOptions { PostTriggers = new List<string> { "createOnly" } });
        }

        [Fact]
        public async Task PostTrigger_Js_WithoutUseJsTriggers_ThrowsBadRequest()
        {
            // NOTE: No UseJsTriggers() call

            var scripts = _container.Scripts;
            await scripts.CreateTriggerAsync(new TriggerProperties
            {
                Id = "myPost",
                TriggerType = TriggerType.Post,
                TriggerOperation = TriggerOperation.All,
                Body = "function myPost() { }"
            });

            var act = () => _container.CreateItemAsync(
                JObject.FromObject(new { id = "1", pk = "a" }),
                new PartitionKey("a"),
                new ItemRequestOptions { PostTriggers = new List<string> { "myPost" } });

            var ex = await act.Should().ThrowAsync<CosmosException>();
            ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
            ex.Which.Message.Should().Contain("UseJsTriggers");
        }

        [Fact]
        public async Task PostTrigger_Js_OnStream_Fires()
        {
            _container.UseJsTriggers();

            await RegisterJsPostTrigger("readDoc", """
                function readDoc() {
                    var context = getContext();
                    var response = context.getResponse();
                    var doc = response.getBody();
                }
                """);

            var json = """{"id":"1","pk":"a"}""";
            using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

            // Should not throw
            await _container.CreateItemStreamAsync(stream, new PartitionKey("a"),
                new ItemRequestOptions { PostTriggers = new List<string> { "readDoc" } });
        }
    }

    // ─── Mixed C# handler + JS triggers ──────────────────────────────────

    public class MixedTriggerTests
    {
        private readonly InMemoryContainer _container = new("test-container", "/pk");

        [Fact]
        public async Task CSharpHandler_TakesPriority_OverJsBody()
        {
            _container.UseJsTriggers();

            // Register a C# handler
            _container.RegisterTrigger("myTrigger", TriggerType.Pre, TriggerOperation.All,
                (Func<JObject, JObject>)(doc =>
                {
                    doc["source"] = "csharp";
                    return doc;
                }));

            // Also register a JS body for the same trigger name
            var scripts = _container.Scripts;
            await scripts.CreateTriggerAsync(new TriggerProperties
            {
                Id = "myTrigger",
                TriggerType = TriggerType.Pre,
                TriggerOperation = TriggerOperation.All,
                Body = """
                    function myTrigger() {
                        var ctx = getContext();
                        var req = ctx.getRequest();
                        var doc = req.getBody();
                        doc["source"] = "javascript";
                        req.setBody(doc);
                    }
                    """
            });

            await _container.CreateItemAsync(
                JObject.FromObject(new { id = "1", pk = "a" }),
                new PartitionKey("a"),
                new ItemRequestOptions { PreTriggers = new List<string> { "myTrigger" } });

            // C# handler should win — read the stored document to verify
            var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
            item["source"]!.Value<string>().Should().Be("csharp");
        }
    }

    // ─── UseJsTriggers extension ─────────────────────────────────────────

    public class UseJsTriggersExtensionTests
    {
        [Fact]
        public void UseJsTriggers_SetsJsTriggerEngine()
        {
            var container = new InMemoryContainer("test", "/pk");
            container.JsTriggerEngine.Should().BeNull();

            container.UseJsTriggers();

            container.JsTriggerEngine.Should().NotBeNull();
            container.JsTriggerEngine.Should().BeOfType<JintTriggerEngine>();
        }

        [Fact]
        public void UseJsTriggers_ReturnsSameContainer()
        {
            var container = new InMemoryContainer("test", "/pk");
            var result = container.UseJsTriggers();
            result.Should().BeSameAs(container);
        }
    }
}
