using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════
//  Gap Coverage Tests 6 — Deep gap analysis TDD
//  Covers: missing SQL functions, query features, unique key validation
//  gaps, stream iterator gaps, behavioral differences, and out-of-scope
//  features documented as skipped tests.
// ═══════════════════════════════════════════════════════════════════════════

// ─── A1. COT (Cotangent) ─────────────────────────────────────────────────

public class CotFunctionTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task Cot_ReturnsCorrectValue()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", angle = 1.0 }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<double>(
            "SELECT VALUE COT(c.angle) FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<double>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        // COT(1.0) = 1/TAN(1.0) ≈ 0.6420926
        results.Should().ContainSingle().Which.Should().BeApproximately(1.0 / Math.Tan(1.0), 0.0001);
    }
}

// ─── A2. CHOOSE ──────────────────────────────────────────────────────────

public class ChooseFunctionTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task Choose_ReturnsValueAtIndex()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"));

        // CHOOSE is 1-based: CHOOSE(2, 'a', 'b', 'c') → 'b'
        var iterator = _container.GetItemQueryIterator<string>(
            "SELECT VALUE CHOOSE(2, 'apple', 'banana', 'cherry') FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<string>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle().Which.Should().Be("banana");
    }

    [Fact]
    public async Task Choose_OutOfBounds_ReturnsUndefined()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"));

        // Index 5 is out of bounds for 3 items → undefined
        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT CHOOSE(5, 'a', 'b', 'c') AS result FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        // In real Cosmos DB, undefined values omit the field entirely.
        // The emulator projects null/undefined as JSON null.
        var token = results.Should().ContainSingle().Subject["result"];
        token.Should().NotBeNull();
        token!.Type.Should().Be(JTokenType.Null);
    }
}

// ─── A3. OBJECTTOARRAY ──────────────────────────────────────────────────

public class ObjectToArrayFunctionTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task ObjectToArray_ConvertsObjectToNameValuePairs()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", props = new { name = "Alice", age = 30 } }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JArray>(
            "SELECT VALUE ObjectToArray(c.props) FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<JArray>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        var arr = results.Should().ContainSingle().Subject;
        arr.Count.Should().Be(2);
        arr[0]["k"]!.Value<string>().Should().Be("name");
        arr[0]["v"]!.Value<string>().Should().Be("Alice");
        arr[1]["k"]!.Value<string>().Should().Be("age");
        arr[1]["v"]!.Value<int>().Should().Be(30);
    }
}

// ─── A4. STRINGJOIN ─────────────────────────────────────────────────────

public class StringJoinFunctionTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task StringJoin_JoinsArrayElements()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", tags = new[] { "red", "green", "blue" } }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<string>(
            "SELECT VALUE StringJoin(',', c.tags) FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<string>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle().Which.Should().Be("red,green,blue");
    }
}

// ─── A5. STRINGSPLIT ────────────────────────────────────────────────────

public class StringSplitFunctionTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task StringSplit_SplitsByDelimiter()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", csv = "red,green,blue" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JArray>(
            "SELECT VALUE StringSplit(c.csv, ',') FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<JArray>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        var arr = results.Should().ContainSingle().Subject;
        arr.Select(t => t.Value<string>()).Should().BeEquivalentTo("red", "green", "blue");
    }
}

// ─── A6–A8. Static DateTime Functions ───────────────────────────────────

public class StaticDateTimeFunctionTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task GetCurrentDateTimeStatic_ReturnsSameValueForAllItems()
    {
        for (var i = 0; i < 3; i++)
            await _container.CreateItemAsync(
                JObject.FromObject(new { id = $"{i}", pk = "a" }),
                new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<string>(
            "SELECT VALUE GetCurrentDateTimeStatic() FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<string>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().HaveCount(3);
        // All three values should be identical (static = same for entire query)
        results.Distinct().Should().ContainSingle();
    }

    [Fact]
    public async Task GetCurrentTicksStatic_ReturnsSameValueForAllItems()
    {
        for (var i = 0; i < 3; i++)
            await _container.CreateItemAsync(
                JObject.FromObject(new { id = $"{i}", pk = "a" }),
                new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<long>(
            "SELECT VALUE GetCurrentTicksStatic() FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<long>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().HaveCount(3);
        results.Distinct().Should().ContainSingle();
    }

    [Fact]
    public async Task GetCurrentTimestampStatic_ReturnsSameValueForAllItems()
    {
        for (var i = 0; i < 3; i++)
            await _container.CreateItemAsync(
                JObject.FromObject(new { id = $"{i}", pk = "a" }),
                new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<long>(
            "SELECT VALUE GetCurrentTimestampStatic() FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<long>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().HaveCount(3);
        results.Distinct().Should().ContainSingle();
    }
}

// ─── A9. DOCUMENTID ─────────────────────────────────────────────────────

public class DocumentIdFunctionTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task DocumentId_ReturnsRidField()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<string>(
            "SELECT VALUE DOCUMENTID(c) FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<string>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        // DOCUMENTID returns the _rid system property
        results.Should().ContainSingle().Which.Should().NotBeNullOrEmpty();
    }
}

// ─── A10. ST_AREA ───────────────────────────────────────────────────────

public class StAreaFunctionTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task StArea_ReturnsAreaForPolygon()
    {
        // A simple square polygon roughly 1° × 1° near the equator
        var polygon = new
        {
            type = "Polygon",
            coordinates = new[]
            {
                new[] { new[] { 0.0, 0.0 }, new[] { 1.0, 0.0 }, new[] { 1.0, 1.0 }, new[] { 0.0, 1.0 }, new[] { 0.0, 0.0 } }
            }
        };
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", region = polygon }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<double>(
            "SELECT VALUE ST_AREA(c.region) FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<double>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        // Should return a positive area value (in square meters)
        results.Should().ContainSingle().Which.Should().BeGreaterThan(0);
    }
}

// ─── B1. NOT LIKE ───────────────────────────────────────────────────────

public class NotLikeTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task NotLike_ExcludesMatchingItems()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }),
            new PartitionKey("a"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", name = "Bob" }),
            new PartitionKey("a"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "3", pk = "a", name = "Alicia" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.name NOT LIKE 'Al%'",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle();
        results[0]["id"]!.Value<string>().Should().Be("2");
    }
}

// ─── C1. ReplaceItemAsync Unique Key Validation ─────────────────────────

public class ReplaceItemUniqueKeyTests
{
    [Fact]
    public async Task ReplaceItem_ViolatesUniqueKey_ThrowsConflict()
    {
        var properties = new ContainerProperties("unique-ctr", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        var container = new InMemoryContainer(properties);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", email = "bob@test.com" }),
            new PartitionKey("a"));

        // Replace item 2 changing email to collide with item 1
        var act = () => container.ReplaceItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", email = "alice@test.com" }),
            "2", new PartitionKey("a"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }
}

// ─── C2. PatchItemAsync Unique Key Validation ───────────────────────────

public class PatchItemUniqueKeyTests
{
    [Fact]
    public async Task PatchItem_ViolatesUniqueKey_ThrowsConflict()
    {
        var properties = new ContainerProperties("unique-ctr", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        var container = new InMemoryContainer(properties);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", email = "bob@test.com" }),
            new PartitionKey("a"));

        // Patch item 2 changing email to collide with item 1
        var act = () => container.PatchItemAsync<JObject>(
            "2", new PartitionKey("a"),
            new[] { PatchOperation.Replace("/email", "alice@test.com") });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }
}

// ─── C3. CreateItemStreamAsync Unique Key Validation ────────────────────

public class StreamCrudUniqueKeyTests
{
    private static MemoryStream ToStream(object obj) =>
        new(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(obj)));

    [Fact]
    public async Task CreateItemStream_ViolatesUniqueKey_ReturnsConflict()
    {
        var properties = new ContainerProperties("unique-ctr", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        var container = new InMemoryContainer(properties);

        await container.CreateItemStreamAsync(
            ToStream(new { id = "1", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));

        var response = await container.CreateItemStreamAsync(
            ToStream(new { id = "2", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));

        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task UpsertItemStream_ViolatesUniqueKey_OfDifferentItem_ReturnsConflict()
    {
        var properties = new ContainerProperties("unique-ctr", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        var container = new InMemoryContainer(properties);

        await container.CreateItemStreamAsync(
            ToStream(new { id = "1", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));

        var response = await container.UpsertItemStreamAsync(
            ToStream(new { id = "2", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));

        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task ReplaceItemStream_ViolatesUniqueKey_ReturnsConflict()
    {
        var properties = new ContainerProperties("unique-ctr", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        var container = new InMemoryContainer(properties);

        await container.CreateItemStreamAsync(
            ToStream(new { id = "1", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));
        await container.CreateItemStreamAsync(
            ToStream(new { id = "2", pk = "a", email = "bob@test.com" }),
            new PartitionKey("a"));

        var response = await container.ReplaceItemStreamAsync(
            ToStream(new { id = "2", pk = "a", email = "alice@test.com" }),
            "2", new PartitionKey("a"));

        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }
}

// ─── D1. Stream Iterator Continuation Token ─────────────────────────────

public class StreamIteratorContinuationTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task GetItemQueryStreamIterator_RespectsContinuationToken()
    {
        for (var i = 0; i < 5; i++)
            await _container.CreateItemAsync(
                JObject.FromObject(new { id = $"{i}", pk = "a" }),
                new PartitionKey("a"));

        var opts = new QueryRequestOptions { PartitionKey = new PartitionKey("a"), MaxItemCount = 2 };

        // First page
        var iterator1 = _container.GetItemQueryStreamIterator(
            "SELECT * FROM c", continuationToken: null, requestOptions: opts);
        var page1 = await iterator1.ReadNextAsync();
        var page1Json = await new StreamReader(page1.Content).ReadToEndAsync();
        var page1Docs = JObject.Parse(page1Json)["Documents"] as JArray;
        var token = page1.Headers["x-ms-continuation"];

        token.Should().NotBeNullOrEmpty("there are more items to fetch");
        page1Docs!.Count.Should().Be(2);

        // Second page using continuation token
        var iterator2 = _container.GetItemQueryStreamIterator(
            "SELECT * FROM c", continuationToken: token, requestOptions: opts);
        var page2 = await iterator2.ReadNextAsync();
        var page2Json = await new StreamReader(page2.Content).ReadToEndAsync();
        var page2Docs = JObject.Parse(page2Json)["Documents"] as JArray;

        page2Docs!.Count.Should().Be(2);

        // Items from page2 should not overlap with page1
        var page1Ids = page1Docs.Select(d => d["id"]!.Value<string>()).ToHashSet();
        var page2Ids = page2Docs.Select(d => d["id"]!.Value<string>()).ToHashSet();
        page1Ids.Overlaps(page2Ids).Should().BeFalse("pages should not have overlapping items");
    }
}

// ─── E1. Triggers Don't Execute ─────────────────────────────────────────

public class TriggerExecutionTests
{
    /// <summary>
    /// Pre-triggers can now modify documents via C# handlers registered with RegisterTrigger.
    /// The trigger is registered as a C# Func&lt;JObject, JObject&gt; and fires when
    /// PreTriggers is specified in ItemRequestOptions.
    /// </summary>
    [Fact]
    public async Task PreTrigger_ShouldModifyDocumentOnCreate()
    {
        var container = new InMemoryContainer("test-container", "/pk");

        // Register a C# pre-trigger that adds a 'createdBy' field
        container.RegisterTrigger("addCreatedBy", TriggerType.Pre, TriggerOperation.Create,
            preHandler: doc =>
            {
                doc["createdBy"] = "trigger";
                return doc;
            });

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"),
            new ItemRequestOptions { PreTriggers = new List<string> { "addCreatedBy" } });

        var response = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
        response.Resource["createdBy"]!.Value<string>().Should().Be("trigger");
    }

    /// <summary>
    /// Demonstrates that CreateTriggerAsync alone (without RegisterTrigger) stores trigger
    /// metadata but does not cause trigger execution. JavaScript bodies are not interpreted.
    /// To get trigger execution, use RegisterTrigger with a C# handler.
    /// </summary>
    [Fact]
    public async Task PreTrigger_CreateTriggerAsyncAlone_DoesNotFireWithoutRegisterTrigger()
    {
        // ── Divergent behavior documentation ──
        // Real Cosmos DB: trigger body is executed as server-side JavaScript.
        //   The trigger can read/modify the incoming document before it is committed.
        // In-Memory Emulator: CreateTriggerAsync stores trigger metadata (returns 201 Created)
        //   but does not execute JavaScript bodies. To get trigger execution, register a
        //   C# handler via container.RegisterTrigger(). If PreTriggers is specified in
        //   ItemRequestOptions but no C# handler is registered, the trigger is not found
        //   and a BadRequest (400) is thrown.
        var container = new InMemoryContainer("test-container", "/pk");

        // This succeeds (201 Created) — metadata is stored.
        var triggerResponse = await container.Scripts.CreateTriggerAsync(new TriggerProperties
        {
            Id = "addCreatedBy",
            TriggerType = TriggerType.Pre,
            TriggerOperation = TriggerOperation.Create,
            Body = @"function() { /* would add createdBy */ }"
        });
        triggerResponse.StatusCode.Should().Be(HttpStatusCode.Created);

        // Create an item without specifying PreTriggers — no trigger fires.
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"));

        // Verify the trigger did NOT modify the document.
        var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
        item["createdBy"].Should().BeNull(
            "CreateTriggerAsync alone does not enable trigger execution — use RegisterTrigger");
    }
}

// ─── E2. DeleteContainerAsync Doesn't Remove From Parent DB ─────────────

public class DeleteContainerParentDbTests
{
    /// <summary>
    /// In real Cosmos DB, deleting a container removes it from the database's container list.
    /// In the emulator, DeleteContainerAsync clears internal data but does not remove itself
    /// from the parent InMemoryDatabase._containers dictionary.
    /// </summary>
    [Fact]
    public async Task DeleteContainer_ShouldRemoveFromDatabase_ContainerList()
    {
        var client = new InMemoryCosmosClient();
        var db = (InMemoryDatabase)(await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
        await db.CreateContainerAsync("ctr1", "/pk");

        var container = db.GetContainer("ctr1");
        await container.DeleteContainerAsync();

        // After deletion, the container should not be listed
        var iterator = db.GetContainerQueryIterator<ContainerProperties>("SELECT * FROM c");
        var containers = new List<ContainerProperties>();
        while (iterator.HasMoreResults)
            containers.AddRange(await iterator.ReadNextAsync());

        containers.Should().NotContain(c => c.Id == "ctr1");
    }
}

// ─── E3. Continuation Tokens Are Plain Integers ─────────────────────────

public class ContinuationTokenFormatTests
{
    /// <summary>
    /// In real Cosmos DB, continuation tokens are opaque base64-encoded JSON strings that
    /// include routing info, range IDs, and composite tokens. In the emulator they are
    /// simple integer offsets like "0", "3", "6". This test documents the ideal behavior.
    /// </summary>
    [Fact(Skip = "The emulator uses simple integer-offset continuation tokens (e.g. '3') " +
        "instead of the opaque base64-encoded JSON tokens used by real Cosmos DB. This is " +
        "intentional for simplicity and does not affect pagination correctness, but code that " +
        "parses or validates continuation token format will behave differently.")]
    public async Task ContinuationToken_ShouldBeOpaqueBase64()
    {
        var container = new InMemoryContainer("test-container", "/pk");
        for (var i = 0; i < 5; i++)
            await container.CreateItemAsync(
                JObject.FromObject(new { id = $"{i}", pk = "a" }),
                new PartitionKey("a"));

        var iterator = container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a"), MaxItemCount = 2 });
        var page = await iterator.ReadNextAsync();

        // Real Cosmos returns something like: eyJfcmlkIjoiLzEi...
        var token = page.ContinuationToken;
        token.Should().NotBeNullOrEmpty();
        // Base64 string should not parse as a plain integer
        int.TryParse(token, out _).Should().BeFalse();
    }

    /// <summary>
    /// Sister test: demonstrates the emulator uses simple integer offsets as tokens.
    /// </summary>
    [Fact]
    public async Task ContinuationToken_EmulatorBehavior_IsPlainIntegerOffset()
    {
        // ── Divergent behavior documentation ──
        // Real Cosmos DB: tokens are opaque, versioned, JSON-based, then base64-encoded.
        //   They contain partition ranges, RIDs, and composite continuation state.
        //   The exact format is undocumented and changes between SDK versions.
        // In-Memory Emulator: tokens are simple integer strings representing the offset
        //   into the result set. E.g., MaxItemCount=2 → first token is "2", next is "4".
        //   Pagination still works correctly; only the token format differs.
        var container = new InMemoryContainer("test-container", "/pk");
        for (var i = 0; i < 5; i++)
            await container.CreateItemAsync(
                JObject.FromObject(new { id = $"{i}", pk = "a" }),
                new PartitionKey("a"));

        var iterator = container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a"), MaxItemCount = 2 });
        var page = await iterator.ReadNextAsync();

        var token = page.ContinuationToken;
        token.Should().NotBeNullOrEmpty();
        int.TryParse(token, out var offset).Should().BeTrue(
            "the emulator uses plain integer offsets as continuation tokens");
        offset.Should().Be(2, "first page with MaxItemCount=2 yields offset 2");
    }
}

// ─── E4. ConflictResolutionPolicy Stored But Not Enforced ───────────────

public class ConflictResolutionPolicyTests
{
    /// <summary>
    /// In real Cosmos DB, the conflict resolution policy with a custom stored procedure
    /// resolves write conflicts in multi-region setups by invoking the specified sproc.
    /// The emulator stores the policy but operates with implicit strong consistency and
    /// single-region semantics, so conflict resolution never actually triggers.
    /// </summary>
    [Fact(Skip = "ConflictResolutionPolicy is stored on ContainerProperties and returned " +
        "on reads, but it is never enforced. The emulator operates in single-region mode " +
        "with implicit strong consistency, so write–write conflicts that would trigger the " +
        "policy in a multi-region setup cannot occur. The stored policy is purely decorative.")]
    public async Task ConflictResolution_CustomSproc_ShouldResolveConflicts()
    {
        var client = new InMemoryCosmosClient();
        var db = (InMemoryDatabase)(await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
        await db.CreateContainerAsync(new ContainerProperties("ctr1", "/pk")
        {
            ConflictResolutionPolicy = new ConflictResolutionPolicy
            {
                Mode = ConflictResolutionMode.Custom,
                ResolutionProcedure = "dbs/testdb/colls/ctr1/sprocs/resolveConflict"
            }
        });

        // In a multi-region real Cosmos account, concurrent writes from different regions
        // would trigger the custom stored procedure. This cannot be simulated.
        Assert.Fail("Cannot simulate multi-region write conflicts in single-region emulator.");
    }

    /// <summary>
    /// Sister test: the policy is stored and echoed back, but has no runtime effect.
    /// </summary>
    [Fact]
    public async Task ConflictResolution_EmulatorBehavior_PolicyStoredButNotEnforced()
    {
        // ── Divergent behavior documentation ──
        // Real Cosmos DB: ConflictResolutionPolicy determines how write conflicts are
        //   resolved in multi-region write configurations. LastWriterWins uses _ts
        //   comparison. Custom mode invokes a stored procedure.
        // In-Memory Emulator: The policy is accepted by ContainerProperties and returned
        //   on ReadContainerAsync / ReplaceContainerAsync. However, since the emulator is
        //   single-region and strongly consistent, no write–write conflicts can occur,
        //   and the policy is never triggered. It's stored for API compatibility only.
        var client = new InMemoryCosmosClient();
        var db = (InMemoryDatabase)(await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
        var containerResponse = await db.CreateContainerAsync(new ContainerProperties("ctr1", "/pk")
        {
            ConflictResolutionPolicy = new ConflictResolutionPolicy
            {
                Mode = ConflictResolutionMode.LastWriterWins
            }
        });

        var readBack = await containerResponse.Container.ReadContainerAsync();
        readBack.Resource.ConflictResolutionPolicy.Mode.Should().Be(
            ConflictResolutionMode.LastWriterWins,
            "the policy is stored and returned but never actually enforced");
    }
}

// ─── E5. PartitionKey.None vs PartitionKey.Null ─────────────────────────

public class PartitionKeyNoneVsNullTests
{
    /// <summary>
    /// In real Cosmos DB, PartitionKey.None represents the absence of a partition key (used
    /// for containers created without a partition key definition in older API versions).
    /// PartitionKey.Null represents an explicit null value. They are semantically distinct.
    /// The emulator treats both as the storage key "null", making them interchangeable.
    /// </summary>
    [Fact(Skip = "The emulator's PartitionKeyToString maps both PartitionKey.None and " +
        "PartitionKey.Null to the string 'null'. In real Cosmos DB, PartitionKey.None has " +
        "special routing behavior for legacy non-partitioned containers, while PartitionKey.Null " +
        "is an explicit null value in the partition key field. The emulator does not support " +
        "non-partitioned (legacy) containers so the distinction is not meaningful here.")]
    public async Task PartitionKeyNone_ShouldNotMatchPartitionKeyNull()
    {
        var container = new InMemoryContainer("test-container", "/pk");

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = (string)null! }),
            PartitionKey.Null);

        // In real Cosmos, reading with PartitionKey.Null would not find an item
        // written with PartitionKey.None
        var act = () => container.ReadItemAsync<JObject>("1", PartitionKey.Null);
        await act.Should().ThrowAsync<CosmosException>();
    }

    /// <summary>
    /// Sister test: demonstrates the emulator treats None and Null identically.
    /// </summary>
    [Fact]
    public async Task PartitionKeyNoneVsNull_EmulatorBehavior_TreatedIdentically()
    {
        // ── Divergent behavior documentation ──
        // Real Cosmos DB: PartitionKey.None → system-defined partition for legacy containers.
        //   PartitionKey.Null → explicit null value in the PK field. An item written with
        //   PartitionKey.None cannot be read with PartitionKey.Null (different routing).
        // In-Memory Emulator: ExtractPartitionKeyValue and PartitionKeyToString both map
        //   None and Null to the string "null". Items are stored with key (id, "null") in
        //   both cases, making them interchangeable. This is correct for modern containers
        //   but differs for legacy non-partitioned scenarios.
        var container = new InMemoryContainer("test-container", "/pk");

        // Create item with explicit null pk value via PartitionKey.Null
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = (string)null! }),
            PartitionKey.Null);

        // In the emulator, PartitionKey.None resolves the same way as PartitionKey.Null
        var response = await container.ReadItemAsync<JObject>("1", PartitionKey.None);
        response.Resource["id"]!.Value<string>().Should().Be("1",
            "the emulator treats PartitionKey.None and PartitionKey.Null identically");
    }
}

// ─── F3. VECTORDISTANCE ─────────────────────────────────────────────────

public class VectorDistanceTests
{
    /// <summary>
    /// VECTORDISTANCE computes similarity between vectors for AI/ML workloads.
    /// Supports cosine similarity (default), dot product, and Euclidean distance.
    /// No vector index policy is required on the in-memory container.
    /// </summary>
    [Fact]
    public async Task VectorDistance_ShouldComputeCosineSimilarity()
    {
        var container = new InMemoryContainer("test-container", "/pk");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0, 0.0 } }),
            new PartitionKey("a"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", embedding = new[] { 0.0, 1.0, 0.0 } }),
            new PartitionKey("a"));

        // VectorDistance defaults to cosine similarity; ORDER BY expression is fully supported
        var iterator = container.GetItemQueryIterator<JObject>(
            "SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c ORDER BY VectorDistance(c.embedding, [1.0, 0.0, 0.0])",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().HaveCount(2);
        // Ascending order: orthogonal (score=0) first, then identical (score=1)
        results[0]["score"]!.Value<double>().Should().BeApproximately(0.0, 1e-9);
        results[1]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9);
    }
}

// ─── F4. Client Encryption Key Operations ───────────────────────────────

public class ClientEncryptionKeyTests
{
    /// <summary>
    /// Client encryption key management (CreateClientEncryptionKeyAsync,
    /// RewrapClientEncryptionKeyAsync, ReadClientEncryptionKeyAsync) requires integration
    /// with Azure Key Vault and the Microsoft Data Encryption (MDE) SDK. These operations
    /// manage envelope encryption where a data encryption key (DEK) is wrapped by a
    /// customer-managed key (CMK) stored in Key Vault.
    /// </summary>
    [Fact(Skip = "Client encryption key operations require Azure Key Vault integration and " +
        "the Microsoft Data Encryption SDK (MDE). CreateClientEncryptionKeyAsync wraps a " +
        "data encryption key (DEK) with a customer-managed key from Key Vault. " +
        "ReadClientEncryptionKeyAsync and RewrapClientEncryptionKeyAsync manage the DEK " +
        "lifecycle. These deep SDK internals (EncryptionKeyWrapProvider, DataEncryptionKey) " +
        "are not meaningful without actual Key Vault access. " +
        "InMemoryDatabase currently throws NotImplementedException for these methods.")]
    public async Task CreateClientEncryptionKey_ShouldCreateAndStoreKey()
    {
        var client = new InMemoryCosmosClient();
        var db = (InMemoryDatabase)(await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;

        // Real code would need: ClientEncryptionKeyProperties with EncryptionAlgorithm,
        // KeyWrapMetadata pointing to Key Vault. The emulator throws NotImplementedException.
        await db.CreateClientEncryptionKeyAsync(
            new ClientEncryptionKeyProperties("dek1", "AEAD_AES_256_CBC_HMAC_SHA256",
                new byte[] { 0x01, 0x02, 0x03 },
                new EncryptionKeyWrapMetadata("akvso", "masterkey1", "https://vault.azure.net/keys/key1/1", "RSA-OAEP")));
    }
}
