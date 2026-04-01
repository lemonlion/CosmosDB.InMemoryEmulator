using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;
using System.Net;
using System.Text;

namespace CosmosDB.InMemoryEmulator.Tests;

public class StatePersistenceTests
{
    [Fact]
    public async Task ExportState_EmptyContainer_ReturnsEmptyJson()
    {
        var container = new InMemoryContainer("test-container", "/partitionKey");

        var json = container.ExportState();

        var parsed = JObject.Parse(json);
        parsed["items"]!.Should().BeOfType<JArray>();
        ((JArray)parsed["items"]!).Should().BeEmpty();
    }

    [Fact]
    public async Task ExportState_WithItems_SerializesAllItems()
    {
        var container = new InMemoryContainer("test-container", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));
        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new PartitionKey("pk1"));

        var json = container.ExportState();

        var parsed = JObject.Parse(json);
        var items = (JArray)parsed["items"]!;
        items.Should().HaveCount(2);
    }

    [Fact]
    public async Task ImportState_RestoresItems()
    {
        var source = new InMemoryContainer("source-container", "/partitionKey");
        await source.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));
        await source.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new PartitionKey("pk1"));

        var exportedJson = source.ExportState();

        var target = new InMemoryContainer("target-container", "/partitionKey");
        target.ImportState(exportedJson);

        target.ItemCount.Should().Be(2);
        var alice = await target.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        alice.Resource.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task ImportState_ClearsExistingDataBeforeImporting()
    {
        var container = new InMemoryContainer("test-container", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "existing", PartitionKey = "pk1", Name = "Existing" },
            new PartitionKey("pk1"));

        var stateJson = """{"items":[{"id":"new","partitionKey":"pk1","name":"New","value":0,"isActive":true,"tags":[]}]}""";
        container.ImportState(stateJson);

        container.ItemCount.Should().Be(1);
        var result = await container.ReadItemAsync<TestDocument>("new", new PartitionKey("pk1"));
        result.Resource.Name.Should().Be("New");
    }

    [Fact]
    public async Task ExportState_ToFile_And_ImportState_FromFile_RoundTrips()
    {
        var tempFile = Path.GetTempFileName();
        try
        {
            var source = new InMemoryContainer("source", "/partitionKey");
            await source.CreateItemAsync(
                new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 42, Tags = ["a", "b"] },
                new PartitionKey("pk1"));

            source.ExportStateToFile(tempFile);

            var target = new InMemoryContainer("target", "/partitionKey");
            target.ImportStateFromFile(tempFile);

            target.ItemCount.Should().Be(1);
            var item = await target.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
            item.Resource.Name.Should().Be("Alice");
            item.Resource.Value.Should().Be(42);
            item.Resource.Tags.Should().BeEquivalentTo(["a", "b"]);
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task ExportState_PreservesPartitionKeyIsolation()
    {
        var container = new InMemoryContainer("test-container", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk2", Name = "Different Alice" },
            new PartitionKey("pk2"));

        var json = container.ExportState();

        var target = new InMemoryContainer("target", "/partitionKey");
        target.ImportState(json);

        target.ItemCount.Should().Be(2);
        var pk1Item = await target.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        pk1Item.Resource.Name.Should().Be("Alice");
        var pk2Item = await target.ReadItemAsync<TestDocument>("1", new PartitionKey("pk2"));
        pk2Item.Resource.Name.Should().Be("Different Alice");
    }

    [Fact]
    public async Task ImportState_WithNestedObjects_PreservesStructure()
    {
        var source = new InMemoryContainer("source", "/partitionKey");
        await source.CreateItemAsync(new TestDocument
        {
            Id = "1",
            PartitionKey = "pk1",
            Name = "Test",
            Nested = new NestedObject { Description = "nested value", Score = 3.14 }
        }, new PartitionKey("pk1"));

        var json = source.ExportState();
        var target = new InMemoryContainer("target", "/partitionKey");
        target.ImportState(json);

        var item = await target.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        item.Resource.Nested.Should().NotBeNull();
        item.Resource.Nested!.Description.Should().Be("nested value");
        item.Resource.Nested!.Score.Should().Be(3.14);
    }

    [Fact]
    public void ImportState_WithInvalidJson_ThrowsException()
    {
        var container = new InMemoryContainer("test-container", "/partitionKey");

        var action = () => container.ImportState("not valid json");

        action.Should().Throw<JsonReaderException>();
    }

    [Fact]
    public async Task ExportState_ItemsAreQueryableAfterImport()
    {
        var source = new InMemoryContainer("source", "/partitionKey");
        await source.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));
        await source.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new PartitionKey("pk1"));

        var json = source.ExportState();
        var target = new InMemoryContainer("target", "/partitionKey");
        target.ImportState(json);

        var queryDef = new QueryDefinition("SELECT * FROM c WHERE c.value > 15");
        var iterator = target.GetItemQueryIterator<TestDocument>(queryDef);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Bob");
    }
}


public class StateImportExportTests
{
    [Fact]
    public async Task ExportState_ImportState_RoundTrip()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));
        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
            new PartitionKey("pk1"));

        var state = container.ExportState();
        state.Should().NotBeNullOrEmpty();

        var newContainer = new InMemoryContainer("test", "/partitionKey");
        newContainer.ImportState(state);

        var read = await newContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Alice");

        newContainer.ItemCount.Should().Be(2);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category A: ExportState Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class ExportStateEdgeCaseTests
{
    [Fact]
    public async Task ExportState_SingleItem_ProducesValidJson()
    {
        var container = new InMemoryContainer("export-single", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var json = container.ExportState();
        var parsed = JObject.Parse(json);
        var items = (JArray)parsed["items"]!;
        items.Should().HaveCount(1);
        items[0]["id"]!.ToString().Should().Be("1");
    }

    [Fact]
    public async Task ExportState_IncludesSystemProperties()
    {
        var container = new InMemoryContainer("export-sys", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var json = container.ExportState();
        var items = (JArray)JObject.Parse(json)["items"]!;
        var item = items[0];

        item["_etag"].Should().NotBeNull();
        item["_ts"].Should().NotBeNull();
    }

    [Fact]
    public async Task ExportState_LargeNumberOfItems_AllSerialized()
    {
        var container = new InMemoryContainer("export-large", "/pk");
        for (var i = 0; i < 500; i++)
            await container.CreateItemStreamAsync(
                new MemoryStream(Encoding.UTF8.GetBytes($$$"""{"id":"{{{i}}}","pk":"a"}""")),
                new PartitionKey("a"));

        var json = container.ExportState();
        var items = (JArray)JObject.Parse(json)["items"]!;
        items.Should().HaveCount(500);
    }

    [Fact]
    public async Task ExportState_WithSpecialCharacters_RoundTrips()
    {
        var container = new InMemoryContainer("export-special", "/pk");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", text = "quotes\"and\\backslash\nnewline\ttab" }),
            new PartitionKey("a"));

        var json = container.ExportState();
        var target = new InMemoryContainer("target", "/pk");
        target.ImportState(json);

        var result = await target.ReadItemAsync<JObject>("1", new PartitionKey("a"));
        result.Resource["text"]!.ToString().Should().Be("quotes\"and\\backslash\nnewline\ttab");
    }

    [Fact]
    public async Task ExportState_WithNullValues_PreservesNulls()
    {
        var container = new InMemoryContainer("export-null", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","field":null}""")),
            new PartitionKey("a"));

        var json = container.ExportState();
        var items = (JArray)JObject.Parse(json)["items"]!;
        items[0]["field"]!.Type.Should().Be(JTokenType.Null);
    }

    [Fact]
    public async Task ExportState_WithBooleanValues_PreservesType()
    {
        var container = new InMemoryContainer("export-bool", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","active":true,"deleted":false}""")),
            new PartitionKey("a"));

        var json = container.ExportState();
        var item = ((JArray)JObject.Parse(json)["items"]!)[0];
        item["active"]!.Type.Should().Be(JTokenType.Boolean);
        item["active"]!.Value<bool>().Should().BeTrue();
        item["deleted"]!.Value<bool>().Should().BeFalse();
    }

    [Fact]
    public async Task ExportState_WithArrays_PreservesArrays()
    {
        var container = new InMemoryContainer("export-arrays", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","pk":"a","tags":["a","b"],"empty":[],"nested":[[1,2],[3]]}""")),
            new PartitionKey("a"));

        var json = container.ExportState();
        var target = new InMemoryContainer("target", "/pk");
        target.ImportState(json);

        var result = await target.ReadItemAsync<JObject>("1", new PartitionKey("a"));
        result.Resource["tags"]!.Should().BeOfType<JArray>();
        ((JArray)result.Resource["tags"]!).Should().HaveCount(2);
        ((JArray)result.Resource["empty"]!).Should().BeEmpty();
    }

    [Fact]
    public async Task ExportState_WithDeeplyNestedObjects_PreservesAll()
    {
        var container = new InMemoryContainer("export-deep", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","pk":"a","a":{"b":{"c":{"d":{"e":"deep"}}}}}""")),
            new PartitionKey("a"));

        var json = container.ExportState();
        var target = new InMemoryContainer("target", "/pk");
        target.ImportState(json);

        var result = await target.ReadItemAsync<JObject>("1", new PartitionKey("a"));
        result.Resource.SelectToken("a.b.c.d.e")!.ToString().Should().Be("deep");
    }

    [Fact]
    public async Task ExportState_OutputIsIndentedJson()
    {
        var container = new InMemoryContainer("export-format", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a"}""")),
            new PartitionKey("a"));

        var json = container.ExportState();
        json.Should().Contain("\n", "ExportState should produce indented (pretty-printed) JSON");
    }

    [Fact]
    public async Task ExportState_CalledTwice_ProducesSameOutput()
    {
        var container = new InMemoryContainer("export-idem", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","name":"test"}""")),
            new PartitionKey("a"));

        var json1 = container.ExportState();
        var json2 = container.ExportState();
        json1.Should().Be(json2);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category B: ImportState Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class ImportStateEdgeCaseTests
{
    [Fact]
    public void ImportState_EmptyItemsArray_ResultsInEmptyContainer()
    {
        var container = new InMemoryContainer("import-empty", "/pk");
        container.ImportState("""{"items":[]}""");
        container.ItemCount.Should().Be(0);
    }

    [Fact]
    public async Task ImportState_MissingItemsKey_ClearsContainer()
    {
        var container = new InMemoryContainer("import-nokey", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a"}""")),
            new PartitionKey("a"));

        container.ImportState("""{"foo":"bar"}""");
        container.ItemCount.Should().Be(0, "missing 'items' key means nothing to import, but ClearItems runs first");
    }

    [Fact]
    public async Task ImportState_EmptyJsonObject_ClearsContainer()
    {
        var container = new InMemoryContainer("import-obj", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a"}""")),
            new PartitionKey("a"));

        container.ImportState("{}");
        container.ItemCount.Should().Be(0);
    }

    [Fact]
    public async Task ImportState_GeneratesNewETags()
    {
        var source = new InMemoryContainer("import-etag-src", "/partitionKey");
        await source.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var sourceRead = await source.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
        var sourceEtag = sourceRead.ETag;

        var target = new InMemoryContainer("import-etag-tgt", "/partitionKey");
        target.ImportState(source.ExportState());

        var targetRead = await target.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
        targetRead.ETag.Should().NotBe(sourceEtag, "import generates new etags");
    }

    [Fact]
    public void ImportState_NullJson_ThrowsException()
    {
        var container = new InMemoryContainer("import-null", "/pk");
        var act = () => container.ImportState(null!);
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void ImportState_EmptyString_ThrowsException()
    {
        var container = new InMemoryContainer("import-empty-str", "/pk");
        var act = () => container.ImportState("");
        act.Should().Throw<JsonReaderException>();
    }

    [Fact]
    public async Task ImportState_DuplicateIds_SamePartitionKey_LastWins()
    {
        var container = new InMemoryContainer("import-dup", "/pk");
        container.ImportState("""{"items":[{"id":"1","pk":"a","name":"first"},{"id":"1","pk":"a","name":"last"}]}""");

        container.ItemCount.Should().Be(1);
        var result = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
        result.Resource["name"]!.ToString().Should().Be("last");
    }

    [Fact]
    public async Task ImportState_DuplicateIds_DifferentPartitionKeys_BothStored()
    {
        var container = new InMemoryContainer("import-dup-pk", "/pk");
        container.ImportState("""{"items":[{"id":"1","pk":"a","name":"A"},{"id":"1","pk":"b","name":"B"}]}""");

        container.ItemCount.Should().Be(2);
        var a = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
        a.Resource["name"]!.ToString().Should().Be("A");
        var b = await container.ReadItemAsync<JObject>("1", new PartitionKey("b"));
        b.Resource["name"]!.ToString().Should().Be("B");
    }

    [Fact]
    public async Task ImportState_CalledMultipleTimes_OnlyLastImportSurvives()
    {
        var container = new InMemoryContainer("import-multi", "/pk");
        container.ImportState("""{"items":[{"id":"A","pk":"a"}]}""");
        container.ImportState("""{"items":[{"id":"B","pk":"b"}]}""");

        container.ItemCount.Should().Be(1);
        var result = await container.ReadItemAsync<JObject>("B", new PartitionKey("b"));
        result.Resource["id"]!.ToString().Should().Be("B");

        var act = () => container.ReadItemAsync<JObject>("A", new PartitionKey("a"));
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task ImportState_WithExtraJsonProperties_PreservesThem()
    {
        var container = new InMemoryContainer("import-extra", "/pk");
        container.ImportState("""{"items":[{"id":"1","pk":"a","customField":"preserved","nestedCustom":{"x":1}}]}""");

        var result = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
        result.Resource["customField"]!.ToString().Should().Be("preserved");
        result.Resource["nestedCustom"]!["x"]!.Value<int>().Should().Be(1);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category C: Change Feed Interaction
// ═══════════════════════════════════════════════════════════════════════════

public class StatePersistenceChangeFeedTests
{
    [Fact]
    public void ImportState_DoesNotPopulateChangeFeed()
    {
        var container = new InMemoryContainer("cf-import", "/pk");
        container.ImportState("""{"items":[{"id":"1","pk":"a"},{"id":"2","pk":"a"}]}""");

        var iter = container.GetChangeFeedIterator<JObject>(
            ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);

        // Change feed should be empty — import bypasses change feed recording
        // HasMoreResults is false when change feed has no entries
        iter.HasMoreResults.Should().BeFalse(
            "ImportState does not record change feed entries — this is by design");
    }

    [Fact]
    public async Task ImportState_SubsequentWritesAppearInChangeFeed()
    {
        var container = new InMemoryContainer("cf-writes", "/pk");
        container.ImportState("""{"items":[{"id":"1","pk":"a"}]}""");

        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","pk":"a"}""")),
            new PartitionKey("a"));

        var iter = container.GetChangeFeedIterator<JObject>(
            ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);
        var page = await iter.ReadNextAsync();

        page.Should().Contain(j => j["id"]!.ToString() == "2");
    }

    [Fact]
    public async Task ExportState_DoesNotIncludeChangeFeedHistory()
    {
        var container = new InMemoryContainer("cf-export", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a"}""")),
            new PartitionKey("a"));

        var json = container.ExportState();
        var parsed = JObject.Parse(json);

        // Export only has "items", no change feed data
        parsed.Properties().Select(p => p.Name).Should().BeEquivalentTo(["items"]);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category D: TTL Interaction
// ═══════════════════════════════════════════════════════════════════════════

public class StatePersistenceTtlTests
{
    [Fact]
    public async Task ImportState_WithDefaultTTL_ImportedItemsRespectTTL()
    {
        var container = new InMemoryContainer("ttl-import", "/pk");
        container.DefaultTimeToLive = 1;
        container.ImportState("""{"items":[{"id":"1","pk":"a"}]}""");

        await Task.Delay(TimeSpan.FromSeconds(2));

        var act = () => container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
        await act.Should().ThrowAsync<CosmosException>()
            .Where(e => e.StatusCode == HttpStatusCode.NotFound);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category E: Hierarchical Partition Keys
// ═══════════════════════════════════════════════════════════════════════════

public class StatePersistenceHierarchicalPkTests
{
    [Fact]
    public async Task ExportImport_HierarchicalPartitionKey_RoundTrips()
    {
        var source = new InMemoryContainer("hk-src", new[] { "/tenantId", "/userId" });
        await source.CreateItemAsync(
            JObject.FromObject(new { id = "1", tenantId = "t1", userId = "u1", name = "Alice" }),
            new PartitionKeyBuilder().Add("t1").Add("u1").Build());

        var json = source.ExportState();
        var target = new InMemoryContainer("hk-tgt", new[] { "/tenantId", "/userId" });
        target.ImportState(json);

        var result = await target.ReadItemAsync<JObject>("1",
            new PartitionKeyBuilder().Add("t1").Add("u1").Build());
        result.Resource["name"]!.ToString().Should().Be("Alice");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category G: File-Based Operations
// ═══════════════════════════════════════════════════════════════════════════

public class StatePersistenceFileTests
{
    [Fact]
    public async Task ExportStateToFile_CreatesFile()
    {
        var tempFile = Path.GetTempFileName();
        try
        {
            var container = new InMemoryContainer("file-create", "/pk");
            await container.CreateItemStreamAsync(
                new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a"}""")),
                new PartitionKey("a"));

            container.ExportStateToFile(tempFile);
            File.Exists(tempFile).Should().BeTrue();
            File.ReadAllText(tempFile).Should().Contain("\"id\": \"1\"");
        }
        finally { File.Delete(tempFile); }
    }

    [Fact]
    public async Task ExportStateToFile_OverwritesExistingFile()
    {
        var tempFile = Path.GetTempFileName();
        try
        {
            File.WriteAllText(tempFile, "old content");

            var container = new InMemoryContainer("file-overwrite", "/pk");
            await container.CreateItemStreamAsync(
                new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a"}""")),
                new PartitionKey("a"));

            container.ExportStateToFile(tempFile);
            File.ReadAllText(tempFile).Should().NotContain("old content");
        }
        finally { File.Delete(tempFile); }
    }

    [Fact]
    public void ImportStateFromFile_FileNotFound_Throws()
    {
        var container = new InMemoryContainer("file-404", "/pk");
        var act = () => container.ImportStateFromFile("/nonexistent/path.json");
        act.Should().Throw<Exception>();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category H: ClearItems
// ═══════════════════════════════════════════════════════════════════════════

public class ClearItemsTests
{
    [Fact]
    public async Task ClearItems_EmptiesAllStorage()
    {
        var container = new InMemoryContainer("clear-all", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a"}""")),
            new PartitionKey("a"));

        container.ClearItems();
        container.ItemCount.Should().Be(0);
    }

    [Fact]
    public void ClearItems_OnEmptyContainer_DoesNotThrow()
    {
        var container = new InMemoryContainer("clear-empty", "/pk");
        var act = () => container.ClearItems();
        act.Should().NotThrow();
    }

    [Fact]
    public async Task ClearItems_ThenCreateItem_WorksNormally()
    {
        var container = new InMemoryContainer("clear-then-create", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a"}""")),
            new PartitionKey("a"));

        container.ClearItems();

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", name = "new" }),
            new PartitionKey("a"));

        container.ItemCount.Should().Be(1);
        var result = await container.ReadItemAsync<JObject>("2", new PartitionKey("a"));
        result.Resource["name"]!.ToString().Should().Be("new");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category I: Concurrency
// ═══════════════════════════════════════════════════════════════════════════

public class StatePersistenceConcurrencyTests
{
    [Fact]
    public async Task ExportState_WhileWritesHappening_DoesNotThrow()
    {
        var container = new InMemoryContainer("conc-export", "/pk");
        for (var i = 0; i < 50; i++)
            await container.CreateItemStreamAsync(
                new MemoryStream(Encoding.UTF8.GetBytes($$$"""{"id":"{{{i}}}","pk":"a"}""")),
                new PartitionKey("a"));

        var tasks = Enumerable.Range(50, 50).Select(async i =>
        {
            await container.CreateItemStreamAsync(
                new MemoryStream(Encoding.UTF8.GetBytes($$$"""{"id":"{{{i}}}","pk":"a"}""")),
                new PartitionKey("a"));
        }).ToList();

        var exportTask = Task.Run(() => container.ExportState());

        var act = async () => await Task.WhenAll(tasks.Append(exportTask));
        await act.Should().NotThrowAsync();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category J: Cross-Container Export/Import
// ═══════════════════════════════════════════════════════════════════════════

public class CrossContainerExportImportTests
{
    [Fact]
    public async Task ExportFromOneContainer_ImportToAnother_DifferentNames()
    {
        var source = new InMemoryContainer("source-name", "/pk");
        await source.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", name = "data" }),
            new PartitionKey("a"));

        var target = new InMemoryContainer("different-name", "/pk");
        target.ImportState(source.ExportState());

        target.ItemCount.Should().Be(1);
        var result = await target.ReadItemAsync<JObject>("1", new PartitionKey("a"));
        result.Resource["name"]!.ToString().Should().Be("data");
    }

    [Fact]
    public async Task ExportState_ImportState_MultipleTimes_NoStateLeakage()
    {
        var container = new InMemoryContainer("leak-test", "/pk");

        // First cycle
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"A","pk":"a"}""")),
            new PartitionKey("a"));
        var json1 = container.ExportState();

        // Wipe and reimport
        container.ClearItems();
        container.ImportState("""{"items":[{"id":"B","pk":"b"}]}""");

        // Second export — should only have B
        var json2 = container.ExportState();
        var items2 = (JArray)JObject.Parse(json2)["items"]!;
        items2.Should().HaveCount(1);
        items2[0]["id"]!.ToString().Should().Be("B");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category K: Data Fidelity After Import
// ═══════════════════════════════════════════════════════════════════════════

public class DataFidelityAfterImportTests
{
    [Fact]
    public async Task ImportState_ItemsCanBeUpdatedAfterImport()
    {
        var container = new InMemoryContainer("fidelity-update", "/pk");
        container.ImportState("""{"items":[{"id":"1","pk":"a","name":"original"}]}""");

        await container.UpsertItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", name = "updated" }),
            new PartitionKey("a"));

        var result = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
        result.Resource["name"]!.ToString().Should().Be("updated");
    }

    [Fact]
    public async Task ImportState_ItemsCanBeDeletedAfterImport()
    {
        var container = new InMemoryContainer("fidelity-delete", "/pk");
        container.ImportState("""{"items":[{"id":"1","pk":"a"}]}""");

        await container.DeleteItemAsync<JObject>("1", new PartitionKey("a"));
        container.ItemCount.Should().Be(0);
    }

    [Fact]
    public async Task ImportState_ItemsReadableViaReadItemStream()
    {
        var container = new InMemoryContainer("fidelity-stream", "/pk");
        container.ImportState("""{"items":[{"id":"1","pk":"a","name":"stream"}]}""");

        using var response = await container.ReadItemStreamAsync("1", new PartitionKey("a"));
        response.StatusCode.Should().Be(HttpStatusCode.OK);

        using var reader = new StreamReader(response.Content);
        var body = await reader.ReadToEndAsync();
        body.Should().Contain("stream");
    }

    [Fact]
    public async Task ImportState_ItemsQueryable()
    {
        var container = new InMemoryContainer("fidelity-query", "/pk");
        container.ImportState("""{"items":[{"id":"1","pk":"a","value":10},{"id":"2","pk":"a","value":20}]}""");

        var iter = container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.value > 15");
        var page = await iter.ReadNextAsync();

        page.Should().HaveCount(1);
        page.First()["id"]!.ToString().Should().Be("2");
    }
}
