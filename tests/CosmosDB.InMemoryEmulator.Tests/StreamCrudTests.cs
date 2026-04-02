using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class StreamCrudTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));
    private static string ReadStream(Stream stream)
    {
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    [Fact]
    public async Task CreateItemStreamAsync_ValidItem_ReturnsCreated()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        var response = await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Content.Should().NotBeNull();
    }

    [Fact]
    public async Task CreateItemStreamAsync_Duplicate_ReturnsConflict()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        var response = await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task ReadItemStreamAsync_ExistingItem_ReturnsOkWithContent()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        var response = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Content.Should().NotBeNull();
        var body = ReadStream(response.Content);
        var jObj = JObject.Parse(body);
        jObj["name"]!.ToString().Should().Be("Test");
    }

    [Fact]
    public async Task ReadItemStreamAsync_NonExistent_ReturnsNotFound()
    {
        var response = await _container.ReadItemStreamAsync("nonexistent", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task UpsertItemStreamAsync_NewItem_ReturnsCreated()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        var response = await _container.UpsertItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task UpsertItemStreamAsync_ExistingItem_ReturnsOk()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Original"}""";
        await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        var updatedJson = """{"id":"1","partitionKey":"pk1","name":"Updated"}""";
        var response = await _container.UpsertItemStreamAsync(ToStream(updatedJson), new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task ReplaceItemStreamAsync_ExistingItem_ReturnsOk()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Original"}""";
        await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        var replacementJson = """{"id":"1","partitionKey":"pk1","name":"Replaced"}""";
        var response = await _container.ReplaceItemStreamAsync(ToStream(replacementJson), "1", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task ReplaceItemStreamAsync_NonExistent_ReturnsNotFound()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        var response = await _container.ReplaceItemStreamAsync(ToStream(json), "1", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task DeleteItemStreamAsync_ExistingItem_ReturnsNoContent()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        var response = await _container.DeleteItemStreamAsync("1", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task DeleteItemStreamAsync_NonExistent_ReturnsNotFound()
    {
        var response = await _container.DeleteItemStreamAsync("nonexistent", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task PatchItemStreamAsync_ExistingItem_ReturnsOk()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original", Value = 1 };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var patchOperations = new[] { PatchOperation.Replace("/name", "Patched") };
        var response = await _container.PatchItemStreamAsync("1", new PartitionKey("pk1"), patchOperations);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task PatchItemStreamAsync_NonExistent_ReturnsNotFound()
    {
        var patchOperations = new[] { PatchOperation.Replace("/name", "Patched") };
        var response = await _container.PatchItemStreamAsync("nonexistent", new PartitionKey("pk1"), patchOperations);

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task UpsertItemStreamAsync_MissingLowercaseId_ThrowsWithSerializerHint()
    {
        var json = """{"Id":"1","partitionKey":"pk1","name":"PascalCase"}""";

        var act = () => _container.UpsertItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<InvalidOperationException>();
        ex.Which.Message.Should().Contain("camelCase");
    }
}


public class StreamETagHandlingTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task UpsertItemStreamAsync_WithIfMatch_CurrentETag_ReturnsOk()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Original"}""";
        var createResponse = await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));
        var etag = createResponse.Headers.ETag;

        var updatedJson = """{"id":"1","partitionKey":"pk1","name":"Updated"}""";
        var response = await _container.UpsertItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(updatedJson)),
            new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = etag });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task UpsertItemStreamAsync_WithIfMatch_StaleETag_ReturnsPreconditionFailed()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Original"}""";
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

        var updatedJson = """{"id":"1","partitionKey":"pk1","name":"Updated"}""";
        var response = await _container.UpsertItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(updatedJson)),
            new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

        response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task PatchItemStreamAsync_WithIfMatch_StaleETag_ReturnsPreconditionFailed()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 10 },
            new PartitionKey("pk1"));

        var response = await _container.PatchItemStreamAsync(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Patched")],
            new PatchItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

        response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task CreateItemStreamAsync_WithNullStream_Throws()
    {
        var act = () => _container.CreateItemStreamAsync(null!, new PartitionKey("pk1"));
        await act.Should().ThrowAsync<Exception>();
    }

    [Fact]
    public async Task ReplaceItemStreamAsync_WithIfMatch_CurrentETag_ReturnsOk()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Original"}""";
        var createResponse = await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));
        var etag = createResponse.Headers.ETag;

        var updatedJson = """{"id":"1","partitionKey":"pk1","name":"Replaced"}""";
        var response = await _container.ReplaceItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(updatedJson)),
            "1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = etag });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task ReplaceItemStreamAsync_WithIfMatch_StaleETag_ReturnsPreconditionFailed()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Original"}""";
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

        var updatedJson = """{"id":"1","partitionKey":"pk1","name":"Replaced"}""";
        var response = await _container.ReplaceItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(updatedJson)),
            "1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

        response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }
}


/// <summary>
/// Validates the stream API contract specifically for DeleteItemStreamAsync with stale ETag.
/// The Upsert/Patch/Replace variants are already tested in CosmosClientApiGapTests.cs
/// (StreamETagHandlingTests). This covers the missing Delete variant.
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.deleteitemstreamasync
/// </summary>
public class StreamDeleteETagTests5
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task DeleteItemStream_WithIfMatch_StaleETag_Returns412_DoesNotThrow()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

        var response = await _container.DeleteItemStreamAsync(
            "1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

        response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }
}


public class StreamETagGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

    [Fact]
    public async Task ReadStream_WithIfNoneMatch_CurrentETag_Returns304()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        var createResponse = await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));
        var etag = createResponse.Headers.ETag;

        var response = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfNoneMatchEtag = etag });

        response.StatusCode.Should().Be(HttpStatusCode.NotModified);
    }

    [Fact]
    public async Task UpsertStream_WithIfMatch_StaleETag_Returns412()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        var response = await _container.UpsertItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","name":"Updated"}"""),
            new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "\"stale\"" });

        response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task ReplaceStream_WithIfMatch_StaleETag_Returns412()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        var response = await _container.ReplaceItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","name":"Updated"}"""),
            "1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "\"stale\"" });

        response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task DeleteStream_WithIfMatch_StaleETag_Returns412()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        var response = await _container.DeleteItemStreamAsync("1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "\"stale\"" });

        response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task PatchStream_WithIfMatch_StaleETag_Returns412()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test","value":10}""";
        await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        var response = await _container.PatchItemStreamAsync("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Patched")],
            new PatchItemRequestOptions { IfMatchEtag = "\"stale\"" });

        response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task CreateStream_ResponseContainsETagHeader()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        var response = await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        response.Headers.ETag.Should().NotBeNullOrEmpty();
    }
}


public class ResponseMetadataGapTests2
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task StreamResponse_StatusCode_InResponseMessage()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        var response = await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task StreamResponse_Content_ContainsDocumentJson()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

        var readResponse = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
        readResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        readResponse.Content.Should().NotBeNull();

        using var reader = new StreamReader(readResponse.Content);
        var body = await reader.ReadToEndAsync();
        body.Should().Contain("\"name\"");
    }
}


public class StreamOperationGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

    [Fact]
    public async Task CreateStream_WithoutId_AutoGeneratesId()
    {
        var json = """{"partitionKey":"pk1","name":"NoId"}""";
        var response = await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task CreateStream_Duplicate_Returns409_NotThrow()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        var response = await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task ReadStream_NotFound_Returns404StatusCode()
    {
        var response = await _container.ReadItemStreamAsync("missing", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task ReplaceStream_NotFound_Returns404StatusCode()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
        var response = await _container.ReplaceItemStreamAsync(ToStream(json), "1", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task DeleteStream_NotFound_Returns404StatusCode()
    {
        var response = await _container.DeleteItemStreamAsync("missing", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task UpsertStream_NewItem_Returns201_Existing_Returns200()
    {
        var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";

        var createResponse = await _container.UpsertItemStreamAsync(ToStream(json), new PartitionKey("pk1"));
        createResponse.StatusCode.Should().Be(HttpStatusCode.Created);

        var updateJson = """{"id":"1","partitionKey":"pk1","name":"Updated"}""";
        var updateResponse = await _container.UpsertItemStreamAsync(ToStream(updateJson), new PartitionKey("pk1"));
        updateResponse.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}


public class StreamOperationGapTests3
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

    [Fact]
    public async Task AllStreamMethods_ReturnStatusCode_NotThrow_OnError()
    {
        // Stream methods return errors via StatusCode, not exceptions
        var readResponse = await _container.ReadItemStreamAsync("missing", new PartitionKey("pk1"));
        readResponse.StatusCode.Should().Be(HttpStatusCode.NotFound);

        var replaceResponse = await _container.ReplaceItemStreamAsync(
            ToStream("""{"id":"missing","partitionKey":"pk1"}"""),
            "missing", new PartitionKey("pk1"));
        replaceResponse.StatusCode.Should().Be(HttpStatusCode.NotFound);

        var deleteResponse = await _container.DeleteItemStreamAsync("missing", new PartitionKey("pk1"));
        deleteResponse.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  B: Response Body Validation
// ═══════════════════════════════════════════════════════════════════════════

public class StreamResponseBodyTests
{
    private readonly InMemoryContainer _container = new("body-test", "/partitionKey");
    private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));
    private static async Task<string> ReadStreamAsync(Stream s) { using var r = new StreamReader(s); return await r.ReadToEndAsync(); }

    [Fact]
    public async Task CreateStream_ResponseContent_ContainsCreatedDocument()
    {
        using var response = await _container.CreateItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","name":"Alice"}"""),
            new PartitionKey("pk1"));

        response.Content.Should().NotBeNull();
        var body = JObject.Parse(await ReadStreamAsync(response.Content));
        body["id"]!.ToString().Should().Be("1");
        body["name"]!.ToString().Should().Be("Alice");
    }

    [Fact]
    public async Task UpsertStream_NewItem_ResponseContent_ContainsDocument()
    {
        using var response = await _container.UpsertItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","name":"Bob"}"""),
            new PartitionKey("pk1"));

        var body = JObject.Parse(await ReadStreamAsync(response.Content));
        body["name"]!.ToString().Should().Be("Bob");
    }

    [Fact]
    public async Task UpsertStream_ExistingItem_ResponseContent_ContainsUpdatedDocument()
    {
        await _container.CreateItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","name":"Old"}"""),
            new PartitionKey("pk1"));

        using var response = await _container.UpsertItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","name":"New"}"""),
            new PartitionKey("pk1"));

        var body = JObject.Parse(await ReadStreamAsync(response.Content));
        body["name"]!.ToString().Should().Be("New");
    }

    [Fact]
    public async Task ReplaceStream_ResponseContent_ContainsReplacedDocument()
    {
        await _container.CreateItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","name":"Original"}"""),
            new PartitionKey("pk1"));

        using var response = await _container.ReplaceItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","name":"Replaced"}"""),
            "1", new PartitionKey("pk1"));

        var body = JObject.Parse(await ReadStreamAsync(response.Content));
        body["name"]!.ToString().Should().Be("Replaced");
    }

    [Fact]
    public async Task ReadStream_AfterCreateStream_ReturnsCompleteDocument()
    {
        await _container.CreateItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","name":"Alice","value":42}"""),
            new PartitionKey("pk1"));

        using var response = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
        var body = JObject.Parse(await ReadStreamAsync(response.Content));
        body["name"]!.ToString().Should().Be("Alice");
        body["value"]!.Value<int>().Should().Be(42);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  C: System Properties in Stream Responses
// ═══════════════════════════════════════════════════════════════════════════

public class StreamSystemPropertyTests
{
    private readonly InMemoryContainer _container = new("sysprop-test", "/partitionKey");
    private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));
    private static async Task<string> ReadStreamAsync(Stream s) { using var r = new StreamReader(s); return await r.ReadToEndAsync(); }

    [Fact]
    public async Task CreateStream_ResponseBody_ContainsEtagSystemProperty()
    {
        using var response = await _container.CreateItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1"}"""),
            new PartitionKey("pk1"));

        var body = JObject.Parse(await ReadStreamAsync(response.Content));
        body["_etag"].Should().NotBeNull();
        body["_etag"]!.ToString().Should().NotBeEmpty();
    }

    [Fact]
    public async Task CreateStream_ResponseBody_ContainsTsSystemProperty()
    {
        using var response = await _container.CreateItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1"}"""),
            new PartitionKey("pk1"));

        var body = JObject.Parse(await ReadStreamAsync(response.Content));
        body["_ts"].Should().NotBeNull();
        body["_ts"]!.Value<long>().Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task UpsertStream_ExistingItem_UpdatesEtagAndTs()
    {
        await _container.CreateItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","name":"Old"}"""),
            new PartitionKey("pk1"));

        using var read1 = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
        var body1 = JObject.Parse(await ReadStreamAsync(read1.Content));
        var etag1 = body1["_etag"]!.ToString();

        await _container.UpsertItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","name":"New"}"""),
            new PartitionKey("pk1"));

        using var read2 = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
        var body2 = JObject.Parse(await ReadStreamAsync(read2.Content));
        body2["_etag"]!.ToString().Should().NotBe(etag1);
    }

    [Fact]
    public async Task ReplaceStream_UpdatesEtag()
    {
        await _container.CreateItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1"}"""),
            new PartitionKey("pk1"));

        using var read1 = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
        var etag1 = JObject.Parse(await ReadStreamAsync(read1.Content))["_etag"]!.ToString();

        await _container.ReplaceItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","name":"replaced"}"""),
            "1", new PartitionKey("pk1"));

        using var read2 = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
        var etag2 = JObject.Parse(await ReadStreamAsync(read2.Content))["_etag"]!.ToString();
        etag2.Should().NotBe(etag1);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  D: IsSuccessStatusCode
// ═══════════════════════════════════════════════════════════════════════════

public class StreamIsSuccessStatusCodeTests
{
    private readonly InMemoryContainer _container = new("success-test", "/partitionKey");
    private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

    [Fact]
    public async Task Stream_SuccessResponses_HaveIsSuccessStatusCode_True()
    {
        using var create = await _container.CreateItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1"}"""), new PartitionKey("pk1"));
        create.IsSuccessStatusCode.Should().BeTrue();

        using var read = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
        read.IsSuccessStatusCode.Should().BeTrue();

        using var upsert = await _container.UpsertItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","name":"updated"}"""), new PartitionKey("pk1"));
        upsert.IsSuccessStatusCode.Should().BeTrue();

        using var replace = await _container.ReplaceItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","name":"replaced"}"""),
            "1", new PartitionKey("pk1"));
        replace.IsSuccessStatusCode.Should().BeTrue();

        using var delete = await _container.DeleteItemStreamAsync("1", new PartitionKey("pk1"));
        delete.IsSuccessStatusCode.Should().BeTrue();
    }

    [Fact]
    public async Task Stream_ErrorResponses_HaveIsSuccessStatusCode_False()
    {
        using var readMiss = await _container.ReadItemStreamAsync("miss", new PartitionKey("pk1"));
        readMiss.IsSuccessStatusCode.Should().BeFalse();

        using var deleteMiss = await _container.DeleteItemStreamAsync("miss", new PartitionKey("pk1"));
        deleteMiss.IsSuccessStatusCode.Should().BeFalse();

        using var replaceMiss = await _container.ReplaceItemStreamAsync(
            ToStream("""{"id":"miss","partitionKey":"pk1"}"""), "miss", new PartitionKey("pk1"));
        replaceMiss.IsSuccessStatusCode.Should().BeFalse();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  E: Response Headers
// ═══════════════════════════════════════════════════════════════════════════

public class StreamCrudResponseHeaderTests
{
    private readonly InMemoryContainer _container = new("hdr-test", "/partitionKey");
    private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

    [Fact]
    public async Task Stream_AllCrudResponses_ContainRequestChargeHeader()
    {
        using var create = await _container.CreateItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1"}"""), new PartitionKey("pk1"));
        create.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();

        using var read = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
        read.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();

        using var upsert = await _container.UpsertItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","x":"y"}"""), new PartitionKey("pk1"));
        upsert.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();

        using var replace = await _container.ReplaceItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","z":"w"}"""), "1", new PartitionKey("pk1"));
        replace.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();

        using var delete = await _container.DeleteItemStreamAsync("1", new PartitionKey("pk1"));
        delete.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task Stream_AllCrudResponses_ContainActivityIdHeader()
    {
        using var create = await _container.CreateItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1"}"""), new PartitionKey("pk1"));
        create.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();

        using var read = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
        read.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();

        using var delete = await _container.DeleteItemStreamAsync("1", new PartitionKey("pk1"));
        delete.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task Stream_WriteResponses_ContainETagHeader()
    {
        using var create = await _container.CreateItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1"}"""), new PartitionKey("pk1"));
        create.Headers.ETag.Should().NotBeNullOrEmpty();

        using var upsert = await _container.UpsertItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","name":"u"}"""), new PartitionKey("pk1"));
        upsert.Headers.ETag.Should().NotBeNullOrEmpty();

        using var replace = await _container.ReplaceItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","name":"r"}"""), "1", new PartitionKey("pk1"));
        replace.Headers.ETag.Should().NotBeNullOrEmpty();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  F: ETag Lifecycle in Stream API
// ═══════════════════════════════════════════════════════════════════════════

public class StreamETagLifecycleTests
{
    private readonly InMemoryContainer _container = new("etag-life", "/partitionKey");
    private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

    [Fact]
    public async Task UpsertStream_ChangesETag()
    {
        using var create = await _container.CreateItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1"}"""), new PartitionKey("pk1"));
        var etag1 = create.Headers.ETag;

        using var upsert = await _container.UpsertItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","name":"u"}"""), new PartitionKey("pk1"));
        upsert.Headers.ETag.Should().NotBe(etag1);
    }

    [Fact]
    public async Task ReplaceStream_ChangesETag()
    {
        using var create = await _container.CreateItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1"}"""), new PartitionKey("pk1"));
        var etag1 = create.Headers.ETag;

        using var replace = await _container.ReplaceItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","name":"r"}"""), "1", new PartitionKey("pk1"));
        replace.Headers.ETag.Should().NotBe(etag1);
    }

    [Fact]
    public async Task ReadStream_ConsecutiveReads_ETagConsistent()
    {
        await _container.CreateItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1"}"""), new PartitionKey("pk1"));

        using var read1 = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
        using var read2 = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
        read1.Headers.ETag.Should().Be(read2.Headers.ETag);
    }

    [Fact]
    public async Task DeleteStream_WithIfMatch_CurrentETag_Succeeds()
    {
        using var create = await _container.CreateItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1"}"""), new PartitionKey("pk1"));
        var etag = create.Headers.ETag;

        using var delete = await _container.DeleteItemStreamAsync("1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = etag });
        delete.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  G: Data Integrity
// ═══════════════════════════════════════════════════════════════════════════

public class StreamDataIntegrityTests
{
    private readonly InMemoryContainer _container = new("integrity-test", "/partitionKey");
    private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));
    private static async Task<string> ReadStreamAsync(Stream s) { using var r = new StreamReader(s); return await r.ReadToEndAsync(); }

    [Fact]
    public async Task CreateStream_ThenTypedRead_DataRoundTrips()
    {
        await _container.CreateItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","name":"Alice","value":42}"""),
            new PartitionKey("pk1"));

        var result = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        result.Resource.Name.Should().Be("Alice");
        result.Resource.Value.Should().Be(42);
    }

    [Fact]
    public async Task TypedCreate_ThenStreamRead_DataRoundTrips()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Bob", Value = 99 },
            new PartitionKey("pk1"));

        using var response = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
        var body = JObject.Parse(await ReadStreamAsync(response.Content));
        body["name"]!.ToString().Should().Be("Bob");
        body["value"]!.Value<int>().Should().Be(99);
    }

    [Fact]
    public async Task UpsertStream_ReplacesEntireDocument_NotMerge()
    {
        await _container.CreateItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","name":"Alice","extra":"field"}"""),
            new PartitionKey("pk1"));

        await _container.UpsertItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","name":"Alice"}"""),
            new PartitionKey("pk1"));

        using var read = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
        var body = JObject.Parse(await ReadStreamAsync(read.Content));
        body["extra"].Should().BeNull("upsert replaces entire document, it does not merge");
    }

    [Fact]
    public async Task DeleteStream_ThenRead_Returns404()
    {
        await _container.CreateItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1"}"""), new PartitionKey("pk1"));

        await _container.DeleteItemStreamAsync("1", new PartitionKey("pk1"));

        using var read = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
        read.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task DeleteStream_ThenCreate_SameId_Succeeds()
    {
        await _container.CreateItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","name":"first"}"""),
            new PartitionKey("pk1"));

        await _container.DeleteItemStreamAsync("1", new PartitionKey("pk1"));

        using var create = await _container.CreateItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","name":"second"}"""),
            new PartitionKey("pk1"));
        create.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task ReplaceStream_FullyReplacesDocument()
    {
        await _container.CreateItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","fieldA":"a","fieldB":"b"}"""),
            new PartitionKey("pk1"));

        await _container.ReplaceItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","fieldA":"updated"}"""),
            "1", new PartitionKey("pk1"));

        using var read = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
        var body = JObject.Parse(await ReadStreamAsync(read.Content));
        body["fieldA"]!.ToString().Should().Be("updated");
        body["fieldB"].Should().BeNull("replace should fully replace, not merge");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  H: Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class StreamEdgeCaseTests
{
    private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));
    private static async Task<string> ReadStreamAsync(Stream s) { using var r = new StreamReader(s); return await r.ReadToEndAsync(); }

    [Fact]
    public async Task CreateStream_CompositePartitionKey_ExtractsCorrectly()
    {
        var container = new InMemoryContainer("composite-pk", new[] { "/tenantId", "/userId" });

        using var response = await container.CreateItemStreamAsync(
            ToStream("""{"id":"1","tenantId":"t1","userId":"u1","name":"Alice"}"""),
            new PartitionKeyBuilder().Add("t1").Add("u1").Build());

        response.StatusCode.Should().Be(HttpStatusCode.Created);

        var result = await container.ReadItemAsync<JObject>("1",
            new PartitionKeyBuilder().Add("t1").Add("u1").Build());
        result.Resource["name"]!.ToString().Should().Be("Alice");
    }

    [Fact]
    public async Task CreateStream_UnicodeContent_PreservedInResponse()
    {
        var container = new InMemoryContainer("unicode-stream", "/pk");

        using var response = await container.CreateItemStreamAsync(
            ToStream("""{"id":"1","pk":"a","name":"日本語テスト🎉"}"""),
            new PartitionKey("a"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);

        using var read = await container.ReadItemStreamAsync("1", new PartitionKey("a"));
        var body = JObject.Parse(await ReadStreamAsync(read.Content));
        body["name"]!.ToString().Should().Be("日本語テスト🎉");
    }

    [Fact]
    public async Task CreateStream_RecordsInChangeFeed()
    {
        var container = new InMemoryContainer("cf-stream", "/pk");

        await container.CreateItemStreamAsync(
            ToStream("""{"id":"1","pk":"a","name":"feedme"}"""),
            new PartitionKey("a"));

        var iter = container.GetChangeFeedIterator<JObject>(
            ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);
        var page = await iter.ReadNextAsync();

        page.Should().Contain(j => j["id"]!.ToString() == "1");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  I: Divergent Behaviors (Skip + Sister Tests)
// ═══════════════════════════════════════════════════════════════════════════

public class StreamDivergentBehaviorTests
{
    private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));
    private static async Task<string> ReadStreamAsync(Stream s) { using var r = new StreamReader(s); return await r.ReadToEndAsync(); }

    [Fact]
    public async Task ReplaceStream_IdMismatch_Returns400()
    {
        var container = new InMemoryContainer("mismatch", "/partitionKey");
        await container.CreateItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1"}"""), new PartitionKey("pk1"));

        using var response = await container.ReplaceItemStreamAsync(
            ToStream("""{"id":"wrong","partitionKey":"pk1"}"""),
            "1", new PartitionKey("pk1"));
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Divergent_ReplaceStream_IdMismatch_AlsoReturns400()
    {
        // The emulator now correctly validates body id matches parameter id,
        // matching real Cosmos DB behavior.
        var container = new InMemoryContainer("mismatch-div", "/partitionKey");
        await container.CreateItemStreamAsync(
            ToStream("""{"id":"1","partitionKey":"pk1","name":"orig"}"""), new PartitionKey("pk1"));

        using var response = await container.ReplaceItemStreamAsync(
            ToStream("""{"id":"wrong","partitionKey":"pk1","name":"replaced"}"""),
            "1", new PartitionKey("pk1"));
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact(Skip = "Real Cosmos DB includes _rid, _self, _attachments system properties. " +
        "InMemoryContainer only enriches with _etag and _ts.")]
    public async Task Stream_ResponseBody_ContainsAllSystemProperties()
    {
        var container = new InMemoryContainer("sysprop-all", "/pk");
        using var response = await container.CreateItemStreamAsync(
            ToStream("""{"id":"1","pk":"a"}"""), new PartitionKey("a"));

        var body = JObject.Parse(await ReadStreamAsync(response.Content));
        body["_rid"].Should().NotBeNull();
        body["_self"].Should().NotBeNull();
        body["_attachments"].Should().NotBeNull();
    }

    [Fact]
    public async Task Divergent_Stream_ResponseBody_ContainsOnlyEtagAndTs()
    {
        // Sister test: only _etag and _ts are present
        var container = new InMemoryContainer("sysprop-div", "/pk");
        using var response = await container.CreateItemStreamAsync(
            ToStream("""{"id":"1","pk":"a"}"""), new PartitionKey("a"));

        var body = JObject.Parse(await ReadStreamAsync(response.Content));
        body["_etag"].Should().NotBeNull();
        body["_ts"].Should().NotBeNull();
        body["_rid"].Should().BeNull("emulator does not generate _rid");
        body["_self"].Should().BeNull("emulator does not generate _self");
    }
}
