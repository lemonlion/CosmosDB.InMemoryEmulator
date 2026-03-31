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
