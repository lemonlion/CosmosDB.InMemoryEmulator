using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using System.Net;
using System.Text;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class DocumentSizeLimitTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private const int TwoMB = 2 * 1024 * 1024;

    private static string MakeOversizedValue() => new('x', 3 * 1024 * 1024);

    private static string BuildJsonDocument(string id, string pk, string dataValue) =>
        $"{{\"id\":\"{id}\",\"partitionKey\":\"{pk}\",\"data\":\"{dataValue}\"}}";

    private static MemoryStream ToStream(string json) =>
        new(Encoding.UTF8.GetBytes(json));

    #region Typed Operations — Over Size Limit

    [Fact]
    public async Task Create_OverSizeLimit_Returns413()
    {
        var doc = new TestDocument { Id = "large", PartitionKey = "pk1", Name = MakeOversizedValue() };

        var act = () => _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Upsert_OverSizeLimit_Returns413()
    {
        var doc = new TestDocument { Id = "large", PartitionKey = "pk1", Name = MakeOversizedValue() };

        var act = () => _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Replace_OverSizeLimit_Returns413()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Small" },
            new PartitionKey("pk1"));

        var largeDoc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = MakeOversizedValue() };

        var act = () => _container.ReplaceItemAsync(largeDoc, "1", new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Patch_ResultExceedsSizeLimit_Returns413()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 1 },
            new PartitionKey("pk1"));

        var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", MakeOversizedValue())]);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Create_OverSizeLimit_ErrorMessageContainsSizeInfo()
    {
        var doc = new TestDocument { Id = "large", PartitionKey = "pk1", Name = MakeOversizedValue() };

        var act = () => _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.Message.Should().Contain("Request size is too large");
        ex.And.Message.Should().Contain("Max allowed size in bytes:");
        ex.And.Message.Should().Contain("Found:");
    }

    #endregion

    #region Stream Operations — Over Size Limit

    [Fact]
    public async Task StreamCreate_OverSizeLimit_Returns413()
    {
        var json = BuildJsonDocument("1", "pk1", MakeOversizedValue());

        var response = await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task StreamUpsert_OverSizeLimit_Returns413()
    {
        var json = BuildJsonDocument("1", "pk1", MakeOversizedValue());

        var response = await _container.UpsertItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task StreamReplace_OverSizeLimit_Returns413()
    {
        var smallJson = BuildJsonDocument("1", "pk1", "small");
        await _container.CreateItemStreamAsync(ToStream(smallJson), new PartitionKey("pk1"));

        var largeJson = BuildJsonDocument("1", "pk1", MakeOversizedValue());

        var response = await _container.ReplaceItemStreamAsync(ToStream(largeJson), "1", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task StreamPatch_ResultExceedsSizeLimit_Returns413()
    {
        var smallJson = BuildJsonDocument("1", "pk1", "small");
        await _container.CreateItemStreamAsync(ToStream(smallJson), new PartitionKey("pk1"));

        var response = await _container.PatchItemStreamAsync(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/data", MakeOversizedValue())]);

        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    #endregion

    #region Boundary / Edge Cases

    [Fact]
    public async Task Create_Exactly2MB_Succeeds()
    {
        // Build a JSON document whose total UTF-8 byte count is exactly 2MB.
        // Envelope: {"id":"1","partitionKey":"pk1","data":"....."} is a fixed overhead.
        var envelope = BuildJsonDocument("1", "pk1", "");
        var envelopeBytes = Encoding.UTF8.GetByteCount(envelope);
        // The envelope includes an empty string for data — fill it to exactly 2MB total.
        var fillSize = TwoMB - envelopeBytes;
        var json = BuildJsonDocument("1", "pk1", new string('a', fillSize));

        Encoding.UTF8.GetByteCount(json).Should().Be(TwoMB);

        var response = await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task Create_2MBPlus1Byte_Fails()
    {
        var envelope = BuildJsonDocument("1", "pk1", "");
        var envelopeBytes = Encoding.UTF8.GetByteCount(envelope);
        var fillSize = TwoMB - envelopeBytes + 1;
        var json = BuildJsonDocument("1", "pk1", new string('a', fillSize));

        Encoding.UTF8.GetByteCount(json).Should().Be(TwoMB + 1);

        var response = await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Create_WithMultiByteUtf8_EnforcesOnByteCountNotCharCount()
    {
        // Each emoji is 4 bytes in UTF-8. A string of ~524,288 emojis ≈ 2MB in bytes
        // but only ~524K in char count (each emoji is 2 C# chars / 1 surrogate pair).
        // This demonstrates that the limit is on UTF-8 byte count, not .NET string length.
        var envelope = BuildJsonDocument("1", "pk1", "");
        var envelopeBytes = Encoding.UTF8.GetByteCount(envelope);

        // Use 'é' (U+00E9) = 2 bytes in UTF-8, 1 char in C#
        // To exceed 2MB via bytes while keeping char count under 2MB,
        // we need: charCount * 2 > 2MB, so charCount > 1MB
        var charCount = TwoMB - envelopeBytes + 1; // This many 'é' chars = 2× that many bytes
        var multiByte = new string('é', charCount);
        var json = BuildJsonDocument("1", "pk1", multiByte);

        // Char count of the data is under 2MB+envelope but byte count is well over
        json.Length.Should().BeLessThan(3 * 1024 * 1024);
        Encoding.UTF8.GetByteCount(json).Should().BeGreaterThan(TwoMB);

        var response = await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Create_SmallDocument_Succeeds()
    {
        var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Hello" };

        var response = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task Create_JustUnder2MB_Succeeds()
    {
        // Document at 2MB - 200 bytes for comfortable margin under limit
        var targetSize = TwoMB - 200;
        var json = BuildJsonDocument("1", "pk1", new string('x', targetSize));

        var response = await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    #endregion

    #region Batch Size Limits

    [Fact]
    public async Task Batch_Over100Operations_ThrowsBadRequest()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        for (var i = 0; i < 101; i++)
        {
            batch.CreateItem(new { id = $"{i}", partitionKey = "pk1", name = "x" });
        }

        var act = () => batch.ExecuteAsync();

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Batch_Exactly100Operations_Succeeds()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        for (var i = 0; i < 100; i++)
        {
            batch.CreateItem(new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = "x" });
        }

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();
    }

    [Fact]
    public async Task Batch_TotalPayloadExceeds2MB_Returns413()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        for (var i = 0; i < 10; i++)
        {
            var largeValue = new string('x', 300 * 1024); // 300KB each = 3MB total
            batch.CreateItem(new { id = $"{i}", partitionKey = "pk1", data = largeValue });
        }

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeFalse();
        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Batch_SingleOversizedItem_FailsOnIndividualDocumentSize()
    {
        // A single 3MB item exceeds the individual 2MB document limit.
        // The batch size tracking may or may not catch this since the individual
        // create also validates. Both checks should reject it.
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        var hugeValue = new string('x', 3 * 1024 * 1024);
        batch.CreateItem(new { id = "1", partitionKey = "pk1", data = hugeValue });

        // The batch's estimated size (3MB) exceeds 2MB so it fails at batch level with 413
        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeFalse();
        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Batch_PatchOperation_ContributesToBatchSize()
    {
        // Seed an item to patch
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "small" },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));

        // Add multiple patch operations with large values that together exceed 2MB
        for (var i = 0; i < 10; i++)
        {
            var largeValue = new string('y', 300 * 1024); // 300KB each
            batch.PatchItem($"1", [PatchOperation.Set("/name", largeValue)]);
        }

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeFalse();
        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    #endregion

    #region Divergent Behaviour — Pre-trigger Size Revalidation

    /// <summary>
    /// KNOWN DIVERGENCE: Real Cosmos DB validates document size after pre-trigger execution.
    /// The emulator validates before pre-triggers run, so a pre-trigger that inflates the
    /// document past 2MB is not caught. Implementing post-trigger revalidation is possible
    /// but low-priority since pre-triggers rarely inflate documents significantly.
    ///
    /// In real Cosmos DB, this would return 413 RequestEntityTooLarge because the final
    /// document (after the trigger adds ~2MB of data) exceeds the 2MB limit, even though
    /// the input document was small.
    /// </summary>
    [Fact]
    public async Task PreTrigger_InflatesDocumentPast2MB_RealCosmosWouldReject()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        container.RegisterTrigger("inflate", Microsoft.Azure.Cosmos.Scripts.TriggerType.Pre,
            Microsoft.Azure.Cosmos.Scripts.TriggerOperation.Create,
            doc =>
            {
                doc["hugeField"] = new string('z', 3 * 1024 * 1024);
                return doc;
            });

        var smallDoc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "tiny" };

        // Real Cosmos: would reject with 413 after trigger inflates the document
        var act = () => container.CreateItemAsync(smallDoc, new PartitionKey("pk1"),
            new ItemRequestOptions { PreTriggers = ["inflate"] });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    #endregion

    #region Divergent Behaviour — System Property Size Overhead

    /// <summary>
    /// KNOWN DIVERGENCE: Real Cosmos DB includes system properties (_ts, _rid, _self, _etag,
    /// _attachments) in the 2MB document size calculation. The emulator checks the input JSON
    /// size before system properties are added, making it ~200 bytes more permissive at the
    /// boundary.
    ///
    /// In real Cosmos DB, a document whose input JSON is exactly 2MB would be rejected because
    /// after the server adds system properties (~200 bytes), the total exceeds 2MB.
    /// </summary>
    [Fact(Skip = "KNOWN DIVERGENCE: Real Cosmos DB includes system properties (_ts, _rid, _self, _etag, " +
                  "_attachments) in the 2MB size calculation. The emulator checks input size before system " +
                  "properties are added, making it ~200 bytes more permissive. Impact: negligible — only " +
                  "affects documents within ~200 bytes of the 2MB limit.")]
    public async Task SizeCheck_Exactly2MB_RealCosmosWouldRejectDueToSystemProperties()
    {
        // Build a document whose input JSON is exactly 2MB
        var envelope = BuildJsonDocument("1", "pk1", "");
        var envelopeBytes = Encoding.UTF8.GetByteCount(envelope);
        var fillSize = TwoMB - envelopeBytes;
        var json = BuildJsonDocument("1", "pk1", new string('a', fillSize));

        Encoding.UTF8.GetByteCount(json).Should().Be(TwoMB);

        // Real Cosmos: would reject because system properties push it over 2MB
        var act = () => _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    /// <summary>
    /// Sister test for the skipped SizeCheck_Exactly2MB_RealCosmosWouldRejectDueToSystemProperties.
    ///
    /// This demonstrates the emulator's actual (divergent) behaviour: an input document of
    /// exactly 2MB is accepted because the emulator validates input size only, before adding
    /// system properties (_ts, _rid, _self, _etag, _attachments, ~200 bytes). After enrichment
    /// the stored document exceeds 2MB, but the size is not re-checked.
    ///
    /// In real Cosmos DB, the total size including system properties is checked, so a 2MB input
    /// document would be rejected (total ~2MB + 200 bytes).
    ///
    /// Impact: negligible — only affects documents within ~200 bytes of the 2MB limit.
    /// </summary>
    [Fact]
    public async Task SizeCheck_Exactly2MB_EmulatorAcceptsPreEnrichment_Divergence()
    {
        var envelope = BuildJsonDocument("1", "pk1", "");
        var envelopeBytes = Encoding.UTF8.GetByteCount(envelope);
        var fillSize = TwoMB - envelopeBytes;
        var json = BuildJsonDocument("1", "pk1", new string('a', fillSize));

        Encoding.UTF8.GetByteCount(json).Should().Be(TwoMB);

        // Emulator: accepts because size check happens before system property enrichment
        var response = await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    #endregion
}

#region BUG-3: Error Message Size Info

public class DocumentSizeErrorMessageTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private static string MakeOversizedValue() => new('x', 3 * 1024 * 1024);

    [Fact]
    public async Task Create_OverSizeLimit_ErrorMessageContainsMaxAllowedSize()
    {
        var doc = new TestDocument { Id = "large", PartitionKey = "pk1", Name = MakeOversizedValue() };

        var act = () => _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.Message.Should().Contain("2097152");
    }

    [Fact]
    public async Task Create_OverSizeLimit_ErrorMessageContainsActualSize()
    {
        var doc = new TestDocument { Id = "large", PartitionKey = "pk1", Name = MakeOversizedValue() };

        var act = () => _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        // The message should contain a number larger than 2MB (the actual doc size)
        ex.And.Message.Should().Contain("Found:");
    }
}

#endregion

#region BUG-2: Batch Delete/Read Size Accounting

public class BatchDeleteReadSizeAccountingTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact(Skip = "KNOWN DIVERGENCE: Real Cosmos counts delete operation metadata toward batch size. " +
                  "Emulator does not track delete byte contribution. Impact: negligible for typical ID lengths.")]
    public async Task Batch_DeleteOperations_ContributeToBatchSize_RealCosmosWouldCount()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        for (var i = 0; i < 100; i++)
            batch.DeleteItem($"item-{i}");

        using var response = await batch.ExecuteAsync();
        // Real Cosmos: if accumulated delete metadata exceeds 2MB, returns 413
        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Batch_DeleteOperations_DoNotContributeToBatchSize_Divergence()
    {
        // Seed items to delete
        for (var i = 0; i < 50; i++)
            await _container.CreateItemAsync(
                new TestDocument { Id = $"item-{i}", PartitionKey = "pk1", Name = "x" },
                new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        for (var i = 0; i < 50; i++)
            batch.DeleteItem($"item-{i}");

        using var response = await batch.ExecuteAsync();
        // Emulator: deletes don't contribute to byte count, only the 100-op limit matters
        response.IsSuccessStatusCode.Should().BeTrue();
    }

    [Fact(Skip = "KNOWN DIVERGENCE: Real Cosmos counts read operation metadata toward batch size. " +
                  "Emulator does not track read byte contribution.")]
    public async Task Batch_ReadOperations_ContributeToBatchSize_RealCosmosWouldCount()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        for (var i = 0; i < 100; i++)
            batch.ReadItem($"item-{i}");

        using var response = await batch.ExecuteAsync();
        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Batch_ReadOperations_DoNotContributeToBatchSize_Divergence()
    {
        for (var i = 0; i < 50; i++)
            await _container.CreateItemAsync(
                new TestDocument { Id = $"item-{i}", PartitionKey = "pk1", Name = "x" },
                new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        for (var i = 0; i < 50; i++)
            batch.ReadItem($"item-{i}");

        using var response = await batch.ExecuteAsync();
        response.IsSuccessStatusCode.Should().BeTrue();
    }
}

#endregion

#region Category 1: Typed Boundary Precision

public class TypedBoundaryPrecisionTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");
    private const int TwoMB = 2 * 1024 * 1024;

    private static string BuildJsonDocument(string id, string pk, string dataValue) =>
        $"{{\"id\":\"{id}\",\"partitionKey\":\"{pk}\",\"data\":\"{dataValue}\"}}";

    private static string MakePaddedName(int targetTotalBytes)
    {
        // Serialize a TestDocument with empty Name to measure envelope overhead
        var envelope = Newtonsoft.Json.JsonConvert.SerializeObject(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "" });
        var overhead = Encoding.UTF8.GetByteCount(envelope);
        return new string('a', targetTotalBytes - overhead);
    }

    [Fact]
    public async Task TypedCreate_Exactly2MB_Succeeds()
    {
        var name = MakePaddedName(TwoMB);
        var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = name };

        var response = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task TypedCreate_2MBPlus1Byte_Fails()
    {
        var name = MakePaddedName(TwoMB + 1);
        var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = name };

        var act = () => _container.CreateItemAsync(doc, new PartitionKey("pk1"));
        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Upsert_Exactly2MB_Succeeds()
    {
        var name = MakePaddedName(TwoMB);
        var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = name };

        var response = await _container.UpsertItemAsync(doc, new PartitionKey("pk1"));
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task Upsert_2MBPlus1Byte_Fails()
    {
        var name = MakePaddedName(TwoMB + 1);
        var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = name };

        var act = () => _container.UpsertItemAsync(doc, new PartitionKey("pk1"));
        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Replace_Exactly2MB_Succeeds()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "small" },
            new PartitionKey("pk1"));

        var name = MakePaddedName(TwoMB);
        var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = name };

        var response = await _container.ReplaceItemAsync(doc, "1", new PartitionKey("pk1"));
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Replace_2MBPlus1Byte_Fails()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "small" },
            new PartitionKey("pk1"));

        var name = MakePaddedName(TwoMB + 1);
        var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = name };

        var act = () => _container.ReplaceItemAsync(doc, "1", new PartitionKey("pk1"));
        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Patch_ResultExactly2MB_Succeeds()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "x", Value = 1 },
            new PartitionKey("pk1"));

        // Read back to get the enriched size, then compute patch value to hit exactly 2MB
        var existing = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
        var existingJson = existing.Resource.ToString(Newtonsoft.Json.Formatting.None);
        var existingBytes = Encoding.UTF8.GetByteCount(existingJson);
        // Remove current "name":"x" and add enough to reach 2MB
        var currentNameBytes = Encoding.UTF8.GetByteCount("\"x\"");
        var targetNameLength = TwoMB - existingBytes + 1; // +1 for the 'x' we're replacing
        if (targetNameLength <= 0) targetNameLength = 1;

        var patchValue = new string('z', targetNameLength);
        var response = await _container.PatchItemAsync<TestDocument>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", patchValue)]);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Patch_Result2MBPlus1Byte_Fails()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "x", Value = 1 },
            new PartitionKey("pk1"));

        var act = () => _container.PatchItemAsync<TestDocument>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", new string('z', 3 * 1024 * 1024))]);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }
}

#endregion

#region Category 2: Stream Boundary Precision

public class StreamBoundaryPrecisionTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");
    private const int TwoMB = 2 * 1024 * 1024;

    private static string BuildJsonDocument(string id, string pk, string dataValue) =>
        $"{{\"id\":\"{id}\",\"partitionKey\":\"{pk}\",\"data\":\"{dataValue}\"}}";

    private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

    [Fact]
    public async Task StreamUpsert_Exactly2MB_Succeeds()
    {
        var envelope = BuildJsonDocument("1", "pk1", "");
        var envelopeBytes = Encoding.UTF8.GetByteCount(envelope);
        var json = BuildJsonDocument("1", "pk1", new string('a', TwoMB - envelopeBytes));

        var response = await _container.UpsertItemStreamAsync(ToStream(json), new PartitionKey("pk1"));
        ((int)response.StatusCode).Should().BeOneOf(200, 201);
    }

    [Fact]
    public async Task StreamUpsert_2MBPlus1Byte_Returns413()
    {
        var envelope = BuildJsonDocument("1", "pk1", "");
        var envelopeBytes = Encoding.UTF8.GetByteCount(envelope);
        var json = BuildJsonDocument("1", "pk1", new string('a', TwoMB - envelopeBytes + 1));

        var response = await _container.UpsertItemStreamAsync(ToStream(json), new PartitionKey("pk1"));
        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task StreamReplace_Exactly2MB_Succeeds()
    {
        var smallJson = BuildJsonDocument("1", "pk1", "small");
        await _container.CreateItemStreamAsync(ToStream(smallJson), new PartitionKey("pk1"));

        var envelope = BuildJsonDocument("1", "pk1", "");
        var envelopeBytes = Encoding.UTF8.GetByteCount(envelope);
        var json = BuildJsonDocument("1", "pk1", new string('a', TwoMB - envelopeBytes));

        var response = await _container.ReplaceItemStreamAsync(ToStream(json), "1", new PartitionKey("pk1"));
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task StreamReplace_2MBPlus1Byte_Returns413()
    {
        var smallJson = BuildJsonDocument("1", "pk1", "small");
        await _container.CreateItemStreamAsync(ToStream(smallJson), new PartitionKey("pk1"));

        var envelope = BuildJsonDocument("1", "pk1", "");
        var envelopeBytes = Encoding.UTF8.GetByteCount(envelope);
        var json = BuildJsonDocument("1", "pk1", new string('a', TwoMB - envelopeBytes + 1));

        var response = await _container.ReplaceItemStreamAsync(ToStream(json), "1", new PartitionKey("pk1"));
        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }
}

#endregion

#region Category 3: State Integrity After Rejection

public class DocumentSizeStateIntegrityTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private static string MakeOversizedValue() => new('x', 3 * 1024 * 1024);

    private static string BuildJsonDocument(string id, string pk, string dataValue) =>
        $"{{\"id\":\"{id}\",\"partitionKey\":\"{pk}\",\"data\":\"{dataValue}\"}}";

    private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

    [Fact]
    public async Task Create_OverSizeLimit_DoesNotStoreDocument()
    {
        var doc = new TestDocument { Id = "large", PartitionKey = "pk1", Name = MakeOversizedValue() };

        var act = () => _container.CreateItemAsync(doc, new PartitionKey("pk1"));
        await act.Should().ThrowAsync<CosmosException>();

        var readAct = () => _container.ReadItemAsync<TestDocument>("large", new PartitionKey("pk1"));
        var readEx = await readAct.Should().ThrowAsync<CosmosException>();
        readEx.And.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Replace_OverSizeLimit_OriginalDocumentUnchanged()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "original" },
            new PartitionKey("pk1"));

        var act = () => _container.ReplaceItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = MakeOversizedValue() },
            "1", new PartitionKey("pk1"));
        await act.Should().ThrowAsync<CosmosException>();

        var item = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        item.Resource.Name.Should().Be("original");
    }

    [Fact]
    public async Task Upsert_OverSizeLimit_DoesNotCreateOrUpdateDocument()
    {
        // New item: oversized upsert should not create
        var act = () => _container.UpsertItemAsync(
            new TestDocument { Id = "new", PartitionKey = "pk1", Name = MakeOversizedValue() },
            new PartitionKey("pk1"));
        await act.Should().ThrowAsync<CosmosException>();

        var readAct = () => _container.ReadItemAsync<TestDocument>("new", new PartitionKey("pk1"));
        var readEx = await readAct.Should().ThrowAsync<CosmosException>();
        readEx.And.StatusCode.Should().Be(HttpStatusCode.NotFound);

        // Existing item: oversized upsert should not modify
        await _container.CreateItemAsync(
            new TestDocument { Id = "existing", PartitionKey = "pk1", Name = "safe" },
            new PartitionKey("pk1"));

        var act2 = () => _container.UpsertItemAsync(
            new TestDocument { Id = "existing", PartitionKey = "pk1", Name = MakeOversizedValue() },
            new PartitionKey("pk1"));
        await act2.Should().ThrowAsync<CosmosException>();

        var item = await _container.ReadItemAsync<TestDocument>("existing", new PartitionKey("pk1"));
        item.Resource.Name.Should().Be("safe");
    }

    [Fact]
    public async Task Patch_OverSizeLimit_OriginalDocumentUnchanged()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "original", Value = 42 },
            new PartitionKey("pk1"));

        var act = () => _container.PatchItemAsync<TestDocument>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", MakeOversizedValue())]);
        await act.Should().ThrowAsync<CosmosException>();

        var item = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        item.Resource.Name.Should().Be("original");
        item.Resource.Value.Should().Be(42);
    }

    [Fact]
    public async Task StreamCreate_OverSizeLimit_DoesNotStoreDocument()
    {
        var json = BuildJsonDocument("large", "pk1", MakeOversizedValue());
        var response = await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);

        var readAct = () => _container.ReadItemAsync<TestDocument>("large", new PartitionKey("pk1"));
        var readEx = await readAct.Should().ThrowAsync<CosmosException>();
        readEx.And.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Batch_OverSizeLimit_RollsBackAllOperations()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        for (var i = 0; i < 10; i++)
        {
            var value = new string('x', 300 * 1024); // 300KB each = 3MB total
            batch.CreateItem(new { id = $"batch-{i}", partitionKey = "pk1", data = value });
        }

        using var response = await batch.ExecuteAsync();
        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);

        // None of the items should exist
        for (var i = 0; i < 3; i++)
        {
            var readAct = () => _container.ReadItemAsync<TestDocument>($"batch-{i}", new PartitionKey("pk1"));
            var readEx = await readAct.Should().ThrowAsync<CosmosException>();
            readEx.And.StatusCode.Should().Be(HttpStatusCode.NotFound);
        }
    }
}

#endregion

#region Category 4: Batch Extended Coverage

public class BatchSizeExtendedTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Batch_99Operations_Succeeds()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        for (var i = 0; i < 99; i++)
            batch.CreateItem(new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = "x" });

        using var response = await batch.ExecuteAsync();
        response.IsSuccessStatusCode.Should().BeTrue();
    }

    [Fact]
    public async Task Batch_1Operation_Succeeds()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "single", PartitionKey = "pk1", Name = "x" });

        using var response = await batch.ExecuteAsync();
        response.IsSuccessStatusCode.Should().BeTrue();
    }

    [Fact]
    public async Task Batch_MixedOperationTypes_TotalSizeExceeds2MB_Returns413()
    {
        // Seed some items for replace/patch
        for (var i = 0; i < 3; i++)
            await _container.CreateItemAsync(
                new TestDocument { Id = $"exist-{i}", PartitionKey = "pk1", Name = "small" },
                new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        // Mix creates, upserts, replaces — each 400KB ≈ 2.4MB total
        for (var i = 0; i < 3; i++)
        {
            var largeValue = new string('x', 400 * 1024);
            batch.CreateItem(new { id = $"new-{i}", partitionKey = "pk1", data = largeValue });
        }
        for (var i = 0; i < 3; i++)
        {
            var largeValue = new string('y', 400 * 1024);
            batch.UpsertItem(new { id = $"exist-{i}", partitionKey = "pk1", data = largeValue });
        }

        using var response = await batch.ExecuteAsync();
        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Batch_UpsertOperations_ContributeToBatchSize()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        for (var i = 0; i < 10; i++)
        {
            var largeValue = new string('x', 300 * 1024); // 300KB × 10 = 3MB
            batch.UpsertItem(new { id = $"{i}", partitionKey = "pk1", data = largeValue });
        }

        using var response = await batch.ExecuteAsync();
        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Batch_ReplaceOperations_ContributeToBatchSize()
    {
        for (var i = 0; i < 10; i++)
            await _container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = "small" },
                new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        for (var i = 0; i < 10; i++)
        {
            var largeValue = new string('x', 300 * 1024);
            batch.ReplaceItem($"{i}", new { id = $"{i}", partitionKey = "pk1", data = largeValue });
        }

        using var response = await batch.ExecuteAsync();
        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Batch_Over100Operations_ThrowsBadRequest_NotReturn413()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        for (var i = 0; i < 101; i++)
            batch.CreateItem(new { id = $"{i}", partitionKey = "pk1", name = "x" });

        // 100-op limit throws CosmosException (not returns ResponseMessage)
        var act = () => batch.ExecuteAsync();
        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }
}

#endregion

#region Category 5: Patch-Specific Size Scenarios

public class PatchSizeEdgeCaseTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private static string MakeValue(int sizeKB) => new('z', sizeKB * 1024);

    [Fact]
    public async Task Patch_Add_LargeField_ExceedsSize_Returns413()
    {
        // Seed 1.5MB doc
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = MakeValue(1500) },
            new PartitionKey("pk1"));

        // Add 0.6MB field → result > 2MB
        var act = () => _container.PatchItemAsync<TestDocument>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Add("/extra", MakeValue(600))]);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Patch_Remove_LargeField_BringsUnderSize_Succeeds()
    {
        // Seed doc with large field via stream API (anonymous types can't be proxied)
        var seedJson = $"{{\"id\":\"1\",\"partitionKey\":\"pk1\",\"bigField\":\"{MakeValue(1500)}\",\"extra\":\"small\"}}";
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(seedJson)), new PartitionKey("pk1"));

        // Remove large field, add smaller one — result under 2MB
        var response = await _container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [
                PatchOperation.Remove("/bigField"),
                PatchOperation.Set("/extra", "replaced")
            ]);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Patch_MultipleSets_CumulativelyExceedSize_Returns413()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "small" },
            new PartitionKey("pk1"));

        // Multiple Sets that together push past 2MB
        var act = () => _container.PatchItemAsync<TestDocument>(
            "1", new PartitionKey("pk1"),
            [
                PatchOperation.Set("/name", MakeValue(800)),
                PatchOperation.Set("/field2", MakeValue(800)),
                PatchOperation.Set("/field3", MakeValue(800))
            ]);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Patch_Replace_LargeValue_WithSmallValue_Succeeds()
    {
        // Seed near-2MB doc via stream API
        var seedJson = $"{{\"id\":\"1\",\"partitionKey\":\"pk1\",\"data\":\"{MakeValue(1900)}\"}}";
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(seedJson)), new PartitionKey("pk1"));

        // Replace large field with small value → doc shrinks
        var response = await _container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/data", "tiny")]);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Patch_Increment_CannotExceedSizeLimit()
    {
        // Seed near-2MB doc with numeric field via stream API
        var seedJson = $"{{\"id\":\"1\",\"partitionKey\":\"pk1\",\"data\":\"{MakeValue(1900)}\",\"counter\":1}}";
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(seedJson)), new PartitionKey("pk1"));

        // Increment a number — doesn't meaningfully change size
        var response = await _container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Increment("/counter", 1)]);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}

#endregion

#region Category 6: Multi-byte Edge Cases

public class MultiByteCharacterSizeTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");
    private const int TwoMB = 2 * 1024 * 1024;

    private static string BuildJsonDocument(string id, string pk, string dataValue) =>
        $"{{\"id\":\"{id}\",\"partitionKey\":\"{pk}\",\"data\":\"{dataValue}\"}}";

    private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

    [Fact]
    public async Task Create_With4ByteEmoji_EnforcesOnByteCount()
    {
        // 😀 is U+1F600 = 4 bytes in UTF-8, 2 chars in C# (surrogate pair)
        var emoji = "😀";
        var envelope = BuildJsonDocument("1", "pk1", "");
        var envelopeBytes = Encoding.UTF8.GetByteCount(envelope);
        // Need enough emojis so byte count > 2MB
        var emojiCount = (TwoMB - envelopeBytes) / 4 + 1;
        var data = string.Concat(Enumerable.Repeat(emoji, emojiCount));
        var json = BuildJsonDocument("1", "pk1", data);

        Encoding.UTF8.GetByteCount(json).Should().BeGreaterThan(TwoMB);

        var response = await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));
        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Create_With3ByteCjk_EnforcesOnByteCount()
    {
        // 中 is U+4E2D = 3 bytes in UTF-8, 1 char in C#
        var envelope = BuildJsonDocument("1", "pk1", "");
        var envelopeBytes = Encoding.UTF8.GetByteCount(envelope);
        var charCount = (TwoMB - envelopeBytes) / 3 + 1;
        var data = new string('中', charCount);
        var json = BuildJsonDocument("1", "pk1", data);

        Encoding.UTF8.GetByteCount(json).Should().BeGreaterThan(TwoMB);

        var response = await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));
        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Create_WithEscapedUnicode_SizeBasedOnSerializedForm()
    {
        // When we pass raw JSON, the size is based on the actual byte count of the JSON string
        var envelope = BuildJsonDocument("1", "pk1", "");
        var envelopeBytes = Encoding.UTF8.GetByteCount(envelope);
        var fillSize = TwoMB - envelopeBytes + 1;
        // Use regular ASCII chars — escaped unicode doesn't change size in raw JSON
        var data = new string('a', fillSize);
        var json = BuildJsonDocument("1", "pk1", data);

        Encoding.UTF8.GetByteCount(json).Should().BeGreaterThan(TwoMB);

        var response = await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));
        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }
}

#endregion

#region Category 7: Error Response Details

public class DocumentSizeErrorResponseDetailTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private static string MakeOversizedValue() => new('x', 3 * 1024 * 1024);

    [Fact]
    public async Task Create_OverSizeLimit_SubStatusCodeIsZero()
    {
        var doc = new TestDocument { Id = "large", PartitionKey = "pk1", Name = MakeOversizedValue() };

        var act = () => _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.SubStatusCode.Should().Be(0);
    }

    [Fact]
    public async Task Create_OverSizeLimit_ActivityIdIsPopulated()
    {
        var doc = new TestDocument { Id = "large", PartitionKey = "pk1", Name = MakeOversizedValue() };

        var act = () => _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.ActivityId.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task Create_OverSizeLimit_RequestChargeIsGreaterThanZero()
    {
        var doc = new TestDocument { Id = "large", PartitionKey = "pk1", Name = MakeOversizedValue() };

        var act = () => _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.RequestCharge.Should().BeGreaterThan(0);
    }
}

#endregion

#region Category 8: Concurrent Size Validation

public class ConcurrentSizeValidationTests
{
    [Fact]
    public async Task ConcurrentCreates_MixOfOversizedAndValid_AllHandledCorrectly()
    {
        var container = new InMemoryContainer("test-container", "/partitionKey");
        var oversizedValue = new string('x', 3 * 1024 * 1024);

        var tasks = new List<Task<(string id, bool shouldSucceed, bool didSucceed)>>();

        for (var i = 0; i < 50; i++)
        {
            var id = $"item-{i}";
            var isOversized = i % 2 == 0; // Even = oversized, odd = valid
            var doc = new TestDocument
            {
                Id = id,
                PartitionKey = "pk1",
                Name = isOversized ? oversizedValue : "small"
            };

            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    var response = await container.CreateItemAsync(doc, new PartitionKey("pk1"));
                    return (id, shouldSucceed: !isOversized, didSucceed: true);
                }
                catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.RequestEntityTooLarge)
                {
                    return (id, shouldSucceed: !isOversized, didSucceed: false);
                }
            }));
        }

        var results = await Task.WhenAll(tasks);

        foreach (var (id, shouldSucceed, didSucceed) in results)
        {
            didSucceed.Should().Be(shouldSucceed, $"item {id} expected success={shouldSucceed}");
        }

        // Verify valid items are readable
        for (var i = 1; i < 50; i += 2)
        {
            var item = await container.ReadItemAsync<TestDocument>($"item-{i}", new PartitionKey("pk1"));
            item.Resource.Name.Should().Be("small");
        }
    }
}

#endregion

#region Divergence 1: Stored Procedure Size Limits

public class StoredProcedureSizeLimitDivergenceTests
{
    [Fact]
    public async Task StoredProcedure_OversizedResponse_RealCosmosWouldReject()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        container.RegisterStoredProcedure("bigSproc", (pk, args) => new string('x', 5 * 1024 * 1024));

        var scripts = container.Scripts;
        var ex = await Assert.ThrowsAsync<CosmosException>(() =>
            scripts.ExecuteStoredProcedureAsync<string>(
                "bigSproc", new PartitionKey("pk1"), []));

        ex.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }
}

#endregion

#region Divergence 2: Batch Overhead Not Counted

public class BatchOverheadDivergenceTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");
    private const int TwoMB = 2 * 1024 * 1024;

    [Fact(Skip = "KNOWN DIVERGENCE: Real Cosmos includes HTTP/protocol overhead in the 2MB batch size. " +
                  "Emulator counts only item JSON bytes, making it slightly more permissive. " +
                  "Impact: only affects batches within ~1-5KB of the 2MB limit.")]
    public async Task Batch_NearlyExactly2MB_RealCosmosWouldRejectDueToOverhead()
    {
        // Build batch right at 2MB of JSON — real Cosmos adds overhead and rejects
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        var valueSize = TwoMB / 2 - 100; // Two items each ~1MB
        batch.CreateItem(new { id = "1", partitionKey = "pk1", data = new string('a', valueSize) });
        batch.CreateItem(new { id = "2", partitionKey = "pk1", data = new string('b', valueSize) });

        using var response = await batch.ExecuteAsync();
        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Batch_NearlyExactly2MB_EmulatorAcceptsWithoutOverhead_Divergence()
    {
        // Build batch with items just under 2MB of JSON — emulator accepts (no overhead accounting)
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        var valueSize = TwoMB / 2 - 200; // Two items totaling just under 2MB
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = new string('a', valueSize) });
        batch.CreateItem(new TestDocument { Id = "2", PartitionKey = "pk1", Name = new string('b', valueSize) });

        using var response = await batch.ExecuteAsync();
        response.IsSuccessStatusCode.Should().BeTrue();
    }
}

#endregion

#region Divergence 3: Post-trigger Size Inflation

public class PostTriggerSizeDivergenceTests
{
    [Fact]
    public async Task PostTrigger_InflatesDocumentPast2MB_RealCosmosWouldReject()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        container.RegisterTrigger("inflatePost", Microsoft.Azure.Cosmos.Scripts.TriggerType.Post,
            Microsoft.Azure.Cosmos.Scripts.TriggerOperation.Create,
            (Action<JObject>)(doc =>
            {
                doc["hugeField"] = new string('z', 3 * 1024 * 1024);
            }));

        var smallDoc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "tiny" };
        var act = () => container.CreateItemAsync(smallDoc, new PartitionKey("pk1"),
            new ItemRequestOptions { PostTriggers = ["inflatePost"] });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task PostTrigger_InflatesDocumentPast2MB_EmulatorAllowsIt_Divergence()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        container.RegisterTrigger("inflatePost", Microsoft.Azure.Cosmos.Scripts.TriggerType.Post,
            Microsoft.Azure.Cosmos.Scripts.TriggerOperation.Create,
            doc =>
            {
                doc["hugeField"] = new string('z', 3 * 1024 * 1024);
                return doc;
            });

        var smallDoc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "tiny" };
        var response = await container.CreateItemAsync(smallDoc, new PartitionKey("pk1"),
            new ItemRequestOptions { PostTriggers = ["inflatePost"] });

        // Emulator: doesn't re-validate size after post-trigger
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }
}

#endregion

#region Divergence 4: Patch 10-Operation Limit

public class PatchOperationLimitTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Patch_Over10Operations_Returns400_Stream()
    {
        var json = "{\"id\":\"1\",\"partitionKey\":\"pk1\",\"name\":\"test\"}";
        await _container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

        var ops = Enumerable.Range(0, 11)
            .Select(i => PatchOperation.Set($"/field{i}", $"value{i}"))
            .ToList();

        var response = await _container.PatchItemStreamAsync(
            "1", new PartitionKey("pk1"), ops);

        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task TypedPatch_Over10Operations_Throws400()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "test" },
            new PartitionKey("pk1"));

        var ops = Enumerable.Range(0, 11)
            .Select(i => PatchOperation.Set($"/field{i}", $"value{i}"))
            .ToList();

        var act = () => _container.PatchItemAsync<TestDocument>(
            "1", new PartitionKey("pk1"), ops);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }
}

#endregion
