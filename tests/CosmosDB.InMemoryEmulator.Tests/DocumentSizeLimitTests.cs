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
    public async Task Create_OverSizeLimit_ErrorMessageContainsSizeTooLarge()
    {
        var doc = new TestDocument { Id = "large", PartitionKey = "pk1", Name = MakeOversizedValue() };

        var act = () => _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.Message.Should().Contain("Request size is too large");
    }

    #endregion

    #region Stream Operations — Over Size Limit

    [Fact]
    public async Task StreamCreate_OverSizeLimit_Throws413()
    {
        var json = BuildJsonDocument("1", "pk1", MakeOversizedValue());

        var act = () => _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task StreamUpsert_OverSizeLimit_Throws413()
    {
        var json = BuildJsonDocument("1", "pk1", MakeOversizedValue());

        var act = () => _container.UpsertItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task StreamReplace_OverSizeLimit_Throws413()
    {
        var smallJson = BuildJsonDocument("1", "pk1", "small");
        await _container.CreateItemStreamAsync(ToStream(smallJson), new PartitionKey("pk1"));

        var largeJson = BuildJsonDocument("1", "pk1", MakeOversizedValue());

        var act = () => _container.ReplaceItemStreamAsync(ToStream(largeJson), "1", new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task StreamPatch_ResultExceedsSizeLimit_Throws413()
    {
        var smallJson = BuildJsonDocument("1", "pk1", "small");
        await _container.CreateItemStreamAsync(ToStream(smallJson), new PartitionKey("pk1"));

        var act = () => _container.PatchItemStreamAsync(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/data", MakeOversizedValue())]);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
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

        var act = () => _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
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

        var act = () => _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.And.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
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
    [Fact(Skip = "KNOWN DIVERGENCE: Real Cosmos DB validates document size after pre-trigger execution. " +
                  "The emulator validates before pre-triggers run, so a pre-trigger that inflates the " +
                  "document past 2MB is not caught. Low-priority: pre-triggers rarely inflate documents significantly.")]
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

    /// <summary>
    /// Sister test for the skipped PreTrigger_InflatesDocumentPast2MB_RealCosmosWouldReject.
    ///
    /// This demonstrates the emulator's actual (divergent) behaviour: when a pre-trigger
    /// inflates a small document past 2MB, the emulator does NOT reject it because size
    /// validation happens before the trigger executes. The document is successfully stored
    /// despite exceeding the 2MB limit.
    ///
    /// Flow in emulator:
    ///   1. Serialize small doc → ~50 bytes
    ///   2. ValidateDocumentSize(~50 bytes) → passes ✓
    ///   3. ExecutePreTriggers → trigger adds ~3MB "hugeField"
    ///   4. Re-serialize → ~3MB (NO re-validation)
    ///   5. Store → success (diverges from real Cosmos)
    ///
    /// In real Cosmos DB, step 4 would be followed by a size check that rejects the document.
    /// </summary>
    [Fact]
    public async Task PreTrigger_InflatesDocumentPast2MB_EmulatorAllowsIt_Divergence()
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

        // Emulator: succeeds because size check is before trigger execution
        var response = await container.CreateItemAsync(smallDoc, new PartitionKey("pk1"),
            new ItemRequestOptions { PreTriggers = ["inflate"] });

        response.StatusCode.Should().Be(HttpStatusCode.Created);
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
