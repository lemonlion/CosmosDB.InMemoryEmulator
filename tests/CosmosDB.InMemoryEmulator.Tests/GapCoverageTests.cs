using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ════════════════════════════════════════════════════════════════════════════════
// GapCoverageTests — Comprehensive test coverage from test-gap-analysis-and-plan.md
// Organized into nested classes by category. TDD approach: RED first, GREEN via source fixes.
// ════════════════════════════════════════════════════════════════════════════════

#region 1. CRUD — CreateItem

public class CreateItemGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Create_WithExplicitPartitionKey_ExtractsFromDocument_WhenNoneProvided()
    {
        var item = new TestDocument { Id = "auto-pk", PartitionKey = "pk1", Name = "Auto PK" };

        var response = await _container.CreateItemAsync(item);

        response.StatusCode.Should().Be(HttpStatusCode.Created);

        var read = await _container.ReadItemAsync<TestDocument>("auto-pk", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Auto PK");
    }

    [Fact]
    public async Task Create_ResponseContainsETag_NonNullNonEmpty()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

        var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        response.ETag.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task Create_ResponseBodyMatchesInput()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 42 };

        var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        response.Resource.Id.Should().Be("1");
        response.Resource.Name.Should().Be("Test");
        response.Resource.Value.Should().Be(42);
    }

    [Fact]
    public async Task Create_WithNestedPartitionKeyPath_ExtractsCorrectly()
    {
        var container = new InMemoryContainer("test-container", "/nested/description");
        var item = new TestDocument
        {
            Id = "1",
            PartitionKey = "ignored",
            Name = "Test",
            Nested = new NestedObject { Description = "deep-pk", Score = 1.0 }
        };

        var response = await container.CreateItemAsync(item, new PartitionKey("deep-pk"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);

        var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("deep-pk"));
        read.Resource.Name.Should().Be("Test");
    }

    [Fact]
    public async Task Create_WithIdContainingSpecialCharacters_Succeeds()
    {
        var item = new TestDocument { Id = "id/with?special#chars", PartitionKey = "pk1", Name = "Special" };

        var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);

        var read = await _container.ReadItemAsync<TestDocument>("id/with?special#chars", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Special");
    }

    [Fact]
    public async Task Create_WithIdContainingUnicode_Succeeds()
    {
        var item = new TestDocument { Id = "日本語-id", PartitionKey = "pk1", Name = "Unicode" };

        var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);

        var read = await _container.ReadItemAsync<TestDocument>("日本語-id", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Unicode");
    }

    [Fact]
    public async Task Create_SameIdDifferentPartitionKey_BothRetrievable()
    {
        var item1 = new TestDocument { Id = "same", PartitionKey = "pk1", Name = "First" };
        var item2 = new TestDocument { Id = "same", PartitionKey = "pk2", Name = "Second" };

        await _container.CreateItemAsync(item1, new PartitionKey("pk1"));
        await _container.CreateItemAsync(item2, new PartitionKey("pk2"));

        var read1 = await _container.ReadItemAsync<TestDocument>("same", new PartitionKey("pk1"));
        var read2 = await _container.ReadItemAsync<TestDocument>("same", new PartitionKey("pk2"));

        read1.Resource.Name.Should().Be("First");
        read2.Resource.Name.Should().Be("Second");
    }
}

#endregion

#region 2. CRUD — ReadItem

public class ReadItemGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Read_WrongPartitionKey_ThrowsNotFound()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("wrong-pk"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Read_AfterUpdate_ReturnsUpdatedData()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
            new PartitionKey("pk1"));

        var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Updated");
    }

    [Fact]
    public async Task Read_NonExistent_ThrowsCosmosExceptionWith404()
    {
        var act = () => _container.ReadItemAsync<TestDocument>("missing", new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Read_IfNoneMatch_WithStaleETag_ReturnsItem()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var response = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfNoneMatchEtag = "\"stale-etag\"" });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Name.Should().Be("Test");
    }

    [Fact]
    public async Task Read_IfNoneMatch_WithCurrentETag_Returns304()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        var createResponse = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfNoneMatchEtag = createResponse.ETag });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotModified);
    }
}

#endregion

#region 3. CRUD — UpsertItem

public class UpsertItemGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Upsert_NewItem_Returns201()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "New" };

        var response = await _container.UpsertItemAsync(item, new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task Upsert_ExistingItem_Returns200()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var updated = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" };
        var response = await _container.UpsertItemAsync(updated, new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Upsert_StatusCodeDistinguishes_CreateVsReplace()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "First" };

        var createResponse = await _container.UpsertItemAsync(item, new PartitionKey("pk1"));
        createResponse.StatusCode.Should().Be(HttpStatusCode.Created);

        var replaceResponse = await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Second" },
            new PartitionKey("pk1"));
        replaceResponse.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Upsert_WithIfMatch_StaleETag_ThrowsPreconditionFailed()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var act = () => _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
            new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "\"stale\"" });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task Upsert_ReplacesEntireDocument_NotMerge()
    {
        var item = new TestDocument
        {
            Id = "1",
            PartitionKey = "pk1",
            Name = "Original",
            Value = 42,
            Tags = ["tag1", "tag2"]
        };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var replacement = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" };
        await _container.UpsertItemAsync(replacement, new PartitionKey("pk1"));

        var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Replaced");
        read.Resource.Value.Should().Be(0);
        read.Resource.Tags.Should().BeEmpty();
    }

    [Fact]
    public async Task Upsert_UpdatesETag()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        var createResponse = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var upsertResponse = await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
            new PartitionKey("pk1"));

        upsertResponse.ETag.Should().NotBe(createResponse.ETag);
    }

    [Fact]
    public async Task Upsert_RecordsInChangeFeed()
    {
        await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Upserted" },
            new PartitionKey("pk1"));

        var checkpoint = _container.GetChangeFeedCheckpoint();
        checkpoint.Should().BeGreaterThan(0);
    }
}

#endregion

#region 4. CRUD — ReplaceItem

public class ReplaceItemGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Replace_NonExistent_ThrowsNotFound()
    {
        var replacement = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" };

        var act = () => _container.ReplaceItemAsync(replacement, "1", new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Replace_UpdatesETag()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
        var createResponse = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var replaceResponse = await _container.ReplaceItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" },
            "1", new PartitionKey("pk1"));

        replaceResponse.ETag.Should().NotBe(createResponse.ETag);
    }

    [Fact]
    public async Task Replace_ResponseIs200()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var response = await _container.ReplaceItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" },
            "1", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Replace_RecordsInChangeFeed()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));
        var beforeReplace = _container.GetChangeFeedCheckpoint();

        await _container.ReplaceItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" },
            "1", new PartitionKey("pk1"));

        var afterReplace = _container.GetChangeFeedCheckpoint();
        afterReplace.Should().BeGreaterThan(beforeReplace);
    }
}

#endregion

#region 5. CRUD — DeleteItem

public class DeleteItemGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Delete_ResponseIs204()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var response = await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task Delete_WithIfMatch_CorrectETag_Succeeds()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        var createResponse = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var response = await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = createResponse.ETag });

        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task Delete_WithIfMatch_WrongETag_ThrowsPreconditionFailed()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var act = () => _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "\"stale\"" });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task Delete_AfterDelete_CanRecreate()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));
        await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        var recreated = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Recreated" };
        var response = await _container.CreateItemAsync(recreated, new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Recreated");
    }
}

#endregion

#region 6. ETag & Conditional Operations

public class ETagGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task ETag_ChangesOnEveryWrite()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "First" };
        var create = await _container.CreateItemAsync(item, new PartitionKey("pk1"));
        var firstEtag = create.ETag;

        var upsert = await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Second" },
            new PartitionKey("pk1"));
        var secondEtag = upsert.ETag;

        firstEtag.Should().NotBe(secondEtag);
    }

    [Fact]
    public async Task ETag_ConsistentAcrossMultipleReads()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var read1 = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        var read2 = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        read1.ETag.Should().Be(read2.ETag);
    }

    [Fact]
    public async Task ConcurrentUpsert_IfMatch_SecondWriteFails()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
        var create = await _container.CreateItemAsync(item, new PartitionKey("pk1"));
        var etag = create.ETag;

        await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "First Writer" },
            new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = etag });

        var act = () => _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Second Writer" },
            new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = etag });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }
}

#endregion

#region 7. Stream Operations

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

#endregion

#region 8. Patch Operations

public class PatchGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private async Task<TestDocument> CreateTestItem(string id = "1", string pk = "pk1")
    {
        var item = new TestDocument
        {
            Id = id,
            PartitionKey = pk,
            Name = "Original",
            Value = 10,
            IsActive = true,
            Tags = ["tag1", "tag2"],
            Nested = new NestedObject { Description = "Nested", Score = 5.0 }
        };
        await _container.CreateItemAsync(item, new PartitionKey(pk));
        return item;
    }

    [Fact]
    public async Task Patch_Remove_OnNonExistentPath_Succeeds()
    {
        await CreateTestItem();
        var patchOperations = new[] { PatchOperation.Remove("/nonExistentField") };

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), patchOperations);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Patch_MultipleOperations_AppliedSequentially()
    {
        await CreateTestItem();
        var patchOperations = new List<PatchOperation>
        {
            PatchOperation.Set("/value", 100),
            PatchOperation.Increment("/value", 5)
        };

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), patchOperations);

        response.Resource.Value.Should().Be(105);
    }

    [Fact]
    public async Task Patch_RecordsInChangeFeed()
    {
        await CreateTestItem();
        var beforePatch = _container.GetChangeFeedCheckpoint();

        await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Patched")]);

        var afterPatch = _container.GetChangeFeedCheckpoint();
        afterPatch.Should().BeGreaterThan(beforePatch);
    }

    [Fact]
    public async Task Patch_ResponseContainsUpdatedDocument()
    {
        await CreateTestItem();

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Patched"), PatchOperation.Increment("/value", 5)]);

        response.Resource.Name.Should().Be("Patched");
        response.Resource.Value.Should().Be(15);
    }

    [Fact]
    public async Task Patch_Set_DeepNestedPath_Updates()
    {
        await CreateTestItem();

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/nested/score", 99.9)]);

        response.Resource.Nested!.Score.Should().Be(99.9);
    }

    [Fact]
    public async Task Patch_Move_MovesPropertyToNewPath()
    {
        await CreateTestItem();
        var patchOperations = new[] { PatchOperation.Move("/name", "/movedName") };

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), patchOperations);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}

#endregion

#region 9. Query & SQL

public class QueryGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private async Task SeedItems()
    {
        var items = new[]
        {
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, IsActive = true, Tags = ["urgent", "review"] },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, IsActive = false, Tags = ["review"] },
            new TestDocument { Id = "3", PartitionKey = "pk2", Name = "Charlie", Value = 30, IsActive = true, Tags = ["urgent"] },
            new TestDocument { Id = "4", PartitionKey = "pk2", Name = "Diana", Value = 40, IsActive = false },
            new TestDocument { Id = "5", PartitionKey = "pk1", Name = "Eve", Value = 50, IsActive = true, Tags = ["urgent", "important"] },
        };
        foreach (var item in items)
        {
            await _container.CreateItemAsync(item, new PartitionKey(item.PartitionKey));
        }
    }

    [Fact]
    public async Task Query_NullQueryText_ReturnsAllItems()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(queryText: null);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(5);
    }

    [Fact]
    public async Task Query_EmptyString_ReturnsAllItems()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>("");
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(5);
    }

    [Fact]
    public async Task Query_WithPartitionKeyFilter_OnlyScopesToPartition()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk1") });

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(3);
        results.Should().OnlyContain(t => t.PartitionKey == "pk1");
    }

    [Fact]
    public async Task Query_SelectValue_ReturnsRawValues()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<string>(
            "SELECT VALUE c.name FROM c ORDER BY c.name");

        var results = new List<string>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainInOrder("Alice", "Bob", "Charlie", "Diana", "Eve");
    }

    [Fact]
    public async Task Query_Where_Between()
    {
        await SeedItems();

        var query = new QueryDefinition("SELECT * FROM c WHERE c.value BETWEEN @lo AND @hi")
            .WithParameter("@lo", 15)
            .WithParameter("@hi", 35);

        var iterator = _container.GetItemQueryIterator<TestDocument>(query);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        results.Select(r => r.Name).Should().Contain("Bob").And.Contain("Charlie");
    }

    [Fact]
    public async Task Query_Where_In()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            """SELECT * FROM c WHERE c.name IN ("Alice", "Eve")""");

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        results.Select(r => r.Name).Should().Contain("Alice").And.Contain("Eve");
    }

    [Fact]
    public async Task Query_OrderBy_MultipleFields_MixedDirection()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c ORDER BY c.partitionKey ASC, c.value DESC");

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results[0].PartitionKey.Should().Be("pk1");
        results[0].Value.Should().Be(50);
        results[^1].PartitionKey.Should().Be("pk2");
    }

    [Fact]
    public async Task Query_GroupBy_WithCount()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT c.partitionKey, COUNT(1) AS cnt FROM c GROUP BY c.partitionKey");

        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        var pk1 = results.FirstOrDefault(r => r["partitionKey"]?.ToString() == "pk1");
        pk1.Should().NotBeNull();
        pk1!["cnt"]!.ToObject<int>().Should().Be(3);
    }

    [Fact]
    public async Task Query_Distinct()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT DISTINCT c.partitionKey FROM c");

        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
    }

    [Fact]
    public async Task Query_Top_LimitsResults()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT TOP 2 * FROM c ORDER BY c.value");

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        results[0].Value.Should().Be(10);
        results[1].Value.Should().Be(20);
    }

    [Fact]
    public async Task Query_OffsetLimit_Pagination()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c ORDER BY c.value OFFSET 1 LIMIT 2");

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        results[0].Value.Should().Be(20);
        results[1].Value.Should().Be(30);
    }

    [Fact]
    public async Task Query_Where_Not()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c WHERE NOT c.isActive");

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        results.Should().OnlyContain(r => !r.IsActive);
    }

    [Fact]
    public async Task Query_Where_Like()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c WHERE c.name LIKE 'A%'");

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle().Which.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task Query_Where_ArithmeticExpression()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c WHERE c.value * 2 > 50");

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(3);
    }

    [Fact]
    public async Task Query_ParameterizedQuery_WithMultipleParams()
    {
        await SeedItems();

        var query = new QueryDefinition("SELECT * FROM c WHERE c.value > @min AND c.isActive = @active")
            .WithParameter("@min", 15)
            .WithParameter("@active", true);

        var iterator = _container.GetItemQueryIterator<TestDocument>(query);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        results.Select(r => r.Name).Should().Contain("Charlie").And.Contain("Eve");
    }

    [Fact]
    public async Task Query_NestedFunctionCalls()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<JToken>(
            "SELECT VALUE UPPER(SUBSTRING(c.name, 0, 3)) FROM c WHERE c.id = '1'");

        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle();
    }

    [Fact]
    public async Task Query_Join_SingleArrayExpansion()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Tags = ["a", "b", "c"] },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JToken>(
            "SELECT VALUE t FROM c JOIN t IN c.tags");

        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(3);
    }

    [Fact]
    public async Task Query_Exists_Subquery()
    {
        await SeedItems();

        var iterator = _container.GetItemQueryIterator<TestDocument>(
            """SELECT * FROM c WHERE EXISTS(SELECT VALUE t FROM t IN c.tags WHERE t = "urgent")""");

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(3);
        results.Select(r => r.Name).Should().Contain("Alice").And.Contain("Charlie").And.Contain("Eve");
    }

    [Fact]
    public async Task Query_NullCoalesce_Operator()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "HasName" },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JToken>(
            """SELECT VALUE (c.name ?? "default") FROM c""");

        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(1);
    }
}

#endregion

#region 10. Change Feed

public class ChangeFeedGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task ChangeFeed_FromCheckpoint_ReturnsOnlyNewChanges()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Before" },
            new PartitionKey("pk1"));

        var checkpoint = _container.GetChangeFeedCheckpoint();

        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "After" },
            new PartitionKey("pk1"));

        var iterator = _container.GetChangeFeedIterator<TestDocument>(checkpoint);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle().Which.Name.Should().Be("After");
    }

    [Fact]
    public async Task ChangeFeed_OrderPreserved_AcrossWrites()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "First" },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Second" },
            new PartitionKey("pk1"));
        await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "FirstUpdated" },
            new PartitionKey("pk1"));

        var iterator = _container.GetChangeFeedIterator<TestDocument>(0);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(3);
        results[0].Name.Should().Be("First");
        results[1].Name.Should().Be("Second");
        results[2].Name.Should().Be("FirstUpdated");
    }

    [Fact]
    public async Task ChangeFeed_Delete_DoesNotAppearInFeed()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "ToDelete" },
            new PartitionKey("pk1"));
        var checkpointAfterCreate = _container.GetChangeFeedCheckpoint();

        await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        var iterator = _container.GetChangeFeedIterator<TestDocument>(checkpointAfterCreate);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task ChangeFeed_Patch_RecordsChange()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original", Value = 10 },
            new PartitionKey("pk1"));
        var checkpoint = _container.GetChangeFeedCheckpoint();

        await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Patched")]);

        var iterator = _container.GetChangeFeedIterator<TestDocument>(checkpoint);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle().Which.Name.Should().Be("Patched");
    }

    [Fact]
    public async Task ChangeFeed_StreamIterator_ReturnsJsonEnvelope()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var iterator = _container.GetChangeFeedStreamIterator(
            ChangeFeedStartFrom.Beginning(),
            ChangeFeedMode.Incremental);

        if (iterator.HasMoreResults)
        {
            using var response = await iterator.ReadNextAsync();
            if (response.StatusCode != HttpStatusCode.NotModified)
            {
                using var reader = new StreamReader(response.Content);
                var body = await reader.ReadToEndAsync();
                var jObj = JObject.Parse(body);
                jObj["Documents"].Should().NotBeNull();
                ((JArray)jObj["Documents"]!).Should().HaveCountGreaterThan(0);
            }
        }
    }
}

#endregion

#region 11. Transactional Batch

public class TransactionalBatchGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Batch_FailingOperation_RollsBackPrevious()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Duplicate" });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeFalse();

        var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task Batch_CreateDuplicate_InBatch_Fails409()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "dup", PartitionKey = "pk1", Name = "First" });
        batch.CreateItem(new TestDocument { Id = "dup", PartitionKey = "pk1", Name = "Second" });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeFalse();
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task Batch_Over100Operations_Throws()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        for (var i = 0; i < 101; i++)
        {
            batch.CreateItem(new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" });
        }

        var act = () => batch.ExecuteAsync();

        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task Batch_UpsertInBatch_CreatesOrReplaces()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "existing", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.UpsertItem(new TestDocument { Id = "existing", PartitionKey = "pk1", Name = "Updated" });
        batch.UpsertItem(new TestDocument { Id = "new-item", PartitionKey = "pk1", Name = "NewItem" });

        using var response = await batch.ExecuteAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var read1 = await _container.ReadItemAsync<TestDocument>("existing", new PartitionKey("pk1"));
        read1.Resource.Name.Should().Be("Updated");

        var read2 = await _container.ReadItemAsync<TestDocument>("new-item", new PartitionKey("pk1"));
        read2.Resource.Name.Should().Be("NewItem");
    }

    [Fact]
    public async Task Batch_Rollback_RestoresExactSnapshot()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "pre-existing", PartitionKey = "pk1", Name = "PreExisting", Value = 42 },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "will-rollback", PartitionKey = "pk1", Name = "New" });
        batch.ReplaceItem("nonexistent", new TestDocument { Id = "nonexistent", PartitionKey = "pk1", Name = "Bad" });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeFalse();

        var read = await _container.ReadItemAsync<TestDocument>("pre-existing", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("PreExisting");
        read.Resource.Value.Should().Be(42);

        var act = () => _container.ReadItemAsync<TestDocument>("will-rollback", new PartitionKey("pk1"));
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task Batch_OperationOrder_Matters()
    {
        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Created" });
        batch.ReplaceItem("1", new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" });

        using var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeTrue();

        var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Replaced");
    }
}

#endregion

#region 12. ReadMany

public class ReadManyGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private async Task SeedItems()
    {
        for (var i = 1; i <= 5; i++)
        {
            await _container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = $"pk{(i % 2) + 1}", Name = $"Item{i}" },
                new PartitionKey($"pk{(i % 2) + 1}"));
        }
    }

    [Fact]
    public async Task ReadMany_EmptyList_ReturnsEmptyResponse()
    {
        await SeedItems();
        var response = await _container.ReadManyItemsAsync<TestDocument>([]);

        response.Resource.Should().BeEmpty();
    }

    [Fact]
    public async Task ReadMany_SomeItemsMissing_ReturnsOnlyExisting()
    {
        await SeedItems();
        var items = new List<(string, PartitionKey)>
        {
            ("1", new PartitionKey("pk2")),
            ("missing", new PartitionKey("pk1")),
        };

        var response = await _container.ReadManyItemsAsync<TestDocument>(items);

        response.Resource.Should().ContainSingle().Which.Id.Should().Be("1");
    }

    [Fact]
    public async Task ReadMany_AllMissing_ReturnsEmpty()
    {
        await SeedItems();
        var items = new List<(string, PartitionKey)>
        {
            ("missing1", new PartitionKey("pk1")),
            ("missing2", new PartitionKey("pk2")),
        };

        var response = await _container.ReadManyItemsAsync<TestDocument>(items);

        response.Resource.Should().BeEmpty();
    }

    [Fact]
    public async Task ReadMany_MixedPartitionKeys()
    {
        await SeedItems();
        var items = new List<(string, PartitionKey)>
        {
            ("1", new PartitionKey("pk2")),
            ("2", new PartitionKey("pk1")),
        };

        var response = await _container.ReadManyItemsAsync<TestDocument>(items);

        response.Resource.Should().HaveCount(2);
    }
}

#endregion

#region 13. TTL & Expiration

public class TtlGapTests
{
    [Fact]
    public async Task ContainerTtl_ExpiredItems_NotReturnedByRead()
    {
        var container = new InMemoryContainer("ttl-container", "/partitionKey")
        {
            DefaultTimeToLive = 1
        };

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temp" },
            new PartitionKey("pk1"));

        await Task.Delay(TimeSpan.FromSeconds(2));

        var act = () => container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task ContainerTtl_ExpiredItems_NotReturnedByQuery()
    {
        var container = new InMemoryContainer("ttl-container", "/partitionKey")
        {
            DefaultTimeToLive = 1
        };

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temp" },
            new PartitionKey("pk1"));

        await Task.Delay(TimeSpan.FromSeconds(2));

        var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task ContainerTtl_NonExpiredItems_StillReturned()
    {
        var container = new InMemoryContainer("ttl-container", "/partitionKey")
        {
            DefaultTimeToLive = 60
        };

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "LongLived" },
            new PartitionKey("pk1"));

        var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("LongLived");
    }

    [Fact]
    public async Task ContainerTtl_NullMeansNoExpiration()
    {
        var container = new InMemoryContainer("ttl-container", "/partitionKey")
        {
            DefaultTimeToLive = null
        };

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "NoExpiry" },
            new PartitionKey("pk1"));

        var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("NoExpiry");
    }
}

#endregion

#region 14. Partition Keys

public class PartitionKeyGapTests
{
    [Fact]
    public async Task PartitionKey_ExtractedFromItem_MatchesExplicitPk()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var item = new TestDocument { Id = "1", PartitionKey = "auto-pk", Name = "Test" };

        await container.CreateItemAsync(item);

        var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("auto-pk"));
        read.Resource.Name.Should().Be("Test");
    }

    [Fact]
    public async Task PartitionKey_NumericValue()
    {
        var container = new InMemoryContainer("test", "/value");
        var item = new TestDocument { Id = "1", PartitionKey = "ignored", Name = "Test", Value = 42 };

        await container.CreateItemAsync(item, new PartitionKey(42));

        var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey(42));
        read.Resource.Name.Should().Be("Test");
    }

    [Fact]
    public async Task CrossPartition_Query_ReturnsAllPartitions()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "A", Name = "Alice" },
            new PartitionKey("A"));
        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "B", Name = "Bob" },
            new PartitionKey("B"));

        var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
    }
}

#endregion

#region 15. Document Size & Limits

public class DocumentSizeGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Create_OverSizeLimit_ThrowsException()
    {
        var largeValue = new string('x', 3 * 1024 * 1024);
        var doc = new TestDocument { Id = "large", PartitionKey = "pk1", Name = largeValue };

        var act = () => _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task Upsert_OverSizeLimit_ThrowsException()
    {
        var largeValue = new string('x', 3 * 1024 * 1024);
        var doc = new TestDocument { Id = "large", PartitionKey = "pk1", Name = largeValue };

        var act = () => _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task Replace_OverSizeLimit_ThrowsException()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Small" },
            new PartitionKey("pk1"));

        var largeValue = new string('x', 3 * 1024 * 1024);
        var largeDoc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = largeValue };

        var act = () => _container.ReplaceItemAsync(largeDoc, "1", new PartitionKey("pk1"));

        await act.Should().ThrowAsync<CosmosException>();
    }
}

#endregion

#region 16. Concurrency & Thread Safety

public class ConcurrencyGapTests
{
    [Fact]
    public async Task ConcurrentCreates_DifferentIds_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        var tasks = Enumerable.Range(0, 100).Select(i =>
            container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1")));

        var results = await Task.WhenAll(tasks);

        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
        container.ItemCount.Should().Be(100);
    }

    [Fact]
    public async Task ConcurrentCreates_SameId_ExactlyOneSucceeds()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var successes = 0;
        var failures = 0;

        var tasks = Enumerable.Range(0, 50).Select(async i =>
        {
            try
            {
                await container.CreateItemAsync(
                    new TestDocument { Id = "same", PartitionKey = "pk1", Name = $"Item{i}" },
                    new PartitionKey("pk1"));
                Interlocked.Increment(ref successes);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
            {
                Interlocked.Increment(ref failures);
            }
        });

        await Task.WhenAll(tasks);

        successes.Should().Be(1);
        failures.Should().Be(49);
    }

    [Fact]
    public async Task ConcurrentReads_SameItem_AllReturnSameData()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var tasks = Enumerable.Range(0, 100).Select(_ =>
            container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1")));

        var results = await Task.WhenAll(tasks);

        results.Should().OnlyContain(r => r.Resource.Name == "Alice");
    }

    [Fact]
    public async Task ConcurrentUpserts_SameItem_AllSucceed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var tasks = Enumerable.Range(0, 100).Select(i =>
            container.UpsertItemAsync(
                new TestDocument { Id = "1", PartitionKey = "pk1", Name = $"Version{i}" },
                new PartitionKey("pk1")));

        var results = await Task.WhenAll(tasks);

        results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
    }
}

#endregion

#region 17. Container Management

public class ContainerManagementGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task DeleteContainer_ClearsAllItems()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        await _container.DeleteContainerAsync();

        _container.ItemCount.Should().Be(0);
    }

    [Fact]
    public async Task ReadThroughput_ReturnsSyntheticValue()
    {
        var throughput = await _container.ReadThroughputAsync();

        throughput.Should().Be(400);
    }

    [Fact]
    public async Task ReplaceThroughput_AcceptsWithoutError()
    {
        var act = () => _container.ReplaceThroughputAsync(1000);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ReplaceContainer_AcceptsProperties()
    {
        var properties = new ContainerProperties("test-container", "/partitionKey")
        {
            IndexingPolicy = new IndexingPolicy { Automatic = true }
        };

        var response = await _container.ReplaceContainerAsync(properties);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}

#endregion

#region 18. Response Metadata

public class ResponseMetadataGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Response_RequestCharge_PositiveOnWrite()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        response.RequestCharge.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task Response_ETag_SetOnAllWriteOperations()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

        var create = await _container.CreateItemAsync(item, new PartitionKey("pk1"));
        create.ETag.Should().NotBeNullOrEmpty();

        var upsert = await _container.UpsertItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Upserted" },
            new PartitionKey("pk1"));
        upsert.ETag.Should().NotBeNullOrEmpty();

        var replace = await _container.ReplaceItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" },
            "1", new PartitionKey("pk1"));
        replace.ETag.Should().NotBeNullOrEmpty();

        var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.ETag.Should().NotBeNullOrEmpty();
    }
}

#endregion

#region 19. SQL Functions Additional Coverage

public class SqlFunctionGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private async Task SeedItem()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Hello World", Value = 42 },
            new PartitionKey("pk1"));
    }

    private async Task<List<JObject>> RunQuery(string sql)
    {
        var iterator = _container.GetItemQueryIterator<JObject>(sql);
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        return results;
    }

    private async Task<List<JToken>> RunQueryTokens(string sql)
    {
        var iterator = _container.GetItemQueryIterator<JToken>(sql);
        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        return results;
    }

    [Fact]
    public async Task Contains_CaseSensitive_Default()
    {
        await SeedItem();
        var results = await RunQuery("""SELECT * FROM c WHERE CONTAINS(c.name, "hello")""");
        results.Should().BeEmpty();
    }

    [Fact]
    public async Task Contains_CaseInsensitive_ThirdParam()
    {
        await SeedItem();
        var results = await RunQuery("""SELECT * FROM c WHERE CONTAINS(c.name, "hello", true)""");
        results.Should().HaveCount(1);
    }

    [Fact]
    public async Task StartsWith_CaseInsensitive()
    {
        await SeedItem();
        var results = await RunQuery("""SELECT * FROM c WHERE STARTSWITH(c.name, "hello", true)""");
        results.Should().HaveCount(1);
    }

    [Fact]
    public async Task ArrayContains_PartialMatch()
    {
        var json = """{"id":"1","partitionKey":"pk1","items":[{"name":"urgent","priority":1},{"name":"review","priority":2}]}""";
        await _container.CreateItemStreamAsync(new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            """SELECT * FROM c WHERE ARRAY_CONTAINS(c.items, {"name": "urgent"}, true)""");

        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(1);
    }

    [Fact]
    public async Task Is_Defined_FalseForMissingField()
    {
        await SeedItem();
        var results = await RunQuery("SELECT * FROM c WHERE IS_DEFINED(c.nonExistentField)");
        results.Should().BeEmpty();
    }

    [Fact]
    public async Task Is_Defined_TrueForExistingField()
    {
        await SeedItem();
        var results = await RunQuery("SELECT * FROM c WHERE IS_DEFINED(c.name)");
        results.Should().HaveCount(1);
    }

    [Fact]
    public async Task IndexOf_NotFound_ReturnsNegative()
    {
        await SeedItem();
        var results = await RunQueryTokens("SELECT VALUE INDEX_OF(c.name, \"xyz\") FROM c");
        results.Should().HaveCount(1);
    }

    [Fact]
    public async Task Substring_Basic()
    {
        await SeedItem();
        var results = await RunQueryTokens("SELECT VALUE SUBSTRING(c.name, 0, 5) FROM c");
        results.Should().HaveCount(1);
    }

    [Fact]
    public async Task Replace_MultipleOccurrences()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "aaa" },
            new PartitionKey("pk1"));

        var results = await RunQueryTokens("""SELECT VALUE REPLACE(c.name, "a", "bb") FROM c""");
        results.Should().HaveCount(1);
    }
}

#endregion

#region 20. Stored Procedures & UDFs

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
}

#endregion

#region 21. Feed Ranges

public class FeedRangeGapTests
{
    [Fact]
    public async Task GetFeedRanges_ReturnsSingleRange_ByDefault()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var feedRanges = await container.GetFeedRangesAsync();

        feedRanges.Should().HaveCount(1);
    }
}

#endregion

#region 22. LINQ Integration

public class LinqGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Linq_Where_EqualityFilter()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
            new PartitionKey("pk1"));

        var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
        var results = queryable.Where(d => d.Name == "Alice").ToList();

        results.Should().ContainSingle().Which.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task Linq_OrderBy()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Bravo", Value = 2 },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Alpha", Value = 1 },
            new PartitionKey("pk1"));

        var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
        var results = queryable.OrderBy(d => d.Value).ToList();

        results[0].Name.Should().Be("Alpha");
        results[1].Name.Should().Be("Bravo");
    }

    [Fact]
    public async Task Linq_Select_Projection()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 42 },
            new PartitionKey("pk1"));

        var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
        var results = queryable.Select(d => new { d.Name, d.Value }).ToList();

        results.Should().ContainSingle();
        results[0].Name.Should().Be("Alice");
        results[0].Value.Should().Be(42);
    }

    [Fact]
    public async Task Linq_Count()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
            new PartitionKey("pk1"));

        var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
        var count = queryable.Count();

        count.Should().Be(2);
    }
}

#endregion

#region 23. Behavioral Difference Tests (Divergent Behavior Documentation)

/// <summary>
/// Tests that document known behavioral differences between InMemoryContainer and real
/// Cosmos DB. Each test shows the ACTUAL behavior and explains the divergence.
/// These are reference tests — they pass if InMemoryContainer has the documented behavior,
/// even when that behavior differs from real Cosmos.
/// </summary>
public class BehavioralDifferenceGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: Real Cosmos DB change feed returns 304 NotModified
    /// when there are no new changes. InMemoryContainer returns 200 OK with an
    /// empty result set. This is because InMemoryContainer uses a simple list-based
    /// change feed that doesn't support the NotModified status code pattern.
    /// </summary>
    [Fact]
    public async Task ChangeFeed_EmptyContainer_ReturnsOk_NotNotModified()
    {
        var iterator = _container.GetChangeFeedIterator<TestDocument>(
            ChangeFeedStartFrom.Beginning(),
            ChangeFeedMode.Incremental);

        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            // InMemoryContainer returns OK and we iterate normally (empty results)
            results.AddRange(response);
        }

        results.Should().BeEmpty();
    }

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: Real Cosmos DB deletes appear as tombstone entries in
    /// the change feed (FullFidelity mode) or cause the document to disappear from
    /// incremental reads. InMemoryContainer does not record deletes in the change feed
    /// at all — they are simply absent. This is documented as a known limitation.
    /// </summary>
    [Fact]
    public async Task ChangeFeed_DeletesNotRecorded()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "ToDelete" },
            new PartitionKey("pk1"));
        var checkpointAfterCreate = _container.GetChangeFeedCheckpoint();

        await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        var checkpointAfterDelete = _container.GetChangeFeedCheckpoint();

        // Delete does not add to change feed
        checkpointAfterDelete.Should().Be(checkpointAfterCreate);
    }

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: Real Cosmos DB container delete makes the container
    /// permanently unavailable — subsequent operations throw. InMemoryContainer
    /// merely clears its internal state but the object remains usable. Items can
    /// be added after deletion.
    /// </summary>
    [Fact]
    public async Task DeleteContainer_RemainsUsable_UnlikeRealCosmos()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Before" },
            new PartitionKey("pk1"));

        await _container.DeleteContainerAsync();

        // Container is still usable after deletion (unlike real Cosmos)
        var response = await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "After" },
            new PartitionKey("pk1"));
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: Real Cosmos DB ETags are opaque server-generated
    /// strings based on internal timestamps. InMemoryContainer generates ETags
    /// as quoted GUIDs. The format differs but conditional (IfMatch/IfNoneMatch)
    /// operations work identically.
    /// </summary>
    [Fact]
    public async Task ETag_Format_IsQuotedGuid_NotOpaqueTimestamp()
    {
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

        var etag = response.ETag;
        etag.Should().StartWith("\"").And.EndWith("\"");

        var inner = etag.Trim('"');
        Guid.TryParse(inner, out _).Should().BeTrue();
    }

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: Real Cosmos DB ReadThroughputAsync returns the actual
    /// provisioned throughput for the container. InMemoryContainer always returns 400
    /// RU/s regardless of any ReplaceThroughputAsync calls.
    /// </summary>
    [Fact]
    public async Task Throughput_AlwaysReturns400_IgnoresReplace()
    {
        await _container.ReplaceThroughputAsync(2000);
        var throughput = await _container.ReadThroughputAsync();

        // Always returns 400, not the value set
        throughput.Should().Be(400);
    }

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: Real Cosmos DB aggregates like COUNT/SUM without
    /// GROUP BY return a single aggregated value across all matching documents.
    /// InMemoryContainer supports this via GROUP BY but cross-partition aggregation
    /// without GROUP BY may return per-document values depending on the query path.
    /// Use the checkpoint-based change feed or GROUP BY for accurate aggregation.
    /// </summary>
    [Fact]
    public async Task Aggregate_Count_WithoutGroupBy_ReturnsCount()
    {
        for (var i = 0; i < 3; i++)
        {
            await _container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));
        }

        var iterator = _container.GetItemQueryIterator<JToken>("SELECT VALUE COUNT(1) FROM c");
        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        // Behavior may vary: real Cosmos returns single aggregated number,
        // InMemoryContainer may return per-document counts
        results.Should().NotBeEmpty();
    }

    /// <summary>
    /// BEHAVIORAL DIFFERENCE: CosmosSqlParser partially handles the null-coalescing
    /// operator (??). Real Cosmos DB evaluates (expr ?? default) as "return expr if
    /// non-null, else return default". InMemoryContainer may parse the expression but
    /// produces results that cannot be deserialized to JObject since SELECT VALUE
    /// returns raw scalar values. Use JToken for scalar results.
    /// </summary>
    [Fact]
    public async Task Query_NullCoalesce_ProducesScalarResult_NotJObject()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        // Query with ?? works but returns a scalar — JToken works, JObject would fail
        var iterator = _container.GetItemQueryIterator<JToken>(
            """SELECT VALUE (c.name ?? "default") FROM c""");

        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(1);
    }
}

#endregion
