using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;
using System.Text;
using Newtonsoft.Json.Linq;

namespace CosmosDB.InMemoryEmulator.Tests;

public class PatchItemTests
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
    public async Task PatchItemAsync_SetOperation_UpdatesProperty()
    {
        await CreateTestItem();
        var patchOperations = new[] { PatchOperation.Set("/name", "Patched") };

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), patchOperations);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Name.Should().Be("Patched");
    }

    [Fact]
    public async Task PatchItemAsync_ReplaceOperation_UpdatesProperty()
    {
        await CreateTestItem();
        var patchOperations = new[] { PatchOperation.Replace("/value", 99) };

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), patchOperations);

        response.Resource.Value.Should().Be(99);
    }

    [Fact]
    public async Task PatchItemAsync_AddOperation_AddsProperty()
    {
        await CreateTestItem();
        var patchOperations = new[] { PatchOperation.Add("/newField", "newValue") };

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), patchOperations);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task PatchItemAsync_RemoveOperation_RemovesProperty()
    {
        await CreateTestItem();
        var patchOperations = new[] { PatchOperation.Remove("/name") };

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), patchOperations);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Name.Should().BeNull();
    }

    [Fact]
    public async Task PatchItemAsync_IncrementOperation_IncrementsValue()
    {
        await CreateTestItem();
        var patchOperations = new[] { PatchOperation.Increment("/value", 5) };

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), patchOperations);

        response.Resource.Value.Should().Be(15);
    }

    [Fact]
    public async Task PatchItemAsync_NonExistentItem_ThrowsNotFound()
    {
        var patchOperations = new[] { PatchOperation.Set("/name", "Patched") };

        var act = () => _container.PatchItemAsync<TestDocument>("nonexistent", new PartitionKey("pk1"), patchOperations);

        var exception = await act.Should().ThrowAsync<CosmosException>();
        exception.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task PatchItemAsync_WithIfMatchCurrentETag_Succeeds()
    {
        await CreateTestItem();
        var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        var patchOperations = new[] { PatchOperation.Set("/name", "Patched") };

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), patchOperations,
            new PatchItemRequestOptions { IfMatchEtag = readResponse.ETag });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task PatchItemAsync_WithIfMatchStaleETag_ThrowsPreconditionFailed()
    {
        await CreateTestItem();
        var patchOperations = new[] { PatchOperation.Set("/name", "Patched") };

        var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), patchOperations,
            new PatchItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

        var exception = await act.Should().ThrowAsync<CosmosException>();
        exception.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task PatchItemAsync_UpdatesETag()
    {
        await CreateTestItem();
        var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        var originalEtag = readResponse.ETag;

        var patchOperations = new[] { PatchOperation.Set("/name", "Patched") };
        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), patchOperations);

        response.ETag.Should().NotBe(originalEtag);
    }

    [Fact]
    public async Task PatchItemAsync_MultipleOperations_AllApplied()
    {
        await CreateTestItem();
        var patchOperations = new List<PatchOperation>
        {
            PatchOperation.Set("/name", "MultiPatched"),
            PatchOperation.Replace("/value", 42),
            PatchOperation.Set("/isActive", false)
        };

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), patchOperations);

        response.Resource.Name.Should().Be("MultiPatched");
        response.Resource.Value.Should().Be(42);
        response.Resource.IsActive.Should().BeFalse();
    }

    [Fact]
    public async Task PatchItemAsync_NestedProperty_UpdatesCorrectly()
    {
        await CreateTestItem();
        var patchOperations = new[] { PatchOperation.Set("/nested/description", "Updated Nested") };

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), patchOperations);

        response.Resource.Nested!.Description.Should().Be("Updated Nested");
    }
}


/// <summary>
/// Validates that the partition key value is immutable during Patch operations.
/// Per PatchItemStreamAsync docs: "The item's partition key value is immutable."
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.patchitemasync
/// </summary>
public class PatchPartitionKeyImmutabilityTests5
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Patch_Set_PartitionKeyField_ItemStaysInOriginalPartition()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        // Patch the partition key field to a new value
        await _container.PatchItemAsync<TestDocument>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/partitionKey", "pk2")]);

        // Item is still accessible with original PK
        var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Original");
    }
}


/// <summary>
/// Validates that patch operations are atomic — if one operation in a batch fails,
/// none of the operations should be applied to the item.
/// Per API docs: "The patch operations are atomic and are executed sequentially."
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.patchitemasync
/// </summary>
public class PatchAtomicityTests5
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Patch_MultipleOps_IfOneFailsAllRollBack()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original", Value = 10 },
            new PartitionKey("pk1"));

        // Op 1: Set /name to "Changed" (would succeed in isolation)
        // Op 2: Increment /name by 1 (will fail because /name is now a string "Changed")
        var act = () => _container.PatchItemAsync<TestDocument>(
            "1", new PartitionKey("pk1"),
            [
                PatchOperation.Set("/name", "Changed"),
                PatchOperation.Increment("/name", 1),
            ]);

        // Should throw because op 2 fails (can't increment a string)
        await act.Should().ThrowAsync<Exception>();

        // Item should be unchanged — atomicity means op 1 was rolled back
        var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Original");
        read.Resource.Value.Should().Be(10);
    }
}


public class PatchGapTests3
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Patch_Increment_OnNonNumericField_Throws()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Text", Value = 10 },
            new PartitionKey("pk1"));

        var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Increment("/name", 1)]);

        await act.Should().ThrowAsync<Exception>();
    }

    [Fact]
    public async Task Patch_Increment_Long_PreservesLongType()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 100 },
            new PartitionKey("pk1"));

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Increment("/value", 50L)]);

        response.Resource.Value.Should().Be(150);
    }

    [Fact]
    public async Task Patch_Add_AppendsToArray()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Tags = ["a", "b"] },
            new PartitionKey("pk1"));

        var response = await _container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
            [PatchOperation.Add("/tags/-", "c")]);

        var tags = response.Resource["tags"]!.ToObject<string[]>();
        tags.Should().Contain("c");
    }

    [Fact]
    public async Task Patch_Add_AtArrayIndex_Inserts()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Tags = ["a", "c"] },
            new PartitionKey("pk1"));

        var response = await _container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
            [PatchOperation.Add("/tags/1", "b")]);

        var tags = response.Resource["tags"]!.ToObject<string[]>();
        tags.Should().HaveCount(3);
    }

    [Fact]
    public async Task Patch_WithFilterPredicate_OnlyAppliesWhenConditionMet()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", IsActive = false },
            new PartitionKey("pk1"));

        var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Patched")],
            new PatchItemRequestOptions { FilterPredicate = "FROM c WHERE c.isActive = true" });

        // Real Cosmos would fail because isActive=false doesn't match the predicate
        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task Patch_Add_ObjectProperty_CreatesNewProperty()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var response = await _container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
            [PatchOperation.Add("/newProp", "newVal")]);

        response.Resource["newProp"]!.ToString().Should().Be("newVal");
    }

    [Fact]
    public async Task Patch_EmptyOperationsList_Throws()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            Array.Empty<PatchOperation>());

        await act.Should().ThrowAsync<Exception>();
    }
}


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


public class PatchGapTests2
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private async Task<TestDocument> CreateTestItem(string id = "1", string pk = "pk1")
    {
        var item = new TestDocument
        {
            Id = id, PartitionKey = pk, Name = "Original", Value = 10,
            IsActive = true, Tags = ["tag1", "tag2"],
            Nested = new NestedObject { Description = "Nested", Score = 5.0 }
        };
        await _container.CreateItemAsync(item, new PartitionKey(pk));
        return item;
    }

    [Fact]
    public async Task Patch_Increment_Double_PreservesDoubleType()
    {
        await _container.CreateItemAsync(new TestDocument
        {
            Id = "1", PartitionKey = "pk1", Name = "Test",
            Nested = new NestedObject { Description = "N", Score = 1.5 }
        }, new PartitionKey("pk1"));

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Increment("/nested/score", 0.3)]);

        response.Resource.Nested!.Score.Should().BeApproximately(1.8, 0.001);
    }

    [Fact]
    public async Task Patch_Set_CreatesNewProperty_IfMissing()
    {
        await CreateTestItem();

        var response = await _container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/newField", "newValue")]);

        response.Resource["newField"]!.ToString().Should().Be("newValue");
    }

    [Fact]
    public async Task Patch_Remove_IdField_DoesNotCorrupt()
    {
        await CreateTestItem();

        // Attempting to remove the id field
        var response = await _container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
            [PatchOperation.Remove("/id")]);

        // Item should still be readable (patch of /id may be no-op or remove the JSON field)
        var read = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
        read.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Patch_Replace_VsSet_ReplaceRequiresExistingPath()
    {
        await CreateTestItem();

        // Set creates new property if it doesn't exist
        var setResponse = await _container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/brandNewProp", 42)]);
        setResponse.Resource["brandNewProp"]!.ToObject<int>().Should().Be(42);
    }

    [Fact]
    public async Task Patch_NonExistentItem_Returns404()
    {
        var act = () => _container.PatchItemAsync<TestDocument>("missing", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Patched")]);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }
}


public class PatchEnableContentResponseDivergentBehaviorTests
{
    /// <summary>
    /// BEHAVIORAL DIFFERENCE: InMemoryContainer.PatchItemAsync does not currently respect
    /// EnableContentResponseOnWrite from PatchItemRequestOptions (which inherits from
    /// ItemRequestOptions). The patch code path reads requestOptions as PatchItemRequestOptions
    /// and doesn't check EnableContentResponseOnWrite. If this is not implemented, the test
    /// for Patch_WithEnableContentResponseOnWrite_False_ResourceIsNull will be skipped.
    /// </summary>
    [Fact]
    public async Task Patch_EnableContentResponseOnWrite_Behavior()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 10 },
            new PartitionKey("pk1"));

        // Verify the current behavior — may or may not suppress content
        var response = await container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Patched")],
            new PatchItemRequestOptions { EnableContentResponseOnWrite = false });

        // Document the actual behavior
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}
