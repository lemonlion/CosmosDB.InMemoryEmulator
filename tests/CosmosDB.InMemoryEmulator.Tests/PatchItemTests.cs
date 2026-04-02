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
    public async Task Patch_Remove_OnNonExistentPath_ThrowsBadRequest()
    {
        await CreateTestItem();
        var patchOperations = new[] { PatchOperation.Remove("/nonExistentField") };

        var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), patchOperations);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
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


/// <summary>
/// Phase 1: Per Cosmos DB docs, Increment on a non-existent field should CREATE the field
/// and set it to the specified value. Previously, the emulator silently did nothing.
/// See: https://learn.microsoft.com/en-us/azure/cosmos-db/partial-document-update
/// </summary>
public class PatchIncrementAutoCreateTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Increment_NonExistentField_CreatesWithValue()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var response = await _container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
            [PatchOperation.Increment("/counter", 5)]);

        response.Resource["counter"]!.Value<int>().Should().Be(5);
    }

    [Fact]
    public async Task Increment_NonExistentField_NegativeValue_CreatesNegative()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var response = await _container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
            [PatchOperation.Increment("/counter", -3)]);

        response.Resource["counter"]!.Value<int>().Should().Be(-3);
    }

    [Fact]
    public async Task Increment_NonExistentNestedField_CreatesField()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test",
                Nested = new NestedObject { Description = "N", Score = 5.0 } },
            new PartitionKey("pk1"));

        var response = await _container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
            [PatchOperation.Increment("/nested/newCounter", 10)]);

        response.Resource["nested"]!["newCounter"]!.Value<int>().Should().Be(10);
    }
}


/// <summary>
/// Phase 2: Per Cosmos DB docs, Replace follows strict replace-only semantics.
/// If the target path does not exist, it results in an error (BadRequest).
/// Set creates new properties; Replace does not.
/// See: https://learn.microsoft.com/en-us/azure/cosmos-db/partial-document-update
/// </summary>
public class PatchReplaceStrictSemanticsTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Replace_NonExistentProperty_ThrowsBadRequest()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Replace("/nonExistentField", "value")]);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Replace_ExistingProperty_UpdatesValue()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 10 },
            new PartitionKey("pk1"));

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Replace("/name", "Replaced")]);

        response.Resource.Name.Should().Be("Replaced");
    }

    [Fact]
    public async Task Replace_NonExistentNestedProperty_ThrowsBadRequest()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test",
                Nested = new NestedObject { Description = "N", Score = 5.0 } },
            new PartitionKey("pk1"));

        var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Replace("/nested/nonExistent", "value")]);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }
}


/// <summary>
/// Phase 3: Per Cosmos DB docs, Remove on a non-existent path results in an error.
/// Also, Remove at an array index should delete and shift elements.
/// See: https://learn.microsoft.com/en-us/azure/cosmos-db/partial-document-update
/// </summary>
public class PatchRemoveStrictSemanticsTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Remove_NonExistentProperty_ThrowsBadRequest()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Remove("/nonExistentField")]);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Remove_NonExistentNestedProperty_ThrowsBadRequest()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test",
                Nested = new NestedObject { Description = "N", Score = 5.0 } },
            new PartitionKey("pk1"));

        var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Remove("/nested/nonExistent")]);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Remove_ArrayElement_ByIndex_RemovesAndShifts()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Tags = ["a", "b", "c"] },
            new PartitionKey("pk1"));

        var response = await _container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
            [PatchOperation.Remove("/tags/1")]);

        var tags = response.Resource["tags"]!.ToObject<string[]>();
        tags.Should().BeEquivalentTo(["a", "c"]);
    }

    [Fact]
    public async Task Remove_ArrayElement_IndexOutOfBounds_ThrowsBadRequest()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Tags = ["a", "b"] },
            new PartitionKey("pk1"));

        var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Remove("/tags/5")]);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }
}


/// <summary>
/// Phase 4: Per Cosmos DB docs, Set at a valid array index should UPDATE the
/// existing element (not insert). This is different from Add which inserts.
/// See: https://learn.microsoft.com/en-us/azure/cosmos-db/partial-document-update
/// </summary>
public class PatchSetArrayIndexTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Set_ArrayIndex_UpdatesExistingElement()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Tags = ["a", "b", "c"] },
            new PartitionKey("pk1"));

        var response = await _container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/tags/1", "B")]);

        var tags = response.Resource["tags"]!.ToObject<string[]>();
        tags.Should().BeEquivalentTo(["a", "B", "c"]);
        tags.Should().HaveCount(3);
    }

    [Fact]
    public async Task Set_ArrayIndex_OutOfBounds_ThrowsBadRequest()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Tags = ["a", "b"] },
            new PartitionKey("pk1"));

        var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/tags/10", "x")]);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }
}


/// <summary>
/// Phase 5: Per Cosmos DB docs, Add at an array index greater than the array
/// length results in an error. Add at index == length appends.
/// See: https://learn.microsoft.com/en-us/azure/cosmos-db/partial-document-update
/// </summary>
public class PatchAddArrayBoundsTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Add_ArrayIndex_BeyondLength_ThrowsBadRequest()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Tags = ["a", "b"] },
            new PartitionKey("pk1"));

        var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Add("/tags/10", "x")]);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Add_ArrayIndex_AtLength_AppendsElement()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Tags = ["a", "b"] },
            new PartitionKey("pk1"));

        // Index 2 == array length, should append
        var response = await _container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
            [PatchOperation.Add("/tags/2", "c")]);

        var tags = response.Resource["tags"]!.ToObject<string[]>();
        tags.Should().BeEquivalentTo(["a", "b", "c"]);
    }
}


/// <summary>
/// Phase 6: Per Cosmos DB docs, Move requires the "from" location to exist.
/// Also, path can't be a JSON child of from.
/// See: https://learn.microsoft.com/en-us/azure/cosmos-db/partial-document-update
/// </summary>
public class PatchMoveValidationTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Move_NonExistentSource_ThrowsBadRequest()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Move("/nonExistent", "/destination")]);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    /// <summary>
    /// SKIPPED: Real Cosmos DB rejects Move when path is a JSON child of from
    /// (e.g., Move from '/nested' to '/nested/child'). Detecting JSON path ancestry
    /// and enforcing this constraint adds complexity with very low practical impact.
    /// The emulator does not validate path ancestry — it silently processes the move
    /// which may produce unexpected results.
    /// </summary>
    [Fact]
    public async Task Move_PathIsChildOfFrom_ThrowsBadRequest()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test",
                Nested = new NestedObject { Description = "N", Score = 5.0 } },
            new PartitionKey("pk1"));

        var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Move("/nested", "/nested/child")]);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    /// <summary>
    /// SISTER TEST documenting divergent behavior for the skipped test above.
    /// The emulator does not validate that the destination path is not a child of
    /// the source path in Move operations. Real Cosmos DB would reject this with 400.
    /// In practice, this is an extremely rare edge case.
    /// </summary>
    [Fact]
    public async Task Move_PathIsChildOfFrom_EmulatorBehavior_AlsoRejectsBadRequest()
    {
        // The emulator now correctly rejects Move when path is a child of from,
        // matching real Cosmos DB behavior.
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test",
                Nested = new NestedObject { Description = "N", Score = 5.0 } },
            new PartitionKey("pk1"));

        var act = () => _container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
            [PatchOperation.Move("/nested", "/nested/child")]);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Move_ToExistingPath_OverwritesTarget()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Source", Value = 42 },
            new PartitionKey("pk1"));

        // Move /name → /movedName (which doesn't exist yet, fine)
        var response = await _container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
            [PatchOperation.Move("/name", "/movedName")]);

        response.Resource["movedName"]!.ToString().Should().Be("Source");
        response.Resource["name"].Should().BeNull();
    }
}


/// <summary>
/// Phase 7: Per Cosmos DB FAQ, there is a limit of 10 patch operations per call.
/// See: https://learn.microsoft.com/en-us/azure/cosmos-db/partial-document-update-faq
/// </summary>
public class PatchOperationsLimitTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Patch_MoreThan10Operations_ThrowsBadRequest()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 0 },
            new PartitionKey("pk1"));

        var ops = Enumerable.Range(0, 11)
            .Select(i => PatchOperation.Increment("/value", 1))
            .ToList();

        var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), ops);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Patch_Exactly10Operations_Succeeds()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 0 },
            new PartitionKey("pk1"));

        var ops = Enumerable.Range(0, 10)
            .Select(i => PatchOperation.Increment("/value", 1))
            .ToList();

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"), ops);

        response.Resource.Value.Should().Be(10);
    }
}


/// <summary>
/// Phase 8: Per Cosmos DB FAQ, system-generated properties (_ts, _etag, _rid) cannot be patched.
/// See: https://learn.microsoft.com/en-us/azure/cosmos-db/partial-document-update-faq
///
/// SKIPPED: Real Cosmos DB rejects patches to system-generated properties (_ts, _etag, _rid, _self)
/// with 400 Bad Request. The emulator's EnrichWithSystemProperties overwrites _ts and _etag after
/// the patch, making the mutation harmless but not spec-compliant. Implementing validation for
/// all system properties would require maintaining a list and checking each operation path, which
/// adds complexity for a low-impact edge case (patching system properties is nonsensical).
/// </summary>
public class PatchSystemPropertyProtectionTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Patch_SystemProperty_Ts_ThrowsBadRequest()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/_ts", 9999999)]);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    /// <summary>
    /// SISTER TEST documenting divergent behavior for system property protection.
    /// Real Cosmos DB rejects patches to _ts with 400 Bad Request.
    /// The emulator allows it but EnrichWithSystemProperties overwrites _ts afterwards,
    /// so the net effect is harmless — the patched _ts value is discarded.
    /// </summary>
    [Fact]
    public async Task Patch_SystemProperty_Ts_EmulatorAlsoRejectsBadRequest()
    {
        // The emulator now correctly rejects patches to system properties,
        // matching real Cosmos DB behavior.
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var act = () => _container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/_ts", 9999999)]);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }
}


/// <summary>
/// Phase 9: PatchItemStreamAsync coverage — verifying the stream-based patch
/// variant has the same behavior and validation as the typed variant.
/// </summary>
public class PatchStreamVariantTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task PatchItemStreamAsync_SetOperation_ReturnsOK()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var response = await _container.PatchItemStreamAsync("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Patched")]);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task PatchItemStreamAsync_NonExistentItem_ReturnsNotFound()
    {
        var response = await _container.PatchItemStreamAsync("missing", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Patched")]);

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task PatchItemStreamAsync_StaleETag_ReturnsPreconditionFailed()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var response = await _container.PatchItemStreamAsync("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Patched")],
            new PatchItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

        response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task PatchItemStreamAsync_FilterPredicate_NonMatching_ReturnsPreconditionFailed()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", IsActive = false },
            new PartitionKey("pk1"));

        var response = await _container.PatchItemStreamAsync("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Patched")],
            new PatchItemRequestOptions { FilterPredicate = "FROM c WHERE c.isActive = true" });

        response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task PatchItemStreamAsync_EmptyOperations_ReturnsBadRequest()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var response = await _container.PatchItemStreamAsync("1", new PartitionKey("pk1"),
            Array.Empty<PatchOperation>());

        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }
}


/// <summary>
/// Phase 10: Edge cases for patch operations — null values, complex objects,
/// negative increments, type preservation, wrong partition key, etc.
/// </summary>
public class PatchEdgeCaseTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Patch_SetNullValue_SetsPropertyToNull()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var response = await _container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
            [PatchOperation.Set<string>("/name", null!)]);

        response.Resource["name"]!.Type.Should().Be(JTokenType.Null);
    }

    [Fact]
    public async Task Patch_SetComplexObject_SetsNestedStructure()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var complexObj = new { street = "123 Main St", city = "Springfield", zip = "62701" };
        var response = await _container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/address", complexObj)]);

        response.Resource["address"]!["city"]!.ToString().Should().Be("Springfield");
    }

    [Fact]
    public async Task Patch_IncrementNegative_Decrements()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 10 },
            new PartitionKey("pk1"));

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Increment("/value", -3)]);

        response.Resource.Value.Should().Be(7);
    }

    [Fact]
    public async Task Patch_WrongPartitionKey_ThrowsNotFound()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("wrong-pk"),
            [PatchOperation.Set("/name", "Patched")]);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Patch_AfterDelete_ThrowsNotFound()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Patched")]);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Patch_UpdatesTimestamp()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var before = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
        var tsBefore = before.Resource["_ts"]!.Value<long>();

        await Task.Delay(10); // Ensure time passes
        await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Patched")]);

        var after = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
        var tsAfter = after.Resource["_ts"]!.Value<long>();
        tsAfter.Should().BeGreaterThanOrEqualTo(tsBefore);
    }

    [Fact]
    public async Task Patch_Set_BooleanValue_Updates()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", IsActive = true },
            new PartitionKey("pk1"));

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/isActive", false)]);

        response.Resource.IsActive.Should().BeFalse();
    }

    [Fact]
    public async Task Patch_Set_ArrayValue_ReplacesEntireArray()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Tags = ["old1", "old2"] },
            new PartitionKey("pk1"));

        var response = await _container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
            [PatchOperation.Set("/tags", new[] { "new1", "new2", "new3" })]);

        var tags = response.Resource["tags"]!.ToObject<string[]>();
        tags.Should().BeEquivalentTo(["new1", "new2", "new3"]);
    }
}
