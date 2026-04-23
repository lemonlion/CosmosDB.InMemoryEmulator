using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═════════════════════════════════════════════════════════════════════════════
//  UniqueKeyPolicy edge-case / red-team tests
// ═════════════════════════════════════════════════════════════════════════════

// ─── 1. Null / Missing Properties ────────────────────────────────────────────

public class UniqueKeyEdgeCaseNullMissingTests
{
    private static InMemoryContainer CreateContainer(string path = "/email")
    {
        var props = new ContainerProperties("uk-null", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { path } } }
            }
        };
        return new InMemoryContainer(props);
    }

    /// <summary>
    /// Two items in the same partition that both lack the unique-key property.
    /// In real Cosmos DB, missing properties are treated as null, and multiple
    /// null values ARE allowed under a unique key constraint.
    /// If the emulator wrongly conflicts on two nulls, this is a false-positive bug.
    /// </summary>
    [Fact]
    public async Task TwoItems_BothMissingUniqueKeyProperty_ShouldNotConflict()
    {
        var container = CreateContainer();

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"));

        // Second item also missing "email" — real Cosmos allows this
        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a" }),
            new PartitionKey("a"));

        // Real Cosmos DB allows multiple items with missing (null) unique key values
        await act.Should().NotThrowAsync();
    }

    /// <summary>
    /// One item has the property, the other doesn't.
    /// These should NOT conflict because their values differ (value vs null).
    /// </summary>
    [Fact]
    public async Task OneItemHasProperty_OtherMissing_ShouldNotConflict()
    {
        var container = CreateContainer();

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));

        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a" }),
            new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }

    /// <summary>
    /// Both items have the property set to an explicit JSON null.
    /// Real Cosmos DB allows multiple nulls under a unique key.
    /// </summary>
    [Fact]
    public async Task TwoItems_BothExplicitNull_ShouldNotConflict()
    {
        var container = CreateContainer();

        var item1 = JObject.FromObject(new { id = "1", pk = "a" });
        item1["email"] = JValue.CreateNull();

        var item2 = JObject.FromObject(new { id = "2", pk = "a" });
        item2["email"] = JValue.CreateNull();

        await container.CreateItemAsync(item1, new PartitionKey("a"));

        var act = () => container.CreateItemAsync(item2, new PartitionKey("a"));

        // Real Cosmos DB allows multiple items with explicit null unique key values
        await act.Should().NotThrowAsync();
    }

    /// <summary>
    /// One item has a missing property and the other has explicit null.
    /// In real Cosmos both are considered "null" and both are allowed.
    /// </summary>
    [Fact]
    public async Task MissingProperty_VsExplicitNull_ShouldNotConflict()
    {
        var container = CreateContainer();

        // item1: property is absent
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"));

        // item2: property is explicit null
        var item2 = JObject.FromObject(new { id = "2", pk = "a" });
        item2["email"] = JValue.CreateNull();

        var act = () => container.CreateItemAsync(item2, new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }
}

// ─── 2. Type Coercion — .ToString() collapses types ─────────────────────────

public class UniqueKeyEdgeCaseTypeCoercionTests
{
    private static InMemoryContainer CreateContainer()
    {
        var props = new ContainerProperties("uk-types", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/code" } } }
            }
        };
        return new InMemoryContainer(props);
    }

    /// <summary>
    /// Numeric 42 vs string "42".  In real Cosmos these are different types and
    /// should NOT conflict.  But the emulator uses .ToString() which yields "42"
    /// for both — potential false-positive conflict.
    /// </summary>
    [Fact]
    public async Task NumericValue_VsStringValue_SameLiteral_ShouldNotConflict()
    {
        var container = CreateContainer();

        var item1 = new JObject { ["id"] = "1", ["pk"] = "a", ["code"] = 42 };
        var item2 = new JObject { ["id"] = "2", ["pk"] = "a", ["code"] = "42" };

        await container.CreateItemAsync(item1, new PartitionKey("a"));

        var act = () => container.CreateItemAsync(item2, new PartitionKey("a"));

        // Real Cosmos treats number 42 and string "42" as different values
        await act.Should().NotThrowAsync();
    }

    /// <summary>
    /// Boolean true vs string "True".  .ToString() on a bool JToken returns "True"
    /// which matches the string "True" — false-positive conflict.
    /// </summary>
    [Fact]
    public async Task BooleanTrue_VsStringTrue_ShouldNotConflict()
    {
        var container = CreateContainer();

        var item1 = new JObject { ["id"] = "1", ["pk"] = "a", ["code"] = true };
        var item2 = new JObject { ["id"] = "2", ["pk"] = "a", ["code"] = "True" };

        await container.CreateItemAsync(item1, new PartitionKey("a"));

        var act = () => container.CreateItemAsync(item2, new PartitionKey("a"));

        // Different JSON types should not conflict
        await act.Should().NotThrowAsync();
    }

    /// <summary>
    /// Integer 1 vs boolean true.  .ToString() yields "1" vs "True" — these won't
    /// collide via ToString, but verify the emulator correctly allows both.
    /// </summary>
    [Fact]
    public async Task Integer1_VsBoolTrue_ShouldNotConflict()
    {
        var container = CreateContainer();

        var item1 = new JObject { ["id"] = "1", ["pk"] = "a", ["code"] = 1 };
        var item2 = new JObject { ["id"] = "2", ["pk"] = "a", ["code"] = true };

        await container.CreateItemAsync(item1, new PartitionKey("a"));

        var act = () => container.CreateItemAsync(item2, new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }

    /// <summary>
    /// Float 1.0 vs integer 1.  .ToString() might yield "1.0" vs "1" — they
    /// shouldn't conflict even though mathematically equal, if Cosmos treats
    /// them as different types.  Actually, in real Cosmos numeric types may
    /// be unified, so this verifies the current behavior.
    /// </summary>
    [Fact]
    public async Task Float1_VsInteger1_ShouldConflict()
    {
        var container = CreateContainer();

        var item1 = new JObject { ["id"] = "1", ["pk"] = "a", ["code"] = 1 };
        var item2 = new JObject { ["id"] = "2", ["pk"] = "a", ["code"] = 1.0 };

        await container.CreateItemAsync(item1, new PartitionKey("a"));

        // Real Cosmos DB treats 1 and 1.0 as the same value for unique key purposes
        var act = () => container.CreateItemAsync(item2, new PartitionKey("a"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }
}

// ─── 3. Empty String vs Null ─────────────────────────────────────────────────

public class UniqueKeyEdgeCaseEmptyStringTests
{
    private static InMemoryContainer CreateContainer()
    {
        var props = new ContainerProperties("uk-empty", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        return new InMemoryContainer(props);
    }

    /// <summary>
    /// Empty string "" vs missing property (null).
    /// These should NOT conflict because "" is a string value while missing is null.
    /// </summary>
    [Fact]
    public async Task EmptyString_VsMissingProperty_ShouldNotConflict()
    {
        var container = CreateContainer();

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "" }),
            new PartitionKey("a"));

        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a" }),
            new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }

    /// <summary>
    /// Two items with empty string "" — these SHOULD conflict because they have
    /// the same non-null value.
    /// </summary>
    [Fact]
    public async Task TwoEmptyStrings_ShouldConflict()
    {
        var container = CreateContainer();

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "" }),
            new PartitionKey("a"));

        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", email = "" }),
            new PartitionKey("a"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }
}

// ─── 4. Array Index Paths ────────────────────────────────────────────────────

public class UniqueKeyEdgeCaseArrayPathTests
{
    /// <summary>
    /// Path /tags/0 should resolve array index 0.
    /// BuildSelectTokenPath converts "0" to "[0]", producing "tags[0]".
    /// Verify this works end-to-end.
    /// </summary>
    [Fact]
    public async Task ArrayIndexPath_ShouldResolveCorrectly()
    {
        var props = new ContainerProperties("uk-arr", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/tags/0" } } }
            }
        };
        var container = new InMemoryContainer(props);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", tags = new[] { "alpha", "beta" } }),
            new PartitionKey("a"));

        // Same first tag — should conflict
        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", tags = new[] { "alpha", "gamma" } }),
            new PartitionKey("a"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    /// <summary>
    /// Different array values at index 0 should not conflict.
    /// </summary>
    [Fact]
    public async Task ArrayIndexPath_DifferentValues_ShouldNotConflict()
    {
        var props = new ContainerProperties("uk-arr2", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/tags/0" } } }
            }
        };
        var container = new InMemoryContainer(props);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", tags = new[] { "alpha" } }),
            new PartitionKey("a"));

        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", tags = new[] { "beta" } }),
            new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }
}

// ─── 5. Deeply Nested Objects ────────────────────────────────────────────────

public class UniqueKeyEdgeCaseDeeplyNestedTests
{
    /// <summary>
    /// Unique key on a 5-level deep nested path.
    /// Verifies CosmosPathToSelectTokenPath handles many segments.
    /// </summary>
    [Fact]
    public async Task DeeplyNestedPath_5Levels_ShouldEnforceUniqueness()
    {
        var props = new ContainerProperties("uk-deep", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/a/b/c/d/e" } } }
            }
        };
        var container = new InMemoryContainer(props);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", a = new { b = new { c = new { d = new { e = "deep-val" } } } } }),
            new PartitionKey("a"));

        // Same deeply nested value — should conflict
        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", a = new { b = new { c = new { d = new { e = "deep-val" } } } } }),
            new PartitionKey("a"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    /// <summary>
    /// Different values at a deeply nested path should not conflict.
    /// </summary>
    [Fact]
    public async Task DeeplyNestedPath_DifferentValues_ShouldNotConflict()
    {
        var props = new ContainerProperties("uk-deep2", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/a/b/c/d/e" } } }
            }
        };
        var container = new InMemoryContainer(props);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", a = new { b = new { c = new { d = new { e = "val1" } } } } }),
            new PartitionKey("a"));

        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", a = new { b = new { c = new { d = new { e = "val2" } } } } }),
            new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }
}

// ─── 6. Multiple UniqueKeys ──────────────────────────────────────────────────

public class UniqueKeyEdgeCaseMultipleKeysTests
{
    /// <summary>
    /// Multiple UniqueKey entries with different paths — all should be enforced independently.
    /// </summary>
    [Fact]
    public async Task MultipleUniqueKeys_AllEnforcedIndependently()
    {
        var props = new ContainerProperties("uk-multi", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys =
                {
                    new UniqueKey { Paths = { "/email" } },
                    new UniqueKey { Paths = { "/phone" } }
                }
            }
        };
        var container = new InMemoryContainer(props);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "a@test.com", phone = "111" }),
            new PartitionKey("a"));

        // Different email but same phone — should conflict on phone
        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", email = "b@test.com", phone = "111" }),
            new PartitionKey("a"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    /// <summary>
    /// Composite unique key (multiple paths in a single UniqueKey).
    /// The combination must be unique, not each path individually.
    /// </summary>
    [Fact]
    public async Task CompositeUniqueKey_SameCombination_ShouldConflict()
    {
        var props = new ContainerProperties("uk-comp", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys =
                {
                    new UniqueKey { Paths = { "/firstName", "/lastName" } }
                }
            }
        };
        var container = new InMemoryContainer(props);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", firstName = "John", lastName = "Doe" }),
            new PartitionKey("a"));

        // Same firstName + lastName combo
        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", firstName = "John", lastName = "Doe" }),
            new PartitionKey("a"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    /// <summary>
    /// Composite unique key — different combination should not conflict.
    /// </summary>
    [Fact]
    public async Task CompositeUniqueKey_DifferentCombination_ShouldNotConflict()
    {
        var props = new ContainerProperties("uk-comp2", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys =
                {
                    new UniqueKey { Paths = { "/firstName", "/lastName" } }
                }
            }
        };
        var container = new InMemoryContainer(props);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", firstName = "John", lastName = "Doe" }),
            new PartitionKey("a"));

        // Same firstName but different lastName
        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", firstName = "John", lastName = "Smith" }),
            new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }
}

// ─── 7. Partition Key Scoping ────────────────────────────────────────────────

public class UniqueKeyEdgeCasePartitionScopeTests
{
    /// <summary>
    /// Unique keys are scoped per logical partition.
    /// Same value in different partitions should NOT conflict.
    /// </summary>
    [Fact]
    public async Task SameUniqueKeyValue_DifferentPartitions_ShouldNotConflict()
    {
        var props = new ContainerProperties("uk-pk", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        var container = new InMemoryContainer(props);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "partition-A", email = "same@test.com" }),
            new PartitionKey("partition-A"));

        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "partition-B", email = "same@test.com" }),
            new PartitionKey("partition-B"));

        await act.Should().NotThrowAsync();
    }
}

// ─── 8. Delete + Recreate ────────────────────────────────────────────────────

public class UniqueKeyEdgeCaseDeleteRecreateTests
{
    /// <summary>
    /// After deleting an item, a new item with the same unique key value
    /// should be accepted.
    /// </summary>
    [Fact]
    public async Task DeletedItem_NewItemCanReuseUniqueKeyValue()
    {
        var props = new ContainerProperties("uk-del", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        var container = new InMemoryContainer(props);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));

        await container.DeleteItemAsync<JObject>("1", new PartitionKey("a"));

        // Should be able to reuse the email
        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }
}

// ─── 9. Upsert Behavior ─────────────────────────────────────────────────────

public class UniqueKeyEdgeCaseUpsertTests
{
    private static InMemoryContainer CreateContainer()
    {
        var props = new ContainerProperties("uk-upsert", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        return new InMemoryContainer(props);
    }

    /// <summary>
    /// Upserting an existing item, changing the unique key to a value held by
    /// another item, should throw Conflict.
    /// </summary>
    [Fact]
    public async Task Upsert_ExistingItem_CollidesWithOtherItem_ShouldConflict()
    {
        var container = CreateContainer();

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", email = "bob@test.com" }),
            new PartitionKey("a"));

        // Upsert item 2 with email of item 1
        var act = () => container.UpsertItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    /// <summary>
    /// Upserting an existing item keeping its own unique key value should succeed.
    /// </summary>
    [Fact]
    public async Task Upsert_SameItem_SameUniqueKey_ShouldSucceed()
    {
        var container = CreateContainer();

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));

        var act = () => container.UpsertItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "alice@test.com", extra = "data" }),
            new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }
}

// ─── 10. PatchItem Bypass ────────────────────────────────────────────────────

public class UniqueKeyEdgeCasePatchTests
{
    /// <summary>
    /// Patching a unique-key property to match another item's value should conflict.
    /// </summary>
    [Fact]
    public async Task PatchItem_SetsUniqueKeyToCollidingValue_ShouldConflict()
    {
        var props = new ContainerProperties("uk-patch", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        var container = new InMemoryContainer(props);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", email = "bob@test.com" }),
            new PartitionKey("a"));

        var act = () => container.PatchItemAsync<JObject>(
            "2", new PartitionKey("a"),
            new[] { PatchOperation.Set("/email", "alice@test.com") });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    /// <summary>
    /// Patching a non-unique-key property should not trigger conflict.
    /// </summary>
    [Fact]
    public async Task PatchItem_NonUniqueKeyProperty_ShouldSucceed()
    {
        var props = new ContainerProperties("uk-patch2", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        var container = new InMemoryContainer(props);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "alice@test.com", name = "Alice" }),
            new PartitionKey("a"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", email = "bob@test.com", name = "Bob" }),
            new PartitionKey("a"));

        var act = () => container.PatchItemAsync<JObject>(
            "2", new PartitionKey("a"),
            new[] { PatchOperation.Set("/name", "Robert") });

        await act.Should().NotThrowAsync();
    }

    /// <summary>
    /// Removing a unique-key property via patch effectively sets it to null.
    /// Should succeed even if another item also has null for that property
    /// (real Cosmos allows multiple nulls).
    /// </summary>
    [Fact]
    public async Task PatchItem_RemoveUniqueKeyProperty_ShouldSucceedIfNullAllowed()
    {
        var props = new ContainerProperties("uk-patch-rm", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        var container = new InMemoryContainer(props);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),  // no email = null
            new PartitionKey("a"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", email = "bob@test.com" }),
            new PartitionKey("a"));

        // Remove email from item 2 — now both items have null email
        var act = () => container.PatchItemAsync<JObject>(
            "2", new PartitionKey("a"),
            new[] { PatchOperation.Remove("/email") });

        // Should succeed because multiple nulls are allowed in real Cosmos
        await act.Should().NotThrowAsync();
    }
}

// ─── 11. Stream APIs ─────────────────────────────────────────────────────────

public class UniqueKeyEdgeCaseStreamTests
{
    private static InMemoryContainer CreateContainer()
    {
        var props = new ContainerProperties("uk-stream", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        return new InMemoryContainer(props);
    }

    private static MemoryStream ToStream(object obj) =>
        new(Encoding.UTF8.GetBytes(JObject.FromObject(obj).ToString()));

    /// <summary>
    /// CreateItemStreamAsync should enforce unique keys.
    /// </summary>
    [Fact]
    public async Task CreateItemStream_DuplicateUniqueKey_ReturnsConflict()
    {
        var container = CreateContainer();

        await container.CreateItemStreamAsync(
            ToStream(new { id = "1", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));

        var response = await container.CreateItemStreamAsync(
            ToStream(new { id = "2", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));

        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    /// <summary>
    /// ReplaceItemStreamAsync should enforce unique keys.
    /// </summary>
    [Fact]
    public async Task ReplaceItemStream_DuplicateUniqueKey_ReturnsConflict()
    {
        var container = CreateContainer();

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

    /// <summary>
    /// UpsertItemStreamAsync should enforce unique keys.
    /// </summary>
    [Fact]
    public async Task UpsertItemStream_DuplicateUniqueKey_ReturnsConflict()
    {
        var container = CreateContainer();

        await container.CreateItemStreamAsync(
            ToStream(new { id = "1", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));
        await container.CreateItemStreamAsync(
            ToStream(new { id = "2", pk = "a", email = "bob@test.com" }),
            new PartitionKey("a"));

        // Upsert item 2 with colliding email
        var response = await container.UpsertItemStreamAsync(
            ToStream(new { id = "2", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));

        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }
}

// ─── 12. Transactional Batch ─────────────────────────────────────────────────

public class UniqueKeyEdgeCaseBatchTests
{
    /// <summary>
    /// A transactional batch that creates two items with colliding unique keys
    /// should fail the entire batch.
    /// </summary>
    [Fact]
    public async Task Batch_TwoCreatesWithCollidingUniqueKey_ShouldFail()
    {
        var props = new ContainerProperties("uk-batch", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        var container = new InMemoryContainer(props);

        var batch = container.CreateTransactionalBatch(new PartitionKey("a"));
        batch.CreateItem(JObject.FromObject(new { id = "1", pk = "a", email = "same@test.com" }));
        batch.CreateItem(JObject.FromObject(new { id = "2", pk = "a", email = "same@test.com" }));

        var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeFalse();
    }

    /// <summary>
    /// A batch create that collides with an existing item should fail.
    /// </summary>
    [Fact]
    public async Task Batch_CreateCollidesWithExistingItem_ShouldFail()
    {
        var props = new ContainerProperties("uk-batch2", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        var container = new InMemoryContainer(props);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));

        var batch = container.CreateTransactionalBatch(new PartitionKey("a"));
        batch.CreateItem(JObject.FromObject(new { id = "2", pk = "a", email = "alice@test.com" }));

        var response = await batch.ExecuteAsync();

        response.IsSuccessStatusCode.Should().BeFalse();
    }
}

// ─── 13. TTL-Expired Items ───────────────────────────────────────────────────

public class UniqueKeyEdgeCaseTtlTests
{
    /// <summary>
    /// If a container has TTL enabled and an item has expired but not yet been
    /// evicted, it should NOT block a new item from taking its unique key value.
    /// This tests whether the emulator correctly ignores expired items during
    /// unique key validation.
    /// </summary>
    [Fact]
    public async Task ExpiredItem_ShouldNotBlockNewItemWithSameUniqueKey()
    {
        var props = new ContainerProperties("uk-ttl", "/pk")
        {
            DefaultTimeToLive = 1,  // 1 second TTL
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        var container = new InMemoryContainer(props);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));

        // Wait for TTL to expire
        await Task.Delay(1500);

        // New item with same email should be allowed since the old item expired
        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }
}

// ─── 14. Hierarchical Partition Keys ─────────────────────────────────────────

public class UniqueKeyEdgeCaseHierarchicalPartitionKeyTests
{
    /// <summary>
    /// Unique keys with hierarchical (composite) partition keys.
    /// Items in different logical partitions (different composite key) should
    /// not conflict even with same unique key value.
    /// </summary>
    [Fact]
    public async Task HierarchicalPartitionKey_DifferentPartitions_ShouldNotConflict()
    {
        var props = new ContainerProperties("uk-hier", new List<string> { "/tenantId", "/region" })
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        var container = new InMemoryContainer(props);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", tenantId = "t1", region = "us", email = "alice@test.com" }),
            new PartitionKeyBuilder().Add("t1").Add("us").Build());

        // Same email, different hierarchical partition
        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "2", tenantId = "t1", region = "eu", email = "alice@test.com" }),
            new PartitionKeyBuilder().Add("t1").Add("eu").Build());

        await act.Should().NotThrowAsync();
    }

    /// <summary>
    /// Same hierarchical partition — should conflict.
    /// </summary>
    [Fact]
    public async Task HierarchicalPartitionKey_SamePartition_ShouldConflict()
    {
        var props = new ContainerProperties("uk-hier2", new List<string> { "/tenantId", "/region" })
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        var container = new InMemoryContainer(props);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", tenantId = "t1", region = "us", email = "alice@test.com" }),
            new PartitionKeyBuilder().Add("t1").Add("us").Build());

        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "2", tenantId = "t1", region = "us", email = "alice@test.com" }),
            new PartitionKeyBuilder().Add("t1").Add("us").Build());

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }
}

// ─── 15. Case Sensitivity ────────────────────────────────────────────────────

public class UniqueKeyEdgeCaseSensitivityTests
{
    /// <summary>
    /// Unique key values should be case-sensitive.
    /// "Alice" and "alice" should NOT conflict.
    /// </summary>
    [Fact]
    public async Task UniqueKeyValues_AreCaseSensitive()
    {
        var props = new ContainerProperties("uk-case", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        var container = new InMemoryContainer(props);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "Alice@test.com" }),
            new PartitionKey("a"));

        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));

        // Different case = different value in Cosmos DB
        await act.Should().NotThrowAsync();
    }
}

// ─── 16. Special Characters in Property Names ────────────────────────────────

public class UniqueKeyEdgeCaseSpecialCharTests
{
    /// <summary>
    /// Property names with dots or special characters.
    /// Cosmos paths use / as separator. A property named "my.field" would be
    /// referenced as /my.field in Cosmos. After path conversion this becomes
    /// "my.field" which SelectToken interprets as nested path "my" → "field".
    /// This would incorrectly resolve the property.
    /// </summary>
    [Fact]
    public async Task PropertyNameWithDot_PathResolvesCorrectly()
    {
        var props = new ContainerProperties("uk-dot", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/my.field" } } }
            }
        };
        var container = new InMemoryContainer(props);

        // Create item with property named "my.field" (single property, not nested)
        var item1 = new JObject
        {
            ["id"] = "1",
            ["pk"] = "a",
            ["my.field"] = "value1"
        };

        await container.CreateItemAsync(item1, new PartitionKey("a"));

        // Same "my.field" value
        var item2 = new JObject
        {
            ["id"] = "2",
            ["pk"] = "a",
            ["my.field"] = "value1"
        };

        var act = () => container.CreateItemAsync(item2, new PartitionKey("a"));

        // Should conflict because both items have the same value for "my.field"
        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    /// <summary>
    /// Property name with spaces.
    /// Path "/my field" → "my field" in SelectToken, which should work with SelectToken.
    /// </summary>
    [Fact]
    public async Task PropertyNameWithSpace_ShouldResolveCorrectly()
    {
        var props = new ContainerProperties("uk-space", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/my field" } } }
            }
        };
        var container = new InMemoryContainer(props);

        var item1 = new JObject
        {
            ["id"] = "1",
            ["pk"] = "a",
            ["my field"] = "value1"
        };

        await container.CreateItemAsync(item1, new PartitionKey("a"));

        var item2 = new JObject
        {
            ["id"] = "2",
            ["pk"] = "a",
            ["my field"] = "value1"
        };

        var act = () => container.CreateItemAsync(item2, new PartitionKey("a"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }
}

// ─── 17. Replace/Upsert Self-Update ─────────────────────────────────────────

public class UniqueKeyEdgeCaseSelfUpdateTests
{
    /// <summary>
    /// Replacing an item should not conflict with its own current unique key value.
    /// </summary>
    [Fact]
    public async Task Replace_SameItem_SameUniqueKey_ShouldSucceed()
    {
        var props = new ContainerProperties("uk-self", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        var container = new InMemoryContainer(props);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "alice@test.com", name = "Alice" }),
            new PartitionKey("a"));

        // Replace the same item, keeping the same email
        var act = () => container.ReplaceItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "alice@test.com", name = "Alice Updated" }),
            "1", new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }
}

// ─── 18. Nested Path with Array Index Combo ──────────────────────────────────

public class UniqueKeyEdgeCaseNestedArrayComboTests
{
    /// <summary>
    /// Path like /data/items/0/code — combining nested objects with array index.
    /// </summary>
    [Fact]
    public async Task NestedObjectWithArrayIndex_ShouldResolve()
    {
        var props = new ContainerProperties("uk-nested-arr", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/data/items/0/code" } } }
            }
        };
        var container = new InMemoryContainer(props);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", data = new { items = new[] { new { code = "ABC" } } } }),
            new PartitionKey("a"));

        // Same code at data.items[0].code
        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", data = new { items = new[] { new { code = "ABC" } } } }),
            new PartitionKey("a"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }
}

// ─── 19. Concurrent Writes ──────────────────────────────────────────────────

public class UniqueKeyEdgeCaseConcurrencyTests
{
    /// <summary>
    /// Many concurrent creates with the same unique key value — exactly one should succeed,
    /// the rest should fail with conflict or duplicate id.
    /// </summary>
    [Fact]
    public async Task ConcurrentCreates_SameUniqueKey_OnlyOneSucceeds()
    {
        var props = new ContainerProperties("uk-conc", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        var container = new InMemoryContainer(props);

        var tasks = Enumerable.Range(0, 20).Select(i =>
            Task.Run(async () =>
            {
                try
                {
                    await container.CreateItemAsync(
                        JObject.FromObject(new { id = i.ToString(), pk = "a", email = "race@test.com" }),
                        new PartitionKey("a"));
                    return true;
                }
                catch (CosmosException)
                {
                    return false;
                }
            })).ToArray();

        var results = await Task.WhenAll(tasks);
        var successCount = results.Count(r => r);

        // Exactly one should succeed
        successCount.Should().Be(1);
    }

    /// <summary>
    /// Concurrent creates with different unique key values should all succeed.
    /// </summary>
    [Fact]
    public async Task ConcurrentCreates_DifferentUniqueKeys_AllSucceed()
    {
        var props = new ContainerProperties("uk-conc2", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        var container = new InMemoryContainer(props);

        var tasks = Enumerable.Range(0, 20).Select(i =>
            Task.Run(async () =>
            {
                await container.CreateItemAsync(
                    JObject.FromObject(new { id = i.ToString(), pk = "a", email = $"user{i}@test.com" }),
                    new PartitionKey("a"));
            })).ToArray();

        var act = () => Task.WhenAll(tasks);

        await act.Should().NotThrowAsync();
    }
}

// ─── 20. Nested Path Unique Key via Cosmos Path Format ───────────────────────

public class UniqueKeyEdgeCaseCosmosPathFormatTests
{
    /// <summary>
    /// The core bug that was fixed: Cosmos paths like "/value/code" must be
    /// converted to "value.code" for SelectToken. Verify this works.
    /// </summary>
    [Fact]
    public async Task NestedCosmosPath_EnforcesUniqueness()
    {
        var props = new ContainerProperties("uk-nested-path", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/value/code" } } }
            }
        };
        var container = new InMemoryContainer(props);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", value = new { code = "X" } }),
            new PartitionKey("a"));

        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", value = new { code = "X" } }),
            new PartitionKey("a"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    /// <summary>
    /// Different nested values should not conflict.
    /// </summary>
    [Fact]
    public async Task NestedCosmosPath_DifferentValues_ShouldNotConflict()
    {
        var props = new ContainerProperties("uk-nested-path2", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/value/code" } } }
            }
        };
        var container = new InMemoryContainer(props);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", value = new { code = "X" } }),
            new PartitionKey("a"));

        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", value = new { code = "Y" } }),
            new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }
}
