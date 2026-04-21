using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═════════════════════════════════════════════════════════════════════════════
//  UniqueKeyPolicy integration tests — issues #10 & #13
//  Validates unique-key enforcement through the full SDK HTTP pipeline.
//  Parity-validated: runs against both FakeCosmosHandler and real emulator.
// ═════════════════════════════════════════════════════════════════════════════

// ─── Nested-path unique key enforcement (Issue #10) ─────────────────────────

[Collection(IntegrationCollection.Name)]
public class UniqueKeyPolicyNestedPathIntegrationTests(EmulatorSession session) : IAsyncLifetime
{
    private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
    private Container _container = null!;

    public async ValueTask InitializeAsync()
    {
        _container = await _fixture.CreateContainerAsync("uk-nested", "/_partitionKey",
            configure: props =>
            {
                props.UniqueKeyPolicy = new UniqueKeyPolicy
                {
                    UniqueKeys = { new UniqueKey { Paths = { "/value/code" } } }
                };
            });
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task CreateItem_NestedPath_DuplicateValue_ThrowsConflict()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", _partitionKey = "ref", value = new { code = "DUPLICATE" } }),
            new PartitionKey("ref"));

        var act = () => _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", _partitionKey = "ref", value = new { code = "DUPLICATE" } }),
            new PartitionKey("ref"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task CreateItem_NestedPath_DifferentValues_Succeeds()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", _partitionKey = "ref", value = new { code = "ABC" } }),
            new PartitionKey("ref"));

        var act = () => _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", _partitionKey = "ref", value = new { code = "XYZ" } }),
            new PartitionKey("ref"));

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task CreateItem_NestedPath_DifferentPartitions_Succeeds()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", _partitionKey = "ref1", value = new { code = "SAME" } }),
            new PartitionKey("ref1"));

        var act = () => _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", _partitionKey = "ref2", value = new { code = "SAME" } }),
            new PartitionKey("ref2"));

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task UpsertItem_NestedPath_DuplicateValue_ThrowsConflict()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", _partitionKey = "ref", value = new { code = "DUP" } }),
            new PartitionKey("ref"));

        var act = () => _container.UpsertItemAsync(
            JObject.FromObject(new { id = "2", _partitionKey = "ref", value = new { code = "DUP" } }),
            new PartitionKey("ref"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task ReplaceItem_NestedPath_CollidesWithExisting_ThrowsConflict()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", _partitionKey = "ref", value = new { code = "A" } }),
            new PartitionKey("ref"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", _partitionKey = "ref", value = new { code = "B" } }),
            new PartitionKey("ref"));

        var act = () => _container.ReplaceItemAsync(
            JObject.FromObject(new { id = "2", _partitionKey = "ref", value = new { code = "A" } }),
            "2", new PartitionKey("ref"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }
}

// ─── Deeply nested path (3+ levels) ─────────────────────────────────────────

[Collection(IntegrationCollection.Name)]
public class UniqueKeyPolicyDeeplyNestedIntegrationTests(EmulatorSession session) : IAsyncLifetime
{
    private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
    private Container _container = null!;

    public async ValueTask InitializeAsync()
    {
        _container = await _fixture.CreateContainerAsync("uk-deep", "/pk",
            configure: props =>
            {
                props.UniqueKeyPolicy = new UniqueKeyPolicy
                {
                    UniqueKeys = { new UniqueKey { Paths = { "/a/b/c" } } }
                };
            });
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task CreateItem_ThreeLevelNesting_DuplicateValue_ThrowsConflict()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "p", a = new { b = new { c = "deep" } } }),
            new PartitionKey("p"));

        var act = () => _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "p", a = new { b = new { c = "deep" } } }),
            new PartitionKey("p"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task CreateItem_ThreeLevelNesting_DifferentValues_Succeeds()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "p", a = new { b = new { c = "val1" } } }),
            new PartitionKey("p"));

        var act = () => _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "p", a = new { b = new { c = "val2" } } }),
            new PartitionKey("p"));

        await act.Should().NotThrowAsync();
    }
}

// ─── UniqueKeyPolicy via Database.CreateContainerAsync (Issue #13) ──────────

[Collection(IntegrationCollection.Name)]
public class UniqueKeyPolicyViaDatabaseIntegrationTests(EmulatorSession session) : IAsyncLifetime
{
    private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
    private Container _container = null!;

    public async ValueTask InitializeAsync()
    {
        _container = await _fixture.CreateContainerAsync("uk-database", "/category",
            configure: props =>
            {
                props.UniqueKeyPolicy = new UniqueKeyPolicy
                {
                    UniqueKeys = { new UniqueKey { Paths = { "/name" } } }
                };
            });
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task CreateItem_ViaFixture_DuplicateValue_ThrowsConflict()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", category = "books", name = "Clean Code" }),
            new PartitionKey("books"));

        var act = () => _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", category = "books", name = "Clean Code" }),
            new PartitionKey("books"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task CreateItem_ViaFixture_NestedPath_DuplicateValue_ThrowsConflict()
    {
        // Uses a separate container with nested path
        var container2 = await _fixture.CreateContainerAsync("uk-database-nested", "/_partitionKey",
            configure: props =>
            {
                props.UniqueKeyPolicy = new UniqueKeyPolicy
                {
                    UniqueKeys = { new UniqueKey { Paths = { "/value/code" } } }
                };
            });

        await container2.CreateItemAsync(
            JObject.FromObject(new { id = "1", _partitionKey = "ref", value = new { code = "DUPLICATE" } }),
            new PartitionKey("ref"));

        var act = () => container2.CreateItemAsync(
            JObject.FromObject(new { id = "2", _partitionKey = "ref", value = new { code = "DUPLICATE" } }),
            new PartitionKey("ref"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task UpsertItem_ViaFixture_DuplicateValue_ThrowsConflict()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", category = "books", name = "Clean Code" }),
            new PartitionKey("books"));

        var act = () => _container.UpsertItemAsync(
            JObject.FromObject(new { id = "2", category = "books", name = "Clean Code" }),
            new PartitionKey("books"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task CreateItem_ViaFixture_DifferentValues_Succeeds()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", category = "books", name = "Book A" }),
            new PartitionKey("books"));

        var act = () => _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", category = "books", name = "Book B" }),
            new PartitionKey("books"));

        await act.Should().NotThrowAsync();
    }
}

// ─── Null / Missing unique key values ───────────────────────────────────────

[Collection(IntegrationCollection.Name)]
public class UniqueKeyPolicyNullMissingIntegrationTests(EmulatorSession session) : IAsyncLifetime
{
    private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
    private Container _container = null!;

    public async ValueTask InitializeAsync()
    {
        _container = await _fixture.CreateContainerAsync("uk-null", "/pk",
            configure: props =>
            {
                props.UniqueKeyPolicy = new UniqueKeyPolicy
                {
                    UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
                };
            });
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task TwoItems_BothMissingUniqueKeyProperty_ShouldNotConflict()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"));

        var act = () => _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a" }),
            new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task OneItemHasProperty_OtherMissing_ShouldNotConflict()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));

        var act = () => _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a" }),
            new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task TwoItems_BothExplicitNull_ShouldNotConflict()
    {
        var item1 = new JObject { ["id"] = "1", ["pk"] = "a", ["email"] = JValue.CreateNull() };
        var item2 = new JObject { ["id"] = "2", ["pk"] = "a", ["email"] = JValue.CreateNull() };

        await _container.CreateItemAsync(item1, new PartitionKey("a"));

        var act = () => _container.CreateItemAsync(item2, new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task OneItemExplicitNull_OtherMissing_ShouldNotConflict()
    {
        var item1 = new JObject { ["id"] = "1", ["pk"] = "a", ["email"] = JValue.CreateNull() };
        var item2 = JObject.FromObject(new { id = "2", pk = "a" });

        await _container.CreateItemAsync(item1, new PartitionKey("a"));

        var act = () => _container.CreateItemAsync(item2, new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }
}

// ─── Type coercion — different JSON types should not conflict ────────────────

[Collection(IntegrationCollection.Name)]
public class UniqueKeyPolicyTypeCoercionIntegrationTests(EmulatorSession session) : IAsyncLifetime
{
    private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
    private Container _container = null!;

    public async ValueTask InitializeAsync()
    {
        _container = await _fixture.CreateContainerAsync("uk-types", "/pk",
            configure: props =>
            {
                props.UniqueKeyPolicy = new UniqueKeyPolicy
                {
                    UniqueKeys = { new UniqueKey { Paths = { "/code" } } }
                };
            });
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task NumericAndString_SameLiteral_ShouldNotConflict()
    {
        // Numeric 42 vs string "42" — different types, should not conflict
        var item1 = new JObject { ["id"] = "1", ["pk"] = "a", ["code"] = 42 };
        var item2 = new JObject { ["id"] = "2", ["pk"] = "a", ["code"] = "42" };

        await _container.CreateItemAsync(item1, new PartitionKey("a"));

        var act = () => _container.CreateItemAsync(item2, new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task BooleanAndString_SameLiteral_ShouldNotConflict()
    {
        // Boolean true vs string "true" — different types
        var item1 = new JObject { ["id"] = "1", ["pk"] = "a", ["code"] = true };
        var item2 = new JObject { ["id"] = "2", ["pk"] = "a", ["code"] = "true" };

        await _container.CreateItemAsync(item1, new PartitionKey("a"));

        var act = () => _container.CreateItemAsync(item2, new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task IntegerAndFloat_SameNumericValue_ShouldConflict()
    {
        // 1 and 1.0 are the same numeric value — should conflict
        var item1 = new JObject { ["id"] = "1", ["pk"] = "a", ["code"] = 1 };
        var item2 = new JObject { ["id"] = "2", ["pk"] = "a", ["code"] = 1.0 };

        await _container.CreateItemAsync(item1, new PartitionKey("a"));

        var act = () => _container.CreateItemAsync(item2, new PartitionKey("a"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task TwoIdenticalStrings_ShouldConflict()
    {
        var item1 = new JObject { ["id"] = "1", ["pk"] = "a", ["code"] = "hello" };
        var item2 = new JObject { ["id"] = "2", ["pk"] = "a", ["code"] = "hello" };

        await _container.CreateItemAsync(item1, new PartitionKey("a"));

        var act = () => _container.CreateItemAsync(item2, new PartitionKey("a"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }
}

// ─── Composite unique keys (multiple paths in one UniqueKey) ─────────────────

[Collection(IntegrationCollection.Name)]
public class UniqueKeyPolicyCompositeIntegrationTests(EmulatorSession session) : IAsyncLifetime
{
    private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
    private Container _container = null!;

    public async ValueTask InitializeAsync()
    {
        _container = await _fixture.CreateContainerAsync("uk-composite", "/pk",
            configure: props =>
            {
                props.UniqueKeyPolicy = new UniqueKeyPolicy
                {
                    UniqueKeys =
                    {
                        new UniqueKey { Paths = { "/firstName", "/lastName" } }
                    }
                };
            });
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task SameFirstAndLastName_ShouldConflict()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", firstName = "John", lastName = "Doe" }),
            new PartitionKey("a"));

        var act = () => _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", firstName = "John", lastName = "Doe" }),
            new PartitionKey("a"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task SameFirstName_DifferentLastName_ShouldSucceed()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", firstName = "John", lastName = "Doe" }),
            new PartitionKey("a"));

        var act = () => _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", firstName = "John", lastName = "Smith" }),
            new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task BothItemsMissingAllCompositePaths_ShouldNotConflict()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"));

        var act = () => _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a" }),
            new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }
}

// ─── Multiple independent unique keys ───────────────────────────────────────

[Collection(IntegrationCollection.Name)]
public class UniqueKeyPolicyMultipleKeysIntegrationTests(EmulatorSession session) : IAsyncLifetime
{
    private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
    private Container _container = null!;

    public async ValueTask InitializeAsync()
    {
        _container = await _fixture.CreateContainerAsync("uk-multi", "/pk",
            configure: props =>
            {
                props.UniqueKeyPolicy = new UniqueKeyPolicy
                {
                    UniqueKeys =
                    {
                        new UniqueKey { Paths = { "/email" } },
                        new UniqueKey { Paths = { "/username" } }
                    }
                };
            });
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task DuplicateEmail_ShouldConflict()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "a@b.com", username = "user1" }),
            new PartitionKey("a"));

        var act = () => _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", email = "a@b.com", username = "user2" }),
            new PartitionKey("a"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task DuplicateUsername_ShouldConflict()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "a@b.com", username = "user1" }),
            new PartitionKey("a"));

        var act = () => _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", email = "x@y.com", username = "user1" }),
            new PartitionKey("a"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task BothUnique_ShouldSucceed()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "a@b.com", username = "user1" }),
            new PartitionKey("a"));

        var act = () => _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", email = "x@y.com", username = "user2" }),
            new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }
}

// ─── Empty string, unicode, case-sensitivity ────────────────────────────────

[Collection(IntegrationCollection.Name)]
public class UniqueKeyPolicyStringVariantsIntegrationTests(EmulatorSession session) : IAsyncLifetime
{
    private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
    private Container _container = null!;

    public async ValueTask InitializeAsync()
    {
        _container = await _fixture.CreateContainerAsync("uk-strings", "/pk",
            configure: props =>
            {
                props.UniqueKeyPolicy = new UniqueKeyPolicy
                {
                    UniqueKeys = { new UniqueKey { Paths = { "/code" } } }
                };
            });
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task TwoEmptyStrings_ShouldConflict()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", code = "" }),
            new PartitionKey("a"));

        var act = () => _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", code = "" }),
            new PartitionKey("a"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task EmptyStringVsNull_ShouldNotConflict()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", code = "" }),
            new PartitionKey("a"));

        // Second item missing "code" (null)
        var act = () => _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a" }),
            new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task CaseSensitive_DifferentCase_ShouldNotConflict()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", code = "Hello" }),
            new PartitionKey("a"));

        var act = () => _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", code = "hello" }),
            new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task UnicodeValues_Identical_ShouldConflict()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", code = "日本語" }),
            new PartitionKey("a"));

        var act = () => _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", code = "日本語" }),
            new PartitionKey("a"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }
}

// ─── TTL interaction with unique keys ───────────────────────────────────────

[Collection(IntegrationCollection.Name)]
public class UniqueKeyPolicyTtlIntegrationTests(EmulatorSession session) : IAsyncLifetime
{
    private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
    private Container _container = null!;

    public async ValueTask InitializeAsync()
    {
        _container = await _fixture.CreateContainerAsync("uk-ttl", "/pk",
            configure: props =>
            {
                props.DefaultTimeToLive = 2;
                props.UniqueKeyPolicy = new UniqueKeyPolicy
                {
                    UniqueKeys = { new UniqueKey { Paths = { "/code" } } }
                };
            });
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task ExpiredItem_ShouldNotBlockNewItemWithSameValue()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", code = "EXPIRE_ME" }),
            new PartitionKey("a"));

        // Wait for TTL to expire
        await Task.Delay(3000);

        // Should succeed — expired item should not block unique key
        var act = () => _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", code = "EXPIRE_ME" }),
            new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }
}

// ─── Upsert self-update (same id) should not conflict ───────────────────────

[Collection(IntegrationCollection.Name)]
public class UniqueKeyPolicyUpsertSelfIntegrationTests(EmulatorSession session) : IAsyncLifetime
{
    private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
    private Container _container = null!;

    public async ValueTask InitializeAsync()
    {
        _container = await _fixture.CreateContainerAsync("uk-upsert-self", "/pk",
            configure: props =>
            {
                props.UniqueKeyPolicy = new UniqueKeyPolicy
                {
                    UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
                };
            });
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task UpsertSameItem_SameValue_ShouldSucceed()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "test@test.com" }),
            new PartitionKey("a"));

        // Upsert same id with same unique value — should succeed (self-update)
        var act = () => _container.UpsertItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "test@test.com", name = "updated" }),
            new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task UpsertSameItem_ChangedUniqueValue_ShouldSucceed()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "old@test.com" }),
            new PartitionKey("a"));

        // Upsert same id with new unique value — should succeed
        var act = () => _container.UpsertItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "new@test.com" }),
            new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }
}
