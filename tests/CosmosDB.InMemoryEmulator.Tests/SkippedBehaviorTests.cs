using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class SkippedBehaviorTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task RequestCharge_ShouldBeNonZero_OnEveryResponse()
    {
        var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" };
        var response = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));
        response.RequestCharge.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task ContinuationToken_ShouldEnablePaginatedResumption()
    {
        for (var i = 0; i < 10; i++)
        {
            await _container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));
        }

        var requestOptions = new QueryRequestOptions { MaxItemCount = 3 };
        var iterator = _container.GetItemQueryIterator<TestDocument>("SELECT * FROM c", requestOptions: requestOptions);

        var firstPage = await iterator.ReadNextAsync();
        firstPage.Count.Should().Be(3);
        firstPage.ContinuationToken.Should().NotBeNullOrEmpty();

        var iterator2 = _container.GetItemQueryIterator<TestDocument>("SELECT * FROM c",
            continuationToken: firstPage.ContinuationToken, requestOptions: requestOptions);
        var secondPage = await iterator2.ReadNextAsync();
        secondPage.Count.Should().Be(3);
    }

    [Fact]
    public async Task LargeDocument_ShouldBeRejected_Over2MB()
    {
        var largeValue = new string('x', 3 * 1024 * 1024);
        var doc = new TestDocument { Id = "large", PartitionKey = "pk1", Name = largeValue };

        var act = () => _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task TimeToLive_ShouldAutoDeleteExpiredDocuments()
    {
        _container.DefaultTimeToLive = 1;
        var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temporary" };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        await Task.Delay(TimeSpan.FromSeconds(2));

        var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task StoredProcedure_ShouldExecuteServerSideLogic()
    {
        _container.RegisterStoredProcedure("sprocId", (pk, args) => "executed");
        var response = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "sprocId", new PartitionKey("pk1"), Array.Empty<dynamic>());
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Should().Be("executed");
    }

    [Fact]
    public async Task PreTrigger_ShouldFireOnCreate()
    {
        _container.RegisterTrigger("validateInsert", TriggerType.Pre, TriggerOperation.Create,
            doc => { doc["triggered"] = true; return doc; });

        var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" };
        var options = new ItemRequestOptions { PreTriggers = new List<string> { "validateInsert" } };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"), options);

        var result = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
        result.Resource["triggered"]!.Value<bool>().Should().BeTrue();
    }

    [Fact]
    public async Task UserDefinedFunction_ShouldBeCallableInQuery()
    {
        _container.RegisterUdf("tax", args => (double)(long)args[0] * 0.2);

        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 100 },
            new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT udf.tax(c.value) AS taxAmount FROM c");
        var iterator = _container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();
        response.Should().ContainSingle();
        response.First()["taxAmount"]!.Value<double>().Should().Be(20.0);
    }

    [Fact]
    public async Task IndexingPolicy_ShouldBeStoredOnContainer()
    {
        var updatedProperties = new ContainerProperties("test-container", "/partitionKey")
        {
            IndexingPolicy = new IndexingPolicy
            {
                Automatic = true,
                IndexingMode = IndexingMode.Consistent
            }
        };

        var replaceResponse = await _container.ReplaceContainerAsync(updatedProperties);
        replaceResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        replaceResponse.Resource.IndexingPolicy.Should().NotBeNull();
        replaceResponse.Resource.IndexingPolicy.Automatic.Should().BeTrue();
    }

    [Fact]
    public async Task CrossPartitionOrderBy_ShouldSortAcrossPartitions()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Bravo", Value = 20 },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk2", Name = "Alpha", Value = 10 },
            new PartitionKey("pk2"));

        var query = new QueryDefinition("SELECT * FROM c ORDER BY c.value ASC");
        var requestOptions = new QueryRequestOptions { MaxItemCount = 1 };
        var iterator = _container.GetItemQueryIterator<TestDocument>(query, requestOptions: requestOptions);

        var allPages = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            allPages.AddRange(page);
        }

        allPages.Should().HaveCount(2);
        allPages[0].Value.Should().Be(10);
        allPages[1].Value.Should().Be(20);
    }

    [Fact]
    public async Task ConflictResolution_ShouldBeStoredOnContainer()
    {
        var readResponse = await _container.ReadContainerAsync();
        readResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        readResponse.Resource.Should().NotBeNull();

        var updatedProperties = new ContainerProperties("test-container", "/partitionKey")
        {
            ConflictResolutionPolicy = new ConflictResolutionPolicy
            {
                Mode = ConflictResolutionMode.LastWriterWins,
                ResolutionPath = "/_ts"
            }
        };
        var replaceResponse = await _container.ReplaceContainerAsync(updatedProperties);
        replaceResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        replaceResponse.Resource.ConflictResolutionPolicy.Mode.Should().Be(ConflictResolutionMode.LastWriterWins);
    }

    [Fact]
    public async Task SessionToken_ShouldBePresentOnResponses()
    {
        var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" };
        var response = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));
        response.Headers.Session.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task MaxItemCount_ShouldLimitPageSize()
    {
        for (var i = 0; i < 10; i++)
        {
            await _container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));
        }

        var requestOptions = new QueryRequestOptions { MaxItemCount = 3 };
        var iterator = _container.GetItemQueryIterator<TestDocument>("SELECT * FROM c", requestOptions: requestOptions);

        var firstPage = await iterator.ReadNextAsync();
        firstPage.Count.Should().BeLessThanOrEqualTo(3);
    }

    [Fact]
    public async Task StreamResponseHeaders_ShouldContainMetadata()
    {
        var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        using var response = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
        response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
        response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
        response.Headers["ETag"].Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task HierarchicalPartitionKey_ShouldSupportMultipleLevels()
    {
        var container = new InMemoryContainer("hierarchical-test", new[] { "/tenantId", "/userId" });

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", tenantId = "t1", userId = "u1", name = "Alice" }),
            new PartitionKeyBuilder().Add("t1").Add("u1").Build());
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "2", tenantId = "t1", userId = "u2", name = "Bob" }),
            new PartitionKeyBuilder().Add("t1").Add("u2").Build());
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "3", tenantId = "t2", userId = "u1", name = "Charlie" }),
            new PartitionKeyBuilder().Add("t2").Add("u1").Build());

        container.ItemCount.Should().Be(3);

        var result = await container.ReadItemAsync<JObject>("1",
            new PartitionKeyBuilder().Add("t1").Add("u1").Build());
        result.Resource["name"]!.ToString().Should().Be("Alice");
    }

    [Fact]
    public async Task TransactionalBatch_ShouldRollbackOnFailure()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
        batch.CreateItem(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" });
        batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Duplicate" });

        using var response = await batch.ExecuteAsync();

        response.StatusCode.Should().NotBe(HttpStatusCode.OK);

        var act = () => _container.ReadItemAsync<TestDocument>("2", new PartitionKey("pk1"));
        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task TransactionalBatch_ShouldRejectOver100Operations()
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
    public async Task Like_SingleCharWildcard_ShouldMatchSingleCharacter()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "cat" },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "cut" },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "coat" },
            new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT * FROM c WHERE c.name LIKE 'c_t'");
        var iterator = _container.GetItemQueryIterator<TestDocument>(query);
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(2);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Request Charge Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class RequestChargeEdgeCaseTests
{
    private readonly InMemoryContainer _container = new("rc-test", "/partitionKey");

    [Fact]
    public async Task RequestCharge_ShouldBe1_OnRead()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

        var response = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        response.RequestCharge.Should().Be(1.0);
    }

    [Fact]
    public async Task RequestCharge_ShouldBe1_OnReplace()
    {
        var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        doc.Name = "B";
        var response = await _container.ReplaceItemAsync(doc, "1", new PartitionKey("pk1"));
        response.RequestCharge.Should().Be(1.0);
    }

    [Fact]
    public async Task RequestCharge_ShouldBe1_OnDelete()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

        var response = await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        response.RequestCharge.Should().Be(1.0);
    }

    [Fact]
    public async Task RequestCharge_ShouldBe1_OnPatch()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

        var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
            new[] { PatchOperation.Set("/name", "B") });
        response.RequestCharge.Should().Be(1.0);
    }

    [Fact]
    public async Task RequestCharge_ShouldBe1_OnQuery()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

        var iter = _container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
        var response = await iter.ReadNextAsync();
        response.RequestCharge.Should().Be(1.0);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Continuation Token Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class ContinuationTokenEdgeCaseTests
{
    private readonly InMemoryContainer _container = new("ct-test", "/partitionKey");

    private async Task SeedItemsAsync(int count)
    {
        for (var i = 0; i < count; i++)
            await _container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}", Value = i },
                new PartitionKey("pk1"));
    }

    [Fact]
    public async Task ContinuationToken_WithOrderBy_ShouldPreserveOrder()
    {
        await SeedItemsAsync(6);

        var opts = new QueryRequestOptions { MaxItemCount = 2 };
        var iter = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c ORDER BY c.value ASC", requestOptions: opts);

        var all = new List<TestDocument>();
        while (iter.HasMoreResults)
        {
            var page = await iter.ReadNextAsync();
            all.AddRange(page);
            if (page.ContinuationToken != null)
            {
                iter = _container.GetItemQueryIterator<TestDocument>(
                    "SELECT * FROM c ORDER BY c.value ASC",
                    continuationToken: page.ContinuationToken, requestOptions: opts);
            }
        }

        all.Select(d => d.Value).Should().BeInAscendingOrder();
    }

    [Fact]
    public async Task ContinuationToken_EmptyResults_ShouldReturnNull()
    {
        var iter = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c WHERE c.name = 'nonexistent'");
        var page = await iter.ReadNextAsync();

        page.Should().BeEmpty();
        page.ContinuationToken.Should().BeNull();
    }

    [Fact]
    public async Task ContinuationToken_FullIteration_CollectsAllItems()
    {
        await SeedItemsAsync(7);

        var opts = new QueryRequestOptions { MaxItemCount = 3 };
        var iter = _container.GetItemQueryIterator<TestDocument>("SELECT * FROM c", requestOptions: opts);

        var all = new List<TestDocument>();
        while (iter.HasMoreResults)
        {
            var page = await iter.ReadNextAsync();
            all.AddRange(page);
        }

        all.Should().HaveCount(7);
    }

    [Fact]
    public async Task ContinuationToken_LastPage_ShouldReturnNull()
    {
        await SeedItemsAsync(3);

        var opts = new QueryRequestOptions { MaxItemCount = 10 };
        var iter = _container.GetItemQueryIterator<TestDocument>("SELECT * FROM c", requestOptions: opts);
        var page = await iter.ReadNextAsync();

        page.Should().HaveCount(3);
        page.ContinuationToken.Should().BeNull();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  TTL Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class TtlSkippedBehaviorEdgeCaseTests
{
    [Fact]
    public async Task ItemTtl_MinusOne_OverridesContainerDefault_NoExpiration()
    {
        var container = new InMemoryContainer("ttl-minus1", "/pk");
        container.DefaultTimeToLive = 1;

        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","_ttl":-1}""")),
            new PartitionKey("a"));

        await Task.Delay(TimeSpan.FromSeconds(2));

        using var response = await container.ReadItemStreamAsync("1", new PartitionKey("a"));
        response.StatusCode.Should().Be(HttpStatusCode.OK,
            "_ttl:-1 should override container default and prevent expiration");
    }

    [Fact]
    public async Task Ttl_ZeroContainerDefault_NoExpiration()
    {
        var container = new InMemoryContainer("ttl-zero", "/pk");
        container.DefaultTimeToLive = 0;

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "a", Name = "A" }, new PartitionKey("a"));

        await Task.Delay(TimeSpan.FromSeconds(1));

        // DefaultTimeToLive=0 is treated as off → no expiration
        var response = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("a"));
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Session Token Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class SessionTokenEdgeCaseTests
{
    [Fact]
    public async Task SessionToken_ShouldBePresent_OnReadResponse()
    {
        var container = new InMemoryContainer("st-test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

        var response = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        response.Headers.Session.Should().NotBeNullOrEmpty();
    }

    [Fact(Skip = "Stream responses do not include x-ms-session-token header in the emulator. " +
        "Typed responses expose Session via Headers.Session, but the raw header is not set on stream responses.")]
    public async Task SessionToken_ShouldBePresent_OnStreamResponse()
    {
        var container = new InMemoryContainer("st-stream", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

        using var response = await container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
        response.Headers["x-ms-session-token"].Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task Divergent_SessionToken_NotPresentOnStreamResponse()
    {
        // Sister test: stream responses don't include x-ms-session-token header
        var container = new InMemoryContainer("st-stream-div", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

        using var response = await container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
        response.Headers["x-ms-session-token"].Should().BeNull(
            "emulator does not set session token on stream responses");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  MaxItemCount Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class MaxItemCountEdgeCaseTests
{
    private readonly InMemoryContainer _container = new("mi-test", "/partitionKey");

    [Fact]
    public async Task MaxItemCount_One_ShouldReturnOnePerPage()
    {
        for (var i = 0; i < 5; i++)
            await _container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"I{i}" },
                new PartitionKey("pk1"));

        var opts = new QueryRequestOptions { MaxItemCount = 1 };
        var iter = _container.GetItemQueryIterator<TestDocument>("SELECT * FROM c", requestOptions: opts);
        var page = await iter.ReadNextAsync();

        page.Count.Should().Be(1);
    }

    [Fact]
    public async Task MaxItemCount_GreaterThanTotal_ShouldReturnAll()
    {
        for (var i = 0; i < 3; i++)
            await _container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"I{i}" },
                new PartitionKey("pk1"));

        var opts = new QueryRequestOptions { MaxItemCount = 100 };
        var iter = _container.GetItemQueryIterator<TestDocument>("SELECT * FROM c", requestOptions: opts);
        var page = await iter.ReadNextAsync();

        page.Count.Should().Be(3);
        page.ContinuationToken.Should().BeNull();
    }

    [Fact]
    public async Task MaxItemCount_WithOrderBy_ShouldPaginateCorrectly()
    {
        for (var i = 0; i < 5; i++)
            await _container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"I{i}", Value = 5 - i },
                new PartitionKey("pk1"));

        var opts = new QueryRequestOptions { MaxItemCount = 2 };
        var iter = _container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c ORDER BY c.value ASC", requestOptions: opts);

        var all = new List<TestDocument>();
        while (iter.HasMoreResults)
        {
            var page = await iter.ReadNextAsync();
            all.AddRange(page);
        }

        all.Select(d => d.Value).Should().BeInAscendingOrder();
        all.Should().HaveCount(5);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Hierarchical PK Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class HierarchicalPkEdgeCaseTests
{
    [Fact]
    public async Task HierarchicalPK_PointReadWithWrongSubKey_ShouldReturn404()
    {
        var container = new InMemoryContainer("hp-404", new[] { "/tenantId", "/userId" });
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", tenantId = "t1", userId = "u1", name = "Alice" }),
            new PartitionKeyBuilder().Add("t1").Add("u1").Build());

        var act = () => container.ReadItemAsync<JObject>("1",
            new PartitionKeyBuilder().Add("t1").Add("u999").Build());
        await act.Should().ThrowAsync<CosmosException>()
            .Where(e => e.StatusCode == HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task HierarchicalPK_ThreeLevels_ShouldWork()
    {
        var container = new InMemoryContainer("hp-3level", new[] { "/a", "/b", "/c" });

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", a = "x", b = "y", c = "z", name = "deep" }),
            new PartitionKeyBuilder().Add("x").Add("y").Add("z").Build());

        var result = await container.ReadItemAsync<JObject>("1",
            new PartitionKeyBuilder().Add("x").Add("y").Add("z").Build());
        result.Resource["name"]!.ToString().Should().Be("deep");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Stream Header Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class StreamHeaderEdgeCaseTests
{
    [Fact]
    public async Task StreamHeaders_OnNotFound_ShouldContainActivityId()
    {
        var container = new InMemoryContainer("sh-test", "/partitionKey");

        using var response = await container.ReadItemStreamAsync("nonexistent", new PartitionKey("pk1"));
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task StreamHeaders_OnConflict_ShouldContainActivityId()
    {
        var container = new InMemoryContainer("sh-409", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

        using var ms = new MemoryStream(Encoding.UTF8.GetBytes(
            """{"id":"1","partitionKey":"pk1","name":"Dup"}"""));
        using var response = await container.CreateItemStreamAsync(ms, new PartitionKey("pk1"));
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Cross-Partition ORDER BY Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class CrossPartitionOrderByEdgeCaseTests
{
    [Fact]
    public async Task CrossPartitionOrderBy_DESC_ShouldSortCorrectly()
    {
        var container = new InMemoryContainer("cp-desc", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A", Value = 10 },
            new PartitionKey("pk1"));
        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk2", Name = "B", Value = 30 },
            new PartitionKey("pk2"));
        await container.CreateItemAsync(
            new TestDocument { Id = "3", PartitionKey = "pk3", Name = "C", Value = 20 },
            new PartitionKey("pk3"));

        var iter = container.GetItemQueryIterator<TestDocument>(
            "SELECT * FROM c ORDER BY c.value DESC");
        var all = new List<TestDocument>();
        while (iter.HasMoreResults)
            all.AddRange(await iter.ReadNextAsync());

        all.Select(d => d.Value).Should().BeInDescendingOrder();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Conflict Resolution Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class ConflictResolutionEdgeCaseTests
{
    [Fact]
    public async Task ConflictResolution_CustomMode_ShouldStoreSprocLink()
    {
        var container = new InMemoryContainer("cr-sproc", "/pk");
        var props = new ContainerProperties("cr-sproc", "/pk")
        {
            ConflictResolutionPolicy = new ConflictResolutionPolicy
            {
                Mode = ConflictResolutionMode.Custom,
                ResolutionProcedure = "dbs/myDb/colls/myCol/sprocs/resolver"
            }
        };

        await container.ReplaceContainerAsync(props);
        var read = await container.ReadContainerAsync();

        read.Resource.ConflictResolutionPolicy.Mode.Should().Be(ConflictResolutionMode.Custom);
        read.Resource.ConflictResolutionPolicy.ResolutionProcedure.Should().Be(
            "dbs/myDb/colls/myCol/sprocs/resolver");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Skipped Tests with Divergent Behavior Sisters
// ═══════════════════════════════════════════════════════════════════════════

public class SkippedBehaviorDivergentTests
{
    [Fact(Skip = "Conflict resolution policy is stored but not enforced. " +
        "The in-memory emulator is single-instance/single-region — no write conflicts can arise. " +
        "Implementing conflict resolution would require simulating multi-region replication.")]
    public void ConflictResolution_ShouldResolveConflicts_AtRuntime()
    {
        // Real Cosmos DB applies conflict resolution policies during multi-region replication.
        // The emulator stores the policy but never invokes it.
    }

    [Fact]
    public async Task Divergent_ConflictResolution_PolicyStoredButNotEnforced()
    {
        // Sister test: policy is stored on the container but has no runtime effect
        var container = new InMemoryContainer("cr-div", "/pk");
        var props = new ContainerProperties("cr-div", "/pk")
        {
            ConflictResolutionPolicy = new ConflictResolutionPolicy
            {
                Mode = ConflictResolutionMode.LastWriterWins,
                ResolutionPath = "/_ts"
            }
        };

        await container.ReplaceContainerAsync(props);

        // Two writes with the same id — normal 409 Conflict (not LWW resolution)
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
        await act.Should().ThrowAsync<CosmosException>()
            .Where(e => e.StatusCode == HttpStatusCode.Conflict);
    }

    [Fact(Skip = "Request charges are always synthetic (1.0 RU). " +
        "Real Cosmos DB computes RU based on document size, index utilization, and query complexity. " +
        "Implementing RU estimation would require replicating the proprietary Cosmos DB cost model.")]
    public void RequestCharge_ShouldReflectActualRUConsumption()
    {
        // Would need the actual Cosmos DB cost model which is proprietary.
    }

    [Fact]
    public async Task Divergent_RequestCharge_IsAlwaysSynthetic_1RU()
    {
        // Sister test: all ops return exactly 1.0 RU
        var container = new InMemoryContainer("ru-div", "/partitionKey");
        var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" };
        var create = await container.CreateItemAsync(doc, new PartitionKey("pk1"));
        var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        var query = await _container_GetQuery(container);

        create.RequestCharge.Should().Be(1.0);
        read.RequestCharge.Should().Be(1.0);
        query.RequestCharge.Should().Be(1.0);
    }

    private static async Task<FeedResponse<TestDocument>> _container_GetQuery(InMemoryContainer container)
    {
        var iter = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
        return await iter.ReadNextAsync();
    }

    [Fact(Skip = "Continuation tokens are simple integer offsets. " +
        "Real Cosmos DB uses opaque base64-encoded JSON strings with internal cursor state. " +
        "Implementing opaque tokens adds complexity without functional benefit for testing.")]
    public void ContinuationToken_ShouldBeOpaqueBase64()
    {
        // Real Cosmos DB continuation tokens are opaque base64 JSON.
        // Emulator uses plain integer offsets (e.g. "3", "10").
    }

    [Fact]
    public async Task Divergent_ContinuationToken_IsPlainIntegerOffset()
    {
        // Sister test: token is a plain integer
        var container = new InMemoryContainer("ct-div", "/partitionKey");
        for (var i = 0; i < 5; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"I{i}" },
                new PartitionKey("pk1"));

        var opts = new QueryRequestOptions { MaxItemCount = 2 };
        var iter = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c", requestOptions: opts);
        var page = await iter.ReadNextAsync();

        page.ContinuationToken.Should().NotBeNull();
        int.TryParse(page.ContinuationToken, out _).Should().BeTrue(
            "emulator uses plain integer offsets as continuation tokens");
    }
}
