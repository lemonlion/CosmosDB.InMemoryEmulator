using System.Net;
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
        var scripts = _container.Scripts;
        var response = await scripts.ExecuteStoredProcedureAsync<string>(
            "sprocId", new PartitionKey("pk1"), Array.Empty<dynamic>());
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task PreTrigger_ShouldFireOnCreate()
    {
        var scripts = _container.Scripts;
        var response = await scripts.CreateTriggerAsync(new TriggerProperties
        {
            Id = "validateInsert",
            Body = "function() { /* validation logic */ }",
            TriggerType = TriggerType.Pre,
            TriggerOperation = TriggerOperation.Create,
        });
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task UserDefinedFunction_ShouldBeCallableInQuery()
    {
        var scripts = _container.Scripts;
        var udfResponse = await scripts.CreateUserDefinedFunctionAsync(new UserDefinedFunctionProperties
        {
            Id = "tax",
            Body = "function(income) { return income * 0.2; }"
        });
        udfResponse.StatusCode.Should().Be(HttpStatusCode.Created);

        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 100 },
            new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT udf.tax(c.value) AS taxAmount FROM c");
        var iterator = _container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();
        response.Should().NotBeNull();
        response.Count.Should().Be(1);
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
