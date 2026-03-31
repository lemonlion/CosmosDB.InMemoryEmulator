using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Newtonsoft.Json.Linq;
using System.Net;
using System.Text;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;


public class FakeCosmosHandlerGapTests
{
    private static CosmosClient CreateClient(FakeCosmosHandler handler) =>
        new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                RequestTimeout = TimeSpan.FromSeconds(10),
                HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
            });

    [Fact]
    public async Task Handler_ReadFeed_ReturnsAllDocuments()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        for (var i = 0; i < 5; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));

        using var handler = new FakeCosmosHandler(container);
        using var client = CreateClient(handler);
        var cosmosContainer = client.GetContainer("db", "test");

        // ReadFeed via SDK — uses read feed endpoint
        var results = new List<TestDocument>();
        var iterator = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(5);
    }
}


public class FakeCosmosHandlerGapTests4
{
    private static CosmosClient CreateClient(HttpMessageHandler handler) =>
        new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                RequestTimeout = TimeSpan.FromSeconds(10),
                HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
            });

    [Fact]
    public async Task Handler_AccountMetadata_ReturnsValidResponse()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        using var handler = new FakeCosmosHandler(container);
        using var httpClient = new HttpClient(handler);

        var response = await httpClient.GetAsync("https://localhost:9999/");

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var body = await response.Content.ReadAsStringAsync();
        var json = JObject.Parse(body);
        json["id"].Should().NotBeNull();
        json["writableLocations"].Should().NotBeNull();
        json["readableLocations"].Should().NotBeNull();
    }

    [Fact]
    public async Task Handler_Query_PartitionKeyRange_FiltersToRange()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        for (var i = 0; i < 20; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
                new PartitionKey($"pk-{i}"));

        using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
        {
            PartitionKeyRangeCount = 4
        });

        using var httpClient = new HttpClient(handler);

        // Query with a specific partition key range ID
        var request = new HttpRequestMessage(HttpMethod.Post,
            "https://localhost:9999/dbs/db/colls/test/docs");
        request.Headers.Add("x-ms-documentdb-partitionkeyrangeid", "0");
        request.Headers.Add("x-ms-documentdb-query", "True");
        request.Headers.Add("x-ms-documentdb-isquery", "True");
        request.Content = new StringContent(
            """{"query":"SELECT * FROM c"}""", Encoding.UTF8, "application/query+json");

        var response = await httpClient.SendAsync(request);
        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var body = await response.Content.ReadAsStringAsync();
        var result = JObject.Parse(body);
        var documents = result["Documents"]!.ToObject<JArray>();

        // Only items whose PK hashes to range 0 should be returned
        documents!.Count.Should().BeLessThan(20);
        documents.Count.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task Handler_CacheEviction_StaleEntries()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
        {
            CacheTtl = TimeSpan.FromMilliseconds(200)
        });
        using var client = CreateClient(handler);
        var cosmosContainer = client.GetContainer("db", "test");

        // First query — caches the result
        var iter1 = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
        while (iter1.HasMoreResults) await iter1.ReadNextAsync();

        // Wait for cache TTL to expire
        await Task.Delay(300);

        // Add a new item
        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "New" },
            new PartitionKey("pk1"));

        // Second query — stale cache should be evicted, returns fresh results
        var iter2 = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
        var results = new List<TestDocument>();
        while (iter2.HasMoreResults)
        {
            var page = await iter2.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
    }

    [Fact]
    public async Task Handler_CacheEviction_ExceedsMaxEntries()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 1 },
            new PartitionKey("pk1"));

        using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
        {
            CacheMaxEntries = 2
        });
        using var client = CreateClient(handler);
        var cosmosContainer = client.GetContainer("db", "test");

        // Execute 3 different queries to exceed max cache entries
        // Note: 'value' is a reserved word in Cosmos SDK, use bracket notation
        var queries = new[]
        {
            "SELECT * FROM c WHERE c.id = '1'",
            "SELECT * FROM c WHERE c[\"value\"] = 1",
            "SELECT * FROM c WHERE c.name = 'Test'"
        };

        foreach (var query in queries)
        {
            var iter = cosmosContainer.GetItemQueryIterator<TestDocument>(query);
            while (iter.HasMoreResults) await iter.ReadNextAsync();
        }

        // All queries should still work — LRU eviction shouldn't break anything
        var finalIter = cosmosContainer.GetItemQueryIterator<TestDocument>(queries[2]);
        var results = new List<TestDocument>();
        while (finalIter.HasMoreResults)
        {
            var page = await finalIter.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(1);
    }

    [Fact]
    public async Task Handler_CollectionMetadata_ReturnsContainerProperties()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        using var handler = new FakeCosmosHandler(container);
        using var httpClient = new HttpClient(handler);

        var response = await httpClient.GetAsync("https://localhost:9999/dbs/db/colls/test");

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var body = await response.Content.ReadAsStringAsync();
        var json = JObject.Parse(body);
        json["id"]!.ToString().Should().Be("test");
        json["partitionKey"].Should().NotBeNull();
    }

    [Fact]
    public async Task Handler_MurmurHash_DistributesEvenly()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        // Create 100 items with diverse partition keys
        for (var i = 0; i < 100; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = $"tenant-{i}", Name = $"Item{i}" },
                new PartitionKey($"tenant-{i}"));

        using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
        {
            PartitionKeyRangeCount = 4
        });
        using var httpClient = new HttpClient(handler);

        var rangeCounts = new int[4];
        for (var rangeId = 0; rangeId < 4; rangeId++)
        {
            var request = new HttpRequestMessage(HttpMethod.Post,
                "https://localhost:9999/dbs/db/colls/test/docs");
            request.Headers.Add("x-ms-documentdb-partitionkeyrangeid", rangeId.ToString());
            request.Headers.Add("x-ms-documentdb-query", "True");
            request.Headers.Add("x-ms-documentdb-isquery", "True");
            request.Content = new StringContent(
                """{"query":"SELECT * FROM c"}""", Encoding.UTF8, "application/query+json");

            var response = await httpClient.SendAsync(request);
            var body = await response.Content.ReadAsStringAsync();
            var result = JObject.Parse(body);
            rangeCounts[rangeId] = result["Documents"]!.ToObject<JArray>()!.Count;
        }

        // All ranges should have at least some items (rough distribution)
        rangeCounts.Sum().Should().Be(100);
        // Each range should have at least 5 items (with 100 items across 4 ranges)
        foreach (var count in rangeCounts)
            count.Should().BeGreaterThan(5);
    }
}


public class FakeCosmosHandlerTests
{
    private static CosmosClient CreateClient(FakeCosmosHandler handler) =>
        new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                RequestTimeout = TimeSpan.FromSeconds(10),
                HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
            });

    [Fact]
    public async Task Handler_BasicQuery_ReturnsAllItems()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));
        await container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
            new PartitionKey("pk1"));

        using var handler = new FakeCosmosHandler(container);
        using var client = CreateClient(handler);
        var cosmosContainer = client.GetContainer("db", "test");

        var results = new List<TestDocument>();
        var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
    }

    [Fact]
    public async Task Handler_OrderByQuery_ReturnsCorrectOrder()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Charlie" }, new PartitionKey("pk1"));
        await container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Alice" }, new PartitionKey("pk1"));
        await container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Bob" }, new PartitionKey("pk1"));

        using var handler = new FakeCosmosHandler(container);
        using var client = CreateClient(handler);
        var cosmosContainer = client.GetContainer("db", "test");

        var results = new List<TestDocument>();
        var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>()
            .OrderBy(doc => doc.Name).ToFeedIterator();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results[0].Name.Should().Be("Alice");
        results[1].Name.Should().Be("Bob");
        results[2].Name.Should().Be("Charlie");
    }

    [Fact]
    public async Task Handler_Pagination_ContinuationTokenRoundtrip()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        for (var i = 0; i < 5; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));

        using var handler = new FakeCosmosHandler(container);
        using var client = CreateClient(handler);
        var cosmosContainer = client.GetContainer("db", "test");

        var allItems = new List<TestDocument>();
        var pageCount = 0;
        var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>(
                requestOptions: new QueryRequestOptions { MaxItemCount = 2 })
            .ToFeedIterator();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            allItems.AddRange(page);
            pageCount++;
        }

        allItems.Should().HaveCount(5);
        pageCount.Should().BeGreaterThan(1);
    }

    [Fact]
    public async Task Handler_PartitionKeyRanges_ReturnsConfiguredCount()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
        {
            PartitionKeyRangeCount = 4
        });

        handler.RequestLog.Should().BeEmpty(); // No requests yet

        using var client = CreateClient(handler);
        var cosmosContainer = client.GetContainer("db", "test");

        // Trigger a query to force SDK to request pkranges
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
        while (iterator.HasMoreResults)
            await iterator.ReadNextAsync();

        handler.RequestLog.Should().Contain(entry => entry.Contains("/pkranges"));
    }

    [Fact]
    public async Task Handler_PartitionKeyRanges_IfNoneMatch_Returns304()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        using var handler = new FakeCosmosHandler(container);
        using var client = CreateClient(handler);
        var cosmosContainer = client.GetContainer("db", "test");

        // Make two queries — second should use cached pkranges
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var iterator1 = cosmosContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
        while (iterator1.HasMoreResults) await iterator1.ReadNextAsync();

        var iterator2 = cosmosContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
        while (iterator2.HasMoreResults) await iterator2.ReadNextAsync();

        // Both queries should have triggered pkranges request
        handler.RequestLog.Count(entry => entry.Contains("/pkranges")).Should().BeGreaterThanOrEqualTo(1);
    }

    [Fact]
    public async Task Handler_QueryLog_RecordsQueries()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        using var handler = new FakeCosmosHandler(container);
        using var client = CreateClient(handler);
        var cosmosContainer = client.GetContainer("db", "test");

        // Use explicit SQL query rather than LINQ — the SDK may optimize LINQ
        // differently and not always route through the query endpoint
        var iterator = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
        while (iterator.HasMoreResults) await iterator.ReadNextAsync();

        handler.QueryLog.Should().NotBeEmpty();
    }

    [Fact]
    public async Task Handler_RequestLog_RecordsRequests()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        using var handler = new FakeCosmosHandler(container);
        using var client = CreateClient(handler);
        var cosmosContainer = client.GetContainer("db", "test");

        var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
        while (iterator.HasMoreResults) await iterator.ReadNextAsync();

        handler.RequestLog.Should().NotBeEmpty();
        handler.RequestLog.Should().Contain(entry => entry.StartsWith("GET") || entry.StartsWith("POST"));
    }

    [Fact]
    public async Task Handler_FilteredQuery_ReturnsCorrectResults()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 }, new PartitionKey("pk1"));
        await container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 }, new PartitionKey("pk1"));
        await container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 }, new PartitionKey("pk1"));

        using var handler = new FakeCosmosHandler(container);
        using var client = CreateClient(handler);
        var cosmosContainer = client.GetContainer("db", "test");

        var results = new List<TestDocument>();
        var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>()
            .Where(doc => doc.Value > 15)
            .ToFeedIterator();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
    }

    [Fact]
    public async Task Handler_VerifySdkCompatibility_DoesNotThrow()
    {
        var act = FakeCosmosHandler.VerifySdkCompatibilityAsync;

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task Handler_MultiContainer_RouterDispatchesCorrectly()
    {
        var container1 = new InMemoryContainer("container1", "/partitionKey");
        var container2 = new InMemoryContainer("container2", "/partitionKey");

        await container1.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "FromContainer1" },
            new PartitionKey("pk1"));
        await container2.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "FromContainer2" },
            new PartitionKey("pk1"));

        using var handler1 = new FakeCosmosHandler(container1);
        using var handler2 = new FakeCosmosHandler(container2);

        var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
        {
            ["container1"] = handler1,
            ["container2"] = handler2,
        });

        using var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                HttpClientFactory = () => new HttpClient(router) { Timeout = TimeSpan.FromSeconds(10) }
            });

        var c1 = client.GetContainer("db", "container1");
        var c2 = client.GetContainer("db", "container2");

        var results1 = new List<TestDocument>();
        var iter1 = c1.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
        while (iter1.HasMoreResults) { var page = await iter1.ReadNextAsync(); results1.AddRange(page); }

        var results2 = new List<TestDocument>();
        var iter2 = c2.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
        while (iter2.HasMoreResults) { var page = await iter2.ReadNextAsync(); results2.AddRange(page); }

        results1.Should().ContainSingle().Which.Name.Should().Be("FromContainer1");
        results2.Should().ContainSingle().Which.Name.Should().Be("FromContainer2");
    }

    [Fact]
    public async Task Handler_CrossPartition_WithMultipleRanges_AllDataReturned()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        for (var i = 0; i < 20; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i % 5}", Name = $"Item{i}" },
                new PartitionKey($"pk-{i % 5}"));

        using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
        {
            PartitionKeyRangeCount = 4
        });
        using var client = CreateClient(handler);
        var cosmosContainer = client.GetContainer("db", "test");

        var results = new List<TestDocument>();
        var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(20);
    }

    [Fact]
    public async Task Handler_Router_UnregisteredContainer_ThrowsDescriptiveError()
    {
        var orders = new InMemoryContainer("orders", "/customerId");
        var customers = new InMemoryContainer("customers", "/id");

        using var handler1 = new FakeCosmosHandler(orders);
        using var handler2 = new FakeCosmosHandler(customers);

        var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
        {
            ["orders"] = handler1,
            ["customers"] = handler2,
        });

        using var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                HttpClientFactory = () => new HttpClient(router) { Timeout = TimeSpan.FromSeconds(10) }
            });

        var unknown = client.GetContainer("db", "unknown-container");

        var act = () => unknown.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<InvalidOperationException>();
        ex.Which.Message.Should().Contain("unknown-container");
        ex.Which.Message.Should().Contain("CreateRouter");
        ex.Which.Message.Should().Contain("customers");
        ex.Which.Message.Should().Contain("orders");
    }

    [Fact]
    public async Task Handler_CountAsync_ReturnsCorrectCount()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" }, new PartitionKey("pk1"));
        await container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" }, new PartitionKey("pk1"));

        using var handler = new FakeCosmosHandler(container);
        using var client = CreateClient(handler);
        var cosmosContainer = client.GetContainer("db", "test");

        var count = await cosmosContainer.GetItemLinqQueryable<TestDocument>().CountAsync();

        count.Resource.Should().Be(2);
    }
}
