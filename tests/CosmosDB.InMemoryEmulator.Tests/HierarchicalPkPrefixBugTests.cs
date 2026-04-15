using System.Net;
using CosmosDB.InMemoryEmulator;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;
using AwesomeAssertions;

namespace CosmosDB.InMemoryEmulator.Tests;

public class HierarchicalPkPrefixBugTests
{
    public class HierarchicalDoc
    {
        [JsonProperty("id")] public string Id { get; set; } = "";
        [JsonProperty("level1")] public string Level1 { get; set; } = "";
        [JsonProperty("level2")] public string Level2 { get; set; } = "";
        [JsonProperty("level3")] public string Level3 { get; set; } = "";
        [JsonProperty("data")] public string Data { get; set; } = "";
    }

    [Fact]
    public async Task PrefixQuery_SingleComponent_ReturnsMatchingItems_Direct()
    {
        var container = new InMemoryContainer("test", new[] { "/level1", "/level2", "/level3" });

        var fullPk1 = new PartitionKeyBuilder().Add("a").Add("b").Add("c").Build();
        var fullPk2 = new PartitionKeyBuilder().Add("a").Add("x").Add("y").Build();
        var fullPk3 = new PartitionKeyBuilder().Add("z").Add("b").Add("c").Build();

        await container.CreateItemAsync(new HierarchicalDoc { Id = "1", Level1 = "a", Level2 = "b", Level3 = "c", Data = "match1" }, fullPk1);
        await container.CreateItemAsync(new HierarchicalDoc { Id = "2", Level1 = "a", Level2 = "x", Level3 = "y", Data = "match2" }, fullPk2);
        await container.CreateItemAsync(new HierarchicalDoc { Id = "3", Level1 = "z", Level2 = "b", Level3 = "c", Data = "nomatch" }, fullPk3);

        // Query with 1-component prefix PK
        var prefixPk = new PartitionKeyBuilder().Add("a").Build();
        var iterator = container.GetItemQueryIterator<HierarchicalDoc>(
            "SELECT * FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = prefixPk });

        var results = new List<HierarchicalDoc>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        results.Select(r => r.Id).Should().BeEquivalentTo(["1", "2"]);
    }

    [Fact]
    public async Task PrefixQuery_TwoComponents_ReturnsMatchingItems_Direct()
    {
        var container = new InMemoryContainer("test", new[] { "/level1", "/level2", "/level3" });

        var fullPk1 = new PartitionKeyBuilder().Add("a").Add("b").Add("c").Build();
        var fullPk2 = new PartitionKeyBuilder().Add("a").Add("b").Add("z").Build();
        var fullPk3 = new PartitionKeyBuilder().Add("a").Add("x").Add("y").Build();

        await container.CreateItemAsync(new HierarchicalDoc { Id = "1", Level1 = "a", Level2 = "b", Level3 = "c", Data = "match1" }, fullPk1);
        await container.CreateItemAsync(new HierarchicalDoc { Id = "2", Level1 = "a", Level2 = "b", Level3 = "z", Data = "match2" }, fullPk2);
        await container.CreateItemAsync(new HierarchicalDoc { Id = "3", Level1 = "a", Level2 = "x", Level3 = "y", Data = "nomatch" }, fullPk3);

        // Query with 2-component prefix PK
        var prefixPk = new PartitionKeyBuilder().Add("a").Add("b").Build();
        var iterator = container.GetItemQueryIterator<HierarchicalDoc>(
            "SELECT * FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = prefixPk });

        var results = new List<HierarchicalDoc>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        results.Select(r => r.Id).Should().BeEquivalentTo(["1", "2"]);
    }

    [Fact]
    public async Task PrefixQuery_ViaFakeCosmosHandler_Works()
    {
        var inMemoryContainer = new InMemoryContainer("test", new[] { "/level1", "/level2", "/level3" });
        var handler = new FakeCosmosHandler(inMemoryContainer);

        using var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                HttpClientFactory = () => new HttpClient(handler)
            });

        var container = client.GetContainer("db", "test");

        var fullPk1 = new PartitionKeyBuilder().Add("a").Add("b").Add("c").Build();
        var fullPk2 = new PartitionKeyBuilder().Add("a").Add("x").Add("y").Build();
        var fullPk3 = new PartitionKeyBuilder().Add("z").Add("b").Add("c").Build();

        await container.CreateItemAsync(new HierarchicalDoc { Id = "1", Level1 = "a", Level2 = "b", Level3 = "c", Data = "match1" }, fullPk1);
        await container.CreateItemAsync(new HierarchicalDoc { Id = "2", Level1 = "a", Level2 = "x", Level3 = "y", Data = "match2" }, fullPk2);
        await container.CreateItemAsync(new HierarchicalDoc { Id = "3", Level1 = "z", Level2 = "b", Level3 = "c", Data = "nomatch" }, fullPk3);

        // Query with prefix PK through FakeCosmosHandler
        var prefixPk = new PartitionKeyBuilder().Add("a").Build();
        var query = new QueryDefinition("SELECT * FROM c WHERE c.level1 = @l1").WithParameter("@l1", "a");
        var iterator = container.GetItemQueryIterator<HierarchicalDoc>(
            query,
            requestOptions: new QueryRequestOptions { PartitionKey = prefixPk });

        var results = new List<HierarchicalDoc>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        results.Select(r => r.Id).Should().BeEquivalentTo(["1", "2"]);
    }

    /// <summary>
    /// GitHub issue #6: prefix PK scoping broken via FakeCosmosHandler + CosmosClient path.
    /// Direct InMemoryContainer works, but the SDK path returns all documents.
    /// </summary>
    [Fact]
    public async Task Issue6_TwoLevelPrefix_ViaFakeCosmosHandler_ScopesCorrectly()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
        var handler = new FakeCosmosHandler(inMemoryContainer);
        using var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                HttpClientFactory = () => new HttpClient(handler)
            });

        var container = client.GetContainer("test-db", "items");

        var tenantId = "tenant-001";
        var region = "eu-west";

        await container.CreateItemAsync(
            new { id = "id-1", tenantId, category = "CategoryA", region, data = "data A" },
            new PartitionKeyBuilder().Add(tenantId).Add("CategoryA").Add(region).Build());

        await container.CreateItemAsync(
            new { id = "id-2", tenantId, category = "CategoryB", region, data = "data B" },
            new PartitionKeyBuilder().Add(tenantId).Add("CategoryB").Add(region).Build());

        // Query scoped to 2-level prefix (tenantId + "CategoryA")
        var prefixPk = new PartitionKeyBuilder().Add(tenantId).Add("CategoryA").Build();
        var requestOptions = new QueryRequestOptions { PartitionKey = prefixPk };

        List<JObject> results;
        using (handler.WithPartitionKey(prefixPk))
        {
            results = new List<JObject>();
            var iterator = container.GetItemQueryIterator<JObject>(
                new QueryDefinition("SELECT * FROM c"), requestOptions: requestOptions);
            while (iterator.HasMoreResults)
            {
                var page = await iterator.ReadNextAsync();
                results.AddRange(page);
            }
        }

        results.Should().HaveCount(1);
        results[0]["category"]!.Value<string>().Should().Be("CategoryA");
    }

    [Fact]
    public async Task Issue6_OneLevelPrefix_ViaFakeCosmosHandler_ScopesCorrectly()
    {
        var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
        var handler = new FakeCosmosHandler(inMemoryContainer);
        using var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                HttpClientFactory = () => new HttpClient(handler)
            });

        var container = client.GetContainer("test-db", "items");

        var region = "eu-west";

        await container.CreateItemAsync(
            new { id = "id-a", tenantId = "tenant-A", category = "CategoryA", region, data = "data A" },
            new PartitionKeyBuilder().Add("tenant-A").Add("CategoryA").Add(region).Build());

        await container.CreateItemAsync(
            new { id = "id-b", tenantId = "tenant-B", category = "CategoryA", region, data = "data B" },
            new PartitionKeyBuilder().Add("tenant-B").Add("CategoryA").Add(region).Build());

        // Query scoped to 1-level prefix (tenant-A only)
        var prefixPk = new PartitionKeyBuilder().Add("tenant-A").Build();
        var requestOptions = new QueryRequestOptions { PartitionKey = prefixPk };

        List<JObject> results;
        using (handler.WithPartitionKey(prefixPk))
        {
            results = new List<JObject>();
            var iterator = container.GetItemQueryIterator<JObject>(
                new QueryDefinition("SELECT * FROM c"), requestOptions: requestOptions);
            while (iterator.HasMoreResults)
            {
                var page = await iterator.ReadNextAsync();
                results.AddRange(page);
            }
        }

        results.Should().HaveCount(1);
        results[0]["tenantId"]!.Value<string>().Should().Be("tenant-A");
        results[0]["tenantId"]!.Value<string>().Should().Be("tenant-A");
    }
}
