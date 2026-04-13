using System.Net;
using CosmosDB.InMemoryEmulator;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
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
}
