using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class IifFunctionTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private async Task SeedItems()
    {
        var items = new[]
        {
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, IsActive = true, Tags = ["dot", "net"] },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, IsActive = false, Tags = ["java"] },
            new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 0, IsActive = true, Tags = [] },
        };
        foreach (var item in items)
        {
            await _container.CreateItemAsync(item, new PartitionKey(item.PartitionKey));
        }
    }

    [Fact]
    public async Task Iif_TrueCondition_ReturnsSecondArgument()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IIF(c.isActive, 'yes', 'no') AS result FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.ToString().Should().Be("yes");
    }

    [Fact]
    public async Task Iif_FalseCondition_ReturnsThirdArgument()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IIF(c.isActive, 'yes', 'no') AS result FROM c WHERE c.id = '2'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.ToString().Should().Be("no");
    }

    [Fact]
    public async Task Iif_WithComparisonExpression_EvaluatesCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IIF(c.value > 15, 'high', 'low') AS level FROM c ORDER BY c.id");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(3);
        results[0]["level"]!.ToString().Should().Be("low");   // value=10
        results[1]["level"]!.ToString().Should().Be("high");  // value=20
        results[2]["level"]!.ToString().Should().Be("low");   // value=0
    }

    [Fact]
    public async Task Iif_WithNumericReturnValues_ReturnsNumbers()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IIF(c.isActive, 1, 0) AS flag FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["flag"]!.Value<long>().Should().Be(1);
    }

    [Fact]
    public async Task Iif_InWhereClause_FiltersCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE IIF(c.isActive, c.value, 0) > 5");

        var results = await QueryAll<TestDocument>(query);

        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Alice");
    }

    [Fact]
    public async Task Iif_NestedIif_EvaluatesCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IIF(c.value > 15, 'high', IIF(c.value > 5, 'medium', 'low')) AS level FROM c ORDER BY c.id");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(3);
        results[0]["level"]!.ToString().Should().Be("medium"); // value=10
        results[1]["level"]!.ToString().Should().Be("high");   // value=20
        results[2]["level"]!.ToString().Should().Be("low");    // value=0
    }

    private async Task<List<T>> QueryAll<T>(QueryDefinition query)
    {
        var iterator = _container.GetItemQueryIterator<T>(query);
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }
        return results;
    }
}
