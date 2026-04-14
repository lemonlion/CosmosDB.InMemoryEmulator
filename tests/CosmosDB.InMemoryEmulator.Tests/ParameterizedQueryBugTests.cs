using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for parameterized query bugs:
/// 1. IS_NULL(@param) returns false when parameter value is null
/// 2. SUM(IIF(field = @param, 1, 0)) returns 0 with parameterized queries
/// </summary>
public class ParameterizedQueryBugTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private async Task SeedItems()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", partitionKey = "pk1", status = "Completed", amount = 50 }),
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", partitionKey = "pk1", status = "Completed", amount = 30 }),
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "3", partitionKey = "pk1", status = "Failed", amount = 20 }),
            new PartitionKey("pk1"));
    }

    // ── Bug 1: IS_NULL(@param) with null parameter ──────────────────────────

    [Fact]
    public async Task IsNull_WithParameterizedNull_ReturnsTrue()
    {
        await SeedItems();

        var query = new QueryDefinition(
            "SELECT IS_NULL(@status) AS isNullResult FROM c WHERE c.partitionKey = @pk")
            .WithParameter("@pk", "pk1")
            .WithParameter("@status", null);

        var results = await DrainQuery<JObject>(query);

        results.Should().HaveCount(3);
        results[0]["isNullResult"]!.Value<bool>().Should().BeTrue();
    }

    [Fact]
    public async Task IsNull_OptionalFilterPattern_WithNullParam_ReturnsAllItems()
    {
        await SeedItems();

        // Common Cosmos pattern: (IS_NULL(@filter) OR c.field = @filter)
        // When @filter is null, IS_NULL(@filter) should be true, so all items match
        var query = new QueryDefinition(
            "SELECT * FROM c WHERE c.partitionKey = @pk AND (IS_NULL(@status) OR c.status = @status)")
            .WithParameter("@pk", "pk1")
            .WithParameter("@status", null);

        var results = await DrainQuery<JObject>(query);

        results.Should().HaveCount(3);
    }

    [Fact]
    public async Task IsNull_OptionalFilterPattern_WithNonNullParam_FiltersCorrectly()
    {
        await SeedItems();

        var query = new QueryDefinition(
            "SELECT * FROM c WHERE c.partitionKey = @pk AND (IS_NULL(@status) OR c.status = @status)")
            .WithParameter("@pk", "pk1")
            .WithParameter("@status", "Completed");

        var results = await DrainQuery<JObject>(query);

        results.Should().HaveCount(2);
    }

    // ── Bug 2: SUM(IIF(field = @param, 1, 0)) ──────────────────────────────

    [Fact]
    public async Task SumIif_WithParameterizedValue_ReturnsCorrectCount()
    {
        await SeedItems();

        var query = new QueryDefinition(
            "SELECT SUM(IIF(c.status = @completedStatus, 1, 0)) AS filesCompleted FROM c WHERE c.partitionKey = @pk")
            .WithParameter("@pk", "pk1")
            .WithParameter("@completedStatus", "Completed");

        var results = await DrainQuery<JObject>(query);

        results.Should().ContainSingle();
        results[0]["filesCompleted"]!.Value<int>().Should().Be(2);
    }

    [Fact]
    public async Task SumIif_FullAggregation_WithParameters_ReturnsCorrectCounts()
    {
        await SeedItems();

        var query = new QueryDefinition(@"
            SELECT
                COUNT(1) AS totalFiles,
                SUM(c.amount) AS totalAmount,
                SUM(IIF(c.status = @completedStatus, 1, 0)) AS filesCompleted,
                SUM(IIF(c.status = @failedStatus, 1, 0)) AS filesFailed
            FROM c
            WHERE c.partitionKey = @pk")
            .WithParameter("@pk", "pk1")
            .WithParameter("@completedStatus", "Completed")
            .WithParameter("@failedStatus", "Failed");

        var results = await DrainQuery<JObject>(query);

        results.Should().ContainSingle();
        var result = results[0];
        result["totalFiles"]!.Value<int>().Should().Be(3);
        result["totalAmount"]!.Value<int>().Should().Be(100);
        result["filesCompleted"]!.Value<int>().Should().Be(2);
        result["filesFailed"]!.Value<int>().Should().Be(1);
    }

    private async Task<List<T>> DrainQuery<T>(QueryDefinition query)
    {
        var iterator = _container.GetItemQueryIterator<T>(query);
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        return results;
    }
}
