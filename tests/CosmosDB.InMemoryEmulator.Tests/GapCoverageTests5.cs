using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════
//  Gap Coverage Tests 5 — Undocumented feature gaps
//  TDD: tests written RED first, then implementation added
// ═══════════════════════════════════════════════════════════════════════════

// ─── 1. Unique Key Policy Enforcement ────────────────────────────────────

public class UniqueKeyPolicyTests
{
    [Fact]
    public async Task CreateItem_ViolatesUniqueKey_ThrowsConflict()
    {
        var properties = new ContainerProperties("unique-ctr", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        var container = new InMemoryContainer(properties);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));

        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task CreateItem_SameUniqueKey_DifferentPartition_Succeeds()
    {
        var properties = new ContainerProperties("unique-ctr", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        var container = new InMemoryContainer(properties);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));

        // Same email but different partition — should succeed (unique keys are per-partition)
        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "b", email = "alice@test.com" }),
            new PartitionKey("b"));

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task CreateItem_CompositeUniqueKey_ViolatesBoth_ThrowsConflict()
    {
        var properties = new ContainerProperties("unique-ctr", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/firstName", "/lastName" } } }
            }
        };
        var container = new InMemoryContainer(properties);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", firstName = "Alice", lastName = "Smith" }),
            new PartitionKey("a"));

        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", firstName = "Alice", lastName = "Smith" }),
            new PartitionKey("a"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task CreateItem_CompositeUniqueKey_OnlyOneDiffers_Succeeds()
    {
        var properties = new ContainerProperties("unique-ctr", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/firstName", "/lastName" } } }
            }
        };
        var container = new InMemoryContainer(properties);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", firstName = "Alice", lastName = "Smith" }),
            new PartitionKey("a"));

        // Different lastName — should succeed
        var act = () => container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", firstName = "Alice", lastName = "Jones" }),
            new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task UpsertItem_ViolatesUniqueKey_OfDifferentItem_ThrowsConflict()
    {
        var properties = new ContainerProperties("unique-ctr", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        var container = new InMemoryContainer(properties);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));

        // Upsert a DIFFERENT item with same email — should conflict
        var act = () => container.UpsertItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task UpsertItem_SameItem_UpdatesWithoutConflict()
    {
        var properties = new ContainerProperties("unique-ctr", "/pk")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };
        var container = new InMemoryContainer(properties);

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "alice@test.com" }),
            new PartitionKey("a"));

        // Upsert same item (id=1) with same email — should succeed (updating self)
        var act = () => container.UpsertItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", email = "alice@test.com", name = "updated" }),
            new PartitionKey("a"));

        await act.Should().NotThrowAsync();
    }
}

// ─── 2. DateTimeDiff ─────────────────────────────────────────────────────

public class DateTimeDiffTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Theory]
    [InlineData("day", "2020-01-01T00:00:00Z", "2020-01-10T00:00:00Z", 9)]
    [InlineData("hour", "2020-01-01T00:00:00Z", "2020-01-01T05:00:00Z", 5)]
    [InlineData("minute", "2020-01-01T00:00:00Z", "2020-01-01T00:30:00Z", 30)]
    [InlineData("second", "2020-01-01T00:00:00Z", "2020-01-01T00:00:45Z", 45)]
    [InlineData("millisecond", "2020-01-01T00:00:00.000Z", "2020-01-01T00:00:00.500Z", 500)]
    [InlineData("year", "2020-01-15T00:00:00Z", "2023-06-15T00:00:00Z", 3)]
    [InlineData("month", "2020-01-15T00:00:00Z", "2020-04-15T00:00:00Z", 3)]
    public async Task DateTimeDiff_ReturnsCorrectDifference(string part, string start, string end, long expected)
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", startDate = start, endDate = end }),
            new PartitionKey("a"));

        var query = new QueryDefinition(
            $"SELECT VALUE DateTimeDiff('{part}', c.startDate, c.endDate) FROM c");

        var iterator = _container.GetItemQueryIterator<long>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<long>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle().Which.Should().Be(expected);
    }

    [Fact]
    public async Task DateTimeDiff_NegativeDiff_ReturnsNegative()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", startDate = "2020-01-10T00:00:00Z", endDate = "2020-01-01T00:00:00Z" }),
            new PartitionKey("a"));

        var query = new QueryDefinition(
            "SELECT VALUE DateTimeDiff('day', c.startDate, c.endDate) FROM c");

        var iterator = _container.GetItemQueryIterator<long>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<long>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle().Which.Should().Be(-9);
    }
}

// ─── 3. Aggregate without GROUP BY (verify existing behavior) ────────────

public class AggregateWithoutGroupByTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task SelectValueCount_ReturnsExactCount()
    {
        for (var i = 0; i < 5; i++)
        {
            await _container.CreateItemAsync(
                JObject.FromObject(new { id = $"{i}", pk = "a" }),
                new PartitionKey("a"));
        }

        var iterator = _container.GetItemQueryIterator<long>(
            "SELECT VALUE COUNT(1) FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<long>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle().Which.Should().Be(5);
    }

    [Fact]
    public async Task SelectValueSum_ReturnsTotalSum()
    {
        for (var i = 1; i <= 4; i++)
        {
            await _container.CreateItemAsync(
                JObject.FromObject(new { id = $"{i}", pk = "a", value = i * 10 }),
                new PartitionKey("a"));
        }

        var iterator = _container.GetItemQueryIterator<double>(
            "SELECT VALUE SUM(c.value) FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<double>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle().Which.Should().Be(100);
    }
}

// ─── 4. LIKE with ESCAPE ────────────────────────────────────────────────

public class LikeWithEscapeTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task Like_WithEscapeClause_MatchesLiteralPercent()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", code = "50% off" }),
            new PartitionKey("a"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", code = "50 items" }),
            new PartitionKey("a"));

        // In real Cosmos: LIKE '50!% off' ESCAPE '!' matches only "50% off"
        var query = new QueryDefinition(
            "SELECT * FROM c WHERE c.code LIKE '50!% off' ESCAPE '!'");

        var iterator = _container.GetItemQueryIterator<JObject>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle();
        results[0]["id"]!.Value<string>().Should().Be("1");
    }
}

// ─── 5. DateTimeFromParts ───────────────────────────────────────────────

public class DateTimeFromPartsTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task DateTimeFromParts_ReturnsFormattedDateTime()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"));

        var query = new QueryDefinition(
            "SELECT VALUE DateTimeFromParts(2020, 3, 15, 10, 30, 0, 0) FROM c");

        var iterator = _container.GetItemQueryIterator<string>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<string>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle().Which.Should().Be("2020-03-15T10:30:00.0000000Z");
    }
}

// ─── 6. DateTimeBin ─────────────────────────────────────────────────────

public class DateTimeBinTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task DateTimeBin_BinsByHour()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", ts = "2020-06-15T10:35:22.1234567Z" }),
            new PartitionKey("a"));

        // DateTimeBin(dateTime, datePart, binSize [, origin])
        // Bin by 1 hour: 10:35 → 10:00
        var query = new QueryDefinition(
            "SELECT VALUE DateTimeBin(c.ts, 'hh', 1) FROM c");

        var iterator = _container.GetItemQueryIterator<string>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<string>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle().Which.Should().Be("2020-06-15T10:00:00.0000000Z");
    }

    [Fact]
    public async Task DateTimeBin_BinsByDay()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", ts = "2020-06-15T10:35:22Z" }),
            new PartitionKey("a"));

        // Bin by 1 day: keeps date, zeros time
        var query = new QueryDefinition(
            "SELECT VALUE DateTimeBin(c.ts, 'dd', 1) FROM c");

        var iterator = _container.GetItemQueryIterator<string>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<string>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle().Which.Should().Be("2020-06-15T00:00:00.0000000Z");
    }
}

// ─── 7. DateTime/Ticks Conversion Functions ─────────────────────────────

public class DateTimeTicksConversionTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task GetCurrentTicks_ReturnsReasonableValue()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"));

        var query = new QueryDefinition("SELECT VALUE GetCurrentTicks() FROM c");

        var iterator = _container.GetItemQueryIterator<long>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<long>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        // Ticks for 2020-01-01 = 637134336000000000, should be well above that
        results.Should().ContainSingle().Which.Should().BeGreaterThan(637134336000000000);
    }

    [Fact]
    public async Task DateTimeToTicks_ConvertsCorrectly()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", ts = "2020-01-01T00:00:00.0000000Z" }),
            new PartitionKey("a"));

        var query = new QueryDefinition("SELECT VALUE DateTimeToTicks(c.ts) FROM c");

        var iterator = _container.GetItemQueryIterator<long>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<long>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle().Which.Should().Be(637134336000000000);
    }

    [Fact]
    public async Task TicksToDateTime_ConvertsCorrectly()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", ticks = 637134336000000000 }),
            new PartitionKey("a"));

        var query = new QueryDefinition("SELECT VALUE TicksToDateTime(c.ticks) FROM c");

        var iterator = _container.GetItemQueryIterator<string>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<string>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle().Which.Should().Be("2020-01-01T00:00:00.0000000Z");
    }

    [Fact]
    public async Task DateTimeToTimestamp_ReturnsUnixMillis()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", ts = "2020-01-01T00:00:00.0000000Z" }),
            new PartitionKey("a"));

        var query = new QueryDefinition("SELECT VALUE DateTimeToTimestamp(c.ts) FROM c");

        var iterator = _container.GetItemQueryIterator<long>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<long>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        // 2020-01-01T00:00:00Z = 1577836800000 ms since epoch
        results.Should().ContainSingle().Which.Should().Be(1577836800000);
    }

    [Fact]
    public async Task TimestampToDateTime_ConvertsFromUnixMillis()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", ms = 1577836800000 }),
            new PartitionKey("a"));

        var query = new QueryDefinition("SELECT VALUE TimestampToDateTime(c.ms) FROM c");

        var iterator = _container.GetItemQueryIterator<string>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<string>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle().Which.Should().Be("2020-01-01T00:00:00.0000000Z");
    }
}
