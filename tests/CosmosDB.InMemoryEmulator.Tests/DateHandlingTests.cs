using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class DateDocument
{
    [JsonProperty("id")]
    public string Id { get; set; } = default!;

    [JsonProperty("partitionKey")]
    public string PartitionKey { get; set; } = default!;

    [JsonProperty("eventDate")]
    public DateTimeOffset EventDate { get; set; }
}

public class MetadataDocument
{
    [JsonProperty("id")]
    public string Id { get; set; } = default!;

    [JsonProperty("partitionKey")]
    public string PartitionKey { get; set; } = default!;

    [JsonProperty("metadata")]
    public Dictionary<string, object> Metadata { get; set; } = default!;
}

public class DateHandlingTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task DateTimeOffset_RoundTrips_WithOriginalOffset()
    {
        var original = new DateTimeOffset(2026, 3, 29, 16, 55, 46, TimeSpan.Zero);
        var document = new DateDocument
        {
            Id = "date-test-1",
            PartitionKey = "pk1",
            EventDate = original
        };

        await _container.CreateItemAsync(document, new PartitionKey("pk1"));

        var response = await _container.ReadItemAsync<DateDocument>("date-test-1", new PartitionKey("pk1"));

        response.Resource.EventDate.Should().Be(original);
    }

    [Fact]
    public async Task DateTimeOffset_RoundTrips_WithPositiveOffset()
    {
        var original = new DateTimeOffset(2026, 3, 29, 17, 55, 46, TimeSpan.FromHours(1));
        var document = new DateDocument
        {
            Id = "date-test-2",
            PartitionKey = "pk1",
            EventDate = original
        };

        await _container.CreateItemAsync(document, new PartitionKey("pk1"));

        var response = await _container.ReadItemAsync<DateDocument>("date-test-2", new PartitionKey("pk1"));

        response.Resource.EventDate.Should().Be(original);
    }

    [Fact]
    public async Task DateTimeOffset_PreservedInQuery()
    {
        var original = new DateTimeOffset(2026, 3, 29, 16, 55, 46, TimeSpan.Zero);
        var document = new DateDocument
        {
            Id = "date-test-3",
            PartitionKey = "pk1",
            EventDate = original
        };

        await _container.CreateItemAsync(document, new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT * FROM c WHERE c.id = 'date-test-3'");
        var iterator = _container.GetItemQueryIterator<DateDocument>(query);
        var results = await iterator.ReadNextAsync();

        results.Resource.Should().ContainSingle()
            .Which.EventDate.Should().Be(original);
    }

    [Fact]
    public async Task DateTimeOffset_InDictionary_PreservedAsString()
    {
        var original = new DateTimeOffset(2026, 3, 29, 16, 55, 46, TimeSpan.Zero);
        var document = new MetadataDocument
        {
            Id = "date-test-4",
            PartitionKey = "pk1",
            Metadata = new()
            {
                ["deceasedDate"] = original
            }
        };

        await _container.CreateItemAsync(document, new PartitionKey("pk1"));

        var response = await _container.ReadItemAsync<MetadataDocument>("date-test-4", new PartitionKey("pk1"));

        var returnedValue = response.Resource.Metadata["deceasedDate"].ToString()!;
        var parsed = DateTimeOffset.Parse(returnedValue);
        parsed.Should().Be(original);
    }

    [Fact]
    public async Task DateTime_Utc_InDictionary_RoundTripsViaStreamRead()
    {
        var utcDate = new DateTime(2026, 3, 29, 19, 52, 37, DateTimeKind.Utc);
        var document = new MetadataDocument
        {
            Id = "date-test-stream",
            PartitionKey = "pk1",
            Metadata = new()
            {
                ["deceasedDate"] = utcDate
            }
        };

        await _container.CreateItemAsync(document, new PartitionKey("pk1"));

        // Simulate the framework path: ReadItemStreamAsync → manual deserialization
        var streamResponse = await _container.ReadItemStreamAsync("date-test-stream", new PartitionKey("pk1"));
        using var reader = new System.IO.StreamReader(streamResponse.Content);
        var json = await reader.ReadToEndAsync();

        // Deserialize with default Newtonsoft settings (what the framework does)
        var deserialized = JsonConvert.DeserializeObject<MetadataDocument>(json);
        var dateValue = deserialized!.Metadata["deceasedDate"];

        // Check what type and value we get
        var actualType = dateValue.GetType().Name;
        if (dateValue is DateTime dt)
        {
            dt.Kind.Should().Be(DateTimeKind.Utc, $"DateTime should remain UTC (actual: {dt}, kind: {dt.Kind}, type: {actualType})");
            dt.Should().Be(utcDate);
        }
        else
        {
            // If it's a string (from DateParseHandling.None), verify it parses correctly
            var parsed = DateTime.Parse(dateValue.ToString()!).ToUniversalTime();
            parsed.Should().Be(utcDate, $"Value was {actualType}: {dateValue}");
        }
    }
}

// ─── DateTimeDiff ────────────────────────────────────────────────────────

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

// ─── DateTimeFromParts ──────────────────────────────────────────────────

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

// ─── DateTimeBin ────────────────────────────────────────────────────────

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

// ─── DateTime/Ticks Conversion Functions ────────────────────────────────

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

// ─── Static DateTime Functions ──────────────────────────────────────────

public class StaticDateTimeFunctionTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task GetCurrentDateTimeStatic_ReturnsSameValueForAllItems()
    {
        for (var i = 0; i < 3; i++)
            await _container.CreateItemAsync(
                JObject.FromObject(new { id = $"{i}", pk = "a" }),
                new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<string>(
            "SELECT VALUE GetCurrentDateTimeStatic() FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<string>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().HaveCount(3);
        // All three values should be identical (static = same for entire query)
        results.Distinct().Should().ContainSingle();
    }

    [Fact]
    public async Task GetCurrentTicksStatic_ReturnsSameValueForAllItems()
    {
        for (var i = 0; i < 3; i++)
            await _container.CreateItemAsync(
                JObject.FromObject(new { id = $"{i}", pk = "a" }),
                new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<long>(
            "SELECT VALUE GetCurrentTicksStatic() FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<long>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().HaveCount(3);
        results.Distinct().Should().ContainSingle();
    }

    [Fact]
    public async Task GetCurrentTimestampStatic_ReturnsSameValueForAllItems()
    {
        for (var i = 0; i < 3; i++)
            await _container.CreateItemAsync(
                JObject.FromObject(new { id = $"{i}", pk = "a" }),
                new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<long>(
            "SELECT VALUE GetCurrentTimestampStatic() FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<long>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().HaveCount(3);
        results.Distinct().Should().ContainSingle();
    }
}

// ─── DateTimeBin Year/Month Support ─────────────────────────────────────

public class DateTimeBinYearMonthTests
{
    [Fact]
    public async Task DateTimeBin_Year_BinsToYearBoundary()
    {
        var container = new InMemoryContainer("test-container", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","dt":"2023-07-15T10:30:00.0000000Z"}""")),
            new PartitionKey("a"));

        var iterator = container.GetItemQueryIterator<string>(
            "SELECT VALUE DateTimeBin(c.dt, 'year', 1) FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<string>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        // Binning to 1 year => 2023-01-01T00:00:00.0000000Z
        results.Should().ContainSingle().Which.Should().Be("2023-01-01T00:00:00.0000000Z");
    }

    [Fact]
    public async Task DateTimeBin_Month_BinsToMonthBoundary()
    {
        var container = new InMemoryContainer("test-container", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","dt":"2023-07-15T10:30:00.0000000Z"}""")),
            new PartitionKey("a"));

        var iterator = container.GetItemQueryIterator<string>(
            "SELECT VALUE DateTimeBin(c.dt, 'month', 1) FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<string>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        // Binning to 1 month => 2023-07-01T00:00:00.0000000Z
        results.Should().ContainSingle().Which.Should().Be("2023-07-01T00:00:00.0000000Z");
    }

    [Fact]
    public async Task DateTimeBin_Quarter_BinsTo3MonthBoundary()
    {
        var container = new InMemoryContainer("test-container", "/pk");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","dt":"2023-08-15T10:30:00.0000000Z"}""")),
            new PartitionKey("a"));

        var iterator = container.GetItemQueryIterator<string>(
            "SELECT VALUE DateTimeBin(c.dt, 'month', 3) FROM c",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<string>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        // Binning to 3 months from origin 2001-01-01 =>
        // Aug 2023 is in the Q3 2023 bin starting at 2023-07-01
        results.Should().ContainSingle().Which.Should().Be("2023-07-01T00:00:00.0000000Z");
    }
}
