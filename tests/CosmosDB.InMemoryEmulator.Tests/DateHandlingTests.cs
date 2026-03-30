using System;
using System.Net;
using System.Threading.Tasks;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
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
