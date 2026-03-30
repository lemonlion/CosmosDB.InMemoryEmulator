using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class StoredProcedureTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task CreateStoredProcedure_ReturnsCreated()
    {
        var scripts = _container.Scripts;

        var response = await scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
        {
            Id = "spBulkDelete",
            Body = "function() { return true; }"
        });

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Resource.Id.Should().Be("spBulkDelete");
    }

    [Fact]
    public async Task ExecuteStoredProcedure_ReturnsOk()
    {
        var scripts = _container.Scripts;

        var response = await scripts.ExecuteStoredProcedureAsync<string>(
            "spBulkDelete",
            new PartitionKey("pk1"),
            Array.Empty<dynamic>());

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task ExecuteStoredProcedure_WithRegisteredHandler_ExecutesLogic()
    {
        _container.RegisterStoredProcedure("spGetCount", (partitionKey, args) =>
        {
            return "42";
        });

        var scripts = _container.Scripts;
        var response = await scripts.ExecuteStoredProcedureAsync<string>(
            "spGetCount",
            new PartitionKey("pk1"),
            Array.Empty<dynamic>());

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Should().Be("42");
    }

    [Fact]
    public async Task ExecuteStoredProcedure_WithRegisteredHandler_ReceivesArguments()
    {
        _container.RegisterStoredProcedure("spConcat", (partitionKey, args) =>
        {
            return string.Join("-", args.Select(a => a?.ToString()));
        });

        var scripts = _container.Scripts;
        var response = await scripts.ExecuteStoredProcedureAsync<string>(
            "spConcat",
            new PartitionKey("pk1"),
            new dynamic[] { "hello", "world" });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Should().Be("hello-world");
    }

    [Fact]
    public async Task ExecuteStoredProcedure_WithoutRegisteredHandler_ReturnsDefault()
    {
        var scripts = _container.Scripts;
        var response = await scripts.ExecuteStoredProcedureAsync<string>(
            "nonExistentSproc",
            new PartitionKey("pk1"),
            Array.Empty<dynamic>());

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task RegisterStoredProcedure_WithContainerAccess_CanReadItems()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new PartitionKey("pk1"));

        _container.RegisterStoredProcedure("spSum", (partitionKey, args) =>
        {
            var query = new QueryDefinition("SELECT VALUE SUM(c.value) FROM c");
            var iterator = _container.GetItemQueryIterator<double>(query);
            var total = 0.0;
            while (iterator.HasMoreResults)
            {
                var response = iterator.ReadNextAsync().GetAwaiter().GetResult();
                foreach (var val in response)
                {
                    total += val;
                }
            }
            return JsonConvert.SerializeObject((int)total);
        });

        var scripts = _container.Scripts;
        var result = await scripts.ExecuteStoredProcedureAsync<string>(
            "spSum",
            new PartitionKey("pk1"),
            Array.Empty<dynamic>());

        result.StatusCode.Should().Be(HttpStatusCode.OK);
        result.Resource.Should().Be("30");
    }

    [Fact]
    public async Task DeregisterStoredProcedure_RemovesHandler()
    {
        _container.RegisterStoredProcedure("spTemp", (partitionKey, args) => "result");
        _container.DeregisterStoredProcedure("spTemp");

        var scripts = _container.Scripts;
        var response = await scripts.ExecuteStoredProcedureAsync<string>(
            "spTemp",
            new PartitionKey("pk1"),
            Array.Empty<dynamic>());

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Should().BeEmpty();
    }
}
