using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json;
using Xunit;
using System.Text;
using Newtonsoft.Json.Linq;

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


public class StoredProcGapTests3
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task StoredProc_WithPartitionKey_ExecutesInPartition()
    {
        _container.RegisterStoredProcedure("addItem", (pk, args) =>
        {
            return $"{{\"partition\":\"{pk}\"}}";
        });

        var response = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "addItem", new PartitionKey("pk1"), []);

        response.Should().NotBeNull();
    }
}


public class StoredProcGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task StoredProc_Register_Execute_ReturnsResult()
    {
        _container.RegisterStoredProcedure("addItem", (pk, args) =>
        {
            return "{\"status\":\"ok\"}";
        });

        var response = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
            "addItem", new PartitionKey("pk1"), []);

        response.Should().NotBeNull();
    }

    [Fact]
    public async Task Udf_NotRegistered_ThrowsOnQuery()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var act = async () =>
        {
            var iterator = _container.GetItemQueryIterator<JToken>(
                "SELECT * FROM c WHERE udf.nonExistent(c.value)");
            while (iterator.HasMoreResults)
            {
                await iterator.ReadNextAsync();
            }
        };

        await act.Should().ThrowAsync<Exception>();
    }
}


public class UdfGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Udf_RegisterAndUseInQuery()
    {
        _container.RegisterUdf("double", args => ((double)args[0]) * 2);

        await _container.CreateItemAsync(
            new UdfDocument { Id = "1", PartitionKey = "pk1", Value = 21 },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JToken>(
            "SELECT VALUE udf.double(c.value) FROM c");

        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(1);
    }

    [Fact]
    public async Task Udf_MultipleArgs()
    {
        _container.RegisterUdf("add", args => (double)args[0] + (double)args[1]);

        await _container.CreateItemAsync(
            new UdfDocument { Id = "1", PartitionKey = "pk1", X = 10, Y = 20 },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemQueryIterator<JToken>(
            "SELECT VALUE udf.add(c.x, c.y) FROM c");

        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(1);
    }
}
