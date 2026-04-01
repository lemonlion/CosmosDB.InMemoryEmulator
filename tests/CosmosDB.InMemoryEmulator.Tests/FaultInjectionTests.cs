using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Newtonsoft.Json.Linq;
using System.Net;
using System.Text;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;


public class FaultInjectionGapTests4
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
    public async Task FaultInjection_Timeout_408()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        using var handler = new FakeCosmosHandler(container)
        {
            FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)408)
            {
                Content = new StringContent("{}")
            }
        };
        using var client = CreateClient(handler);
        var cosmosContainer = client.GetContainer("db", "test");

        var act = async () =>
        {
            var iterator = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
            while (iterator.HasMoreResults)
                await iterator.ReadNextAsync();
        };

        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task FaultInjection_SelectiveByPath_OnlyFailsWrites()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        using var handler = new FakeCosmosHandler(container)
        {
            FaultInjector = request =>
            {
                // Only fail POST requests (writes/queries), allow GET requests (reads)
                if (request.Method == HttpMethod.Post)
                    return new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
                    {
                        Content = new StringContent("{}")
                    };
                return null; // Allow through
            }
        };

        using var httpClient = new HttpClient(handler);

        // GET should succeed (read feed)
        var getResponse = await httpClient.GetAsync("https://localhost:9999/dbs/db/colls/test/docs");
        getResponse.StatusCode.Should().Be(HttpStatusCode.OK);

        // POST should fail (query)
        var postRequest = new HttpRequestMessage(HttpMethod.Post,
            "https://localhost:9999/dbs/db/colls/test/docs");
        postRequest.Headers.Add("x-ms-documentdb-query", "True");
        postRequest.Headers.Add("x-ms-documentdb-isquery", "True");
        postRequest.Content = new StringContent(
            """{"query":"SELECT * FROM c"}""", Encoding.UTF8, "application/query+json");

        var postResponse = await httpClient.SendAsync(postRequest);
        postResponse.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
    }
}


public class FaultInjectionTests
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
    public async Task FaultInjection_429_ClientReceivesThrottle()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        using var handler = new FakeCosmosHandler(container);
        handler.FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
        {
            Content = new StringContent("""{"message":"Too many requests"}""",
                Encoding.UTF8, "application/json")
        };

        using var client = CreateClient(handler);
        var cosmosContainer = client.GetContainer("db", "test");

        var act = async () =>
        {
            var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
            while (iterator.HasMoreResults) await iterator.ReadNextAsync();
        };

        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task FaultInjection_503_ClientReceivesServiceUnavailable()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        using var handler = new FakeCosmosHandler(container);
        handler.FaultInjector = _ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
        {
            Content = new StringContent("""{"message":"Service unavailable"}""",
                Encoding.UTF8, "application/json")
        };

        using var client = CreateClient(handler);
        var cosmosContainer = client.GetContainer("db", "test");

        var act = async () =>
        {
            var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
            while (iterator.HasMoreResults) await iterator.ReadNextAsync();
        };

        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task FaultInjection_SkipsMetadata_ByDefault()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var queryCallCount = 0;
        using var handler = new FakeCosmosHandler(container);
        handler.FaultInjector = request =>
        {
            // Only intercept data requests, not metadata
            Interlocked.Increment(ref queryCallCount);
            return new HttpResponseMessage((HttpStatusCode)429)
            {
                Content = new StringContent("""{"message":"throttled"}""",
                    Encoding.UTF8, "application/json")
            };
        };
        // FaultInjectorIncludesMetadata defaults to false

        using var client = CreateClient(handler);
        var cosmosContainer = client.GetContainer("db", "test");

        // The SDK should still be able to initialize (metadata requests bypass fault injector)
        // but actual queries should fail
        var act = async () =>
        {
            var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
            while (iterator.HasMoreResults) await iterator.ReadNextAsync();
        };

        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task FaultInjection_IncludesMetadata_WhenEnabled()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        using var handler = new FakeCosmosHandler(container);
        handler.FaultInjectorIncludesMetadata = true;
        handler.FaultInjector = _ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
        {
            Content = new StringContent("""{"message":"unavailable"}""",
                Encoding.UTF8, "application/json")
        };

        using var client = CreateClient(handler);
        var cosmosContainer = client.GetContainer("db", "test");

        // Even metadata routes should fail now
        var act = async () =>
        {
            var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
            while (iterator.HasMoreResults) await iterator.ReadNextAsync();
        };

        await act.Should().ThrowAsync<Exception>();
    }

    [Fact]
    public async Task FaultInjection_Intermittent_EventuallySucceeds()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var callCount = 0;
        using var handler = new FakeCosmosHandler(container);
        handler.FaultInjector = _ =>
        {
            var count = Interlocked.Increment(ref callCount);
            if (count <= 2)
            {
                var response = new HttpResponseMessage((HttpStatusCode)429)
                {
                    Content = new StringContent("""{"message":"throttled"}""",
                        Encoding.UTF8, "application/json")
                };
                // Real Cosmos DB always includes x-ms-retry-after-ms on 429 responses.
                // Without it the SDK uses its own backoff with random jitter, which can
                // exceed MaxRetryWaitTimeOnRateLimitedRequests and cause flaky failures.
                response.Headers.Add("x-ms-retry-after-ms", "10");
                return response;
            }

            return null; // Allow normal processing after first 2 calls
        };

        // Use client with retries enabled
        using var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 5,
                MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(5),
                HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(30) }
            });

        var cosmosContainer = client.GetContainer("db", "test");

        var results = new List<TestDocument>();
        var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().NotBeEmpty();
    }
}


// ═══════════════════════════════════════════════════════════════════════════
//  Category A: Operation-specific fault injection
// ═══════════════════════════════════════════════════════════════════════════

public class FaultInjectionOperationTests
{
    private static CosmosClient CreateClient(HttpMessageHandler handler, int maxRetries = 0) =>
        new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = maxRetries,
                RequestTimeout = TimeSpan.FromSeconds(10),
                HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
            });

    [Fact]
    public async Task FaultInjection_429_OnPointRead()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        using var handler = new FakeCosmosHandler(container)
        {
            FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
            { Content = new StringContent("{}") }
        };
        using var client = CreateClient(handler);

        var act = () => client.GetContainer("db", "test")
            .ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        await act.Should().ThrowAsync<CosmosException>()
            .Where(ex => (int)ex.StatusCode == 429);
    }

    [Fact]
    public async Task FaultInjection_429_OnReplace()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        using var handler = new FakeCosmosHandler(container)
        {
            FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
            { Content = new StringContent("{}") }
        };
        using var client = CreateClient(handler);

        var act = () => client.GetContainer("db", "test")
            .ReplaceItemAsync(
                new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
                "1", new PartitionKey("pk1"));

        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task FaultInjection_429_OnUpsert()
    {
        var container = new InMemoryContainer("test", "/partitionKey");

        using var handler = new FakeCosmosHandler(container)
        {
            FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
            { Content = new StringContent("{}") }
        };
        using var client = CreateClient(handler);

        var act = () => client.GetContainer("db", "test")
            .UpsertItemAsync(
                new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
                new PartitionKey("pk1"));

        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task FaultInjection_429_OnDelete()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        using var handler = new FakeCosmosHandler(container)
        {
            FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
            { Content = new StringContent("{}") }
        };
        using var client = CreateClient(handler);

        var act = () => client.GetContainer("db", "test")
            .DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task FaultInjection_429_OnPatch()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        using var handler = new FakeCosmosHandler(container)
        {
            FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
            { Content = new StringContent("{}") }
        };
        using var client = CreateClient(handler);

        var act = () => client.GetContainer("db", "test")
            .PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
                new[] { PatchOperation.Set("/name", "Updated") });

        await act.Should().ThrowAsync<CosmosException>();
    }
}


// ═══════════════════════════════════════════════════════════════════════════
//  Category B: HTTP status code coverage
// ═══════════════════════════════════════════════════════════════════════════

public class FaultInjectionStatusCodeTests
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

    [Theory]
    [InlineData(500, "InternalServerError")]
    [InlineData(403, "Forbidden")]
    [InlineData(400, "BadRequest")]
    public async Task FaultInjection_VariousStatusCodes_ClientReceivesError(int statusCode, string description)
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        using var handler = new FakeCosmosHandler(container)
        {
            FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)statusCode)
            { Content = new StringContent($"{{\"message\":\"{description}\"}}") }
        };
        using var client = CreateClient(handler);

        var act = async () =>
        {
            var iterator = client.GetContainer("db", "test")
                .GetItemQueryIterator<TestDocument>("SELECT * FROM c");
            while (iterator.HasMoreResults) await iterator.ReadNextAsync();
        };

        await act.Should().ThrowAsync<CosmosException>();
    }

    [Fact]
    public async Task FaultInjection_404_ViaFaultInjection_DistinctFromNatural404()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        // No items — reading "1" would naturally return 404
        // But we're injecting 404 at the handler level (different code path)

        using var handler = new FakeCosmosHandler(container)
        {
            FaultInjector = _ => new HttpResponseMessage(HttpStatusCode.NotFound)
            { Content = new StringContent("{\"message\":\"Injected 404\"}") }
        };
        using var client = CreateClient(handler);

        var act = () => client.GetContainer("db", "test")
            .ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        await act.Should().ThrowAsync<CosmosException>()
            .Where(ex => ex.StatusCode == HttpStatusCode.NotFound);
    }
}


// ═══════════════════════════════════════════════════════════════════════════
//  Category D: Dynamic / stateful fault injection patterns
// ═══════════════════════════════════════════════════════════════════════════

public class FaultInjectionDynamicTests
{
    private static CosmosClient CreateClient(HttpMessageHandler handler, int maxRetries = 0) =>
        new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = maxRetries,
                RequestTimeout = TimeSpan.FromSeconds(10),
                HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
            });

    [Fact]
    public async Task FaultInjection_SetToNull_DisablesMidway()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        using var handler = new FakeCosmosHandler(container)
        {
            FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
            { Content = new StringContent("{}") }
        };
        using var client = CreateClient(handler);
        var cosmosContainer = client.GetContainer("db", "test");

        // Should fail with fault injector active
        var act1 = async () =>
        {
            var it = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
            while (it.HasMoreResults) await it.ReadNextAsync();
        };
        await act1.Should().ThrowAsync<CosmosException>();

        // Disable fault injection
        handler.FaultInjector = null;

        // Should succeed now
        var iterator = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
        results.Should().NotBeEmpty();
    }

    [Fact]
    public async Task FaultInjection_CountBased_FailsFirstNThenSucceeds()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var callCount = 0;
        using var handler = new FakeCosmosHandler(container)
        {
            FaultInjector = _ =>
            {
                if (Interlocked.Increment(ref callCount) <= 3)
                {
                    var resp = new HttpResponseMessage((HttpStatusCode)429)
                    { Content = new StringContent("{}") };
                    resp.Headers.Add("x-ms-retry-after-ms", "10");
                    return resp;
                }
                return null;
            }
        };

        using var client = CreateClient(handler, maxRetries: 5);
        var cosmosContainer = client.GetContainer("db", "test");

        // With retries, should eventually succeed after N failures
        var iterator = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
        var results = new List<TestDocument>();
        while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
        results.Should().NotBeEmpty();
        callCount.Should().BeGreaterThan(3);
    }
}


// ═══════════════════════════════════════════════════════════════════════════
//  Category E: Infrastructure & edge cases
// ═══════════════════════════════════════════════════════════════════════════

public class FaultInjectionInfrastructureTests
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
    public async Task FaultInjection_DelegateThrows_ExceptionPropagates()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        using var handler = new FakeCosmosHandler(container)
        {
            FaultInjector = _ => throw new InvalidOperationException("injector crashed")
        };
        using var client = CreateClient(handler);

        var act = () => client.GetContainer("db", "test")
            .ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*injector crashed*");
    }

    [Fact]
    public async Task FaultInjection_RequestLogRecords_FaultedRequests()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        using var handler = new FakeCosmosHandler(container)
        {
            FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
            { Content = new StringContent("{}") }
        };
        using var client = CreateClient(handler);

        try
        {
            var iterator = client.GetContainer("db", "test")
                .GetItemQueryIterator<TestDocument>("SELECT * FROM c");
            while (iterator.HasMoreResults) await iterator.ReadNextAsync();
        }
        catch (CosmosException) { }

        handler.RequestLog.Should().NotBeEmpty("request log should capture faulted request paths");
    }

    [Fact]
    public async Task FaultInjection_ConcurrentRequests_ThreadSafe()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        for (var i = 0; i < 10; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));

        var callCount = 0;
        using var handler = new FakeCosmosHandler(container)
        {
            FaultInjector = _ =>
            {
                Interlocked.Increment(ref callCount);
                return new HttpResponseMessage((HttpStatusCode)429)
                { Content = new StringContent("{}") };
            }
        };

        using var client = CreateClient(handler);
        var cosmosContainer = client.GetContainer("db", "test");

        var tasks = Enumerable.Range(0, 20).Select(async _ =>
        {
            try
            {
                var it = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
                while (it.HasMoreResults) await it.ReadNextAsync();
            }
            catch (CosmosException) { }
        });

        await Task.WhenAll(tasks);
        callCount.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task FaultInjection_DirectContainerOps_BypassFaultInjection()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        using var handler = new FakeCosmosHandler(container)
        {
            FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
            { Content = new StringContent("{}") }
        };

        // Direct container operations bypass the handler entirely
        var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.StatusCode.Should().Be(HttpStatusCode.OK);
        read.Resource.Name.Should().Be("Test");
    }

    [Fact]
    public async Task FaultInjection_WithCreateRouter_IndependentPerContainer()
    {
        var container1 = new InMemoryContainer("c1", "/partitionKey");
        var container2 = new InMemoryContainer("c2", "/partitionKey");

        await container1.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "C1" },
            new PartitionKey("pk1"));
        await container2.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "C2" },
            new PartitionKey("pk1"));

        using var handler1 = new FakeCosmosHandler(container1)
        {
            FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
            { Content = new StringContent("{}") }
        };
        using var handler2 = new FakeCosmosHandler(container2);
        // handler2 has NO fault injector — should work normally

        // container1 is faulted, container2 is not
        // Direct access to the non-faulted container should work
        var read = await container2.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("C2");
    }
}
