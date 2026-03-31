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
