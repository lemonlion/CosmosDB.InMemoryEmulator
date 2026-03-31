using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using AwesomeAssertions;
using CosmosDB.InMemoryEmulator.ProductionExtensions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ════════════════════════════════════════════════════════════════════════════════
// Minimal test app for integration tests
// ════════════════════════════════════════════════════════════════════════════════

public record CosmosTestItem(
    [property: JsonPropertyName("id")] string Id,
    [property: JsonPropertyName("partitionKey")] string PartitionKey,
    [property: JsonPropertyName("name")] string Name);

/// <summary>
/// A repository that resolves Container from DI — simulates the BreakfastProvider pattern.
/// </summary>
public class TestRepository
{
    private readonly Container _container;

    public TestRepository(Container container)
    {
        _container = container;
    }

    public async Task<CosmosTestItem> CreateAsync(CosmosTestItem item)
    {
        var response = await _container.CreateItemAsync(item, new PartitionKey(item.PartitionKey));
        return response.Resource;
    }

    public async Task<CosmosTestItem?> GetByIdAsync(string id, string partitionKey)
    {
        try
        {
            var response = await _container.ReadItemAsync<CosmosTestItem>(id, new PartitionKey(partitionKey));
            return response.Resource;
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task<List<CosmosTestItem>> GetAllAsync()
    {
        var iterator = _container.GetItemLinqQueryable<CosmosTestItem>(true)
            .ToFeedIteratorOverridable();

        var results = new List<CosmosTestItem>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        return results;
    }

    public async Task<CosmosTestItem> UpsertAsync(CosmosTestItem item)
    {
        var response = await _container.UpsertItemAsync(item, new PartitionKey(item.PartitionKey));
        return response.Resource;
    }
}

/// <summary>
/// A repository that resolves Container from CosmosClient — simulates Pattern 3 (Acquisition.Api).
/// </summary>
public class ClientResolvedRepository
{
    private readonly Container _container;

    public ClientResolvedRepository(CosmosClient client)
    {
        _container = client.GetContainer("TestDb", "items");
    }

    public async Task<CosmosTestItem> CreateAsync(CosmosTestItem item)
    {
        var response = await _container.CreateItemAsync(item, new PartitionKey(item.PartitionKey));
        return response.Resource;
    }

    public async Task<CosmosTestItem?> GetByIdAsync(string id, string partitionKey)
    {
        try
        {
            var response = await _container.ReadItemAsync<CosmosTestItem>(id, new PartitionKey(partitionKey));
            return response.Resource;
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════════
// Helper to build a TestServer-backed IHost with "production" + "test" services
// ════════════════════════════════════════════════════════════════════════════════

public class TestAppHost : IAsyncDisposable
{
    private IHost? _host;
    private HttpClient? _httpClient;

    public IServiceProvider Services => _host!.Services;
    public HttpClient HttpClient => _httpClient!;

    private TestAppHost() { }

    public static async Task<TestAppHost> CreateAsync(
        Action<IServiceCollection>? configureTestServices = null,
        Action<IServiceCollection>? configureBaseServices = null,
        Action<IEndpointRouteBuilder>? configureEndpoints = null)
    {
        var instance = new TestAppHost();

        var builder = new HostBuilder()
            .ConfigureWebHost(webBuilder =>
            {
                webBuilder.UseTestServer();
                webBuilder.ConfigureServices(services =>
                {
                    services.AddRouting();

                    if (configureBaseServices != null)
                    {
                        configureBaseServices(services);
                    }
                    else
                    {
                        // Default: simulate a real app registering CosmosClient + Container
                        services.AddSingleton<CosmosClient>(_ =>
                            new CosmosClient("AccountEndpoint=https://fail.example.com:9999/;AccountKey=dGVzdA==;"));
                        services.AddSingleton(sp =>
                        {
                            var client = sp.GetRequiredService<CosmosClient>();
                            return client.GetContainer("ProductionDb", "items");
                        });
                        services.AddSingleton<TestRepository>();
                    }
                });

                // Test services override production registrations (same as ConfigureTestServices)
                if (configureTestServices != null)
                {
                    webBuilder.ConfigureServices(configureTestServices);
                }

                webBuilder.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints =>
                    {
                        if (configureEndpoints != null)
                        {
                            configureEndpoints(endpoints);
                        }
                        else
                        {
                            endpoints.MapGet("/items/{id}", async (string id, TestRepository repo) =>
                            {
                                var item = await repo.GetByIdAsync(id, id);
                                return item is not null ? Results.Ok(item) : Results.NotFound();
                            });

                            endpoints.MapPost("/items", async (CosmosTestItem item, TestRepository repo) =>
                            {
                                var created = await repo.CreateAsync(item);
                                return Results.Created($"/items/{created.Id}", created);
                            });

                            endpoints.MapGet("/items", async (TestRepository repo) =>
                            {
                                var items = await repo.GetAllAsync();
                                return Results.Ok(items);
                            });
                        }
                    });
                });
            });

        instance._host = await builder.StartAsync();
        instance._httpClient = instance._host.GetTestClient();
        return instance;
    }

    public async ValueTask DisposeAsync()
    {
        _httpClient?.Dispose();
        if (_host != null)
        {
            await _host.StopAsync();
            _host.Dispose();
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════════
// Phase 4: Integration Tests
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class WebApplicationFactoryIntegrationTests : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

    [Fact]
    public async Task CreateAndReadItem()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
        var client = app.HttpClient;

        var item = new CosmosTestItem("1", "1", "Test");
        var postResponse = await client.PostAsJsonAsync("/items", item);
        postResponse.StatusCode.Should().Be(HttpStatusCode.Created);

        var getResponse = await client.GetAsync("/items/1");
        getResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        var result = await getResponse.Content.ReadFromJsonAsync<CosmosTestItem>(JsonOptions);
        result!.Name.Should().Be("Test");
    }

    [Fact]
    public async Task ReadNotFound()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
        var client = app.HttpClient;

        var response = await client.GetAsync("/items/nonexistent");
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task ListItems()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
        var client = app.HttpClient;

        await client.PostAsJsonAsync("/items", new CosmosTestItem("1", "1", "Alice"));
        await client.PostAsJsonAsync("/items", new CosmosTestItem("2", "2", "Bob"));
        await client.PostAsJsonAsync("/items", new CosmosTestItem("3", "3", "Charlie"));

        var response = await client.GetAsync("/items");
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var items = await response.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
        items.Should().HaveCount(3);
    }

    [Fact]
    public async Task LinqQueryWorks()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
        var client = app.HttpClient;

        await client.PostAsJsonAsync("/items", new CosmosTestItem("1", "1", "Alice"));
        await client.PostAsJsonAsync("/items", new CosmosTestItem("2", "2", "Bob"));

        var response = await client.GetAsync("/items");
        var items = await response.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
        items.Should().HaveCount(2);
    }

    [Fact]
    public async Task ClientIsAccessible()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

        var cosmosClient = app.Services.GetRequiredService<CosmosClient>();
        cosmosClient.Should().BeOfType<InMemoryCosmosClient>();

        var container = cosmosClient.GetContainer("in-memory-db", "items");
        container.Should().BeOfType<InMemoryContainer>();
    }

    [Fact]
    public async Task IsolatedPerFactory()
    {
        await using var app1 = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
        await using var app2 = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

        var client1 = app1.HttpClient;
        var client2 = app2.HttpClient;

        await client1.PostAsJsonAsync("/items", new CosmosTestItem("1", "1", "Factory1Item"));

        var response1 = await client1.GetAsync("/items/1");
        response1.StatusCode.Should().Be(HttpStatusCode.OK);

        var response2 = await client2.GetAsync("/items/1");
        response2.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task CalledFromConfigureTestServices_FullRoundTrip()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
        var client = app.HttpClient;

        // Create
        var postResponse = await client.PostAsJsonAsync("/items",
            new CosmosTestItem("rt1", "rt1", "RoundTrip"));
        postResponse.StatusCode.Should().Be(HttpStatusCode.Created);
        var created = await postResponse.Content.ReadFromJsonAsync<CosmosTestItem>(JsonOptions);
        created!.Id.Should().Be("rt1");
        created.Name.Should().Be("RoundTrip");

        // Read
        var getResponse = await client.GetAsync("/items/rt1");
        getResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        var read = await getResponse.Content.ReadFromJsonAsync<CosmosTestItem>(JsonOptions);
        read!.Name.Should().Be("RoundTrip");

        // List
        var listResponse = await client.GetAsync("/items");
        var items = await listResponse.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
        items.Should().ContainSingle().Which.Name.Should().Be("RoundTrip");
    }

    [Fact]
    public async Task ConstructorResolvedContainer_Pattern3()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureBaseServices: services =>
            {
                services.AddSingleton<CosmosClient>(_ =>
                    new CosmosClient("AccountEndpoint=https://fail.example.com:9999/;AccountKey=dGVzdA==;"));
                services.AddSingleton<ClientResolvedRepository>();
            },
            configureTestServices: services =>
            {
                services.UseInMemoryCosmosDB(o =>
                {
                    o.DatabaseName = "TestDb";
                    o.AddContainer("items", "/partitionKey");
                });
            },
            configureEndpoints: endpoints =>
            {
                endpoints.MapPost("/client-items", async (CosmosTestItem item, ClientResolvedRepository repo) =>
                {
                    var created = await repo.CreateAsync(item);
                    return Results.Created($"/client-items/{created.Id}", created);
                });
                endpoints.MapGet("/client-items/{id}", async (string id, ClientResolvedRepository repo) =>
                {
                    var result = await repo.GetByIdAsync(id, id);
                    return result is not null ? Results.Ok(result) : Results.NotFound();
                });
            });

        var client = app.HttpClient;

        var postResponse = await client.PostAsJsonAsync("/client-items",
            new CosmosTestItem("p3-1", "p3-1", "Pattern3"));
        postResponse.StatusCode.Should().Be(HttpStatusCode.Created);

        var getResponse = await client.GetAsync("/client-items/p3-1");
        getResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        var result = await getResponse.Content.ReadFromJsonAsync<CosmosTestItem>(JsonOptions);
        result!.Name.Should().Be("Pattern3");
    }

    [Fact]
    public async Task ZeroConfig_JustWorks()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB());
        var client = app.HttpClient;

        var postResponse = await client.PostAsJsonAsync("/items",
            new CosmosTestItem("z1", "z1", "ZeroConfig"));
        postResponse.StatusCode.Should().Be(HttpStatusCode.Created);

        var getResponse = await client.GetAsync("/items/z1");
        getResponse.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task ZeroConfig_AutoDetect_ContainerNameMatchesProduction()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB());

        // The auto-detected container should be the one from the production factory:
        // client.GetContainer("ProductionDb", "items")
        var container = app.Services.GetRequiredService<Container>();
        container.Id.Should().Be("items");

        // Verify the client also returns the same container instance
        var cosmosClient = (InMemoryCosmosClient)app.Services.GetRequiredService<CosmosClient>();
        var clientContainer = cosmosClient.GetContainer("ProductionDb", "items");
        container.Should().BeSameAs(clientContainer);
    }

    [Fact]
    public async Task UpsertAndQuery()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

        var container = app.Services.GetRequiredService<Container>();

        await container.UpsertItemAsync(
            new CosmosTestItem("u1", "u1", "Original"),
            new PartitionKey("u1"));
        await container.UpsertItemAsync(
            new CosmosTestItem("u1", "u1", "Updated"),
            new PartitionKey("u1"));
        await container.UpsertItemAsync(
            new CosmosTestItem("u2", "u2", "Second"),
            new PartitionKey("u2"));

        var httpClient = app.HttpClient;
        var response = await httpClient.GetAsync("/items");
        var items = await response.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
        items.Should().HaveCount(2);
        items.Should().Contain(i => i.Id == "u1" && i.Name == "Updated");
        items.Should().Contain(i => i.Id == "u2" && i.Name == "Second");
    }
}
