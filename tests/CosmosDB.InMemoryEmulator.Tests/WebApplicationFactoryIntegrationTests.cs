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
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ════════════════════════════════════════════════════════════════════════════════
// Minimal test app for integration tests
// ════════════════════════════════════════════════════════════════════════════════

public record CosmosTestItem(
    [property: JsonPropertyName("id")]
    [property: Newtonsoft.Json.JsonProperty("id")]
    string Id,
    [property: JsonPropertyName("partitionKey")]
    [property: Newtonsoft.Json.JsonProperty("partitionKey")]
    string PartitionKey,
    [property: JsonPropertyName("name")]
    [property: Newtonsoft.Json.JsonProperty("name")]
    string Name);

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

    public async Task DeleteAsync(string id, string partitionKey)
    {
        await _container.DeleteItemAsync<CosmosTestItem>(id, new PartitionKey(partitionKey));
    }

    public async Task<CosmosTestItem> ReplaceAsync(CosmosTestItem item)
    {
        var response = await _container.ReplaceItemAsync(item, item.Id, new PartitionKey(item.PartitionKey));
        return response.Resource;
    }

    public async Task PatchNameAsync(string id, string partitionKey, string newName)
    {
        await _container.PatchItemAsync<CosmosTestItem>(id, new PartitionKey(partitionKey),
            new[] { PatchOperation.Set("/name", newName) });
    }

    public async Task<List<CosmosTestItem>> GetFilteredByNameAsync(string name)
    {
        var iterator = _container.GetItemLinqQueryable<CosmosTestItem>(true)
            .Where(x => x.Name == name)
            .ToFeedIteratorOverridable();

        var results = new List<CosmosTestItem>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        return results;
    }

    public async Task<List<CosmosTestItem>> QuerySqlAsync(string sql)
    {
        var iterator = _container.GetItemQueryIterator<CosmosTestItem>(new QueryDefinition(sql));
        var results = new List<CosmosTestItem>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        return results;
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
// Typed CosmosClient for Pattern 2 testing
// ════════════════════════════════════════════════════════════════════════════════

public class TestCosmosClient : InMemoryCosmosClient { }

/// <summary>
/// Repository that resolves a typed InMemoryCosmosClient — simulates Pattern 2.
/// </summary>
public class TypedClientRepository
{
    private readonly Container _container;

    public TypedClientRepository(TestCosmosClient client)
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
                                try
                                {
                                    var created = await repo.CreateAsync(item);
                                    return Results.Created($"/items/{created.Id}", created);
                                }
                                catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
                                {
                                    return Results.Conflict();
                                }
                            });

                            endpoints.MapGet("/items", async (TestRepository repo) =>
                            {
                                var items = await repo.GetAllAsync();
                                return Results.Ok(items);
                            });

                            endpoints.MapDelete("/items/{id}", async (string id, TestRepository repo) =>
                            {
                                try
                                {
                                    await repo.DeleteAsync(id, id);
                                    return Results.NoContent();
                                }
                                catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                                {
                                    return Results.NotFound();
                                }
                            });

                            endpoints.MapPut("/items/{id}", async (string id, CosmosTestItem item, TestRepository repo) =>
                            {
                                try
                                {
                                    var replaced = await repo.ReplaceAsync(item);
                                    return Results.Ok(replaced);
                                }
                                catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                                {
                                    return Results.NotFound();
                                }
                            });

                            endpoints.MapPut("/items", async (CosmosTestItem item, TestRepository repo) =>
                            {
                                var upserted = await repo.UpsertAsync(item);
                                return Results.Ok(upserted);
                            });

                            endpoints.MapMethods("/items/{id}/name", new[] { "PATCH" },
                                async (string id, HttpContext ctx) =>
                                {
                                    var body = await ctx.Request.ReadFromJsonAsync<JsonElement>();
                                    var newName = body.GetProperty("name").GetString()!;
                                    var repo = ctx.RequestServices.GetRequiredService<TestRepository>();
                                    try
                                    {
                                        await repo.PatchNameAsync(id, id, newName);
                                        return Results.Ok();
                                    }
                                    catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                                    {
                                        return Results.NotFound();
                                    }
                                });

                            endpoints.MapGet("/items/search", async (string name, TestRepository repo) =>
                            {
                                var items = await repo.GetFilteredByNameAsync(name);
                                return Results.Ok(items);
                            });

                            endpoints.MapPost("/items/query", async (HttpContext ctx) =>
                            {
                                var body = await ctx.Request.ReadFromJsonAsync<JsonElement>();
                                var sql = body.GetProperty("sql").GetString()!;
                                var repo = ctx.RequestServices.GetRequiredService<TestRepository>();
                                var items = await repo.QuerySqlAsync(sql);
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
        cosmosClient.Should().NotBeNull();

        var container = cosmosClient.GetContainer("in-memory-db", "items");
        container.Should().NotBeNull();
        container.Id.Should().Be("items");
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

        // Verify the client also returns a container with the same name
        var cosmosClient = app.Services.GetRequiredService<CosmosClient>();
        var clientContainer = cosmosClient.GetContainer("ProductionDb", "items");
        clientContainer.Id.Should().Be("items");
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

// ════════════════════════════════════════════════════════════════════════════════
// Phase A: Fix Existing Issues
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class WafLinqFilterTests : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
    public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

    [Fact]
    public async Task LinqQueryWithFilter_ReturnsMatchingItems()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
        var client = app.HttpClient;

        await client.PostAsJsonAsync("/items", new CosmosTestItem("1", "1", "Alice"));
        await client.PostAsJsonAsync("/items", new CosmosTestItem("2", "2", "Bob"));

        var response = await client.GetAsync("/items/search?name=Alice");
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var items = await response.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
        items.Should().ContainSingle().Which.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task LinqWithNativeToFeedIterator_WorksWithUseInMemoryCosmosDB()
    {
        // UseInMemoryCosmosDB uses FakeCosmosHandler, so native .ToFeedIterator() works
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

        var container = app.Services.GetRequiredService<Container>();

        await container.CreateItemAsync(new CosmosTestItem("1", "1", "Alice"), new PartitionKey("1"));
        await container.CreateItemAsync(new CosmosTestItem("2", "2", "Bob"), new PartitionKey("2"));

        // Use native .ToFeedIterator() — NOT .ToFeedIteratorOverridable()
        var iterator = container.GetItemLinqQueryable<CosmosTestItem>(true)
            .Where(x => x.Name == "Alice")
            .ToFeedIterator();

        var results = new List<CosmosTestItem>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle().Which.Name.Should().Be("Alice");
    }
}

// ════════════════════════════════════════════════════════════════════════════════
// Phase B: Missing CRUD Operations via HTTP
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class WafCrudViaHttpTests : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
    public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

    [Fact]
    public async Task DeleteItem_ViaHttp_RemovesItem()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
        var client = app.HttpClient;

        await client.PostAsJsonAsync("/items", new CosmosTestItem("d1", "d1", "ToDelete"));

        var deleteResponse = await client.DeleteAsync("/items/d1");
        deleteResponse.StatusCode.Should().Be(HttpStatusCode.NoContent);

        var getResponse = await client.GetAsync("/items/d1");
        getResponse.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task DeleteItem_ViaHttp_NotFound_Returns404()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
        var client = app.HttpClient;

        var response = await client.DeleteAsync("/items/nonexistent");
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task ReplaceItem_ViaHttp_UpdatesItem()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
        var client = app.HttpClient;

        await client.PostAsJsonAsync("/items", new CosmosTestItem("r1", "r1", "Original"));

        var putResponse = await client.PutAsJsonAsync("/items/r1",
            new CosmosTestItem("r1", "r1", "Replaced"));
        putResponse.StatusCode.Should().Be(HttpStatusCode.OK);

        var getResponse = await client.GetAsync("/items/r1");
        var result = await getResponse.Content.ReadFromJsonAsync<CosmosTestItem>(JsonOptions);
        result!.Name.Should().Be("Replaced");
    }

    [Fact]
    public async Task UpsertItem_ViaHttp_CreatesOrUpdates()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
        var client = app.HttpClient;

        // Upsert new
        var upsertResponse = await client.PutAsJsonAsync("/items",
            new CosmosTestItem("up1", "up1", "New"));
        upsertResponse.StatusCode.Should().Be(HttpStatusCode.OK);

        // Upsert existing
        var upsertResponse2 = await client.PutAsJsonAsync("/items",
            new CosmosTestItem("up1", "up1", "Updated"));
        upsertResponse2.StatusCode.Should().Be(HttpStatusCode.OK);

        var getResponse = await client.GetAsync("/items/up1");
        var result = await getResponse.Content.ReadFromJsonAsync<CosmosTestItem>(JsonOptions);
        result!.Name.Should().Be("Updated");
    }

    [Fact]
    public async Task PatchItem_ViaHttp_PartialUpdate()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
        var client = app.HttpClient;

        await client.PostAsJsonAsync("/items", new CosmosTestItem("p1", "p1", "Original"));

        var patchContent = JsonContent.Create(new { name = "Patched" });
        var patchRequest = new HttpRequestMessage(new HttpMethod("PATCH"), "/items/p1/name")
        {
            Content = patchContent
        };
        var patchResponse = await client.SendAsync(patchRequest);
        patchResponse.StatusCode.Should().Be(HttpStatusCode.OK);

        var getResponse = await client.GetAsync("/items/p1");
        var result = await getResponse.Content.ReadFromJsonAsync<CosmosTestItem>(JsonOptions);
        result!.Name.Should().Be("Patched");
    }

    [Fact]
    public async Task CreateDuplicateItem_ViaHttp_Returns409()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
        var client = app.HttpClient;

        var item = new CosmosTestItem("dup1", "dup1", "First");
        await client.PostAsJsonAsync("/items", item);

        var duplicateResponse = await client.PostAsJsonAsync("/items", item);
        duplicateResponse.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }
}

// ════════════════════════════════════════════════════════════════════════════════
// Phase C: Missing DI Integration Patterns
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class WafDiPatternTests : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
    public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

    [Fact]
    public async Task UseInMemoryCosmosContainers_WorksThroughWaf()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureBaseServices: services =>
            {
                services.AddSingleton<CosmosClient>(_ =>
                    new CosmosClient("AccountEndpoint=https://fail.example.com:9999/;AccountKey=dGVzdA==;"));
                services.AddSingleton(sp => sp.GetRequiredService<CosmosClient>().GetContainer("Db", "items"));
                services.AddSingleton<TestRepository>();
            },
            configureTestServices: services =>
            {
                services.UseInMemoryCosmosContainers(o => o.AddContainer("items", "/partitionKey"));
            });
        var client = app.HttpClient;

        var postResponse = await client.PostAsJsonAsync("/items",
            new CosmosTestItem("c1", "c1", "ContainerPattern"));
        postResponse.StatusCode.Should().Be(HttpStatusCode.Created);

        var getResponse = await client.GetAsync("/items/c1");
        getResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        var result = await getResponse.Content.ReadFromJsonAsync<CosmosTestItem>(JsonOptions);
        result!.Name.Should().Be("ContainerPattern");
    }

    [Fact]
    public async Task TypedClient_WorksThroughWaf()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureBaseServices: services =>
            {
                services.AddSingleton<TypedClientRepository>();
            },
            configureTestServices: services =>
            {
                services.UseInMemoryCosmosDB<TestCosmosClient>(o =>
                {
                    o.DatabaseName = "TestDb";
                    o.AddContainer("items", "/partitionKey");
                });
            },
            configureEndpoints: endpoints =>
            {
                endpoints.MapPost("/typed-items", async (CosmosTestItem item, TypedClientRepository repo) =>
                {
                    var created = await repo.CreateAsync(item);
                    return Results.Created($"/typed-items/{created.Id}", created);
                });
                endpoints.MapGet("/typed-items/{id}", async (string id, TypedClientRepository repo) =>
                {
                    var result = await repo.GetByIdAsync(id, id);
                    return result is not null ? Results.Ok(result) : Results.NotFound();
                });
            });
        var client = app.HttpClient;

        var postResponse = await client.PostAsJsonAsync("/typed-items",
            new CosmosTestItem("t1", "t1", "TypedClient"));
        postResponse.StatusCode.Should().Be(HttpStatusCode.Created);

        var getResponse = await client.GetAsync("/typed-items/t1");
        getResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        var result = await getResponse.Content.ReadFromJsonAsync<CosmosTestItem>(JsonOptions);
        result!.Name.Should().Be("TypedClient");
    }

    [Fact]
    public async Task MultipleContainers_SameApp_IsolatedData()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureBaseServices: services =>
            {
                services.AddRouting();
            },
            configureTestServices: services =>
            {
                services.UseInMemoryCosmosDB(o =>
                {
                    o.AddContainer("orders", "/partitionKey");
                    o.AddContainer("products", "/partitionKey");
                });
            },
            configureEndpoints: endpoints =>
            {
                endpoints.MapPost("/orders", async (CosmosTestItem item, CosmosClient cosmosClient) =>
                {
                    var container = cosmosClient.GetContainer("in-memory-db", "orders");
                    var response = await container.CreateItemAsync(item, new PartitionKey(item.PartitionKey));
                    return Results.Created($"/orders/{response.Resource.Id}", response.Resource);
                });
                endpoints.MapGet("/orders", async (CosmosClient cosmosClient) =>
                {
                    var container = cosmosClient.GetContainer("in-memory-db", "orders");
                    var iterator = container.GetItemQueryIterator<CosmosTestItem>("SELECT * FROM c");
                    var results = new List<CosmosTestItem>();
                    while (iterator.HasMoreResults)
                    {
                        var page = await iterator.ReadNextAsync();
                        results.AddRange(page);
                    }

                    return Results.Ok(results);
                });
                endpoints.MapGet("/products", async (CosmosClient cosmosClient) =>
                {
                    var container = cosmosClient.GetContainer("in-memory-db", "products");
                    var iterator = container.GetItemQueryIterator<CosmosTestItem>("SELECT * FROM c");
                    var results = new List<CosmosTestItem>();
                    while (iterator.HasMoreResults)
                    {
                        var page = await iterator.ReadNextAsync();
                        results.AddRange(page);
                    }

                    return Results.Ok(results);
                });
            });
        var client = app.HttpClient;

        await client.PostAsJsonAsync("/orders", new CosmosTestItem("o1", "o1", "Order1"));

        var ordersResponse = await client.GetAsync("/orders");
        var orders = await ordersResponse.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
        orders.Should().ContainSingle();

        var productsResponse = await client.GetAsync("/products");
        var products = await productsResponse.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
        products.Should().BeEmpty();
    }

    [Fact]
    public async Task OnHandlerCreated_FaultInjection_ViaWaf()
    {
        FakeCosmosHandler? capturedHandler = null;

        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o =>
                {
                    o.AddContainer("items", "/partitionKey");
                    o.OnHandlerCreated = (_, handler) => capturedHandler = handler;
                }));

        capturedHandler.Should().NotBeNull();

        var client = app.HttpClient;

        // Seed an item first
        await client.PostAsJsonAsync("/items", new CosmosTestItem("f1", "f1", "Faulted"));

        // Now inject a fault — all reads return 503
        capturedHandler!.FaultInjector = _ =>
            new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);

        // TestServer propagates unhandled exceptions rather than returning 500,
        // so the CosmosException surfaces directly to the caller
        Func<Task> act = async () => await client.GetAsync("/items/f1");
        await act.Should().ThrowAsync<CosmosException>()
            .Where(ex => ex.StatusCode == HttpStatusCode.ServiceUnavailable);
    }

    [Fact]
    public async Task HttpMessageHandlerWrapper_RequestCounting_ViaWaf()
    {
        var requestCount = 0;

        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o =>
                {
                    o.AddContainer("items", "/partitionKey");
                    o.WithHttpMessageHandlerWrapper(innerHandler =>
                    {
                        var countingHandler = new CountingDelegatingHandler(innerHandler, () => Interlocked.Increment(ref requestCount));
                        return countingHandler;
                    });
                }));
        var client = app.HttpClient;

        await client.PostAsJsonAsync("/items", new CosmosTestItem("c1", "c1", "Counted"));
        await client.GetAsync("/items/c1");

        requestCount.Should().BeGreaterThan(0);
    }
}

/// <summary>
/// DelegatingHandler that counts requests passing through it.
/// </summary>
public class CountingDelegatingHandler : DelegatingHandler
{
    private readonly Action _onRequest;

    public CountingDelegatingHandler(HttpMessageHandler inner, Action onRequest) : base(inner)
    {
        _onRequest = onRequest;
    }

    protected override Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        _onRequest();
        return base.SendAsync(request, cancellationToken);
    }
}

// ════════════════════════════════════════════════════════════════════════════════
// Phase D: Query & Data Patterns
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class WafQueryPatternTests : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
    public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

    [Fact]
    public async Task SqlQuery_ViaHttpEndpoint_ReturnsFilteredResults()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
        var client = app.HttpClient;

        await client.PostAsJsonAsync("/items", new CosmosTestItem("q1", "q1", "Alice"));
        await client.PostAsJsonAsync("/items", new CosmosTestItem("q2", "q2", "Bob"));
        await client.PostAsJsonAsync("/items", new CosmosTestItem("q3", "q3", "Alice"));

        var queryResponse = await client.PostAsJsonAsync("/items/query",
            new { sql = "SELECT * FROM c WHERE c.name = 'Alice'" });
        queryResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        var items = await queryResponse.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
        items.Should().HaveCount(2);
        items.Should().OnlyContain(i => i.Name == "Alice");
    }

    [Fact]
    public async Task EmptyContainer_ListReturnsEmptyArray()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
        var client = app.HttpClient;

        var response = await client.GetAsync("/items");
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var items = await response.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
        items.Should().BeEmpty();
    }
}

// ════════════════════════════════════════════════════════════════════════════════
// Phase E: Error & Edge Cases
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class WafEdgeCaseTests : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
    public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

    [Fact]
    public async Task ConcurrentRequests_AllSucceed()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
        var client = app.HttpClient;

        var tasks = Enumerable.Range(1, 10).Select(i =>
            client.PostAsJsonAsync("/items",
                new CosmosTestItem($"c{i}", $"c{i}", $"Item{i}")));
        var responses = await Task.WhenAll(tasks);

        responses.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);

        var listResponse = await client.GetAsync("/items");
        var items = await listResponse.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
        items.Should().HaveCount(10);
    }

    [Fact]
    public async Task ItemWithSpecialCharactersInId_RoundTrips()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

        // Use direct container access to avoid HTTP URL encoding issues with the test endpoint
        var container = app.Services.GetRequiredService<Container>();
        var id = "item+special chars&more";
        var item = new CosmosTestItem(id, id, "Special");
        await container.CreateItemAsync(item, new PartitionKey(id));

        var read = await container.ReadItemAsync<CosmosTestItem>(id, new PartitionKey(id));
        read.Resource.Name.Should().Be("Special");
        read.Resource.Id.Should().Be(id);
    }

    [Fact]
    public async Task ETagConcurrency_ViaDirectContainer_InWaf()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

        var container = app.Services.GetRequiredService<Container>();

        // Create and capture ETag
        var createResponse = await container.CreateItemAsync(
            new CosmosTestItem("e1", "e1", "Original"), new PartitionKey("e1"));
        var originalETag = createResponse.ETag;

        // Update the item, which changes the ETag
        await container.ReplaceItemAsync(
            new CosmosTestItem("e1", "e1", "Updated"), "e1", new PartitionKey("e1"));

        // Try to replace with stale ETag — should fail
        var act = () => container.ReplaceItemAsync(
            new CosmosTestItem("e1", "e1", "StaleUpdate"), "e1", new PartitionKey("e1"),
            new ItemRequestOptions { IfMatchEtag = originalETag });
        await act.Should().ThrowAsync<CosmosException>()
            .Where(ex => ex.StatusCode == HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task ItemWithDifferentIdAndPartitionKey_ViaHttp()
    {
        // Demonstrates that GET /items/{id} assumes PK == ID — items with different PK won't be found
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

        var container = app.Services.GetRequiredService<Container>();

        // Create an item where PK differs from ID
        await container.CreateItemAsync(
            new CosmosTestItem("abc", "xyz", "DiffPk"), new PartitionKey("xyz"));

        var client = app.HttpClient;
        // The HTTP endpoint hardcodes PK=ID, so this returns 404
        var response = await client.GetAsync("/items/abc");
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);

        // But direct container access with correct PK works
        var read = await container.ReadItemAsync<CosmosTestItem>("abc", new PartitionKey("xyz"));
        read.Resource.Name.Should().Be("DiffPk");
    }
}

// ════════════════════════════════════════════════════════════════════════════════
// Phase F: Difficult Behaviours (Skip + Sister Tests)
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class WafDivergentBehaviorTests : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
    public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

    [Fact(Skip = "UseInMemoryCosmosDB creates a single FakeCosmosHandler and CosmosClient at registration time. "
        + "The Container resolved from client.GetContainer() returns the same in-memory backing store regardless of scope. "
        + "Per-request data isolation would require a scoped FakeCosmosHandler factory. "
        + "See sister test: ScopedLifetime_ActualBehavior_ReturnsSameDataAcrossScopes")]
    public async Task ScopedLifetime_PreservesPerRequestIsolation()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureBaseServices: services =>
            {
                services.AddScoped<CosmosClient>(_ =>
                    new CosmosClient("AccountEndpoint=https://fail.example.com:9999/;AccountKey=dGVzdA==;"));
                services.AddScoped(sp => sp.GetRequiredService<CosmosClient>().GetContainer("Db", "items"));
                services.AddScoped<TestRepository>();
            },
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

        var client = app.HttpClient;
        // If scoped, data written in one request should be isolated from another
        await client.PostAsJsonAsync("/items", new CosmosTestItem("s1", "s1", "ScopedItem"));
        var response = await client.GetAsync("/items");
        var items = await response.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
        items.Should().BeEmpty(); // Would need true scoped isolation
    }

    [Fact]
    public async Task ScopedLifetime_ActualBehavior_ReturnsSameDataAcrossScopes()
    {
        // DIVERGENT BEHAVIOR: Even when the base service registers Container as Scoped,
        // UseInMemoryCosmosDB replaces it with a Singleton CosmosClient. The Container
        // resolved from client.GetContainer() shares the same in-memory store. Data written
        // in one scope is visible in all other scopes. This matches real CosmosClient behavior
        // (the SDK client is typically a singleton), but differs from what "Scoped" lifetime
        // might imply for DI-resolved Container instances.
        await using var app = await TestAppHost.CreateAsync(
            configureBaseServices: services =>
            {
                services.AddScoped<CosmosClient>(_ =>
                    new CosmosClient("AccountEndpoint=https://fail.example.com:9999/;AccountKey=dGVzdA==;"));
                services.AddScoped(sp => sp.GetRequiredService<CosmosClient>().GetContainer("Db", "items"));
                services.AddScoped<TestRepository>();
            },
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

        var client = app.HttpClient;
        await client.PostAsJsonAsync("/items", new CosmosTestItem("s1", "s1", "ScopedItem"));

        var response = await client.GetAsync("/items");
        var items = await response.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
        items.Should().ContainSingle().Which.Name.Should().Be("ScopedItem");
    }

    [Fact(Skip = "HTTP pagination requires exposing continuation tokens through the HTTP API layer. "
        + "The FakeCosmosHandler supports pagination internally, but surfacing this through a minimal API "
        + "endpoint would require custom response headers and request parameters beyond the scope of the "
        + "integration pattern being tested. See sister test: Pagination_WorksViaDirectContainerAccess_InWafContext")]
    public async Task Pagination_ContinuationToken_ViaHttp()
    {
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
        var client = app.HttpClient;

        for (var i = 0; i < 100; i++)
            await client.PostAsJsonAsync("/items", new CosmosTestItem($"p{i}", $"p{i}", $"Item{i}"));

        // Would need continuation token header support in the endpoint
        var response = await client.GetAsync("/items?pageSize=10");
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Pagination_WorksViaDirectContainerAccess_InWafContext()
    {
        // DIVERGENT BEHAVIOR: Pagination via FeedIterator works correctly at the Container
        // level. However, when accessed through HTTP endpoints in a WAF context, pagination
        // requires the API layer to thread continuation tokens through request/response headers —
        // this is an application concern, not an emulator concern.
        await using var app = await TestAppHost.CreateAsync(
            configureTestServices: services =>
                services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

        var container = app.Services.GetRequiredService<Container>();

        for (var i = 0; i < 5; i++)
            await container.CreateItemAsync(
                new CosmosTestItem($"p{i}", $"p{i}", $"Item{i}"), new PartitionKey($"p{i}"));

        var iterator = container.GetItemQueryIterator<CosmosTestItem>(
            "SELECT * FROM c",
            requestOptions: new QueryRequestOptions { MaxItemCount = 2 });

        var allItems = new List<CosmosTestItem>();
        var pageCount = 0;
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            allItems.AddRange(page);
            pageCount++;
        }

        allItems.Should().HaveCount(5);
        pageCount.Should().BeGreaterThan(1);
    }
}
