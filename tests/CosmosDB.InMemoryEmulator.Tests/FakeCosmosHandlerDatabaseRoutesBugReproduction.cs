using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Bug: FakeCosmosHandler does not support database and container management HTTP routes.
///
/// Found while migrating 5 open-source projects to use InMemoryEmulator:
/// - ASOS/SimpleEventStore
/// - Azure/Microsoft.Extensions.Caching.Cosmos
/// - MassTransit/MassTransit
/// - Avanade/Beef
/// - Particular/NServiceBus.Persistence.CosmosDB
///
/// Every real-world project calls CreateDatabaseIfNotExistsAsync and/or
/// CreateContainerIfNotExistsAsync before using containers. FakeCosmosHandler
/// throws "unrecognised route POST /dbs" for these operations, forcing all
/// 5 projects to fall back to InMemoryCosmosClient (which doesn't support
/// custom serializers via CosmosClientOptions) instead of the recommended
/// FakeCosmosHandler approach (which preserves the full SDK pipeline).
///
/// See forks:
/// - https://github.com/McNultyyy/SimpleEventStore/tree/use-inmemory-emulator
/// - https://github.com/McNultyyy/Microsoft.Extensions.Caching.Cosmos/tree/use-inmemory-emulator
/// - https://github.com/McNultyyy/MassTransit/tree/use-inmemory-emulator
/// - https://github.com/McNultyyy/Beef/tree/use-inmemory-emulator
/// - https://github.com/McNultyyy/NServiceBus.Persistence.CosmosDB/tree/use-inmemory-emulator
/// </summary>
public class FakeCosmosHandlerDatabaseRoutesBugReproduction
{
    [Fact]
    public async Task FakeCosmosHandler_ShouldSupportCreateDatabaseIfNotExistsAsync()
    {
        var inMemoryContainer = new InMemoryContainer("test-container", "/partitionKey");
        var handler = new FakeCosmosHandler(inMemoryContainer);

        var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                HttpClientFactory = () => new HttpClient(handler)
            });

        // Currently throws: "FakeCosmosHandler: unrecognised route POST /dbs"
        var act = async () => await client.CreateDatabaseIfNotExistsAsync("test-db");
        await act.Should().NotThrowAsync(
            "FakeCosmosHandler should support CreateDatabaseIfNotExistsAsync since nearly all real projects need it");
    }

    [Fact]
    public async Task FakeCosmosHandler_ShouldSupportCreateContainerIfNotExistsAsync()
    {
        var inMemoryContainer = new InMemoryContainer("test-container", "/partitionKey");
        var handler = new FakeCosmosHandler(inMemoryContainer);

        var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                HttpClientFactory = () => new HttpClient(handler)
            });

        // Projects typically create a database first, then a container
        var act = async () =>
        {
            var database = client.GetDatabase("test-db");
            await database.CreateContainerIfNotExistsAsync("my-container", "/partitionKey");
        };
        await act.Should().NotThrowAsync(
            "FakeCosmosHandler should support CreateContainerIfNotExistsAsync since nearly all real projects need it");
    }
}
