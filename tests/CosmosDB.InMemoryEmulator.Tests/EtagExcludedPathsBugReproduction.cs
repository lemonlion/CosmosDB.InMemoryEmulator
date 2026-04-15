using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Bug: /_etag/? not auto-added to IndexingPolicy.ExcludedPaths on container creation.
///
/// Found while migrating ASOS/SimpleEventStore to use InMemoryEmulator.
/// See: https://github.com/McNultyyy/SimpleEventStore/tree/use-inmemory-emulator
///
/// Real Cosmos DB automatically adds "/_etag/?" to ExcludedPaths whenever a container
/// is created with a custom IndexingPolicy. The InMemoryEmulator does not do this,
/// causing tests that assert on ExcludedPaths.Count to fail.
/// </summary>
public class EtagExcludedPathsBugReproduction
{
    [Fact]
    public async Task ContainerProperties_ShouldAutoAddEtagToExcludedPaths()
    {
        var client = new InMemoryCosmosClient();
        var database = (await client.CreateDatabaseIfNotExistsAsync("etag-index-db")).Database;

        var containerProperties = new ContainerProperties("test-container", "/partitionKey")
        {
            IndexingPolicy = new IndexingPolicy
            {
                ExcludedPaths = { new ExcludedPath { Path = "/body/*" }, new ExcludedPath { Path = "/metaData/*" } }
            }
        };
        await database.CreateContainerIfNotExistsAsync(containerProperties);

        var container = database.GetContainer("test-container");
        var response = await container.ReadContainerAsync();
        var excludedPaths = response.Resource.IndexingPolicy.ExcludedPaths.Select(p => p.Path).ToList();

        // Real Cosmos auto-adds /_etag/? to excluded paths
        excludedPaths.Should().Contain("/_etag/?",
            "Cosmos DB auto-adds /_etag/? to ExcludedPaths when a custom IndexingPolicy is provided");
    }
}
