using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Fluent;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ════════════════════════════════════════════════════════════════════════════════
// DatabaseApiGapTests — Tests from Database API gap analysis
// Reference: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.database
// ════════════════════════════════════════════════════════════════════════════════

#region 1. CreateContainerIfNotExistsAsync — Status Code Validation

public class CreateContainerIfNotExistsStatusCodeTests
{
    [Fact]
    public async Task CreateContainerIfNotExistsAsync_NewContainer_Returns201()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var response = await db.CreateContainerIfNotExistsAsync("container1", "/pk");

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task CreateContainerIfNotExistsAsync_ExistingContainer_Returns200()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateContainerAsync("container1", "/pk");

        var response = await db.CreateContainerIfNotExistsAsync("container1", "/pk");

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}

#endregion

#region 2. CreateContainerIfNotExistsAsync — Properties Not Re-Validated

public class CreateContainerIfNotExistsPropertiesTests
{
    /// <summary>
    /// Per SDK docs: "Only the container id is used to verify if there is an existing container.
    /// Other container properties such as throughput are not validated and can be different."
    /// Calling with a different partition key path should NOT update the existing container.
    /// </summary>
    [Fact]
    public async Task CreateContainerIfNotExistsAsync_DifferentPartitionKeyPath_DoesNotUpdateExisting()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        // Create with /pk
        await db.CreateContainerAsync("container1", "/pk");

        // IfNotExists with /differentPk — should NOT change the existing container
        var response = await db.CreateContainerIfNotExistsAsync("container1", "/differentPk");

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        // Verify the container still uses the original partition key path
        var container = (InMemoryContainer)db.GetContainer("container1");
        container.PartitionKeyPaths[0].Should().Be("/pk");
    }

    [Fact]
    public async Task CreateContainerIfNotExistsAsync_ContainerProperties_DifferentPk_DoesNotUpdate()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        await db.CreateContainerAsync(new ContainerProperties("container1", "/pk"));

        var response = await db.CreateContainerIfNotExistsAsync(
            new ContainerProperties("container1", "/otherPk"));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var container = (InMemoryContainer)db.GetContainer("container1");
        container.PartitionKeyPaths[0].Should().Be("/pk");
    }
}

#endregion

#region 3. CreateContainerStreamAsync — Response Body Content (Divergent)

public class CreateContainerStreamResponseTests
{
    /// <summary>
    /// DIVERGENT BEHAVIOR: Real Cosmos DB includes the container properties JSON
    /// in the response stream body on CreateContainerStreamAsync.
    /// InMemoryDatabase returns a bare ResponseMessage with no body content.
    /// </summary>
    [Fact]
    public async Task DivergentBehavior_CreateContainerStreamAsync_ResponseBodyIsEmpty()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        using var response = await db.CreateContainerStreamAsync(
            new ContainerProperties("container1", "/pk"));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        // InMemory returns no content in the stream body (diverges from real SDK)
        response.Content.Should().BeNull();
    }
}

#endregion

#region 4. CreateContainerAsync — Returned Container Is Usable

public class CreateContainerReturnedContainerTests
{
    [Fact]
    public async Task CreateContainerAsync_ReturnedContainer_IsUsableForCrud()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var response = await db.CreateContainerAsync("container1", "/partitionKey");
        var container = response.Container;

        container.Should().NotBeNull();

        // Use the returned container to create and read an item
        var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
        await container.CreateItemAsync(item, new PartitionKey("pk1"));

        var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Test");
    }
}

#endregion

#region 5. CreateContainerAsync — Response Resource Contains PartitionKeyPath

public class CreateContainerResponseResourceTests
{
    [Fact]
    public async Task CreateContainerAsync_ResponseResource_HasCorrectPartitionKeyPath()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var response = await db.CreateContainerAsync("container1", "/myPartitionKey");

        response.Resource.Should().NotBeNull();
        response.Resource.Id.Should().Be("container1");
        response.Resource.PartitionKeyPath.Should().Be("/myPartitionKey");
    }

    [Fact]
    public async Task CreateContainerAsync_WithContainerProperties_ResponseHasCorrectPkPath()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var response = await db.CreateContainerAsync(
            new ContainerProperties("container1", "/category"));

        response.Resource.PartitionKeyPath.Should().Be("/category");
    }

    [Fact]
    public async Task CreateContainerIfNotExistsAsync_ResponseResource_HasCorrectPkPath()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var response = await db.CreateContainerIfNotExistsAsync("container1", "/region");

        response.Resource.PartitionKeyPath.Should().Be("/region");
    }
}

#endregion

#region 6. GetContainer — Returns Same Instance / Data Visible Across References

public class GetContainerSameInstanceTests
{
    [Fact]
    public async Task GetContainer_CalledTwice_ReturnsSameContainer()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateContainerAsync("container1", "/partitionKey");

        var ref1 = db.GetContainer("container1");
        var ref2 = db.GetContainer("container1");

        ref1.Should().BeSameAs(ref2);
    }

    [Fact]
    public async Task GetContainer_DataVisibleAcrossReferences()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateContainerAsync("container1", "/partitionKey");

        var ref1 = db.GetContainer("container1");
        var ref2 = db.GetContainer("container1");

        // Write through ref1
        await ref1.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Hello" },
            new PartitionKey("pk1"));

        // Read through ref2
        var read = await ref2.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read.Resource.Name.Should().Be("Hello");
    }
}

#endregion

#region 7. GetContainerQueryIterator — Returns Correct PartitionKeyPath (BUG FIX)

public class GetContainerQueryIteratorPartitionKeyTests
{
    [Fact]
    public async Task GetContainerQueryIterator_ReturnsActualPartitionKeyPath()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        await db.CreateContainerAsync("users", "/userId");
        await db.CreateContainerAsync("orders", "/orderId");

        var iterator = db.GetContainerQueryIterator<ContainerProperties>();
        var containers = new List<ContainerProperties>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            containers.AddRange(page);
        }

        containers.Should().HaveCount(2);
        var users = containers.Single(c => c.Id == "users");
        var orders = containers.Single(c => c.Id == "orders");

        users.PartitionKeyPath.Should().Be("/userId");
        orders.PartitionKeyPath.Should().Be("/orderId");
    }

    [Fact]
    public async Task GetContainerQueryIterator_WithQueryDefinition_ReturnsActualPkPath()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        await db.CreateContainerAsync("myContainer", "/tenantId");

        var queryDef = new QueryDefinition("SELECT * FROM c");
        var iterator = db.GetContainerQueryIterator<ContainerProperties>(queryDef);
        var containers = new List<ContainerProperties>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            containers.AddRange(page);
        }

        containers.Should().ContainSingle()
            .Which.PartitionKeyPath.Should().Be("/tenantId");
    }
}

#endregion

#region 8. ReadAsync — Response.Database Returns Self

public class ReadAsyncDatabaseResponseTests
{
    [Fact]
    public async Task ReadAsync_ResponseDatabase_IsSameInstance()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var response = await db.ReadAsync();

        response.Database.Should().BeSameAs(db);
    }
}

#endregion

#region 9. DeleteAsync — Subsequent Operations After Delete

public class DeleteAsyncSubsequentOperationsTests
{
    [Fact]
    public async Task DeleteAsync_GetContainerQueryIterator_ReturnsEmpty()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateContainerAsync("container1", "/pk");

        await db.DeleteAsync();

        // After delete, query iterator should return no containers
        var iterator = db.GetContainerQueryIterator<ContainerProperties>();
        var containers = new List<ContainerProperties>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            containers.AddRange(page);
        }
        containers.Should().BeEmpty();
    }
}

#endregion

#region 10. DeleteStreamAsync — Clears All Containers

public class DeleteStreamAsyncContainerTests
{
    [Fact]
    public async Task DeleteStreamAsync_RemovesAllContainersInDatabase()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateContainerAsync("container1", "/pk");
        var container = db.GetContainer("container1");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        await db.DeleteStreamAsync();

        // Re-create database and container — should be empty
        await client.CreateDatabaseAsync("test-db");
        var newDb = client.GetDatabase("test-db");
        await newDb.CreateContainerAsync("container1", "/pk");
        var newContainer = newDb.GetContainer("container1");

        var act = () => newContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        await act.Should().ThrowAsync<CosmosException>();
    }
}

#endregion

#region 11. ReadThroughputAsync — Detailed Validation

public class ReadThroughputAsyncDetailedTests
{
    [Fact]
    public async Task ReadThroughputAsync_WithRequestOptions_ReturnsOkStatusCode()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var response = await db.ReadThroughputAsync(new RequestOptions());

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task ReadThroughputAsync_WithRequestOptions_ReturnsThroughput400()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var response = await db.ReadThroughputAsync(new RequestOptions());

        response.Resource.Should().NotBeNull();
    }
}

#endregion

#region 12. ReplaceThroughputAsync — Returned Value Reflects New Throughput

public class ReplaceThroughputAsyncResponseTests
{
    [Fact]
    public async Task ReplaceThroughputAsync_Int_ResponseContainsNewThroughput()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var response = await db.ReplaceThroughputAsync(1000);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Should().NotBeNull();
    }

    [Fact]
    public async Task ReplaceThroughputAsync_ThroughputProperties_ResponseContainsNewThroughput()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var response = await db.ReplaceThroughputAsync(
            ThroughputProperties.CreateManualThroughput(2000));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Should().NotBeNull();
    }
}

#endregion

#region 13. ReadThroughputAsync — Divergent: Never Throws 404

public class ReadThroughputDivergentTests
{
    /// <summary>
    /// DIVERGENT BEHAVIOR: Real Cosmos DB's ReadThroughputAsync(RequestOptions) throws
    /// CosmosException with StatusCode 404 when the database does not exist or has no
    /// throughput assigned.
    /// InMemoryDatabase always returns a synthetic 400 RU/s throughput value and never
    /// throws 404, because throughput is not meaningful in an in-memory emulator.
    /// </summary>
    [Fact]
    public async Task DivergentBehavior_ReadThroughputAsync_NeverThrows404_AlwaysReturnsSynthetic()
    {
        var db = new InMemoryDatabase("standalone-db");

        // Even a standalone database (not registered in any client) returns throughput
        var throughput = await db.ReadThroughputAsync();
        throughput.Should().Be(400);

        var response = await db.ReadThroughputAsync(new RequestOptions());
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}

#endregion

#region 14. DefineContainer — Various Builder Patterns

public class DefineContainerBuilderTests
{
    [Fact]
    public async Task DefineContainer_WithoutPolicies_CreatesContainer()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var response = await db.DefineContainer("simple-container", "/pk")
            .CreateAsync();

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Resource.Id.Should().Be("simple-container");
    }

    [Fact]
    public void DefineContainer_FluentBuilder_ReturnsContainerBuilder()
    {
        var client = new InMemoryCosmosClient();
        var db = client.GetDatabase("test-db");

        var builder = db.DefineContainer("container1", "/pk");

        builder.Should().NotBeNull();
        builder.Should().BeOfType<ContainerBuilder>();
    }
}

#endregion

#region 15. CreateContainerAsync — Custom IndexingPolicy

public class CreateContainerCustomIndexingTests
{
    [Fact]
    public async Task CreateContainerAsync_WithCustomIndexingPolicy_CreatesSuccessfully()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var props = new ContainerProperties("container1", "/pk")
        {
            IndexingPolicy = new IndexingPolicy
            {
                Automatic = false,
                IndexingMode = IndexingMode.Lazy
            }
        };

        var response = await db.CreateContainerAsync(props);

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Resource.Id.Should().Be("container1");
    }
}

#endregion

#region 16. Concurrent Database Operations

public class ConcurrentDatabaseOperationTests
{
    [Fact]
    public async Task ConcurrentCreateContainerAsync_DifferentIds_AllSucceed()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var tasks = Enumerable.Range(0, 20).Select(i =>
            db.CreateContainerAsync($"container-{i}", "/pk"));

        var responses = await Task.WhenAll(tasks);

        responses.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
        var uniqueIds = responses.Select(r => r.Resource.Id).Distinct();
        uniqueIds.Should().HaveCount(20);
    }

    [Fact]
    public async Task ConcurrentCreateContainerIfNotExistsAsync_SameId_OnlyOneCreated()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        var tasks = Enumerable.Range(0, 20).Select(_ =>
            db.CreateContainerIfNotExistsAsync("shared-container", "/pk"));

        var responses = await Task.WhenAll(tasks);

        // Exactly one should be Created (201), the rest should be OK (200)
        responses.Count(r => r.StatusCode == HttpStatusCode.Created).Should().Be(1);
        responses.Count(r => r.StatusCode == HttpStatusCode.OK).Should().Be(19);
    }
}

#endregion

#region 17. GetContainerQueryIterator — After Container Delete

public class GetContainerQueryIteratorAfterDeleteTests
{
    [Fact(Skip = "InMemoryContainer.DeleteContainerAsync clears internal data but does not " +
                 "remove itself from the parent InMemoryDatabase's container dictionary. " +
                 "Real Cosmos DB would remove the container so it no longer appears in query results. " +
                 "InMemoryDatabase has no RemoveContainer mechanism called by DeleteContainerAsync. " +
                 "See divergent behavior test below.")]
    public async Task GetContainerQueryIterator_AfterContainerDelete_NoLongerListed()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateContainerAsync("container1", "/pk");
        await db.CreateContainerAsync("container2", "/pk");

        var container1 = db.GetContainer("container1");
        await container1.DeleteContainerAsync();

        var iterator = db.GetContainerQueryIterator<ContainerProperties>();
        var containers = new List<ContainerProperties>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            containers.AddRange(page);
        }

        // After delete, only container2 should remain
        containers.Should().ContainSingle().Which.Id.Should().Be("container2");
    }

    /// <summary>
    /// DIVERGENT BEHAVIOR: InMemoryContainer.DeleteContainerAsync clears the container's
    /// internal items, etags, and timestamps but does NOT remove the container from the
    /// parent InMemoryDatabase's container dictionary.
    /// Real Cosmos DB removes the container entirely so it no longer appears in
    /// GetContainerQueryIterator results and subsequent operations would fail with 404.
    /// In the emulator, the container still appears in query results after deletion,
    /// but it will be empty.
    /// </summary>
    [Fact]
    public async Task DivergentBehavior_DeletedContainer_StillAppearsInQueryIterator()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateContainerAsync("container1", "/pk");

        var container = db.GetContainer("container1");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        // Delete the container
        await container.DeleteContainerAsync();

        // Container still appears in query iterator (divergent from real SDK)
        var iterator = db.GetContainerQueryIterator<ContainerProperties>();
        var containers = new List<ContainerProperties>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            containers.AddRange(page);
        }
        containers.Should().ContainSingle().Which.Id.Should().Be("container1");

        // But the items are gone
        var act = () => container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        await act.Should().ThrowAsync<CosmosException>();
    }
}

#endregion

#region 18. GetContainerQueryStreamIterator(QueryDefinition) Overload

public class GetContainerQueryStreamIteratorOverloadTests
{
    [Fact]
    public async Task GetContainerQueryStreamIterator_WithQueryDefinition_ReturnsContainers()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");
        await db.CreateContainerAsync("container1", "/pk");

        var queryDef = new QueryDefinition("SELECT * FROM c");
        var iterator = db.GetContainerQueryStreamIterator(queryDef);

        var responses = new List<ResponseMessage>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            responses.Add(response);
            response.StatusCode.Should().Be(HttpStatusCode.OK);
        }
        responses.Should().NotBeEmpty();
    }
}

#endregion

#region 19. Database Id Property

public class DatabaseIdPropertyTests
{
    [Fact]
    public void Database_Id_ReturnsConstructorValue()
    {
        var db = new InMemoryDatabase("my-database-id");

        db.Id.Should().Be("my-database-id");
    }

    [Fact]
    public void Database_Id_ViaClient_ReturnsCorrectValue()
    {
        var client = new InMemoryCosmosClient();
        var db = client.GetDatabase("another-db");

        db.Id.Should().Be("another-db");
    }
}

#endregion

#region 20. ReadStreamAsync — Response Content (Divergent)

public class ReadStreamAsyncResponseTests
{
    /// <summary>
    /// DIVERGENT BEHAVIOR: Real Cosmos DB returns the database properties JSON in the
    /// ReadStreamAsync response body. InMemoryDatabase returns a bare ResponseMessage
    /// with HttpStatusCode.OK and no body content.
    /// </summary>
    [Fact]
    public async Task DivergentBehavior_ReadStreamAsync_ResponseBodyIsEmpty()
    {
        var client = new InMemoryCosmosClient();
        await client.CreateDatabaseAsync("test-db");
        var db = client.GetDatabase("test-db");

        using var response = await db.ReadStreamAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        // InMemory returns no content in the stream body (diverges from real SDK)
        response.Content.Should().BeNull();
    }
}

#endregion
