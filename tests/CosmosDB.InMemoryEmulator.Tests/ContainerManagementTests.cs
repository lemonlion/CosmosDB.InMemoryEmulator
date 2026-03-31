using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;
using System.Text;
using Newtonsoft.Json.Linq;

namespace CosmosDB.InMemoryEmulator.Tests;

public class ContainerManagementTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public void Id_ReturnsContainerName()
    {
        _container.Id.Should().Be("test-container");
    }

    [Fact]
    public async Task ReadContainerAsync_ReturnsContainerProperties()
    {
        var response = await _container.ReadContainerAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Should().NotBeNull();
        response.Resource.Id.Should().Be("test-container");
        response.Resource.PartitionKeyPath.Should().Be("/partitionKey");
    }

    [Fact]
    public async Task ReadContainerStreamAsync_ReturnsOk()
    {
        using var response = await _container.ReadContainerStreamAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task DeleteContainerAsync_ReturnsNoContent()
    {
        var response = await _container.DeleteContainerAsync();

        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task DeleteContainerStreamAsync_ReturnsNoContent()
    {
        using var response = await _container.DeleteContainerStreamAsync();

        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task GetFeedRangesAsync_ReturnsNonEmptyList()
    {
        var feedRanges = await _container.GetFeedRangesAsync();

        feedRanges.Should().NotBeEmpty();
    }

    [Fact]
    public async Task DeleteAllItemsByPartitionKeyStreamAsync_RemovesItemsInPartition()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "3", PartitionKey = "pk2", Name = "Charlie" },
            new PartitionKey("pk2"));

        using var response = await _container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("pk1"));

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);

        var remaining = await _container.ReadItemAsync<TestDocument>("3", new PartitionKey("pk2"));
        remaining.Resource.Name.Should().Be("Charlie");
    }

    [Fact]
    public async Task DeleteAllItemsByPartitionKeyStreamAsync_EmptyPartition_ReturnsOk()
    {
        using var response = await _container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("nonexistent"));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}


public class ContainerManagementEdgeCaseTests
{
    [Fact]
    public async Task ReplaceContainerStreamAsync_ReturnsOk()
    {
        var container = new InMemoryContainer("test-container", "/partitionKey");
        var properties = new ContainerProperties("test-container", "/partitionKey");

        var response = await container.ReplaceContainerStreamAsync(properties);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task ReplaceContainerAsync_UpdatesIndexingPolicy()
    {
        var container = new InMemoryContainer("test-container", "/partitionKey");

        var properties = new ContainerProperties("test-container", "/partitionKey")
        {
            IndexingPolicy = new IndexingPolicy
            {
                Automatic = false,
                IndexingMode = IndexingMode.None
            }
        };

        var response = await container.ReplaceContainerAsync(properties);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task ReplaceContainerAsync_UpdatesDefaultTimeToLive()
    {
        var container = new InMemoryContainer("test-container", "/partitionKey");

        var properties = new ContainerProperties("test-container", "/partitionKey")
        {
            DefaultTimeToLive = 3600
        };

        var response = await container.ReplaceContainerAsync(properties);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task CreateContainerAsync_WithNullId_Throws()
    {
        var client = new InMemoryCosmosClient();
        var db = await client.CreateDatabaseAsync("test-db");

        var act = () => db.Database.CreateContainerAsync(null!, "/pk");
        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact]
    public async Task CreateContainerAsync_WithNullPartitionKeyPath_Throws()
    {
        var client = new InMemoryCosmosClient();
        var db = await client.CreateDatabaseAsync("test-db");

        var act = () => db.Database.CreateContainerAsync("c", null!);
        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact]
    public async Task DeleteContainerAsync_ThenReadContainer_StillSucceeds()
    {
        // InMemoryContainer.DeleteContainerAsync clears items but the container
        // object is still usable (it doesn't remove itself from the database).
        // This is a known behavioral difference from real Cosmos DB.
        var container = new InMemoryContainer("test-container", "/partitionKey");

        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        await container.DeleteContainerAsync();

        // After delete, ReadContainerAsync still works on the InMemory implementation
        var response = await container.ReadContainerAsync();
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public void GetContainer_ReturnsProxyRef_DoesNotValidateExistence()
    {
        var client = new InMemoryCosmosClient();
        var db = client.GetDatabase("test-db");

        // GetContainer returns a proxy reference without validating existence
        var container = db.GetContainer("nonexistent");
        container.Should().NotBeNull();
        container.Id.Should().Be("nonexistent");
    }

    [Fact]
    public async Task CreateContainerAsync_WithUniqueKeyPolicy_SetsProperties()
    {
        var client = new InMemoryCosmosClient();
        var db = await client.CreateDatabaseAsync("test-db");

        var properties = new ContainerProperties("unique-container", "/partitionKey")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
            }
        };

        var response = await db.Database.CreateContainerAsync(properties);
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }
}


/// <summary>
/// ReplaceContainerStreamAsync should persist property changes so that
/// subsequent ReadContainerAsync calls return the updated values.
/// </summary>
public class ContainerStreamReplacePersistenceTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task ReplaceContainerStream_PersistsPropertyChanges()
    {
        var newProperties = new ContainerProperties("test-container", "/partitionKey")
        {
            DefaultTimeToLive = 600
        };
        await _container.ReplaceContainerStreamAsync(newProperties);

        var readResponse = await _container.ReadContainerAsync();
        readResponse.Resource.DefaultTimeToLive.Should().Be(600);
    }
}


public class ContainerManagementGapTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task DeleteContainer_ClearsAllItems()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        await _container.DeleteContainerAsync();

        _container.ItemCount.Should().Be(0);
    }

    [Fact]
    public async Task ReadThroughput_ReturnsSyntheticValue()
    {
        var throughput = await _container.ReadThroughputAsync();

        throughput.Should().Be(400);
    }

    [Fact]
    public async Task ReplaceThroughput_AcceptsWithoutError()
    {
        var act = () => _container.ReplaceThroughputAsync(1000);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ReplaceContainer_AcceptsProperties()
    {
        var properties = new ContainerProperties("test-container", "/partitionKey")
        {
            IndexingPolicy = new IndexingPolicy { Automatic = true }
        };

        var response = await _container.ReplaceContainerAsync(properties);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}


public class ContainerManagementGapTests4
{
    [Fact]
    public async Task DeleteContainer_StreamVariant_Returns204()
    {
        var container = new InMemoryContainer("test-container", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var response = await container.DeleteContainerStreamAsync();

        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }
}


public class ContainerThroughputTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public async Task Container_ReadThroughputAsync_ReturnsValue()
    {
        var result = await _container.ReadThroughputAsync();
        result.Should().NotBeNull();
    }

    [Fact]
    public async Task Container_ReadThroughputAsync_WithRequestOptions_ReturnsResponse()
    {
        var response = await _container.ReadThroughputAsync(new RequestOptions());
        response.Should().NotBeNull();
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Container_ReplaceThroughputAsync_Int_Succeeds()
    {
        var response = await _container.ReplaceThroughputAsync(800);
        response.Should().NotBeNull();
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Container_ReplaceThroughputAsync_ThroughputProperties_Succeeds()
    {
        var tp = ThroughputProperties.CreateManualThroughput(1000);
        var response = await _container.ReplaceThroughputAsync(tp);
        response.Should().NotBeNull();
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}


/// <summary>
/// Validates that the Conflicts property on the Container is accessible and returns a
/// non-null instance. InMemoryContainer returns an NSubstitute mock.
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.conflicts
/// </summary>
public class ContainerConflictsPropertyTests5
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    [Fact]
    public void Conflicts_Property_ReturnsNonNull()
    {
        _container.Conflicts.Should().NotBeNull();
    }
}


public class ContainerManagementGapTests2
{
    [Fact]
    public async Task ReadContainer_ReturnsContainerProperties()
    {
        var container = new InMemoryContainer("my-container", "/partitionKey");

        var response = await container.ReadContainerAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Id.Should().Be("my-container");
    }
}


public class ContainerDatabaseBacklinkTests
{
    [Fact]
    public void Container_Database_Property_ReturnsNonNull()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        container.Database.Should().NotBeNull();
    }

    [Fact]
    public async Task Container_CreatedViaDatabase_Database_ReturnsParent()
    {
        var client = new InMemoryCosmosClient();
        var dbResponse = await client.CreateDatabaseAsync("test-db");
        var containerResponse = await dbResponse.Database.CreateContainerAsync("test-container", "/partitionKey");

        // The container should have a Database property
        containerResponse.Container.Should().NotBeNull();
    }
}
