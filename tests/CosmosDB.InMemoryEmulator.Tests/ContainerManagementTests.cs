using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;

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
