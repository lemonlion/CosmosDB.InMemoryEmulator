#nullable disable
using System.Collections.Concurrent;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Fluent;
using NSubstitute;

namespace CosmosDB.InMemoryEmulator;

public sealed class InMemoryDatabase : Database
{
    private readonly ConcurrentDictionary<string, InMemoryContainer> _containers = new();

    public InMemoryDatabase(string id)
    {
        Id = id;
    }

    public override string Id { get; }

    public override CosmosClient Client => null;

    internal InMemoryContainer GetOrCreateContainer(string containerId, string partitionKeyPath = "/id")
    {
        return _containers.GetOrAdd(containerId, name => new InMemoryContainer(name, partitionKeyPath));
    }

    public override Task<ContainerResponse> CreateContainerIfNotExistsAsync(
        string id, string partitionKeyPath, int? throughput = null,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var isNew = !_containers.ContainsKey(id);
        var container = _containers.GetOrAdd(id, name => new InMemoryContainer(name, partitionKeyPath));
        var response = BuildContainerResponse(container, isNew ? HttpStatusCode.Created : HttpStatusCode.OK);
        return Task.FromResult(response);
    }

    public override Task<ContainerResponse> CreateContainerIfNotExistsAsync(
        ContainerProperties containerProperties, int? throughput = null,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var id = containerProperties.Id;
        var pkPath = containerProperties.PartitionKeyPath ?? "/id";
        return CreateContainerIfNotExistsAsync(id, pkPath, throughput, requestOptions, cancellationToken);
    }

    public override Task<ContainerResponse> CreateContainerAsync(
        string id, string partitionKeyPath, int? throughput = null,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var container = _containers.GetOrAdd(id, name => new InMemoryContainer(name, partitionKeyPath));
        var response = BuildContainerResponse(container, HttpStatusCode.Created);
        return Task.FromResult(response);
    }

    public override Task<ContainerResponse> CreateContainerAsync(
        ContainerProperties containerProperties, int? throughput = null,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var id = containerProperties.Id;
        var pkPath = containerProperties.PartitionKeyPath ?? "/id";
        return CreateContainerAsync(id, pkPath, throughput, requestOptions, cancellationToken);
    }

    public override Container GetContainer(string id)
    {
        return GetOrCreateContainer(id);
    }

    public override Task<DatabaseResponse> ReadAsync(
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var response = Substitute.For<DatabaseResponse>();
        response.Database.Returns(this);
        response.StatusCode.Returns(HttpStatusCode.OK);
        response.Resource.Returns(new DatabaseProperties(Id));
        return Task.FromResult(response);
    }

    public override Task<DatabaseResponse> DeleteAsync(
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var response = Substitute.For<DatabaseResponse>();
        response.StatusCode.Returns(HttpStatusCode.NoContent);
        return Task.FromResult(response);
    }

    private static ContainerResponse BuildContainerResponse(Container container, HttpStatusCode statusCode)
    {
        var response = Substitute.For<ContainerResponse>();
        response.Container.Returns(container);
        response.StatusCode.Returns(statusCode);
        response.Resource.Returns(new ContainerProperties(container.Id, "/id"));
        return response;
    }

    // ── Not implemented overrides ───────────────────────────────────────────

    public override Task<int?> ReadThroughputAsync(CancellationToken cancellationToken = default)
        => Task.FromResult<int?>(null);

    public override Task<ThroughputResponse> ReadThroughputAsync(RequestOptions requestOptions, CancellationToken cancellationToken = default)
        => throw new System.NotImplementedException();

    public override Task<ThroughputResponse> ReplaceThroughputAsync(int throughput, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
        => throw new System.NotImplementedException();

    public override Task<ThroughputResponse> ReplaceThroughputAsync(ThroughputProperties throughputProperties, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
        => throw new System.NotImplementedException();

    public override Task<ResponseMessage> CreateContainerStreamAsync(ContainerProperties containerProperties, int? throughput = null, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
        => throw new System.NotImplementedException();

    public override Task<ContainerResponse> CreateContainerIfNotExistsAsync(ContainerProperties containerProperties, ThroughputProperties throughputProperties, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
        => throw new System.NotImplementedException();

    public override Task<ContainerResponse> CreateContainerAsync(ContainerProperties containerProperties, ThroughputProperties throughputProperties, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
        => throw new System.NotImplementedException();

    public override FeedIterator<T> GetContainerQueryIterator<T>(QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
        => throw new System.NotImplementedException();

    public override FeedIterator GetContainerQueryStreamIterator(QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
        => throw new System.NotImplementedException();

    public override FeedIterator<T> GetContainerQueryIterator<T>(string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
        => throw new System.NotImplementedException();

    public override FeedIterator GetContainerQueryStreamIterator(string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
        => throw new System.NotImplementedException();

    public override Task<ResponseMessage> ReadStreamAsync(RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
        => throw new System.NotImplementedException();

    public override Task<ResponseMessage> DeleteStreamAsync(RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
        => throw new System.NotImplementedException();

    public override User GetUser(string id) => throw new System.NotImplementedException();
    public override Task<UserResponse> CreateUserAsync(string id, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
        => throw new System.NotImplementedException();
    public override Task<UserResponse> UpsertUserAsync(string id, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
        => throw new System.NotImplementedException();
    public override FeedIterator<T> GetUserQueryIterator<T>(string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
        => throw new System.NotImplementedException();
    public override FeedIterator<T> GetUserQueryIterator<T>(QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
        => throw new System.NotImplementedException();
    public override ClientEncryptionKey GetClientEncryptionKey(string id) => throw new System.NotImplementedException();
    public override FeedIterator<ClientEncryptionKeyProperties> GetClientEncryptionKeyQueryIterator(QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
        => throw new System.NotImplementedException();
    public override Task<ClientEncryptionKeyResponse> CreateClientEncryptionKeyAsync(ClientEncryptionKeyProperties clientEncryptionKeyProperties, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
        => throw new System.NotImplementedException();
    public override ContainerBuilder DefineContainer(string name, string partitionKeyPath)
        => throw new System.NotImplementedException();
    public override Task<ResponseMessage> CreateContainerStreamAsync(ContainerProperties containerProperties, ThroughputProperties throughputProperties, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
        => throw new System.NotImplementedException();
}
