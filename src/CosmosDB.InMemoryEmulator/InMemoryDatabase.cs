#nullable disable
using System.Collections.Concurrent;
using System.Linq;
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
    private readonly InMemoryCosmosClient _client;

    public InMemoryDatabase(string id) : this(id, null) { }

    public InMemoryDatabase(string id, InMemoryCosmosClient client)
    {
        Id = id;
        _client = client;
    }

    public override string Id { get; }

    public override CosmosClient Client => _client;

    internal InMemoryContainer GetOrCreateContainer(string containerId, string partitionKeyPath = "/id")
    {
        return _containers.GetOrAdd(containerId, name => new InMemoryContainer(name, partitionKeyPath));
    }

    // ── CreateContainerIfNotExistsAsync ─────────────────────────────────────

    public override Task<ContainerResponse> CreateContainerIfNotExistsAsync(
        string id, string partitionKeyPath, int? throughput = null,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var isNew = !_containers.ContainsKey(id);
        var container = _containers.GetOrAdd(id, name => new InMemoryContainer(name, partitionKeyPath));
        var response = BuildContainerResponse(container, partitionKeyPath, isNew ? HttpStatusCode.Created : HttpStatusCode.OK);
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

    public override Task<ContainerResponse> CreateContainerIfNotExistsAsync(
        ContainerProperties containerProperties, ThroughputProperties throughputProperties,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        return CreateContainerIfNotExistsAsync(containerProperties, (int?)null, requestOptions, cancellationToken);
    }

    // ── CreateContainerAsync ────────────────────────────────────────────────

    public override Task<ContainerResponse> CreateContainerAsync(
        string id, string partitionKeyPath, int? throughput = null,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var container = new InMemoryContainer(id, partitionKeyPath);
        if (!_containers.TryAdd(id, container))
        {
            throw new CosmosException("Container already exists.", HttpStatusCode.Conflict, 0, string.Empty, 0);
        }
        var response = BuildContainerResponse(container, partitionKeyPath, HttpStatusCode.Created);
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

    public override Task<ContainerResponse> CreateContainerAsync(
        ContainerProperties containerProperties, ThroughputProperties throughputProperties,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        return CreateContainerAsync(containerProperties, (int?)null, requestOptions, cancellationToken);
    }

    // ── CreateContainerStreamAsync ──────────────────────────────────────────

    public override Task<ResponseMessage> CreateContainerStreamAsync(
        ContainerProperties containerProperties, int? throughput = null,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var id = containerProperties.Id;
        var pkPath = containerProperties.PartitionKeyPath ?? "/id";
        var container = new InMemoryContainer(id, pkPath);
        if (!_containers.TryAdd(id, container))
        {
            return Task.FromResult(new ResponseMessage(HttpStatusCode.Conflict));
        }
        return Task.FromResult(new ResponseMessage(HttpStatusCode.Created));
    }

    public override Task<ResponseMessage> CreateContainerStreamAsync(
        ContainerProperties containerProperties, ThroughputProperties throughputProperties,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        return CreateContainerStreamAsync(containerProperties, (int?)null, requestOptions, cancellationToken);
    }

    // ── GetContainer ────────────────────────────────────────────────────────

    public override Container GetContainer(string id)
    {
        return GetOrCreateContainer(id);
    }

    // ── GetContainerQueryIterator ───────────────────────────────────────────

    public override FeedIterator<T> GetContainerQueryIterator<T>(
        string queryText = null, string continuationToken = null,
        QueryRequestOptions requestOptions = null)
    {
        return new InMemoryFeedIterator<T>(
            () => _containers.Values
                .Select(c => (T)(object)new ContainerProperties(c.Id, "/id"))
                .ToList());
    }

    public override FeedIterator<T> GetContainerQueryIterator<T>(
        QueryDefinition queryDefinition, string continuationToken = null,
        QueryRequestOptions requestOptions = null)
    {
        return GetContainerQueryIterator<T>((string)null, continuationToken, requestOptions);
    }

    // ── Read / Delete ───────────────────────────────────────────────────────

    public override Task<DatabaseResponse> ReadAsync(
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var response = Substitute.For<DatabaseResponse>();
        response.Database.Returns(this);
        response.StatusCode.Returns(HttpStatusCode.OK);
        response.Resource.Returns(new DatabaseProperties(Id));
        return Task.FromResult(response);
    }

    public override Task<ResponseMessage> ReadStreamAsync(
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(new ResponseMessage(HttpStatusCode.OK));
    }

    public override Task<DatabaseResponse> DeleteAsync(
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        _containers.Clear();
        _client?.RemoveDatabase(Id);
        var response = Substitute.For<DatabaseResponse>();
        response.StatusCode.Returns(HttpStatusCode.NoContent);
        return Task.FromResult(response);
    }

    public override Task<ResponseMessage> DeleteStreamAsync(
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        _containers.Clear();
        _client?.RemoveDatabase(Id);
        return Task.FromResult(new ResponseMessage(HttpStatusCode.NoContent));
    }

    // ── Response builder (reuses NSubstitute pattern from BuildDatabaseResponse) ─

    private static ContainerResponse BuildContainerResponse(Container container, string partitionKeyPath, HttpStatusCode statusCode)
    {
        var response = Substitute.For<ContainerResponse>();
        response.Container.Returns(container);
        response.StatusCode.Returns(statusCode);
        response.Resource.Returns(new ContainerProperties(container.Id, partitionKeyPath ?? "/id"));
        return response;
    }

    // ── Throughput (not meaningful for in-memory, but returns sensible defaults) ─

    public override Task<int?> ReadThroughputAsync(CancellationToken cancellationToken = default)
        => Task.FromResult<int?>(400);

    public override Task<ThroughputResponse> ReadThroughputAsync(RequestOptions requestOptions, CancellationToken cancellationToken = default)
    {
        var response = Substitute.For<ThroughputResponse>();
        response.StatusCode.Returns(HttpStatusCode.OK);
        response.Resource.Returns(ThroughputProperties.CreateManualThroughput(400));
        return Task.FromResult(response);
    }

    public override Task<ThroughputResponse> ReplaceThroughputAsync(int throughput, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var response = Substitute.For<ThroughputResponse>();
        response.StatusCode.Returns(HttpStatusCode.OK);
        response.Resource.Returns(ThroughputProperties.CreateManualThroughput(throughput));
        return Task.FromResult(response);
    }

    public override Task<ThroughputResponse> ReplaceThroughputAsync(ThroughputProperties throughputProperties, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var response = Substitute.For<ThroughputResponse>();
        response.StatusCode.Returns(HttpStatusCode.OK);
        response.Resource.Returns(throughputProperties);
        return Task.FromResult(response);
    }

    // ── Stream query iterators ──────────────────────────────────────────────

    public override FeedIterator GetContainerQueryStreamIterator(QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
        => GetContainerQueryStreamIterator((string)null, continuationToken, requestOptions);

    public override FeedIterator GetContainerQueryStreamIterator(string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
    {
        return new InMemoryStreamFeedIterator(
            () => _containers.Values
                .Select(c => (object)new { id = c.Id })
                .ToList(),
            "DocumentCollections");
    }

    // ── DefineContainer (fluent builder) ────────────────────────────────────

    public override ContainerBuilder DefineContainer(string name, string partitionKeyPath)
        => new ContainerBuilder(this, name, partitionKeyPath);

    // ── Not implemented overrides (users, encryption) ───────────────────────
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
}
