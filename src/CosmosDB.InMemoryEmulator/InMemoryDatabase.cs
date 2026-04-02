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

/// <summary>
/// In-memory implementation of <see cref="Database"/> for testing.
/// Manages a collection of <see cref="InMemoryContainer"/> instances.
/// Containers are created lazily via <see cref="GetContainer"/> or explicitly
/// via <see cref="CreateContainerAsync"/> / <see cref="CreateContainerIfNotExistsAsync"/>.
/// </summary>
/// <remarks>
/// Throughput operations return synthetic values (400 RU/s by default).
/// User operations return stub responses with synthetic metadata.
/// Client encryption key operations throw <see cref="System.NotImplementedException"/>.
/// </remarks>
public class InMemoryDatabase : Database
{
    private readonly ConcurrentDictionary<string, InMemoryContainer> _containers = new();
    private readonly ConcurrentDictionary<string, InMemoryUser> _users = new();
    private readonly InMemoryCosmosClient _client;

    /// <summary>
    /// Creates a new <see cref="InMemoryDatabase"/> with no parent client.
    /// </summary>
    /// <param name="id">The database identifier.</param>
    public InMemoryDatabase(string id) : this(id, null) { }

    /// <summary>
    /// Creates a new <see cref="InMemoryDatabase"/> owned by the given <paramref name="client"/>.
    /// </summary>
    /// <param name="id">The database identifier.</param>
    /// <param name="client">The owning <see cref="InMemoryCosmosClient"/>, or null.</param>
    public InMemoryDatabase(string id, InMemoryCosmosClient client)
    {
        Id = id;
        _client = client;
    }

    /// <summary>The database identifier.</summary>
    public override string Id { get; }

    /// <summary>The owning <see cref="InMemoryCosmosClient"/>, or null.</summary>
    public override CosmosClient Client => _client;

    /// <summary>
    /// Gets or creates an <see cref="InMemoryContainer"/> with the given identifier and partition key path.
    /// Used internally by DI extensions and <see cref="GetContainer"/>.
    /// </summary>
    /// <param name="containerId">The container identifier.</param>
    /// <param name="partitionKeyPath">The JSON path to the partition key field (e.g. <c>/partitionKey</c>).</param>
    internal InMemoryContainer GetOrCreateContainer(string containerId, string partitionKeyPath = "/id")
    {
        var container = _containers.GetOrAdd(containerId, name => new InMemoryContainer(name, partitionKeyPath));
        container.OnDeleted ??= () => _containers.TryRemove(containerId, out _);
        return container;
    }

    // ── CreateContainerIfNotExistsAsync ─────────────────────────────────────

    public override Task<ContainerResponse> CreateContainerIfNotExistsAsync(
        string id, string partitionKeyPath, int? throughput = null,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var isNew = !_containers.ContainsKey(id);
        var container = _containers.GetOrAdd(id, name => new InMemoryContainer(name, partitionKeyPath));
        container.OnDeleted ??= () => _containers.TryRemove(id, out _);
        var response = BuildContainerResponse(container, partitionKeyPath, isNew ? HttpStatusCode.Created : HttpStatusCode.OK);
        return Task.FromResult(response);
    }

    public override Task<ContainerResponse> CreateContainerIfNotExistsAsync(
        ContainerProperties containerProperties, int? throughput = null,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var id = containerProperties.Id;
        if (string.IsNullOrEmpty(containerProperties.PartitionKeyPath) && containerProperties.PartitionKeyPaths is null)
            containerProperties.PartitionKeyPath = "/id";
        var isNew = !_containers.ContainsKey(id);
        var container = _containers.GetOrAdd(id, _ => new InMemoryContainer(containerProperties));
        container.OnDeleted ??= () => _containers.TryRemove(id, out _);
        if (isNew)
        {
            container.DefaultTimeToLive = containerProperties.DefaultTimeToLive;
            if (containerProperties.IndexingPolicy is not null)
                container.IndexingPolicy = containerProperties.IndexingPolicy;
        }
        var response = BuildContainerResponse(container, containerProperties, isNew ? HttpStatusCode.Created : HttpStatusCode.OK);
        return Task.FromResult(response);
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
        ArgumentNullException.ThrowIfNull(id);
        ArgumentException.ThrowIfNullOrEmpty(id);
        ArgumentNullException.ThrowIfNull(partitionKeyPath);
        var container = new InMemoryContainer(id, partitionKeyPath);
        container.OnDeleted = () => _containers.TryRemove(id, out _);
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
        if (string.IsNullOrEmpty(containerProperties.PartitionKeyPath) && containerProperties.PartitionKeyPaths is null)
            containerProperties.PartitionKeyPath = "/id";
        var container = new InMemoryContainer(containerProperties);
        container.OnDeleted = () => _containers.TryRemove(id, out _);
        container.DefaultTimeToLive = containerProperties.DefaultTimeToLive;
        if (containerProperties.IndexingPolicy is not null)
            container.IndexingPolicy = containerProperties.IndexingPolicy;
        if (!_containers.TryAdd(id, container))
        {
            throw new CosmosException("Container already exists.", HttpStatusCode.Conflict, 0, string.Empty, 0);
        }
        var response = BuildContainerResponse(container, containerProperties, HttpStatusCode.Created);
        return Task.FromResult(response);
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
        if (string.IsNullOrEmpty(containerProperties.PartitionKeyPath) && containerProperties.PartitionKeyPaths is null)
            containerProperties.PartitionKeyPath = "/id";
        var container = new InMemoryContainer(containerProperties);
        container.OnDeleted = () => _containers.TryRemove(id, out _);
        container.DefaultTimeToLive = containerProperties.DefaultTimeToLive;
        if (containerProperties.IndexingPolicy is not null)
            container.IndexingPolicy = containerProperties.IndexingPolicy;
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
                .Select(c => (T)(object)new ContainerProperties(c.Id, c.PartitionKeyPaths))
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
        _users.Clear();
        _client?.RemoveDatabase(Id);
        var response = Substitute.For<DatabaseResponse>();
        response.StatusCode.Returns(HttpStatusCode.NoContent);
        return Task.FromResult(response);
    }

    public override Task<ResponseMessage> DeleteStreamAsync(
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        _containers.Clear();
        _users.Clear();
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

    private static ContainerResponse BuildContainerResponse(Container container, ContainerProperties properties, HttpStatusCode statusCode)
    {
        var response = Substitute.For<ContainerResponse>();
        response.Container.Returns(container);
        response.StatusCode.Returns(statusCode);
        response.Resource.Returns(properties);
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

    // ── User management (stub store — no authorization enforced) ───────────

    public override User GetUser(string id)
    {
        return _users.GetOrAdd(id, uid => new InMemoryUser(uid, () => _users.TryRemove(uid, out _)));
    }

    public override Task<UserResponse> CreateUserAsync(string id, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var user = new InMemoryUser(id, () => _users.TryRemove(id, out _));
        if (!_users.TryAdd(id, user))
            throw new CosmosException($"User '{id}' already exists.", HttpStatusCode.Conflict, 0, string.Empty, 0);

        var response = Substitute.For<UserResponse>();
        response.StatusCode.Returns(HttpStatusCode.Created);
        response.Resource.Returns(new UserProperties(id));
        response.User.Returns(user);
        return Task.FromResult(response);
    }

    public override Task<UserResponse> UpsertUserAsync(string id, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var isNew = !_users.ContainsKey(id);
        var user = _users.GetOrAdd(id, uid => new InMemoryUser(uid, () => _users.TryRemove(uid, out _)));

        var response = Substitute.For<UserResponse>();
        response.StatusCode.Returns(isNew ? HttpStatusCode.Created : HttpStatusCode.OK);
        response.Resource.Returns(new UserProperties(id));
        response.User.Returns(user);
        return Task.FromResult(response);
    }

    public override FeedIterator<T> GetUserQueryIterator<T>(string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
    {
        return new InMemoryFeedIterator<T>(
            () => _users.Values
                .Select(u => (T)(object)new UserProperties(u.Id))
                .ToList());
    }

    public override FeedIterator<T> GetUserQueryIterator<T>(QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
    {
        return GetUserQueryIterator<T>((string)null, continuationToken, requestOptions);
    }

    // ── Not implemented overrides (encryption) ──────────────────────────────
    public override ClientEncryptionKey GetClientEncryptionKey(string id) => throw new System.NotImplementedException();
    public override FeedIterator<ClientEncryptionKeyProperties> GetClientEncryptionKeyQueryIterator(QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
        => throw new System.NotImplementedException();
    public override Task<ClientEncryptionKeyResponse> CreateClientEncryptionKeyAsync(ClientEncryptionKeyProperties clientEncryptionKeyProperties, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
        => throw new System.NotImplementedException();
}
