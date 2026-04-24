#nullable disable
#pragma warning disable CS0618 // InMemoryCosmosClient is obsolete but InMemoryDatabase still depends on it
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
internal class InMemoryDatabase : Database
{
    private readonly ConcurrentDictionary<string, InMemoryContainer> _containers = new();
    private readonly ConcurrentDictionary<string, bool> _explicitlyCreatedContainers = new();
    private readonly ConcurrentDictionary<string, InMemoryUser> _users = new();
    private readonly InMemoryCosmosClient _client;
    // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/create-a-collection
    //   "The user specified manual throughput (RU/s) for the collection expressed in units of
    //    100 request units per second. The minimum is 400 up to 1,000,000 (or higher by
    //    requesting a limit increase)."
    private int _throughput = 400;

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
        var isNew = false;
        var container = _containers.GetOrAdd(containerId, name => { isNew = true; return new InMemoryContainer(name, partitionKeyPath); });
        if (isNew)
            container.ExplicitlyCreated = false;
        container.OnDeleted ??= () => _containers.TryRemove(containerId, out _);
        container.SetParentDatabase(Id);
        return container;
    }

    internal InMemoryContainer GetOrCreateContainer(ContainerProperties containerProperties)
    {
        var isNew = false;
        var container = _containers.GetOrAdd(containerProperties.Id, _ => { isNew = true; return new InMemoryContainer(containerProperties); });
        if (isNew)
            container.ExplicitlyCreated = false;
        container.OnDeleted ??= () => _containers.TryRemove(containerProperties.Id, out _);
        container.SetParentDatabase(Id);
        return container;
    }

    // ── CreateContainerIfNotExistsAsync ─────────────────────────────────────

    // Ref: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.database.createcontainerifnotexistsasync
    //   "Check if a container exists, and if it doesn't, create it. Only the container id is
    //    used to verify if there is an existing container. Other container properties such as
    //    throughput are not validated and can be different then the passed properties."
    //   StatusCode: 201 Created - New container is created. 200 OK - Container already exists.
    public override Task<ContainerResponse> CreateContainerIfNotExistsAsync(
        string id, string partitionKeyPath, int? throughput = null,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var created = false;
        var container = _containers.GetOrAdd(id, name => { created = true; return new InMemoryContainer(name, partitionKeyPath); });
        container.OnDeleted ??= () => _containers.TryRemove(id, out _);
        container.SetParentDatabase(Id);
        container.ExplicitlyCreated = true;
        _explicitlyCreatedContainers.TryAdd(id, true);
        var response = BuildContainerResponse(container, partitionKeyPath, created ? HttpStatusCode.Created : HttpStatusCode.OK);
        return Task.FromResult(response);
    }

    public override Task<ContainerResponse> CreateContainerIfNotExistsAsync(
        ContainerProperties containerProperties, int? throughput = null,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var id = containerProperties.Id;
        // TODO: No official source found — needs verification
        //   Defaulting partition key path to "/id" when not specified is an emulator convenience;
        //   the REST API requires a partitionKey definition for API version 2018-12-31 and higher.
        if (string.IsNullOrEmpty(containerProperties.PartitionKeyPath) && containerProperties.PartitionKeyPaths is null)
            containerProperties.PartitionKeyPath = "/id";
        var created = false;
        var container = _containers.GetOrAdd(id, _ => { created = true; return new InMemoryContainer(containerProperties); });
        container.OnDeleted ??= () => _containers.TryRemove(id, out _);
        container.SetParentDatabase(Id);
        container.ExplicitlyCreated = true;
        _explicitlyCreatedContainers.TryAdd(id, true);
        if (created)
        {
            container.DefaultTimeToLive = containerProperties.DefaultTimeToLive;
            if (containerProperties.IndexingPolicy is not null)
                container.IndexingPolicy = containerProperties.IndexingPolicy;
        }
        var response = BuildContainerResponse(container, containerProperties, created ? HttpStatusCode.Created : HttpStatusCode.OK);
        return Task.FromResult(response);
    }

    public override Task<ContainerResponse> CreateContainerIfNotExistsAsync(
        ContainerProperties containerProperties, ThroughputProperties throughputProperties,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        return CreateContainerIfNotExistsAsync(containerProperties, (int?)null, requestOptions, cancellationToken);
    }

    // ── CreateContainerAsync ────────────────────────────────────────────────

    // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/create-a-collection
    //   "201 Created - The operation was successful."
    //   "409 Conflict - The ID provided for the new collection has been taken by an
    //    existing collection."
    public override Task<ContainerResponse> CreateContainerAsync(
        string id, string partitionKeyPath, int? throughput = null,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);
        ArgumentException.ThrowIfNullOrEmpty(id);
        InMemoryCosmosClient.ValidateResourceName(id, "Container");
        ArgumentNullException.ThrowIfNull(partitionKeyPath);
        var container = new InMemoryContainer(id, partitionKeyPath);
        container.OnDeleted = () => _containers.TryRemove(id, out _);
        container.SetParentDatabase(Id);
        container.ExplicitlyCreated = true;
        if (!_containers.TryAdd(id, container))
        {
            // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/create-a-collection
            //   "409 Conflict - The ID provided for the new collection has been taken by an
            //    existing collection."
            throw InMemoryCosmosException.Create("Container already exists.", HttpStatusCode.Conflict, 0, string.Empty, 0);
        }
        _explicitlyCreatedContainers.TryAdd(id, true);
        var response = BuildContainerResponse(container, partitionKeyPath, HttpStatusCode.Created);
        return Task.FromResult(response);
    }

    public override Task<ContainerResponse> CreateContainerAsync(
        ContainerProperties containerProperties, int? throughput = null,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var id = containerProperties.Id;
        InMemoryCosmosClient.ValidateResourceName(id, "Container");
        // TODO: No official source found — needs verification
        //   Defaulting partition key path to "/id" when not specified is an emulator convenience;
        //   the REST API requires a partitionKey definition for API version 2018-12-31 and higher.
        if (string.IsNullOrEmpty(containerProperties.PartitionKeyPath) && containerProperties.PartitionKeyPaths is null)
            containerProperties.PartitionKeyPath = "/id";
        var container = new InMemoryContainer(containerProperties);
        container.OnDeleted = () => _containers.TryRemove(id, out _);
        container.SetParentDatabase(Id);
        container.ExplicitlyCreated = true;
        container.DefaultTimeToLive = containerProperties.DefaultTimeToLive;
        if (containerProperties.IndexingPolicy is not null)
            container.IndexingPolicy = containerProperties.IndexingPolicy;
        if (!_containers.TryAdd(id, container))
        {
            // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/create-a-collection
            //   "409 Conflict - The ID provided for the new collection has been taken by an
            //    existing collection."
            throw InMemoryCosmosException.Create("Container already exists.", HttpStatusCode.Conflict, 0, string.Empty, 0);
        }
        _explicitlyCreatedContainers.TryAdd(id, true);
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

    // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/create-a-collection
    //   "201 Created - The operation was successful."
    //   "409 Conflict - The ID provided for the new collection has been taken by an
    //    existing collection."
    public override Task<ResponseMessage> CreateContainerStreamAsync(
        ContainerProperties containerProperties, int? throughput = null,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var id = containerProperties.Id;
        // TODO: No official source found — needs verification
        //   Defaulting partition key path to "/id" when not specified is an emulator convenience.
        if (string.IsNullOrEmpty(containerProperties.PartitionKeyPath) && containerProperties.PartitionKeyPaths is null)
            containerProperties.PartitionKeyPath = "/id";
        var container = new InMemoryContainer(containerProperties);
        container.OnDeleted = () => _containers.TryRemove(id, out _);
        container.SetParentDatabase(Id);
        container.ExplicitlyCreated = true;
        container.DefaultTimeToLive = containerProperties.DefaultTimeToLive;
        if (containerProperties.IndexingPolicy is not null)
            container.IndexingPolicy = containerProperties.IndexingPolicy;
        if (!_containers.TryAdd(id, container))
        {
            return Task.FromResult(CreateStreamResponse(HttpStatusCode.Conflict));
        }
        _explicitlyCreatedContainers.TryAdd(id, true);
        return Task.FromResult(CreateStreamResponse(HttpStatusCode.Created));
    }

    public override Task<ResponseMessage> CreateContainerStreamAsync(
        ContainerProperties containerProperties, ThroughputProperties throughputProperties,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        return CreateContainerStreamAsync(containerProperties, (int?)null, requestOptions, cancellationToken);
    }

    // ── GetContainer ────────────────────────────────────────────────────────

    // Ref: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.database.getcontainer
    //   "Returns a Container reference. Reference doesn't guarantee existence.
    //    Please ensure container already exists or is created through a create operation."
    public override Container GetContainer(string id)
    {
        return GetOrCreateContainer(id);
    }

    internal bool IsContainerExplicitlyCreated(string id)
    {
        return _explicitlyCreatedContainers.ContainsKey(id);
    }

    internal IEnumerable<InMemoryContainer> GetAllContainers() => _containers.Values;

    // ── GetContainerQueryIterator ───────────────────────────────────────────

    // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/list-collections
    //   "Performing a GET on the collections resource of a particular database, i.e. the colls
    //    URI path, returns a list of the collections in the database."
    //   Status code: "200 OK - The operation was successful."
    public override FeedIterator<T> GetContainerQueryIterator<T>(
        string queryText = null, string continuationToken = null,
        QueryRequestOptions requestOptions = null)
    {
        var offset = int.TryParse(continuationToken, out var o) ? o : 0;
        IEnumerable<ContainerProperties> items = _containers.Values
            .Select(c => new ContainerProperties(c.Id, c.PartitionKeyPaths));
        var idFilter = InMemoryCosmosClient.ExtractIdFilter(queryText);
        if (idFilter is not null)
            items = items.Where(cp => string.Equals(cp.Id, idFilter, StringComparison.Ordinal));
        return new InMemoryFeedIterator<T>(items.Select(cp => (T)(object)cp).ToList(), requestOptions?.MaxItemCount, offset);
    }

    public override FeedIterator<T> GetContainerQueryIterator<T>(
        QueryDefinition queryDefinition, string continuationToken = null,
        QueryRequestOptions requestOptions = null)
    {
        return GetContainerQueryIterator<T>(queryDefinition?.QueryText, continuationToken, requestOptions);
    }

    // ── Read / Delete ───────────────────────────────────────────────────────

    // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/get-a-database
    //   "200 Ok - The operation was successful."
    //   "404 Not Found - The database is no longer a resource, i.e. the resource was deleted."
    public override Task<DatabaseResponse> ReadAsync(
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        if (_client != null && !_client.IsDatabaseExplicitlyCreated(Id))
        {
            // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/get-a-database
            //   "404 Not Found - The database is no longer a resource, i.e. the resource was deleted."
            throw InMemoryCosmosException.Create($"Database '{Id}' not found.", HttpStatusCode.NotFound, 1003, Guid.NewGuid().ToString(), 0);
        }
        var response = Substitute.For<DatabaseResponse>();
        response.Database.Returns(this);
        response.StatusCode.Returns(HttpStatusCode.OK);
        response.Resource.Returns(new DatabaseProperties(Id));
        return Task.FromResult(response);
    }

    public override Task<ResponseMessage> ReadStreamAsync(
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(CreateStreamResponse(HttpStatusCode.OK));
    }

    // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/delete-a-database
    //   "204 No Content - The delete operation was successful."
    //   "Performing a DELETE on a database deletes the database resource and its child
    //    resources, that is, collections, documents, attachments, stored procedures,
    //    triggers, user-defined functions, users, and permissions within the database."
    public override Task<DatabaseResponse> DeleteAsync(
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        foreach (var container in _containers.Values)
            container.DeleteContainerAsync().GetAwaiter().GetResult();
        _containers.Clear();
        _explicitlyCreatedContainers.Clear();
        _users.Clear();
        _client?.RemoveDatabase(Id);
        var response = Substitute.For<DatabaseResponse>();
        response.StatusCode.Returns(HttpStatusCode.NoContent);
        return Task.FromResult(response);
    }

    // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/delete-a-database
    //   "204 No Content - The delete operation was successful."
    public override Task<ResponseMessage> DeleteStreamAsync(
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        foreach (var container in _containers.Values)
            container.DeleteContainerAsync().GetAwaiter().GetResult();
        _containers.Clear();
        _explicitlyCreatedContainers.Clear();
        _users.Clear();
        _client?.RemoveDatabase(Id);
        return Task.FromResult(CreateStreamResponse(HttpStatusCode.NoContent));
    }

    private static ResponseMessage CreateStreamResponse(HttpStatusCode statusCode)
    {
        var msg = new ResponseMessage(statusCode);
        msg.Headers["x-ms-activity-id"] = Guid.NewGuid().ToString();
        msg.Headers["x-ms-request-charge"] = "1";
        msg.Headers["x-ms-session-token"] = "0:0#0";
        return msg;
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

    // Ref: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.database.readthroughputasync
    //   "Gets database throughput in measurement of request units per second in the
    //    Azure Cosmos service."
    //   StatusCode 404: "NotFound - This means the database does not exist or has no
    //    throughput assigned."
    public override Task<int?> ReadThroughputAsync(CancellationToken cancellationToken = default)
        => Task.FromResult<int?>(_throughput);

    public override Task<ThroughputResponse> ReadThroughputAsync(RequestOptions requestOptions, CancellationToken cancellationToken = default)
    {
        var response = Substitute.For<ThroughputResponse>();
        response.StatusCode.Returns(HttpStatusCode.OK);
        response.Resource.Returns(ThroughputProperties.CreateManualThroughput(_throughput));
        return Task.FromResult(response);
    }

    // Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/set-throughput
    //   "After you create an Azure Cosmos DB container or a database, you can update the
    //    provisioned throughput."
    public override Task<ThroughputResponse> ReplaceThroughputAsync(int throughput, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        _throughput = throughput;
        var response = Substitute.For<ThroughputResponse>();
        response.StatusCode.Returns(HttpStatusCode.OK);
        response.Resource.Returns(ThroughputProperties.CreateManualThroughput(throughput));
        return Task.FromResult(response);
    }

    public override Task<ThroughputResponse> ReplaceThroughputAsync(ThroughputProperties throughputProperties, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        if (throughputProperties?.Throughput.HasValue == true)
            _throughput = throughputProperties.Throughput.Value;
        var response = Substitute.For<ThroughputResponse>();
        response.StatusCode.Returns(HttpStatusCode.OK);
        response.Resource.Returns(throughputProperties);
        return Task.FromResult(response);
    }

    // ── Stream query iterators ──────────────────────────────────────────────

    // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/list-collections
    //   "returns a list of the collections in the database"
    //   Response body property: "DocumentCollections - This property is the array containing
    //    the collections returned as part of the list operation."
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

    // Ref: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.database.definecontainer
    //   "Creates a containerBuilder."
    public override ContainerBuilder DefineContainer(string name, string partitionKeyPath)
        => new ContainerBuilder(this, name, partitionKeyPath);

    // ── User management (stub store — no authorization enforced) ───────────

    // Ref: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.database.getuser
    //   "Returns a User reference. Reference doesn't guarantee existence. Please ensure
    //    user already exists or is created through a create operation."
    public override User GetUser(string id)
    {
        if (_users.TryGetValue(id, out var existing))
            return existing;

        // Return a proxy that is NOT registered in _users.
        // ReadAsync will check _users and throw 404 if not explicitly created.
        return new InMemoryUser(id, () => _users.TryRemove(id, out _), _users);
    }

    // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/create-a-user
    //   "201 Created - The operation was successful."
    //   "409 Conflict - The ID provided for the new user has been taken by an existing user."
    public override Task<UserResponse> CreateUserAsync(string id, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var user = new InMemoryUser(id, () => _users.TryRemove(id, out _));
        if (!_users.TryAdd(id, user))
            // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/create-a-user
            //   "409 Conflict - The ID provided for the new user has been taken by an existing user."
            throw InMemoryCosmosException.Create($"User '{id}' already exists.", HttpStatusCode.Conflict, 0, string.Empty, 0);

        var response = Substitute.For<UserResponse>();
        response.StatusCode.Returns(HttpStatusCode.Created);
        response.Resource.Returns(new UserProperties(id));
        response.User.Returns(user);
        return Task.FromResult(response);
    }

    // Ref: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.database.upsertuserasync
    //   "Upserts a user as an asynchronous operation in the Azure Cosmos service."
    // TODO: No official source found for UpsertUser status codes — needs verification.
    //   The SDK docs do not specify 201/200 status codes for upsert. The 201 Created / 200 OK
    //   pattern here mirrors standard Cosmos DB upsert semantics observed on the emulator.
    public override Task<UserResponse> UpsertUserAsync(string id, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var created = false;
        var user = _users.GetOrAdd(id, uid => { created = true; return new InMemoryUser(uid, () => _users.TryRemove(uid, out _)); });

        var response = Substitute.For<UserResponse>();
        response.StatusCode.Returns(created ? HttpStatusCode.Created : HttpStatusCode.OK);
        response.Resource.Returns(new UserProperties(id));
        response.User.Returns(user);
        return Task.FromResult(response);
    }

    // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/list-users
    //   "To return a list of the users under a database, performing a GET operation on
    //    the users resource of a particular database."
    //   Status code: "200 Ok - The operation was successful."
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
