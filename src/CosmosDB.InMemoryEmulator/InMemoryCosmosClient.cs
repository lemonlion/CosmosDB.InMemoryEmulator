#nullable disable
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using NSubstitute;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// In-memory implementation of <see cref="CosmosClient"/> for testing.
/// Manages a collection of <see cref="InMemoryDatabase"/> instances, each containing
/// <see cref="InMemoryContainer"/> instances. Databases and containers are created
/// lazily on first access via <see cref="GetDatabase"/> or <see cref="GetContainer"/>.
/// </summary>
/// <remarks>
/// <para>
/// This class is intentionally <b>not sealed</b> to support Pattern 2 (typed CosmosClient subclasses).
/// In test projects, you can create one-liner subclasses that shadow production typed clients:
/// <code>
/// // Production code (unchanged):
/// public class BiometricCosmosClient : CosmosClient { ... }
///
/// // Test project shadow type:
/// public class BiometricCosmosClient : InMemoryCosmosClient { }
/// </code>
/// </para>
/// <para>
/// The <see cref="Endpoint"/> returns <c>https://localhost:8081/</c> and throughput/RU values
/// are synthetic. No network calls are made.
/// </para>
/// </remarks>
public class InMemoryCosmosClient : CosmosClient
{
    private readonly ConcurrentDictionary<string, InMemoryDatabase> _databases = new();

    private static readonly Uri EmulatorEndpoint = new("https://localhost:8081/");
    private readonly CosmosClientOptions _clientOptions = new();
    private readonly CosmosResponseFactory _responseFactory = Substitute.For<CosmosResponseFactory>();

    /// <summary>Returns <c>https://localhost:8081/</c>.</summary>
    public override Uri Endpoint => EmulatorEndpoint;

    /// <summary>Returns a default <see cref="CosmosClientOptions"/> instance.</summary>
    public override CosmosClientOptions ClientOptions => _clientOptions;

    /// <summary>Returns a stubbed <see cref="CosmosResponseFactory"/>.</summary>
    public override CosmosResponseFactory ResponseFactory => _responseFactory;

    /// <summary>
    /// Creates a database if it does not already exist. Returns <see cref="HttpStatusCode.Created"/>
    /// for new databases or <see cref="HttpStatusCode.OK"/> for existing ones.
    /// </summary>
    /// <param name="id">The database identifier.</param>
    /// <param name="throughput">Ignored. Throughput is not enforced in-memory.</param>
    /// <param name="requestOptions">Ignored.</param>
    /// <param name="cancellationToken">Ignored.</param>
    public override Task<DatabaseResponse> CreateDatabaseIfNotExistsAsync(
        string id, int? throughput = null, RequestOptions requestOptions = null,
        CancellationToken cancellationToken = default)
    {
        var isNew = !_databases.ContainsKey(id);
        var database = _databases.GetOrAdd(id, name => new InMemoryDatabase(name, this));
        var response = BuildDatabaseResponse(database, isNew ? HttpStatusCode.Created : HttpStatusCode.OK);
        return Task.FromResult(response);
    }

    public override Task<DatabaseResponse> CreateDatabaseIfNotExistsAsync(
        string id, ThroughputProperties throughputProperties, RequestOptions requestOptions = null,
        CancellationToken cancellationToken = default)
    {
        return CreateDatabaseIfNotExistsAsync(id, (int?)null, requestOptions, cancellationToken);
    }

    /// <summary>
    /// Creates a new database. Throws <see cref="CosmosException"/> with
    /// <see cref="HttpStatusCode.Conflict"/> if a database with the same <paramref name="id"/> already exists.
    /// </summary>
    /// <param name="id">The database identifier.</param>
    /// <param name="throughput">Ignored. Throughput is not enforced in-memory.</param>
    /// <param name="requestOptions">Ignored.</param>
    /// <param name="cancellationToken">Ignored.</param>
    public override Task<DatabaseResponse> CreateDatabaseAsync(
        string id, int? throughput = null, RequestOptions requestOptions = null,
        CancellationToken cancellationToken = default)
    {
        var database = new InMemoryDatabase(id, this);
        if (!_databases.TryAdd(id, database))
        {
            throw new CosmosException("Database already exists.", HttpStatusCode.Conflict, 0, string.Empty, 0);
        }
        var response = BuildDatabaseResponse(database, HttpStatusCode.Created);
        return Task.FromResult(response);
    }

    public override Task<DatabaseResponse> CreateDatabaseAsync(
        string id, ThroughputProperties throughputProperties, RequestOptions requestOptions = null,
        CancellationToken cancellationToken = default)
    {
        return CreateDatabaseAsync(id, (int?)null, requestOptions, cancellationToken);
    }

    /// <summary>
    /// Creates a new database using stream semantics. Returns <see cref="HttpStatusCode.Created"/>
    /// on success or <see cref="HttpStatusCode.Conflict"/> if the database already exists.
    /// </summary>
    public override Task<ResponseMessage> CreateDatabaseStreamAsync(
        DatabaseProperties databaseProperties, int? throughput = null,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var id = databaseProperties.Id;
        var database = new InMemoryDatabase(id, this);
        if (!_databases.TryAdd(id, database))
        {
            return Task.FromResult(new ResponseMessage(HttpStatusCode.Conflict));
        }
        return Task.FromResult(new ResponseMessage(HttpStatusCode.Created));
    }

    /// <summary>
    /// Gets or creates an <see cref="InMemoryDatabase"/> with the given <paramref name="id"/>.
    /// The database is created lazily on first access.
    /// </summary>
    /// <param name="id">The database identifier.</param>
    public override Database GetDatabase(string id)
    {
        return _databases.GetOrAdd(id, name => new InMemoryDatabase(name, this));
    }

    /// <summary>
    /// Gets or creates a container within the specified database. Both the database and
    /// container are created lazily on first access.
    /// </summary>
    /// <param name="databaseId">The database identifier.</param>
    /// <param name="containerId">The container identifier.</param>
    public override Container GetContainer(string databaseId, string containerId)
    {
        var database = _databases.GetOrAdd(databaseId, name => new InMemoryDatabase(name, this));
        return database.GetOrCreateContainer(containerId);
    }

    /// <summary>
    /// Returns a feed iterator over all databases. The query text is ignored;
    /// all databases are returned as <see cref="DatabaseProperties"/>.
    /// </summary>
    public override FeedIterator<T> GetDatabaseQueryIterator<T>(
        string queryText = null, string continuationToken = null,
        QueryRequestOptions requestOptions = null)
    {
        return new InMemoryFeedIterator<T>(
            () => _databases.Values
                .Select(db => new DatabaseProperties(db.Id))
                .Cast<T>()
                .ToList());
    }

    public override FeedIterator<T> GetDatabaseQueryIterator<T>(
        QueryDefinition queryDefinition, string continuationToken = null,
        QueryRequestOptions requestOptions = null)
    {
        return GetDatabaseQueryIterator<T>((string)null, continuationToken, requestOptions);
    }

    public override FeedIterator GetDatabaseQueryStreamIterator(
        string queryText = null, string continuationToken = null,
        QueryRequestOptions requestOptions = null)
    {
        return new InMemoryStreamFeedIterator(
            () => _databases.Values
                .Select(db => new { id = db.Id })
                .ToList(),
            "Databases");
    }

    public override FeedIterator GetDatabaseQueryStreamIterator(
        QueryDefinition queryDefinition, string continuationToken = null,
        QueryRequestOptions requestOptions = null)
    {
        return GetDatabaseQueryStreamIterator((string)null, continuationToken, requestOptions);
    }

    /// <summary>
    /// Returns synthetic <see cref="AccountProperties"/> with Id "in-memory-emulator".
    /// Falls back to an NSubstitute stub if reflection fails.
    /// </summary>
    public override Task<AccountProperties> ReadAccountAsync()
    {
        try
        {
            var account = (AccountProperties)Activator.CreateInstance(
                typeof(AccountProperties),
                nonPublic: true);
            var idProp = typeof(AccountProperties).GetProperty(nameof(AccountProperties.Id));
            idProp?.SetValue(account, "in-memory-emulator");
            return Task.FromResult(account);
        }
        catch (Exception ex)
        {
            Trace.TraceWarning(
                "InMemoryCosmosClient: AccountProperties reflection failed " +
                $"({ex.GetType().Name}: {ex.Message}). " +
                "Falling back to NSubstitute stub.");
            var stub = Substitute.For<AccountProperties>();
            return Task.FromResult(stub);
        }
    }

    internal bool RemoveDatabase(string id)
    {
        return _databases.TryRemove(id, out _);
    }

    /// <summary>
    /// No-op disposal. Prevents <see cref="NullReferenceException"/> from the base class.
    /// </summary>
    protected override void Dispose(bool disposing)
    {
        // No-op — prevent NullReferenceException from base class.
    }

    private static DatabaseResponse BuildDatabaseResponse(Database database, HttpStatusCode statusCode)
    {
        var response = Substitute.For<DatabaseResponse>();
        response.Database.Returns(database);
        response.StatusCode.Returns(statusCode);
        response.Resource.Returns(new DatabaseProperties(database.Id));
        return response;
    }
}
