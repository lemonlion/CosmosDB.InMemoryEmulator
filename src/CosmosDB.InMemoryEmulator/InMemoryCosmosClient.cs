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

public sealed class InMemoryCosmosClient : CosmosClient
{
    private readonly ConcurrentDictionary<string, InMemoryDatabase> _databases = new();

    private static readonly Uri EmulatorEndpoint = new("https://localhost:8081/");
    private readonly CosmosClientOptions _clientOptions = new();
    private readonly CosmosResponseFactory _responseFactory = Substitute.For<CosmosResponseFactory>();

    public override Uri Endpoint => EmulatorEndpoint;
    public override CosmosClientOptions ClientOptions => _clientOptions;
    public override CosmosResponseFactory ResponseFactory => _responseFactory;

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

    public override Database GetDatabase(string id)
    {
        return _databases.GetOrAdd(id, name => new InMemoryDatabase(name, this));
    }

    public override Container GetContainer(string databaseId, string containerId)
    {
        var database = _databases.GetOrAdd(databaseId, name => new InMemoryDatabase(name, this));
        return database.GetOrCreateContainer(containerId);
    }

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
