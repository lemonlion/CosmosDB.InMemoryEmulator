#nullable disable
using System.Collections.Concurrent;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using NSubstitute;

namespace CosmosDB.InMemoryEmulator;

public sealed class InMemoryCosmosClient : CosmosClient
{
    private readonly ConcurrentDictionary<string, InMemoryDatabase> _databases = new();

    public override Task<DatabaseResponse> CreateDatabaseIfNotExistsAsync(
        string id, int? throughput = null, RequestOptions requestOptions = null,
        CancellationToken cancellationToken = default)
    {
        var isNew = !_databases.ContainsKey(id);
        var database = _databases.GetOrAdd(id, name => new InMemoryDatabase(name));
        var response = BuildDatabaseResponse(database, isNew ? HttpStatusCode.Created : HttpStatusCode.OK);
        return Task.FromResult(response);
    }

    public override Task<DatabaseResponse> CreateDatabaseAsync(
        string id, int? throughput = null, RequestOptions requestOptions = null,
        CancellationToken cancellationToken = default)
    {
        var database = _databases.GetOrAdd(id, name => new InMemoryDatabase(name));
        var response = BuildDatabaseResponse(database, HttpStatusCode.Created);
        return Task.FromResult(response);
    }

    public override Database GetDatabase(string id)
    {
        return _databases.GetOrAdd(id, name => new InMemoryDatabase(name));
    }

    public override Container GetContainer(string databaseId, string containerId)
    {
        var database = _databases.GetOrAdd(databaseId, name => new InMemoryDatabase(name));
        return database.GetOrCreateContainer(containerId);
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
