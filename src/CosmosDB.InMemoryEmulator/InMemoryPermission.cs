#nullable disable
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NSubstitute;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// In-memory implementation of <see cref="Permission"/> for testing.
/// Stores permission properties and returns synthetic responses.
/// No actual authorization is enforced.
/// </summary>
public sealed class InMemoryPermission : Permission
{
    private readonly ConcurrentDictionary<string, PermissionProperties> _parentPermissions;

    public InMemoryPermission(string id, ConcurrentDictionary<string, PermissionProperties> parentPermissions)
    {
        Id = id;
        _parentPermissions = parentPermissions;
    }

    public override string Id { get; }

    // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/get-a-permission
    //   "200 Ok: The operation was successful."
    //   "404 Not Found: The permission is no longer a resource, that is, the permission was deleted."
    public override Task<PermissionResponse> ReadAsync(
        int? tokenExpiryInSeconds = null,
        RequestOptions requestOptions = null,
        CancellationToken cancellationToken = default)
    {
        if (!_parentPermissions.TryGetValue(Id, out var props))
            throw InMemoryCosmosException.Create($"Permission '{Id}' not found.", HttpStatusCode.NotFound, 0, string.Empty, 0);

        return Task.FromResult(BuildPermissionResponse(props, HttpStatusCode.OK));
    }

    // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/replace-a-permission
    //   "200 Ok: The replace operation was successful."
    //   "404 Not Found: The user to be replaced is no longer a resource, that is, the permission was deleted."
    public override Task<PermissionResponse> ReplaceAsync(
        PermissionProperties permissionProperties,
        int? tokenExpiryInSeconds = null,
        RequestOptions requestOptions = null,
        CancellationToken cancellationToken = default)
    {
        if (!_parentPermissions.ContainsKey(Id))
            throw InMemoryCosmosException.Create($"Permission '{Id}' not found.", HttpStatusCode.NotFound, 0, string.Empty, 0);

        var updated = WithSyntheticMetadata(permissionProperties);
        _parentPermissions[Id] = updated;
        return Task.FromResult(BuildPermissionResponse(updated, HttpStatusCode.OK));
    }

    // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/delete-a-permission
    //   "204 No Content: The delete operation was successful."
    //   "404 Not Found: The permission to be deleted is no longer a resource, i.e. the permission was deleted."
    public override Task<PermissionResponse> DeleteAsync(
        RequestOptions requestOptions = null,
        CancellationToken cancellationToken = default)
    {
        if (!_parentPermissions.TryRemove(Id, out _))
            throw InMemoryCosmosException.Create($"Permission '{Id}' not found.", HttpStatusCode.NotFound, 0, string.Empty, 0);

        return Task.FromResult(BuildPermissionResponse(null, HttpStatusCode.NoContent));
    }

    // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/create-a-permission
    //   "_etag: It is a system generated property that represents the resource etag
    //    required for optimistic concurrency control."
    //   "_ts: It is a system generated property. It specifies the last updated timestamp
    //    of the resource. The value is a timestamp."
    //   "_token: It is a system generated resource token for the particular resource and user."
    internal static PermissionProperties WithSyntheticMetadata(PermissionProperties source)
    {
        // PermissionProperties has private setters for ETag, Token, LastModified.
        // Roundtrip through JSON to set them.
        var json = JObject.FromObject(source);
        json["_etag"] = $"\"{Guid.NewGuid()}\"";
        json["_ts"] = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        json["_token"] = $"type=resource&ver=1&sig=stub_{source.Id}";
        return JsonConvert.DeserializeObject<PermissionProperties>(json.ToString());
    }

    private PermissionResponse BuildPermissionResponse(PermissionProperties props, HttpStatusCode statusCode)
    {
        var response = Substitute.For<PermissionResponse>();
        response.StatusCode.Returns(statusCode);
        response.Resource.Returns(props);
        response.Permission.Returns(this);
        return response;
    }
}
