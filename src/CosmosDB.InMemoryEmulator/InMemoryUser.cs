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
/// In-memory implementation of <see cref="User"/> for testing.
/// Manages permissions in a <see cref="ConcurrentDictionary{TKey,TValue}"/>
/// and returns synthetic responses. No actual authorization is enforced.
/// </summary>
public sealed class InMemoryUser : User
{
    private readonly ConcurrentDictionary<string, PermissionProperties> _permissions = new();
    private readonly Action _onDeleted;
    private readonly ConcurrentDictionary<string, InMemoryUser> _parentUsers;
    private string _id;

    public InMemoryUser(string id, Action onDeleted = null)
    {
        _id = id;
        _onDeleted = onDeleted;
    }

    /// <summary>
    /// Creates a proxy user that checks <paramref name="parentUsers"/> for existence on ReadAsync.
    /// If the user is not in the dictionary, ReadAsync throws 404 NotFound.
    /// </summary>
    internal InMemoryUser(string id, Action onDeleted, ConcurrentDictionary<string, InMemoryUser> parentUsers)
    {
        _id = id;
        _onDeleted = onDeleted;
        _parentUsers = parentUsers;
    }

    public override string Id => _id;

    // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/get-a-user
    //   "200 OK: The operation was successful."
    //   "404 Not Found: The user is no longer a resource, that is, the user was deleted."
    public override Task<UserResponse> ReadAsync(
        RequestOptions requestOptions = null,
        CancellationToken cancellationToken = default)
    {
        if (_parentUsers is not null && !_parentUsers.ContainsKey(_id))
        {
            throw InMemoryCosmosException.Create(
                $"User with id '{_id}' not found.",
                HttpStatusCode.NotFound, 0, Guid.NewGuid().ToString(), 0);
        }

        return Task.FromResult(BuildUserResponse(CreateUserProperties(_id), HttpStatusCode.OK));
    }

    // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/replace-a-user
    //   "The only replaceable property is the id of the user."
    //   "200 Ok: The replace operation was successful."
    //   "400 Bad Request: The JSON body is invalid."
    public override Task<UserResponse> ReplaceAsync(
        UserProperties userProperties,
        RequestOptions requestOptions = null,
        CancellationToken cancellationToken = default)
    {
        if (userProperties.Id != _id)
            throw InMemoryCosmosException.Create($"Replacing user id is not allowed.", HttpStatusCode.BadRequest, 0, string.Empty, 0);

        _id = userProperties.Id;
        return Task.FromResult(BuildUserResponse(CreateUserProperties(_id), HttpStatusCode.OK));
    }

    // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/delete-a-user
    //   "204 No Content: The delete operation was successful."
    public override Task<UserResponse> DeleteAsync(
        RequestOptions requestOptions = null,
        CancellationToken cancellationToken = default)
    {
        _onDeleted?.Invoke();
        return Task.FromResult(BuildUserResponse(null, HttpStatusCode.NoContent));
    }

    public override Permission GetPermission(string id)
    {
        return new InMemoryPermission(id, _permissions);
    }

    // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/create-a-permission
    //   "201 Created: The operation was successful."
    //   "409 Conflict: The ID provided for the new permission has been taken by an existing permission."
    public override Task<PermissionResponse> CreatePermissionAsync(
        PermissionProperties permissionProperties,
        int? tokenExpiryInSeconds = null,
        RequestOptions requestOptions = null,
        CancellationToken cancellationToken = default)
    {
        var props = InMemoryPermission.WithSyntheticMetadata(permissionProperties);
        if (!_permissions.TryAdd(props.Id, props))
            throw InMemoryCosmosException.Create($"Permission '{props.Id}' already exists.", HttpStatusCode.Conflict, 0, string.Empty, 0);

        var perm = new InMemoryPermission(props.Id, _permissions);
        var response = Substitute.For<PermissionResponse>();
        response.StatusCode.Returns(HttpStatusCode.Created);
        response.Resource.Returns(props);
        response.Permission.Returns(perm);
        return Task.FromResult(response);
    }

    // Ref: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.user.upsertpermissionasync
    //   Upsert semantics: creates if new (201 Created), replaces if existing (200 OK).
    public override Task<PermissionResponse> UpsertPermissionAsync(
        PermissionProperties permissionProperties,
        int? tokenExpiryInSeconds = null,
        RequestOptions requestOptions = null,
        CancellationToken cancellationToken = default)
    {
        var props = InMemoryPermission.WithSyntheticMetadata(permissionProperties);
        var isNew = !_permissions.ContainsKey(props.Id);
        _permissions[props.Id] = props;

        var perm = new InMemoryPermission(props.Id, _permissions);
        var response = Substitute.For<PermissionResponse>();
        response.StatusCode.Returns(isNew ? HttpStatusCode.Created : HttpStatusCode.OK);
        response.Resource.Returns(props);
        response.Permission.Returns(perm);
        return Task.FromResult(response);
    }

    // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/list-permissions
    //   "Performing a GET on the permissions URI path returns a list of permissions for the user."
    public override FeedIterator<T> GetPermissionQueryIterator<T>(
        string queryText = null,
        string continuationToken = null,
        QueryRequestOptions requestOptions = null)
    {
        return new InMemoryFeedIterator<T>(
            () => _permissions.Values.Cast<T>().ToList());
    }

    public override FeedIterator<T> GetPermissionQueryIterator<T>(
        QueryDefinition queryDefinition,
        string continuationToken = null,
        QueryRequestOptions requestOptions = null)
    {
        return GetPermissionQueryIterator<T>((string)null, continuationToken, requestOptions);
    }

    // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/get-a-user
    //   "_etag: It is a system generated property representing the resource etag required
    //    for optimistic concurrency control."
    //   "_ts: It is a system generated property. It specifies the last updated timestamp
    //    of the resource. The value is a timestamp."
    internal static UserProperties CreateUserProperties(string id)
    {
        var json = new JObject
        {
            ["id"] = id,
            ["_etag"] = $"\"{Guid.NewGuid()}\"",
            ["_ts"] = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
        };
        return JsonConvert.DeserializeObject<UserProperties>(json.ToString());
    }

    private UserResponse BuildUserResponse(UserProperties props, HttpStatusCode statusCode)
    {
        var response = Substitute.For<UserResponse>();
        response.StatusCode.Returns(statusCode);
        response.Resource.Returns(props);
        response.User.Returns(this);
        return response;
    }
}
