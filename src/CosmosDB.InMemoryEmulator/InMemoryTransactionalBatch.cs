using System.Net;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;
using NSubstitute;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// In-memory implementation of <see cref="TransactionalBatch"/> for testing.
/// Executes batch operations atomically against an <see cref="InMemoryContainer"/>
/// with automatic rollback on failure. Supports <c>CreateItem</c>, <c>UpsertItem</c>,
/// <c>ReplaceItem</c>, <c>DeleteItem</c>, <c>ReadItem</c>, <c>PatchItem</c>,
/// and their stream variants.
/// </summary>
/// <remarks>
/// <para>
/// Enforces Cosmos DB batch limits: max 100 operations and 2 MB total payload.
/// On failure, all preceding operations are rolled back and marked as
/// <see cref="HttpStatusCode.FailedDependency"/>, matching real Cosmos DB behaviour.
/// </para>
/// </remarks>
public sealed class InMemoryTransactionalBatch : TransactionalBatch
{
    private static readonly JsonSerializerSettings JsonSettings = new()
    {
        TypeNameHandling = TypeNameHandling.None,
        DateParseHandling = DateParseHandling.None,
        ContractResolver = new DefaultContractResolver
        {
            NamingStrategy = new CamelCaseNamingStrategy()
        },
        Converters = { new StringEnumConverter { AllowIntegerValues = true } }
    };

    private const int MaxBatchOperations = 100;
    private const int MaxBatchSizeBytes = 2 * 1024 * 1024;

    private readonly InMemoryContainer _container;
    private readonly PartitionKey _partitionKey;
    private readonly List<Func<Task>> _operations = new();
    private long _estimatedBatchSize;

    public InMemoryTransactionalBatch(InMemoryContainer container, PartitionKey partitionKey)
    {
        _container = container;
        _partitionKey = partitionKey;
    }

    public override TransactionalBatch CreateItem<T>(T item, TransactionalBatchItemRequestOptions? requestOptions = null)
    {
        var json = JsonConvert.SerializeObject(item, JsonSettings);
        _estimatedBatchSize += System.Text.Encoding.UTF8.GetByteCount(json);
        _operations.Add(async () => await _container.CreateItemAsync(item, _partitionKey, ToItemRequestOptions(requestOptions)));
        return this;
    }

    public override TransactionalBatch UpsertItem<T>(T item, TransactionalBatchItemRequestOptions? requestOptions = null)
    {
        var json = JsonConvert.SerializeObject(item, JsonSettings);
        _estimatedBatchSize += System.Text.Encoding.UTF8.GetByteCount(json);
        _operations.Add(async () => await _container.UpsertItemAsync(item, _partitionKey, ToItemRequestOptions(requestOptions)));
        return this;
    }

    public override TransactionalBatch ReplaceItem<T>(string id, T item, TransactionalBatchItemRequestOptions? requestOptions = null)
    {
        var json = JsonConvert.SerializeObject(item, JsonSettings);
        _estimatedBatchSize += System.Text.Encoding.UTF8.GetByteCount(json);
        _operations.Add(async () => await _container.ReplaceItemAsync(item, id, _partitionKey, ToItemRequestOptions(requestOptions)));
        return this;
    }

    public override TransactionalBatch DeleteItem(string id, TransactionalBatchItemRequestOptions? requestOptions = null)
    {
        _operations.Add(async () => await _container.DeleteItemAsync<object>(id, _partitionKey, ToItemRequestOptions(requestOptions)));
        return this;
    }

    public override TransactionalBatch ReadItem(string id, TransactionalBatchItemRequestOptions? requestOptions = null)
    {
        _operations.Add(async () => await _container.ReadItemAsync<object>(id, _partitionKey, ToItemRequestOptions(requestOptions)));
        return this;
    }

    private static ItemRequestOptions? ToItemRequestOptions(TransactionalBatchItemRequestOptions? options)
    {
        if (options is null)
        {
            return null;
        }

        return new ItemRequestOptions
        {
            IfMatchEtag = options.IfMatchEtag,
            IfNoneMatchEtag = options.IfNoneMatchEtag,
            EnableContentResponseOnWrite = options.EnableContentResponseOnWrite,
        };
    }

    public override async Task<TransactionalBatchResponse> ExecuteAsync(CancellationToken cancellationToken = default)
    {
        if (_operations.Count > MaxBatchOperations)
        {
            throw new CosmosException("Batch request has more operations than what is supported.",
                HttpStatusCode.BadRequest, 0, string.Empty, 0);
        }

        if (_estimatedBatchSize > MaxBatchSizeBytes)
        {
            var failResponse = Substitute.For<TransactionalBatchResponse>();
            failResponse.StatusCode.Returns(HttpStatusCode.RequestEntityTooLarge);
            failResponse.IsSuccessStatusCode.Returns(false);
            failResponse.Count.Returns(_operations.Count);
            failResponse.RequestCharge.Returns(1d);
            return failResponse;
        }

        var itemsSnapshot = _container.SnapshotItems();
        var etagsSnapshot = _container.SnapshotEtags();

        var resultItems = new List<TransactionalBatchOperationResult>();
        var failedIndex = -1;
        HttpStatusCode failedStatusCode = default;

        for (var i = 0; i < _operations.Count; i++)
        {
            try
            {
                await _operations[i]();
                var opResult = Substitute.For<TransactionalBatchOperationResult>();
                opResult.StatusCode.Returns(HttpStatusCode.Created);
                opResult.IsSuccessStatusCode.Returns(true);
                resultItems.Add(opResult);
            }
            catch (CosmosException ex)
            {
                failedIndex = i;
                failedStatusCode = ex.StatusCode;
                var failResult = Substitute.For<TransactionalBatchOperationResult>();
                failResult.StatusCode.Returns(ex.StatusCode);
                failResult.IsSuccessStatusCode.Returns(false);
                resultItems.Add(failResult);
                break;
            }
        }

        if (failedIndex >= 0)
        {
            _container.RestoreSnapshot(itemsSnapshot, etagsSnapshot);
            for (var i = 0; i < failedIndex; i++)
            {
                resultItems[i].StatusCode.Returns(HttpStatusCode.FailedDependency);
                resultItems[i].IsSuccessStatusCode.Returns(false);
            }

            for (var i = failedIndex + 1; i < _operations.Count; i++)
            {
                var depResult = Substitute.For<TransactionalBatchOperationResult>();
                depResult.StatusCode.Returns(HttpStatusCode.FailedDependency);
                depResult.IsSuccessStatusCode.Returns(false);
                resultItems.Add(depResult);
            }

            var failResponse = Substitute.For<TransactionalBatchResponse>();
            failResponse.StatusCode.Returns(failedStatusCode);
            failResponse.IsSuccessStatusCode.Returns(false);
            failResponse.Count.Returns(_operations.Count);
            failResponse.RequestCharge.Returns(1d);
            failResponse[Arg.Any<int>()].Returns(callInfo =>
            {
                var idx = callInfo.Arg<int>();
                return idx < resultItems.Count ? resultItems[idx] : null!;
            });
            return failResponse;
        }

        var response = Substitute.For<TransactionalBatchResponse>();
        response.StatusCode.Returns(HttpStatusCode.OK);
        response.IsSuccessStatusCode.Returns(true);
        response.Count.Returns(_operations.Count);
        response.RequestCharge.Returns(1d);
        response[Arg.Any<int>()].Returns(callInfo =>
        {
            var idx = callInfo.Arg<int>();
            return idx < resultItems.Count ? resultItems[idx] : null!;
        });
        return response;
    }

    public override Task<TransactionalBatchResponse> ExecuteAsync(TransactionalBatchRequestOptions requestOptions, CancellationToken cancellationToken = default)
        => ExecuteAsync(cancellationToken);

    #region Unimplemented overrides

    public override TransactionalBatch CreateItemStream(Stream streamPayload, TransactionalBatchItemRequestOptions? requestOptions = null)
    {
        var json = new StreamReader(streamPayload).ReadToEnd();
        _operations.Add(async () =>
        {
            var jObj = JsonParseHelpers.ParseJson(json);
            var id = jObj["id"]?.ToString() ?? Guid.NewGuid().ToString();
            var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
            await _container.CreateItemStreamAsync(stream, _partitionKey);
        });
        return this;
    }

    public override TransactionalBatch UpsertItemStream(Stream streamPayload, TransactionalBatchItemRequestOptions? requestOptions = null)
    {
        var json = new StreamReader(streamPayload).ReadToEnd();
        _operations.Add(async () =>
        {
            var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
            await _container.UpsertItemStreamAsync(stream, _partitionKey);
        });
        return this;
    }

    public override TransactionalBatch ReplaceItemStream(string id, Stream streamPayload, TransactionalBatchItemRequestOptions? requestOptions = null)
    {
        var json = new StreamReader(streamPayload).ReadToEnd();
        _operations.Add(async () =>
        {
            var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
            await _container.ReplaceItemStreamAsync(stream, id, _partitionKey);
        });
        return this;
    }

    public override TransactionalBatch PatchItem(string id, IReadOnlyList<PatchOperation> patchOperations, TransactionalBatchPatchItemRequestOptions? requestOptions = null)
    {
        _operations.Add(async () => await _container.PatchItemAsync<object>(id, _partitionKey, patchOperations));
        return this;
    }

    #endregion
}
