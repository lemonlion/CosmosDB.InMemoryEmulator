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
public class InMemoryTransactionalBatch : TransactionalBatch
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
    private readonly Dictionary<int, string> _readResults = new();

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
        var operationIndex = _operations.Count;
        _operations.Add(async () =>
        {
            var result = await _container.ReadItemAsync<object>(id, _partitionKey, ToItemRequestOptions(requestOptions));
            _readResults[operationIndex] = JsonConvert.SerializeObject(result.Resource, JsonSettings);
        });
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

        var operationResults = new List<(HttpStatusCode status, bool isSuccess)>();
        var failedIndex = -1;
        HttpStatusCode failedStatusCode = default;

        for (var i = 0; i < _operations.Count; i++)
        {
            try
            {
                await _operations[i]();
                var statusCode = _readResults.ContainsKey(i) ? HttpStatusCode.OK : HttpStatusCode.Created;
                operationResults.Add((statusCode, true));
            }
            catch (CosmosException ex)
            {
                failedIndex = i;
                failedStatusCode = ex.StatusCode;
                operationResults.Add((ex.StatusCode, false));
                break;
            }
        }

        if (failedIndex >= 0)
        {
            _container.RestoreSnapshot(itemsSnapshot, etagsSnapshot);
            for (var i = 0; i < failedIndex; i++)
            {
                operationResults[i] = (HttpStatusCode.FailedDependency, false);
            }

            for (var i = failedIndex + 1; i < _operations.Count; i++)
            {
                operationResults.Add((HttpStatusCode.FailedDependency, false));
            }

            return new InMemoryBatchResponse(failedStatusCode, false, operationResults, _readResults);
        }

        return new InMemoryBatchResponse(HttpStatusCode.OK, true, operationResults, _readResults);
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

    private sealed class InMemoryBatchResponse : TransactionalBatchResponse
    {
        private readonly HttpStatusCode _statusCode;
        private readonly bool _isSuccess;
        private readonly List<(HttpStatusCode status, bool isSuccess)> _operationResults;
        private readonly Dictionary<int, string> _readResults;

        public InMemoryBatchResponse(
            HttpStatusCode statusCode,
            bool isSuccess,
            List<(HttpStatusCode status, bool isSuccess)> operationResults,
            Dictionary<int, string> readResults)
        {
            _statusCode = statusCode;
            _isSuccess = isSuccess;
            _operationResults = operationResults;
            _readResults = readResults;
        }

        public override HttpStatusCode StatusCode => _statusCode;
        public override bool IsSuccessStatusCode => _isSuccess;
        public override int Count => _operationResults.Count;
        public override double RequestCharge => 1d;

        public override TransactionalBatchOperationResult this[int index]
        {
            get
            {
                if (index < 0 || index >= _operationResults.Count) return null!;
                var (status, isSuccess) = _operationResults[index];
                var result = Substitute.For<TransactionalBatchOperationResult>();
                result.StatusCode.Returns(status);
                result.IsSuccessStatusCode.Returns(isSuccess);
                return result;
            }
        }

        public override TransactionalBatchOperationResult<T> GetOperationResultAtIndex<T>(int index)
        {
            var (status, isSuccess) = _operationResults[index];
            var result = Substitute.For<TransactionalBatchOperationResult<T>>();
            result.StatusCode.Returns(status);
            result.IsSuccessStatusCode.Returns(isSuccess);

            if (_readResults.TryGetValue(index, out var json))
            {
                result.Resource.Returns(JsonConvert.DeserializeObject<T>(json, JsonSettings)!);
            }

            return result;
        }

        protected override void Dispose(bool disposing) { }
    }
}
