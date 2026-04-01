#nullable disable
using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using NSubstitute;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// In-memory implementation of <see cref="Container"/> for testing. Supports the full
/// breadth of the Cosmos DB SDK surface area: CRUD, SQL queries (40+ built-in functions),
/// LINQ, transactional batches, change feed, patch operations, ETags, TTL, ReadMany,
/// stream APIs, stored procedures, UDFs, state persistence, and more.
/// </summary>
/// <remarks>
/// <para>
/// Create an instance directly for simple unit tests, or use the DI extension methods
/// (<see cref="ServiceCollectionExtensions"/>) for integration tests.
/// For HTTP-level interception with zero production code changes, wrap this container
/// in a <see cref="FakeCosmosHandler"/>.
/// </para>
/// <para>
/// Thread-safe for concurrent reads and writes. All data is stored in
/// <see cref="ConcurrentDictionary{TKey,TValue}"/> collections keyed by (id, partitionKey).
/// </para>
/// </remarks>
public class InMemoryContainer : Container
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

    private static readonly HashSet<string> AggregateFunctions =
        new(StringComparer.OrdinalIgnoreCase) { "COUNT", "SUM", "AVG", "MIN", "MAX" };

    private const int RegexCacheMaxSize = 256;
    private static readonly ConcurrentDictionary<(string Pattern, RegexOptions Options), Regex> RegexCache = new();

    private const int MaxDocumentSizeBytes = 2 * 1024 * 1024;
    private const double SyntheticRequestCharge = 1.0;
    private const double EarthRadiusMeters = 6_371_000.0;

    private readonly ConcurrentDictionary<(string Id, string PartitionKey), string> _items = new();
    private readonly ConcurrentDictionary<(string Id, string PartitionKey), string> _etags = new();
    private readonly ConcurrentDictionary<(string Id, string PartitionKey), DateTimeOffset> _timestamps = new();
    private readonly List<(DateTimeOffset Timestamp, string Id, string PartitionKey, string Json, bool IsDelete)> _changeFeed = new();
    private readonly object _changeFeedLock = new();
    private ContainerProperties _containerProperties;
    private readonly Scripts _scripts;
    private readonly Dictionary<string, Func<object[], object>> _userDefinedFunctions = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, Func<PartitionKey, dynamic[], string>> _storedProcedures = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, RegisteredTrigger> _triggers = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, TriggerProperties> _triggerProperties = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Optional JavaScript trigger engine. When set, triggers that have a <see cref="TriggerProperties.Body"/>
    /// but no C# handler will be executed via this engine. Set by calling <c>UseJsTriggers()</c>
    /// from the <c>CosmosDB.InMemoryEmulator.JsTriggers</c> package.
    /// </summary>
    public IJsTriggerEngine JsTriggerEngine { get; set; }

    private sealed record RegisteredTrigger(
        TriggerType TriggerType,
        TriggerOperation TriggerOperation,
        Func<JObject, JObject> PreHandler,
        Action<JObject> PostHandler);

    private sealed class UndefinedValue
    {
        public static readonly UndefinedValue Instance = new();
        public override string ToString() => null;
        private UndefinedValue() { }
    }

    internal Action OnDeleted { get; set; }

    /// <summary>
    /// Creates a new <see cref="InMemoryContainer"/> with a single partition key path.
    /// </summary>
    /// <param name="id">The container identifier. Defaults to <c>"in-memory-container"</c>.</param>
    /// <param name="partitionKeyPath">The JSON path to the partition key field (e.g. <c>/partitionKey</c>). Defaults to <c>/id</c>.</param>
    public InMemoryContainer(string id = "in-memory-container", string partitionKeyPath = "/id")
    {
        Id = id;
        _containerProperties = new ContainerProperties(id, partitionKeyPath);
        PartitionKeyPaths = new[] { partitionKeyPath };
        _scripts = ConfigureScripts();
    }

    /// <summary>
    /// Creates a new <see cref="InMemoryContainer"/> with composite (hierarchical) partition key paths.
    /// </summary>
    /// <param name="id">The container identifier.</param>
    /// <param name="partitionKeyPaths">One or more JSON paths for the composite partition key.</param>
    public InMemoryContainer(string id, IReadOnlyList<string> partitionKeyPaths)
    {
        Id = id;
        PartitionKeyPaths = partitionKeyPaths;
        _containerProperties = new ContainerProperties(id, partitionKeyPaths);
        _scripts = ConfigureScripts();
    }

    /// <summary>
    /// Creates a new <see cref="InMemoryContainer"/> from a <see cref="ContainerProperties"/> instance.
    /// This allows specifying advanced settings such as <see cref="UniqueKeyPolicy"/>.
    /// </summary>
    /// <param name="containerProperties">The container properties to use.</param>
    public InMemoryContainer(ContainerProperties containerProperties)
    {
        _containerProperties = containerProperties;
        Id = containerProperties.Id;
        PartitionKeyPaths = containerProperties.PartitionKeyPaths ?? new[] { containerProperties.PartitionKeyPath };
        _scripts = ConfigureScripts();
    }

    // ─── Properties ───────────────────────────────────────────────────────────

    /// <summary>The container identifier.</summary>
    public override string Id { get; } = default!;

    /// <summary>Returns a stubbed <see cref="Microsoft.Azure.Cosmos.Database"/> instance.</summary>
    public override Database Database => Substitute.For<Database>();

    /// <summary>Returns a stubbed <see cref="Microsoft.Azure.Cosmos.Conflicts"/> instance.</summary>
    public override Conflicts Conflicts => Substitute.For<Conflicts>();

    /// <summary>The <see cref="Microsoft.Azure.Cosmos.Scripts.Scripts"/> instance for executing stored procedures and UDFs.</summary>
    public override Scripts Scripts => _scripts;

    /// <summary>
    /// Container-level default TTL in seconds. When set, items expire after this duration
    /// unless overridden by a per-item <c>_ttl</c> property. Set to <c>null</c> to disable.
    /// Items are lazily evicted on the next read attempt.
    /// </summary>
    public int? DefaultTimeToLive { get; set; }

    /// <summary>The partition key path(s) for this container.</summary>
    public IReadOnlyList<string> PartitionKeyPaths { get; }

    /// <summary>
    /// Number of feed ranges returned by <see cref="GetFeedRangesAsync"/>. Defaults to 1.
    /// Set to a higher value to simulate multiple physical partitions so that
    /// FeedRange-scoped queries and change feed iterators return subsets of data.
    /// </summary>
    public int FeedRangeCount { get; set; } = 1;

    // ─── Public helpers for test infrastructure ───────────────────────────────

    /// <summary>
    /// Removes all items, ETags, timestamps, and change feed entries from the container.
    /// </summary>
    public void ClearItems()
    {
        _items.Clear();
        _etags.Clear();
        _timestamps.Clear();
        lock (_changeFeedLock) { _changeFeed.Clear(); }
    }

    /// <summary>Returns the number of items currently stored in the container.</summary>
    public int ItemCount => _items.Count;

    // ─── State persistence ────────────────────────────────────────────────────

    /// <summary>
    /// Exports the current container state as a JSON string.
    /// </summary>
    public string ExportState()
    {
        var items = _items.Select(kvp => JsonParseHelpers.ParseJson(kvp.Value)).ToList();
        var state = new JObject { ["items"] = new JArray(items) };
        return state.ToString(Formatting.Indented);
    }

    /// <summary>
    /// Imports container state from a JSON string, replacing all existing data.
    /// </summary>
    public void ImportState(string json)
    {
        var state = JObject.Parse(json);
        ClearItems();

        if (state["items"] is JArray items)
        {
            foreach (var item in items)
            {
                var itemJson = item.ToString(Formatting.None);
                var id = item["id"]?.ToString() ?? "";
                var jObj = JsonParseHelpers.ParseJson(itemJson);
                var pk = ExtractPartitionKeyValue(null, jObj);
                var key = (id, pk);
                var importEtag = $"\"{ Guid.NewGuid()}\"";
                _etags[key] = importEtag;
                _timestamps[key] = DateTimeOffset.UtcNow;
                _items[key] = EnrichWithSystemProperties(itemJson, importEtag, _timestamps[key]);
            }
        }
    }

    /// <summary>
    /// Exports the current container state to a file.
    /// </summary>
    public void ExportStateToFile(string filePath)
    {
        File.WriteAllText(filePath, ExportState());
    }

    /// <summary>
    /// Imports container state from a file, replacing all existing data.
    /// </summary>
    public void ImportStateFromFile(string filePath)
    {
        ImportState(File.ReadAllText(filePath));
    }

    // ─── Point-in-time restore ────────────────────────────────────────────────

    /// <summary>
    /// Restores the container to its state at the specified point in time by replaying
    /// the change feed. All current data is replaced with the reconstructed state.
    /// </summary>
    /// <param name="pointInTime">The timestamp to restore to. Only changes recorded
    /// at or before this time are included.</param>
    public void RestoreToPointInTime(DateTimeOffset pointInTime)
    {
        List<(DateTimeOffset Timestamp, string Id, string PartitionKey, string Json, bool IsDelete)> feedSnapshot;
        lock (_changeFeedLock)
        {
            feedSnapshot = _changeFeed
                .Where(e => e.Timestamp <= pointInTime)
                .ToList();
        }

        // Replay: keep the last entry per (Id, PartitionKey), skip if it was a delete
        var lastPerKey = new Dictionary<(string Id, string PartitionKey), (string Json, bool IsDelete)>();
        foreach (var entry in feedSnapshot)
        {
            lastPerKey[(entry.Id, entry.PartitionKey)] = (entry.Json, entry.IsDelete);
        }

        _items.Clear();
        _etags.Clear();
        _timestamps.Clear();

        foreach (var kvp in lastPerKey)
        {
            if (kvp.Value.IsDelete) continue;

            var key = kvp.Key;
            var etag = $"\"{Guid.NewGuid()}\"";
            _items[key] = kvp.Value.Json;
            _etags[key] = etag;
            _timestamps[key] = pointInTime;
        }
    }

    // ─── IndexingPolicy ───────────────────────────────────────────────────────

    /// <summary>
    /// The indexing policy for this container. Accepted and stored but does not affect
    /// query performance — all queries scan all items regardless of indexing settings.
    /// </summary>
    public IndexingPolicy IndexingPolicy { get; set; } = new()
    {
        Automatic = true,
        IndexingMode = IndexingMode.Consistent,
        IncludedPaths = { new IncludedPath { Path = "/*" } },
    };

    /// <summary>
    /// Registers a user-defined function that can be called in SQL queries as <c>udf.name(args)</c>.
    /// </summary>
    public void RegisterUdf(string name, Func<object[], object> implementation)
    {
        _userDefinedFunctions["UDF." + name.TrimStart('.')] = implementation;
    }

    /// <summary>
    /// Registers a stored procedure handler that is invoked when <c>ExecuteStoredProcedureAsync</c> is called.
    /// </summary>
    public void RegisterStoredProcedure(string sprocId, Func<PartitionKey, dynamic[], string> handler)
    {
        _storedProcedures[sprocId] = handler;
    }

    /// <summary>
    /// Removes a previously registered stored procedure handler.
    /// </summary>
    public void DeregisterStoredProcedure(string sprocId)
    {
        _storedProcedures.Remove(sprocId);
    }

    /// <summary>
    /// Registers a pre-trigger handler invoked when <c>ItemRequestOptions.PreTriggers</c> includes this trigger's ID.
    /// The handler receives the document as a <see cref="JObject"/> and must return the (possibly modified) document.
    /// </summary>
    public void RegisterTrigger(string triggerId, TriggerType triggerType, TriggerOperation triggerOperation,
        Func<JObject, JObject> preHandler)
    {
        _triggers[triggerId] = new RegisteredTrigger(triggerType, triggerOperation, preHandler, null);
    }

    /// <summary>
    /// Registers a post-trigger handler invoked when <c>ItemRequestOptions.PostTriggers</c> includes this trigger's ID.
    /// The handler receives the committed document as a <see cref="JObject"/>.
    /// If the handler throws, the write is rolled back (matching real Cosmos DB transactional semantics).
    /// </summary>
    public void RegisterTrigger(string triggerId, TriggerType triggerType, TriggerOperation triggerOperation,
        Action<JObject> postHandler)
    {
        _triggers[triggerId] = new RegisteredTrigger(triggerType, triggerOperation, null, postHandler);
    }

    /// <summary>
    /// Removes a previously registered trigger handler.
    /// </summary>
    public void DeregisterTrigger(string triggerId)
    {
        _triggers.Remove(triggerId);
    }

    /// <summary>
    /// Returns a checkpoint value representing the current position in the change feed.
    /// Pass this to <see cref="GetChangeFeedIterator{T}(long)"/> to read changes since this point.
    /// </summary>
    public long GetChangeFeedCheckpoint()
    {
        lock (_changeFeedLock)
        {
            return _changeFeed.Count;
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Item CRUD — Typed
    // ═══════════════════════════════════════════════════════════════════════════

    public override Task<ItemResponse<T>> CreateItemAsync<T>(
        T item, PartitionKey? partitionKey = null,
        ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var json = JsonConvert.SerializeObject(item, JsonSettings);
        ValidateDocumentSize(json);
        var jObj = JsonParseHelpers.ParseJson(json);

        jObj = ExecutePreTriggers(requestOptions, jObj, "Create");
        json = jObj.ToString(Newtonsoft.Json.Formatting.None);

        var itemId = jObj["id"]?.ToString() ?? throw new InvalidOperationException("Item must have an 'id' property.");
        var pk = ExtractPartitionKeyValue(partitionKey, jObj);
        var key = ItemKey(itemId, pk);

        ValidateUniqueKeys(jObj, pk);

        if (!_items.TryAdd(key, json))
        {
            throw new CosmosException($"Entity with the specified id already exists in the system. id = {itemId}",
                HttpStatusCode.Conflict, 0, string.Empty, 0);
        }

        var etag = GenerateETag();
        _etags[key] = etag;
        _timestamps[key] = DateTimeOffset.UtcNow;
        _items[key] = EnrichWithSystemProperties(json, etag, _timestamps[key]);
        RecordChangeFeed(itemId, pk, _items[key]);

        try
        {
            ExecutePostTriggers(requestOptions, JsonParseHelpers.ParseJson(_items[key]), "Create");
        }
        catch
        {
            _items.TryRemove(key, out _);
            _etags.TryRemove(key, out _);
            _timestamps.TryRemove(key, out _);
            throw;
        }

        var suppressContent = requestOptions?.EnableContentResponseOnWrite == false;
        return Task.FromResult(CreateItemResponse(item, HttpStatusCode.Created, etag, suppressContent));
    }

    public override Task<ItemResponse<T>> ReadItemAsync<T>(
        string id, PartitionKey partitionKey,
        ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var pk = PartitionKeyToString(partitionKey);
        var key = ItemKey(id, pk);

        if (!_items.TryGetValue(key, out var json) || IsExpired(key))
        {
            EvictIfExpired(key);
            throw new CosmosException($"Entity with the specified id or name already exists. id = {id}",
                HttpStatusCode.NotFound, 0, string.Empty, 0);
        }

        CheckIfNoneMatch(requestOptions, key);
        var etag = _etags.GetValueOrDefault(key);
        var result = JsonConvert.DeserializeObject<T>(json, JsonSettings);
        return Task.FromResult(CreateItemResponse(result, HttpStatusCode.OK, etag));
    }

    public override Task<ItemResponse<T>> UpsertItemAsync<T>(
        T item, PartitionKey? partitionKey = null,
        ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var json = JsonConvert.SerializeObject(item, JsonSettings);
        ValidateDocumentSize(json);
        var jObj = JsonParseHelpers.ParseJson(json);

        jObj = ExecutePreTriggers(requestOptions, jObj, "Upsert");
        json = jObj.ToString(Newtonsoft.Json.Formatting.None);

        var itemId = jObj["id"]?.ToString() ?? throw new InvalidOperationException("Item must have an 'id' property.");
        var pk = ExtractPartitionKeyValue(partitionKey, jObj);
        var key = ItemKey(itemId, pk);

        if (requestOptions?.IfMatchEtag is not null && !_items.ContainsKey(key))
        {
            throw new CosmosException($"Entity with the specified id does not exist. id = {itemId}",
                HttpStatusCode.NotFound, 0, string.Empty, 0);
        }

        CheckIfMatch(requestOptions, key);
        ValidateUniqueKeys(jObj, pk, excludeItemId: itemId);
        var existed = _items.ContainsKey(key);
        var previousJson = existed ? _items[key] : null;
        var previousEtag = existed ? _etags.GetValueOrDefault(key) : null;
        var previousTimestamp = existed ? _timestamps.GetValueOrDefault(key) : default(DateTimeOffset?);
        var etag = GenerateETag();
        _etags[key] = etag;
        _timestamps[key] = DateTimeOffset.UtcNow;
        _items[key] = EnrichWithSystemProperties(json, etag, _timestamps[key]);
        RecordChangeFeed(itemId, pk, _items[key]);

        try
        {
            ExecutePostTriggers(requestOptions, JsonParseHelpers.ParseJson(_items[key]), "Upsert");
        }
        catch
        {
            if (existed && previousJson is not null)
            {
                _items[key] = previousJson;
                _etags[key] = previousEtag!;
                if (previousTimestamp.HasValue) _timestamps[key] = previousTimestamp.Value;
            }
            else
            {
                _items.TryRemove(key, out _);
                _etags.TryRemove(key, out _);
                _timestamps.TryRemove(key, out _);
            }
            throw;
        }

        var suppressContent = requestOptions?.EnableContentResponseOnWrite == false;
        return Task.FromResult(CreateItemResponse(item, existed ? HttpStatusCode.OK : HttpStatusCode.Created, etag, suppressContent));
    }

    public override Task<ItemResponse<T>> ReplaceItemAsync<T>(
        T item, string id, PartitionKey? partitionKey = null,
        ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var json = JsonConvert.SerializeObject(item, JsonSettings);
        ValidateDocumentSize(json);
        var jObj = JsonParseHelpers.ParseJson(json);

        jObj = ExecutePreTriggers(requestOptions, jObj, "Replace");
        json = jObj.ToString(Newtonsoft.Json.Formatting.None);

        var pk = ExtractPartitionKeyValue(partitionKey, jObj);
        var key = ItemKey(id, pk);

        if (!_items.ContainsKey(key))
        {
            throw new CosmosException($"Entity with the specified id does not exist. id = {id}",
                HttpStatusCode.NotFound, 0, string.Empty, 0);
        }

        CheckIfMatch(requestOptions, key);
        ValidateUniqueKeys(jObj, pk, excludeItemId: id);
        var previousJson = _items[key];
        var previousEtag = _etags.GetValueOrDefault(key);
        var previousTimestamp = _timestamps.GetValueOrDefault(key);
        var etag = GenerateETag();
        _etags[key] = etag;
        _timestamps[key] = DateTimeOffset.UtcNow;
        _items[key] = EnrichWithSystemProperties(json, etag, _timestamps[key]);
        RecordChangeFeed(id, pk, _items[key]);

        try
        {
            ExecutePostTriggers(requestOptions, JsonParseHelpers.ParseJson(_items[key]), "Replace");
        }
        catch
        {
            _items[key] = previousJson;
            _etags[key] = previousEtag!;
            _timestamps[key] = previousTimestamp;
            throw;
        }

        var suppressContent = requestOptions?.EnableContentResponseOnWrite == false;
        return Task.FromResult(CreateItemResponse(item, HttpStatusCode.OK, etag, suppressContent));
    }

    public override Task<ItemResponse<T>> DeleteItemAsync<T>(
        string id, PartitionKey partitionKey,
        ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var pk = PartitionKeyToString(partitionKey);
        var key = ItemKey(id, pk);

        if (!_items.ContainsKey(key))
        {
            throw new CosmosException($"Entity with the specified id does not exist. id = {id}",
                HttpStatusCode.NotFound, 0, string.Empty, 0);
        }

        CheckIfMatch(requestOptions, key);
        _items.TryRemove(key, out _);
        _etags.TryRemove(key, out _);
        _timestamps.TryRemove(key, out _);
        RecordDeleteTombstone(id, pk);
        return Task.FromResult(CreateItemResponse(default(T), HttpStatusCode.NoContent));
    }

    public override Task<ItemResponse<T>> PatchItemAsync<T>(
        string id, PartitionKey partitionKey,
        IReadOnlyList<PatchOperation> patchOperations,
        PatchItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (patchOperations is null || patchOperations.Count == 0)
        {
            throw new CosmosException("Patch request has no operations.",
                HttpStatusCode.BadRequest, 0, string.Empty, 0);
        }

        var pk = PartitionKeyToString(partitionKey);
        var key = ItemKey(id, pk);

        if (!_items.TryGetValue(key, out var existingJson))
        {
            throw new CosmosException($"Entity with the specified id does not exist. id = {id}",
                HttpStatusCode.NotFound, 0, string.Empty, 0);
        }

        CheckIfMatch(requestOptions, key);

        var jObj = JsonParseHelpers.ParseJson(existingJson);

        if (requestOptions?.FilterPredicate is not null)
        {
            var predicateSql = $"SELECT * {requestOptions.FilterPredicate}";
            var predicateParsed = CosmosSqlParser.Parse(predicateSql);
            if (predicateParsed.Where is not null)
            {
                var matches = EvaluateWhereExpression(predicateParsed.Where, jObj, predicateParsed.FromAlias,
                    new Dictionary<string, object>(), null);
                if (!matches)
                {
                    throw new CosmosException("Precondition Failed",
                        HttpStatusCode.PreconditionFailed, 0, string.Empty, 0);
                }
            }
        }

        ApplyPatchOperations(jObj, patchOperations);
        var updatedJson = jObj.ToString(Formatting.None);
        ValidateDocumentSize(updatedJson);
        ValidateUniqueKeys(jObj, pk, excludeItemId: id);
        var etag = GenerateETag();
        _etags[key] = etag;
        _timestamps[key] = DateTimeOffset.UtcNow;
        var enrichedJson = EnrichWithSystemProperties(updatedJson, etag, _timestamps[key]);
        _items[key] = enrichedJson;
        RecordChangeFeed(id, pk, enrichedJson);
        var suppressContent = requestOptions?.EnableContentResponseOnWrite == false;
        var result = JsonConvert.DeserializeObject<T>(enrichedJson, JsonSettings);
        return Task.FromResult(CreateItemResponse(result, HttpStatusCode.OK, etag, suppressContent));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Item CRUD — Stream
    // ═══════════════════════════════════════════════════════════════════════════

    public override Task<ResponseMessage> CreateItemStreamAsync(
        Stream streamPayload, PartitionKey partitionKey,
        ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var json = ReadStream(streamPayload);
        ValidateDocumentSize(json);
        var jObj = JsonParseHelpers.ParseJson(json);

        jObj = ExecutePreTriggers(requestOptions, jObj, "Create");
        json = jObj.ToString(Newtonsoft.Json.Formatting.None);

        var itemId = jObj["id"]?.ToString() ?? Guid.NewGuid().ToString();
        var pk = PartitionKeyToString(partitionKey);
        var key = ItemKey(itemId, pk);

        if (!ValidateUniqueKeysStream(jObj, pk))
        {
            return Task.FromResult(CreateResponseMessage(HttpStatusCode.Conflict));
        }

        if (!_items.TryAdd(key, json))
        {
            return Task.FromResult(CreateResponseMessage(HttpStatusCode.Conflict));
        }

        var etag = GenerateETag();
        _etags[key] = etag;
        _timestamps[key] = DateTimeOffset.UtcNow;
        var enrichedJson = EnrichWithSystemProperties(json, etag, _timestamps[key]);
        _items[key] = enrichedJson;
        RecordChangeFeed(itemId, pk, enrichedJson);

        try
        {
            ExecutePostTriggers(requestOptions, JsonParseHelpers.ParseJson(enrichedJson), "Create");
        }
        catch
        {
            _items.TryRemove(key, out _);
            _etags.TryRemove(key, out _);
            _timestamps.TryRemove(key, out _);
            throw;
        }

        return Task.FromResult(CreateResponseMessage(HttpStatusCode.Created, enrichedJson, etag));
    }

    public override Task<ResponseMessage> ReadItemStreamAsync(
        string id, PartitionKey partitionKey,
        ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var pk = PartitionKeyToString(partitionKey);
        var key = ItemKey(id, pk);
        if (!_items.TryGetValue(key, out var json) || IsExpired(key))
        {
            EvictIfExpired(key);
            return Task.FromResult(CreateResponseMessage(HttpStatusCode.NotFound));
        }
        var etag = _etags.GetValueOrDefault(key);
        if (!CheckIfNoneMatchStream(requestOptions, key))
        {
            return Task.FromResult(CreateResponseMessage(HttpStatusCode.NotModified));
        }
        return Task.FromResult(CreateResponseMessage(HttpStatusCode.OK, json, etag));
    }

    public override Task<ResponseMessage> UpsertItemStreamAsync(
        Stream streamPayload, PartitionKey partitionKey,
        ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var json = ReadStream(streamPayload);
        ValidateDocumentSize(json);
        var jObj = JsonParseHelpers.ParseJson(json);

        jObj = ExecutePreTriggers(requestOptions, jObj, "Upsert");
        json = jObj.ToString(Newtonsoft.Json.Formatting.None);

        var itemId = jObj["id"]?.ToString() ?? throw new InvalidOperationException(
            "Item must have an 'id' property (case-sensitive, lowercase). " +
            "If your C# model uses PascalCase 'Id', ensure CosmosClientOptions.Serializer is configured " +
            "with camelCase naming (e.g. CosmosJsonDotNetSerializer with CamelCasePropertyNamesContractResolver).");
        var pk = PartitionKeyToString(partitionKey);
        var key = ItemKey(itemId, pk);
        if (!CheckIfMatchStream(requestOptions, key))
        {
            return Task.FromResult(CreateResponseMessage(HttpStatusCode.PreconditionFailed));
        }
        if (!ValidateUniqueKeysStream(jObj, pk, excludeItemId: itemId))
        {
            return Task.FromResult(CreateResponseMessage(HttpStatusCode.Conflict));
        }
        var existed = _items.ContainsKey(key);
        var etag = GenerateETag();
        _etags[key] = etag;
        _timestamps[key] = DateTimeOffset.UtcNow;
        var enrichedJson = EnrichWithSystemProperties(json, etag, _timestamps[key]);
        _items[key] = enrichedJson;
        RecordChangeFeed(itemId, pk, enrichedJson);

        try
        {
            ExecutePostTriggers(requestOptions, JsonParseHelpers.ParseJson(enrichedJson), "Upsert");
        }
        catch
        {
            if (existed)
            {
                // Stream rollback is best-effort — we don't have the previous json
            }
            else
            {
                _items.TryRemove(key, out _);
                _etags.TryRemove(key, out _);
                _timestamps.TryRemove(key, out _);
            }
            throw;
        }

        return Task.FromResult(CreateResponseMessage(existed ? HttpStatusCode.OK : HttpStatusCode.Created, enrichedJson, etag));
    }

    public override Task<ResponseMessage> ReplaceItemStreamAsync(
        Stream streamPayload, string id, PartitionKey partitionKey,
        ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var json = ReadStream(streamPayload);
        ValidateDocumentSize(json);
        var pk = PartitionKeyToString(partitionKey);
        var key = ItemKey(id, pk);
        if (!_items.ContainsKey(key))
        {
            return Task.FromResult(CreateResponseMessage(HttpStatusCode.NotFound));
        }

        if (!CheckIfMatchStream(requestOptions, key))
        {
            return Task.FromResult(CreateResponseMessage(HttpStatusCode.PreconditionFailed));
        }

        var jObj = JsonParseHelpers.ParseJson(json);

        jObj = ExecutePreTriggers(requestOptions, jObj, "Replace");
        json = jObj.ToString(Newtonsoft.Json.Formatting.None);

        if (!ValidateUniqueKeysStream(jObj, pk, excludeItemId: id))
        {
            return Task.FromResult(CreateResponseMessage(HttpStatusCode.Conflict));
        }

        var etag = GenerateETag();
        _etags[key] = etag;
        _timestamps[key] = DateTimeOffset.UtcNow;
        var enrichedJson = EnrichWithSystemProperties(json, etag, _timestamps[key]);
        _items[key] = enrichedJson;
        RecordChangeFeed(id, pk, enrichedJson);

        try
        {
            ExecutePostTriggers(requestOptions, JsonParseHelpers.ParseJson(enrichedJson), "Replace");
        }
        catch
        {
            // Stream rollback is best-effort
            throw;
        }

        return Task.FromResult(CreateResponseMessage(HttpStatusCode.OK, enrichedJson, etag));
    }

    public override Task<ResponseMessage> DeleteItemStreamAsync(
        string id, PartitionKey partitionKey,
        ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var pk = PartitionKeyToString(partitionKey);
        var key = ItemKey(id, pk);
        if (!_items.ContainsKey(key))
        {
            return Task.FromResult(CreateResponseMessage(HttpStatusCode.NotFound));
        }

        if (!CheckIfMatchStream(requestOptions, key))
        {
            return Task.FromResult(CreateResponseMessage(HttpStatusCode.PreconditionFailed));
        }

        _items.TryRemove(key, out _);

        _etags.TryRemove(key, out _);
        _timestamps.TryRemove(key, out _);
        RecordDeleteTombstone(id, pk);
        return Task.FromResult(CreateResponseMessage(HttpStatusCode.NoContent));
    }

    public override Task<ResponseMessage> PatchItemStreamAsync(
        string id, PartitionKey partitionKey,
        IReadOnlyList<PatchOperation> patchOperations,
        PatchItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var pk = PartitionKeyToString(partitionKey);
        var key = ItemKey(id, pk);
        if (!_items.TryGetValue(key, out var existingJson))
        {
            return Task.FromResult(CreateResponseMessage(HttpStatusCode.NotFound));
        }

        if (!CheckIfMatchStream(requestOptions, key))
        {
            return Task.FromResult(CreateResponseMessage(HttpStatusCode.PreconditionFailed));
        }

        var jObj = JsonParseHelpers.ParseJson(existingJson);

        if (requestOptions?.FilterPredicate is not null)
        {
            var predicateSql = $"SELECT * {requestOptions.FilterPredicate}";
            var predicateParsed = CosmosSqlParser.Parse(predicateSql);
            if (predicateParsed.Where is not null)
            {
                var matches = EvaluateWhereExpression(predicateParsed.Where, jObj, predicateParsed.FromAlias,
                    new Dictionary<string, object>(), null);
                if (!matches)
                {
                    return Task.FromResult(CreateResponseMessage(HttpStatusCode.PreconditionFailed));
                }
            }
        }

        ApplyPatchOperations(jObj, patchOperations);
        var updatedJson = jObj.ToString(Formatting.None);
        ValidateDocumentSize(updatedJson);
        var etag = GenerateETag();
        _etags[key] = etag;
        _timestamps[key] = DateTimeOffset.UtcNow;
        var enrichedJson = EnrichWithSystemProperties(updatedJson, etag, _timestamps[key]);
        _items[key] = enrichedJson;
        RecordChangeFeed(id, pk, enrichedJson);
        return Task.FromResult(CreateResponseMessage(HttpStatusCode.OK, enrichedJson, etag));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  ReadMany
    // ═══════════════════════════════════════════════════════════════════════════

    public override Task<FeedResponse<T>> ReadManyItemsAsync<T>(
        IReadOnlyList<(string id, PartitionKey partitionKey)> items,
        ReadManyRequestOptions readManyRequestOptions = null, CancellationToken cancellationToken = default)
    {
        var results = new List<T>();
        foreach (var (itemId, pk) in items)
        {
            var pkStr = PartitionKeyToString(pk);
            var key = ItemKey(itemId, pkStr);
            if (_items.TryGetValue(key, out var json))
            {
                var deserialized = JsonConvert.DeserializeObject<T>(json, JsonSettings);
                if (deserialized is not null)
                {
                    results.Add(deserialized);
                }
            }
        }
        return Task.FromResult<FeedResponse<T>>(new InMemoryFeedResponse<T>(results));
    }

    public override Task<ResponseMessage> ReadManyItemsStreamAsync(
        IReadOnlyList<(string id, PartitionKey partitionKey)> items,
        ReadManyRequestOptions readManyRequestOptions = null, CancellationToken cancellationToken = default)
    {
        var results = new JArray();
        foreach (var (itemId, pk) in items)
        {
            var pkStr = PartitionKeyToString(pk);
            var key = ItemKey(itemId, pkStr);
            if (_items.TryGetValue(key, out var json))
            {
                results.Add(JsonParseHelpers.ParseJson(json));
            }
        }
        var envelope = new JObject { ["Documents"] = results };
        return Task.FromResult(CreateResponseMessage(HttpStatusCode.OK, envelope.ToString(Formatting.None)));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Query — Typed FeedIterator
    // ═══════════════════════════════════════════════════════════════════════════

    public override FeedIterator<T> GetItemQueryIterator<T>(
        QueryDefinition queryDefinition, string continuationToken = null,
        QueryRequestOptions requestOptions = null)
    {
        var parameters = ExtractQueryParameters(queryDefinition);
        var filtered = FilterItemsByQuery(queryDefinition.QueryText, parameters, requestOptions);
        var items = filtered.Select(json => JsonConvert.DeserializeObject<T>(json, JsonSettings)).ToList();
        var initialOffset = ParseContinuationToken(continuationToken);
        return new InMemoryFeedIterator<T>(items, requestOptions?.MaxItemCount, initialOffset);
    }

    public override FeedIterator<T> GetItemQueryIterator<T>(
        string queryText = null, string continuationToken = null,
        QueryRequestOptions requestOptions = null)
    {
        List<T> items;
        if (string.IsNullOrEmpty(queryText))
        {
            items = GetAllItemsForPartition(requestOptions)
                .Select(json => JsonConvert.DeserializeObject<T>(json, JsonSettings)).ToList();
        }
        else
        {
            var filtered = FilterItemsByQuery(queryText, new Dictionary<string, object>(), requestOptions);
            items = filtered.Select(json => JsonConvert.DeserializeObject<T>(json, JsonSettings)).ToList();
        }
        var initialOffset = ParseContinuationToken(continuationToken);
        return new InMemoryFeedIterator<T>(items, requestOptions?.MaxItemCount, initialOffset);
    }

    public override FeedIterator<T> GetItemQueryIterator<T>(
        FeedRange feedRange, QueryDefinition queryDefinition, string continuationToken = null,
        QueryRequestOptions requestOptions = null)
    {
        var parameters = ExtractQueryParameters(queryDefinition);
        var filtered = FilterItemsByQuery(queryDefinition.QueryText, parameters, requestOptions);
        filtered = FilterByFeedRange(filtered, feedRange);
        var items = filtered.Select(json => JsonConvert.DeserializeObject<T>(json, JsonSettings)).ToList();
        var initialOffset = ParseContinuationToken(continuationToken);
        return new InMemoryFeedIterator<T>(items, requestOptions?.MaxItemCount, initialOffset);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Query — Stream FeedIterator
    // ═══════════════════════════════════════════════════════════════════════════

    public override FeedIterator GetItemQueryStreamIterator(
        QueryDefinition queryDefinition, string continuationToken = null,
        QueryRequestOptions requestOptions = null)
    {
        var parameters = ExtractQueryParameters(queryDefinition);
        var filtered = FilterItemsByQuery(queryDefinition.QueryText, parameters, requestOptions);
        return CreateStreamFeedIterator(filtered, ParseContinuationToken(continuationToken), requestOptions?.MaxItemCount);
    }

    public override FeedIterator GetItemQueryStreamIterator(
        string queryText = null, string continuationToken = null,
        QueryRequestOptions requestOptions = null)
    {
        var offset = ParseContinuationToken(continuationToken);
        if (string.IsNullOrEmpty(queryText))
        {
            return CreateStreamFeedIterator(GetAllItemsForPartition(requestOptions).ToList(), offset, requestOptions?.MaxItemCount);
        }

        return CreateStreamFeedIterator(FilterItemsByQuery(queryText, new Dictionary<string, object>(), requestOptions), offset, requestOptions?.MaxItemCount);
    }

    public override FeedIterator GetItemQueryStreamIterator(
        FeedRange feedRange, QueryDefinition queryDefinition, string continuationToken = null,
        QueryRequestOptions requestOptions = null)
    {
        var parameters = ExtractQueryParameters(queryDefinition);
        var filtered = FilterItemsByQuery(queryDefinition.QueryText, parameters, requestOptions);
        filtered = FilterByFeedRange(filtered, feedRange);
        return CreateStreamFeedIterator(filtered, ParseContinuationToken(continuationToken), requestOptions?.MaxItemCount);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  LINQ
    // ═══════════════════════════════════════════════════════════════════════════

    public override IOrderedQueryable<T> GetItemLinqQueryable<T>(
        bool allowSynchronousQueryExecution = false, string continuationToken = null,
        QueryRequestOptions requestOptions = null, CosmosLinqSerializerOptions linqSerializerOptions = null)
    {
        return GetAllItemsForPartition(requestOptions)
            .Select(json => JsonConvert.DeserializeObject<T>(json, JsonSettings))
            .AsQueryable()
            .OrderBy(item => 0);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  TransactionalBatch
    // ═══════════════════════════════════════════════════════════════════════════

    public override TransactionalBatch CreateTransactionalBatch(PartitionKey partitionKey)
        => new InMemoryTransactionalBatch(this, partitionKey);

    // ═══════════════════════════════════════════════════════════════════════════
    //  Change Feed — Iterators
    // ═══════════════════════════════════════════════════════════════════════════

    public override FeedIterator<T> GetChangeFeedIterator<T>(
        ChangeFeedStartFrom changeFeedStartFrom, ChangeFeedMode changeFeedMode,
        ChangeFeedRequestOptions changeFeedRequestOptions = null)
    {
        var pageSize = changeFeedRequestOptions?.PageSizeHint;
        var typeName = changeFeedStartFrom.GetType().Name;
        var feedRange = ExtractFeedRangeFromStartFrom(changeFeedStartFrom);

        // "Now" and "Time" start types use lazy evaluation so items added
        // after iterator creation are included when ReadNextAsync is called.
        if (typeName.Contains("Now", StringComparison.OrdinalIgnoreCase) ||
            typeName.Contains("Time", StringComparison.OrdinalIgnoreCase))
        {
            // Capture the checkpoint (current feed length) at creation time for "Now",
            // or extract the timestamp for "Time" filtering.
            DateTimeOffset? capturedTimestamp = null;
            if (typeName.Contains("Now", StringComparison.OrdinalIgnoreCase))
            {
                lock (_changeFeedLock)
                {
                    capturedTimestamp = _changeFeed.Count > 0
                        ? _changeFeed[^1].Timestamp
                        : DateTimeOffset.UtcNow;
                }
            }
            else
            {
                var startTime = ExtractStartTime(changeFeedStartFrom);
                if (startTime.HasValue)
                    capturedTimestamp = new DateTimeOffset(startTime.Value, TimeSpan.Zero);
            }

            var isNowStart = typeName.Contains("Now", StringComparison.OrdinalIgnoreCase);
            var ts = capturedTimestamp;
            var capturedRange = feedRange;
            return new InMemoryFeedIterator<T>(() =>
            {
                lock (_changeFeedLock)
                {
                    var entries = ts.HasValue
                        ? _changeFeed.Where(entry => isNowStart
                            ? entry.Timestamp > ts.Value
                            : entry.Timestamp >= ts.Value).ToList()
                        : _changeFeed.ToList();

                    entries = FilterChangeFeedEntriesByFeedRange(entries, capturedRange);

                    if (changeFeedMode == ChangeFeedMode.Incremental)
                    {
                        entries = entries
                            .GroupBy(entry => (entry.Id, entry.PartitionKey))
                            .Select(group => group.Last())
                            .Where(entry => !entry.IsDelete)
                            .ToList();
                    }

                    return entries
                        .Select(entry => JsonConvert.DeserializeObject<T>(entry.Json, JsonSettings))
                        .ToList();
                }
            }, pageSize);
        }

        // "Beginning" and other start types — eager evaluation is fine
        List<(DateTimeOffset Timestamp, string Id, string PartitionKey, string Json, bool IsDelete)> eagerEntries;
        lock (_changeFeedLock)
        {
            eagerEntries = _changeFeed.ToList();
        }

        eagerEntries = FilterChangeFeedEntriesByFeedRange(eagerEntries, feedRange);

        if (changeFeedMode == ChangeFeedMode.Incremental)
        {
            eagerEntries = eagerEntries
                .GroupBy(entry => (entry.Id, entry.PartitionKey))
                .Select(group => group.Last())
                .Where(entry => !entry.IsDelete)
                .ToList();
        }

        var items = eagerEntries
            .Select(entry => JsonConvert.DeserializeObject<T>(entry.Json, JsonSettings))
            .ToList();
        return new InMemoryFeedIterator<T>(items, pageSize);
    }

    /// <summary>
    /// Returns a change feed iterator that reads changes from the given checkpoint position.
    /// Obtain a checkpoint via <see cref="GetChangeFeedCheckpoint"/>.
    /// </summary>
    public FeedIterator<T> GetChangeFeedIterator<T>(long checkpoint)
    {
        List<T> items;
        lock (_changeFeedLock)
        {
            items = _changeFeed
                .Skip((int)checkpoint)
                .Select(entry => JsonConvert.DeserializeObject<T>(entry.Json, JsonSettings))
                .ToList();
        }
        return new InMemoryFeedIterator<T>(items);
    }

    public override FeedIterator GetChangeFeedStreamIterator(
        ChangeFeedStartFrom changeFeedStartFrom, ChangeFeedMode changeFeedMode,
        ChangeFeedRequestOptions changeFeedRequestOptions = null)
    {
        var feedRange = ExtractFeedRangeFromStartFrom(changeFeedStartFrom);
        List<string> items;
        lock (_changeFeedLock)
        {
            var entries = FilterChangeFeedByStartFrom(changeFeedStartFrom);
            entries = FilterChangeFeedEntriesByFeedRange(entries, feedRange);

            if (changeFeedMode == ChangeFeedMode.Incremental)
            {
                entries = entries
                    .GroupBy(entry => (entry.Id, entry.PartitionKey))
                    .Select(group => group.Last())
                    .Where(entry => !entry.IsDelete)
                    .ToList();
            }

            items = entries.Select(entry => entry.Json).ToList();
        }
        return CreateStreamFeedIterator(items);
    }

    private List<(DateTimeOffset Timestamp, string Id, string PartitionKey, string Json, bool IsDelete)> FilterChangeFeedByStartFrom(
        ChangeFeedStartFrom startFrom)
    {
        var typeName = startFrom.GetType().Name;

        if (typeName.Contains("Now", StringComparison.OrdinalIgnoreCase))
        {
            var now = DateTimeOffset.UtcNow;
            return _changeFeed.Where(entry => entry.Timestamp > now).ToList();
        }

        if (typeName.Contains("Time", StringComparison.OrdinalIgnoreCase))
        {
            var startTime = ExtractStartTime(startFrom);
            if (startTime.HasValue)
            {
                var dto = new DateTimeOffset(startTime.Value, TimeSpan.Zero);
                return _changeFeed.Where(entry => entry.Timestamp >= dto).ToList();
            }
        }

        return _changeFeed.ToList();
    }

    private static DateTime? ExtractStartTime(ChangeFeedStartFrom startFrom)
    {
        foreach (var prop in startFrom.GetType().GetProperties(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance))
        {
            if (prop.PropertyType == typeof(DateTime) && prop.GetValue(startFrom) is DateTime dt)
            {
                return dt;
            }
        }

        foreach (var field in startFrom.GetType().GetFields(BindingFlags.NonPublic | BindingFlags.Instance))
        {
            if (field.FieldType == typeof(DateTime) && field.GetValue(startFrom) is DateTime dt)
            {
                return dt;
            }
        }

        return null;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  FeedRange Filtering Helpers
    // ═══════════════════════════════════════════════════════════════════════════

    private static FeedRange ExtractFeedRangeFromStartFrom(ChangeFeedStartFrom startFrom)
    {
        // ChangeFeedStartFrom subtypes (Beginning, Now, Time, ContinuationAndFeedRange)
        // store the FeedRange in an internal property. Use reflection to extract it.
        foreach (var prop in startFrom.GetType().GetProperties(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance))
        {
            if (typeof(FeedRange).IsAssignableFrom(prop.PropertyType) && prop.GetValue(startFrom) is FeedRange fr)
                return fr;
        }

        foreach (var field in startFrom.GetType().GetFields(BindingFlags.NonPublic | BindingFlags.Instance))
        {
            if (typeof(FeedRange).IsAssignableFrom(field.FieldType) && field.GetValue(startFrom) is FeedRange fr)
                return fr;
        }

        return null;
    }

    private List<string> FilterByFeedRange(List<string> items, FeedRange feedRange)
    {
        var (min, max) = ParseFeedRangeBoundaries(feedRange);
        if (min == null) return items;

        return items.Where(json =>
        {
            var pkValue = ExtractPartitionKeyValueFromJson(json);
            var hash = PartitionKeyHash.MurmurHash3(pkValue);
            return IsHashInRange(hash, min.Value, max.Value);
        }).ToList();
    }

    private List<(DateTimeOffset Timestamp, string Id, string PartitionKey, string Json, bool IsDelete)>
        FilterChangeFeedEntriesByFeedRange(
            List<(DateTimeOffset Timestamp, string Id, string PartitionKey, string Json, bool IsDelete)> entries,
            FeedRange feedRange)
    {
        var (min, max) = ParseFeedRangeBoundaries(feedRange);
        if (min == null) return entries;

        return entries.Where(entry =>
        {
            var hash = PartitionKeyHash.MurmurHash3(entry.PartitionKey);
            return IsHashInRange(hash, min.Value, max.Value);
        }).ToList();
    }

    private static (uint? Min, uint? Max) ParseFeedRangeBoundaries(FeedRange feedRange)
    {
        if (feedRange == null) return (null, null);

        try
        {
            var json = feedRange.ToJsonString();
            var obj = JObject.Parse(json);
            var rangeObj = obj["Range"];
            if (rangeObj == null) return (null, null);

            var minStr = rangeObj["min"]?.ToString() ?? "";
            var maxStr = rangeObj["max"]?.ToString() ?? "";

            var minVal = string.IsNullOrEmpty(minStr) ? 0u : Convert.ToUInt32(minStr, 16);
            var maxVal = string.Equals(maxStr, "FF", StringComparison.OrdinalIgnoreCase)
                ? uint.MaxValue
                : Convert.ToUInt32(maxStr, 16);

            return (minVal, maxVal);
        }
        catch
        {
            return (null, null);
        }
    }

    private static bool IsHashInRange(uint hash, uint min, uint max)
    {
        return hash >= min && (max == uint.MaxValue || hash < max);
    }

    private string ExtractPartitionKeyValueFromJson(string json)
    {
        var jObj = JsonConvert.DeserializeObject<JObject>(json, JsonSettings);
        if (jObj == null) return "";

        if (PartitionKeyPaths is { Count: > 0 })
        {
            var parts = PartitionKeyPaths.Select(path => jObj.SelectToken(path.TrimStart('/'))?.ToString()).ToList();
            if (parts.Count == 1) return parts[0] ?? "";
            return string.Join("|", parts.Select(p => p ?? ""));
        }

        return "";
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Change Feed — Processor Builders
    // ═══════════════════════════════════════════════════════════════════════════

    public override ChangeFeedProcessorBuilder GetChangeFeedProcessorBuilder<T>(
        string processorName, Container.ChangesHandler<T> onChangesDelegate)
        => ChangeFeedProcessorBuilderFactory.Create(processorName,
            new InMemoryChangeFeedProcessor<T>(this, onChangesDelegate));

    public override ChangeFeedProcessorBuilder GetChangeFeedProcessorBuilder<T>(
        string processorName, Container.ChangeFeedHandler<T> onChangesDelegate)
        => ChangeFeedProcessorBuilderFactory.Create(processorName,
            new InMemoryChangeFeedProcessor<T>(this, onChangesDelegate));

    public override ChangeFeedProcessorBuilder GetChangeFeedProcessorBuilder(
        string processorName, Container.ChangeFeedStreamHandler onChangesDelegate)
        => ChangeFeedProcessorBuilderFactory.Create(processorName,
            new InMemoryChangeFeedStreamProcessor(this, onChangesDelegate));

    public override ChangeFeedProcessorBuilder GetChangeFeedProcessorBuilderWithManualCheckpoint<T>(
        string processorName, Container.ChangeFeedHandlerWithManualCheckpoint<T> onChangesDelegate)
        => ChangeFeedProcessorBuilderFactory.Create(processorName, new NoOpChangeFeedProcessor());

    public override ChangeFeedProcessorBuilder GetChangeFeedProcessorBuilderWithManualCheckpoint(
        string processorName, Container.ChangeFeedStreamHandlerWithManualCheckpoint onChangesDelegate)
        => ChangeFeedProcessorBuilderFactory.Create(processorName, new NoOpChangeFeedProcessor());

    public override ChangeFeedEstimator GetChangeFeedEstimator(
        string processorName, Container leaseContainer)
        => Substitute.For<ChangeFeedEstimator>();

    public override ChangeFeedProcessorBuilder GetChangeFeedEstimatorBuilder(
        string processorName, Container.ChangesEstimationHandler estimationDelegate,
        TimeSpan? estimationPeriod = null)
        => ChangeFeedProcessorBuilderFactory.Create(processorName, new NoOpChangeFeedProcessor());

    // ═══════════════════════════════════════════════════════════════════════════
    //  Feed Ranges
    // ═══════════════════════════════════════════════════════════════════════════

    public override Task<IReadOnlyList<FeedRange>> GetFeedRangesAsync(CancellationToken cancellationToken = default)
    {
        var count = Math.Max(1, FeedRangeCount);
        var ranges = new List<FeedRange>(count);
        var step = 0x1_0000_0000L / count;

        for (var i = 0; i < count; i++)
        {
            var min = PartitionKeyHash.RangeBoundaryToHex(i * step);
            var max = (i == count - 1) ? "FF" : PartitionKeyHash.RangeBoundaryToHex((i + 1) * step);
            ranges.Add(FeedRange.FromJsonString($"{{\"Range\":{{\"min\":\"{min}\",\"max\":\"{max}\"}}}}"));
        }

        return Task.FromResult<IReadOnlyList<FeedRange>>(ranges);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Container management
    // ═══════════════════════════════════════════════════════════════════════════

    public override Task<ContainerResponse> ReadContainerAsync(
        ContainerRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var r = Substitute.For<ContainerResponse>();
        r.StatusCode.Returns(HttpStatusCode.OK);
        r.Resource.Returns(_containerProperties);
        r.Container.Returns(this);
        return Task.FromResult(r);
    }

    public override Task<ResponseMessage> ReadContainerStreamAsync(
        ContainerRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
        => Task.FromResult(CreateResponseMessage(HttpStatusCode.OK, JsonConvert.SerializeObject(_containerProperties, JsonSettings)));

    public override Task<ContainerResponse> ReplaceContainerAsync(
        ContainerProperties containerProperties, ContainerRequestOptions requestOptions = null,
        CancellationToken cancellationToken = default)
    {
        _containerProperties = containerProperties;
        var r = Substitute.For<ContainerResponse>();
        r.StatusCode.Returns(HttpStatusCode.OK);
        r.Resource.Returns(containerProperties);
        r.Container.Returns(this);
        return Task.FromResult(r);
    }

    public override Task<ResponseMessage> ReplaceContainerStreamAsync(
        ContainerProperties containerProperties, ContainerRequestOptions requestOptions = null,
        CancellationToken cancellationToken = default)
    {
        _containerProperties = containerProperties;
        return Task.FromResult(CreateResponseMessage(HttpStatusCode.OK, JsonConvert.SerializeObject(containerProperties, JsonSettings)));
    }

    public override Task<ContainerResponse> DeleteContainerAsync(
        ContainerRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        _items.Clear();
        _etags.Clear();
        _timestamps.Clear();
        OnDeleted?.Invoke();
        var r = Substitute.For<ContainerResponse>();
        r.StatusCode.Returns(HttpStatusCode.NoContent);
        r.Container.Returns(this);
        return Task.FromResult(r);
    }

    public override Task<ResponseMessage> DeleteContainerStreamAsync(
        ContainerRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        _items.Clear();
        _etags.Clear();
        _timestamps.Clear();
        OnDeleted?.Invoke();
        return Task.FromResult(CreateResponseMessage(HttpStatusCode.NoContent));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Throughput
    // ═══════════════════════════════════════════════════════════════════════════

    public override Task<int?> ReadThroughputAsync(CancellationToken cancellationToken = default)
        => Task.FromResult<int?>(400);

    public override Task<ThroughputResponse> ReadThroughputAsync(
        RequestOptions requestOptions, CancellationToken cancellationToken = default)
    {
        var r = Substitute.For<ThroughputResponse>();
        r.StatusCode.Returns(HttpStatusCode.OK);
        r.Resource.Returns(ThroughputProperties.CreateManualThroughput(400));
        return Task.FromResult(r);
    }

    public override Task<ThroughputResponse> ReplaceThroughputAsync(
        int throughput, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var r = Substitute.For<ThroughputResponse>();
        r.StatusCode.Returns(HttpStatusCode.OK);
        r.Resource.Returns(ThroughputProperties.CreateManualThroughput(throughput));
        return Task.FromResult(r);
    }

    public override Task<ThroughputResponse> ReplaceThroughputAsync(
        ThroughputProperties throughputProperties, RequestOptions requestOptions = null,
        CancellationToken cancellationToken = default)
    {
        var r = Substitute.For<ThroughputResponse>();
        r.StatusCode.Returns(HttpStatusCode.OK);
        r.Resource.Returns(throughputProperties);
        return Task.FromResult(r);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Delete all items by partition key
    // ═══════════════════════════════════════════════════════════════════════════

    public override Task<ResponseMessage> DeleteAllItemsByPartitionKeyStreamAsync(
        PartitionKey partitionKey, RequestOptions requestOptions = null,
        CancellationToken cancellationToken = default)
    {
        var pk = PartitionKeyToString(partitionKey);
        foreach (var key in _items.Keys.Where(k => k.PartitionKey == pk).ToList())
        {
            _items.TryRemove(key, out _);
            _etags.TryRemove(key, out _);
            _timestamps.TryRemove(key, out _);
        }
        return Task.FromResult(CreateResponseMessage(HttpStatusCode.OK));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Private helpers — Key management & ETag
    // ═══════════════════════════════════════════════════════════════════════════

    private static (string Id, string PartitionKey) ItemKey(string id, string partitionKey) => (id, partitionKey);

    private string ExtractPartitionKeyValue(PartitionKey? partitionKey, JObject jObj)
    {
        if (partitionKey.HasValue)
        {
            if (partitionKey.Value == PartitionKey.Null)
            {
                return null;
            }

            if (partitionKey.Value != PartitionKey.None)
            {
                return PartitionKeyToString(partitionKey.Value);
            }
        }

        if (PartitionKeyPaths is { Count: > 0 })
        {
            var parts = PartitionKeyPaths.Select(path => jObj.SelectToken(path.TrimStart('/'))?.ToString()).ToList();
            var nonNull = parts.Where(p => p is not null).ToList();
            if (nonNull.Count > 0)
            {
                return nonNull.Count == 1 ? nonNull[0] : string.Join("|", nonNull);
            }
        }

        return jObj["id"]?.ToString();
    }

    private static string PartitionKeyToString(PartitionKey partitionKey)
    {
        if (partitionKey == PartitionKey.None || partitionKey == PartitionKey.Null)
        {
            return null;
        }

        var raw = partitionKey.ToString();
        if (raw.StartsWith("["))
        {
            try
            {
                var arr = JArray.Parse(raw);
                if (arr.Count == 1)
                {
                    return arr[0].Type == JTokenType.String ? arr[0].Value<string>() : arr[0].ToString();
                }

                return string.Join("|", arr.Select(t => t.Type == JTokenType.String ? t.Value<string>() : t.ToString()));
            }
            catch { /* fall through */ }
        }
        return raw;
    }

    private static string GenerateETag() => $"\"{Guid.NewGuid()}\"";

    private static string EnrichWithSystemProperties(string json, string etag, DateTimeOffset timestamp)
    {
        var jObj = JsonParseHelpers.ParseJson(json);
        jObj["_etag"] = etag;
        jObj["_ts"] = timestamp.ToUnixTimeSeconds();
        return jObj.ToString(Formatting.None);
    }

    private static int ParseContinuationToken(string continuationToken)
    {
        if (continuationToken is not null && int.TryParse(continuationToken, out var offset))
        {
            return offset;
        }

        return 0;
    }

    private void RecordChangeFeed(string id, string partitionKey, string json, bool isDelete = false)
    {
        lock (_changeFeedLock)
        {
            _changeFeed.Add((DateTimeOffset.UtcNow, id, partitionKey, json, isDelete));
        }
    }

    private void RecordDeleteTombstone(string id, string pk)
    {
        var tombstone = new JObject { ["id"] = id, ["_deleted"] = true, ["_ts"] = DateTimeOffset.UtcNow.ToUnixTimeSeconds() };
        var pkValues = pk.Split('|');
        for (var i = 0; i < PartitionKeyPaths.Count; i++)
        {
            tombstone[PartitionKeyPaths[i].TrimStart('/')] = i < pkValues.Length ? pkValues[i] : null;
        }
        RecordChangeFeed(id, pk, tombstone.ToString(Newtonsoft.Json.Formatting.None), isDelete: true);
    }

    private static TriggerOperation OperationNameToTriggerOp(string operationName) => operationName switch
    {
        "Create" => TriggerOperation.Create,
        "Replace" => TriggerOperation.Replace,
        "Upsert" => TriggerOperation.Upsert,
        "Delete" => TriggerOperation.Delete,
        _ => TriggerOperation.All
    };

    private static bool TriggerOperationMatches(TriggerOperation registered, TriggerOperation current)
        => registered == TriggerOperation.All || registered == current;

    private JObject ExecutePreTriggers(ItemRequestOptions requestOptions, JObject jObj, string operationName)
    {
        var triggerNames = requestOptions?.PreTriggers;
        if (triggerNames is null) return jObj;

        var currentOp = OperationNameToTriggerOp(operationName);

        foreach (var name in triggerNames)
        {
            // Priority 1: C# handler
            if (_triggers.TryGetValue(name, out var trigger))
            {
                if (trigger.TriggerType != TriggerType.Pre || trigger.PreHandler is null) continue;
                if (!TriggerOperationMatches(trigger.TriggerOperation, currentOp)) continue;

                jObj = trigger.PreHandler(jObj);
                continue;
            }

            // Priority 2: JS body via JsTriggerEngine
            if (_triggerProperties.TryGetValue(name, out var props) && props.Body is not null)
            {
                if (props.TriggerType != TriggerType.Pre) continue;
                if (!TriggerOperationMatches(props.TriggerOperation, currentOp)) continue;

                if (JsTriggerEngine is null)
                {
                    throw new CosmosException(
                        $"Trigger '{name}' has a JavaScript body but no JS trigger engine is configured. " +
                        "Install the CosmosDB.InMemoryEmulator.JsTriggers package and call container.UseJsTriggers().",
                        HttpStatusCode.BadRequest, 0, string.Empty, 0);
                }

                jObj = JsTriggerEngine.ExecutePreTrigger(props.Body, jObj);
                continue;
            }

            throw new CosmosException(
                $"Trigger '{name}' is not registered. Register it via RegisterTrigger() or CreateTriggerAsync() before referencing it in PreTriggers.",
                HttpStatusCode.BadRequest, 0, string.Empty, 0);
        }

        return jObj;
    }

    private void ExecutePostTriggers(ItemRequestOptions requestOptions, JObject committedDoc, string operationName)
    {
        var triggerNames = requestOptions?.PostTriggers;
        if (triggerNames is null) return;

        var currentOp = OperationNameToTriggerOp(operationName);

        foreach (var name in triggerNames)
        {
            // Priority 1: C# handler
            if (_triggers.TryGetValue(name, out var trigger))
            {
                if (trigger.TriggerType != TriggerType.Post || trigger.PostHandler is null) continue;
                if (!TriggerOperationMatches(trigger.TriggerOperation, currentOp)) continue;

                try
                {
                    trigger.PostHandler(committedDoc);
                }
                catch (CosmosException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    throw new CosmosException(
                        $"Post-trigger '{name}' failed: {ex.Message}",
                        HttpStatusCode.InternalServerError, 0, string.Empty, 0);
                }
                continue;
            }

            // Priority 2: JS body via JsTriggerEngine
            if (_triggerProperties.TryGetValue(name, out var props) && props.Body is not null)
            {
                if (props.TriggerType != TriggerType.Post) continue;
                if (!TriggerOperationMatches(props.TriggerOperation, currentOp)) continue;

                if (JsTriggerEngine is null)
                {
                    throw new CosmosException(
                        $"Trigger '{name}' has a JavaScript body but no JS trigger engine is configured. " +
                        "Install the CosmosDB.InMemoryEmulator.JsTriggers package and call container.UseJsTriggers().",
                        HttpStatusCode.BadRequest, 0, string.Empty, 0);
                }

                try
                {
                    JsTriggerEngine.ExecutePostTrigger(props.Body, committedDoc);
                }
                catch (CosmosException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    throw new CosmosException(
                        $"Post-trigger '{name}' failed: {ex.Message}",
                        HttpStatusCode.InternalServerError, 0, string.Empty, 0);
                }
                continue;
            }

            throw new CosmosException(
                $"Trigger '{name}' is not registered. Register it via RegisterTrigger() or CreateTriggerAsync() before referencing it in PostTriggers.",
                HttpStatusCode.BadRequest, 0, string.Empty, 0);
        }
    }

    private void CheckIfMatch(ItemRequestOptions requestOptions, (string Id, string PartitionKey) key)
    {
        if (requestOptions?.IfMatchEtag is null)
        {
            return;
        }

        if (requestOptions.IfMatchEtag == "*")
        {
            return;
        }

        if (!_etags.TryGetValue(key, out var currentEtag))
        {
            return;
        }

        if (requestOptions.IfMatchEtag != currentEtag)
        {
            throw new CosmosException("Precondition Failed", HttpStatusCode.PreconditionFailed, 0, string.Empty, 0);
        }
    }

    private void CheckIfNoneMatch(ItemRequestOptions requestOptions, (string Id, string PartitionKey) key)
    {
        if (requestOptions?.IfNoneMatchEtag is null)
        {
            return;
        }

        if (requestOptions.IfNoneMatchEtag == "*" && _etags.ContainsKey(key))
        {
            throw new CosmosException("Not Modified", HttpStatusCode.NotModified, 0, string.Empty, 0);
        }

        if (_etags.TryGetValue(key, out var currentEtag) && requestOptions.IfNoneMatchEtag == currentEtag)
        {
            throw new CosmosException("Not Modified", HttpStatusCode.NotModified, 0, string.Empty, 0);
        }
    }

    private bool CheckIfMatchStream(ItemRequestOptions requestOptions, (string Id, string PartitionKey) key)
    {
        if (requestOptions?.IfMatchEtag is null)
        {
            return true;
        }

        if (requestOptions.IfMatchEtag == "*")
        {
            return true;
        }

        if (_etags.TryGetValue(key, out var currentEtag) && requestOptions.IfMatchEtag != currentEtag)
        {
            return false;
        }

        return true;
    }

    private bool CheckIfNoneMatchStream(ItemRequestOptions requestOptions, (string Id, string PartitionKey) key)
    {
        if (requestOptions?.IfNoneMatchEtag is null)
        {
            return true;
        }

        if (requestOptions.IfNoneMatchEtag == "*" && _etags.ContainsKey(key))
        {
            return false;
        }

        if (_etags.TryGetValue(key, out var currentEtag) && requestOptions.IfNoneMatchEtag == currentEtag)
        {
            return false;
        }

        return true;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Private helpers — Serialization & Response factories
    // ═══════════════════════════════════════════════════════════════════════════

    private static string ReadStream(Stream stream)
    {
        using var reader = new StreamReader(stream, Encoding.UTF8, true, 1024, true);
        return reader.ReadToEnd();
    }

    private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

    private static readonly CosmosDiagnostics FakeDiagnostics = CreateFakeDiagnostics();

    private static CosmosDiagnostics CreateFakeDiagnostics()
    {
        var d = Substitute.For<CosmosDiagnostics>();
        d.GetClientElapsedTime().Returns(TimeSpan.Zero);
        return d;
    }

    private static ItemResponse<T> CreateItemResponse<T>(T item, HttpStatusCode statusCode, string etag = null, bool suppressContent = false)
    {
        var r = Substitute.For<ItemResponse<T>>();
        r.StatusCode.Returns(statusCode);
        r.Resource.Returns(suppressContent ? default : item);
        var headers = new Headers
        {
            ["x-ms-session-token"] = $"0:{Guid.NewGuid():N}"
        };
        r.Headers.Returns(headers);
        r.Diagnostics.Returns(FakeDiagnostics);
        r.RequestCharge.Returns(SyntheticRequestCharge);
        r.ActivityId.Returns(Guid.NewGuid().ToString());
        r.ETag.Returns(etag ?? $"\"{Guid.NewGuid()}\"");
        return r;
    }

    private static ResponseMessage CreateResponseMessage(HttpStatusCode statusCode, string json = null, string etag = null)
    {
        var msg = new ResponseMessage(statusCode) { Content = json is not null ? ToStream(json) : null };
        msg.Headers["x-ms-activity-id"] = Guid.NewGuid().ToString();
        msg.Headers["x-ms-request-charge"] = SyntheticRequestCharge.ToString(CultureInfo.InvariantCulture);
        if (etag is not null)
        {
            msg.Headers["ETag"] = etag;
        }

        return msg;
    }

    private static void ValidateDocumentSize(string json)
    {
        if (Encoding.UTF8.GetByteCount(json) > MaxDocumentSizeBytes)
        {
            throw new CosmosException("Request size is too large.", HttpStatusCode.RequestEntityTooLarge, 0, string.Empty, 0);
        }
    }

    private void ValidateUniqueKeys(JObject jObj, string partitionKey, string excludeItemId = null)
    {
        var policy = _containerProperties.UniqueKeyPolicy;
        if (policy == null || policy.UniqueKeys.Count == 0) return;

        foreach (var uniqueKey in policy.UniqueKeys)
        {
            var newValues = uniqueKey.Paths.Select(p => jObj.SelectToken(p.TrimStart('/'))?.ToString()).ToList();

            foreach (var (existingKey, existingJson) in _items)
            {
                if (existingKey.PartitionKey != partitionKey) continue;
                if (excludeItemId != null && existingKey.Id == excludeItemId) continue;

                var existingObj = JsonParseHelpers.ParseJson(existingJson);
                var existingValues = uniqueKey.Paths.Select(p => existingObj.SelectToken(p.TrimStart('/'))?.ToString()).ToList();

                if (newValues.SequenceEqual(existingValues))
                {
                    throw new CosmosException(
                        "Unique index constraint violation.",
                        HttpStatusCode.Conflict, 0, string.Empty, 0);
                }
            }
        }
    }

    private bool ValidateUniqueKeysStream(JObject jObj, string partitionKey, string excludeItemId = null)
    {
        try
        {
            ValidateUniqueKeys(jObj, partitionKey, excludeItemId);
            return true;
        }
        catch (CosmosException)
        {
            return false;
        }
    }

    private bool IsExpired((string Id, string PartitionKey) key)
    {
        if (!_timestamps.TryGetValue(key, out var ts))
        {
            return false;
        }

        var elapsed = (DateTimeOffset.UtcNow - ts).TotalSeconds;

        if (_items.TryGetValue(key, out var json))
        {
            var jObj = JsonParseHelpers.ParseJson(json);
            var itemTtl = jObj["_ttl"];
            if (itemTtl is not null && int.TryParse(itemTtl.ToString(), out var perItemTtl))
            {
                return elapsed >= perItemTtl;
            }
        }

        if (DefaultTimeToLive is null or <= 0)
        {
            return false;
        }

        return elapsed >= DefaultTimeToLive.Value;
    }

    private void EvictIfExpired((string Id, string PartitionKey) key)
    {
        if (!IsExpired(key))
        {
            return;
        }

        _items.TryRemove(key, out _);
        _etags.TryRemove(key, out _);
        _timestamps.TryRemove(key, out _);
    }

    internal Dictionary<(string Id, string PartitionKey), string> SnapshotItems()
        => new(_items);

    internal Dictionary<(string Id, string PartitionKey), string> SnapshotEtags()
        => new(_etags);

    internal void RestoreSnapshot(
        Dictionary<(string Id, string PartitionKey), string> itemsSnapshot,
        Dictionary<(string Id, string PartitionKey), string> etagsSnapshot)
    {
        _items.Clear();
        foreach (var kvp in itemsSnapshot)
        {
            _items[kvp.Key] = kvp.Value;
        }

        _etags.Clear();
        foreach (var kvp in etagsSnapshot)
        {
            _etags[kvp.Key] = kvp.Value;
        }
    }

    private Scripts ConfigureScripts()
    {
        var scripts = Substitute.For<Scripts>();

        scripts.CreateStoredProcedureAsync(
            Arg.Any<StoredProcedureProperties>(), Arg.Any<RequestOptions>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                var props = ci.Arg<StoredProcedureProperties>();
                var r = Substitute.For<StoredProcedureResponse>();
                r.StatusCode.Returns(HttpStatusCode.Created);
                r.Resource.Returns(props);
                return r;
            });

        scripts.ExecuteStoredProcedureAsync<string>(
            Arg.Any<string>(), Arg.Any<PartitionKey>(), Arg.Any<dynamic[]>(),
            Arg.Any<StoredProcedureRequestOptions>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                var sprocId = ci.ArgAt<string>(0);
                var pk = ci.Arg<PartitionKey>();
                var sprocArgs = ci.ArgAt<dynamic[]>(2);
                var r = Substitute.For<StoredProcedureExecuteResponse<string>>();
                r.StatusCode.Returns(HttpStatusCode.OK);
                r.RequestCharge.Returns(SyntheticRequestCharge);
                if (_storedProcedures.TryGetValue(sprocId, out var handler))
                {
                    r.Resource.Returns(handler(pk, sprocArgs));
                }
                return r;
            });

        scripts.CreateTriggerAsync(
            Arg.Any<TriggerProperties>(), Arg.Any<RequestOptions>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                var props = ci.Arg<TriggerProperties>();
                _triggerProperties[props.Id] = props;
                var r = Substitute.For<TriggerResponse>();
                r.StatusCode.Returns(HttpStatusCode.Created);
                r.Resource.Returns(props);
                return r;
            });

        scripts.ReadTriggerAsync(
            Arg.Any<string>(), Arg.Any<RequestOptions>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                var triggerId = ci.ArgAt<string>(0);
                if (!_triggerProperties.TryGetValue(triggerId, out var props))
                {
                    throw new CosmosException($"Trigger '{triggerId}' not found.",
                        HttpStatusCode.NotFound, 0, string.Empty, 0);
                }
                var r = Substitute.For<TriggerResponse>();
                r.StatusCode.Returns(HttpStatusCode.OK);
                r.Resource.Returns(props);
                return r;
            });

        scripts.ReplaceTriggerAsync(
            Arg.Any<TriggerProperties>(), Arg.Any<RequestOptions>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                var props = ci.Arg<TriggerProperties>();
                if (!_triggerProperties.ContainsKey(props.Id))
                {
                    throw new CosmosException($"Trigger '{props.Id}' not found.",
                        HttpStatusCode.NotFound, 0, string.Empty, 0);
                }
                _triggerProperties[props.Id] = props;
                var r = Substitute.For<TriggerResponse>();
                r.StatusCode.Returns(HttpStatusCode.OK);
                r.Resource.Returns(props);
                return r;
            });

        scripts.DeleteTriggerAsync(
            Arg.Any<string>(), Arg.Any<RequestOptions>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                var triggerId = ci.ArgAt<string>(0);
                if (!_triggerProperties.Remove(triggerId))
                {
                    throw new CosmosException($"Trigger '{triggerId}' not found.",
                        HttpStatusCode.NotFound, 0, string.Empty, 0);
                }
                _triggers.Remove(triggerId);
                var r = Substitute.For<TriggerResponse>();
                r.StatusCode.Returns(HttpStatusCode.NoContent);
                return r;
            });

        scripts.CreateUserDefinedFunctionAsync(
            Arg.Any<UserDefinedFunctionProperties>(), Arg.Any<RequestOptions>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                var props = ci.Arg<UserDefinedFunctionProperties>();
                if (!_userDefinedFunctions.ContainsKey("UDF." + props.Id))
                {
                    _userDefinedFunctions["UDF." + props.Id] = _ => null;
                }
                var r = Substitute.For<UserDefinedFunctionResponse>();
                r.StatusCode.Returns(HttpStatusCode.Created);
                r.Resource.Returns(props);
                return r;
            });

        return scripts;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Private helpers — Query execution pipeline
    // ═══════════════════════════════════════════════════════════════════════════

    private List<string> GetAllItemsForPartition(QueryRequestOptions requestOptions)
    {
        if (requestOptions?.PartitionKey is not null
            && requestOptions.PartitionKey != PartitionKey.None
            && requestOptions.PartitionKey != PartitionKey.Null)
        {
            var pk = PartitionKeyToString(requestOptions.PartitionKey.Value);
            return _items.Where(kvp => kvp.Key.PartitionKey == pk && !IsExpired(kvp.Key)).Select(kvp => kvp.Value).ToList();
        }
        return _items.Where(kvp => !IsExpired(kvp.Key)).Select(kvp => kvp.Value).ToList();
    }

    private const string UdfRegistryKey = "__udf_registry__";

    private List<string> FilterItemsByQuery(
        string queryText, IDictionary<string, object> parameters, QueryRequestOptions requestOptions)
    {
        if (_userDefinedFunctions.Count > 0)
        {
            parameters[UdfRegistryKey] = _userDefinedFunctions;
        }

        // Snapshot static datetime values so they remain constant for the entire query
        parameters["__staticDateTime"] = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ");
        parameters["__staticTicks"] = DateTime.UtcNow.Ticks;
        parameters["__staticTimestamp"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        var parsed = CosmosSqlParser.Parse(queryText);

        var items = GetAllItemsForPartition(requestOptions);

        // FROM alias IN c.field — top-level array iteration (must come before JOINs/WHERE)
        if (parsed.FromSource is not null)
        {
            items = ExpandFromSource(items, parsed);
        }

        // JOIN expansion (supports multiple JOINs) — must come before WHERE
        if (parsed.Joins is { Length: > 0 })
        {
            items = ExpandAllJoins(items, parsed);
        }
        else if (parsed.Join is not null)
        {
            items = ExpandJoinedItems(items, parsed);
        }

        // WHERE
        if (parsed.Where is not null)
        {
            items = items.Where(json =>
            {
                var jObj = JsonParseHelpers.ParseJson(json);
                return EvaluateWhereExpression(parsed.Where, jObj, parsed.FromAlias, parameters, parsed.Join);
            }).ToList();
        }

        // GROUP BY / HAVING — also handles SELECT projection for grouped results
        var groupByApplied = false;
        if (parsed.GroupByFields is { Length: > 0 })
        {
            items = ApplyGroupBy(items, parsed, parameters);
            groupByApplied = true;
        }

        // ORDER BY
        if (parsed.OrderByFields is { Length: > 0 })
        {
            items = ApplyOrderByFields(items, parsed.OrderByFields, parsed.FromAlias, parameters);
        }
        else if (parsed.OrderBy is not null)
        {
            items = ApplyOrderBy(items, parsed.OrderBy, parsed.FromAlias);
        }
        else if (parsed.RankExpression is not null)
        {
            items = ApplyOrderByRank(items, parsed.RankExpression, parsed.FromAlias, parameters);
        }

        // TOP
        if (parsed.TopCount.HasValue)
        {
            items = items.Take(parsed.TopCount.Value).ToList();
        }

        // OFFSET / LIMIT
        if (parsed.Offset.HasValue)
        {
            items = items.Skip(parsed.Offset.Value).ToList();
        }

        if (parsed.Limit.HasValue)
        {
            items = items.Take(parsed.Limit.Value).ToList();
        }

        // SELECT projection (skip if GROUP BY already projected)
        if (!groupByApplied && !parsed.IsSelectAll && parsed.SelectFields.Length > 0)
        {
            items = ProjectFields(items, parsed, parameters);
        }

        // DISTINCT — applied after projection so dedup works on projected shapes
        if (parsed.IsDistinct)
        {
            items = items.Distinct().ToList();
        }

        // VALUE SELECT — unwrap scalar values from projected JObjects
        if (parsed.IsValueSelect && parsed.SelectFields.Length == 1)
        {
            items = UnwrapValueSelect(items);
        }

        return items;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Query helpers — ORDER BY
    // ═══════════════════════════════════════════════════════════════════════════

    private static List<string> ApplyOrderBy(List<string> items, OrderByClause orderBy, string fromAlias)
        => ApplyOrderByFields(items, new[] { new OrderByField(orderBy.Field, orderBy.Ascending) }, fromAlias);

    private static List<string> ApplyOrderByRank(
        List<string> items, SqlExpression rankExpr, string fromAlias, IDictionary<string, object> parameters)
    {
        // ORDER BY RANK sorts by the evaluated expression descending (highest score first).
        return items
            .OrderByDescending(json =>
            {
                var jObj = JsonParseHelpers.ParseJson(json);
                var score = EvaluateSqlExpression(rankExpr, jObj, fromAlias, parameters);
                return score switch
                {
                    double d => d,
                    long l => (double)l,
                    int i => (double)i,
                    _ => 0.0
                };
            })
            .ToList();
    }

    private static List<string> ApplyOrderByFields(
        List<string> items, OrderByField[] orderByFields, string fromAlias,
        IDictionary<string, object> parameters = null)
    {
        if (orderByFields.Length == 0)
        {
            return items;
        }

        IOrderedEnumerable<string> ordered = null;
        foreach (var field in orderByFields)
        {
            object KeySelector(string json)
            {
                var jObj = JsonParseHelpers.ParseJson(json);

                if (field.Expression is not null)
                {
                    var value = EvaluateSqlExpression(field.Expression, jObj, fromAlias,
                        parameters ?? new Dictionary<string, object>());
                    return value switch
                    {
                        double d => (object)d,
                        long l => (object)(double)l,
                        int i => (object)(double)i,
                        decimal dec => (object)(double)dec,
                        _ => null
                    };
                }

                var fieldPath = field.Field;
                if (fieldPath.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
                {
                    fieldPath = fieldPath[(fromAlias.Length + 1)..];
                }

                var token = jObj.SelectToken(fieldPath);
                if (token is null)
                {
                    return null;
                }

                return token.Type switch
                {
                    JTokenType.Integer => token.Value<long>(),
                    JTokenType.Float => token.Value<double>(),
                    _ => (object)token.ToString()
                };
            }

            var asc = field.Ascending;
            var comparer = Comparer<object>.Create((l, r) =>
            {
                var result = CompareValues(l, r);
                return asc ? result : -result;
            });
            ordered = ordered == null ? items.OrderBy(KeySelector, comparer) : ordered.ThenBy(KeySelector, comparer);
        }
        return ordered?.ToList() ?? items;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Query helpers — GROUP BY / HAVING
    // ═══════════════════════════════════════════════════════════════════════════

    private List<string> ApplyGroupBy(List<string> items, CosmosSqlQuery parsed, IDictionary<string, object> parameters)
    {
        var fromAlias = parsed.FromAlias;

        var groups = items.GroupBy(json =>
        {
            var jObj = JsonParseHelpers.ParseJson(json);
            var keyParts = new List<string>();
            foreach (var field in parsed.GroupByFields)
            {
                var path = field;
                if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
                {
                    path = path[(fromAlias.Length + 1)..];
                }

                var token = jObj.SelectToken(path);
                keyParts.Add(token?.ToString() ?? "null");
            }
            return string.Join("|", keyParts);
        });

        var hasAggregate = parsed.SelectFields.Any(f =>
        {
            var expr = f.Expression.TrimStart();
            return AggregateFunctions.Any(fn => expr.StartsWith(fn + "(", StringComparison.OrdinalIgnoreCase));
        });

        if (!hasAggregate)
        {
            return groups.Select(g =>
            {
                var jObj = JsonParseHelpers.ParseJson(g.First());
                var projected = new JObject();
                foreach (var field in parsed.SelectFields)
                {
                    var path = field.Expression;
                    if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
                    {
                        path = path[(fromAlias.Length + 1)..];
                    }

                    var outputName = field.Alias ?? path.Split('.').Last();
                    projected[outputName] = jObj.SelectToken(path)?.DeepClone();
                }
                return projected.ToString(Formatting.None);
            }).ToList();
        }

        var result = new List<string>();
        foreach (var group in groups)
        {
            var groupItems = group.ToList();
            var resultObj = new JObject();

            foreach (var field in parsed.SelectFields)
            {
                var expr = field.Expression.TrimStart();
                string funcName = null;
                string innerArg = null;

                foreach (var fn in AggregateFunctions)
                {
                    if (expr.StartsWith(fn + "(", StringComparison.OrdinalIgnoreCase))
                    {
                        funcName = fn;
                        var open = expr.IndexOf('(');
                        var close = expr.LastIndexOf(')');
                        if (open >= 0 && close > open)
                        {
                            innerArg = expr.Substring(open + 1, close - open - 1).Trim();
                        }

                        break;
                    }
                }

                var outputName = field.Alias ?? field.Expression;

                if (funcName == "COUNT")
                {
                    if (innerArg is "1" or "*" or null)
                    {
                        resultObj[outputName] = groupItems.Count;
                    }
                    else
                    {
                        var countPath = innerArg;
                        if (countPath.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
                            countPath = countPath[(fromAlias.Length + 1)..];
                        resultObj[outputName] = groupItems.Count(json =>
                        {
                            var jObj = JsonParseHelpers.ParseJson(json);
                            return jObj.SelectToken(countPath) != null;
                        });
                    }
                }
                else if (funcName is "SUM" or "AVG" && innerArg != null)
                {
                    var values = ExtractNumericValues(groupItems, innerArg, fromAlias);
                    if (funcName == "SUM")
                    {
                        resultObj[outputName] = values.Sum();
                    }
                    else // AVG
                    {
                        if (values.Count > 0)
                            resultObj[outputName] = values.Average();
                    }
                }
                else if (funcName is "MIN" or "MAX" && innerArg != null)
                {
                    var tokens = ExtractTokenValues(groupItems, innerArg, fromAlias);
                    if (tokens.Count > 0)
                    {
                        var numericValues = new List<double>();
                        var stringValues = new List<string>();
                        foreach (var t in tokens)
                        {
                            if (t.Type is JTokenType.Integer or JTokenType.Float)
                                numericValues.Add(t.Value<double>());
                            else if (t.Type == JTokenType.String)
                                stringValues.Add(t.Value<string>());
                        }

                        if (numericValues.Count > 0)
                            resultObj[outputName] = funcName == "MIN" ? numericValues.Min() : numericValues.Max();
                        else if (stringValues.Count > 0)
                            resultObj[outputName] = funcName == "MIN"
                                ? stringValues.OrderBy(s => s, StringComparer.Ordinal).First()
                                : stringValues.OrderByDescending(s => s, StringComparer.Ordinal).First();
                    }
                }
                else
                {
                    var path = field.Expression;
                    if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
                    {
                        path = path[(fromAlias.Length + 1)..];
                    }

                    var fieldOutputName = field.Alias ?? path.Split('.').Last();
                    var jObj = JsonParseHelpers.ParseJson(groupItems[0]);
                    resultObj[fieldOutputName] = jObj.SelectToken(path)?.DeepClone();
                }
            }

            if (parsed.Having is not null)
            {
                if (!EvaluateHavingCondition(parsed.Having, groupItems, resultObj, fromAlias, parameters))
                {
                    continue;
                }
            }

            result.Add(resultObj.ToString(Formatting.None));
        }
        return result;
    }

    private static List<double> ExtractNumericValues(List<string> items, string innerArg, string fromAlias)
    {
        var values = new List<double>();
        foreach (var json in items)
        {
            var jObj = JsonParseHelpers.ParseJson(json);
            var path = innerArg;
            if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
            {
                path = path[(fromAlias.Length + 1)..];
            }

            var token = jObj.SelectToken(path);
            if (token != null && double.TryParse(token.ToString(), NumberStyles.Any,
                    CultureInfo.InvariantCulture, out var val))
            {
                values.Add(val);
            }
        }
        return values;
    }

    private static List<JToken> ExtractTokenValues(List<string> items, string innerArg, string fromAlias)
    {
        var tokens = new List<JToken>();
        foreach (var json in items)
        {
            var jObj = JsonParseHelpers.ParseJson(json);
            var path = innerArg;
            if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
            {
                path = path[(fromAlias.Length + 1)..];
            }

            var token = jObj.SelectToken(path);
            if (token != null && token.Type != JTokenType.Null)
            {
                tokens.Add(token);
            }
        }
        return tokens;
    }

    private static bool EvaluateHavingCondition(
        WhereExpression having, List<string> groupItems, JObject resultObj,
        string fromAlias, IDictionary<string, object> parameters)
    {
        if (having is SqlExpressionCondition sec)
        {
            return IsTruthy(EvaluateHavingSqlExpression(sec.Expression, groupItems, resultObj, fromAlias, parameters));
        }

        return EvaluateWhereExpression(having, resultObj, fromAlias, parameters, null);
    }

    private static object EvaluateHavingSqlExpression(
        SqlExpression expr, List<string> groupItems, JObject resultObj,
        string fromAlias, IDictionary<string, object> parameters)
    {
        return expr switch
        {
            FunctionCallExpression func when AggregateFunctions.Contains(func.FunctionName) =>
                EvaluateHavingAggregate(func, groupItems, fromAlias),
            BinaryExpression bin => EvaluateHavingBinaryExpression(bin, groupItems, resultObj, fromAlias, parameters),
            LiteralExpression lit => lit.Value,
            IdentifierExpression ident => ResolveValue(ident.Name, resultObj, fromAlias, parameters),
            _ => EvaluateSqlExpression(expr, resultObj, fromAlias, parameters)
        };
    }

    private static object EvaluateHavingBinaryExpression(
        BinaryExpression bin, List<string> groupItems, JObject resultObj,
        string fromAlias, IDictionary<string, object> parameters)
    {
        var left = EvaluateHavingSqlExpression(bin.Left, groupItems, resultObj, fromAlias, parameters);
        var right = EvaluateHavingSqlExpression(bin.Right, groupItems, resultObj, fromAlias, parameters);
        return bin.Operator switch
        {
            BinaryOp.GreaterThan => CompareValues(left, right) > 0,
            BinaryOp.GreaterThanOrEqual => CompareValues(left, right) >= 0,
            BinaryOp.LessThan => CompareValues(left, right) < 0,
            BinaryOp.LessThanOrEqual => CompareValues(left, right) <= 0,
            BinaryOp.Equal => (object)ValuesEqual(left, right),
            BinaryOp.NotEqual => !ValuesEqual(left, right),
            BinaryOp.And => IsTruthy(left) && IsTruthy(right),
            BinaryOp.Or => IsTruthy(left) || IsTruthy(right),
            _ => null
        };
    }

    private static object EvaluateHavingAggregate(
        FunctionCallExpression func, List<string> groupItems, string fromAlias)
    {
        switch (func.FunctionName)
        {
            case "COUNT": return (double)groupItems.Count;
            case "SUM":
            case "AVG":
            case "MIN":
            case "MAX":
                {
                    if (func.Arguments.Length < 1)
                    {
                        return 0.0;
                    }

                    var innerArg = func.Arguments[0] is IdentifierExpression ident ? ident.Name : "1";
                    var values = ExtractNumericValues(groupItems, innerArg, fromAlias);
                    return func.FunctionName switch
                    {
                        "SUM" => values.Sum(),
                        "AVG" => values.Count > 0 ? values.Average() : 0.0,
                        "MIN" => values.Count > 0 ? values.Min() : 0.0,
                        "MAX" => values.Count > 0 ? values.Max() : 0.0,
                        _ => 0.0
                    };
                }
            default: return null;
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Query helpers — JOIN expansion
    // ═══════════════════════════════════════════════════════════════════════════

    private static List<string> ExpandJoinedItems(List<string> items, CosmosSqlQuery parsed)
    {
        if (parsed.Join is null)
        {
            return items;
        }

        var expanded = new List<string>();
        foreach (var json in items)
        {
            var jObj = JsonParseHelpers.ParseJson(json);
            var arrayToken = jObj.SelectToken(parsed.Join.ArrayField);
            if (arrayToken is JArray jArray)
            {
                foreach (var element in jArray)
                {
                    var combined = new JObject(jObj.Properties())
                    {
                        [parsed.Join.Alias] = element
                    };
                    expanded.Add(combined.ToString(Formatting.None));
                }
            }
        }
        return expanded;
    }

    private static List<string> ExpandAllJoins(List<string> items, CosmosSqlQuery parsed)
    {
        var current = items;
        foreach (var join in parsed.Joins)
        {
            var expanded = new List<string>();
            foreach (var json in current)
            {
                var jObj = JsonParseHelpers.ParseJson(json);
                var arrayToken = jObj.SelectToken(join.ArrayField);
                if (arrayToken is JArray jArray)
                {
                    foreach (var element in jArray)
                    {
                        var combined = new JObject(jObj.Properties())
                        {
                            [join.Alias] = element
                        };
                        expanded.Add(combined.ToString(Formatting.None));
                    }
                }
            }
            current = expanded;
        }
        return current;
    }

    /// <summary>
    /// Expands top-level <c>FROM alias IN c.field</c> — each array element becomes a result row.
    /// </summary>
    private static List<string> ExpandFromSource(List<string> items, CosmosSqlQuery parsed)
    {
        var sourcePath = parsed.FromSource;
        // The FromSource is the full dotted path (e.g. "c.tags"). We need to resolve it
        // relative to each document, but the outer alias isn't available here (the FROM clause
        // redefines the alias). Use a simple heuristic: strip the first segment if it looks
        // like an alias (contains a dot).
        var dotIndex = sourcePath.IndexOf('.');
        var arrayPath = dotIndex >= 0 ? sourcePath[(dotIndex + 1)..] : sourcePath;

        var expanded = new List<string>();
        foreach (var json in items)
        {
            var jObj = JsonParseHelpers.ParseJson(json);
            var arrayToken = jObj.SelectToken(arrayPath);
            if (arrayToken is not JArray jArray)
                continue;

            foreach (var element in jArray)
            {
                // The alias is the range variable (e.g. "item" in FROM item IN c.items).
                // - For object elements: spread properties at root so "item.price" → strip alias → "price" resolves.
                // - Also keep the element under the alias for "SELECT item" / "SELECT VALUE item".
                var combined = element is JObject elementObj
                    ? new JObject(elementObj.Properties()) { [parsed.FromAlias] = element.DeepClone() }
                    : new JObject { [parsed.FromAlias] = element };
                expanded.Add(combined.ToString(Formatting.None));
            }
        }
        return expanded;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Query helpers — SELECT projection
    // ═══════════════════════════════════════════════════════════════════════════

    private static List<string> ProjectFields(List<string> items, CosmosSqlQuery parsed, IDictionary<string, object> parameters)
    {
        var hasAggregate = parsed.SelectFields.Any(f =>
        {
            var expr = f.Expression.TrimStart();
            return AggregateFunctions.Any(fn => expr.StartsWith(fn + "(", StringComparison.OrdinalIgnoreCase));
        });

        if (hasAggregate)
        {
            return ProjectAggregateFields(items, parsed);
        }

        var projected = new List<string>();
        foreach (var json in items)
        {
            var jObj = JsonParseHelpers.ParseJson(json);
            var resultObj = new JObject();
            foreach (var field in parsed.SelectFields)
            {
                var outputName = field.Alias ?? field.Expression.Split('.').Last();

                if (field.SqlExpr is not null and not IdentifierExpression)
                {
                    var value = EvaluateSqlExpression(field.SqlExpr, jObj, parsed.FromAlias, parameters);
                    if (value is not UndefinedValue)
                        resultObj[outputName] = value is not null ? JToken.FromObject(value) : JValue.CreateNull();
                }
                else
                {
                    var path = field.Expression;

                    // When the expression is exactly the FROM alias (e.g., SELECT VALUE root),
                    // return the entire document rather than looking for a property named "root".
                    // Exception: for FROM alias IN c.field, the alias is a property on the expanded
                    // JObject — use the alias value (the array element) instead of the whole doc.
                    if (string.Equals(path, parsed.FromAlias, StringComparison.OrdinalIgnoreCase))
                    {
                        var aliasToken = jObj[parsed.FromAlias];
                        if (parsed.FromSource is not null && aliasToken is not null)
                        {
                            resultObj[outputName] = aliasToken.DeepClone();
                        }
                        else
                        {
                            resultObj[outputName] = jObj.DeepClone();
                        }
                        continue;
                    }

                    if (path.StartsWith(parsed.FromAlias + ".", StringComparison.OrdinalIgnoreCase))
                    {
                        path = path[(parsed.FromAlias.Length + 1)..];
                    }

                    var token = jObj.SelectToken(path);
                    outputName = field.Alias ?? path.Split('.').Last();
                    resultObj[outputName] = token?.DeepClone();
                }
            }
            projected.Add(resultObj.ToString(Formatting.None));
        }
        return projected;
    }

    private static List<string> UnwrapValueSelect(List<string> items)
    {
        var unwrapped = new List<string>();
        foreach (var json in items)
        {
            var jObj = JsonParseHelpers.ParseJson(json);
            if (!jObj.HasValues)
            {
                // Empty object means the projected value was undefined — skip (omit from results)
                continue;
            }

            var first = jObj.Properties().FirstOrDefault();
            if (first?.Value is not null)
            {
                var val = first.Value;
                if (val.Type == JTokenType.String)
                {
                    unwrapped.Add(JsonConvert.SerializeObject(val.Value<string>()));
                }
                else
                {
                    unwrapped.Add(val.ToString(Formatting.None));
                }
            }
            else
            {
                unwrapped.Add("null");
            }
        }
        return unwrapped;
    }

    private static List<string> ProjectAggregateFields(List<string> items, CosmosSqlQuery parsed)
    {
        var resultObj = new JObject();
        foreach (var field in parsed.SelectFields)
        {
            var expr = field.Expression.TrimStart();
            string funcName = null;
            string innerArg = null;

            foreach (var fn in AggregateFunctions)
            {
                if (expr.StartsWith(fn + "(", StringComparison.OrdinalIgnoreCase))
                {
                    funcName = fn.ToUpperInvariant();
                    var open = expr.IndexOf('(');
                    var close = expr.LastIndexOf(')');
                    if (open >= 0 && close > open)
                    {
                        innerArg = expr.Substring(open + 1, close - open - 1).Trim();
                    }

                    break;
                }
            }

            var outputName = field.Alias ?? field.Expression;

            if (funcName == "COUNT")
            {
                if (innerArg is "1" or "*" or null)
                {
                    resultObj[outputName] = items.Count;
                }
                else
                {
                    var countPath = innerArg;
                    if (countPath.StartsWith(parsed.FromAlias + ".", StringComparison.OrdinalIgnoreCase))
                        countPath = countPath[(parsed.FromAlias.Length + 1)..];
                    resultObj[outputName] = items.Count(json =>
                    {
                        var jObj = JsonParseHelpers.ParseJson(json);
                        return jObj.SelectToken(countPath) != null;
                    });
                }
            }
            else if (funcName is "SUM" or "AVG" && innerArg != null)
            {
                var values = ExtractNumericValues(items, innerArg, parsed.FromAlias);
                if (funcName == "SUM")
                {
                    resultObj[outputName] = values.Sum();
                }
                else // AVG
                {
                    if (values.Count > 0)
                        resultObj[outputName] = values.Average();
                    // else: omit field entirely (undefined)
                }
            }
            else if (funcName is "MIN" or "MAX" && innerArg != null)
            {
                var tokens = ExtractTokenValues(items, innerArg, parsed.FromAlias);
                if (tokens.Count > 0)
                {
                    // Try numeric first
                    var numericValues = new List<double>();
                    var stringValues = new List<string>();
                    foreach (var t in tokens)
                    {
                        if (t.Type is JTokenType.Integer or JTokenType.Float)
                            numericValues.Add(t.Value<double>());
                        else if (t.Type == JTokenType.String)
                            stringValues.Add(t.Value<string>());
                    }

                    if (numericValues.Count > 0)
                        resultObj[outputName] = funcName == "MIN" ? numericValues.Min() : numericValues.Max();
                    else if (stringValues.Count > 0)
                        resultObj[outputName] = funcName == "MIN"
                            ? stringValues.OrderBy(s => s, StringComparer.Ordinal).First()
                            : stringValues.OrderByDescending(s => s, StringComparer.Ordinal).First();
                }
            }
            else
            {
                var path = field.Expression;
                if (path.StartsWith(parsed.FromAlias + ".", StringComparison.OrdinalIgnoreCase))
                {
                    path = path[(parsed.FromAlias.Length + 1)..];
                }

                if (items.Count > 0)
                {
                    var jObj = JsonParseHelpers.ParseJson(items[0]);
                    resultObj[outputName] = jObj.SelectToken(path)?.DeepClone();
                }
            }
        }
        return new List<string> { resultObj.ToString(Formatting.None) };
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Query helpers — Parameter extraction
    // ═══════════════════════════════════════════════════════════════════════════

    private static Dictionary<string, object> ExtractQueryParameters(QueryDefinition queryDefinition)
    {
        var parameters = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
        try
        {
            foreach (var (Name, Value) in queryDefinition.GetQueryParameters())
            {
                parameters[Name] = Value;
            }

            if (parameters.Count > 0)
            {
                return parameters;
            }
        }
        catch { /* Fall through to reflection */ }

        try
        {
            var internalField = typeof(QueryDefinition)
                .GetFields(BindingFlags.NonPublic | BindingFlags.Instance)
                .FirstOrDefault(f => f.Name.Contains("parameter", StringComparison.OrdinalIgnoreCase));

            if (internalField?.GetValue(queryDefinition) is System.Collections.IEnumerable enumerable)
            {
                foreach (var item in enumerable)
                {
                    var itemType = item.GetType();
                    var nameProp = itemType.GetProperty("Name") ?? itemType.GetProperty("Item1");
                    var valueProp = itemType.GetProperty("Value") ?? itemType.GetProperty("Item2");
                    if (nameProp is not null && valueProp is not null)
                    {
                        var name = nameProp.GetValue(item)?.ToString();
                        if (name is not null)
                        {
                            parameters[name] = valueProp.GetValue(item);
                        }
                    }
                    else
                    {
                        var nameField = itemType.GetField("Name") ?? itemType.GetField("Item1");
                        var valueField = itemType.GetField("Value") ?? itemType.GetField("Item2");
                        if (nameField is not null && valueField is not null)
                        {
                            var name = nameField.GetValue(item)?.ToString();
                            if (name is not null)
                            {
                                parameters[name] = valueField.GetValue(item);
                            }
                        }
                    }
                }
            }
        }
        catch { /* Parameters cannot be extracted */ }

        return parameters;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  WHERE expression evaluation
    // ═══════════════════════════════════════════════════════════════════════════

    private static bool EvaluateWhereExpression(
        WhereExpression expression, JObject item, string fromAlias,
        IDictionary<string, object> parameters, JoinClause join)
    {
        return expression switch
        {
            ComparisonCondition c => EvaluateComparison(c, item, fromAlias, parameters),
            AndCondition a => EvaluateWhereExpression(a.Left, item, fromAlias, parameters, join)
                              && EvaluateWhereExpression(a.Right, item, fromAlias, parameters, join),
            OrCondition o => EvaluateWhereExpression(o.Left, item, fromAlias, parameters, join)
                             || EvaluateWhereExpression(o.Right, item, fromAlias, parameters, join),
            NotCondition n => !EvaluateWhereExpression(n.Inner, item, fromAlias, parameters, join),
            FunctionCondition f => EvaluateFunction(f, item, fromAlias, parameters),
            ExistsCondition e => EvaluateExists(e, item, fromAlias, parameters, join),
            SqlExpressionCondition s => IsTruthy(EvaluateSqlExpression(s.Expression, item, fromAlias, parameters)),
            _ => true
        };
    }

    private static bool EvaluateComparison(
        ComparisonCondition comparison, JObject item, string fromAlias, IDictionary<string, object> parameters)
    {
        var leftValue = ResolveValue(comparison.Left, item, fromAlias, parameters);
        var rightValue = ResolveValue(comparison.Right, item, fromAlias, parameters);
        if (leftValue is UndefinedValue || rightValue is UndefinedValue)
            return false;
        return comparison.Operator switch
        {
            ComparisonOp.Equal => ValuesEqual(leftValue, rightValue),
            ComparisonOp.NotEqual => !ValuesEqual(leftValue, rightValue),
            ComparisonOp.LessThan => CompareValues(leftValue, rightValue) < 0,
            ComparisonOp.GreaterThan => CompareValues(leftValue, rightValue) > 0,
            ComparisonOp.LessThanOrEqual => CompareValues(leftValue, rightValue) <= 0,
            ComparisonOp.GreaterThanOrEqual => CompareValues(leftValue, rightValue) >= 0,
            ComparisonOp.Like => EvaluateLike(leftValue, rightValue),
            _ => false
        };
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Value resolution & comparison
    // ═══════════════════════════════════════════════════════════════════════════

    private static object ResolveValue(
        string expression, JObject item, string fromAlias, IDictionary<string, object> parameters)
    {
        var trimmed = expression.Trim();
        if (trimmed.StartsWith("@"))
        {
            return parameters.TryGetValue(trimmed, out var v) ? v : null;
        }

        if (trimmed.StartsWith("'") && trimmed.EndsWith("'"))
        {
            return trimmed[1..^1];
        }

        if (string.Equals(trimmed, "true", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (string.Equals(trimmed, "false", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (string.Equals(trimmed, "null", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        if (double.TryParse(trimmed, NumberStyles.Any, CultureInfo.InvariantCulture, out var num))
        {
            return num;
        }

        var jsonPath = trimmed;
        if (jsonPath.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
        {
            jsonPath = jsonPath[(fromAlias.Length + 1)..];
        }

        var token = item.SelectToken(jsonPath);
        if (token == null)
        {
            return UndefinedValue.Instance;
        }

        return token.Type switch
        {
            JTokenType.String => token.Value<string>(),
            JTokenType.Integer => token.Value<long>(),
            JTokenType.Float => token.Value<double>(),
            JTokenType.Boolean => token.Value<bool>(),
            JTokenType.Null => null,
            JTokenType.Undefined => UndefinedValue.Instance,
            _ => token.ToString()
        };
    }

    private static bool ValuesEqual(object left, object right)
    {
        if (left is UndefinedValue || right is UndefinedValue)
        {
            return false;
        }

        if (left is null && right is null)
        {
            return true;
        }

        if (left is null || right is null)
        {
            return false;
        }

        if ((left is double or long) && (right is double or long))
        {
            if (double.TryParse(left.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var l) &&
                double.TryParse(right.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var r))
            {
                return Math.Abs(l - r) < 0.0001;
            }
        }
        return string.Equals(left.ToString(), right.ToString(), StringComparison.Ordinal);
    }

    private static int CompareValues(object left, object right)
    {
        if (left is null && right is null)
        {
            return 0;
        }

        if (left is null)
        {
            return -1;
        }

        if (right is null)
        {
            return 1;
        }

        if (double.TryParse(left.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var l) &&
            double.TryParse(right.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var r))
        {
            return l.CompareTo(r);
        }

        return string.Compare(left.ToString(), right.ToString(), StringComparison.Ordinal);
    }

    private static bool EvaluateLike(object left, object right, string escapeChar = null)
    {
        if (left is null || right is null)
        {
            return false;
        }

        var patternStr = right.ToString();
        if (escapeChar is { Length: > 0 })
        {
            var esc = escapeChar[0];
            var sb = new System.Text.StringBuilder();
            for (var i = 0; i < patternStr.Length; i++)
            {
                if (patternStr[i] == esc && i + 1 < patternStr.Length)
                {
                    sb.Append(Regex.Escape(patternStr[i + 1].ToString()));
                    i++;
                }
                else if (patternStr[i] == '%')
                    sb.Append(".*");
                else if (patternStr[i] == '_')
                    sb.Append('.');
                else
                    sb.Append(Regex.Escape(patternStr[i].ToString()));
            }
            var pattern = $"^{sb}$";
            return GetOrCreateRegex(pattern, RegexOptions.None).IsMatch(left.ToString());
        }

        var simplePattern = ConvertLikeToRegex(patternStr);
        return GetOrCreateRegex(simplePattern, RegexOptions.None).IsMatch(left.ToString());
    }

    private static string ConvertLikeToRegex(string pattern)
    {
        var sb = new System.Text.StringBuilder("^");
        foreach (var ch in pattern)
        {
            if (ch == '%') sb.Append(".*");
            else if (ch == '_') sb.Append('.');
            else sb.Append(Regex.Escape(ch.ToString()));
        }
        sb.Append('$');
        return sb.ToString();
    }

    private static Regex GetOrCreateRegex(string pattern, RegexOptions options)
    {
        var key = (pattern, options);
        if (RegexCache.TryGetValue(key, out var cached))
        {
            return cached;
        }

        var regex = new Regex(pattern, options | RegexOptions.Compiled);
        if (RegexCache.Count < RegexCacheMaxSize)
        {
            RegexCache.TryAdd(key, regex);
        }

        return regex;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Legacy FunctionCondition evaluation (backward compat)
    // ═══════════════════════════════════════════════════════════════════════════

    private static bool EvaluateFunction(
        FunctionCondition func, JObject item, string fromAlias, IDictionary<string, object> parameters)
    {
        switch (func.FunctionName)
        {
            case "STARTSWITH":
                {
                    if (func.Arguments.Length < 2)
                    {
                        return false;
                    }

                    var fieldValue = ResolveValue(func.Arguments[0], item, fromAlias, parameters)?.ToString();
                    var prefix = ResolveValue(func.Arguments[1], item, fromAlias, parameters)?.ToString();
                    if (fieldValue is null || prefix is null)
                    {
                        return false;
                    }

                    var ignoreCase = func.Arguments.Length >= 3 &&
                        string.Equals(ResolveValue(func.Arguments[2], item, fromAlias, parameters)?.ToString(), "true", StringComparison.OrdinalIgnoreCase);
                    return fieldValue.StartsWith(prefix, ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
                }
            case "ENDSWITH":
                {
                    if (func.Arguments.Length < 2)
                    {
                        return false;
                    }

                    var fieldValue = ResolveValue(func.Arguments[0], item, fromAlias, parameters)?.ToString();
                    var suffix = ResolveValue(func.Arguments[1], item, fromAlias, parameters)?.ToString();
                    if (fieldValue is null || suffix is null)
                    {
                        return false;
                    }

                    var ignoreCase = func.Arguments.Length >= 3 &&
                        string.Equals(ResolveValue(func.Arguments[2], item, fromAlias, parameters)?.ToString(), "true", StringComparison.OrdinalIgnoreCase);
                    return fieldValue.EndsWith(suffix, ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
                }
            case "CONTAINS":
                {
                    if (func.Arguments.Length < 2)
                    {
                        return false;
                    }

                    var fieldValue = ResolveValue(func.Arguments[0], item, fromAlias, parameters)?.ToString();
                    var search = ResolveValue(func.Arguments[1], item, fromAlias, parameters)?.ToString();
                    if (fieldValue is null || search is null)
                    {
                        return false;
                    }

                    var ignoreCase = func.Arguments.Length >= 3 &&
                        string.Equals(ResolveValue(func.Arguments[2], item, fromAlias, parameters)?.ToString(), "true", StringComparison.OrdinalIgnoreCase);
                    return fieldValue.Contains(search, ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
                }
            case "ARRAY_CONTAINS":
                {
                    if (func.Arguments.Length < 2)
                    {
                        return false;
                    }

                    var arrayPath = func.Arguments[0].Trim();
                    if (arrayPath.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
                    {
                        arrayPath = arrayPath[(fromAlias.Length + 1)..];
                    }

                    var arrayToken = item.SelectToken(arrayPath);
                    if (arrayToken is not JArray jArray)
                    {
                        return false;
                    }

                    var searchValue = ResolveValue(func.Arguments[1], item, fromAlias, parameters);
                    if (searchValue is null)
                    {
                        return false;
                    }

                    var searchStr = searchValue.ToString();
                    var partial = func.Arguments.Length >= 3 &&
                        string.Equals(func.Arguments[2].Trim(), "true", StringComparison.OrdinalIgnoreCase);
                    return ArrayContainsMatch(jArray, searchStr, partial);
                }
            case "IS_DEFINED":
                {
                    if (func.Arguments.Length < 1)
                    {
                        return false;
                    }

                    var path = func.Arguments[0].Trim();
                    if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
                    {
                        path = path[(fromAlias.Length + 1)..];
                    }

                    var token = item.SelectToken(path);
                    return token is not null && token.Type != JTokenType.Undefined;
                }
            case "IS_NULL":
                {
                    if (func.Arguments.Length < 1)
                    {
                        return false;
                    }

                    var path = func.Arguments[0].Trim();
                    if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
                    {
                        path = path[(fromAlias.Length + 1)..];
                    }

                    var token = item.SelectToken(path);
                    return token is not null && token.Type is JTokenType.Null;
                }
            default:
                return true;
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  EXISTS subquery evaluation
    // ═══════════════════════════════════════════════════════════════════════════

    private static bool EvaluateExists(
        ExistsCondition exists, JObject item, string fromAlias,
        IDictionary<string, object> parameters, JoinClause join)
    {
        try
        {
            var raw = exists.RawSubquery.Trim();
            var queryToParse = raw.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase)
                ? raw
                : "SELECT * FROM " + raw;
            var subquery = CosmosSqlParser.Parse(queryToParse);

            // Handle FROM alias IN source.path (array iteration in EXISTS subquery)
            if (subquery.FromSource is not null)
            {
                var sourcePath = subquery.FromSource;
                // Strip outer alias prefix (e.g. "c.tags" → "tags" when fromAlias is "c")
                if (sourcePath.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
                {
                    sourcePath = sourcePath[(fromAlias.Length + 1)..];
                }

                var arrayToken = item.SelectToken(sourcePath);
                if (arrayToken is not JArray jArray || jArray.Count == 0)
                {
                    return false;
                }

                if (subquery.Where is not null)
                {
                    var iterAlias = subquery.FromAlias;
                    foreach (var element in jArray)
                    {
                        // Create a combined object with the array element available under its alias
                        var combined = new JObject(item.Properties());
                        combined[iterAlias] = element is JObject elementObj
                            ? elementObj.DeepClone()
                            : new JValue(element.ToObject<object>());
                        if (EvaluateWhereExpression(subquery.Where, combined, iterAlias, parameters, null))
                        {
                            return true;
                        }
                    }
                    return false;
                }
                return true;
            }

            if (subquery.Join is not null)
            {
                var arrayPath = subquery.Join.ArrayField;
                var sourceAlias = subquery.Join.SourceAlias;
                if (sourceAlias.Equals(fromAlias, StringComparison.OrdinalIgnoreCase))
                {
                    var arrayToken = item.SelectToken(arrayPath);
                    if (arrayToken is not JArray jArray || jArray.Count == 0)
                    {
                        return false;
                    }

                    if (subquery.Where is not null)
                    {
                        foreach (var element in jArray)
                        {
                            if (element is JObject elementObj)
                            {
                                var combined = new JObject(item.Properties())
                                {
                                    [subquery.Join.Alias] = elementObj
                                };
                                if (EvaluateWhereExpression(subquery.Where, combined, subquery.FromAlias, parameters, subquery.Join))
                                {
                                    return true;
                                }
                            }
                        }
                        return false;
                    }
                    return true;
                }
            }
            if (subquery.Where is not null)
            {
                return EvaluateWhereExpression(subquery.Where, item, fromAlias, parameters, join);
            }

            return true;
        }
        catch
        {
            return false;
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  SqlExpression tree evaluation
    // ═══════════════════════════════════════════════════════════════════════════

    private static object EvaluateSqlExpression(
        SqlExpression expr, JObject item, string fromAlias, IDictionary<string, object> parameters)
    {
        return expr switch
        {
            LiteralExpression lit => lit.Value,
            IdentifierExpression ident => ResolveValue(ident.Name, item, fromAlias, parameters),
            ParameterExpression param => parameters.TryGetValue(param.Name, out var v) ? v : null,
            FunctionCallExpression func => EvaluateSqlFunction(func, item, fromAlias, parameters),
            BinaryExpression bin => EvaluateBinaryExpression(bin, item, fromAlias, parameters),
            UnaryExpression unary => EvaluateUnaryExpression(unary, item, fromAlias, parameters),
            BetweenExpression between => EvalBetween(between, item, fromAlias, parameters),
            InExpression inExpr => EvalIn(inExpr, item, fromAlias, parameters),
            LikeExpression like => EvaluateLike(
                EvaluateSqlExpression(like.Value, item, fromAlias, parameters),
                EvaluateSqlExpression(like.Pattern, item, fromAlias, parameters),
                like.EscapeChar),
            ExistsExpression exists => EvaluateExists(new ExistsCondition(exists.RawSubquery), item, fromAlias, parameters, null),
            CoalesceExpression coal => EvalCoalesce(coal, item, fromAlias, parameters),
            TernaryExpression tern => IsTruthy(EvaluateSqlExpression(tern.Condition, item, fromAlias, parameters))
                ? EvaluateSqlExpression(tern.IfTrue, item, fromAlias, parameters)
                : EvaluateSqlExpression(tern.IfFalse, item, fromAlias, parameters),
            ObjectLiteralExpression obj => EvaluateObjectLiteral(obj, item, fromAlias, parameters),
            ArrayLiteralExpression arr => EvaluateArrayLiteral(arr, item, fromAlias, parameters),
            PropertyAccessExpression prop => ResolveValue(
                $"{CosmosSqlParser.ExprToString(prop.Object)}.{prop.Property}", item, fromAlias, parameters),
            IndexAccessExpression idx => ResolveValue(
                $"{CosmosSqlParser.ExprToString(idx.Object)}[{CosmosSqlParser.ExprToString(idx.Index)}]", item, fromAlias, parameters),
            SubqueryExpression sub => EvaluateSubquery(sub.Subquery, item, fromAlias, parameters),
            _ => null
        };
    }

    private static object EvalBetween(BetweenExpression b, JObject item, string fromAlias, IDictionary<string, object> parameters)
    {
        var value = EvaluateSqlExpression(b.Value, item, fromAlias, parameters);
        var low = EvaluateSqlExpression(b.Low, item, fromAlias, parameters);
        var high = EvaluateSqlExpression(b.High, item, fromAlias, parameters);
        return CompareValues(value, low) >= 0 && CompareValues(value, high) <= 0;
    }

    private static object EvalCoalesce(CoalesceExpression coal, JObject item, string fromAlias, IDictionary<string, object> parameters)
    {
        var left = EvaluateSqlExpression(coal.Left, item, fromAlias, parameters);
        return left is not null and not UndefinedValue ? left : EvaluateSqlExpression(coal.Right, item, fromAlias, parameters);
    }

    private static object EvalIn(InExpression inExpr, JObject item, string fromAlias, IDictionary<string, object> parameters)
    {
        var value = EvaluateSqlExpression(inExpr.Value, item, fromAlias, parameters);
        return inExpr.List.Any(li => ValuesEqual(value, EvaluateSqlExpression(li, item, fromAlias, parameters)));
    }

    private static object EvaluateBinaryExpression(
        BinaryExpression bin, JObject item, string fromAlias, IDictionary<string, object> parameters)
    {
        var left = EvaluateSqlExpression(bin.Left, item, fromAlias, parameters);
        var right = EvaluateSqlExpression(bin.Right, item, fromAlias, parameters);
        return bin.Operator switch
        {
            BinaryOp.Equal => (object)ValuesEqual(left, right),
            BinaryOp.NotEqual => !ValuesEqual(left, right),
            BinaryOp.LessThan => CompareValues(left, right) < 0,
            BinaryOp.GreaterThan => CompareValues(left, right) > 0,
            BinaryOp.LessThanOrEqual => CompareValues(left, right) <= 0,
            BinaryOp.GreaterThanOrEqual => CompareValues(left, right) >= 0,
            BinaryOp.And => IsTruthy(left) && IsTruthy(right),
            BinaryOp.Or => IsTruthy(left) || IsTruthy(right),
            BinaryOp.Like => EvaluateLike(left, right),
            BinaryOp.Add => ArithmeticOp(left, right, (a, b) => a + b),
            BinaryOp.Subtract => ArithmeticOp(left, right, (a, b) => a - b),
            BinaryOp.Multiply => ArithmeticOp(left, right, (a, b) => a * b),
            BinaryOp.Divide => ArithmeticOp(left, right, (a, b) => b != 0 ? a / b : double.NaN),
            BinaryOp.Modulo => ArithmeticOp(left, right, (a, b) => b != 0 ? a % b : double.NaN),
            BinaryOp.StringConcat => (left?.ToString() ?? "") + (right?.ToString() ?? ""),
            BinaryOp.BitwiseAnd => BitwiseOp(left, right, (a, b) => a & b),
            BinaryOp.BitwiseOr => BitwiseOp(left, right, (a, b) => a | b),
            BinaryOp.BitwiseXor => BitwiseOp(left, right, (a, b) => a ^ b),
            _ => null
        };
    }

    private static object EvaluateUnaryExpression(
        UnaryExpression unary, JObject item, string fromAlias, IDictionary<string, object> parameters)
    {
        var operand = EvaluateSqlExpression(unary.Operand, item, fromAlias, parameters);
        return unary.Operator switch
        {
            UnaryOp.Not => !IsTruthy(operand),
            UnaryOp.Negate => operand is double d ? (object)(-d) : operand is long l ? (object)(-l) : null,
            UnaryOp.BitwiseNot => operand is long lng ? (object)(~lng) : null,
            _ => null
        };
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  SQL function evaluation
    // ═══════════════════════════════════════════════════════════════════════════

    private static object EvaluateSqlFunction(
        FunctionCallExpression func, JObject item, string fromAlias, IDictionary<string, object> parameters)
    {
        // ARRAY(subquery) — evaluate subquery and collect results into a JArray
        if (string.Equals(func.FunctionName, "ARRAY", StringComparison.OrdinalIgnoreCase) &&
            func.Arguments.Length == 1 && func.Arguments[0] is SubqueryExpression subExpr)
        {
            return EvaluateArraySubquery(subExpr.Subquery, item, fromAlias, parameters);
        }

        var args = func.Arguments.Select(a => EvaluateSqlExpression(a, item, fromAlias, parameters)).ToArray();

        switch (func.FunctionName)
        {
            // ── Type checking ──
            case "IS_DEFINED":
                {
                    if (func.Arguments.Length < 1)
                    {
                        return false;
                    }

                    if (func.Arguments[0] is IdentifierExpression ident)
                    {
                        var path = ident.Name;
                        if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
                        {
                            path = path[(fromAlias.Length + 1)..];
                        }

                        return item.SelectToken(path) != null;
                    }
                    return args[0] != null;
                }
            case "IS_NULL": return args.Length > 0 && args[0] is null;
            case "IS_ARRAY":
                return args.Length > 0 && ResolveTokenType(func.Arguments, item, fromAlias) is JArray;
            case "IS_BOOL": return args.Length > 0 && args[0] is bool;
            case "IS_NUMBER": return args.Length > 0 && args[0] is long or double or int or float or decimal;
            case "IS_STRING": return args.Length > 0 && args[0] is string;
            case "IS_OBJECT":
                return args.Length > 0 && ResolveTokenType(func.Arguments, item, fromAlias) is JObject;
            case "IS_PRIMITIVE":
                {
                    if (args.Length == 0)
                    {
                        return false;
                    }

                    if (args[0] is null)
                    {
                        return true;
                    }

                    var tokenForPrimitive = ResolveTokenType(func.Arguments, item, fromAlias);
                    if (tokenForPrimitive is JArray or JObject)
                    {
                        return false;
                    }

                    return args[0] is string or bool or long or double;
                }
            case "IS_FINITE_NUMBER":
                {
                    if (args.Length == 0)
                    {
                        return false;
                    }

                    return args[0] switch
                    {
                        long => true,
                        int => true,
                        double d => !double.IsInfinity(d) && !double.IsNaN(d),
                        float f => !float.IsInfinity(f) && !float.IsNaN(f),
                        decimal => true,
                        _ => false,
                    };
                }
            case "IS_INTEGER":
                {
                    if (args.Length == 0)
                    {
                        return false;
                    }

                    return args[0] is long or int;
                }

            // ── String functions ──
            case "STARTSWITH":
                {
                    if (args.Length < 2)
                    {
                        return false;
                    }

                    var s = args[0]?.ToString(); var p = args[1]?.ToString();
                    if (s is null || p is null)
                    {
                        return false;
                    }

                    var ic = args.Length >= 3 && string.Equals(args[2]?.ToString(), "true", StringComparison.OrdinalIgnoreCase);
                    return s.StartsWith(p, ic ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
                }
            case "ENDSWITH":
                {
                    if (args.Length < 2)
                    {
                        return false;
                    }

                    var s = args[0]?.ToString(); var p = args[1]?.ToString();
                    if (s is null || p is null)
                    {
                        return false;
                    }

                    var ic = args.Length >= 3 && string.Equals(args[2]?.ToString(), "true", StringComparison.OrdinalIgnoreCase);
                    return s.EndsWith(p, ic ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
                }
            case "CONTAINS":
                {
                    if (args.Length < 2)
                    {
                        return false;
                    }

                    var s = args[0]?.ToString(); var p = args[1]?.ToString();
                    if (s is null || p is null)
                    {
                        return false;
                    }

                    var ic = args.Length >= 3 && string.Equals(args[2]?.ToString(), "true", StringComparison.OrdinalIgnoreCase);
                    return s.Contains(p, ic ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
                }
            case "CONCAT": return string.Concat(args.Select(a => a?.ToString() ?? ""));
            case "LENGTH": return args.Length > 0 ? (object)(long)(args[0]?.ToString()?.Length ?? 0) : null;
            case "LOWER": return args.Length > 0 ? args[0]?.ToString()?.ToLowerInvariant() : null;
            case "UPPER": return args.Length > 0 ? args[0]?.ToString()?.ToUpperInvariant() : null;
            case "TRIM": return args.Length > 0 ? args[0]?.ToString()?.Trim() : null;
            case "LTRIM": return args.Length > 0 ? args[0]?.ToString()?.TrimStart() : null;
            case "RTRIM": return args.Length > 0 ? args[0]?.ToString()?.TrimEnd() : null;
            case "REVERSE": return args.Length > 0 && args[0] is string rs ? new string(rs.Reverse().ToArray()) : null;
            case "LEFT":
                {
                    if (args.Length < 2)
                    {
                        return null;
                    }

                    var s = args[0]?.ToString(); var c = ToLong(args[1]);
                    return s != null && c.HasValue ? s[..(int)Math.Min(c.Value, s.Length)] : null;
                }
            case "RIGHT":
                {
                    if (args.Length < 2)
                    {
                        return null;
                    }

                    var s = args[0]?.ToString(); var c = ToLong(args[1]);
                    return s != null && c.HasValue ? s[Math.Max(0, s.Length - (int)c.Value)..] : null;
                }
            case "SUBSTRING":
                {
                    if (args.Length < 3)
                    {
                        return null;
                    }

                    var s = args[0]?.ToString(); var start = ToLong(args[1]); var len = ToLong(args[2]);
                    if (s is null || !start.HasValue || !len.HasValue)
                    {
                        return null;
                    }

                    var si = (int)Math.Min(start.Value, s.Length);
                    var li = (int)Math.Min(len.Value, s.Length - si);
                    return s.Substring(si, li);
                }
            case "REPLACE":
                {
                    if (args.Length < 3)
                    {
                        return null;
                    }

                    var s = args[0]?.ToString(); var find = args[1]?.ToString(); var rep = args[2]?.ToString();
                    return s != null && find != null ? s.Replace(find, rep ?? "") : null;
                }
            case "INDEX_OF":
                {
                    if (args.Length < 2)
                    {
                        return null;
                    }

                    var s = args[0]?.ToString(); var sub = args[1]?.ToString();
                    return s != null && sub != null ? (object)(long)s.IndexOf(sub, StringComparison.Ordinal) : null;
                }
            case "REGEXMATCH":
                {
                    if (args.Length < 2)
                    {
                        return false;
                    }

                    var input = args[0]?.ToString(); var pattern = args[1]?.ToString();
                    if (input is null || pattern is null)
                    {
                        return false;
                    }

                    var options = RegexOptions.None;
                    if (args.Length >= 3)
                    {
                        var modifiers = args[2]?.ToString() ?? "";
                        foreach (var ch in modifiers)
                        {
                            options |= ch switch
                            {
                                'i' => RegexOptions.IgnoreCase,
                                'm' => RegexOptions.Multiline,
                                's' => RegexOptions.Singleline,
                                'x' => RegexOptions.IgnorePatternWhitespace,
                                _ => RegexOptions.None,
                            };
                        }
                    }
                    return GetOrCreateRegex(pattern, options).IsMatch(input);
                }
            case "REPLICATE":
                {
                    if (args.Length < 2)
                    {
                        return null;
                    }

                    var s = args[0]?.ToString();
                    var count = ToLong(args[1]);
                    if (s is null || !count.HasValue || count.Value < 0)
                    {
                        return null;
                    }

                    return count.Value == 0 ? "" : string.Concat(Enumerable.Repeat(s, (int)Math.Min(count.Value, 10000)));
                }
            case "STRING_EQUALS":
            case "STRINGEQUALS":
                {
                    if (args.Length < 2)
                    {
                        return null;
                    }

                    var s1 = args[0]?.ToString();
                    var s2 = args[1]?.ToString();
                    if (s1 is null || s2 is null)
                    {
                        return null;
                    }

                    var ic = args.Length >= 3 && string.Equals(args[2]?.ToString(), "true", StringComparison.OrdinalIgnoreCase);
                    return string.Equals(s1, s2, ic ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
                }
            case "STRINGTOARRAY":
                {
                    if (args.Length == 0)
                    {
                        return null;
                    }

                    var s = args[0]?.ToString();
                    if (s is null)
                    {
                        return null;
                    }

                    try
                    {
                        var token = JToken.Parse(s);
                        return token is JArray ? token : UndefinedValue.Instance;
                    }
                    catch
                    {
                        return UndefinedValue.Instance;
                    }
                }
            case "STRINGTOBOOLEAN":
                {
                    if (args.Length == 0)
                    {
                        return null;
                    }

                    var s = args[0]?.ToString()?.Trim();
                    return s switch
                    {
                        "true" => true,
                        "false" => (object)false,
                        _ => UndefinedValue.Instance,
                    };
                }
            case "STRINGTONULL":
                {
                    if (args.Length == 0)
                    {
                        return null;
                    }

                    return args[0]?.ToString()?.Trim() == "null" ? null : UndefinedValue.Instance;
                }
            case "STRINGTONUMBER":
                {
                    if (args.Length == 0)
                    {
                        return null;
                    }

                    var s = args[0]?.ToString()?.Trim();
                    if (s is null)
                    {
                        return null;
                    }

                    if (long.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var longVal))
                    {
                        return longVal;
                    }

                    if (double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var doubleVal))
                    {
                        return doubleVal;
                    }

                    return UndefinedValue.Instance;
                }
            case "STRINGTOOBJECT":
                {
                    if (args.Length == 0)
                    {
                        return null;
                    }

                    var s = args[0]?.ToString();
                    if (s is null)
                    {
                        return null;
                    }

                    try
                    {
                        var token = JToken.Parse(s);
                        return token is JObject ? token : UndefinedValue.Instance;
                    }
                    catch
                    {
                        return UndefinedValue.Instance;
                    }
                }
            case "TOSTRING" or "ToString": return args.Length > 0 ? args[0]?.ToString() : null;
            case "TONUMBER" or "ToNumber":
                {
                    if (args.Length == 0)
                    {
                        return null;
                    }

                    if (args[0] is long or double)
                    {
                        return args[0];
                    }

                    if (args[0] != null && double.TryParse(args[0].ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var n))
                    {
                        return n;
                    }

                    return null;
                }
            case "TOBOOLEAN" or "ToBoolean":
                {
                    if (args.Length == 0)
                    {
                        return null;
                    }

                    if (args[0] is bool)
                    {
                        return args[0];
                    }

                    if (args[0] != null && bool.TryParse(args[0].ToString(), out var b))
                    {
                        return b;
                    }

                    return null;
                }

            // ── Array functions ──
            case "ARRAY_CONTAINS":
                {
                    if (func.Arguments.Length < 2)
                    {
                        return false;
                    }

                    if (func.Arguments[0] is IdentifierExpression ident)
                    {
                        var arrayPath = ident.Name;
                        if (arrayPath.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
                        {
                            arrayPath = arrayPath[(fromAlias.Length + 1)..];
                        }

                        var arrayToken = item.SelectToken(arrayPath);
                        if (arrayToken is not JArray jArray)
                        {
                            return false;
                        }

                        var partial = args.Length >= 3 && string.Equals(args[2]?.ToString(), "true", StringComparison.OrdinalIgnoreCase);
                        var searchValue = args[1];
                        if (searchValue is JObject searchObj)
                        {
                            return ArrayContainsMatchJObject(jArray, searchObj, partial);
                        }

                        var searchStr = searchValue?.ToString();
                        if (searchStr is null)
                        {
                            return false;
                        }

                        return ArrayContainsMatch(jArray, searchStr, partial);
                    }
                    return false;
                }
            case "ARRAY_LENGTH":
                {
                    if (func.Arguments.Length < 1)
                    {
                        return null;
                    }

                    if (func.Arguments[0] is IdentifierExpression ident)
                    {
                        var path = ident.Name;
                        if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
                        {
                            path = path[(fromAlias.Length + 1)..];
                        }

                        var token = item.SelectToken(path);
                        return token is JArray arr ? (object)(long)arr.Count : null;
                    }
                    return null;
                }
            case "ARRAY_SLICE":
                {
                    if (func.Arguments.Length < 2)
                    {
                        return null;
                    }

                    if (func.Arguments[0] is IdentifierExpression ident)
                    {
                        var path = ident.Name;
                        if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
                        {
                            path = path[(fromAlias.Length + 1)..];
                        }

                        var token = item.SelectToken(path);
                        if (token is not JArray arr)
                        {
                            return null;
                        }

                        var start = (int)(ToLong(args[1]) ?? 0);
                        if (start < 0)
                        {
                            start = Math.Max(0, arr.Count + start);
                        }

                        var length = args.Length >= 3 ? (int)(ToLong(args[2]) ?? arr.Count) : arr.Count;
                        return new JArray(arr.Skip(start).Take(length));
                    }
                    return null;
                }
            case "ARRAY_CONCAT":
                {
                    var result = new JArray();
                    foreach (var argExpr in func.Arguments)
                    {
                        JArray ja = null;
                        if (argExpr is IdentifierExpression ident)
                        {
                            var path = ident.Name;
                            if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
                            {
                                path = path[(fromAlias.Length + 1)..];
                            }

                            ja = item.SelectToken(path) as JArray;
                        }
                        else
                        {
                            var evaluated = EvaluateSqlExpression(argExpr, item, fromAlias, parameters);
                            ja = evaluated as JArray;
                        }

                        if (ja is not null)
                        {
                            foreach (var el in ja)
                            {
                                result.Add(el.DeepClone());
                            }
                        }
                    }
                    return result;
                }

            // ── Math functions ──
            case "ABS": return args.Length > 0 ? MathOp(args[0], Math.Abs) : null;
            case "CEILING": return args.Length > 0 ? MathOp(args[0], Math.Ceiling) : null;
            case "FLOOR": return args.Length > 0 ? MathOp(args[0], Math.Floor) : null;
            case "ROUND": return args.Length > 0 ? MathOp(args[0], Math.Round) : null;
            case "SQRT": return args.Length > 0 ? MathOp(args[0], Math.Sqrt) : null;
            case "SQUARE": return args.Length > 0 ? MathOp(args[0], v => v * v) : null;
            case "POWER": return args.Length >= 2 ? ArithmeticOp(args[0], args[1], Math.Pow) : null;
            case "EXP": return args.Length > 0 ? MathOp(args[0], Math.Exp) : null;
            case "LOG": return args.Length > 0 ? MathOp(args[0], Math.Log) : null;
            case "LOG10": return args.Length > 0 ? MathOp(args[0], Math.Log10) : null;
            case "SIGN": return args.Length > 0 ? MathOp(args[0], v => Math.Sign(v)) : null;
            case "TRUNC": return args.Length > 0 ? MathOp(args[0], Math.Truncate) : null;
            case "PI": return Math.PI;
            case "SIN": return args.Length > 0 ? MathOp(args[0], Math.Sin) : null;
            case "COS": return args.Length > 0 ? MathOp(args[0], Math.Cos) : null;
            case "TAN": return args.Length > 0 ? MathOp(args[0], Math.Tan) : null;
            case "COT": return args.Length > 0 ? MathOp(args[0], v => 1.0 / Math.Tan(v)) : null;
            case "ASIN": return args.Length > 0 ? MathOp(args[0], Math.Asin) : null;
            case "ACOS": return args.Length > 0 ? MathOp(args[0], Math.Acos) : null;
            case "ATAN": return args.Length > 0 ? MathOp(args[0], Math.Atan) : null;
            case "ATN2": return args.Length >= 2 ? ArithmeticOp(args[0], args[1], Math.Atan2) : null;
            case "DEGREES": return args.Length > 0 ? MathOp(args[0], v => v * (180.0 / Math.PI)) : null;
            case "RADIANS": return args.Length > 0 ? MathOp(args[0], v => v * (Math.PI / 180.0)) : null;
            case "RAND": return new Random().NextDouble();

            // ── Integer math functions ──
            case "NUMBERBIN":
                {
                    if (args.Length < 2)
                    {
                        return null;
                    }

                    if (double.TryParse(args[0]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var val) &&
                        double.TryParse(args[1]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var binSize) &&
                        binSize != 0)
                    {
                        return Math.Floor(val / binSize) * binSize;
                    }

                    return null;
                }
            case "INTADD": return args.Length >= 2 ? BitwiseOp(args[0], args[1], (a, b) => a + b) : null;
            case "INTSUB": return args.Length >= 2 ? BitwiseOp(args[0], args[1], (a, b) => a - b) : null;
            case "INTMUL": return args.Length >= 2 ? BitwiseOp(args[0], args[1], (a, b) => a * b) : null;
            case "INTDIV":
                {
                    if (args.Length < 2)
                    {
                        return null;
                    }

                    var dividend = ToLong(args[0]);
                    var divisor = ToLong(args[1]);
                    if (!dividend.HasValue || !divisor.HasValue || divisor.Value == 0)
                    {
                        return null;
                    }

                    return dividend.Value / divisor.Value;
                }
            case "INTMOD":
                {
                    if (args.Length < 2)
                    {
                        return null;
                    }

                    var dividend = ToLong(args[0]);
                    var divisor = ToLong(args[1]);
                    if (!dividend.HasValue || !divisor.HasValue || divisor.Value == 0)
                    {
                        return null;
                    }

                    return dividend.Value % divisor.Value;
                }
            case "INTBITAND": return args.Length >= 2 ? BitwiseOp(args[0], args[1], (a, b) => a & b) : null;
            case "INTBITOR": return args.Length >= 2 ? BitwiseOp(args[0], args[1], (a, b) => a | b) : null;
            case "INTBITXOR": return args.Length >= 2 ? BitwiseOp(args[0], args[1], (a, b) => a ^ b) : null;
            case "INTBITNOT":
                {
                    if (args.Length == 0)
                    {
                        return null;
                    }

                    var value = ToLong(args[0]);
                    return value.HasValue ? ~value.Value : null;
                }
            case "INTBITLEFTSHIFT": return args.Length >= 2 ? BitwiseOp(args[0], args[1], (a, b) => a << (int)b) : null;
            case "INTBITRIGHTSHIFT": return args.Length >= 2 ? BitwiseOp(args[0], args[1], (a, b) => a >> (int)b) : null;

            // ── Aggregates (passthrough for non-GROUP-BY contexts) ──
            // When there is no GROUP BY, each document is emitted individually and the
            // SDK accumulates partial results client-side (cross-partition fan-out model).
            // COUNT always emits 1 per document (each document counts as one row).
            // SUM/MIN/MAX emit the field value so the SDK can aggregate across partitions.
            // AVG is handled by the SDK via sum+count, so SUM and COUNT passthroughs suffice.
            case "COUNT":
                return 1L;
            case "SUM":
            case "AVG":
            case "MIN":
            case "MAX":
                return args.Length > 0 ? args[0] : null;

            // ── Conditional functions ──
            case "IIF":
                {
                    if (args.Length < 3)
                    {
                        return null;
                    }

                    return IsTruthy(args[0]) ? args[1] : args[2];
                }

            // ── Extended array functions ──
            case "ARRAY_CONTAINS_ANY":
                {
                    if (func.Arguments.Length < 2)
                    {
                        return false;
                    }

                    var sourceArray = ResolveJArray(func.Arguments[0], item, fromAlias, parameters);
                    var searchArray = ResolveJArray(func.Arguments[1], item, fromAlias, parameters);
                    if (sourceArray is null || searchArray is null || searchArray.Count == 0)
                    {
                        return false;
                    }

                    var sourceValues = new HashSet<string>(sourceArray.Select(t => t.ToString()));
                    return searchArray.Any(t => sourceValues.Contains(t.ToString()));
                }
            case "ARRAY_CONTAINS_ALL":
                {
                    if (func.Arguments.Length < 2)
                    {
                        return false;
                    }

                    var sourceArray = ResolveJArray(func.Arguments[0], item, fromAlias, parameters);
                    var searchArray = ResolveJArray(func.Arguments[1], item, fromAlias, parameters);
                    if (sourceArray is null)
                    {
                        return false;
                    }

                    if (searchArray is null || searchArray.Count == 0)
                    {
                        return true;
                    }

                    var sourceValues = new HashSet<string>(sourceArray.Select(t => t.ToString()));
                    return searchArray.All(t => sourceValues.Contains(t.ToString()));
                }
            case "SETINTERSECT":
                {
                    if (func.Arguments.Length < 2)
                    {
                        return new JArray();
                    }

                    var arr1 = ResolveJArray(func.Arguments[0], item, fromAlias, parameters);
                    var arr2 = ResolveJArray(func.Arguments[1], item, fromAlias, parameters);
                    if (arr1 is null || arr2 is null)
                    {
                        return new JArray();
                    }

                    var set2 = new HashSet<string>(arr2.Select(t => t.ToString()));
                    var result = new JArray();
                    var seen = new HashSet<string>();
                    foreach (var element in arr1)
                    {
                        var val = element.ToString();
                        if (set2.Contains(val) && seen.Add(val))
                        {
                            result.Add(element.DeepClone());
                        }
                    }
                    return result;
                }
            case "SETUNION":
                {
                    if (func.Arguments.Length < 2)
                    {
                        return new JArray();
                    }

                    var arr1 = ResolveJArray(func.Arguments[0], item, fromAlias, parameters);
                    var arr2 = ResolveJArray(func.Arguments[1], item, fromAlias, parameters);
                    var result = new JArray();
                    var seen = new HashSet<string>();
                    if (arr1 is not null)
                    {
                        foreach (var element in arr1)
                        {
                            if (seen.Add(element.ToString()))
                            {
                                result.Add(element.DeepClone());
                            }
                        }
                    }
                    if (arr2 is not null)
                    {
                        foreach (var element in arr2)
                        {
                            if (seen.Add(element.ToString()))
                            {
                                result.Add(element.DeepClone());
                            }
                        }
                    }
                    return result;
                }

            // ── Array/Object utility functions ──
            case "CHOOSE":
                {
                    if (args.Length < 2) return null;
                    var idx = ToLong(args[0]);
                    if (!idx.HasValue || idx.Value < 1 || idx.Value >= args.Length) return null;
                    return args[(int)idx.Value];
                }
            case "OBJECTTOARRAY":
                {
                    if (args.Length < 1) return null;
                    JObject obj;
                    if (args[0] is JObject jo) obj = jo;
                    else if (args[0] is string s) { try { obj = JObject.Parse(s); } catch { return null; } }
                    else return null;
                    var result = new JArray();
                    foreach (var prop in obj.Properties())
                    {
                        result.Add(new JObject { ["k"] = prop.Name, ["v"] = prop.Value.DeepClone() });
                    }
                    return result;
                }
            case "ARRAYTOOBJECT":
                {
                    if (args.Length < 1) return UndefinedValue.Instance;
                    JArray arr;
                    if (args[0] is JArray ja) arr = ja;
                    else if (args[0] is string s) { try { arr = JArray.Parse(s); } catch { return UndefinedValue.Instance; } }
                    else return UndefinedValue.Instance;
                    var result = new JObject();
                    foreach (var element in arr)
                    {
                        if (element is JObject kvObj && kvObj["k"] != null && kvObj["v"] != null)
                        {
                            result[kvObj["k"]!.Value<string>()] = kvObj["v"]!.DeepClone();
                        }
                        else
                        {
                            return UndefinedValue.Instance;
                        }
                    }
                    return result;
                }

            // ── String utility functions ──
            case "STRINGJOIN":
                {
                    if (args.Length < 2) return null;
                    var separator = args[0]?.ToString();
                    if (separator is null) return null;
                    JArray joinArr;
                    if (args[1] is JArray ja) joinArr = ja;
                    else if (args[1] is string s) { try { joinArr = JArray.Parse(s); } catch { return null; } }
                    else return null;
                    return string.Join(separator, joinArr.Select(t => t.Value<string>()));
                }
            case "STRINGSPLIT":
                {
                    if (args.Length < 2) return null;
                    var input = args[0]?.ToString();
                    var delimiter = args[1]?.ToString();
                    if (input is null || delimiter is null) return null;
                    var parts = input.Split(delimiter);
                    return new JArray(parts.Select(p => (JToken)p));
                }

            // ── Item functions ──
            case "DOCUMENTID":
                {
                    // DOCUMENTID returns the _rid system property of the current document.
                    // The emulator synthesises from id since it doesn't generate _rid.
                    return item["_rid"]?.Value<string>() ?? item["id"]?.Value<string>();
                }

            // ── Full-text search functions (approximate) ──
            // These use case-insensitive substring matching instead of real NLP tokenization.
            case "FULLTEXTCONTAINS":
                {
                    if (args.Length < 2) return false;
                    var text = args[0]?.ToString();
                    var term = args[1]?.ToString();
                    if (text is null || term is null) return false;
                    return text.Contains(term, StringComparison.OrdinalIgnoreCase);
                }
            case "FULLTEXTCONTAINSALL":
                {
                    if (args.Length < 2) return false;
                    var text = args[0]?.ToString();
                    if (text is null) return false;
                    for (var i = 1; i < args.Length; i++)
                    {
                        var term = args[i]?.ToString();
                        if (term is null || !text.Contains(term, StringComparison.OrdinalIgnoreCase))
                            return false;
                    }
                    return true;
                }
            case "FULLTEXTCONTAINSANY":
                {
                    if (args.Length < 2) return false;
                    var text = args[0]?.ToString();
                    if (text is null) return false;
                    for (var i = 1; i < args.Length; i++)
                    {
                        var term = args[i]?.ToString();
                        if (term is not null && text.Contains(term, StringComparison.OrdinalIgnoreCase))
                            return true;
                    }
                    return false;
                }
            case "FULLTEXTSCORE":
                {
                    // Naive term-frequency scoring: count occurrences of each search term
                    // in the field text. Returns the total count as a double.
                    // Real Cosmos DB uses BM25 with IDF and length normalization.
                    if (args.Length < 2) return 0.0;
                    var text = args[0]?.ToString();
                    if (text is null) return 0.0;

                    var score = 0.0;
                    // arg[1] may be a JArray (from [...] literal) or individual terms
                    if (args[1] is JArray searchTerms)
                    {
                        foreach (var termToken in searchTerms)
                        {
                            var term = termToken.Value<string>();
                            if (term is not null)
                                score += CountOccurrences(text, term);
                        }
                    }
                    else
                    {
                        for (var i = 1; i < args.Length; i++)
                        {
                            var term = args[i]?.ToString();
                            if (term is not null)
                                score += CountOccurrences(text, term);
                        }
                    }
                    return score;
                }

            // ── Spatial functions ──
            case "ST_DISTANCE":
                return args.Length >= 2 ? StDistance(args[0], args[1]) : null;
            case "ST_WITHIN":
                return args.Length >= 2 ? (object)StWithin(args[0], args[1]) : null;
            case "ST_INTERSECTS":
                return args.Length >= 2 ? (object)StIntersects(args[0], args[1]) : null;
            case "ST_ISVALID":
                return args.Length >= 1 ? (object)StIsValid(args[0]) : false;
            case "ST_ISVALIDDETAILED":
                return args.Length >= 1 ? StIsValidDetailed(args[0]) : null;
            case "ST_AREA":
                return args.Length >= 1 ? StArea(args[0]) : null;

            // ── Vector functions ──
            case "VECTORDISTANCE":
                return args.Length >= 2 ? VectorDistanceFunc(args) : null;

            // ── Date/time functions ──
            case "GETCURRENTDATETIME": return DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ");
            case "GETCURRENTTIMESTAMP": return (long)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            case "GETCURRENTDATETIMESTATIC": return parameters.TryGetValue("__staticDateTime", out var sdt) ? sdt : DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ");
            case "GETCURRENTTICKSSTATIC": return parameters.TryGetValue("__staticTicks", out var stk) ? stk : (object)DateTime.UtcNow.Ticks;
            case "GETCURRENTTIMESTAMPSTATIC": return parameters.TryGetValue("__staticTimestamp", out var sts) ? sts : (object)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            case "DATETIMEADD":
                {
                    if (args.Length < 3)
                    {
                        return null;
                    }

                    var part = args[0]?.ToString()?.ToLowerInvariant();
                    var number = ToLong(args[1]);
                    var dateTime = args[2]?.ToString();
                    if (part is null || !number.HasValue || dateTime is null)
                    {
                        return null;
                    }

                    if (!DateTime.TryParse(dateTime, null, System.Globalization.DateTimeStyles.RoundtripKind, out var dt))
                    {
                        return null;
                    }

                    var n = (int)number.Value;
                    dt = part switch
                    {
                        "year" or "yyyy" or "yy" => dt.AddYears(n),
                        "month" or "mm" or "m" => dt.AddMonths(n),
                        "day" or "dd" or "d" => dt.AddDays(n),
                        "hour" or "hh" => dt.AddHours(n),
                        "minute" or "mi" or "n" => dt.AddMinutes(n),
                        "second" or "ss" or "s" => dt.AddSeconds(n),
                        "millisecond" or "ms" => dt.AddMilliseconds(n),
                        _ => dt,
                    };
                    return dt.ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ");
                }
            case "DATETIMEPART":
                {
                    if (args.Length < 2)
                    {
                        return null;
                    }

                    var part = args[0]?.ToString()?.ToLowerInvariant();
                    var dateTime = args[1]?.ToString();
                    if (part is null || dateTime is null)
                    {
                        return null;
                    }

                    if (!DateTime.TryParse(dateTime, null, System.Globalization.DateTimeStyles.RoundtripKind, out var dt))
                    {
                        return null;
                    }

                    return part switch
                    {
                        "year" or "yyyy" or "yy" => (object)(long)dt.Year,
                        "month" or "mm" or "m" => (long)dt.Month,
                        "day" or "dd" or "d" => (long)dt.Day,
                        "hour" or "hh" => (long)dt.Hour,
                        "minute" or "mi" or "n" => (long)dt.Minute,
                        "second" or "ss" or "s" => (long)dt.Second,
                        "millisecond" or "ms" => (long)dt.Millisecond,
                        _ => null,
                    };
                }
            case "DATETIMEDIFF":
                {
                    if (args.Length < 3) return null;
                    var part = args[0]?.ToString()?.ToLowerInvariant();
                    var startStr = args[1]?.ToString();
                    var endStr = args[2]?.ToString();
                    if (part is null || startStr is null || endStr is null) return null;
                    if (!DateTime.TryParse(startStr, null, System.Globalization.DateTimeStyles.RoundtripKind, out var dtStart)) return null;
                    if (!DateTime.TryParse(endStr, null, System.Globalization.DateTimeStyles.RoundtripKind, out var dtEnd)) return null;

                    return part switch
                    {
                        "year" or "yyyy" or "yy" => (object)(long)(dtEnd.Year - dtStart.Year),
                        "month" or "mm" or "m" => (long)((dtEnd.Year - dtStart.Year) * 12 + dtEnd.Month - dtStart.Month),
                        "day" or "dd" or "d" => (long)(dtEnd.Date - dtStart.Date).TotalDays,
                        "hour" or "hh" => (long)(dtEnd - dtStart).TotalHours,
                        "minute" or "mi" or "n" => (long)(dtEnd - dtStart).TotalMinutes,
                        "second" or "ss" or "s" => (long)(dtEnd - dtStart).TotalSeconds,
                        "millisecond" or "ms" => (long)(dtEnd - dtStart).TotalMilliseconds,
                        _ => null,
                    };
                }
            case "DATETIMEFROMPARTS":
                {
                    if (args.Length < 7) return null;
                    var y = ToLong(args[0]);
                    var mo = ToLong(args[1]);
                    var d = ToLong(args[2]);
                    var h = ToLong(args[3]);
                    var mi = ToLong(args[4]);
                    var s = ToLong(args[5]);
                    var ms = ToLong(args[6]);
                    if (!y.HasValue || !mo.HasValue || !d.HasValue || !h.HasValue || !mi.HasValue || !s.HasValue || !ms.HasValue) return null;
                    var dt = new DateTime((int)y.Value, (int)mo.Value, (int)d.Value,
                        (int)h.Value, (int)mi.Value, (int)s.Value, (int)ms.Value, DateTimeKind.Utc);
                    return dt.ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ");
                }
            case "DATETIMEBIN":
                {
                    if (args.Length < 3) return null;
                    var dtStr = args[0]?.ToString();
                    var part = args[1]?.ToString()?.ToLowerInvariant();
                    var binSize = ToLong(args[2]);
                    if (dtStr is null || part is null || !binSize.HasValue) return null;
                    if (!DateTime.TryParse(dtStr, null, System.Globalization.DateTimeStyles.RoundtripKind, out var dt)) return null;

                    var origin = args.Length >= 4 && args[3] is string originStr
                        && DateTime.TryParse(originStr, null, System.Globalization.DateTimeStyles.RoundtripKind, out var o)
                        ? o : new DateTime(2001, 1, 1, 0, 0, 0, DateTimeKind.Utc);

                    var bs = (int)binSize.Value;
                    if (part is "year" or "yyyy" or "yy")
                    {
                        var yearBin = (int)(Math.Floor((double)(dt.Year - origin.Year) / bs) * bs);
                        dt = new DateTime(origin.Year + yearBin, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                    }
                    else if (part is "month" or "mm")
                    {
                        var totalMonths = (dt.Year - origin.Year) * 12 + (dt.Month - origin.Month);
                        var binned = (int)(Math.Floor((double)totalMonths / bs) * bs);
                        dt = new DateTime(origin.Year, origin.Month, 1, 0, 0, 0, DateTimeKind.Utc).AddMonths(binned);
                    }
                    else
                    {
                        dt = part switch
                        {
                            "day" or "dd" or "d" =>
                                origin.AddDays(Math.Floor((dt - origin).TotalDays / bs) * bs),
                            "hour" or "hh" =>
                                origin.AddHours(Math.Floor((dt - origin).TotalHours / bs) * bs),
                            "minute" or "mi" or "n" =>
                                origin.AddMinutes(Math.Floor((dt - origin).TotalMinutes / bs) * bs),
                            "second" or "ss" or "s" =>
                                origin.AddSeconds(Math.Floor((dt - origin).TotalSeconds / bs) * bs),
                            "millisecond" or "ms" =>
                                origin.AddMilliseconds(Math.Floor((dt - origin).TotalMilliseconds / bs) * bs),
                            _ => dt,
                        };
                    }
                    return dt.ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ");
                }
            case "DATETIMETOTICKS":
                {
                    if (args.Length < 1) return null;
                    var dtStr = args[0]?.ToString();
                    if (dtStr is null) return null;
                    if (!DateTime.TryParse(dtStr, null, System.Globalization.DateTimeStyles.RoundtripKind, out var dt)) return null;
                    return (object)dt.ToUniversalTime().Ticks;
                }
            case "TICKSTODATETIME":
                {
                    if (args.Length < 1) return null;
                    var ticks = ToLong(args[0]);
                    if (!ticks.HasValue) return null;
                    var dt = new DateTime(ticks.Value, DateTimeKind.Utc);
                    return dt.ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ");
                }
            case "DATETIMETOTIMESTAMP":
                {
                    if (args.Length < 1) return null;
                    var dtStr = args[0]?.ToString();
                    if (dtStr is null) return null;
                    if (!DateTime.TryParse(dtStr, null, System.Globalization.DateTimeStyles.RoundtripKind, out var dt)) return null;
                    return (object)new DateTimeOffset(dt.ToUniversalTime()).ToUnixTimeMilliseconds();
                }
            case "TIMESTAMPTODATETIME":
                {
                    if (args.Length < 1) return null;
                    var ms = ToLong(args[0]);
                    if (!ms.HasValue) return null;
                    var dt = DateTimeOffset.FromUnixTimeMilliseconds(ms.Value).UtcDateTime;
                    return dt.ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ");
                }
            case "GETCURRENTTICKS": return (object)DateTime.UtcNow.Ticks;

            // ── COALESCE function ──
            case "COALESCE":
                {
                    foreach (var arg in args)
                    {
                        if (arg is not null)
                        {
                            return arg;
                        }
                    }

                    return null;
                }

            default:
                if (func.FunctionName.StartsWith("UDF.", StringComparison.OrdinalIgnoreCase))
                {
                    if (parameters.TryGetValue(UdfRegistryKey, out var registry) &&
                        registry is Dictionary<string, Func<object[], object>> udfs &&
                        udfs.TryGetValue(func.FunctionName, out var udfImpl))
                    {
                        return udfImpl(args);
                    }

                    throw new NotSupportedException(
                        $"Unregistered user-defined function: {func.FunctionName}. " +
                        "Call RegisterUdf() on the InMemoryContainer to register it before querying.");
                }

                throw new NotSupportedException($"Unsupported Cosmos SQL function: {func.FunctionName}");
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Utility helpers
    // ═══════════════════════════════════════════════════════════════════════════

    private static bool IsTruthy(object value) => value switch
    {
        null => false,
        UndefinedValue => false,
        bool b => b,
        long l => l != 0,
        double d => d != 0,
        string s => s.Length > 0,
        _ => true
    };

    private static int CountOccurrences(string text, string term)
    {
        var count = 0;
        var idx = 0;
        while ((idx = text.IndexOf(term, idx, StringComparison.OrdinalIgnoreCase)) >= 0)
        {
            count++;
            idx += term.Length;
        }
        return count;
    }

    private static JObject EvaluateObjectLiteral(
        ObjectLiteralExpression obj, JObject item, string fromAlias, IDictionary<string, object> parameters)
    {
        var result = new JObject();
        foreach (var prop in obj.Properties)
        {
            var value = EvaluateSqlExpression(prop.Value, item, fromAlias, parameters);
            if (value is not UndefinedValue)
                result[prop.Key] = value is not null ? JToken.FromObject(value) : JValue.CreateNull();
        }
        return result;
    }

    private static JArray EvaluateArrayLiteral(
        ArrayLiteralExpression arr, JObject item, string fromAlias, IDictionary<string, object> parameters)
    {
        var result = new JArray();
        foreach (var element in arr.Elements)
        {
            var value = EvaluateSqlExpression(element, item, fromAlias, parameters);
            result.Add(value is not null and not UndefinedValue ? JToken.FromObject(value) : JValue.CreateNull());
        }
        return result;
    }

    private static object EvaluateSubquery(
        CosmosSqlQuery subquery, JObject item, string fromAlias, IDictionary<string, object> parameters)
    {
        var results = ExecuteSubqueryAgainstItem(subquery, item, fromAlias, parameters);
        if (subquery.IsValueSelect && results.Count > 0)
        {
            return JsonParseHelpers.ParseJsonToken(results[0]).ToObject<object>();
        }

        return results.Count > 0 ? JsonParseHelpers.ParseJsonToken(results[0]).ToObject<object>() : null;
    }

    private static object EvaluateArraySubquery(
        CosmosSqlQuery subquery, JObject item, string fromAlias, IDictionary<string, object> parameters)
    {
        var results = ExecuteSubqueryAgainstItem(subquery, item, fromAlias, parameters);
        var jArray = new JArray();
        foreach (var resultJson in results)
        {
            jArray.Add(JsonParseHelpers.ParseJsonToken(resultJson));
        }
        return jArray;
    }

    private static List<string> ExecuteSubqueryAgainstItem(
        CosmosSqlQuery subquery, JObject item, string fromAlias, IDictionary<string, object> parameters)
    {
        // Determine the data to iterate over
        List<JObject> sourceItems;

        if (subquery.FromSource is not null)
        {
            // FROM alias IN path — expand the array at path, aliasing each element
            var sourcePath = subquery.FromSource;
            if (sourcePath.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
            {
                sourcePath = sourcePath[(fromAlias.Length + 1)..];
            }

            var arrayToken = item.SelectToken(sourcePath);
            if (arrayToken is not JArray sourceArray)
            {
                return [];
            }

            sourceItems = [];
            foreach (var element in sourceArray)
            {
                var combined = new JObject(item.Properties())
                {
                    [subquery.FromAlias] = element
                };
                sourceItems.Add(combined);
            }
        }
        else if (subquery.Join is not null)
        {
            // Correlated subquery with JOIN — expand the join array from the parent item
            var arrayPath = subquery.Join.ArrayField;
            var sourceAlias = subquery.Join.SourceAlias;
            JToken arrayToken;

            if (sourceAlias.Equals(fromAlias, StringComparison.OrdinalIgnoreCase) ||
                sourceAlias.Equals(subquery.FromAlias, StringComparison.OrdinalIgnoreCase))
            {
                arrayToken = item.SelectToken(arrayPath);
            }
            else
            {
                arrayToken = item.SelectToken($"{sourceAlias}.{arrayPath}");
            }

            if (arrayToken is not JArray jArray)
            {
                return [];
            }

            sourceItems = [];
            foreach (var element in jArray)
            {
                var combined = new JObject(item.Properties())
                {
                    [subquery.Join.Alias] = element
                };
                sourceItems.Add(combined);
            }
        }
        else
        {
            // Non-correlated — treat the current item as the sole source
            sourceItems = [item];
        }

        // Apply WHERE filter
        if (subquery.Where is not null)
        {
            sourceItems = sourceItems.Where(sourceItem =>
                EvaluateWhereExpression(subquery.Where, sourceItem, subquery.FromAlias, parameters, subquery.Join)
            ).ToList();
        }

        // Apply SELECT projection
        var results = new List<string>();
        foreach (var sourceItem in sourceItems)
        {
            if (subquery.IsSelectAll)
            {
                results.Add(sourceItem.ToString(Formatting.None));
            }
            else
            {
                var projected = new JObject();
                foreach (var field in subquery.SelectFields)
                {
                    var outputName = field.Alias ?? field.Expression.Split('.').Last();
                    if (field.SqlExpr is not null and not IdentifierExpression)
                    {
                        var value = EvaluateSqlExpression(field.SqlExpr, sourceItem, subquery.FromAlias, parameters);
                        if (value is not UndefinedValue)
                            projected[outputName] = value is not null ? JToken.FromObject(value) : JValue.CreateNull();
                    }
                    else
                    {
                        var path = field.Expression;
                        if (string.Equals(path, subquery.FromAlias, StringComparison.OrdinalIgnoreCase))
                        {
                            // For range variables (FROM t IN c.tags), use the aliased element
                            var aliasToken = sourceItem[subquery.FromAlias];
                            projected[outputName] = aliasToken is not null
                                ? aliasToken.DeepClone()
                                : sourceItem.DeepClone();
                            continue;
                        }
                        if (path.StartsWith(subquery.FromAlias + ".", StringComparison.OrdinalIgnoreCase))
                        {
                            path = path[(subquery.FromAlias.Length + 1)..];
                        }
                        var token = sourceItem.SelectToken(path);
                        outputName = field.Alias ?? path.Split('.').Last();
                        projected[outputName] = token?.DeepClone();
                    }
                }

                if (subquery.IsValueSelect)
                {
                    var first = projected.Properties().FirstOrDefault();
                    if (first?.Value is not null)
                    {
                        results.Add(first.Value.Type == JTokenType.String
                            ? JsonConvert.SerializeObject(first.Value.Value<string>())
                            : first.Value.ToString(Formatting.None));
                    }
                }
                else
                {
                    results.Add(projected.ToString(Formatting.None));
                }
            }
        }

        // Apply DISTINCT
        if (subquery.IsDistinct)
        {
            results = results.Distinct().ToList();
        }

        // Apply TOP
        if (subquery.TopCount.HasValue)
        {
            results = results.Take(subquery.TopCount.Value).ToList();
        }

        // Apply ORDER BY
        if (subquery.OrderByFields is { Length: > 0 })
        {
            IOrderedEnumerable<string> ordered = null;
            foreach (var field in subquery.OrderByFields)
            {
                var fieldPath = field.Field;
                if (fieldPath.StartsWith(subquery.FromAlias + ".", StringComparison.OrdinalIgnoreCase))
                    fieldPath = fieldPath[(subquery.FromAlias.Length + 1)..];

                object KeySelector(string json)
                {
                    var token = JsonParseHelpers.ParseJsonToken(json);
                    // For SELECT VALUE results, the token is the scalar value itself
                    if (token is not JObject obj)
                    {
                        // If the ORDER BY field matches the FROM alias, use the raw value
                        return token.Type switch
                        {
                            JTokenType.Integer => token.Value<long>(),
                            JTokenType.Float => token.Value<double>(),
                            _ => (object)token.ToString()
                        };
                    }
                    var selected = obj.SelectToken(fieldPath);
                    if (selected is null) return null;
                    return selected.Type switch
                    {
                        JTokenType.Integer => selected.Value<long>(),
                        JTokenType.Float => selected.Value<double>(),
                        _ => (object)selected.ToString()
                    };
                }

                var asc = field.Ascending;
                var comparer = Comparer<object>.Create((l, r) =>
                {
                    var result = CompareValues(l, r);
                    return asc ? result : -result;
                });
                ordered = ordered == null ? results.OrderBy(KeySelector, comparer) : ordered.ThenBy(KeySelector, comparer);
            }
            results = ordered?.ToList() ?? results;
        }

        // Apply OFFSET
        if (subquery.Offset.HasValue)
        {
            results = results.Skip(subquery.Offset.Value).ToList();
        }

        // Apply LIMIT
        if (subquery.Limit.HasValue)
        {
            results = results.Take(subquery.Limit.Value).ToList();
        }

        return results;
    }

    private static object ArithmeticOp(object left, object right, Func<double, double, double> op)
    {
        if (left is null || right is null)
        {
            return null;
        }

        if (double.TryParse(left.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var l) &&
            double.TryParse(right.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var r))
        {
            var result = op(l, r);
            if (left is long or int && right is long or int && result == Math.Floor(result) && !double.IsInfinity(result))
            {
                return (long)result;
            }

            return result;
        }

        return null;
    }

    private static object BitwiseOp(object left, object right, Func<long, long, long> op)
    {
        if (left is null || right is null)
        {
            return null;
        }

        if (long.TryParse(left.ToString(), out var l) && long.TryParse(right.ToString(), out var r))
        {
            return op(l, r);
        }

        return null;
    }

    private static object MathOp(object value, Func<double, double> op)
    {
        if (value is null)
        {
            return null;
        }

        if (double.TryParse(value.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var n))
        {
            return op(n);
        }

        return null;
    }

    private static long? ToLong(object value) => value switch
    {
        long l => l,
        double d => (long)d,
        int i => i,
        _ when value != null && long.TryParse(value.ToString(), out var p) => p,
        _ => null
    };

    private static JToken ResolveTokenType(SqlExpression[] arguments, JObject item, string fromAlias)
    {
        if (arguments.Length < 1)
        {
            return null;
        }

        if (arguments[0] is IdentifierExpression ident)
        {
            var path = ident.Name;
            if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
            {
                path = path[(fromAlias.Length + 1)..];
            }

            return item.SelectToken(path);
        }
        return null;
    }

    private static JArray ResolveJArray(
        SqlExpression argument, JObject item, string fromAlias, IDictionary<string, object> parameters)
    {
        if (argument is IdentifierExpression ident)
        {
            var path = ident.Name;
            if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
            {
                path = path[(fromAlias.Length + 1)..];
            }

            return item.SelectToken(path) as JArray;
        }

        var evaluated = EvaluateSqlExpression(argument, item, fromAlias, parameters);
        return evaluated as JArray;
    }

    private static bool ArrayContainsMatch(JArray jArray, string searchStr, bool partial)
    {
        foreach (var element in jArray)
        {
            if (element.Type == JTokenType.Object && searchStr.StartsWith("{"))
            {
                var searchObj = JsonParseHelpers.ParseJson(searchStr);
                if (partial)
                {
                    var allMatch = searchObj.Properties().All(prop =>
                    {
                        var elementValue = element[prop.Name];
                        return elementValue is not null && JToken.DeepEquals(elementValue, prop.Value);
                    });
                    if (allMatch)
                    {
                        return true;
                    }
                }
                else if (JToken.DeepEquals(element, searchObj))
                {
                    return true;
                }
            }
            else if (string.Equals(element.ToString(), searchStr, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }
        return false;
    }

    private static bool ArrayContainsMatchJObject(JArray jArray, JObject searchObj, bool partial)
    {
        foreach (var element in jArray)
        {
            if (element.Type != JTokenType.Object)
            {
                continue;
            }

            if (partial)
            {
                var allMatch = searchObj.Properties().All(prop =>
                {
                    var elementValue = element[prop.Name];
                    return elementValue is not null && JToken.DeepEquals(elementValue, prop.Value);
                });
                if (allMatch)
                {
                    return true;
                }
            }
            else if (JToken.DeepEquals(element, searchObj))
            {
                return true;
            }
        }
        return false;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Patch operations
    // ═══════════════════════════════════════════════════════════════════════════

    private static void ApplyPatchOperations(JObject jObj, IReadOnlyList<PatchOperation> patchOperations)
    {
        foreach (var operation in patchOperations)
        {
            var path = GetPatchPath(operation);
            var segments = path.TrimStart('/').Split('/');
            var propertyName = segments.Last();
            var parentPath = string.Join(".", segments.Take(segments.Length - 1));
            var rawParent = segments.Length > 1 ? jObj.SelectToken(parentPath) : (JToken)jObj;

            switch (operation.OperationType)
            {
                case PatchOperationType.Add:
                    {
                        var value = GetPatchValue(operation);
                        var newToken = value is not null ? JToken.FromObject(value) : JValue.CreateNull();

                        if (propertyName == "-" && rawParent is JArray appendArray)
                        {
                            appendArray.Add(newToken);
                        }
                        else if (int.TryParse(propertyName, out var insertIdx) && rawParent is JArray insertArray)
                        {
                            insertArray.Insert(insertIdx, newToken);
                        }
                        else
                        {
                            var parent = rawParent as JObject ?? jObj;
                            parent[propertyName] = newToken;
                        }

                        break;
                    }
                case PatchOperationType.Set:
                case PatchOperationType.Replace:
                    {
                        var value = GetPatchValue(operation);
                        var parent = rawParent as JObject ?? jObj;
                        parent[propertyName] = value is not null ? JToken.FromObject(value) : JValue.CreateNull();
                        break;
                    }
                case PatchOperationType.Remove:
                    {
                        var parent = rawParent as JObject ?? jObj;
                        parent.Remove(propertyName);
                        break;
                    }
                case PatchOperationType.Move:
                    {
                        var sourcePath = GetPatchSourcePath(operation);
                        if (sourcePath is not null)
                        {
                            var sourceSegments = sourcePath.TrimStart('/').Split('/');
                            var sourcePropertyName = sourceSegments.Last();
                            var sourceParentPath = string.Join(".", sourceSegments.Take(sourceSegments.Length - 1));
                            var sourceParent = sourceSegments.Length > 1
                                ? jObj.SelectToken(sourceParentPath) as JObject ?? jObj
                                : jObj;
                            var sourceValue = sourceParent[sourcePropertyName];
                            if (sourceValue is not null)
                            {
                                sourceParent.Remove(sourcePropertyName);
                                var parent = rawParent as JObject ?? jObj;
                                parent[propertyName] = sourceValue;
                            }
                        }

                        break;
                    }
                case PatchOperationType.Increment:
                    {
                        var incrementValue = GetPatchValue(operation);
                        var parent = rawParent as JObject ?? jObj;
                        var existingToken = parent[propertyName];
                        if (existingToken is not null && incrementValue is not null)
                        {
                            var existingDouble = existingToken.Value<double>();
                            var incrementDouble = Convert.ToDouble(incrementValue);
                            var result = existingDouble + incrementDouble;
                            if (existingToken.Type == JTokenType.Integer && result == Math.Floor(result))
                            {
                                parent[propertyName] = (long)result;
                            }
                            else
                            {
                                parent[propertyName] = result;
                            }
                        }
                        break;
                    }
            }
        }
    }

    private static string GetPatchPath(PatchOperation operation) => operation.Path;

    private static string GetPatchSourcePath(PatchOperation operation)
    {
        var fromProp = operation.GetType()
            .GetProperty("From", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
        return fromProp?.GetValue(operation)?.ToString();
    }

    private static object GetPatchValue(PatchOperation operation)
    {
        var valueProp = operation.GetType()
            .GetProperty("Value", BindingFlags.Public | BindingFlags.Instance);
        return valueProp?.GetValue(operation);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Stream FeedIterator factory
    // ═══════════════════════════════════════════════════════════════════════════

    private static FeedIterator CreateStreamFeedIterator(List<string> items, int initialOffset = 0, int? maxItemCount = null)
    {
        var pageSize = maxItemCount ?? items.Count;
        if (pageSize <= 0) pageSize = items.Count;
        var offset = initialOffset;
        var hasRead = false;

        var feedIterator = Substitute.For<FeedIterator>();
        feedIterator.HasMoreResults.Returns(_ => !hasRead || offset < items.Count);
        feedIterator.ReadNextAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            hasRead = true;
            var page = items.Skip(offset).Take(pageSize).ToList();
            offset += page.Count;
            var documentsArray = new JArray(page.Select(JsonParseHelpers.ParseJson));
            var envelope = new JObject
            {
                ["Documents"] = documentsArray,
                ["_count"] = documentsArray.Count,
                ["_rid"] = string.Empty
            };
            var stream = ToStream(envelope.ToString(Formatting.None));
            var response = new ResponseMessage(HttpStatusCode.OK) { Content = stream };
            if (offset < items.Count)
                response.Headers.Add("x-ms-continuation", offset.ToString());
            return Task.FromResult(response);
        });
        return feedIterator;
    }



    // ═══════════════════════════════════════════════════════════════════════════
    //  InMemoryFeedResponse (for ReadManyItemsAsync)
    // ═══════════════════════════════════════════════════════════════════════════

    private sealed class InMemoryFeedResponse<T> : FeedResponse<T>
    {
        private readonly IReadOnlyList<T> _items;
        public InMemoryFeedResponse(IReadOnlyList<T> items) => _items = items;
        public override Headers Headers { get; } = new();
        public override IEnumerable<T> Resource => _items;
        public override HttpStatusCode StatusCode => HttpStatusCode.OK;
        public override CosmosDiagnostics Diagnostics => FakeDiagnostics;
        public override int Count => _items.Count;
        public override string IndexMetrics => null;
        public override string ContinuationToken => null;
        public override double RequestCharge => SyntheticRequestCharge;
        public override string ActivityId => string.Empty;
        public override string ETag => null;
        public override IEnumerator<T> GetEnumerator() => _items.GetEnumerator();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Vector distance helpers
    // ═══════════════════════════════════════════════════════════════════════════

    private static object VectorDistanceFunc(object[] args)
    {
        // VECTORDISTANCE(vector1, vector2 [, bool_bruteForce] [, {distanceFunction:'cosine'|'dotproduct'|'euclidean'})
        var vec1 = ToDoubleArray(args[0]);
        var vec2 = ToDoubleArray(args[1]);
        if (vec1 is null || vec2 is null || vec1.Length != vec2.Length || vec1.Length == 0)
            return null;

        // 3rd arg (bool bruteForce) is accepted but ignored in the emulator
        // 4th arg (object options) may contain distanceFunction override
        var distanceFunction = "cosine";
        if (args.Length > 3)
        {
            var options = args[3] switch
            {
                JObject jo => jo,
                string s when s.TrimStart().StartsWith("{") => JObject.Parse(s),
                _ => null,
            };
            var df = options?["distanceFunction"]?.ToString();
            if (df is not null) distanceFunction = df;
        }

        return distanceFunction.ToLowerInvariant() switch
        {
            "cosine" => CosineSimilarity(vec1, vec2),
            "dotproduct" => DotProduct(vec1, vec2),
            "euclidean" => EuclideanDistance(vec1, vec2),
            _ => CosineSimilarity(vec1, vec2),
        };
    }

    private static double CosineSimilarity(double[] a, double[] b)
    {
        double dot = 0, magA = 0, magB = 0;
        for (var i = 0; i < a.Length; i++)
        {
            dot += a[i] * b[i];
            magA += a[i] * a[i];
            magB += b[i] * b[i];
        }
        var denominator = Math.Sqrt(magA) * Math.Sqrt(magB);
        return denominator == 0 ? 0 : dot / denominator;
    }

    private static double DotProduct(double[] a, double[] b)
    {
        double sum = 0;
        for (var i = 0; i < a.Length; i++)
            sum += a[i] * b[i];
        return sum;
    }

    private static double EuclideanDistance(double[] a, double[] b)
    {
        double sum = 0;
        for (var i = 0; i < a.Length; i++)
        {
            var diff = a[i] - b[i];
            sum += diff * diff;
        }
        return Math.Sqrt(sum);
    }

    private static double[] ToDoubleArray(object value)
    {
        JArray ja = value switch
        {
            JArray arr => arr,
            string s when s.TrimStart().StartsWith("[") => JArray.Parse(s),
            _ => null,
        };
        if (ja is null) return null;

        var result = new double[ja.Count];
        for (var i = 0; i < ja.Count; i++)
        {
            if (ja[i].Type is not (JTokenType.Float or JTokenType.Integer))
                return null;
            result[i] = ja[i].Value<double>();
        }
        return result;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Spatial function helpers
    // ═══════════════════════════════════════════════════════════════════════════

    private static object StDistance(object left, object right)
    {
        var p1 = ExtractPoint(left);
        var p2 = ExtractPoint(right);
        if (p1 is null || p2 is null)
        {
            return null;
        }

        return HaversineDistanceMeters(p1.Value.Lat, p1.Value.Lon, p2.Value.Lat, p2.Value.Lon);
    }

    private static bool StWithin(object point, object region)
    {
        var p = ExtractPoint(point);
        if (p is null)
        {
            return false;
        }

        var polygon = ExtractPolygonRings(region);
        if (polygon is not null)
        {
            return PointInPolygon(p.Value, polygon);
        }

        var circle = ExtractCircle(region);
        if (circle is not null)
        {
            var dist = HaversineDistanceMeters(p.Value.Lat, p.Value.Lon, circle.Value.Center.Lat, circle.Value.Center.Lon);
            return dist <= circle.Value.RadiusMeters;
        }

        return false;
    }

    private static bool StIntersects(object geo1, object geo2)
    {
        var p1 = ExtractPoint(geo1);
        var p2 = ExtractPoint(geo2);

        if (p1 is not null && p2 is not null)
        {
            return Math.Abs(p1.Value.Lat - p2.Value.Lat) < 1e-10 &&
                   Math.Abs(p1.Value.Lon - p2.Value.Lon) < 1e-10;
        }

        if (p1 is not null)
        {
            return StWithin(geo1, geo2);
        }

        if (p2 is not null)
        {
            return StWithin(geo2, geo1);
        }

        var poly1 = ExtractPolygonRings(geo1);
        var poly2 = ExtractPolygonRings(geo2);
        if (poly1 is not null && poly2 is not null)
        {
            return PolygonsShareAnyPoint(poly1, poly2);
        }

        return false;
    }

    private static bool StIsValid(object geo)
    {
        var obj = AsJObject(geo);
        if (obj is null)
        {
            return false;
        }

        var type = obj["type"]?.ToString();
        return type switch
        {
            "Point" => ExtractPoint(geo) is not null,
            "Polygon" => ExtractPolygonRings(geo) is not null && ValidatePolygonRings(ExtractPolygonRings(geo)),
            "LineString" => ExtractCoordinateArray(obj["coordinates"]) is { Count: >= 2 },
            "MultiPoint" => ExtractCoordinateArray(obj["coordinates"]) is { Count: >= 1 },
            _ => false
        };
    }

    private static JObject StIsValidDetailed(object geo)
    {
        var obj = AsJObject(geo);
        if (obj is null)
        {
            return JObject.FromObject(new { valid = false, reason = "Not a valid GeoJSON object." });
        }

        var type = obj["type"]?.ToString();
        if (string.IsNullOrEmpty(type))
        {
            return JObject.FromObject(new { valid = false, reason = "GeoJSON object missing 'type' property." });
        }

        switch (type)
        {
            case "Point":
                if (ExtractPoint(geo) is null)
                {
                    return JObject.FromObject(new { valid = false, reason = "Point must have coordinates [longitude, latitude]." });
                }

                return JObject.FromObject(new { valid = true, reason = "" });

            case "Polygon":
                var rings = ExtractPolygonRings(geo);
                if (rings is null)
                {
                    return JObject.FromObject(new { valid = false, reason = "Polygon coordinates must be an array of linear rings." });
                }

                if (!ValidatePolygonRings(rings))
                {
                    return JObject.FromObject(new { valid = false, reason = "Polygon rings must be closed (first and last position must be identical) and have at least 4 positions." });
                }

                return JObject.FromObject(new { valid = true, reason = "" });

            case "LineString":
                if (ExtractCoordinateArray(obj["coordinates"]) is not { Count: >= 2 })
                {
                    return JObject.FromObject(new { valid = false, reason = "LineString must have at least 2 positions." });
                }

                return JObject.FromObject(new { valid = true, reason = "" });

            default:
                return JObject.FromObject(new { valid = false, reason = $"Unsupported GeoJSON type: {type}." });
        }
    }

    private readonly record struct GeoPoint(double Lat, double Lon);

    private readonly record struct GeoCircle(GeoPoint Center, double RadiusMeters);

    private static object StArea(object geo)
    {
        var rings = ExtractPolygonRings(geo);
        if (rings is null || rings.Count == 0) return null;
        // Approximate area using spherical excess formula for the outer ring, minus holes
        var area = SphericalPolygonArea(rings[0]);
        for (var i = 1; i < rings.Count; i++)
            area -= SphericalPolygonArea(rings[i]);
        return Math.Abs(area);
    }

    private static double SphericalPolygonArea(List<GeoPoint> ring)
    {
        // Spherical excess method (Girard's theorem) for area on a sphere
        double sum = 0;
        for (var i = 0; i < ring.Count - 1; i++)
        {
            var lon1 = ring[i].Lon * Math.PI / 180;
            var lat1 = ring[i].Lat * Math.PI / 180;
            var lon2 = ring[(i + 1) % (ring.Count - 1)].Lon * Math.PI / 180;
            var lat2 = ring[(i + 1) % (ring.Count - 1)].Lat * Math.PI / 180;
            sum += (lon2 - lon1) * (2 + Math.Sin(lat1) + Math.Sin(lat2));
        }
        return Math.Abs(sum * EarthRadiusMeters * EarthRadiusMeters / 2.0);
    }

    private static JObject AsJObject(object value)
    {
        return value switch
        {
            JObject jo => jo,
            string s when s.TrimStart().StartsWith("{") => JsonParseHelpers.ParseJson(s),
            _ => null
        };
    }

    private static GeoPoint? ExtractPoint(object value)
    {
        var obj = AsJObject(value);
        if (obj is null)
        {
            return null;
        }

        if (!string.Equals(obj["type"]?.ToString(), "Point", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        if (obj["coordinates"] is not JArray coords || coords.Count < 2)
        {
            return null;
        }

        var lon = coords[0].Value<double>();
        var lat = coords[1].Value<double>();
        if (lat < -90 || lat > 90 || lon < -180 || lon > 180)
        {
            return null;
        }

        return new GeoPoint(lat, lon);
    }

    private static List<List<GeoPoint>> ExtractPolygonRings(object value)
    {
        var obj = AsJObject(value);
        if (obj is null)
        {
            return null;
        }

        if (!string.Equals(obj["type"]?.ToString(), "Polygon", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        if (obj["coordinates"] is not JArray coords || coords.Count == 0)
        {
            return null;
        }

        var rings = new List<List<GeoPoint>>();
        foreach (var ring in coords)
        {
            var points = ExtractCoordinateArray(ring);
            if (points is null || points.Count < 4)
            {
                return null;
            }

            rings.Add(points);
        }

        return rings;
    }

    private static GeoCircle? ExtractCircle(object value)
    {
        var obj = AsJObject(value);
        if (obj is null)
        {
            return null;
        }

        var center = obj["center"];
        var radius = obj["radius"];
        if (center is null || radius is null)
        {
            return null;
        }

        var centerPoint = ExtractPoint(center);
        if (centerPoint is null)
        {
            return null;
        }

        if (!double.TryParse(radius.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var radiusMeters))
        {
            return null;
        }

        return new GeoCircle(centerPoint.Value, radiusMeters);
    }

    private static List<GeoPoint> ExtractCoordinateArray(JToken token)
    {
        if (token is not JArray arr)
        {
            return null;
        }

        var points = new List<GeoPoint>();
        foreach (var item in arr)
        {
            if (item is JArray coord && coord.Count >= 2)
            {
                var lon = coord[0].Value<double>();
                var lat = coord[1].Value<double>();
                points.Add(new GeoPoint(lat, lon));
            }
            else
            {
                return null;
            }
        }

        return points;
    }

    private static bool ValidatePolygonRings(List<List<GeoPoint>> rings)
    {
        foreach (var ring in rings)
        {
            if (ring.Count < 4)
            {
                return false;
            }

            var first = ring[0];
            var last = ring[^1];
            if (Math.Abs(first.Lat - last.Lat) > 1e-10 || Math.Abs(first.Lon - last.Lon) > 1e-10)
            {
                return false;
            }
        }

        return true;
    }

    private static double HaversineDistanceMeters(double lat1, double lon1, double lat2, double lon2)
    {
        var dLat = DegreesToRadians(lat2 - lat1);
        var dLon = DegreesToRadians(lon2 - lon1);
        var a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2) +
                Math.Cos(DegreesToRadians(lat1)) * Math.Cos(DegreesToRadians(lat2)) *
                Math.Sin(dLon / 2) * Math.Sin(dLon / 2);
        var c = 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
        return EarthRadiusMeters * c;
    }

    private static double DegreesToRadians(double degrees) => degrees * (Math.PI / 180.0);

    private static bool PointInPolygon(GeoPoint point, List<List<GeoPoint>> rings)
    {
        if (rings.Count == 0)
        {
            return false;
        }

        var inOuter = PointInRing(point, rings[0]);
        if (!inOuter)
        {
            return false;
        }

        for (var i = 1; i < rings.Count; i++)
        {
            if (PointInRing(point, rings[i]))
            {
                return false;
            }
        }

        return true;
    }

    private static bool PointInRing(GeoPoint point, List<GeoPoint> ring)
    {
        var inside = false;
        for (int i = 0, j = ring.Count - 1; i < ring.Count; j = i++)
        {
            if ((ring[i].Lat > point.Lat) != (ring[j].Lat > point.Lat) &&
                point.Lon < (ring[j].Lon - ring[i].Lon) * (point.Lat - ring[i].Lat) / (ring[j].Lat - ring[i].Lat) + ring[i].Lon)
            {
                inside = !inside;
            }
        }

        return inside;
    }

    private static bool PolygonsShareAnyPoint(List<List<GeoPoint>> poly1, List<List<GeoPoint>> poly2)
    {
        foreach (var point in poly1[0])
        {
            if (PointInPolygon(point, poly2))
            {
                return true;
            }
        }

        foreach (var point in poly2[0])
        {
            if (PointInPolygon(point, poly1))
            {
                return true;
            }
        }

        return false;
    }
}
