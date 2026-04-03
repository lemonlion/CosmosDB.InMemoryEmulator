using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// A custom <see cref="HttpMessageHandler"/> that intercepts all Cosmos SDK HTTP
/// requests and serves responses from an <see cref="InMemoryContainer"/>, including
/// account metadata, collection metadata, partition key ranges, and query execution.
/// </summary>
/// <remarks>
/// <para>
/// Documents are distributed across partition key ranges using a deterministic MurmurHash3
/// hash of the document's partition key value. Each range receives only the documents
/// whose hash maps to it. The SDK fans out queries to all ranges and merges the results.
/// </para>
/// <para>
/// ResourceIds are generated via direct byte construction with deterministic IDs.
/// </para>
/// <para>
/// Fault injection: set <see cref="FaultInjector"/> to a delegate that inspects incoming
/// requests and optionally returns an error response. When the delegate returns a non-null
/// <see cref="HttpResponseMessage"/>, that response is returned immediately without
/// executing the normal handler logic. This enables testing retry policies, rate-limiting
/// (429), transient failures (503), and timeout scenarios.
/// </para>
/// <para>
/// Multi-container: use <see cref="CreateRouter"/> to create a routing handler that
/// dispatches requests to different handler instances based on the container name in the
/// URL path. This allows a single <see cref="CosmosClient"/> to query multiple containers.
/// </para>
/// <para>
/// SDK compatibility: call <see cref="VerifySdkCompatibilityAsync"/> once during test suite
/// setup to detect breaking changes in the Cosmos SDK's internal HTTP contract before they
/// cause silent data corruption.
/// </para>
/// </remarks>
/// <summary>
/// Configuration options for <see cref="FakeCosmosHandler"/>.
/// </summary>
public sealed class FakeCosmosHandlerOptions
{
    /// <summary>
    /// How long query result cache entries remain valid before eviction.
    /// Defaults to 5 minutes.
    /// </summary>
    public TimeSpan CacheTtl { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Maximum number of query result cache entries before overflow eviction.
    /// Defaults to 100.
    /// </summary>
    public int CacheMaxEntries { get; init; } = 100;

    /// <summary>
    /// Number of partition key ranges to expose. The SDK sends a separate query
    /// per range; the handler serves the same data regardless. Increasing this
    /// exercises cross-partition fan-out paths in the SDK.
    /// Defaults to 1.
    /// </summary>
    public int PartitionKeyRangeCount { get; init; } = 1;
}

/// <summary>
/// Document type used by <see cref="FakeCosmosHandler.VerifySdkCompatibilityAsync"/>
/// to validate SDK compatibility. Public because NSubstitute proxy generation
/// requires accessible types.
/// </summary>
public sealed class CompatibilityDocument
{
    [JsonProperty("id")]
    public string Id { get; set; } = "";

    [JsonProperty("partitionKey")]
    public string PartitionKey { get; set; } = "";

    [JsonProperty("name")]
    public string Name { get; set; } = "";

    [JsonProperty("value")]
    public int Value { get; set; }
}

public class FakeCosmosHandler : HttpMessageHandler
{
    private readonly InMemoryContainer _container;
    private readonly ConcurrentBag<string> _requestLog = new();
    private readonly ConcurrentBag<string> _queryLog = new();
    private readonly QueryResultCache _queryResultCache;
    private readonly string _collectionRid;
    private readonly string _databaseRid;
    private readonly int _partitionKeyRangeCount;
    private readonly string _partitionKeyPath;
    private static int _ridCounter;
    private const string PkRangesEtag = "\"pk-etag-1\"";

    /// <summary>Recorded HTTP requests in the form "METHOD /path".</summary>
    public IReadOnlyCollection<string> RequestLog => _requestLog;

    /// <summary>The backing in-memory container that stores all data for this handler.</summary>
    public InMemoryContainer BackingContainer => _container;

    /// <summary>Recorded SQL query strings that were executed.</summary>
    public IReadOnlyCollection<string> QueryLog => _queryLog;

    /// <summary>
    /// Optional fault injection delegate. When set, it is called before normal request
    /// handling. If it returns a non-null response, that response is used immediately.
    /// By default, metadata requests (account, collection, pkranges) are excluded to avoid
    /// breaking SDK initialisation. Set <see cref="FaultInjectorIncludesMetadata"/> to
    /// <c>true</c> to also affect metadata routes.
    /// </summary>
    public Func<HttpRequestMessage, HttpResponseMessage?>? FaultInjector { get; set; }

    /// <summary>
    /// When <c>true</c>, the <see cref="FaultInjector"/> delegate is also invoked for
    /// metadata requests (account info, collection metadata, partition key ranges).
    /// Defaults to <c>false</c> so SDK initialisation is not disrupted.
    /// </summary>
    public bool FaultInjectorIncludesMetadata { get; set; }

    public FakeCosmosHandler(InMemoryContainer container)
        : this(container, new FakeCosmosHandlerOptions())
    {
    }

    public FakeCosmosHandler(InMemoryContainer container, FakeCosmosHandlerOptions options)
    {
        _container = container;
        _partitionKeyRangeCount = Math.Max(1, options.PartitionKeyRangeCount);
        _queryResultCache = new QueryResultCache(options.CacheTtl, options.CacheMaxEntries);
        (_databaseRid, _collectionRid) = GenerateResourceIds(container.Id);
        _partitionKeyPath = container.PartitionKeyPaths.FirstOrDefault()?.TrimStart('/') ?? "id";
    }

    private static (string DbRid, string CollRid) GenerateResourceIds(string containerId)
    {
        // Cosmos RID format: DB = 4-byte little-endian uint, Collection = DB bytes + 4 more bytes.
        // Use atomic counter for the DB portion so every handler instance gets a unique RID,
        // even if containers share the same name. Collection portion uses MurmurHash3 of the ID.
        var instanceId = (uint)Interlocked.Increment(ref _ridCounter);
        var dbBytes = BitConverter.GetBytes(instanceId);
        var containerHash = PartitionKeyHash.MurmurHash3(containerId);
        var collBytes = new byte[8];
        Buffer.BlockCopy(dbBytes, 0, collBytes, 0, 4);
        Buffer.BlockCopy(BitConverter.GetBytes(containerHash), 0, collBytes, 4, 4);
        return (Convert.ToBase64String(dbBytes), Convert.ToBase64String(collBytes));
    }

    protected override async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var path = request.RequestUri?.AbsolutePath ?? "";
        var method = request.Method.Method;
        _requestLog.Add($"{method} {path}");

        if (FaultInjectorIncludesMetadata && FaultInjector is not null)
        {
            var earlyFault = FaultInjector(request);
            if (earlyFault is not null)
            {
                return earlyFault;
            }
        }

        if (method == "GET" && path is "/" or "")
        {
            return CreateJsonResponse(AccountMetadata);
        }

        if (path.Contains("/pkranges"))
        {
            return HandlePartitionKeyRanges(request);
        }

        if (method == "GET" && path.Contains("/colls/") && !path.Contains("/docs"))
        {
            return CreateJsonResponse(GetCollectionMetadata());
        }

        if (!FaultInjectorIncludesMetadata && FaultInjector is not null)
        {
            var faultResponse = FaultInjector(request);
            if (faultResponse is not null)
            {
                return faultResponse;
            }
        }

        // Document-specific routes: /docs/{id} (point read, replace, delete, patch)
        if (path.Contains("/docs/") && HasDocumentId(path))
        {
            switch (method)
            {
                case "GET":
                    return await HandlePointReadAsync(request, cancellationToken);
                case "PUT":
                    return await HandleReplaceAsync(request, cancellationToken);
                case "DELETE":
                    return await HandleDeleteAsync(request, cancellationToken);
                case "PATCH":
                    return await HandlePatchAsync(request, cancellationToken);
            }
        }

        // POST /docs (overloaded: query plan, query, upsert, create)
        if (method == "POST" && path.Contains("/docs"))
        {
            if (request.Headers.TryGetValues("x-ms-cosmos-is-query-plan-request", out var qpValues) &&
                qpValues.Any(v => v.Equals("True", StringComparison.OrdinalIgnoreCase)))
            {
                return await HandleQueryPlanAsync(request, cancellationToken);
            }

            if (IsQueryRequest(request))
            {
                return await HandleQueryAsync(request, cancellationToken);
            }

            if (IsUpsertRequest(request))
            {
                return await HandleUpsertAsync(request, cancellationToken);
            }

            return await HandleCreateAsync(request, cancellationToken);
        }

        if (method == "GET" && path.Contains("/docs"))
        {
            return await HandleReadFeedAsync(request, cancellationToken);
        }

        return new HttpResponseMessage(HttpStatusCode.NotFound)
        {
            Content = new StringContent(
                $"{{\"message\":\"FakeCosmosHandler: unrecognised route {method} {path}\"}}",
                Encoding.UTF8,
                "application/json")
        };
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  CRUD route handlers
    // ═══════════════════════════════════════════════════════════════════════════

    private async Task<HttpResponseMessage> HandleCreateAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var body = await request.Content!.ReadAsStringAsync(cancellationToken);
        var pk = ExtractPartitionKey(request) ?? PartitionKey.None;
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(body));
        var result = await _container.CreateItemStreamAsync(stream, pk, BuildItemRequestOptions(request), cancellationToken);
        return ConvertToHttpResponse(result);
    }

    private async Task<HttpResponseMessage> HandleUpsertAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var body = await request.Content!.ReadAsStringAsync(cancellationToken);
        var pk = ExtractPartitionKey(request) ?? PartitionKey.None;
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(body));
        var result = await _container.UpsertItemStreamAsync(stream, pk, BuildItemRequestOptions(request), cancellationToken);
        return ConvertToHttpResponse(result);
    }

    private async Task<HttpResponseMessage> HandlePointReadAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var id = ExtractDocumentId(request);
        var pk = ExtractPartitionKey(request) ?? PartitionKey.None;
        var result = await _container.ReadItemStreamAsync(id, pk, BuildItemRequestOptions(request), cancellationToken);
        return ConvertToHttpResponse(result);
    }

    private async Task<HttpResponseMessage> HandleReplaceAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var id = ExtractDocumentId(request);
        var body = await request.Content!.ReadAsStringAsync(cancellationToken);
        var pk = ExtractPartitionKey(request) ?? PartitionKey.None;
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(body));
        var result = await _container.ReplaceItemStreamAsync(stream, id, pk, BuildItemRequestOptions(request), cancellationToken);
        return ConvertToHttpResponse(result);
    }

    private async Task<HttpResponseMessage> HandleDeleteAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var id = ExtractDocumentId(request);
        var pk = ExtractPartitionKey(request) ?? PartitionKey.None;
        var result = await _container.DeleteItemStreamAsync(id, pk, BuildItemRequestOptions(request), cancellationToken);
        return ConvertToHttpResponse(result);
    }

    private async Task<HttpResponseMessage> HandlePatchAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var id = ExtractDocumentId(request);
        var body = await request.Content!.ReadAsStringAsync(cancellationToken);
        var pk = ExtractPartitionKey(request) ?? PartitionKey.None;
        var (operations, condition) = ParsePatchBody(body);
        var options = new PatchItemRequestOptions();
        if (condition is not null)
        {
            options.FilterPredicate = condition;
        }
        if (request.Headers.IfMatch.Any())
        {
            options.IfMatchEtag = request.Headers.IfMatch.First().Tag;
        }
        var result = await _container.PatchItemStreamAsync(id, pk, operations, options, cancellationToken);
        return ConvertToHttpResponse(result);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  CRUD helpers
    // ═══════════════════════════════════════════════════════════════════════════

    private static HttpResponseMessage ConvertToHttpResponse(ResponseMessage cosmosResponse)
    {
        var httpResponse = new HttpResponseMessage(cosmosResponse.StatusCode);
        if (cosmosResponse.Content is not null)
        {
            using var reader = new StreamReader(cosmosResponse.Content);
            var json = reader.ReadToEnd();
            if (json.Length > 0)
            {
                httpResponse.Content = new StringContent(json, Encoding.UTF8, "application/json");
            }
        }

        httpResponse.Headers.Add("x-ms-request-charge", "1");
        httpResponse.Headers.Add("x-ms-activity-id", Guid.NewGuid().ToString());
        httpResponse.Headers.Add("x-ms-session-token", "0:0#1");

        var etag = cosmosResponse.Headers["ETag"];
        if (etag is not null)
        {
            httpResponse.Headers.TryAddWithoutValidation("etag", etag);
        }

        return httpResponse;
    }

    private static string ExtractDocumentId(HttpRequestMessage request)
    {
        var path = request.RequestUri?.AbsolutePath ?? "";
        var docsIndex = path.LastIndexOf("/docs/", StringComparison.OrdinalIgnoreCase);
        if (docsIndex >= 0)
        {
            var id = path[(docsIndex + 6)..];
            return Uri.UnescapeDataString(id.TrimEnd('/'));
        }
        return "";
    }

    private static bool HasDocumentId(string path)
    {
        var docsIndex = path.LastIndexOf("/docs/", StringComparison.OrdinalIgnoreCase);
        if (docsIndex < 0) return false;
        var afterDocs = path[(docsIndex + 6)..].TrimEnd('/');
        return afterDocs.Length > 0;
    }

    private static ItemRequestOptions BuildItemRequestOptions(HttpRequestMessage request)
    {
        var options = new ItemRequestOptions();
        if (request.Headers.IfMatch.Any())
        {
            options.IfMatchEtag = request.Headers.IfMatch.First().Tag;
        }
        if (request.Headers.IfNoneMatch.Any())
        {
            options.IfNoneMatchEtag = request.Headers.IfNoneMatch.First().Tag;
        }
        return options;
    }

    private static bool IsQueryRequest(HttpRequestMessage request)
    {
        var contentType = request.Content?.Headers?.ContentType?.MediaType ?? "";
        if (contentType.Contains("query+json", StringComparison.OrdinalIgnoreCase))
            return true;
        if (request.Headers.TryGetValues("x-ms-documentdb-isquery", out var values) &&
            values.Any(v => v.Equals("True", StringComparison.OrdinalIgnoreCase)))
            return true;
        return false;
    }

    private static bool IsUpsertRequest(HttpRequestMessage request)
    {
        return request.Headers.TryGetValues("x-ms-documentdb-is-upsert", out var values) &&
               values.Any(v => v.Equals("True", StringComparison.OrdinalIgnoreCase));
    }

    private static (IReadOnlyList<PatchOperation> Operations, string? Condition) ParsePatchBody(string body)
    {
        var jObj = JObject.Parse(body);
        var operations = new List<PatchOperation>();
        var condition = jObj["condition"]?.ToString();

        foreach (var op in jObj["operations"]!.ToObject<JArray>()!)
        {
            var opType = op["op"]!.ToString().ToLowerInvariant();
            var opPath = op["path"]!.ToString();
            var value = op["value"];

            operations.Add(opType switch
            {
                "set" => PatchOperation.Set(opPath, value),
                "replace" => PatchOperation.Set(opPath, value),
                "add" => PatchOperation.Add(opPath, value),
                "remove" => PatchOperation.Remove(opPath),
                "incr" => PatchOperation.Increment(opPath, value!.Value<double>()),
                _ => throw new InvalidOperationException($"Unknown patch operation: {opType}")
            });
        }

        return (operations, condition);
    }

    private HttpResponseMessage HandlePartitionKeyRanges(HttpRequestMessage request)
    {
        if (request.Headers.IfNoneMatch.Any(etag => etag.Tag == PkRangesEtag))
        {
            var notModified = new HttpResponseMessage(HttpStatusCode.NotModified);
            notModified.Headers.Add("x-ms-request-charge", "0");
            notModified.Headers.Add("x-ms-activity-id", Guid.NewGuid().ToString());
            notModified.Headers.ETag = new System.Net.Http.Headers.EntityTagHeaderValue(PkRangesEtag);
            return notModified;
        }

        var response = CreateJsonResponse(GetPartitionKeyRanges());
        response.Headers.ETag = new System.Net.Http.Headers.EntityTagHeaderValue(PkRangesEtag);
        return response;
    }

    /// <summary>
    /// Handles the gateway query plan request that the SDK sends on non-Windows platforms
    /// (where the native ServiceInterop DLL is unavailable). Parses the SQL query and
    /// returns a <c>PartitionedQueryExecutionInfo</c> with accurate metadata so that the
    /// SDK builds the same execution pipeline (ORDER BY merge sort, aggregate accumulation,
    /// DISTINCT deduplication, etc.) as it would on Windows via ServiceInterop.
    /// </summary>
    private async Task<HttpResponseMessage> HandleQueryPlanAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var body = await request.Content!.ReadAsStringAsync(cancellationToken);
        var queryBody = JsonParseHelpers.ParseJson(body);
        var sqlQuery = queryBody["query"]?.ToString() ?? "SELECT * FROM c";

        var queryInfo = new JObject
        {
            ["distinctType"] = "None",
            ["top"] = null,
            ["offset"] = null,
            ["limit"] = null,
            ["orderBy"] = new JArray(),
            ["orderByExpressions"] = new JArray(),
            ["groupByExpressions"] = new JArray(),
            ["groupByAliases"] = new JArray(),
            ["aggregates"] = new JArray(),
            ["groupByAliasToAggregateType"] = new JObject(),
            ["rewrittenQuery"] = "",
            ["hasSelectValue"] = false,
            ["hasNonStreamingOrderBy"] = false
        };

        if (CosmosSqlParser.TryParse(sqlQuery, out var parsed))
        {
            // ORDER BY
            if (parsed.OrderByFields is { Length: > 0 })
            {
                var orderByArr = new JArray();
                var orderByExprArr = new JArray();
                foreach (var field in parsed.OrderByFields)
                {
                    orderByArr.Add(field.Ascending ? "Ascending" : "Descending");
                    orderByExprArr.Add(field.Field ?? CosmosSqlParser.ExprToString(field.Expression));
                }

                queryInfo["orderBy"] = orderByArr;
                queryInfo["orderByExpressions"] = orderByExprArr;
                queryInfo["hasNonStreamingOrderBy"] = true;
            }

            // TOP
            if (parsed.TopCount.HasValue)
            {
                queryInfo["top"] = parsed.TopCount.Value;
            }

            // OFFSET / LIMIT
            if (parsed.Offset.HasValue)
            {
                queryInfo["offset"] = parsed.Offset.Value;
            }

            if (parsed.Limit.HasValue)
            {
                queryInfo["limit"] = parsed.Limit.Value;
            }

            // DISTINCT
            if (parsed.IsDistinct)
            {
                queryInfo["distinctType"] = parsed.OrderByFields is { Length: > 0 }
                    ? "Ordered"
                    : "Unordered";
            }

            // GROUP BY
            if (parsed.GroupByFields is { Length: > 0 })
            {
                queryInfo["groupByExpressions"] = new JArray(parsed.GroupByFields);
            }

            // Aggregates — detect COUNT, SUM, MIN, MAX, AVG in SELECT fields
            var aggregates = new JArray();
            var groupByAliasToAgg = new JObject();
            foreach (var field in parsed.SelectFields)
            {
                DetectAggregates(field.SqlExpr, aggregates, groupByAliasToAgg, field.Alias);
            }

            if (aggregates.Count > 0)
            {
                queryInfo["aggregates"] = aggregates;
            }

            if (groupByAliasToAgg.Count > 0)
            {
                queryInfo["groupByAliasToAggregateType"] = groupByAliasToAgg;
            }

            // SELECT VALUE
            if (parsed.IsValueSelect)
            {
                queryInfo["hasSelectValue"] = true;
            }

            // Rewritten query — on non-Windows platforms the SDK uses this verbatim.
            // For ORDER BY queries the SDK expects documents wrapped with
            // orderByItems + payload as separate SELECT fields (not SELECT VALUE).
            // For OFFSET/LIMIT queries, the SDK pipeline applies OFFSET/LIMIT
            // from the queryInfo fields, so the rewritten query must NOT include them
            // (otherwise they'd be applied twice: once by the container and once by the SDK).
            if (parsed.OrderByFields is { Length: > 0 })
            {
                queryInfo["rewrittenQuery"] = BuildOrderByRewrittenQuery(parsed);
            }
            else if (parsed.Offset.HasValue || parsed.Limit.HasValue)
            {
                queryInfo["rewrittenQuery"] = StripOffsetLimit(sqlQuery);
            }
            else
            {
                queryInfo["rewrittenQuery"] = sqlQuery;
            }
        }
        else
        {
            queryInfo["rewrittenQuery"] = sqlQuery;
        }

        var queryPlan = new JObject
        {
            ["partitionedQueryExecutionInfoVersion"] = 2,
            ["queryInfo"] = queryInfo,
            ["queryRanges"] = new JArray(new JObject
            {
                ["min"] = "",
                ["max"] = "FF",
                ["isMinInclusive"] = true,
                ["isMaxInclusive"] = false
            })
        };

        return CreateJsonResponse(queryPlan.ToString(Formatting.None));
    }

    private static void DetectAggregates(
        SqlExpression? expr, JArray aggregates, JObject groupByAliasToAgg, string? alias)
    {
        if (expr is FunctionCallExpression func)
        {
            var name = func.FunctionName.ToUpperInvariant();
            string? aggType = name switch
            {
                "COUNT" => "Count",
                "SUM" => "Sum",
                "MIN" => "Min",
                "MAX" => "Max",
                "AVG" => "Average",
                _ => null
            };

            if (aggType is not null)
            {
                if (!aggregates.Any(a => a.ToString() == aggType))
                {
                    aggregates.Add(aggType);
                }

                if (alias is not null)
                {
                    groupByAliasToAgg[alias] = aggType;
                }
            }
        }
        else if (expr is BinaryExpression bin)
        {
            DetectAggregates(bin.Left, aggregates, groupByAliasToAgg, alias);
            DetectAggregates(bin.Right, aggregates, groupByAliasToAgg, alias);
        }
        else if (expr is UnaryExpression unary)
        {
            DetectAggregates(unary.Operand, aggregates, groupByAliasToAgg, alias);
        }
    }

    private static bool HasAggregateInSelect(CosmosSqlQuery parsed)
    {
        return parsed.SelectFields.Any(field => ContainsAggregate(field.SqlExpr));
    }

    private static bool ContainsAggregate(SqlExpression? expr)
    {
        return expr switch
        {
            FunctionCallExpression func => func.FunctionName.ToUpperInvariant() is "COUNT" or "SUM" or "MIN" or "MAX" or "AVG",
            BinaryExpression bin => ContainsAggregate(bin.Left) || ContainsAggregate(bin.Right),
            UnaryExpression unary => ContainsAggregate(unary.Operand),
            _ => false
        };
    }

    private static bool ContainsAvg(SqlExpression? expr)
    {
        return expr switch
        {
            FunctionCallExpression func => func.FunctionName.Equals("AVG", StringComparison.OrdinalIgnoreCase),
            BinaryExpression bin => ContainsAvg(bin.Left) || ContainsAvg(bin.Right),
            UnaryExpression unary => ContainsAvg(unary.Operand),
            _ => false
        };
    }

    private async Task<HttpResponseMessage> HandleQueryAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var body = await request.Content!.ReadAsStringAsync(cancellationToken);
        var queryBody = JsonParseHelpers.ParseJson(body);
        var sqlQuery = queryBody["query"]?.ToString() ?? "SELECT * FROM c";
        _queryLog.Add(sqlQuery);

        var partitionKey = ExtractPartitionKey(request);
        var maxItemCount = ExtractMaxItemCount(request);
        var continuation = DecodeContinuation(request);
        var rangeId = ExtractPartitionKeyRangeId(request);

        List<JToken> allDocuments;
        string cacheKey;
        int offset;
        string? payloadPropertyName = null;

        if (continuation is not null && _queryResultCache.TryGet(continuation.Value.Key, out var cached))
        {
            allDocuments = cached;
            cacheKey = continuation.Value.Key;
            offset = continuation.Value.Offset;
        }
        else
        {
            offset = 0;
            cacheKey = Guid.NewGuid().ToString("N");

            if (CosmosSqlParser.TryParse(sqlQuery, out var parsed))
            {
                var orderByItemsField = parsed.SelectFields
                    .FirstOrDefault(field => IsOrderByItemsArray(field.SqlExpr));
                var isOrderByQuery = orderByItemsField is not null && parsed.OrderByFields is { Length: > 0 };

                if (isOrderByQuery)
                {
                    var payloadField = parsed.SelectFields
                        .FirstOrDefault(field => string.Equals(field.Alias, "payload", StringComparison.OrdinalIgnoreCase))
                        ?? parsed.SelectFields
                            .FirstOrDefault(field => field != orderByItemsField);
                    var orderByAlias = orderByItemsField!.Alias ?? "orderByItems";
                    payloadPropertyName = payloadField?.Alias ?? "payload";

                    allDocuments = await HandleOrderByQueryAsync(
                        parsed, queryBody, partitionKey, orderByAlias, payloadPropertyName, cancellationToken);
                }
                else
                {
                    var simplifiedSql = CosmosSqlParser.SimplifySdkQuery(parsed);
                    var queryDef = BuildQueryDefinition(simplifiedSql, queryBody);
                    var requestOptions = partitionKey is not null
                        ? new QueryRequestOptions { PartitionKey = partitionKey }
                        : null;
                    allDocuments = await DrainIterator(
                        _container.GetItemQueryIterator<JToken>(queryDef, requestOptions: requestOptions),
                        cancellationToken);

                    // On non-Windows platforms, the SDK's AggregateQueryPipelineStage
                    // expects each document to be a CosmosArray containing an object
                    // with an "item" field. AVG additionally needs {"sum", "count"} form
                    // since the SDK computes weighted averages across partitions.
                    if (parsed.IsValueSelect && HasAggregateInSelect(parsed)
                        && allDocuments.Count > 0 && allDocuments[0] is not JArray)
                    {
                        var isAvg = ContainsAvg(parsed.SelectFields[0].SqlExpr);
                        allDocuments = allDocuments.Select(d =>
                        {
                            JToken itemValue = isAvg
                                ? new JObject { ["sum"] = d, ["count"] = 1 }
                                : d.DeepClone();
                            return (JToken)new JArray(new JObject { ["item"] = itemValue });
                        }).ToList();
                    }
                }
            }
            else
            {
                var queryDef = BuildQueryDefinition(sqlQuery, queryBody);
                var requestOptions = partitionKey is not null
                    ? new QueryRequestOptions { PartitionKey = partitionKey }
                    : null;
                allDocuments = await DrainIterator(
                    _container.GetItemQueryIterator<JToken>(queryDef, requestOptions: requestOptions),
                    cancellationToken);
            }
        }

        allDocuments = FilterDocumentsByRange(allDocuments, rangeId, payloadPropertyName);

        return BuildPagedResponse(allDocuments, offset, maxItemCount, cacheKey);
    }

    private async Task<List<JToken>> HandleOrderByQueryAsync(
        CosmosSqlQuery parsed, JObject queryBody, PartitionKey? partitionKey,
        string orderByAlias, string payloadAlias, CancellationToken cancellationToken)
    {
        string simplifiedSql;
        try
        {
            simplifiedSql = CosmosSqlParser.SimplifySdkQuery(parsed);
        }
        catch
        {
            // If SimplifySdkQuery fails (e.g. SDK changed internal format),
            // fall back to executing SQL with SELECT VALUE <alias> and ORDER BY from the parsed structure.
            simplifiedSql = BuildFallbackOrderBySql(parsed);
        }

        var queryDef = BuildQueryDefinition(simplifiedSql, queryBody);
        var requestOptions = partitionKey is not null
            ? new QueryRequestOptions { PartitionKey = partitionKey }
            : null;
        var feedIterator = _container.GetItemQueryIterator<JObject>(queryDef, requestOptions: requestOptions);
        var rawDocuments = new List<JObject>();
        while (feedIterator.HasMoreResults)
        {
            var page = await feedIterator.ReadNextAsync(cancellationToken);
            rawDocuments.AddRange(page);
        }

        List<string> orderByPaths;
        try
        {
            orderByPaths = ExtractOrderByItemPaths(parsed);
        }
        catch
        {
            // Fallback: derive from ORDER BY fields if AST walking fails
            orderByPaths = parsed.OrderByFields?.Select(field =>
                StripFromAlias(field.Field, parsed.FromAlias)).ToList() ?? [];
        }

        var documents = new List<JToken>();
        foreach (var doc in rawDocuments)
        {
            var orderByItems = new JArray();
            foreach (var path in orderByPaths)
            {
                var value = doc.SelectToken(path)?.DeepClone() ?? JValue.CreateNull();
                orderByItems.Add(new JObject { ["item"] = value });
            }

            var wrapped = new JObject
            {
                ["_rid"] = doc["_rid"]?.ToString() ?? Guid.NewGuid().ToString("N")[..8],
                [orderByAlias] = orderByItems,
                [payloadAlias] = doc.DeepClone()
            };
            documents.Add(wrapped);
        }

        return documents;
    }

    /// <summary>
    /// Strips OFFSET ... LIMIT ... from a SQL query string so the SDK pipeline
    /// can apply OFFSET/LIMIT itself (avoiding double application).
    /// </summary>
    private static string StripOffsetLimit(string sql)
    {
        return Regex.Replace(sql, @"\s+OFFSET\s+\d+\s+LIMIT\s+\d+", "", RegexOptions.IgnoreCase).TrimEnd();
    }

    /// <summary>
    /// Builds the ORDER BY rewritten query in the format the SDK expects:
    /// <c>SELECT c._rid, [{"item": c.field}] AS orderByItems, c AS payload FROM c ... ORDER BY c.field ASC</c>
    /// </summary>
    private static string BuildOrderByRewrittenQuery(CosmosSqlQuery parsed)
    {
        var alias = parsed.FromAlias ?? "c";

        // Build orderByItems array: [{"item": c.field1}, {"item": c.field2}]
        var orderByItemsParts = parsed.OrderByFields!
            .Select(field => $"{{\"item\": {field.Field}}}")
            .ToList();
        var orderByItemsArray = $"[{string.Join(", ", orderByItemsParts)}]";

        // Build SELECT with top-level fields: _rid, orderByItems, payload
        var sb = new StringBuilder($"SELECT {alias}._rid, ");
        sb.Append(orderByItemsArray);
        sb.Append(" AS orderByItems, ");
        sb.Append($"{alias} AS payload");

        // FROM clause
        sb.Append($" FROM {alias}");

        // WHERE clause — reconstruct from the where expression if present
        if (parsed.WhereExpr is not null)
        {
            sb.Append(" WHERE ");
            sb.Append(CosmosSqlParser.ExprToString(parsed.WhereExpr));
        }

        // ORDER BY clause
        var orderByStr = string.Join(", ", parsed.OrderByFields!.Select(field =>
            $"{field.Field} {(field.Ascending ? "ASC" : "DESC")}"));
        sb.Append($" ORDER BY {orderByStr}");

        return sb.ToString();
    }

    private static string BuildFallbackOrderBySql(CosmosSqlQuery parsed)
    {
        var sb = new StringBuilder("SELECT ");

        if (parsed.IsDistinct)
        {
            sb.Append("DISTINCT ");
        }

        if (parsed.TopCount.HasValue)
        {
            sb.Append($"TOP {parsed.TopCount.Value} ");
        }

        sb.Append($"VALUE {parsed.FromAlias} FROM {parsed.FromAlias}");

        if (parsed.OrderByFields is { Length: > 0 })
        {
            var orderByStr = string.Join(", ", parsed.OrderByFields.Select(field =>
                $"{field.Field} {(field.Ascending ? "ASC" : "DESC")}"));
            sb.Append($" ORDER BY {orderByStr}");
        }

        return sb.ToString();
    }

    private static List<string> ExtractOrderByItemPaths(CosmosSqlQuery parsed)
    {
        var orderByItemsField = parsed.SelectFields
            .FirstOrDefault(field => IsOrderByItemsArray(field.SqlExpr));

        if (orderByItemsField?.SqlExpr is ArrayLiteralExpression arrayExpr)
        {
            var paths = new List<string>();
            foreach (var element in arrayExpr.Elements)
            {
                if (element is ObjectLiteralExpression obj)
                {
                    var itemProp = obj.Properties.FirstOrDefault(property =>
                        string.Equals(property.Key, "item", StringComparison.OrdinalIgnoreCase));
                    if (itemProp.Value is IdentifierExpression ident)
                    {
                        paths.Add(StripFromAlias(ident.Name, parsed.FromAlias));
                    }
                }
            }

            if (paths.Count > 0)
            {
                return paths;
            }
        }

        if (parsed.OrderByFields is { Length: > 0 })
        {
            return parsed.OrderByFields
                .Select(field => StripFromAlias(field.Field, parsed.FromAlias))
                .ToList();
        }

        return [];
    }

    private static string StripFromAlias(string path, string fromAlias)
    {
        if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
        {
            return path[(fromAlias.Length + 1)..];
        }

        return path;
    }

    private static bool IsOrderByItemsArray(SqlExpression? expr)
    {
        return expr is ArrayLiteralExpression { Elements.Length: > 0 } arrayLiteral
            && arrayLiteral.Elements.All(element =>
                element is ObjectLiteralExpression obj
                && obj.Properties.Any(prop =>
                    string.Equals(prop.Key, "item", StringComparison.OrdinalIgnoreCase)));
    }

    private List<JToken> FilterDocumentsByRange(List<JToken> documents, string? rangeId, string? payloadPropertyName = null)
    {
        if (_partitionKeyRangeCount <= 1 || rangeId is null)
        {
            return documents;
        }

        var rangeIndex = int.Parse(rangeId);
        return documents.Where(document => GetRangeIndex(document, payloadPropertyName) == rangeIndex).ToList();
    }

    private int GetRangeIndex(JToken document, string? payloadPropertyName)
    {
        if (document is not JObject obj)
        {
            return 0;
        }

        var targetDoc = payloadPropertyName is not null && obj[payloadPropertyName] is JObject payload
            ? payload : obj;
        var pkValue = targetDoc.SelectToken(_partitionKeyPath)?.ToString() ?? "";
        return PartitionKeyHash.GetRangeIndex(pkValue, _partitionKeyRangeCount);
    }

    private async Task<HttpResponseMessage> HandleReadFeedAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var maxItemCount = ExtractMaxItemCount(request);
        var continuation = DecodeContinuation(request);
        var rangeId = ExtractPartitionKeyRangeId(request);

        List<JToken> allDocuments;
        string cacheKey;
        int offset;

        if (continuation is not null && _queryResultCache.TryGet(continuation.Value.Key, out var cached))
        {
            allDocuments = cached;
            cacheKey = continuation.Value.Key;
            offset = continuation.Value.Offset;
        }
        else
        {
            offset = 0;
            cacheKey = Guid.NewGuid().ToString("N");

            var requestOptions = new QueryRequestOptions();
            var partitionKey = ExtractPartitionKey(request);
            if (partitionKey is not null)
            {
                requestOptions.PartitionKey = partitionKey;
            }

            allDocuments = await DrainIterator(
                _container.GetItemQueryIterator<JObject>(requestOptions: requestOptions),
                cancellationToken);
        }

        allDocuments = FilterDocumentsByRange(allDocuments, rangeId);

        return BuildPagedResponse(allDocuments, offset, maxItemCount, cacheKey);
    }

    private HttpResponseMessage BuildPagedResponse(
        List<JToken> allDocuments, int offset, int maxItemCount, string cacheKey)
    {
        var paged = allDocuments.Skip(offset).ToList();
        string? continuationToken = null;
        if (maxItemCount > 0 && paged.Count > maxItemCount)
        {
            paged = paged.Take(maxItemCount).ToList();
            _queryResultCache.Set(cacheKey, allDocuments);
            continuationToken = EncodeContinuation(cacheKey, offset + maxItemCount);
        }
        else
        {
            _queryResultCache.Remove(cacheKey);
        }

        var responseBody = new JObject
        {
            ["_rid"] = _collectionRid,
            ["Documents"] = new JArray(paged),
            ["_count"] = paged.Count
        };

        var response = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(responseBody.ToString(), Encoding.UTF8, "application/json")
        };

        response.Headers.Add("x-ms-request-charge", "1");
        response.Headers.Add("x-ms-activity-id", Guid.NewGuid().ToString());
        response.Headers.Add("x-ms-session-token", "0:0#1");
        response.Headers.Add("x-ms-item-count", paged.Count.ToString());

        if (continuationToken is not null)
        {
            response.Headers.Add("x-ms-continuation", continuationToken);
        }

        return response;
    }

    private static async Task<List<JToken>> DrainIterator<T>(
        FeedIterator<T> feedIterator, CancellationToken cancellationToken)
        where T : JToken
    {
        var allDocuments = new List<JToken>();
        while (feedIterator.HasMoreResults)
        {
            var page = await feedIterator.ReadNextAsync(cancellationToken);
            allDocuments.AddRange(page);
        }

        return allDocuments;
    }

    private static QueryDefinition BuildQueryDefinition(string sqlQuery, JObject queryBody)
    {
        var queryDef = new QueryDefinition(sqlQuery);
        if (queryBody["parameters"] is JArray parameters)
        {
            foreach (var parameter in parameters)
            {
                var paramName = parameter["name"]?.ToString();
                var paramValue = parameter["value"];
                if (paramName is not null)
                {
                    queryDef = queryDef.WithParameter(paramName, paramValue);
                }
            }
        }

        return queryDef;
    }

    private static PartitionKey? ExtractPartitionKey(HttpRequestMessage request)
    {
        if (request.Headers.TryGetValues("x-ms-documentdb-partitionkey", out var values))
        {
            var raw = values.FirstOrDefault();
            if (raw is not null)
            {
                try
                {
                    var arr = JArray.Parse(raw);
                    if (arr.Count == 1)
                    {
                        return arr[0].Type switch
                        {
                            JTokenType.String => new PartitionKey(arr[0].Value<string>()),
                            JTokenType.Integer or JTokenType.Float => new PartitionKey(arr[0].Value<double>()),
                            JTokenType.Boolean => new PartitionKey(arr[0].Value<bool>()),
                            JTokenType.Null => PartitionKey.Null,
                            _ => new PartitionKey(arr[0].ToString())
                        };
                    }

                    if (arr.Count > 1)
                    {
                        var builder = new PartitionKeyBuilder();
                        foreach (var token in arr)
                        {
                            switch (token.Type)
                            {
                                case JTokenType.String:
                                    builder.Add(token.Value<string>()!);
                                    break;
                                case JTokenType.Integer or JTokenType.Float:
                                    builder.Add(token.Value<double>());
                                    break;
                                case JTokenType.Boolean:
                                    builder.Add(token.Value<bool>());
                                    break;
                                case JTokenType.Null:
                                    builder.AddNullValue();
                                    break;
                                default:
                                    builder.Add(token.ToString());
                                    break;
                            }
                        }
                        return builder.Build();
                    }
                }
                catch
                {
                    // Ignore malformed partition key headers
                }
            }
        }

        return null;
    }

    private static int ExtractMaxItemCount(HttpRequestMessage request)
    {
        if (request.Headers.TryGetValues("x-ms-max-item-count", out var values) &&
            int.TryParse(values.FirstOrDefault(), out var count) && count > 0)
        {
            return count;
        }

        return 0;
    }

    private static string? ExtractPartitionKeyRangeId(HttpRequestMessage request)
    {
        if (request.Headers.TryGetValues("x-ms-documentdb-partitionkeyrangeid", out var values))
        {
            return values.FirstOrDefault();
        }

        return null;
    }

    private static (string Key, int Offset)? DecodeContinuation(HttpRequestMessage request)
    {
        if (!request.Headers.TryGetValues("x-ms-continuation", out var values))
        {
            return null;
        }

        var raw = values.FirstOrDefault();
        if (raw is null)
        {
            return null;
        }

        try
        {
            var json = Encoding.UTF8.GetString(Convert.FromBase64String(raw));
            var obj = JsonParseHelpers.ParseJson(json);
            var key = obj["key"]?.Value<string>();
            var offset = obj["offset"]?.Value<int>() ?? 0;
            return key is not null ? (key, offset) : null;
        }
        catch
        {
            return null;
        }
    }

    private static string EncodeContinuation(string cacheKey, int offset)
    {
        var json = $"{{\"v\":2,\"key\":\"{cacheKey}\",\"offset\":{offset}}}";
        return Convert.ToBase64String(Encoding.UTF8.GetBytes(json));
    }

    private static HttpResponseMessage CreateJsonResponse(string json)
    {
        var response = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json")
        };
        response.Headers.Add("x-ms-request-charge", "0");
        response.Headers.Add("x-ms-activity-id", Guid.NewGuid().ToString());
        response.Headers.Add("x-ms-session-token", "0:0#1");
        return response;
    }

    private const string AccountMetadata = """
        {
            "id": "fake-account",
            "_rid": "",
            "databasesLink": "/dbs/",
            "mediaLink": "/media/",
            "addressesLink": "",
            "userConsistencyPolicy": { "defaultConsistencyLevel": "Session" },
            "writableLocations": [
                { "name": "East US", "databaseAccountEndpoint": "https://localhost:9999/" }
            ],
            "readableLocations": [
                { "name": "East US", "databaseAccountEndpoint": "https://localhost:9999/" }
            ],
            "systemReplicationPolicy": { "minReplicaSetSize": 1, "maxReplicaCount": 4 },
            "readPolicy": { "primaryReadCoefficient": 1, "secondaryReadCoefficient": 1 },
            "queryEngineConfiguration": "{\"maxSqlQueryInputLength\":262144,\"maxJoinsPerSqlQuery\":5,\"maxLogicalAndPerSqlQuery\":500,\"maxLogicalOrPerSqlQuery\":500,\"maxUdfRefPerSqlQuery\":10,\"maxInExpressionItemsCount\":16000,\"queryMaxInMemorySortDocumentCount\":500,\"maxQueryRequestTimeoutFraction\":0.9,\"sqlAllowNonFiniteNumbers\":false,\"sqlAllowAggregateFunctions\":true,\"sqlAllowSubQuery\":true,\"sqlAllowScalarSubQuery\":true,\"allowNewKeywords\":true,\"sqlAllowLike\":true,\"sqlAllowGroupByClause\":true,\"maxSpatialQueryCells\":12,\"spatialMaxGeometryPointCount\":256,\"sqlDisableOptimizationFlags\":0,\"sqlAllowTop\":true,\"enableSpatialIndexing\":true}"
        }
        """;

    private string GetCollectionMetadata()
    {
        var paths = new JArray(_container.PartitionKeyPaths.Select(path => (JToken)path));
        var policy = _container.IndexingPolicy;
        var includedPaths = new JArray(policy.IncludedPaths.Select(p => new JObject { ["path"] = p.Path }));
        var excludedPaths = new JArray(policy.ExcludedPaths.Select(p => new JObject { ["path"] = p.Path }));
        if (excludedPaths.Count == 0)
            excludedPaths = new JArray(new JObject { ["path"] = "/\"_etag\"/?" });
        var metadata = new JObject
        {
            ["id"] = _container.Id,
            ["_rid"] = _collectionRid,
            ["_self"] = $"dbs/{_databaseRid}/colls/{_collectionRid}/",
            ["_etag"] = "\"00000000-0000-0000-0000-000000000000\"",
            ["_ts"] = 1700000000,
            ["partitionKey"] = new JObject
            {
                ["paths"] = paths,
                ["kind"] = "Hash",
                ["version"] = 2
            },
            ["indexingPolicy"] = new JObject
            {
                ["indexingMode"] = policy.IndexingMode.ToString().ToLowerInvariant(),
                ["automatic"] = policy.Automatic,
                ["includedPaths"] = includedPaths,
                ["excludedPaths"] = excludedPaths
            },
            ["geospatialConfig"] = new JObject { ["type"] = "Geography" }
        };
        return metadata.ToString(Formatting.None);
    }

    private string GetPartitionKeyRanges()
    {
        var ranges = new JArray();
        var step = 0x1_0000_0000L / _partitionKeyRangeCount;
        for (var i = 0; i < _partitionKeyRangeCount; i++)
        {
            var minInclusive = PartitionKeyHash.RangeBoundaryToHex(i * step);
            var maxExclusive = i == _partitionKeyRangeCount - 1 ? "FF" : PartitionKeyHash.RangeBoundaryToHex((i + 1) * step);
            ranges.Add(new JObject
            {
                ["id"] = i.ToString(),
                ["_rid"] = _collectionRid,
                ["minInclusive"] = minInclusive,
                ["maxExclusive"] = maxExclusive,
                ["throughputFraction"] = 1.0 / _partitionKeyRangeCount,
                ["status"] = "online",
                ["_self"] = $"dbs/{_databaseRid}/colls/{_collectionRid}/pkranges/{i}/",
                ["_ts"] = 1700000000,
                ["_etag"] = PkRangesEtag.Trim('"')
            });
        }

        var result = new JObject
        {
            ["_rid"] = _collectionRid,
            ["PartitionKeyRanges"] = ranges,
            ["_count"] = _partitionKeyRangeCount
        };
        return result.ToString(Formatting.None);
    }

    /// <summary>
    /// Creates a routing <see cref="HttpMessageHandler"/> that dispatches requests to
    /// different <see cref="FakeCosmosHandler"/> instances based on the container name
    /// in the URL path. This enables a single <see cref="CosmosClient"/> to query
    /// multiple in-memory containers.
    /// </summary>
    /// <param name="handlers">
    /// A dictionary mapping container names to their handlers. The first entry is used
    /// as the default for account-level requests (e.g. GET /).
    /// </param>
    public static HttpMessageHandler CreateRouter(
        IReadOnlyDictionary<string, FakeCosmosHandler> handlers)
    {
        return new RoutingHandler(handlers);
    }

    /// <summary>
    /// Runs a self-test to verify that the current Cosmos SDK version still uses the
    /// HTTP contract this handler expects (URL patterns, header names, response formats,
    /// ORDER BY wrapping, pagination, aggregates). Call this once during test suite setup
    /// to detect SDK breaking changes early, rather than getting silent data corruption.
    /// </summary>
    public static async Task VerifySdkCompatibilityAsync()
    {
        var sdkVersion = typeof(CosmosClient).Assembly.GetName().Version?.ToString() ?? "unknown";

        var container = new InMemoryContainer("compat-check", "/partitionKey");
        await container.CreateItemAsync(
            new CompatibilityDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
            new PartitionKey("pk"));
        await container.CreateItemAsync(
            new CompatibilityDocument { Id = "2", PartitionKey = "pk", Name = "Bob", Value = 20 },
            new PartitionKey("pk"));
        await container.CreateItemAsync(
            new CompatibilityDocument { Id = "3", PartitionKey = "pk", Name = "Charlie", Value = 30 },
            new PartitionKey("pk"));

        using var handler = new FakeCosmosHandler(container);
        using var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                RequestTimeout = TimeSpan.FromSeconds(10),
                HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
            });

        var cosmosContainer = client.GetContainer("fakeDb", "compat-check");

        var allItems = await DrainFeedIteratorAsync(
            cosmosContainer.GetItemLinqQueryable<CompatibilityDocument>().ToFeedIterator());
        if (allItems.Count != 3)
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): expected 3 items from basic query but got {allItems.Count}. " +
                "The Cosmos SDK may have changed its internal HTTP contract.");
        }

        var ordered = await DrainFeedIteratorAsync(
            cosmosContainer.GetItemLinqQueryable<CompatibilityDocument>()
                .OrderBy(document => document.Name)
                .ToFeedIterator());
        if (ordered.Count != 3 || ordered[0].Name != "Alice" || ordered[2].Name != "Charlie")
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): ORDER BY query returned unexpected results. " +
                "The Cosmos SDK ORDER BY response format may have changed.");
        }

        var filtered = await DrainFeedIteratorAsync(
            cosmosContainer.GetItemLinqQueryable<CompatibilityDocument>()
                .Where(document => document.Value > 15)
                .ToFeedIterator());
        if (filtered.Count != 2)
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): expected 2 items from filtered query but got {filtered.Count}. " +
                "The Cosmos SDK may have changed its query or header format.");
        }

        var paginatedItems = new List<CompatibilityDocument>();
        var pageIterator = cosmosContainer.GetItemLinqQueryable<CompatibilityDocument>(
                requestOptions: new QueryRequestOptions { MaxItemCount = 1 })
            .ToFeedIterator();
        var pageCount = 0;
        while (pageIterator.HasMoreResults)
        {
            var page = await pageIterator.ReadNextAsync();
            paginatedItems.AddRange(page);
            pageCount++;
        }

        if (paginatedItems.Count != 3)
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): paginated query returned {paginatedItems.Count} items instead of 3. " +
                "The Cosmos SDK may have changed its pagination or continuation token format.");
        }

        if (pageCount < 2)
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): expected multiple pages with MaxItemCount=1 but got {pageCount} page(s). " +
                "The Cosmos SDK may have changed how it sends the x-ms-max-item-count header.");
        }

        var countResult = await cosmosContainer.GetItemLinqQueryable<CompatibilityDocument>().CountAsync();
        if (countResult.Resource != 3)
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): CountAsync returned {countResult.Resource} instead of 3. " +
                "The Cosmos SDK may have changed its aggregate query format.");
        }

        if (!handler.RequestLog.Any(entry => entry.Contains("/pkranges")))
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): no partition key range request was detected. " +
                "The Cosmos SDK may have changed how it discovers partition key ranges.");
        }

        if (!handler.QueryLog.Any())
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): no query was logged. " +
                "The Cosmos SDK may have changed how it sends query requests.");
        }

        // CRUD roundtrip — verifies that create/read/delete HTTP routes work through the SDK
        var crudDoc = new CompatibilityDocument { Id = "crud-test", PartitionKey = "pk", Name = "CrudTest", Value = 99 };
        var createResponse = await cosmosContainer.CreateItemAsync(crudDoc, new PartitionKey("pk"));
        if (createResponse.StatusCode != HttpStatusCode.Created)
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): CreateItemAsync returned {createResponse.StatusCode} instead of Created. " +
                "The Cosmos SDK may have changed its CRUD HTTP contract.");
        }

        var readResponse = await cosmosContainer.ReadItemAsync<CompatibilityDocument>("crud-test", new PartitionKey("pk"));
        if (readResponse.Resource?.Name != "CrudTest")
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): ReadItemAsync returned unexpected resource. " +
                "The Cosmos SDK may have changed its point-read HTTP contract.");
        }

        var deleteResponse = await cosmosContainer.DeleteItemAsync<CompatibilityDocument>("crud-test", new PartitionKey("pk"));
        if (deleteResponse.StatusCode != HttpStatusCode.NoContent)
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): DeleteItemAsync returned {deleteResponse.StatusCode} instead of NoContent. " +
                "The Cosmos SDK may have changed its delete HTTP contract.");
        }
    }

    private static async Task<List<T>> DrainFeedIteratorAsync<T>(FeedIterator<T> iterator)
    {
        var items = new List<T>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            items.AddRange(page);
        }

        return items;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _queryResultCache.Clear();
        }

        base.Dispose(disposing);
    }

    private sealed class RoutingHandler : HttpMessageHandler
    {
        private readonly Dictionary<string, FakeCosmosHandler> _handlers;
        private readonly FakeCosmosHandler _default;

        public RoutingHandler(IReadOnlyDictionary<string, FakeCosmosHandler> handlers)
        {
            if (!handlers.Any())
                throw new ArgumentException("At least one handler must be registered with CreateRouter().", nameof(handlers));
            _handlers = new(handlers, StringComparer.Ordinal);
            _default = handlers.Values.First();
        }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var path = request.RequestUri?.AbsolutePath ?? "";

            if (path is "/" or "")
            {
                return InvokeHandlerAsync(_default, request, cancellationToken);
            }

            var match = Regex.Match(path, @"/dbs/[^/]+/colls/([^/]+)");
            if (match.Success)
            {
                var containerName = match.Groups[1].Value;
                if (_handlers.TryGetValue(containerName, out var handler))
                {
                    return InvokeHandlerAsync(handler, request, cancellationToken);
                }

                // SDK internal routes use base64-encoded RIDs (e.g. "AQAAAA==") for
                // partition key range and other metadata requests. Fall back to the
                // default handler for these rather than throwing.
                if (containerName.Contains('=') || path.Contains("/pkranges"))
                {
                    return InvokeHandlerAsync(_default, request, cancellationToken);
                }

                throw new InvalidOperationException(
                    $"Container '{containerName}' is not registered with CreateRouter(). " +
                    $"Registered containers: {string.Join(", ", _handlers.Keys.OrderBy(k => k))}. " +
                    $"Add it to the dictionary passed to FakeCosmosHandler.CreateRouter().");
            }

            return InvokeHandlerAsync(_default, request, cancellationToken);
        }

        private static Task<HttpResponseMessage> InvokeHandlerAsync(
            FakeCosmosHandler handler, HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var invoker = new HttpMessageInvoker(handler, disposeHandler: false);
            return invoker.SendAsync(request, cancellationToken);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                foreach (var handler in _handlers.Values)
                {
                    handler.Dispose();
                }
            }

            base.Dispose(disposing);
        }
    }

    /// <summary>
    /// Thread-safe query result cache with TTL-based eviction and bounded size.
    /// Prevents unbounded memory growth when consumers abandon mid-page iteration.
    /// </summary>
    private sealed class QueryResultCache
    {
        private readonly ConcurrentDictionary<string, CacheEntry> _entries = new();
        private readonly TimeSpan _ttl;
        private readonly int _maxEntries;

        public QueryResultCache(TimeSpan ttl, int maxEntries)
        {
            _ttl = ttl;
            _maxEntries = maxEntries;
        }

        public bool TryGet(string key, out List<JToken> value)
        {
            EvictStale();
            if (_entries.TryGetValue(key, out var entry) && !entry.IsExpired(_ttl))
            {
                entry.Touch();
                value = entry.Items;
                return true;
            }

            _entries.TryRemove(key, out _);
            value = null!;
            return false;
        }

        public void Set(string key, List<JToken> items)
        {
            EvictStale();
            _entries[key] = new CacheEntry(items);
        }

        public void Remove(string key)
        {
            _entries.TryRemove(key, out _);
        }

        public void Clear()
        {
            _entries.Clear();
        }

        private void EvictStale()
        {
            var keysToRemove = _entries
                .Where(pair => pair.Value.IsExpired(_ttl))
                .Select(pair => pair.Key)
                .ToList();

            foreach (var key in keysToRemove)
            {
                _entries.TryRemove(key, out _);
            }

            // If still over capacity after TTL eviction, remove oldest entries
            if (_entries.Count > _maxEntries)
            {
                var excess = _entries
                    .OrderBy(pair => pair.Value.LastAccessed)
                    .Take(_entries.Count - _maxEntries)
                    .Select(pair => pair.Key)
                    .ToList();

                foreach (var key in excess)
                {
                    _entries.TryRemove(key, out _);
                }
            }
        }

        private sealed class CacheEntry
        {
            public List<JToken> Items { get; }
            public DateTime CreatedAt { get; }
            public DateTime LastAccessed { get; private set; }

            public CacheEntry(List<JToken> items)
            {
                Items = items;
                CreatedAt = DateTime.UtcNow;
                LastAccessed = DateTime.UtcNow;
            }

            public bool IsExpired(TimeSpan ttl) => DateTime.UtcNow - CreatedAt > ttl;

            public void Touch() => LastAccessed = DateTime.UtcNow;
        }
    }
}
