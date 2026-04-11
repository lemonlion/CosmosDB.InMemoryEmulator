#nullable disable
using System.Text;
using System.Text.Json;
using Microsoft.Azure.Cosmos;

namespace CosmosDB.InMemoryEmulator;

internal sealed class InMemoryStreamFeedIterator : FeedIterator
{
    private readonly Func<IReadOnlyList<object>> _itemsFactory;
    private readonly string _wrapperProperty;
    private bool _hasMoreResults = true;

    public InMemoryStreamFeedIterator(Func<IReadOnlyList<object>> itemsFactory, string wrapperProperty)
    {
        _itemsFactory = itemsFactory;
        _wrapperProperty = wrapperProperty;
    }

    public override bool HasMoreResults => _hasMoreResults;

    public override Task<ResponseMessage> ReadNextAsync(CancellationToken cancellationToken = default)
    {
        _hasMoreResults = false;

        var items = _itemsFactory();
        var json = JsonSerializer.Serialize(
            new Dictionary<string, object> { [_wrapperProperty] = items },
            new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

        var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        var response = new ResponseMessage(System.Net.HttpStatusCode.OK) { Content = stream };
        response.Headers["x-ms-activity-id"] = Guid.NewGuid().ToString();
        response.Headers["x-ms-request-charge"] = "1";
        response.Headers["x-ms-session-token"] = "0:0#1";
        response.Headers["x-ms-item-count"] = items.Count.ToString();
        return Task.FromResult(response);
    }
}
