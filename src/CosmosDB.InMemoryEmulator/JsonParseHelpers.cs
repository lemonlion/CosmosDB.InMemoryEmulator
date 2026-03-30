using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CosmosDB.InMemoryEmulator;

internal static class JsonParseHelpers
{
    internal static JObject ParseJson(string json)
    {
        using var reader = new JsonTextReader(new StringReader(json))
        {
            DateParseHandling = DateParseHandling.None
        };
        return JObject.Load(reader);
    }

    internal static JToken ParseJsonToken(string json)
    {
        using var reader = new JsonTextReader(new StringReader(json))
        {
            DateParseHandling = DateParseHandling.None
        };
        return JToken.Load(reader);
    }
}
