using Newtonsoft.Json.Linq;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// Engine for executing JavaScript trigger bodies.
/// Implemented by the optional <c>CosmosDB.InMemoryEmulator.JsTriggers</c> package.
/// </summary>
public interface IJsTriggerEngine
{
    /// <summary>
    /// Executes a pre-trigger JavaScript body. Returns the (possibly modified) document.
    /// </summary>
    JObject ExecutePreTrigger(string jsBody, JObject document);

    /// <summary>
    /// Executes a post-trigger JavaScript body with read-only access to the committed document.
    /// </summary>
    void ExecutePostTrigger(string jsBody, JObject document);
}
