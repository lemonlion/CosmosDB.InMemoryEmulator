using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json.Linq;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// Provides test-only operations on a container: stored procedure, UDF, and trigger
/// registration, state import/export, point-in-time restore, and item reset.
/// Implemented by <see cref="InMemoryContainer"/>.
/// </summary>
public interface IContainerTestSetup
{
    // ─── C# delegate registrations ────────────────────────────────────────────

    /// <summary>
    /// Registers a stored procedure handler invoked by <c>ExecuteStoredProcedureAsync</c>.
    /// </summary>
    void RegisterStoredProcedure(string id, Func<PartitionKey, dynamic[], string> handler);

    /// <summary>
    /// Registers a user-defined function callable in SQL queries as <c>udf.name(args)</c>.
    /// </summary>
    void RegisterUdf(string name, Func<object[], object> handler);

    /// <summary>
    /// Registers a pre- or post-trigger handler.
    /// For pre-triggers, the handler receives and returns the (possibly modified) document.
    /// For post-triggers, wrap an <see cref="Action{JObject}"/> in a lambda that returns the input.
    /// </summary>
    void RegisterTrigger(string id, TriggerType type, TriggerOperation operation,
        Func<JObject, JObject> handler);

    // ─── JS body registrations (requires JsTriggers package) ──────────────────

    /// <summary>
    /// Registers a stored procedure from a JavaScript function body.
    /// Requires the <c>CosmosDB.InMemoryEmulator.JsTriggers</c> package.
    /// </summary>
    /// <exception cref="NotImplementedException">
    /// Thrown when the JsTriggers package is not installed.
    /// </exception>
    void RegisterStoredProcedure(string id, string jsBody);

    /// <summary>
    /// Registers a UDF from a JavaScript function body.
    /// Requires the <c>CosmosDB.InMemoryEmulator.JsTriggers</c> package.
    /// </summary>
    /// <exception cref="NotImplementedException">
    /// Thrown when the JsTriggers package is not installed.
    /// </exception>
    void RegisterUdf(string name, string jsBody);

    /// <summary>
    /// Registers a trigger from a JavaScript function body.
    /// Requires the <c>CosmosDB.InMemoryEmulator.JsTriggers</c> package.
    /// </summary>
    /// <exception cref="NotImplementedException">
    /// Thrown when the JsTriggers package is not installed.
    /// </exception>
    void RegisterTrigger(string id, TriggerType type, TriggerOperation operation,
        string jsBody);

    // ─── State management ─────────────────────────────────────────────────────

    /// <summary>Exports the current container state as a JSON string.</summary>
    string ExportState();

    /// <summary>Exports the current container state to a file.</summary>
    void ExportStateToFile(string path);

    /// <summary>Imports container state from a JSON string, replacing all existing data.</summary>
    void ImportState(string json);

    /// <summary>Imports container state from a file, replacing all existing data.</summary>
    void ImportStateFromFile(string path);

    /// <summary>
    /// Restores the container to its state at the specified point in time
    /// by replaying the change feed.
    /// </summary>
    void RestoreToPointInTime(DateTimeOffset timestamp);

    // ─── Reset ────────────────────────────────────────────────────────────────

    /// <summary>Removes all items, ETags, timestamps, and change feed entries.</summary>
    void ClearItems();
}
