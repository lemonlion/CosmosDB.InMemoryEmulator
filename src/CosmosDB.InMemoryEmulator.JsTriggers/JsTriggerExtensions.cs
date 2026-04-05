namespace CosmosDB.InMemoryEmulator.JsTriggers;

/// <summary>
/// Extension methods for configuring JavaScript support on <see cref="InMemoryContainer"/>.
/// </summary>
public static class JsTriggerExtensions
{
    /// <summary>
    /// Enables JavaScript trigger body interpretation using the Jint engine.
    /// Call this on an <see cref="InMemoryContainer"/> to allow triggers registered
    /// via <c>CreateTriggerAsync</c> (with a JS body) to execute.
    /// </summary>
    public static InMemoryContainer UseJsTriggers(this InMemoryContainer container)
    {
        container.JsTriggerEngine = new JintTriggerEngine();
        return container;
    }

    /// <summary>
    /// Enables JavaScript stored procedure execution using the Jint engine.
    /// Call this on an <see cref="InMemoryContainer"/> to allow stored procedures created
    /// via <c>CreateStoredProcedureAsync</c> (with a JS body) to execute when no C# handler is registered.
    /// </summary>
    public static InMemoryContainer UseJsStoredProcedures(this InMemoryContainer container)
    {
        container.SprocEngine = new JintSprocEngine();
        return container;
    }
}
