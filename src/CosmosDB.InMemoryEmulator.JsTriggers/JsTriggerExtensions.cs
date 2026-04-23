namespace CosmosDB.InMemoryEmulator.JsTriggers;

/// <summary>
/// Extension methods for configuring JavaScript support on <see cref="IContainerTestSetup"/>.
/// </summary>
public static class JsTriggerExtensions
{
    /// <summary>
    /// Enables JavaScript trigger body interpretation using the Jint engine.
    /// Call this on an <see cref="IContainerTestSetup"/> to allow triggers registered
    /// via <c>CreateTriggerAsync</c> (with a JS body) to execute.
    /// </summary>
    // Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-use-stored-procedures-triggers-udfs
    //   "After you define one or more stored procedures, triggers, or UDFs, you can load and view them"
    //   Triggers must be registered before they can be executed. This enables the Jint-based JS engine
    //   for trigger body interpretation in the in-memory emulator.
    public static IContainerTestSetup UseJsTriggers(this IContainerTestSetup container)
    {
        container.JsTriggerEngine = new JintTriggerEngine();
        return container;
    }

    /// <summary>
    /// Enables JavaScript stored procedure execution using the Jint engine.
    /// Call this on an <see cref="IContainerTestSetup"/> to allow stored procedures created
    /// via <c>CreateStoredProcedureAsync</c> (with a JS body) to execute when no C# handler is registered.
    /// </summary>
    // Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-use-stored-procedures-triggers-udfs
    //   Stored procedures must be registered before execution. This enables the Jint-based JS engine
    //   for stored procedure execution in the in-memory emulator.
    public static IContainerTestSetup UseJsStoredProcedures(this IContainerTestSetup container)
    {
        container.SprocEngine = new JintSprocEngine();
        return container;
    }
}
