#nullable disable
using System.Net;
using System.Reflection;
using Microsoft.Azure.Cosmos;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// Factory for creating <see cref="CosmosException"/> instances with a non-null
/// <see cref="CosmosException.Diagnostics"/> property.
/// <para>
/// Previous versions threw a subclass (<c>InMemoryCosmosException</c>) which broke
/// <c>Assert.ThrowsAsync&lt;CosmosException&gt;</c> and similar exact-type-match
/// assertions because the thrown type was not <see cref="CosmosException"/> itself.
/// This factory returns a plain <see cref="CosmosException"/> so that all standard
/// SDK error-handling patterns work unchanged.
/// </para>
/// </summary>
public static class InMemoryCosmosException
{
    private static readonly CosmosDiagnostics Diagnostics = new InMemoryExceptionDiagnostics();

    private static readonly FieldInfo DiagnosticsField =
        typeof(CosmosException).GetField("<Diagnostics>k__BackingField", BindingFlags.NonPublic | BindingFlags.Instance)
        ?? throw new MissingFieldException(nameof(CosmosException), "<Diagnostics>k__BackingField");

    /// <summary>
    /// Creates a new <see cref="CosmosException"/> with a non-null <see cref="CosmosDiagnostics"/>.
    /// </summary>
    public static CosmosException Create(string message, HttpStatusCode statusCode, int subStatusCode, string activityId, double requestCharge)
    {
        var ex = new CosmosException(message, statusCode, subStatusCode, activityId, requestCharge);
        DiagnosticsField.SetValue(ex, Diagnostics);
        return ex;
    }

    private sealed class InMemoryExceptionDiagnostics : CosmosDiagnostics
    {
        public override TimeSpan GetClientElapsedTime() => TimeSpan.Zero;
        public override IReadOnlyList<(string regionName, Uri uri)> GetContactedRegions() => Array.Empty<(string, Uri)>();
        public override string ToString() => "{}";
    }
}
