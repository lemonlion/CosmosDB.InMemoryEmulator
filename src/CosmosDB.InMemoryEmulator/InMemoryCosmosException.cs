#nullable disable
using System.Net;
using Microsoft.Azure.Cosmos;

namespace CosmosDB.InMemoryEmulator;

public sealed class InMemoryCosmosException : CosmosException
{
    private static readonly CosmosDiagnostics _diagnostics = new InMemoryExceptionDiagnostics();

    public InMemoryCosmosException(string message, HttpStatusCode statusCode, int subStatusCode, string activityId, double requestCharge)
        : base(message, statusCode, subStatusCode, activityId, requestCharge)
    {
    }

    public override CosmosDiagnostics Diagnostics => _diagnostics;

    private sealed class InMemoryExceptionDiagnostics : CosmosDiagnostics
    {
        public override TimeSpan GetClientElapsedTime() => TimeSpan.Zero;
        public override IReadOnlyList<(string regionName, Uri uri)> GetContactedRegions() => Array.Empty<(string, Uri)>();
        public override string ToString() => "{}";
    }
}
