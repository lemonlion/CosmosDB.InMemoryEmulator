using System.Net;

namespace CosmosDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Cached check for whether a Cosmos DB emulator is reachable at localhost:8081.
/// </summary>
public static class EmulatorDetector
{
    private static readonly Lazy<bool> IsAvailableLazy = new(() =>
    {
        try
        {
            using var handler = new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = (_, _, _, _) => true
            };
            using var http = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(3) };
            var result = http.GetAsync("https://localhost:8081/").GetAwaiter().GetResult();
            return result.StatusCode is HttpStatusCode.OK or HttpStatusCode.Unauthorized;
        }
        catch
        {
            return false;
        }
    });

    /// <summary>True if a Cosmos DB emulator is responding on localhost:8081.</summary>
    public static bool IsAvailable => IsAvailableLazy.Value;
}
