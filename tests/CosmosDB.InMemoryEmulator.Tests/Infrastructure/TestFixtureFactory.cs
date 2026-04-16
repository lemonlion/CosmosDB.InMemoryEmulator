namespace CosmosDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Creates the appropriate <see cref="ITestContainerFixture"/> based on
/// the <c>COSMOS_TEST_TARGET</c> environment variable.
/// </summary>
public static class TestFixtureFactory
{
    /// <summary>
    /// Creates a fixture for the target specified by <c>COSMOS_TEST_TARGET</c>.
    /// Defaults to <see cref="TestTarget.InMemory"/> when the variable is unset.
    /// </summary>
    public static ITestContainerFixture Create()
    {
        var target = Environment.GetEnvironmentVariable("COSMOS_TEST_TARGET")?.ToLowerInvariant() switch
        {
            "emulator-linux" => TestTarget.EmulatorLinux,
            _ => TestTarget.InMemory
        };

        return target == TestTarget.InMemory
            ? new InMemoryTestFixture()
            : new EmulatorTestFixture();
    }
}
