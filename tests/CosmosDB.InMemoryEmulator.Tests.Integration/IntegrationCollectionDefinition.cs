using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// xUnit collection definition binding <see cref="EmulatorSession"/> as a
/// shared fixture for all integration tests. The collection name is referenced
/// via <see cref="IntegrationCollection.Name"/>.
/// </summary>
[CollectionDefinition(IntegrationCollection.Name)]
public sealed class IntegrationCollectionDefinition : ICollectionFixture<EmulatorSession>
{
}
