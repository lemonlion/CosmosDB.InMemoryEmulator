using System.Reflection;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Canary tests that validate assumptions InMemoryContainer makes about the Cosmos SDK's
/// internal structure. If any of these fail after an SDK upgrade, the corresponding feature
/// will need attention. Each test name describes the assumption being validated.
/// </summary>
public class SdkReflectionCompatibilityTests
{
    private static readonly Assembly _cosmosAssembly = typeof(Container).Assembly;

    // ── ChangeFeedProcessorBuilder reflection assumptions ──

    [Theory]
    [InlineData("changeFeedProcessor")]
    [InlineData("isBuilt")]
    [InlineData("changeFeedLeaseOptions")]
    [InlineData("changeFeedProcessorOptions")]
    [InlineData("monitoredContainer")]
    [InlineData("applyBuilderConfiguration")]
    public void ChangeFeedProcessorBuilder_HasExpectedPrivateField(string fieldName)
    {
        typeof(ChangeFeedProcessorBuilder)
            .GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Instance)
            .Should().NotBeNull(
                $"InMemoryChangeFeedProcessor relies on private field '{fieldName}'. " +
                "If this field was renamed or removed, the reflection-based builder will " +
                "fall back to an NSubstitute stub that does not poll the change feed.");
    }

    [Theory]
    [InlineData("Microsoft.Azure.Cosmos.ChangeFeed.Configuration.ChangeFeedLeaseOptions")]
    [InlineData("Microsoft.Azure.Cosmos.ChangeFeed.Configuration.ChangeFeedProcessorOptions")]
    [InlineData("Microsoft.Azure.Cosmos.ContainerInlineCore")]
    [InlineData("Microsoft.Azure.Cosmos.ContainerInternal")]
    [InlineData("Microsoft.Azure.Cosmos.ChangeFeed.LeaseManagement.DocumentServiceLeaseStoreManager")]
    public void CosmosAssembly_ContainsExpectedInternalType(string typeName)
    {
        _cosmosAssembly.GetType(typeName)
            .Should().NotBeNull(
                $"InMemoryChangeFeedProcessor relies on internal type '{typeName}'. " +
                "If removed, the builder factory will fall back to an NSubstitute stub.");
    }

    [Fact]
    public void ChangeFeedProcessorBuilderFactory_IsReflectionCompatible()
    {
        ChangeFeedProcessorBuilderFactory.IsReflectionCompatible()
            .Should().BeTrue(
                "All reflection assumptions for ChangeFeedProcessorBuilder are met. " +
                "If this fails, the builder will fall back to a stub but change feed " +
                "processor tests relying on handler invocation may not work.");
    }

    // ── PatchOperation public API assumptions ──

    [Fact]
    public void PatchOperation_HasPublicPathProperty()
    {
        var operation = PatchOperation.Set("/test", "value");

        operation.Path.Should().Be("/test",
            "InMemoryContainer uses PatchOperation.Path directly. " +
            "If this property is removed, patch operations will break.");
    }

    [Fact]
    public void PatchOperation_ConcreteType_HasPublicValueProperty()
    {
        var operation = PatchOperation.Set("/name", "Alice");
        var valueProp = operation.GetType()
            .GetProperty("Value", BindingFlags.Public | BindingFlags.Instance);

        valueProp.Should().NotBeNull(
            "InMemoryContainer reads PatchOperation<T>.Value via reflection on the " +
            "concrete type. If this property is renamed or made non-public, patch " +
            "Set/Add/Replace/Increment operations will fail.");
    }

    // ── NSubstitute proxy-ability (types must remain non-sealed) ──

    [Theory]
    [InlineData(typeof(ItemResponse<object>), "CRUD responses")]
    [InlineData(typeof(CosmosDiagnostics), "all response diagnostics")]
    [InlineData(typeof(Scripts), "stored procedures, triggers, UDFs")]
    [InlineData(typeof(StoredProcedureResponse), "sproc creation")]
    [InlineData(typeof(StoredProcedureExecuteResponse<string>), "sproc execution")]
    [InlineData(typeof(TriggerResponse), "trigger creation")]
    [InlineData(typeof(UserDefinedFunctionResponse), "UDF creation")]
    [InlineData(typeof(ContainerResponse), "container management")]
    [InlineData(typeof(ThroughputResponse), "throughput operations")]
    [InlineData(typeof(Database), "Database property")]
    [InlineData(typeof(Conflicts), "Conflicts property")]
    [InlineData(typeof(FeedIterator), "stream feed iterators")]
    [InlineData(typeof(FeedRange), "feed ranges")]
    [InlineData(typeof(ChangeFeedEstimator), "change feed estimation")]
    [InlineData(typeof(ChangeFeedProcessorBuilder), "change feed processor fallback")]
    public void SdkType_IsNotSealed_ForNSubstituteProxying(Type sdkType, string usedFor)
    {
        sdkType.IsSealed.Should().BeFalse(
            $"InMemoryContainer uses NSubstitute.For<{sdkType.Name}>() for {usedFor}. " +
            "If this type becomes sealed, NSubstitute cannot create a proxy and the " +
            "feature will need a concrete implementation or alternative approach.");
    }

    // ── QueryDefinition parameter extraction ──

    [Fact]
    public void QueryDefinition_GetQueryParameters_IsAvailable()
    {
        var query = new QueryDefinition("SELECT * FROM c WHERE c.id = @id")
            .WithParameter("@id", "test");

        var parameters = query.GetQueryParameters();

        parameters.Should().ContainSingle()
            .Which.Should().Be(("@id", (object)"test"),
                "InMemoryContainer uses GetQueryParameters() as the primary path for " +
                "extracting parameterised query values. Reflection is only a fallback.");
    }
}
