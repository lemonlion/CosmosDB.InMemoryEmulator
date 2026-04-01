using System.Reflection;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using NSubstitute;
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

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 1: Missing Reflection Canary Tests
// ═══════════════════════════════════════════════════════════════════════════

public class PatchOperationReflectionTests
{
    [Fact]
    public void PatchOperation_Move_ConcreteType_HasFromProperty()
    {
        var operation = PatchOperation.Move("/source", "/destination");
        var fromProp = operation.GetType()
            .GetProperty("From", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);

        fromProp.Should().NotBeNull(
            "InMemoryContainer uses reflection to read PatchOperation<Move>.From. " +
            "If this property is removed, Move operations will silently do nothing.");
        fromProp!.GetValue(operation).Should().Be("/source");
    }

    [Fact]
    public void PatchOperation_Set_GetPatchValue_ReturnsExpectedValue()
    {
        var operation = PatchOperation.Set("/name", "Alice");
        var valueProp = operation.GetType()
            .GetProperty("Value", BindingFlags.Public | BindingFlags.Instance);

        valueProp.Should().NotBeNull();
        valueProp!.GetValue(operation).Should().Be("Alice",
            "GetPatchValue() uses this reflection path to extract the value.");
    }

    [Fact]
    public void PatchOperation_Move_GetPatchSourcePath_ReturnsFromPath()
    {
        var operation = PatchOperation.Move("/source", "/destination");
        var fromProp = operation.GetType()
            .GetProperty("From", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);

        fromProp.Should().NotBeNull();
        fromProp!.GetValue(operation).Should().Be("/source",
            "GetPatchSourcePath() uses this reflection path for Move operations.");
    }

    [Fact]
    public void PatchOperation_Remove_DoesNotHavePublicValueProperty()
    {
        var operation = PatchOperation.Remove("/test");
        var valueProp = operation.GetType()
            .GetProperty("Value", BindingFlags.Public | BindingFlags.Instance);

        valueProp.Should().BeNull(
            "PatchOperation.Remove should not have a Value property. " +
            "GetPatchValue() will correctly return null for Remove operations.");
    }
}

public class ChangeFeedStartFromReflectionTests
{
    [Theory]
    [InlineData("Beginning")]
    [InlineData("Now")]
    [InlineData("Time")]
    public void ChangeFeedStartFrom_SubtypeName_ContainsExpectedKeyword(string expectedKeyword)
    {
        var startFrom = expectedKeyword switch
        {
            "Beginning" => ChangeFeedStartFrom.Beginning(),
            "Now" => ChangeFeedStartFrom.Now(),
            "Time" => ChangeFeedStartFrom.Time(DateTime.UtcNow),
            _ => throw new ArgumentException(expectedKeyword)
        };

        startFrom.GetType().Name.Should().Contain(expectedKeyword,
            $"InMemoryContainer dispatches change feed behaviour based on " +
            $"GetType().Name containing '{expectedKeyword}'. If the SDK renames " +
            "this subtype, change feed iterators will misroute.");
    }

    [Fact]
    public void ChangeFeedStartFrom_Time_HasDateTimePropertyOrField()
    {
        var startFrom = ChangeFeedStartFrom.Time(new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        var type = startFrom.GetType();

        var dateTimeMembers = type
            .GetProperties(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.PropertyType == typeof(DateTime))
            .Cast<MemberInfo>()
            .Concat(type
                .GetFields(BindingFlags.NonPublic | BindingFlags.Instance)
                .Where(f => f.FieldType == typeof(DateTime)))
            .ToList();

        dateTimeMembers.Should().NotBeEmpty(
            "InMemoryContainer.ExtractStartTime() uses reflection to find a DateTime " +
            "property or field on ChangeFeedStartFrom.Time subtypes. If none exist, " +
            "time-based change feed filtering will not work.");
    }

    [Theory]
    [InlineData("Beginning")]
    [InlineData("Now")]
    [InlineData("Time")]
    public async Task ChangeFeedStartFrom_HasFeedRangePropertyOrField(string startType)
    {
        var container = new InMemoryContainer("feed-range-test", "/pk");
        var ranges = await container.GetFeedRangesAsync();
        var feedRange = ranges.First();
        var startFrom = startType switch
        {
            "Beginning" => ChangeFeedStartFrom.Beginning(feedRange),
            "Now" => ChangeFeedStartFrom.Now(feedRange),
            "Time" => ChangeFeedStartFrom.Time(DateTime.UtcNow, feedRange),
            _ => throw new ArgumentException(startType)
        };

        var type = startFrom.GetType();
        var feedRangeMembers = type
            .GetProperties(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance)
            .Where(p => typeof(FeedRange).IsAssignableFrom(p.PropertyType))
            .Cast<MemberInfo>()
            .Concat(type
                .GetFields(BindingFlags.NonPublic | BindingFlags.Instance)
                .Where(f => typeof(FeedRange).IsAssignableFrom(f.FieldType)))
            .ToList();

        feedRangeMembers.Should().NotBeEmpty(
            $"InMemoryContainer.ExtractFeedRangeFromStartFrom() uses reflection to find a " +
            $"FeedRange property/field on ChangeFeedStartFrom.{startType} subtypes. " +
            "If none exist, feed-range-scoped change feeds will ignore the range filter.");
    }
}

public class QueryDefinitionReflectionTests
{
    [Fact]
    public void QueryDefinition_HasInternalFieldContainingParameterInName()
    {
        var field = typeof(QueryDefinition)
            .GetFields(BindingFlags.NonPublic | BindingFlags.Instance)
            .FirstOrDefault(f => f.Name.Contains("parameter", StringComparison.OrdinalIgnoreCase));

        field.Should().NotBeNull(
            "InMemoryContainer uses a reflection fallback that looks for an internal field " +
            "with 'parameter' in its name on QueryDefinition. This is secondary to " +
            "GetQueryParameters() but guards against older SDK versions.");
    }

    [Fact]
    public void QueryDefinition_GetQueryParameters_MultipleParams_AllExtracted()
    {
        var query = new QueryDefinition("SELECT * FROM c WHERE c.name = @name AND c.age = @age")
            .WithParameter("@name", "Alice")
            .WithParameter("@age", 30);

        var parameters = query.GetQueryParameters();

        parameters.Should().HaveCount(2);
        parameters.Should().Contain(("@name", (object)"Alice"));
        parameters.Should().Contain(("@age", (object)30));
    }
}

public class AccountPropertiesReflectionTests
{
    [Fact]
    public void AccountProperties_HasNonPublicParameterlessConstructor()
    {
        var ctor = typeof(AccountProperties).GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance, null, Type.EmptyTypes, null);

        ctor.Should().NotBeNull(
            "InMemoryCosmosClient.ReadAccountAsync() creates AccountProperties via " +
            "Activator.CreateInstance(nonPublic: true). If the constructor is removed, " +
            "it falls back to NSubstitute.");
    }

    [Fact]
    public void AccountProperties_Id_HasPublicSettableProperty()
    {
        var idProp = typeof(AccountProperties).GetProperty(nameof(AccountProperties.Id));

        idProp.Should().NotBeNull();
        idProp!.SetMethod.Should().NotBeNull(
            "InMemoryCosmosClient.ReadAccountAsync() sets AccountProperties.Id via " +
            "reflection. If the setter is removed, the account ID will be null.");
    }
}

public class ChangeFeedInternalTypeReflectionTests
{
    private static readonly Assembly _cosmosAssembly = typeof(Container).Assembly;

    [Fact]
    public void ChangeFeedLeaseOptions_HasLeasePrefixProperty()
    {
        var type = _cosmosAssembly.GetType(
            "Microsoft.Azure.Cosmos.ChangeFeed.Configuration.ChangeFeedLeaseOptions");
        type.Should().NotBeNull();

        var leasePrefixProp = type!.GetProperty("LeasePrefix");
        leasePrefixProp.Should().NotBeNull(
            "ChangeFeedProcessorBuilderFactory sets LeasePrefix on ChangeFeedLeaseOptions. " +
            "If renamed, the change feed processor will use a default prefix.");
    }

    [Fact]
    public void ChangeFeedProcessorOptions_HasNonPublicConstructor()
    {
        var type = _cosmosAssembly.GetType(
            "Microsoft.Azure.Cosmos.ChangeFeed.Configuration.ChangeFeedProcessorOptions");
        type.Should().NotBeNull();

        var ctor = type!.GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance, null, Type.EmptyTypes, null)
            ?? type.GetConstructor(
                BindingFlags.Public | BindingFlags.Instance, null, Type.EmptyTypes, null);

        ctor.Should().NotBeNull(
            "ChangeFeedProcessorBuilderFactory creates ChangeFeedProcessorOptions via " +
            "Activator.CreateInstance(nonPublic: true). If this constructor is removed, " +
            "the builder factory falls back to NSubstitute.");
    }

    [Fact]
    public void ChangeFeedProcessorBuilderFactory_CanConstructApplyConfigDelegate()
    {
        var leaseStoreManagerType = _cosmosAssembly.GetType(
            "Microsoft.Azure.Cosmos.ChangeFeed.LeaseManagement.DocumentServiceLeaseStoreManager");
        var containerInternalType = _cosmosAssembly.GetType("Microsoft.Azure.Cosmos.ContainerInternal");
        var leaseOptionsType = _cosmosAssembly.GetType(
            "Microsoft.Azure.Cosmos.ChangeFeed.Configuration.ChangeFeedLeaseOptions");
        var processorOptionsType = _cosmosAssembly.GetType(
            "Microsoft.Azure.Cosmos.ChangeFeed.Configuration.ChangeFeedProcessorOptions");

        var allTypes = new[] { leaseStoreManagerType, containerInternalType, leaseOptionsType, processorOptionsType };
        allTypes.Should().NotContainNulls();

        var actionType = typeof(Action<,,,,,>).MakeGenericType(
            leaseStoreManagerType!,
            containerInternalType!,
            typeof(string),
            leaseOptionsType!,
            processorOptionsType!,
            containerInternalType!);

        actionType.Should().NotBeNull(
            "ChangeFeedProcessorBuilderFactory constructs a 6-parameter Action delegate " +
            "for the applyBuilderConfiguration field. If any of these internal types change, " +
            "the delegate cannot be built.");

        actionType.GetMethod("Invoke")!.GetParameters().Should().HaveCount(6);
    }
}

public class FeedIteratorSetupReflectionTests
{
    [Fact]
    public void InMemoryFeedIteratorSetup_CreateMethod_IsResolvable()
    {
        var method = typeof(InMemoryFeedIteratorSetup)
            .GetMethod("CreateInMemoryFeedIterator", BindingFlags.NonPublic | BindingFlags.Static);

        method.Should().NotBeNull(
            "InMemoryFeedIteratorSetup.Register() reflects on its own " +
            "CreateInMemoryFeedIterator method to build a generic factory. " +
            "If renamed, LINQ ToFeedIteratorOverridable() calls will throw.");

        method!.IsGenericMethodDefinition.Should().BeTrue(
            "CreateInMemoryFeedIterator should be generic so it can be " +
            "MakeGenericMethod'd for any element type T.");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 2: Missing NSubstitute Sealed-Type Canary Tests
// ═══════════════════════════════════════════════════════════════════════════

public class SdkSealedTypeCanaryTests
{
    [Theory]
    [InlineData(typeof(DatabaseResponse), "database CRUD responses")]
    [InlineData(typeof(UserResponse), "user management")]
    [InlineData(typeof(PermissionResponse), "permission management")]
    [InlineData(typeof(TransactionalBatchResponse), "batch execution")]
    [InlineData(typeof(TransactionalBatchOperationResult), "batch operation results")]
    [InlineData(typeof(TransactionalBatchOperationResult<object>), "typed batch results")]
    [InlineData(typeof(AccountProperties), "ReadAccountAsync fallback")]
    public void SdkType_IsNotSealed_ForNSubstituteProxying(Type sdkType, string usedFor)
    {
        sdkType.IsSealed.Should().BeFalse(
            $"InMemoryContainer uses NSubstitute.For<{sdkType.Name}>() for {usedFor}. " +
            "If this type becomes sealed, NSubstitute cannot create a proxy and the " +
            "feature will need a concrete implementation or alternative approach.");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 3: Edge Case & Integration Canary Tests
// ═══════════════════════════════════════════════════════════════════════════

public class SdkCompatibilityIntegrationTests
{
    [Fact]
    public async Task FakeCosmosHandler_VerifySdkCompatibilityAsync_Passes()
    {
        await FakeCosmosHandler.VerifySdkCompatibilityAsync();
    }

    [Fact]
    public void ChangeFeedProcessorBuilderFactory_Create_ReturnsWorkingBuilder()
    {
        var processor = Substitute.For<ChangeFeedProcessor>();
        var builder = ChangeFeedProcessorBuilderFactory.Create("test-processor", processor);

        builder.Should().NotBeNull();

        // Validate builder supports fluent API — WithInstanceName doesn't need ContainerInternal
        var configured = builder.WithInstanceName("instance-1");
        configured.Should().NotBeNull();
    }
}
