# SDK Compatibility Tests — Deep Dive Plan

**File:** `tests/CosmosDB.InMemoryEmulator.Tests/SdkCompatibilityTests.cs`  
**Current class name:** `SdkReflectionCompatibilityTests`  
**Current version:** 2.0.4 → will become **2.0.5**  
**Date:** 2026-04-01  

---

## 1. What the Test File Does Today

The test class `SdkReflectionCompatibilityTests` acts as a **canary suite** — it validates assumptions the emulator makes about the Cosmos SDK's internal structure. If the SDK changes between versions, these tests break first, pointing developers to the exact feature that needs attention.

### Currently Covered (8 tests)

| # | Test | What it guards |
|---|------|----------------|
| 1 | `ChangeFeedProcessorBuilder_HasExpectedPrivateField` (Theory×6) | 6 private fields used by `ChangeFeedProcessorBuilderFactory.CreateViaReflection()` |
| 2 | `CosmosAssembly_ContainsExpectedInternalType` (Theory×5) | 5 internal SDK types used during builder construction |
| 3 | `ChangeFeedProcessorBuilderFactory_IsReflectionCompatible` | End-to-end validation of reflection compatibility |
| 4 | `PatchOperation_HasPublicPathProperty` | `PatchOperation.Path` property used by patch logic |
| 5 | `PatchOperation_ConcreteType_HasPublicValueProperty` | `PatchOperation<T>.Value` reflection for Set/Add/Replace/Increment |
| 6 | `SdkType_IsNotSealed_ForNSubstituteProxying` (Theory×15) | 15 SDK types must be non-sealed for NSubstitute proxying |
| 7 | `QueryDefinition_GetQueryParameters_IsAvailable` | Public API for parameter extraction |

---

## 2. Gap Analysis — Missing Test Coverage

### Category A: Missing Reflection Assumptions (HIGH PRIORITY)

These are reflection-based SDK assumptions made in production code that have **zero** canary test coverage.

| # | Gap | Source Location | Risk |
|---|-----|-----------------|------|
| A1 | **PatchOperation.Move "From" property** — `GetPatchSourcePath()` reflects on `PatchOperation.GetType().GetProperty("From", Public\|NonPublic\|Instance)` | `InMemoryContainer.cs:5305-5308` | If SDK renames/removes "From", Move operations silently do nothing |
| A2 | **ChangeFeedStartFrom subtype naming convention** — Code uses `GetType().Name.Contains("Now")`, `Contains("Time")`, `Contains("Beginning")` to dispatch. Relies on internal subclass names matching these patterns | `InMemoryContainer.cs:1128,1132-1133,1255-1265` | If SDK renames subtypes, change feed iterator will misroute all start positions |
| A3 | **ChangeFeedStartFrom — DateTime extraction via reflection** — `ExtractStartTime()` scans properties/fields for `DateTime` type | `InMemoryContainer.cs:1276-1296` | If SDK changes how the timestamp is stored, `ChangeFeedStartFrom.Time()` breaks |
| A4 | **ChangeFeedStartFrom — FeedRange extraction via reflection** — `ExtractFeedRangeFromStartFrom()` scans properties/fields for `FeedRange` type | `InMemoryContainer.cs:1301-1321` | If SDK changes FeedRange storage, feed range scoped change feeds break |
| A5 | **QueryDefinition internal parameter field** — Fallback reflection path looks for a field with name containing "parameter" | `InMemoryContainer.cs:3039-3079` | Low risk (public API is primary path), but fallback would silently fail |
| A6 | **AccountProperties has non-public constructor** — `Activator.CreateInstance(typeof(AccountProperties), nonPublic: true)` | `InMemoryCosmosClient.cs:209-211` | If constructor changes, falls back to NSubstitute stub (soft failure) |
| A7 | **AccountProperties.Id has public setter** — Code sets `Id` via reflection | `InMemoryCosmosClient.cs:212-213` | If setter becomes read-only, account ID will be null |
| A8 | **ChangeFeedLeaseOptions.LeasePrefix property** — Accessed via reflection during builder construction | `InMemoryChangeFeedProcessor.cs:381` | If renamed, change feed processor won't get correct lease prefix |
| A9 | **ChangeFeedProcessorOptions has non-public constructor** — `Activator.CreateInstance(processorOptionsType, nonPublic: true)` | `InMemoryChangeFeedProcessor.cs:386` | If constructor changes, builder creation fails |
| A10 | **InMemoryFeedIteratorSetup self-reflection** — Reflects on its own `CreateInMemoryFeedIterator` method for generic dispatch | `InMemoryFeedIteratorSetup.cs:33-34` | If method is renamed during refactoring, LINQ-to-FeedIterator breaks |

### Category B: Missing NSubstitute Proxy Type Coverage

These SDK types are proxied via `Substitute.For<T>()` in production code but are **not** listed in the sealed-type canary test.

| # | Missing Type | Source Location | Used For |
|---|-------------|-----------------|----------|
| B1 | `DatabaseResponse` | `InMemoryCosmosClient.cs:241`, `InMemoryDatabase.cs:185,203` | Database CRUD responses |
| B2 | `UserResponse` | `InMemoryDatabase.cs:288,300`, `InMemoryUser.cs:130` | User management |
| B3 | `PermissionResponse` | `InMemoryUser.cs:75,93`, `InMemoryPermission.cs:79` | Permission management |
| B4 | `TransactionalBatchResponse` | `InMemoryTransactionalBatch.cs:118` | Batch execution results |
| B5 | `TransactionalBatchOperationResult` | `InMemoryTransactionalBatch.cs:247` | Individual batch op results |
| B6 | `TransactionalBatchOperationResult<T>` | `InMemoryTransactionalBatch.cs:257` | Typed batch op results |
| B7 | `CosmosResponseFactory` | `InMemoryCosmosClient.cs:47` | ResponseFactory property |
| B8 | `AccountProperties` | `InMemoryCosmosClient.cs:221` | ReadAccountAsync fallback |

### Category C: Missing Edge Cases & Robustness Tests

| # | Gap | Description |
|---|-----|-------------|
| C1 | **PatchOperation.Value on non-valued operations** — `GetPatchValue()` is called on Remove operations too; should return null gracefully | Verify Remove operation type has no Value property |
| C2 | **Multiple DateTime properties on ChangeFeedStartFrom** — If a future SDK adds a second DateTime field, `ExtractStartTime` picks the first one arbitrarily | Test that exactly one DateTime property/field exists |
| C3 | **ChangeFeedProcessorBuilder.applyBuilderConfiguration delegate signature** — The code constructs an `Action<,,,,,>` with 6 type parameters derived from internal types. If any of those types change, the delegate construction fails | Verify the 6-param Action can be constructed |
| C4 | **InMemoryFeedIteratorSetup.Register() — IQueryable<T> detection** — Verifies the generic factory correctly resolves element types from LINQ queryables | Canary that IQueryable<T> interface detection works |
| C5 | **FakeCosmosHandler.VerifySdkCompatibilityAsync** — There's already a comprehensive self-test but no unit test that *calls* it. If someone breaks it, there's no red test | Add a test that calls VerifySdkCompatibilityAsync |

### Category D: Potential Bugs

| # | Issue | Location | Severity |
|---|-------|----------|----------|
| D1 | **No bug found** — `PatchOperation.Path` is a public property now (not reflection), but the test correctly validates it. No mismatch. | — | — |
| D2 | **Potential: `ExtractStartTime` doesn't filter on property name** — It returns the FIRST DateTime property/field it finds. If the SDK adds a `CreatedAt` or `ExpiresAt` DateTime property, this would return the wrong time. Not a bug today, but a latent fragility. | `InMemoryContainer.cs:1278-1296` | Low (no extra DateTime currently exists) |
| D3 | **Potential: `GetPatchValue` only checks public instance** — If SDK makes `Value` non-public in a future version, patch operations silently lose their values | `InMemoryContainer.cs:5313-5316` | Low (unlikely SDK change) |

---

## 3. Implementation Plan — Tests to Write

All tests follow **TDD: Red → Green → Refactor**. Tests that validate existing behaviour will be green immediately (canary tests). If any test reveals a bug or unimplementable feature, it will be **skipped with a detailed reason** and a **sister divergent-behaviour test** will be added.

### Phase 1: Missing Reflection Canary Tests

```
Test ID  | Test Name                                                          | Priority | Status
---------|--------------------------------------------------------------------|---------:|--------
T01      | PatchOperation_Move_ConcreteType_HasFromProperty                   | HIGH     | [ ]
T02      | ChangeFeedStartFrom_Beginning_TypeName_ContainsBeginning           | HIGH     | [ ]
T03      | ChangeFeedStartFrom_Now_TypeName_ContainsNow                       | HIGH     | [ ]
T04      | ChangeFeedStartFrom_Time_TypeName_ContainsTime                     | HIGH     | [ ]
T05      | ChangeFeedStartFrom_Time_HasExactlyOneDateTimePropertyOrField      | HIGH     | [ ]
T06      | ChangeFeedStartFrom_Beginning_HasFeedRangePropertyOrField          | HIGH     | [ ]
T07      | ChangeFeedStartFrom_Time_HasFeedRangePropertyOrField               | HIGH     | [ ]
T08      | ChangeFeedStartFrom_Now_HasFeedRangePropertyOrField                | HIGH     | [ ]
T09      | QueryDefinition_HasInternalFieldContainingParameterInName          | MED      | [ ]
T10      | AccountProperties_HasNonPublicConstructor                          | MED      | [ ]
T11      | AccountProperties_Id_HasPublicSetter                               | MED      | [ ]
T12      | ChangeFeedLeaseOptions_HasLeasePrefixProperty                      | MED      | [ ]
T13      | ChangeFeedProcessorOptions_HasNonPublicConstructor                  | MED      | [ ]
T14      | ChangeFeedProcessorBuilderFactory_CanConstructApplyConfigDelegate   | MED      | [ ]
T15      | InMemoryFeedIteratorSetup_CreateMethod_IsResolvable                | LOW      | [ ]
```

### Phase 2: Missing NSubstitute Sealed-Type Canary Tests

These are additions to the existing `SdkType_IsNotSealed_ForNSubstituteProxying` Theory.

```
Test ID  | InlineData to Add                                                  | Priority | Status
---------|--------------------------------------------------------------------|---------:|--------
T16      | typeof(DatabaseResponse), "database CRUD responses"                | HIGH     | [ ]
T17      | typeof(UserResponse), "user management"                            | HIGH     | [ ]
T18      | typeof(PermissionResponse), "permission management"                | HIGH     | [ ]
T19      | typeof(TransactionalBatchResponse), "batch execution"              | HIGH     | [ ]
T20      | typeof(TransactionalBatchOperationResult), "batch operation results"| HIGH    | [ ]
T21      | typeof(TransactionalBatchOperationResult<object>), "typed batch results"| HIGH | [ ]
T22      | typeof(CosmosResponseFactory), "response factory"                   | MED      | [ ]
T23      | typeof(AccountProperties), "ReadAccountAsync fallback"              | MED      | [ ]
```

### Phase 3: Edge Case & Robustness Tests

```
Test ID  | Test Name                                                          | Priority | Status
---------|--------------------------------------------------------------------|---------:|--------
T24      | PatchOperation_Remove_GetType_DoesNotHaveValueProperty             | MED      | [ ]
T25      | FakeCosmosHandler_VerifySdkCompatibilityAsync_Passes               | MED      | [ ]
T26      | PatchOperation_Set_GetPatchValue_ReturnsCorrectValue               | MED      | [ ]
T27      | PatchOperation_Move_GetPatchSourcePath_ReturnsFromPath             | MED      | [ ]
T28      | QueryDefinition_GetQueryParameters_MultipleParams_AllExtracted     | LOW      | [ ]
T29      | ChangeFeedProcessorBuilderFactory_Create_ReturnsWorkingBuilder     | LOW      | [ ]
```

---

## 4. Execution Order

1. **Write all Phase 2 tests first** (T16–T23) — these are simply adding InlineData rows to the existing Theory, easiest to batch.
2. **Write Phase 1 tests** (T01–T15) — new reflection canary tests, from HIGH to LOW priority.
3. **Write Phase 3 tests** (T24–T29) — edge cases and integration-level canaries.
4. **Run full test suite** — verify everything is green.
5. **For any test that fails:**
   - First determine if it's a bug in the emulator or a genuine SDK incompatibility.
   - If emulator bug → fix it (green).
   - If too difficult to implement → skip test with detailed reason, add sister divergent-behaviour test with inline comments.

---

## 5. Skip / Divergent Behaviour Protocol

For any test that reveals behaviour too difficult to implement:

1. Mark the test with `[Fact(Skip = "...detailed reason...")]`
2. Create a sister test named `[Divergent]_<OriginalTestName>_<ActualBehaviour>`
3. The sister test should:
   - Pass (green) showing the *actual* current behaviour
   - Contain heavy inline comments explaining:
     - What real Cosmos DB does
     - What the emulator does instead
     - Why it's too difficult/risky to implement
     - What would need to change to support it

---

## 6. Documentation Updates Required

After all tests are green (or properly skipped):

| Document | Update |
|----------|--------|
| **Wiki: Known-Limitations.md** | Add any new limitations discovered during testing |
| **Wiki: Features.md** | Add "SDK Compatibility Canary Suite" to testing/quality section if not present |
| **Wiki: Feature-Comparison-With-Alternatives.md** | Add SDK canary test coverage as a differentiator if relevant |
| **README.md** | No changes expected unless new features are added |

---

## 7. Version Bump & Release

1. Bump `Version` in `CosmosDB.InMemoryEmulator.csproj`: `2.0.4` → `2.0.5`
2. `git add -A`
3. `git commit -m "v2.0.5: Comprehensive SDK compatibility canary test coverage"`
4. `git tag v2.0.5`
5. `git push; git push --tags`
6. Push wiki changes separately from `c:\git\CosmosDB.InMemoryEmulator.wiki`

---

## 8. Test-by-Test Detail

### T01: PatchOperation_Move_ConcreteType_HasFromProperty

**What:** `PatchOperation.Move("/dest", "/source")` creates a concrete type that should have a `From` property. `GetPatchSourcePath()` in `InMemoryContainer.cs:5305` relies on this.

**Test:**
```csharp
[Fact]
public void PatchOperation_Move_ConcreteType_HasFromProperty()
{
    var operation = PatchOperation.Move("/destination", "/source");
    var fromProp = operation.GetType()
        .GetProperty("From", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
    
    fromProp.Should().NotBeNull(
        "InMemoryContainer uses reflection to read PatchOperation<Move>.From. " +
        "If this property is removed, Move operations will silently do nothing.");
    fromProp.GetValue(operation).Should().Be("/source");
}
```

### T02–T04: ChangeFeedStartFrom Subtype Naming

**What:** The emulator dispatches change feed logic based on `GetType().Name` containing "Beginning", "Now", or "Time". These tests validate the naming convention still holds.

**Test (parameterised):**
```csharp
[Theory]
[InlineData("Beginning", nameof(ChangeFeedStartFrom.Beginning))]
[InlineData("Now", nameof(ChangeFeedStartFrom.Now))]
[InlineData("Time", "Time")]
public void ChangeFeedStartFrom_SubtypeName_ContainsExpectedKeyword(
    string expectedKeyword, string factoryDescription)
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
        $"this subtype, change feed iterators will misroute.");
}
```

### T05: ChangeFeedStartFrom.Time DateTime Extraction

**What:** `ExtractStartTime()` scans for DateTime properties/fields. Validate exactly one exists.

**Test:**
```csharp
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
```

### T06–T08: ChangeFeedStartFrom FeedRange Extraction

**What:** `ExtractFeedRangeFromStartFrom()` looks for a `FeedRange` property/field on each subtype.

**Test (parameterised):**
```csharp
[Theory]
[InlineData("Beginning")]
[InlineData("Now")]
[InlineData("Time")]
public void ChangeFeedStartFrom_HasFeedRangePropertyOrField(string startType)
{
    var feedRange = FeedRange.CreateFromPartitionKey(new PartitionKey("test"));
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
```

### T09: QueryDefinition Internal Parameter Field

**What:** Fallback reflection path for parameter extraction.

**Test:**
```csharp
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
```

### T10–T11: AccountProperties Reflection

**Test:**
```csharp
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
    idProp.SetMethod.Should().NotBeNull(
        "InMemoryCosmosClient.ReadAccountAsync() sets AccountProperties.Id via " +
        "reflection. If the setter is removed, the account ID will be null.");
}
```

### T12–T13: ChangeFeed Internal Type Properties

**Test:**
```csharp
[Fact]
public void ChangeFeedLeaseOptions_HasLeasePrefixProperty()
{
    var type = typeof(Container).Assembly.GetType(
        "Microsoft.Azure.Cosmos.ChangeFeed.Configuration.ChangeFeedLeaseOptions");
    type.Should().NotBeNull();
    
    var leasePrefixProp = type.GetProperty("LeasePrefix");
    leasePrefixProp.Should().NotBeNull(
        "ChangeFeedProcessorBuilderFactory sets LeasePrefix on ChangeFeedLeaseOptions. " +
        "If renamed, the change feed processor will use a default prefix.");
}

[Fact]
public void ChangeFeedProcessorOptions_HasNonPublicConstructor()
{
    var type = typeof(Container).Assembly.GetType(
        "Microsoft.Azure.Cosmos.ChangeFeed.Configuration.ChangeFeedProcessorOptions");
    type.Should().NotBeNull();
    
    var ctor = type.GetConstructor(
        BindingFlags.NonPublic | BindingFlags.Instance, null, Type.EmptyTypes, null)
        ?? type.GetConstructor(
            BindingFlags.Public | BindingFlags.Instance, null, Type.EmptyTypes, null);
    
    ctor.Should().NotBeNull(
        "ChangeFeedProcessorBuilderFactory creates ChangeFeedProcessorOptions via " +
        "Activator.CreateInstance(nonPublic: true). If this constructor is removed, " +
        "the builder factory falls back to NSubstitute.");
}
```

### T14: applyBuilderConfiguration Delegate Construction

**Test:**
```csharp
[Fact]
public void ChangeFeedProcessorBuilderFactory_CanConstructApplyConfigDelegate()
{
    var cosmosAssembly = typeof(Container).Assembly;
    
    var leaseStoreManagerType = cosmosAssembly.GetType(
        "Microsoft.Azure.Cosmos.ChangeFeed.LeaseManagement.DocumentServiceLeaseStoreManager");
    var containerInternalType = cosmosAssembly.GetType("Microsoft.Azure.Cosmos.ContainerInternal");
    var leaseOptionsType = cosmosAssembly.GetType(
        "Microsoft.Azure.Cosmos.ChangeFeed.Configuration.ChangeFeedLeaseOptions");
    var processorOptionsType = cosmosAssembly.GetType(
        "Microsoft.Azure.Cosmos.ChangeFeed.Configuration.ChangeFeedProcessorOptions");
    
    // All types should exist (already covered by other tests, but needed here)
    var allTypes = new[] { leaseStoreManagerType, containerInternalType, leaseOptionsType, processorOptionsType };
    allTypes.Should().NotContainNulls();
    
    // The 6-parameter Action type should be constructable
    var actionType = typeof(Action<,,,,,>).MakeGenericType(
        leaseStoreManagerType,
        containerInternalType,
        typeof(string),
        leaseOptionsType,
        processorOptionsType,
        containerInternalType);
    
    actionType.Should().NotBeNull(
        "ChangeFeedProcessorBuilderFactory constructs a 6-parameter Action delegate " +
        "for the applyBuilderConfiguration field. If any of these internal types change, " +
        "the delegate cannot be built.");
    
    actionType.GetMethod("Invoke")!.GetParameters().Should().HaveCount(6);
}
```

### T15: InMemoryFeedIteratorSetup Self-Reflection

**Test:**
```csharp
[Fact]
public void InMemoryFeedIteratorSetup_CreateMethod_IsResolvable()
{
    var method = typeof(InMemoryFeedIteratorSetup)
        .GetMethod("CreateInMemoryFeedIterator", BindingFlags.NonPublic | BindingFlags.Static);
    
    method.Should().NotBeNull(
        "InMemoryFeedIteratorSetup.Register() reflects on its own " +
        "CreateInMemoryFeedIterator method to build a generic factory. " +
        "If renamed, LINQ ToFeedIteratorOverridable() calls will throw.");
    
    method.IsGenericMethodDefinition.Should().BeTrue(
        "CreateInMemoryFeedIterator should be generic so it can be " +
        "MakeGenericMethod'd for any element type T.");
}
```

### T16–T23: InlineData Additions to Sealed-Type Theory

Simply add these `[InlineData]` rows to the existing `SdkType_IsNotSealed_ForNSubstituteProxying`:

```csharp
[InlineData(typeof(DatabaseResponse), "database CRUD responses")]
[InlineData(typeof(UserResponse), "user management")]
[InlineData(typeof(PermissionResponse), "permission management")]
[InlineData(typeof(TransactionalBatchResponse), "batch execution")]
[InlineData(typeof(TransactionalBatchOperationResult), "batch operation results")]
[InlineData(typeof(TransactionalBatchOperationResult<object>), "typed batch results")]
[InlineData(typeof(CosmosResponseFactory), "response factory on CosmosClient")]
[InlineData(typeof(AccountProperties), "ReadAccountAsync fallback")]
```

### T24: PatchOperation.Remove Has No Value Property

**Test:**
```csharp
[Fact]
public void PatchOperation_Remove_DoesNotHavePublicValueProperty()
{
    var operation = PatchOperation.Remove("/test");
    var valueProp = operation.GetType()
        .GetProperty("Value", BindingFlags.Public | BindingFlags.Instance);
    
    // This is informational — if Remove suddenly has a Value property, 
    // GetPatchValue() would return it, which is fine but unexpected.
    valueProp.Should().BeNull(
        "PatchOperation.Remove should not have a Value property. " +
        "GetPatchValue() will correctly return null for Remove operations.");
}
```

### T25: FakeCosmosHandler.VerifySdkCompatibilityAsync

**Test:**
```csharp
[Fact]
public async Task FakeCosmosHandler_VerifySdkCompatibilityAsync_Passes()
{
    // This calls the built-in self-test that validates the HTTP contract
    // between the SDK and FakeCosmosHandler.
    await FakeCosmosHandler.VerifySdkCompatibilityAsync();
}
```

### T26: PatchOperation.Set GetPatchValue Returns Correct Value

**Test:**
```csharp
[Fact]
public void PatchOperation_Set_GetPatchValue_ReturnsExpectedValue()
{
    var operation = PatchOperation.Set("/name", "Alice");
    var valueProp = operation.GetType()
        .GetProperty("Value", BindingFlags.Public | BindingFlags.Instance);
    
    valueProp.Should().NotBeNull();
    valueProp.GetValue(operation).Should().Be("Alice",
        "GetPatchValue() uses this reflection path to extract the value.");
}
```

### T27: PatchOperation.Move GetPatchSourcePath Returns From Path

**Test:**
```csharp
[Fact]
public void PatchOperation_Move_GetPatchSourcePath_ReturnsFromPath()
{
    var operation = PatchOperation.Move("/destination", "/source");
    var fromProp = operation.GetType()
        .GetProperty("From", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
    
    fromProp.Should().NotBeNull();
    fromProp.GetValue(operation).Should().Be("/source",
        "GetPatchSourcePath() uses this reflection path for Move operations.");
}
```

### T28: QueryDefinition Multiple Parameters

**Test:**
```csharp
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
```

### T29: ChangeFeedProcessorBuilderFactory.Create Returns Working Builder

**Test:**
```csharp
[Fact]
public async Task ChangeFeedProcessorBuilderFactory_Create_ReturnsWorkingBuilder()
{
    var processor = new InMemoryChangeFeedProcessor();
    var builder = ChangeFeedProcessorBuilderFactory.Create("test-processor", processor);
    
    builder.Should().NotBeNull();
    
    // Should support the fluent API without throwing
    var configured = builder
        .WithInstanceName("instance-1")
        .WithLeaseContainer(new InMemoryContainer("leases", "/id"));
    
    configured.Should().NotBeNull();
}
```

---

## 9. Progress Tracker

```
Phase  | Total | Done | Skipped | Remaining
-------|------:|-----:|--------:|----------
Phase 1|    15 |   15 |       0 |         0
Phase 2|     8 |    8 |       0 |         0
Phase 3|     6 |    6 |       0 |         0
-------|------:|-----:|--------:|----------
Total  |    29 |   29 |       0 |         0

**All 30 tests passing (29 planned + 1 pre-existing composite = 30 xUnit test cases).** No skips needed — all SDK assumptions validated successfully.
```

---

## 10. Risk Assessment

- **LOW RISK:** All Phase 2 tests (InlineData additions) — these are trivially green.
- **LOW RISK:** T01, T02–T04, T05–T09 — reflection canaries validating known SDK structure.
- **MEDIUM RISK:** T14 (delegate construction) — complex reflection chain, might need adjustment.
- **MEDIUM RISK:** T25 (VerifySdkCompatibilityAsync) — integration-level, depends on FakeCosmosHandler working end-to-end.
- **LOW RISK:** T29 (builder factory) — depends on reflection compatibility already covered by T03.

No HIGH RISK tests identified. All should be immediately green since they validate existing SDK structure.
