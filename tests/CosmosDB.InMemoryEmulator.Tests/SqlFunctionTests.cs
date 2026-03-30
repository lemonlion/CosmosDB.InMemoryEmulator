using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class SqlFunctionTests
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

    private async Task SeedItems()
    {
        var items = new[]
        {
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice Anderson", Value = 10, IsActive = true, Tags = ["dot", "net"] },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob Brown", Value = 20, IsActive = false, Tags = ["java"] },
            new TestDocument
            {
                Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30, IsActive = true, Tags = ["dot"],
                Nested = new NestedObject { Description = "nested value", Score = 3.14 }
            },
            new TestDocument { Id = "4", PartitionKey = "pk1", Name = "  diana  ", Value = 0, IsActive = true, Tags = [] },
            new TestDocument { Id = "5", PartitionKey = "pk1", Name = "Eve", Value = -5, IsActive = false, Tags = ["a", "b", "c"] },
        };
        foreach (var item in items)
        {
            await _container.CreateItemAsync(item, new PartitionKey(item.PartitionKey));
        }
    }

    [Fact]
    public async Task StartsWith_MatchesPrefix()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE STARTSWITH(c.name, @prefix)")
            .WithParameter("@prefix", "Ali");

        var results = await QueryAll<TestDocument>(query);

        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Alice Anderson");
    }

    [Fact]
    public async Task StartsWith_CaseInsensitive()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE STARTSWITH(c.name, @prefix, true)")
            .WithParameter("@prefix", "ali");

        var results = await QueryAll<TestDocument>(query);

        results.Should().HaveCount(1);
    }

    [Fact]
    public async Task EndsWith_MatchesSuffix()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE ENDSWITH(c.name, @suffix)")
            .WithParameter("@suffix", "Anderson");

        var results = await QueryAll<TestDocument>(query);

        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Alice Anderson");
    }

    [Fact]
    public async Task Contains_MatchesSubstring()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE CONTAINS(c.name, @sub)")
            .WithParameter("@sub", "Brown");

        var results = await QueryAll<TestDocument>(query);

        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Bob Brown");
    }

    [Fact]
    public async Task Contains_CaseInsensitive()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE CONTAINS(c.name, @sub, true)")
            .WithParameter("@sub", "brown");

        var results = await QueryAll<TestDocument>(query);

        results.Should().HaveCount(1);
    }

    [Fact]
    public async Task ArrayContains_MatchesElement()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE ARRAY_CONTAINS(c.tags, @tag)")
            .WithParameter("@tag", "dot");

        var results = await QueryAll<TestDocument>(query);

        results.Should().HaveCount(2);
    }

    [Fact]
    public async Task ArrayLength_FiltersOnLength()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE ARRAY_LENGTH(c.tags) > 1");

        var results = await QueryAll<TestDocument>(query);

        // Item 1: ["dot","net"] (2), Item 5: ["a","b","c"] (3)
        results.Should().HaveCount(2);
    }

    [Fact]
    public async Task IsDefined_ReturnsFalseForUndefinedProperty()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE IS_DEFINED(c.nonExistentProperty)");

        var results = await QueryAll<TestDocument>(query);

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task IsNull_ReturnsTrueForNullProperty()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE IS_NULL(c.nested)");

        var results = await QueryAll<TestDocument>(query);

        // Items 1,2,4,5 have null nested, item 3 has a nested object
        results.Should().HaveCount(4);
    }

    [Fact]
    public async Task StringConcat_ConcatenatesStrings()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT CONCAT(c.name, '-', c.id) AS combined FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["combined"]!.ToString().Should().Be("Alice Anderson-1");
    }

    [Fact]
    public async Task Lower_ConvertsToLowerCase()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT LOWER(c.name) AS lowerName FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["lowerName"]!.ToString().Should().Be("alice anderson");
    }

    [Fact]
    public async Task Upper_ConvertsToUpperCase()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT UPPER(c.name) AS upperName FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["upperName"]!.ToString().Should().Be("ALICE ANDERSON");
    }

    [Fact]
    public async Task Trim_RemovesWhitespace()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT TRIM(c.name) AS trimmedName FROM c WHERE c.id = '4'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["trimmedName"]!.ToString().Should().Be("diana");
    }

    [Fact]
    public async Task Ltrim_RemovesLeadingWhitespace()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT LTRIM(c.name) AS trimmedName FROM c WHERE c.id = '4'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["trimmedName"]!.ToString().Should().Be("diana  ");
    }

    [Fact]
    public async Task Rtrim_RemovesTrailingWhitespace()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT RTRIM(c.name) AS trimmedName FROM c WHERE c.id = '4'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["trimmedName"]!.ToString().Should().Be("  diana");
    }

    [Fact]
    public async Task Left_ReturnsLeftCharacters()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT LEFT(c.name, 3) AS prefix FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["prefix"]!.ToString().Should().Be("Ali");
    }

    [Fact]
    public async Task Right_ReturnsRightCharacters()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT RIGHT(c.name, 5) AS suffix FROM c WHERE c.id = '2'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["suffix"]!.ToString().Should().Be("Brown");
    }

    [Fact]
    public async Task Length_ReturnsStringLength()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT LENGTH(c.name) AS nameLen FROM c WHERE c.id = '5'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["nameLen"]!.Value<int>().Should().Be(3);
    }

    [Fact]
    public async Task Substring_ReturnsSubstring()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT SUBSTRING(c.name, 0, 3) AS sub FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["sub"]!.ToString().Should().Be("Ali");
    }

    [Fact]
    public async Task IndexOf_ReturnsPosition()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT INDEX_OF(c.name, 'Brown') AS pos FROM c WHERE c.id = '2'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["pos"]!.Value<int>().Should().Be(4);
    }

    [Fact]
    public async Task Replace_ReplacesSubstring()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT REPLACE(c.name, 'Alice', 'Alicia') AS replaced FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["replaced"]!.ToString().Should().Be("Alicia Anderson");
    }

    [Fact]
    public async Task Abs_ReturnsAbsoluteValue()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT ABS(c.value) AS absVal FROM c WHERE c.id = '5'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["absVal"]!.Value<int>().Should().Be(5);
    }

    [Fact]
    public async Task Floor_ReturnsFloor()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT FLOOR(c.nested.score) AS floored FROM c WHERE c.id = '3'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["floored"]!.Value<double>().Should().Be(3.0);
    }

    [Fact]
    public async Task Ceiling_ReturnsCeiling()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT CEILING(c.nested.score) AS ceiled FROM c WHERE c.id = '3'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["ceiled"]!.Value<double>().Should().Be(4.0);
    }

    [Fact]
    public async Task Round_ReturnsRoundedValue()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT ROUND(c.nested.score) AS rounded FROM c WHERE c.id = '3'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["rounded"]!.Value<double>().Should().Be(3.0);
    }

    // ── Additional Math functions ──

    [Fact]
    public async Task Sqrt_ReturnsSquareRoot()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT SQRT(c.value) AS sqrtVal FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["sqrtVal"]!.Value<double>().Should().BeApproximately(Math.Sqrt(10), 0.0001);
    }

    [Fact]
    public async Task Square_ReturnsSquared()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT SQUARE(c.value) AS sq FROM c WHERE c.id = '2'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["sq"]!.Value<double>().Should().Be(400);
    }

    [Fact]
    public async Task Power_ReturnsPower()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT POWER(c.value, 2) AS pw FROM c WHERE c.id = '3'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["pw"]!.Value<long>().Should().Be(900);
    }

    [Fact]
    public async Task Exp_ReturnsExponential()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT EXP(1) AS expVal FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["expVal"]!.Value<double>().Should().BeApproximately(Math.E, 0.0001);
    }

    [Fact]
    public async Task Log_ReturnsNaturalLog()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT LOG(c.value) AS logVal FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["logVal"]!.Value<double>().Should().BeApproximately(Math.Log(10), 0.0001);
    }

    [Fact]
    public async Task Log10_ReturnsLog10()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT LOG10(c.value) AS log10Val FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["log10Val"]!.Value<double>().Should().BeApproximately(1.0, 0.0001);
    }

    [Fact]
    public async Task Sign_ReturnsSign()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT SIGN(c.value) AS signVal FROM c WHERE c.id = '5'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["signVal"]!.Value<double>().Should().Be(-1);
    }

    [Fact]
    public async Task Trunc_ReturnsTruncated()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT TRUNC(c.nested.score) AS truncVal FROM c WHERE c.id = '3'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["truncVal"]!.Value<double>().Should().Be(3.0);
    }

    [Fact]
    public async Task Pi_ReturnsPi()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT PI() AS piVal FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["piVal"]!.Value<double>().Should().BeApproximately(Math.PI, 0.0001);
    }

    [Fact]
    public async Task Sin_ReturnsSine()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT SIN(1) AS sinVal FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["sinVal"]!.Value<double>().Should().BeApproximately(Math.Sin(1), 0.0001);
    }

    [Fact]
    public async Task Cos_ReturnsCosine()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT COS(0) AS cosVal FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["cosVal"]!.Value<double>().Should().Be(1.0);
    }

    [Fact]
    public async Task Tan_ReturnsTangent()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT TAN(0) AS tanVal FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["tanVal"]!.Value<double>().Should().Be(0.0);
    }

    [Fact]
    public async Task Asin_ReturnsArcSine()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT ASIN(1) AS asinVal FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["asinVal"]!.Value<double>().Should().BeApproximately(Math.PI / 2, 0.0001);
    }

    [Fact]
    public async Task Acos_ReturnsArcCosine()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT ACOS(1) AS acosVal FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["acosVal"]!.Value<double>().Should().Be(0.0);
    }

    [Fact]
    public async Task Atan_ReturnsArcTangent()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT ATAN(1) AS atanVal FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["atanVal"]!.Value<double>().Should().BeApproximately(Math.PI / 4, 0.0001);
    }

    [Fact]
    public async Task Atn2_ReturnsArcTangent2()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT ATN2(1, 1) AS atn2Val FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["atn2Val"]!.Value<double>().Should().BeApproximately(Math.PI / 4, 0.0001);
    }

    [Fact]
    public async Task Degrees_ConvertsToDegrees()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT DEGREES(PI()) AS degVal FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["degVal"]!.Value<double>().Should().BeApproximately(180.0, 0.0001);
    }

    [Fact]
    public async Task Radians_ConvertsToRadians()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT RADIANS(180) AS radVal FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["radVal"]!.Value<double>().Should().BeApproximately(Math.PI, 0.0001);
    }

    [Fact]
    public async Task Rand_ReturnsValueBetweenZeroAndOne()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT RAND() AS randVal FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        var randVal = results[0]["randVal"]!.Value<double>();
        randVal.Should().BeGreaterThanOrEqualTo(0.0).And.BeLessThan(1.0);
    }

    // ── Reverse and RegexMatch ──

    [Fact]
    public async Task Reverse_ReversesString()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT REVERSE(c.name) AS reversed FROM c WHERE c.id = '5'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["reversed"]!.ToString().Should().Be("evE");
    }

    [Fact]
    public async Task RegexMatch_MatchesPattern()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE RegexMatch(c.name, '^Alice')");

        var results = await QueryAll<TestDocument>(query);

        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Alice Anderson");
    }

    [Fact]
    public async Task RegexMatch_CaseInsensitive()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE RegexMatch(c.name, '^alice', 'i')");

        var results = await QueryAll<TestDocument>(query);

        results.Should().HaveCount(1);
    }

    // ── Type checking functions ──

    [Fact]
    public async Task IsArray_ReturnsTrueForArray()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE IS_ARRAY(c.tags) FROM c WHERE c.id = '1'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task IsBool_ReturnsTrueForBoolean()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE IS_BOOL(c.isActive) FROM c WHERE c.id = '1'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task IsNumber_ReturnsTrueForNumber()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE IS_NUMBER(c.value) FROM c WHERE c.id = '1'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task IsString_ReturnsTrueForString()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE IS_STRING(c.name) FROM c WHERE c.id = '1'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task IsObject_ReturnsTrueForObject()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE IS_OBJECT(c.nested) FROM c WHERE c.id = '3'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task IsPrimitive_ReturnsTrueForPrimitive()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE IS_PRIMITIVE(c.name) FROM c WHERE c.id = '1'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    // ── Conversion functions ──

    [Fact]
    public async Task ToNumber_ConvertsStringToNumber()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE ToNumber('42.5') FROM c WHERE c.id = '1'");

        var results = await QueryAll<double>(query);

        results.Should().ContainSingle().Which.Should().Be(42.5);
    }

    [Fact]
    public async Task ToBoolean_ConvertsStringToBoolean()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE ToBoolean('true') FROM c WHERE c.id = '1'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    // ── IS_FINITE_NUMBER and IS_INTEGER ──

    [Fact]
    public async Task IsFiniteNumber_ReturnsTrueForFiniteNumber()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE IS_FINITE_NUMBER(c.value) FROM c WHERE c.id = '1'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task IsFiniteNumber_ReturnsFalseForString()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE IS_FINITE_NUMBER(c.name) FROM c WHERE c.id = '1'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeFalse();
    }

    [Fact]
    public async Task IsInteger_ReturnsTrueForIntegerValue()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE IS_INTEGER(c.value) FROM c WHERE c.id = '1'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task IsInteger_ReturnsFalseForDouble()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE IS_INTEGER(c.nested.score) FROM c WHERE c.id = '3'");

        var results = await QueryAll<bool>(query);

        results.Should().ContainSingle().Which.Should().BeFalse();
    }

    [Fact]
    public async Task IsFiniteNumber_InWhereClause_FiltersCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE IS_FINITE_NUMBER(c.value)");

        var results = await QueryAll<TestDocument>(query);

        results.Should().HaveCount(5);
    }

    [Fact]
    public async Task IsInteger_InWhereClause_FiltersCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE IS_INTEGER(c.value)");

        var results = await QueryAll<TestDocument>(query);

        results.Should().HaveCount(5);
    }

    // ── Array functions ──

    [Fact]
    public async Task ArraySlice_ReturnsSlice()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT ARRAY_SLICE(c.tags, 0, 1) AS sliced FROM c WHERE c.id = '5'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        var sliced = (JArray)results[0]["sliced"]!;
        sliced.Should().HaveCount(1);
        sliced[0]!.ToString().Should().Be("a");
    }

    [Fact]
    public async Task ArrayConcat_ConcatenatesArrays()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT ARRAY_CONCAT(c.tags, c.tags) AS doubled FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        var doubled = (JArray)results[0]["doubled"]!;
        doubled.Should().HaveCount(4);
    }

    // ── New string functions ──

    [Fact]
    public async Task Replicate_RepeatsString()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT REPLICATE(c.name, 3) AS rep FROM c WHERE c.id = '5'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["rep"]!.ToString().Should().Be("EveEveEve");
    }

    [Fact]
    public async Task Replicate_ZeroCount_ReturnsEmpty()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT REPLICATE(c.name, 0) AS rep FROM c WHERE c.id = '5'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["rep"]!.ToString().Should().BeEmpty();
    }

    [Fact]
    public async Task StringEquals_MatchesExact()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE STRING_EQUALS(c.name, 'Eve')");

        var results = await QueryAll<TestDocument>(query);

        results.Should().HaveCount(1);
        results[0].Id.Should().Be("5");
    }

    [Fact]
    public async Task StringEquals_CaseInsensitive()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE STRING_EQUALS(c.name, 'eve', true)");

        var results = await QueryAll<TestDocument>(query);

        results.Should().HaveCount(1);
        results[0].Id.Should().Be("5");
    }

    [Fact]
    public async Task StringToArray_ParsesJsonArray()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE StringToArray('[1, 2, 3]') FROM c WHERE c.id = '1'");

        var results = await QueryAll<JArray>(query);

        results.Should().ContainSingle();
        results[0].Should().HaveCount(3);
    }

    [Fact]
    public async Task StringToBoolean_ParsesTrueAndFalse()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT StringToBoolean('true') AS t, StringToBoolean('false') AS f FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["t"]!.Value<bool>().Should().BeTrue();
        results[0]["f"]!.Value<bool>().Should().BeFalse();
    }

    [Fact]
    public async Task StringToNumber_ParsesInteger()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE StringToNumber('42') FROM c WHERE c.id = '1'");

        var results = await QueryAll<long>(query);

        results.Should().ContainSingle().Which.Should().Be(42);
    }

    [Fact]
    public async Task StringToNumber_ParsesDecimal()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE StringToNumber('3.14') FROM c WHERE c.id = '1'");

        var results = await QueryAll<double>(query);

        results.Should().ContainSingle().Which.Should().Be(3.14);
    }

    [Fact]
    public async Task StringToObject_ParsesJsonObject()
    {
        await SeedItems();
        var query = new QueryDefinition("""SELECT VALUE StringToObject('{"a": 1}') FROM c WHERE c.id = '1'""");

        var results = await QueryAll<JObject>(query);

        results.Should().ContainSingle();
        results[0]["a"]!.Value<int>().Should().Be(1);
    }

    // ── Integer math functions ──

    [Fact]
    public async Task NumberBin_RoundsDownToNearestBin()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT NumberBin(c.value, 7) AS binned FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["binned"]!.Value<double>().Should().Be(7);
    }

    [Fact]
    public async Task IntAdd_AddsIntegers()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IntAdd(c.value, 5) AS result FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.Value<long>().Should().Be(15);
    }

    [Fact]
    public async Task IntSub_SubtractsIntegers()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IntSub(c.value, 3) AS result FROM c WHERE c.id = '2'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.Value<long>().Should().Be(17);
    }

    [Fact]
    public async Task IntMul_MultipliesIntegers()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IntMul(c.value, 3) AS result FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.Value<long>().Should().Be(30);
    }

    [Fact]
    public async Task IntDiv_DividesIntegers()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IntDiv(c.value, 3) AS result FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.Value<long>().Should().Be(3);
    }

    [Fact]
    public async Task IntDiv_DivisionByZero_ReturnsNull()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IntDiv(c.value, 0) AS result FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.Type.Should().Be(JTokenType.Null);
    }

    [Fact]
    public async Task IntMod_ReturnsRemainder()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IntMod(c.value, 3) AS result FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.Value<long>().Should().Be(1);
    }

    [Fact]
    public async Task IntBitAnd_ReturnsBitwiseAnd()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IntBitAnd(c.value, 6) AS result FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.Value<long>().Should().Be(10 & 6);
    }

    [Fact]
    public async Task IntBitOr_ReturnsBitwiseOr()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IntBitOr(c.value, 5) AS result FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.Value<long>().Should().Be(10 | 5);
    }

    [Fact]
    public async Task IntBitXor_ReturnsBitwiseXor()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IntBitXor(c.value, 7) AS result FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.Value<long>().Should().Be(10 ^ 7);
    }

    [Fact]
    public async Task IntBitNot_ReturnsBitwiseNot()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IntBitNot(c.value) AS result FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.Value<long>().Should().Be(~10L);
    }

    [Fact]
    public async Task IntBitLeftShift_ShiftsLeft()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IntBitLeftShift(c.value, 2) AS result FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.Value<long>().Should().Be(10L << 2);
    }

    [Fact]
    public async Task IntBitRightShift_ShiftsRight()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT IntBitRightShift(c.value, 1) AS result FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["result"]!.Value<long>().Should().Be(10L >> 1);
    }

    [Fact]
    public async Task SumAggregate_ReturnsCorrectSum()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT SUM(c.value) AS total FROM c");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["total"]!.Value<int>().Should().Be(55);
    }

    [Fact]
    public async Task AvgAggregate_ReturnsCorrectAverage()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT AVG(c.value) AS average FROM c");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["average"]!.Value<double>().Should().Be(11.0);
    }

    [Fact]
    public async Task MinAggregate_ReturnsMinValue()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT MIN(c.value) AS minVal FROM c");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["minVal"]!.Value<int>().Should().Be(-5);
    }

    [Fact]
    public async Task MaxAggregate_ReturnsMaxValue()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT MAX(c.value) AS maxVal FROM c");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["maxVal"]!.Value<int>().Should().Be(30);
    }

    [Fact]
    public async Task GroupBy_GroupsCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT c.isActive, COUNT(1) AS cnt FROM c GROUP BY c.isActive");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(2);
        var activeGroup = results.First(r => r["isActive"]!.Value<bool>());
        activeGroup["cnt"]!.Value<int>().Should().Be(3);
        var inactiveGroup = results.First(r => !r["isActive"]!.Value<bool>());
        inactiveGroup["cnt"]!.Value<int>().Should().Be(2);
    }

    [Fact]
    public async Task GroupByHaving_FiltersGroups()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT c.isActive, COUNT(1) AS cnt FROM c GROUP BY c.isActive HAVING COUNT(1) > 2");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["isActive"]!.Value<bool>().Should().BeTrue();
        results[0]["cnt"]!.Value<int>().Should().Be(3);
    }

    [Fact]
    public async Task InExpression_FiltersMultipleValues()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE c.name IN ('Alice Anderson', 'Bob Brown', 'NotExist')");

        var results = await QueryAll<TestDocument>(query);

        results.Should().HaveCount(2);
    }

    [Fact]
    public async Task NotIn_FiltersExcludedValues()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE c.name NOT IN ('Alice Anderson', 'Bob Brown')");

        var results = await QueryAll<TestDocument>(query);

        results.Should().HaveCount(3);
    }

    [Fact]
    public async Task Between_FiltersRange()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE c.value BETWEEN 10 AND 30");

        var results = await QueryAll<TestDocument>(query);

        results.Should().HaveCount(3);
    }

    [Fact]
    public async Task EmptyResult_ReturnsEmptyCollection()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE c.name = 'NonExistent'");

        var results = await QueryAll<TestDocument>(query);

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task ToString_ReturnsStringRepresentation()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT ToString(c.value) AS str FROM c WHERE c.id = '1'");

        var results = await QueryAll<JObject>(query);

        results.Should().HaveCount(1);
        results[0]["str"]!.ToString().Should().Be("10");
    }

    [Fact]
    public async Task ValueSelect_ReturnsScalarValues()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT VALUE c.name FROM c WHERE c.id = '1'");

        var results = await QueryAll<string>(query);

        results.Should().HaveCount(1);
        results[0].Should().Be("Alice Anderson");
    }

    [Fact]
    public async Task NestedPropertyAccess_QueriesCorrectly()
    {
        await SeedItems();
        var query = new QueryDefinition("SELECT * FROM c WHERE c.nested.score > 3.0");

        var results = await QueryAll<TestDocument>(query);

        results.Should().HaveCount(1);
        results[0].Id.Should().Be("3");
    }

    private async Task<List<T>> QueryAll<T>(QueryDefinition query)
    {
        var iterator = _container.GetItemQueryIterator<T>(query);
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }
        return results;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Spatial functions
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task StDistance_BetweenTwoPoints_ReturnsMeters()
    {
        var container = new InMemoryContainer("geo-test", "/partitionKey");
        await container.CreateItemAsync(new GeoDocument
        {
            Id = "1",
            PartitionKey = "pk1",
            Location = new GeoJsonGeometry { Type = "Point", Coordinates = new[] { -0.1278, 51.5074 } }
        }, new PartitionKey("pk1"));

        var query = new QueryDefinition(
            "SELECT VALUE ST_DISTANCE(c.location, {'type': 'Point', 'coordinates': [-2.2426, 53.4808]}) FROM c");

        var results = await QueryAll<double>(container, query);

        results.Should().ContainSingle();
        results[0].Should().BeApproximately(262_000, 5_000); // London to Manchester ~262km
    }

    [Fact]
    public async Task StDistance_WithNonPoint_ReturnsEmpty()
    {
        var container = new InMemoryContainer("geo-test", "/partitionKey");
        await container.CreateItemAsync(new GeoDocument
        {
            Id = "1",
            PartitionKey = "pk1",
            Name = "not a point"
        }, new PartitionKey("pk1"));

        var query = new QueryDefinition(
            "SELECT VALUE ST_DISTANCE(c.location, {'type': 'Point', 'coordinates': [0, 0]}) FROM c");

        var results = await QueryAll<object>(container, query);

        results.Should().ContainSingle().Which.Should().BeNull();
    }

    [Fact]
    public async Task StWithin_PointInsidePolygon_ReturnsTrue()
    {
        var container = new InMemoryContainer("geo-test", "/partitionKey");
        await container.CreateItemAsync(new GeoDocument
        {
            Id = "1",
            PartitionKey = "pk1",
            Location = new GeoJsonGeometry { Type = "Point", Coordinates = new[] { -0.1278, 51.5074 } }
        }, new PartitionKey("pk1"));

        // Polygon covering most of southern England
        var query = new QueryDefinition("""
            SELECT * FROM c WHERE ST_WITHIN(c.location, {
                'type': 'Polygon',
                'coordinates': [[[-2.0, 50.0], [1.0, 50.0], [1.0, 52.0], [-2.0, 52.0], [-2.0, 50.0]]]
            })
            """);

        var results = await QueryAll<GeoDocument>(container, query);

        results.Should().ContainSingle();
    }

    [Fact]
    public async Task StWithin_PointOutsidePolygon_ReturnsEmpty()
    {
        var container = new InMemoryContainer("geo-test", "/partitionKey");
        await container.CreateItemAsync(new GeoDocument
        {
            Id = "1",
            PartitionKey = "pk1",
            Location = new GeoJsonGeometry { Type = "Point", Coordinates = new[] { -0.1278, 51.5074 } }
        }, new PartitionKey("pk1"));

        // Polygon in France
        var query = new QueryDefinition("""
            SELECT * FROM c WHERE ST_WITHIN(c.location, {
                'type': 'Polygon',
                'coordinates': [[[1.0, 43.0], [3.0, 43.0], [3.0, 45.0], [1.0, 45.0], [1.0, 43.0]]]
            })
            """);

        var results = await QueryAll<GeoDocument>(container, query);

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task StIntersects_PointInPolygon_ReturnsTrue()
    {
        var container = new InMemoryContainer("geo-test", "/partitionKey");
        await container.CreateItemAsync(new GeoDocument
        {
            Id = "1",
            PartitionKey = "pk1",
            Location = new GeoJsonGeometry { Type = "Point", Coordinates = new[] { -0.1278, 51.5074 } }
        }, new PartitionKey("pk1"));

        var query = new QueryDefinition("""
            SELECT * FROM c WHERE ST_INTERSECTS(c.location, {
                'type': 'Polygon',
                'coordinates': [[[-1.0, 51.0], [0.0, 51.0], [0.0, 52.0], [-1.0, 52.0], [-1.0, 51.0]]]
            })
            """);

        var results = await QueryAll<GeoDocument>(container, query);

        results.Should().ContainSingle();
    }

    [Fact]
    public async Task StIsValid_ValidPoint_ReturnsTrue()
    {
        var container = new InMemoryContainer("geo-test", "/partitionKey");
        await container.CreateItemAsync(new GeoDocument
        {
            Id = "1",
            PartitionKey = "pk1",
            Location = new GeoJsonGeometry { Type = "Point", Coordinates = new[] { -0.1278, 51.5074 } }
        }, new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT VALUE ST_ISVALID(c.location) FROM c");

        var results = await QueryAll<bool>(container, query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task StIsValid_InvalidGeoJson_ReturnsFalse()
    {
        var container = new InMemoryContainer("geo-test", "/partitionKey");
        await container.CreateItemAsync(new GeoDocument
        {
            Id = "1",
            PartitionKey = "pk1",
            Location = new GeoJsonGeometry { Type = "Point", Coordinates = new[] { 999.0, 999.0 } }
        }, new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT VALUE ST_ISVALID(c.location) FROM c");

        var results = await QueryAll<bool>(container, query);

        results.Should().ContainSingle().Which.Should().BeFalse();
    }

    [Fact]
    public async Task StIsValidDetailed_ValidPolygon_ReturnsValidTrue()
    {
        var container = new InMemoryContainer("geo-test", "/partitionKey");
        await container.CreateItemAsync(new GeoDocument
        {
            Id = "1",
            PartitionKey = "pk1",
            Area = new GeoJsonGeometry
            {
                Type = "Polygon",
                Coordinates = new[] { new[] { new[] { 0.0, 0.0 }, new[] { 1.0, 0.0 }, new[] { 1.0, 1.0 }, new[] { 0.0, 0.0 } } }
            }
        }, new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT VALUE ST_ISVALIDDETAILED(c.area) FROM c");

        var results = await QueryAll<JObject>(container, query);

        results.Should().ContainSingle();
        results[0]["valid"]!.Value<bool>().Should().BeTrue();
    }

    [Fact]
    public async Task StIsValidDetailed_InvalidPolygon_ReturnsValidFalseWithReason()
    {
        var container = new InMemoryContainer("geo-test", "/partitionKey");
        await container.CreateItemAsync(new GeoDocument
        {
            Id = "1",
            PartitionKey = "pk1",
            Area = new GeoJsonGeometry
            {
                Type = "Polygon",
                Coordinates = new[] { new[] { new[] { 0.0, 0.0 }, new[] { 1.0, 0.0 } } }
            }
        }, new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT VALUE ST_ISVALIDDETAILED(c.area) FROM c");

        var results = await QueryAll<JObject>(container, query);

        results.Should().ContainSingle();
        results[0]["valid"]!.Value<bool>().Should().BeFalse();
        results[0]["reason"]!.ToString().Should().NotBeEmpty();
    }

    // ── Additional spatial tests ──

    [Fact]
    public async Task StDistance_BetweenSamePoint_ReturnsZero()
    {
        var container = new InMemoryContainer("geo-test", "/partitionKey");
        await container.CreateItemAsync(new GeoDocument
        {
            Id = "1",
            PartitionKey = "pk1",
            Location = new GeoJsonGeometry { Type = "Point", Coordinates = new[] { -0.1278, 51.5074 } }
        }, new PartitionKey("pk1"));

        var query = new QueryDefinition(
            "SELECT VALUE ST_DISTANCE(c.location, {'type': 'Point', 'coordinates': [-0.1278, 51.5074]}) FROM c");

        var results = await QueryAll<double>(container, query);

        results.Should().ContainSingle().Which.Should().Be(0);
    }

    [Fact]
    public async Task StWithin_PointInCircle_ReturnsTrue()
    {
        var container = new InMemoryContainer("geo-test", "/partitionKey");
        await container.CreateItemAsync(new GeoDocument
        {
            Id = "1",
            PartitionKey = "pk1",
            Location = new GeoJsonGeometry { Type = "Point", Coordinates = new[] { -0.1278, 51.5074 } }
        }, new PartitionKey("pk1"));

        var query = new QueryDefinition("""
            SELECT * FROM c WHERE ST_WITHIN(c.location, {
                'center': {'type': 'Point', 'coordinates': [-0.1278, 51.5074]},
                'radius': 1000
            })
            """);

        var results = await QueryAll<GeoDocument>(container, query);

        results.Should().ContainSingle();
    }

    [Fact]
    public async Task StIntersects_TwoOverlappingPolygons_ReturnsTrue()
    {
        var container = new InMemoryContainer("geo-test", "/partitionKey");
        await container.CreateItemAsync(new GeoDocument
        {
            Id = "1",
            PartitionKey = "pk1",
            Area = new GeoJsonGeometry
            {
                Type = "Polygon",
                Coordinates = new[] { new[] { new[] { 0.0, 0.0 }, new[] { 2.0, 0.0 }, new[] { 2.0, 2.0 }, new[] { 0.0, 2.0 }, new[] { 0.0, 0.0 } } }
            }
        }, new PartitionKey("pk1"));

        var query = new QueryDefinition("""
            SELECT * FROM c WHERE ST_INTERSECTS(c.area, {
                'type': 'Polygon',
                'coordinates': [[[1.0, 1.0], [3.0, 1.0], [3.0, 3.0], [1.0, 3.0], [1.0, 1.0]]]
            })
            """);

        var results = await QueryAll<GeoDocument>(container, query);

        results.Should().ContainSingle();
    }

    [Fact]
    public async Task StIsValid_ValidLineString_ReturnsTrue()
    {
        var container = new InMemoryContainer("geo-test", "/partitionKey");
        await container.CreateItemAsync(new GeoDocument
        {
            Id = "1",
            PartitionKey = "pk1",
            Area = new GeoJsonGeometry
            {
                Type = "LineString",
                Coordinates = new[] { new[] { 0.0, 0.0 }, new[] { 1.0, 1.0 }, new[] { 2.0, 0.0 } }
            }
        }, new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT VALUE ST_ISVALID(c.area) FROM c");

        var results = await QueryAll<bool>(container, query);

        results.Should().ContainSingle().Which.Should().BeTrue();
    }

    [Fact]
    public async Task StIsValidDetailed_LineStringTooFewPoints_ReturnsInvalid()
    {
        var container = new InMemoryContainer("geo-test", "/partitionKey");
        await container.CreateItemAsync(new GeoDocument
        {
            Id = "1",
            PartitionKey = "pk1",
            Area = new GeoJsonGeometry
            {
                Type = "LineString",
                Coordinates = new[] { new[] { 0.0, 0.0 } }
            }
        }, new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT VALUE ST_ISVALIDDETAILED(c.area) FROM c");

        var results = await QueryAll<JObject>(container, query);

        results.Should().ContainSingle();
        results[0]["valid"]!.Value<bool>().Should().BeFalse();
    }

    [Fact]
    public async Task StDistance_InWhereClause_FiltersNearbyPoints()
    {
        var container = new InMemoryContainer("geo-test", "/partitionKey");
        await container.CreateItemAsync(new GeoDocument
        {
            Id = "london",
            PartitionKey = "pk1",
            Location = new GeoJsonGeometry { Type = "Point", Coordinates = new[] { -0.1278, 51.5074 } }
        }, new PartitionKey("pk1"));
        await container.CreateItemAsync(new GeoDocument
        {
            Id = "paris",
            PartitionKey = "pk1",
            Location = new GeoJsonGeometry { Type = "Point", Coordinates = new[] { 2.3522, 48.8566 } }
        }, new PartitionKey("pk1"));

        var query = new QueryDefinition("""
            SELECT * FROM c WHERE ST_DISTANCE(c.location, {'type': 'Point', 'coordinates': [-0.13, 51.51]}) < 10000
            """);

        var results = await QueryAll<GeoDocument>(container, query);

        results.Should().ContainSingle().Which.Id.Should().Be("london");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  UDF registration
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task RegisteredUdf_IsCalledDuringQuery()
    {
        var container = new InMemoryContainer("udf-test", "/partitionKey");
        container.RegisterUdf("doubleValue", args =>
        {
            var val = Convert.ToDouble(args[0]);
            return val * 2;
        });

        await container.CreateItemAsync(new UdfDocument
        {
            Id = "1",
            PartitionKey = "pk1",
            Value = 21
        }, new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT VALUE udf.doubleValue(c.value) FROM c");

        var results = await QueryAll<double>(container, query);

        results.Should().ContainSingle().Which.Should().Be(42);
    }

    [Fact]
    public async Task RegisteredUdf_InWhereClause_FiltersCorrectly()
    {
        var container = new InMemoryContainer("udf-test", "/partitionKey");
        container.RegisterUdf("isEven", args =>
        {
            var val = Convert.ToInt64(args[0]);
            return val % 2 == 0;
        });

        await container.CreateItemAsync(new UdfDocument { Id = "1", PartitionKey = "pk1", Value = 10 }, new PartitionKey("pk1"));
        await container.CreateItemAsync(new UdfDocument { Id = "2", PartitionKey = "pk1", Value = 11 }, new PartitionKey("pk1"));
        await container.CreateItemAsync(new UdfDocument { Id = "3", PartitionKey = "pk1", Value = 12 }, new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT * FROM c WHERE udf.isEven(c.value)");

        var results = await QueryAll<JObject>(container, query);

        results.Should().HaveCount(2);
    }

    [Fact]
    public async Task UnregisteredUdf_ThrowsNotSupportedException()
    {
        var container = new InMemoryContainer("udf-test", "/partitionKey");
        await container.CreateItemAsync(new UdfDocument { Id = "1", PartitionKey = "pk1", Value = 10 }, new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT VALUE udf.missing(c.value) FROM c");

        var act = async () => await QueryAll<object>(container, query);

        await act.Should().ThrowAsync<NotSupportedException>()
            .WithMessage("*RegisterUdf*");
    }

    [Fact]
    public async Task RegisteredUdf_WithMultipleArgs_ReceivesAllArgs()
    {
        var container = new InMemoryContainer("udf-test", "/partitionKey");
        container.RegisterUdf("add", args =>
        {
            var a = Convert.ToDouble(args[0]);
            var b = Convert.ToDouble(args[1]);
            return a + b;
        });

        await container.CreateItemAsync(new UdfDocument { Id = "1", PartitionKey = "pk1", X = 10, Y = 32 }, new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT VALUE udf.add(c.x, c.y) FROM c");

        var results = await QueryAll<double>(container, query);

        results.Should().ContainSingle().Which.Should().Be(42);
    }

    private static async Task<List<T>> QueryAll<T>(InMemoryContainer container, QueryDefinition query)
    {
        var iterator = container.GetItemQueryIterator<T>(query);
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }
        return results;
    }
}
