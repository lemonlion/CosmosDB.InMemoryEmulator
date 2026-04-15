using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Edge-case tests for Issue #9 fix: Removing CamelCaseNamingStrategy from
/// InMemoryContainer and InMemoryTransactionalBatch JsonSettings.
/// These tests probe corners that could break after the change.
/// </summary>
public class Issue9EdgeCaseTests
{
    // ═══════════════════════════════════════════════════════════════════════════
    //  Test model classes
    // ═══════════════════════════════════════════════════════════════════════════

    public enum Priority { Low, Medium, High, Critical }

    public enum StatusCode { OK = 200, NotFound = 404, InternalServerError = 500 }

    private class EnumDocument
    {
        [JsonProperty("id")] public string Id { get; set; } = "";
        [JsonProperty("pk")] public string Pk { get; set; } = "";
        public Priority Priority { get; set; }
        public StatusCode Code { get; set; }
    }

    private class MixedCaseAlphabet
    {
        [JsonProperty("id")] public string Id { get; set; } = "";
        [JsonProperty("pk")] public string Pk { get; set; } = "";
        public string a { get; set; } = "";
        public string A { get; set; } = "";
        public string aa { get; set; } = "";
        public string AA { get; set; } = "";
        public string aA { get; set; } = "";
        public string Aa { get; set; } = "";
    }

    private class NestedCaseSensitive
    {
        [JsonProperty("id")] public string Id { get; set; } = "";
        [JsonProperty("pk")] public string Pk { get; set; } = "";
        public InnerLevel Level1 { get; set; } = new();
    }

    private class InnerLevel
    {
        public string x { get; set; } = "";
        public string X { get; set; } = "";
        public DeepLevel Deep { get; set; } = new();
    }

    private class DeepLevel
    {
        public string y { get; set; } = "";
        public string Y { get; set; } = "";
    }

    private class JsonPropertyOverrideDoc
    {
        [JsonProperty("id")] public string Id { get; set; } = "";
        [JsonProperty("pk")] public string Pk { get; set; } = "";
        [JsonProperty("custom_name")] public string PascalCaseProperty { get; set; } = "";
        [JsonProperty("LOUD_NAME")] public string quietProperty { get; set; } = "";
        [JsonProperty("MiXeD")] public string AnotherProperty { get; set; } = "";
    }

    private class PascalCaseDoc
    {
        [JsonProperty("id")] public string Id { get; set; } = "";
        [JsonProperty("pk")] public string Pk { get; set; } = "";
        public string FirstName { get; set; } = "";
        public string LastName { get; set; } = "";
        public int ItemCount { get; set; }
    }

    private class SystemPropsDoc
    {
        [JsonProperty("id")] public string Id { get; set; } = "";
        [JsonProperty("pk")] public string Pk { get; set; } = "";
        public string _etag { get; set; } = "";
        public long _ts { get; set; }
        public string Data { get; set; } = "";
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  1. Mixed casing combinations: a, A, aa, AA, aA, Aa
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MixedCaseCombinations_AllSurviveRoundTrip_ViaStrongType()
    {
        var container = new InMemoryContainer("mixed-case", "/pk");

        var item = new MixedCaseAlphabet
        {
            Id = "1", Pk = "p",
            a = "lower-a", A = "upper-A",
            aa = "lower-aa", AA = "upper-AA",
            aA = "lower-a-upper-A", Aa = "upper-A-lower-a"
        };

        await container.CreateItemAsync(item, new PartitionKey("p"));
        var response = await container.ReadItemAsync<MixedCaseAlphabet>("1", new PartitionKey("p"));
        var result = response.Resource;

        result.a.Should().Be("lower-a");
        result.A.Should().Be("upper-A");
        result.aa.Should().Be("lower-aa");
        result.AA.Should().Be("upper-AA");
        result.aA.Should().Be("lower-a-upper-A");
        result.Aa.Should().Be("upper-A-lower-a");
    }

    [Fact]
    public async Task MixedCaseCombinations_AllSurviveRoundTrip_ViaJObject()
    {
        var container = new InMemoryContainer("mixed-case-jo", "/pk");

        var jObj = new JObject
        {
            ["id"] = "1", ["pk"] = "p",
            ["a"] = "1", ["A"] = "2",
            ["aa"] = "3", ["AA"] = "4",
            ["aA"] = "5", ["Aa"] = "6"
        };

        await container.CreateItemAsync(jObj, new PartitionKey("p"));
        var response = await container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
        var result = response.Resource;

        result["a"]!.ToString().Should().Be("1");
        result["A"]!.ToString().Should().Be("2");
        result["aa"]!.ToString().Should().Be("3");
        result["AA"]!.ToString().Should().Be("4");
        result["aA"]!.ToString().Should().Be("5");
        result["Aa"]!.ToString().Should().Be("6");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  2. PascalCase properties NOT transformed (regression guard)
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task PascalCaseProperties_NotCamelCased_AfterFix()
    {
        var container = new InMemoryContainer("pascal", "/pk");

        var item = new PascalCaseDoc
        {
            Id = "1", Pk = "p",
            FirstName = "Jane", LastName = "Doe", ItemCount = 42
        };

        await container.CreateItemAsync(item, new PartitionKey("p"));
        var response = await container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
        var result = response.Resource;

        // Properties should keep their original PascalCase
        result["FirstName"]!.ToString().Should().Be("Jane");
        result["LastName"]!.ToString().Should().Be("Doe");
        result["ItemCount"]!.Value<int>().Should().Be(42);

        // Must NOT have camelCase versions
        result["firstName"].Should().BeNull();
        result["lastName"].Should().BeNull();
        result["itemCount"].Should().BeNull();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  3. System properties (_etag, _ts, _rid) still work
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SystemProperties_StillPopulated_WithoutNamingStrategy()
    {
        var container = new InMemoryContainer("sys-props", "/pk");

        var jObj = new JObject
        {
            ["id"] = "1", ["pk"] = "p", ["Data"] = "hello"
        };

        await container.CreateItemAsync(jObj, new PartitionKey("p"));
        var response = await container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
        var result = response.Resource;

        // System properties should be present with underscore prefix
        result["_etag"].Should().NotBeNull();
        result["_ts"].Should().NotBeNull();
        result["_rid"].Should().NotBeNull();

        // Original data preserved
        result["Data"]!.ToString().Should().Be("hello");
    }

    [Fact]
    public async Task SystemProperties_NotConfusedWithUserProperties_StartingWithUnderscore()
    {
        var container = new InMemoryContainer("underscore-props", "/pk");

        var jObj = new JObject
        {
            ["id"] = "1", ["pk"] = "p",
            ["_customField"] = "user-value",
            ["_anotherCustom"] = 123
        };

        await container.CreateItemAsync(jObj, new PartitionKey("p"));
        var response = await container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
        var result = response.Resource;

        result["_customField"]!.ToString().Should().Be("user-value");
        result["_anotherCustom"]!.Value<int>().Should().Be(123);
        // System properties also present
        result["_etag"].Should().NotBeNull();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  4. Enum serialization still works with StringEnumConverter
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task EnumProperties_SerializeAsStrings_WithStringEnumConverter()
    {
        var container = new InMemoryContainer("enum-test", "/pk");

        var item = new EnumDocument
        {
            Id = "1", Pk = "p",
            Priority = Priority.Critical,
            Code = StatusCode.NotFound
        };

        await container.CreateItemAsync(item, new PartitionKey("p"));

        // Read as JObject to inspect raw serialized form
        var response = await container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
        var result = response.Resource;

        result["Priority"]!.ToString().Should().Be("Critical");
        result["Code"]!.ToString().Should().Be("NotFound");

        // Round-trip back to strong type
        var typed = await container.ReadItemAsync<EnumDocument>("1", new PartitionKey("p"));
        typed.Resource.Priority.Should().Be(Priority.Critical);
        typed.Resource.Code.Should().Be(StatusCode.NotFound);
    }

    [Theory]
    [InlineData(Priority.Low)]
    [InlineData(Priority.Medium)]
    [InlineData(Priority.High)]
    [InlineData(Priority.Critical)]
    public async Task EnumValues_AllRoundTrip_Correctly(Priority priority)
    {
        var container = new InMemoryContainer($"enum-rt-{priority}", "/pk");

        var item = new EnumDocument { Id = "1", Pk = "p", Priority = priority, Code = StatusCode.OK };
        await container.CreateItemAsync(item, new PartitionKey("p"));

        var response = await container.ReadItemAsync<EnumDocument>("1", new PartitionKey("p"));
        response.Resource.Priority.Should().Be(priority);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  5. Transactional batch preserves case-sensitive properties
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task TransactionalBatch_Create_PreservesCaseSensitiveProperties()
    {
        var container = new InMemoryContainer("batch-case", "/pk");

        var batch = container.CreateTransactionalBatch(new PartitionKey("p"));
        batch.CreateItem(new MixedCaseAlphabet
        {
            Id = "b1", Pk = "p",
            a = "low", A = "HIGH",
            aa = "double-low", AA = "DOUBLE-HIGH",
            aA = "mixed1", Aa = "mixed2"
        });

        using var batchResponse = await batch.ExecuteAsync();
        batchResponse.StatusCode.Should().Be(HttpStatusCode.OK);

        var response = await container.ReadItemAsync<JObject>("b1", new PartitionKey("p"));
        var result = response.Resource;

        result["a"]!.ToString().Should().Be("low");
        result["A"]!.ToString().Should().Be("HIGH");
        result["aa"]!.ToString().Should().Be("double-low");
        result["AA"]!.ToString().Should().Be("DOUBLE-HIGH");
        result["aA"]!.ToString().Should().Be("mixed1");
        result["Aa"]!.ToString().Should().Be("mixed2");
    }

    [Fact]
    public async Task TransactionalBatch_Upsert_PreservesCaseSensitiveProperties()
    {
        var container = new InMemoryContainer("batch-upsert", "/pk");

        // Create first
        var jObj = new JObject
        {
            ["id"] = "u1", ["pk"] = "p",
            ["x"] = "original-lower", ["X"] = "original-upper"
        };
        await container.CreateItemAsync(jObj, new PartitionKey("p"));

        // Upsert via batch
        var batch = container.CreateTransactionalBatch(new PartitionKey("p"));
        var upsertObj = new JObject
        {
            ["id"] = "u1", ["pk"] = "p",
            ["x"] = "updated-lower", ["X"] = "updated-upper"
        };
        batch.UpsertItem(upsertObj);

        using var batchResponse = await batch.ExecuteAsync();
        batchResponse.StatusCode.Should().Be(HttpStatusCode.OK);

        var response = await container.ReadItemAsync<JObject>("u1", new PartitionKey("p"));
        response.Resource["x"]!.ToString().Should().Be("updated-lower");
        response.Resource["X"]!.ToString().Should().Be("updated-upper");
    }

    [Fact]
    public async Task TransactionalBatch_Replace_PreservesCaseSensitiveProperties()
    {
        var container = new InMemoryContainer("batch-replace", "/pk");

        var jObj = new JObject
        {
            ["id"] = "r1", ["pk"] = "p",
            ["d"] = "orig-d", ["D"] = "orig-D"
        };
        await container.CreateItemAsync(jObj, new PartitionKey("p"));

        var batch = container.CreateTransactionalBatch(new PartitionKey("p"));
        var replaceObj = new JObject
        {
            ["id"] = "r1", ["pk"] = "p",
            ["d"] = "new-d", ["D"] = "new-D"
        };
        batch.ReplaceItem("r1", replaceObj);

        using var batchResponse = await batch.ExecuteAsync();
        batchResponse.StatusCode.Should().Be(HttpStatusCode.OK);

        var response = await container.ReadItemAsync<JObject>("r1", new PartitionKey("p"));
        response.Resource["d"]!.ToString().Should().Be("new-d");
        response.Resource["D"]!.ToString().Should().Be("new-D");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  6. Change feed returns items with correct casing
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ChangeFeed_ReturnsItems_WithCaseSensitiveProperties()
    {
        var container = new InMemoryContainer("cf-case", "/pk");

        var jObj = new JObject
        {
            ["id"] = "cf1", ["pk"] = "p",
            ["d"] = "lower-data", ["D"] = "UPPER-data",
            ["myProp"] = "keep-case"
        };
        await container.CreateItemAsync(jObj, new PartitionKey("p"));

        var iterator = container.GetChangeFeedIterator<JObject>(
            ChangeFeedStartFrom.Beginning(),
            ChangeFeedMode.Incremental);

        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            if (response.StatusCode == HttpStatusCode.NotModified) break;
            results.AddRange(response);
        }

        results.Should().ContainSingle();
        var result = results[0];
        result["d"]!.ToString().Should().Be("lower-data");
        result["D"]!.ToString().Should().Be("UPPER-data");
        result["myProp"]!.ToString().Should().Be("keep-case");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  7. SQL queries referencing case-sensitive properties
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SqlQuery_SelectBothCasings_ReturnsDistinctValues()
    {
        var container = new InMemoryContainer("sql-case", "/pk");

        var jObj = new JObject
        {
            ["id"] = "q1", ["pk"] = "p",
            ["d"] = "lower-val", ["D"] = "UPPER-val"
        };
        await container.CreateItemAsync(jObj, new PartitionKey("p"));

        var query = new QueryDefinition("SELECT c.d, c.D FROM c WHERE c.id = 'q1'");
        var iter = container.GetItemQueryIterator<JObject>(query);
        var results = new List<JObject>();
        while (iter.HasMoreResults)
        {
            var page = await iter.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle();
        results[0]["d"]!.ToString().Should().Be("lower-val");
        results[0]["D"]!.ToString().Should().Be("UPPER-val");
    }

    [Fact]
    public async Task SqlQuery_FilterOnCaseSensitiveProperty_MatchesCorrectly()
    {
        var container = new InMemoryContainer("sql-filter", "/pk");

        var item1 = new JObject
        {
            ["id"] = "1", ["pk"] = "p",
            ["val"] = "match", ["Val"] = "no-match"
        };
        var item2 = new JObject
        {
            ["id"] = "2", ["pk"] = "p",
            ["val"] = "no-match", ["Val"] = "match"
        };

        await container.CreateItemAsync(item1, new PartitionKey("p"));
        await container.CreateItemAsync(item2, new PartitionKey("p"));

        // Filter on lowercase 'val'
        var query = new QueryDefinition("SELECT c.id FROM c WHERE c.val = 'match'");
        var iter = container.GetItemQueryIterator<JObject>(query);
        var results = new List<JObject>();
        while (iter.HasMoreResults)
        {
            var page = await iter.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle();
        results[0]["id"]!.ToString().Should().Be("1");
    }

    [Fact]
    public async Task SqlQuery_PascalCasePropertyName_NotCamelCased()
    {
        var container = new InMemoryContainer("sql-pascal", "/pk");

        var item = new PascalCaseDoc
        {
            Id = "1", Pk = "p",
            FirstName = "Ada", LastName = "Lovelace", ItemCount = 1
        };

        await container.CreateItemAsync(item, new PartitionKey("p"));

        // Query using PascalCase property name (should match the stored property)
        var query = new QueryDefinition("SELECT c.FirstName, c.LastName FROM c");
        var iter = container.GetItemQueryIterator<JObject>(query);
        var results = new List<JObject>();
        while (iter.HasMoreResults)
        {
            var page = await iter.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle();
        results[0]["FirstName"]!.ToString().Should().Be("Ada");
        results[0]["LastName"]!.ToString().Should().Be("Lovelace");
    }

    [Fact]
    public async Task SqlQuery_OrderBy_CaseSensitiveProperty()
    {
        var container = new InMemoryContainer("sql-order", "/pk");

        await container.CreateItemAsync(new JObject
        {
            ["id"] = "1", ["pk"] = "p", ["s"] = "beta", ["S"] = "alpha"
        }, new PartitionKey("p"));
        await container.CreateItemAsync(new JObject
        {
            ["id"] = "2", ["pk"] = "p", ["s"] = "alpha", ["S"] = "beta"
        }, new PartitionKey("p"));

        // ORDER BY lowercase s
        var query = new QueryDefinition("SELECT c.id, c.s FROM c ORDER BY c.s");
        var iter = container.GetItemQueryIterator<JObject>(query);
        var results = new List<JObject>();
        while (iter.HasMoreResults)
        {
            var page = await iter.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        results[0]["id"]!.ToString().Should().Be("2"); // alpha first
        results[1]["id"]!.ToString().Should().Be("1"); // beta second
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  8. LINQ queries
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Linq_FilterOnPascalCaseProperty_Works()
    {
        var container = new InMemoryContainer("linq-pascal", "/pk");

        var item = new PascalCaseDoc
        {
            Id = "1", Pk = "p", FirstName = "Grace", LastName = "Hopper", ItemCount = 99
        };

        await container.CreateItemAsync(item, new PartitionKey("p"));

        var queryable = container.GetItemLinqQueryable<PascalCaseDoc>(allowSynchronousQueryExecution: true);
        var results = queryable.Where(d => d.FirstName == "Grace").ToList();

        results.Should().ContainSingle();
        results[0].LastName.Should().Be("Hopper");
        results[0].ItemCount.Should().Be(99);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  9. Patch operations on case-sensitive property paths
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Patch_SetCaseSensitiveProperty_LowercaseD()
    {
        var container = new InMemoryContainer("patch-case-d", "/pk");

        var jObj = new JObject
        {
            ["id"] = "1", ["pk"] = "p",
            ["d"] = "original", ["D"] = "ORIGINAL"
        };
        await container.CreateItemAsync(jObj, new PartitionKey("p"));

        // Patch only lowercase d
        var patchOps = new[] { PatchOperation.Set("/d", "patched-lower") };
        await container.PatchItemAsync<JObject>("1", new PartitionKey("p"), patchOps);

        var response = await container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
        response.Resource["d"]!.ToString().Should().Be("patched-lower");
        response.Resource["D"]!.ToString().Should().Be("ORIGINAL"); // Must NOT change
    }

    [Fact]
    public async Task Patch_SetCaseSensitiveProperty_UppercaseD()
    {
        var container = new InMemoryContainer("patch-case-D", "/pk");

        var jObj = new JObject
        {
            ["id"] = "1", ["pk"] = "p",
            ["d"] = "original", ["D"] = "ORIGINAL"
        };
        await container.CreateItemAsync(jObj, new PartitionKey("p"));

        // Patch only uppercase D
        var patchOps = new[] { PatchOperation.Set("/D", "PATCHED-UPPER") };
        await container.PatchItemAsync<JObject>("1", new PartitionKey("p"), patchOps);

        var response = await container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
        response.Resource["d"]!.ToString().Should().Be("original"); // Must NOT change
        response.Resource["D"]!.ToString().Should().Be("PATCHED-UPPER");
    }

    [Fact]
    public async Task Patch_AddNewCaseSensitiveProperty_PreservesCasing()
    {
        var container = new InMemoryContainer("patch-add", "/pk");

        var jObj = new JObject
        {
            ["id"] = "1", ["pk"] = "p", ["existing"] = "data"
        };
        await container.CreateItemAsync(jObj, new PartitionKey("p"));

        var patchOps = new[]
        {
            PatchOperation.Add("/NewField", "PascalValue"),
            PatchOperation.Add("/newField", "camelValue")
        };
        await container.PatchItemAsync<JObject>("1", new PartitionKey("p"), patchOps);

        var response = await container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
        response.Resource["NewField"]!.ToString().Should().Be("PascalValue");
        response.Resource["newField"]!.ToString().Should().Be("camelValue");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  10. [JsonProperty] attribute interaction
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task JsonPropertyAttribute_OverridesPropertyName_Correctly()
    {
        var container = new InMemoryContainer("json-attr", "/pk");

        var item = new JsonPropertyOverrideDoc
        {
            Id = "1", Pk = "p",
            PascalCaseProperty = "custom-value",
            quietProperty = "loud-value",
            AnotherProperty = "mixed-value"
        };

        await container.CreateItemAsync(item, new PartitionKey("p"));
        var response = await container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
        var result = response.Resource;

        // [JsonProperty] specified names must be used
        result["custom_name"]!.ToString().Should().Be("custom-value");
        result["LOUD_NAME"]!.ToString().Should().Be("loud-value");
        result["MiXeD"]!.ToString().Should().Be("mixed-value");

        // Original C# property names must NOT appear
        result["PascalCaseProperty"].Should().BeNull();
        result["quietProperty"].Should().BeNull();
        result["AnotherProperty"].Should().BeNull();
    }

    [Fact]
    public async Task JsonPropertyAttribute_RoundTripsCorrectly()
    {
        var container = new InMemoryContainer("json-attr-rt", "/pk");

        var item = new JsonPropertyOverrideDoc
        {
            Id = "1", Pk = "p",
            PascalCaseProperty = "val1",
            quietProperty = "val2",
            AnotherProperty = "val3"
        };

        await container.CreateItemAsync(item, new PartitionKey("p"));
        var response = await container.ReadItemAsync<JsonPropertyOverrideDoc>("1", new PartitionKey("p"));
        var result = response.Resource;

        result.PascalCaseProperty.Should().Be("val1");
        result.quietProperty.Should().Be("val2");
        result.AnotherProperty.Should().Be("val3");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  11. Deeply nested objects with case-sensitive properties
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task DeepNesting_PreservesCaseSensitiveProperties()
    {
        var container = new InMemoryContainer("deep-nest", "/pk");

        var item = new NestedCaseSensitive
        {
            Id = "1", Pk = "p",
            Level1 = new InnerLevel
            {
                x = "inner-lower", X = "inner-UPPER",
                Deep = new DeepLevel
                {
                    y = "deep-lower", Y = "deep-UPPER"
                }
            }
        };

        await container.CreateItemAsync(item, new PartitionKey("p"));
        var response = await container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
        var result = response.Resource;

        var level1 = result["Level1"] as JObject;
        level1.Should().NotBeNull();
        level1!["x"]!.ToString().Should().Be("inner-lower");
        level1["X"]!.ToString().Should().Be("inner-UPPER");

        var deep = level1["Deep"] as JObject;
        deep.Should().NotBeNull();
        deep!["y"]!.ToString().Should().Be("deep-lower");
        deep["Y"]!.ToString().Should().Be("deep-UPPER");
    }

    [Fact]
    public async Task DeepNesting_StrongTypeRoundTrip()
    {
        var container = new InMemoryContainer("deep-nest-rt", "/pk");

        var item = new NestedCaseSensitive
        {
            Id = "1", Pk = "p",
            Level1 = new InnerLevel
            {
                x = "a", X = "B",
                Deep = new DeepLevel { y = "c", Y = "D" }
            }
        };

        await container.CreateItemAsync(item, new PartitionKey("p"));
        var response = await container.ReadItemAsync<NestedCaseSensitive>("1", new PartitionKey("p"));
        var result = response.Resource;

        result.Level1.x.Should().Be("a");
        result.Level1.X.Should().Be("B");
        result.Level1.Deep.y.Should().Be("c");
        result.Level1.Deep.Y.Should().Be("D");
    }

    [Fact]
    public async Task SqlQuery_OnNestedCaseSensitiveProperties()
    {
        var container = new InMemoryContainer("deep-query", "/pk");

        var jObj = new JObject
        {
            ["id"] = "1", ["pk"] = "p",
            ["nested"] = new JObject
            {
                ["x"] = "find-me",
                ["X"] = "not-me"
            }
        };
        await container.CreateItemAsync(jObj, new PartitionKey("p"));

        var query = new QueryDefinition(
            "SELECT c.nested.x AS lowerVal, c.nested.X AS upperVal FROM c");
        var iter = container.GetItemQueryIterator<JObject>(query);
        var results = new List<JObject>();
        while (iter.HasMoreResults)
        {
            var page = await iter.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().ContainSingle();
        results[0]["lowerVal"]!.ToString().Should().Be("find-me");
        results[0]["upperVal"]!.ToString().Should().Be("not-me");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  12. Arrays containing objects with case-sensitive properties
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ArraysOfObjects_PreserveCaseSensitiveProperties()
    {
        var container = new InMemoryContainer("array-case", "/pk");

        var jObj = new JObject
        {
            ["id"] = "1", ["pk"] = "p",
            ["items"] = new JArray(
                new JObject { ["x"] = "a1", ["X"] = "A1" },
                new JObject { ["x"] = "a2", ["X"] = "A2" }
            )
        };

        await container.CreateItemAsync(jObj, new PartitionKey("p"));
        var response = await container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
        var items = response.Resource["items"] as JArray;

        items.Should().NotBeNull();
        items!.Count.Should().Be(2);
        items[0]["x"]!.ToString().Should().Be("a1");
        items[0]["X"]!.ToString().Should().Be("A1");
        items[1]["x"]!.ToString().Should().Be("a2");
        items[1]["X"]!.ToString().Should().Be("A2");
    }

    [Fact]
    public async Task SqlQuery_WithJoin_OnArrayWithCaseSensitiveProperties()
    {
        var container = new InMemoryContainer("array-join", "/pk");

        var jObj = new JObject
        {
            ["id"] = "1", ["pk"] = "p",
            ["records"] = new JArray(
                new JObject { ["n"] = "rec1-lower", ["N"] = "REC1-upper" },
                new JObject { ["n"] = "rec2-lower", ["N"] = "REC2-upper" }
            )
        };

        await container.CreateItemAsync(jObj, new PartitionKey("p"));

        var query = new QueryDefinition(
            "SELECT r.n, r.N FROM c JOIN r IN c.records");
        var iter = container.GetItemQueryIterator<JObject>(query);
        var results = new List<JObject>();
        while (iter.HasMoreResults)
        {
            var page = await iter.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        results[0]["n"]!.ToString().Should().Be("rec1-lower");
        results[0]["N"]!.ToString().Should().Be("REC1-upper");
        results[1]["n"]!.ToString().Should().Be("rec2-lower");
        results[1]["N"]!.ToString().Should().Be("REC2-upper");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Additional edge cases
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ReadMany_PreservesCaseSensitiveProperties()
    {
        var container = new InMemoryContainer("readmany-case", "/pk");

        await container.CreateItemAsync(new JObject
        {
            ["id"] = "rm1", ["pk"] = "p",
            ["d"] = "doc1-lower", ["D"] = "DOC1-upper"
        }, new PartitionKey("p"));

        await container.CreateItemAsync(new JObject
        {
            ["id"] = "rm2", ["pk"] = "p",
            ["d"] = "doc2-lower", ["D"] = "DOC2-upper"
        }, new PartitionKey("p"));

        var readItems = new List<(string id, PartitionKey pk)>
        {
            ("rm1", new PartitionKey("p")),
            ("rm2", new PartitionKey("p"))
        };

        var response = await container.ReadManyItemsAsync<JObject>(readItems);
        var results = response.Resource.ToList();

        results.Should().HaveCount(2);
        foreach (var result in results)
        {
            result["d"].Should().NotBeNull();
            result["D"].Should().NotBeNull();
            result["d"]!.ToString().Should().NotBe(result["D"]!.ToString());
        }
    }

    [Fact]
    public async Task StreamApi_PreservesCaseSensitiveProperties()
    {
        var container = new InMemoryContainer("stream-case", "/pk");

        var json = """{"id":"s1","pk":"p","d":"stream-lower","D":"STREAM-UPPER"}""";
        var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

        await container.CreateItemStreamAsync(stream, new PartitionKey("p"));

        var response = await container.ReadItemAsync<JObject>("s1", new PartitionKey("p"));
        response.Resource["d"]!.ToString().Should().Be("stream-lower");
        response.Resource["D"]!.ToString().Should().Be("STREAM-UPPER");
    }

    [Fact]
    public async Task Patch_NestedCaseSensitivePath_UpdatesCorrectProperty()
    {
        var container = new InMemoryContainer("patch-nested", "/pk");

        var jObj = new JObject
        {
            ["id"] = "1", ["pk"] = "p",
            ["nested"] = new JObject
            {
                ["v"] = "lower-original",
                ["V"] = "UPPER-original"
            }
        };
        await container.CreateItemAsync(jObj, new PartitionKey("p"));

        // Patch only the lowercase nested property
        var patchOps = new[] { PatchOperation.Set("/nested/v", "lower-patched") };
        await container.PatchItemAsync<JObject>("1", new PartitionKey("p"), patchOps);

        var response = await container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
        var nested = response.Resource["nested"] as JObject;
        nested!["v"]!.ToString().Should().Be("lower-patched");
        nested["V"]!.ToString().Should().Be("UPPER-original"); // Must NOT change
    }

    [Fact]
    public async Task MultipleDocuments_WithDifferentPropertyCasing_IndependentlyCorrect()
    {
        var container = new InMemoryContainer("multi-doc", "/pk");

        await container.CreateItemAsync(new JObject
        {
            ["id"] = "1", ["pk"] = "p", ["Status"] = "Active"
        }, new PartitionKey("p"));

        await container.CreateItemAsync(new JObject
        {
            ["id"] = "2", ["pk"] = "p", ["status"] = "inactive"
        }, new PartitionKey("p"));

        var r1 = await container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
        var r2 = await container.ReadItemAsync<JObject>("2", new PartitionKey("p"));

        // Doc 1: "Status" (PascalCase)
        r1.Resource["Status"]!.ToString().Should().Be("Active");
        r1.Resource["status"].Should().BeNull(); // must NOT exist

        // Doc 2: "status" (camelCase)
        r2.Resource["status"]!.ToString().Should().Be("inactive");
        r2.Resource["Status"].Should().BeNull(); // must NOT exist
    }

    [Fact]
    public async Task SingleLetterProperties_AllTwentySixPreserved()
    {
        var container = new InMemoryContainer("26-letters", "/pk");

        var jObj = new JObject { ["id"] = "1", ["pk"] = "p" };

        // Add a-z and A-Z as property names
        for (var c = 'a'; c <= 'z'; c++)
        {
            jObj[c.ToString()] = $"lower-{c}";
            jObj[char.ToUpper(c).ToString()] = $"upper-{char.ToUpper(c)}";
        }

        await container.CreateItemAsync(jObj, new PartitionKey("p"));
        var response = await container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
        var result = response.Resource;

        for (var c = 'a'; c <= 'z'; c++)
        {
            result[c.ToString()]!.ToString().Should().Be($"lower-{c}",
                $"lowercase property '{c}' should be preserved");
            result[char.ToUpper(c).ToString()]!.ToString().Should().Be($"upper-{char.ToUpper(c)}",
                $"uppercase property '{char.ToUpper(c)}' should be preserved");
        }
    }

    [Fact]
    public async Task Batch_PatchOnCaseSensitiveProperties()
    {
        var container = new InMemoryContainer("batch-patch", "/pk");

        var jObj = new JObject
        {
            ["id"] = "bp1", ["pk"] = "p",
            ["v"] = "low-orig", ["V"] = "UP-orig"
        };
        await container.CreateItemAsync(jObj, new PartitionKey("p"));

        var batch = container.CreateTransactionalBatch(new PartitionKey("p"));
        batch.PatchItem("bp1", new List<PatchOperation>
        {
            PatchOperation.Set("/v", "low-patched")
        });

        using var batchResponse = await batch.ExecuteAsync();
        batchResponse.StatusCode.Should().Be(HttpStatusCode.OK);

        var response = await container.ReadItemAsync<JObject>("bp1", new PartitionKey("p"));
        response.Resource["v"]!.ToString().Should().Be("low-patched");
        response.Resource["V"]!.ToString().Should().Be("UP-orig"); // Must NOT change
    }

    [Fact]
    public async Task Enum_WithCaseSensitiveProperties_OnSameDocument()
    {
        var container = new InMemoryContainer("enum-mixed", "/pk");

        var jObj = new JObject
        {
            ["id"] = "1", ["pk"] = "p",
            ["status"] = "Low",
            ["Status"] = "High"
        };

        await container.CreateItemAsync(jObj, new PartitionKey("p"));
        var response = await container.ReadItemAsync<JObject>("1", new PartitionKey("p"));

        response.Resource["status"]!.ToString().Should().Be("Low");
        response.Resource["Status"]!.ToString().Should().Be("High");
    }
}
