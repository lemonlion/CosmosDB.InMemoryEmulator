using System.Collections.ObjectModel;
using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for Cosmos DB computed properties — virtual top-level properties defined on
/// a container with a Name and Query, evaluated at query time but not persisted on documents.
/// </summary>
public class ComputedPropertyTests
{
    private static InMemoryContainer CreateContainerWithComputedProperties(
        params (string Name, string Query)[] definitions)
    {
        var props = new ContainerProperties("test", "/pk")
        {
            ComputedProperties = new Collection<ComputedProperty>(
                definitions.Select(d => new ComputedProperty
                {
                    Name = d.Name,
                    Query = d.Query
                }).ToList())
        };
        return new InMemoryContainer(props);
    }

    private static async Task SeedItems(InMemoryContainer container, params object[] items)
    {
        foreach (var item in items)
        {
            var jObj = JObject.FromObject(item);
            await container.CreateItemAsync(jObj, new PartitionKey(jObj["pk"]!.ToString()));
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  SELECT projection
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ComputedProperty_ProjectedInSelect()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

        await SeedItems(container,
            new { id = "1", pk = "p", name = "Alice" },
            new { id = "2", pk = "p", name = "BOB" });

        var query = new QueryDefinition("SELECT c.cp_lowerName FROM c");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(2);
        results.Select(r => r["cp_lowerName"]?.ToString()).Should().BeEquivalentTo("alice", "bob");
    }

    [Fact]
    public async Task ComputedProperty_NotIncludedInSelectStar()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

        await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

        var query = new QueryDefinition("SELECT * FROM c");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();
        var doc = response.First();

        doc["cp_lowerName"].Should().BeNull("SELECT * must not include computed properties");
        doc["name"]!.ToString().Should().Be("Alice");
    }

    [Fact]
    public async Task ComputedProperty_ExplicitSelectWithPersistedAndComputed()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

        await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

        var query = new QueryDefinition("SELECT c.name, c.cp_lowerName FROM c");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();
        var doc = response.First();

        doc["name"]!.ToString().Should().Be("Alice");
        doc["cp_lowerName"]!.ToString().Should().Be("alice");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  WHERE clause
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ComputedProperty_UsedInWhereClause()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

        await SeedItems(container,
            new { id = "1", pk = "p", name = "Alice" },
            new { id = "2", pk = "p", name = "Bob" });

        var query = new QueryDefinition("SELECT c.id FROM c WHERE c.cp_lowerName = 'alice'");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();

        response.Should().HaveCount(1);
        response.First()["id"]!.ToString().Should().Be("1");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  ORDER BY
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ComputedProperty_UsedInOrderBy()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

        await SeedItems(container,
            new { id = "1", pk = "p", name = "Charlie" },
            new { id = "2", pk = "p", name = "Alice" },
            new { id = "3", pk = "p", name = "Bob" });

        var query = new QueryDefinition(
            "SELECT c.id FROM c ORDER BY c.cp_lowerName ASC");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();

        response.Select(r => r["id"]!.ToString()).Should().ContainInOrder("2", "3", "1");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  GROUP BY
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ComputedProperty_UsedInGroupBy()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_category", "SELECT VALUE LOWER(c.category) FROM c"));

        await SeedItems(container,
            new { id = "1", pk = "p", category = "Books" },
            new { id = "2", pk = "p", category = "books" },
            new { id = "3", pk = "p", category = "Electronics" });

        var query = new QueryDefinition(
            "SELECT c.cp_category, COUNT(1) AS cnt FROM c GROUP BY c.cp_category");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();
        var results = response.ToList();

        var booksGroup = results.First(r => r["cp_category"]!.ToString() == "books");
        booksGroup["cnt"]!.Value<int>().Should().Be(2);

        var elecGroup = results.First(r => r["cp_category"]!.ToString() == "electronics");
        elecGroup["cnt"]!.Value<int>().Should().Be(1);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Null / undefined handling
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ComputedProperty_EvaluatesToNull_WhenSourceMissing()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

        // Item has no "name" field
        await SeedItems(container, new { id = "1", pk = "p", age = 30 });

        var query = new QueryDefinition("SELECT c.id, c.cp_lowerName FROM c");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();
        var doc = response.First();

        doc["id"]!.ToString().Should().Be("1");
        // LOWER(undefined) evaluates to null — the property is present but null-valued
        doc["cp_lowerName"]!.Type.Should().Be(JTokenType.Null);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Multiple computed properties
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ComputedProperty_MultipleOnSameContainer()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"),
            ("cp_upperName", "SELECT VALUE UPPER(c.name) FROM c"));

        await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

        var query = new QueryDefinition("SELECT c.cp_lowerName, c.cp_upperName FROM c");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();
        var doc = response.First();

        doc["cp_lowerName"]!.ToString().Should().Be("alice");
        doc["cp_upperName"]!.ToString().Should().Be("ALICE");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  ReplaceContainerAsync updates
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ComputedProperty_UpdatedViaReplaceContainer()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

        await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

        // Verify original works
        var query = new QueryDefinition("SELECT c.cp_lowerName FROM c");
        var iter1 = container.GetItemQueryIterator<JObject>(query);
        var res1 = await iter1.ReadNextAsync();
        res1.First()["cp_lowerName"]!.ToString().Should().Be("alice");

        // Replace with a different computed property
        var readResp = await container.ReadContainerAsync();
        var updatedProps = readResp.Resource;
        updatedProps.ComputedProperties = new Collection<ComputedProperty>
        {
            new ComputedProperty
            {
                Name = "cp_upperName",
                Query = "SELECT VALUE UPPER(c.name) FROM c"
            }
        };
        await container.ReplaceContainerAsync(updatedProps);

        // Old computed property no longer exists
        var query2 = new QueryDefinition("SELECT c.cp_upperName FROM c");
        var iter2 = container.GetItemQueryIterator<JObject>(query2);
        var res2 = await iter2.ReadNextAsync();
        res2.First()["cp_upperName"]!.ToString().Should().Be("ALICE");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Arithmetic expression
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ComputedProperty_ArithmeticExpression()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_discountedPrice", "SELECT VALUE c.price * 0.8 FROM c"));

        await SeedItems(container, new { id = "1", pk = "p", price = 100.0 });

        var query = new QueryDefinition("SELECT c.cp_discountedPrice FROM c");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();
        var doc = response.First();

        doc["cp_discountedPrice"]!.Value<double>().Should().Be(80.0);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  String concatenation
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ComputedProperty_ConcatExpression()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_fullName", "SELECT VALUE CONCAT(c.first, ' ', c.last) FROM c"));

        await SeedItems(container, new { id = "1", pk = "p", first = "Jane", last = "Doe" });

        var query = new QueryDefinition("SELECT c.cp_fullName FROM c");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();

        response.First()["cp_fullName"]!.ToString().Should().Be("Jane Doe");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Partition-scoped queries
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ComputedProperty_WithPartitionKeyFilter()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

        await SeedItems(container,
            new { id = "1", pk = "p1", name = "Alice" },
            new { id = "2", pk = "p2", name = "Bob" });

        var query = new QueryDefinition("SELECT c.cp_lowerName FROM c");
        var options = new QueryRequestOptions { PartitionKey = new PartitionKey("p1") };
        var iterator = container.GetItemQueryIterator<JObject>(query, requestOptions: options);
        var response = await iterator.ReadNextAsync();

        response.Should().HaveCount(1);
        response.First()["cp_lowerName"]!.ToString().Should().Be("alice");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  No regression for containers without computed properties
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ComputedProperty_NoComputedProperties_ZeroOverhead()
    {
        var container = new InMemoryContainer("test", "/pk");

        await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

        var query = new QueryDefinition("SELECT c.name FROM c");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();

        response.Should().HaveCount(1);
        response.First()["name"]!.ToString().Should().Be("Alice");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Container read returns computed properties
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ComputedProperty_StoredAndReturnedOnContainerRead()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

        var response = await container.ReadContainerAsync();
        var cps = response.Resource.ComputedProperties;

        cps.Should().HaveCount(1);
        cps[0].Name.Should().Be("cp_lowerName");
        cps[0].Query.Should().Be("SELECT VALUE LOWER(c.name) FROM c");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Document read does not include computed properties
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ComputedProperty_DoesNotPersistOnDocument()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

        await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

        // Direct point read — computed property should NOT be on the document
        var readResponse = await container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
        readResponse.Resource["cp_lowerName"].Should().BeNull(
            "computed properties are virtual and not persisted on documents");
        readResponse.Resource["name"]!.ToString().Should().Be("Alice");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Phase 2: Query clause combinations
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ComputedProperty_UsedWithDistinct()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_category", "SELECT VALUE LOWER(c.category) FROM c"));

        await SeedItems(container,
            new { id = "1", pk = "p", category = "Books" },
            new { id = "2", pk = "p", category = "books" },
            new { id = "3", pk = "p", category = "Electronics" });

        var query = new QueryDefinition("SELECT DISTINCT c.cp_category FROM c");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(2);
    }

    [Fact]
    public async Task ComputedProperty_UsedWithTop()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

        await SeedItems(container,
            new { id = "1", pk = "p", name = "Charlie" },
            new { id = "2", pk = "p", name = "Alice" },
            new { id = "3", pk = "p", name = "Bob" });

        var query = new QueryDefinition(
            "SELECT TOP 2 c.cp_lowerName FROM c ORDER BY c.cp_lowerName");
        var results = new List<JObject>();
        var iterator = container.GetItemQueryIterator<JObject>(query);
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(2);
        results[0]["cp_lowerName"]!.ToString().Should().Be("alice");
        results[1]["cp_lowerName"]!.ToString().Should().Be("bob");
    }

    [Fact]
    public async Task ComputedProperty_UsedWithOffsetLimit()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

        await SeedItems(container,
            new { id = "1", pk = "p", name = "Alice" },
            new { id = "2", pk = "p", name = "Bob" },
            new { id = "3", pk = "p", name = "Charlie" });

        var query = new QueryDefinition(
            "SELECT c.cp_lowerName FROM c ORDER BY c.cp_lowerName OFFSET 1 LIMIT 1");
        var results = new List<JObject>();
        var iterator = container.GetItemQueryIterator<JObject>(query);
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().ContainSingle()
            .Which["cp_lowerName"]!.ToString().Should().Be("bob");
    }

    [Fact]
    public async Task ComputedProperty_UsedWithValueSelect()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

        await SeedItems(container,
            new { id = "1", pk = "p", name = "Alice" },
            new { id = "2", pk = "p", name = "Bob" });

        var query = new QueryDefinition("SELECT VALUE c.cp_lowerName FROM c");
        var iterator = container.GetItemQueryIterator<JToken>(query);
        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Select(r => r.ToString()).Should().BeEquivalentTo("alice", "bob");
    }

    [Fact]
    public async Task ComputedProperty_UsedInAggregateFunction()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_discountedPrice", "SELECT VALUE c.price * 0.8 FROM c"));

        await SeedItems(container,
            new { id = "1", pk = "p", price = 100.0 },
            new { id = "2", pk = "p", price = 200.0 });

        var query = new QueryDefinition("SELECT VALUE SUM(c.cp_discountedPrice) FROM c");
        var iterator = container.GetItemQueryIterator<JToken>(query);
        var results = new List<JToken>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().ContainSingle().Which.Value<double>().Should().Be(240.0);
    }

    [Fact]
    public async Task ComputedProperty_UsedWithAlias()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

        await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

        var query = new QueryDefinition("SELECT c.cp_lowerName AS lowered FROM c");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();

        response.First()["lowered"]!.ToString().Should().Be("alice");
    }

    [Fact]
    public async Task ComputedProperty_UsedInWhereWithComparison()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_discountedPrice", "SELECT VALUE c.price * 0.8 FROM c"));

        await SeedItems(container,
            new { id = "1", pk = "p", price = 100.0 },
            new { id = "2", pk = "p", price = 200.0 });

        var query = new QueryDefinition("SELECT c.id FROM c WHERE c.cp_discountedPrice < 100");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();

        response.Should().ContainSingle().Which["id"]!.ToString().Should().Be("1");
    }

    [Fact]
    public async Task ComputedProperty_UsedInWhereWithBooleanLogic()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

        await SeedItems(container,
            new { id = "1", pk = "p", name = "Alice" },
            new { id = "2", pk = "p", name = "Bob" },
            new { id = "3", pk = "p", name = "Charlie" });

        var query = new QueryDefinition(
            "SELECT c.id FROM c WHERE c.cp_lowerName = 'alice' OR c.cp_lowerName = 'bob'");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();

        response.Should().HaveCount(2);
    }

    [Fact]
    public async Task ComputedProperty_UsedInExpressionInSelect()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_discountedPrice", "SELECT VALUE c.price * 0.8 FROM c"));

        await SeedItems(container, new { id = "1", pk = "p", price = 100.0 });

        var query = new QueryDefinition(
            "SELECT c.price - c.cp_discountedPrice AS savings FROM c");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();

        response.First()["savings"]!.Value<double>().Should().BeApproximately(20.0, 0.01);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Phase 3: Expression types
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ComputedProperty_NestedPropertyAccess()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_city", "SELECT VALUE c.address.city FROM c"));

        await SeedItems(container,
            new { id = "1", pk = "p", address = new { city = "London" } });

        var query = new QueryDefinition("SELECT c.cp_city FROM c");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();

        response.First()["cp_city"]!.ToString().Should().Be("London");
    }

    [Fact]
    public async Task ComputedProperty_MathFunctions()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_rounded", "SELECT VALUE ROUND(c.price) FROM c"));

        await SeedItems(container, new { id = "1", pk = "p", price = 19.99 });

        var query = new QueryDefinition("SELECT c.cp_rounded FROM c");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();

        response.First()["cp_rounded"]!.Value<double>().Should().Be(20.0);
    }

    [Fact]
    public async Task ComputedProperty_StringLengthFunction()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_nameLen", "SELECT VALUE LENGTH(c.name) FROM c"));

        await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

        var query = new QueryDefinition("SELECT c.cp_nameLen FROM c");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();

        response.First()["cp_nameLen"]!.Value<int>().Should().Be(5);
    }

    [Fact]
    public async Task ComputedProperty_BooleanExpression()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_hasAli", "SELECT VALUE CONTAINS(c.name, 'ali', true) FROM c"));

        await SeedItems(container,
            new { id = "1", pk = "p", name = "Alice" },
            new { id = "2", pk = "p", name = "Bob" });

        var query = new QueryDefinition("SELECT c.id, c.cp_hasAli FROM c ORDER BY c.id");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();
        var results = response.ToList();

        results[0]["cp_hasAli"]!.Value<bool>().Should().BeTrue();
        results[1]["cp_hasAli"]!.Value<bool>().Should().BeFalse();
    }

    [Fact]
    public async Task ComputedProperty_TypeCheckFunction()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_isStr", "SELECT VALUE IS_STRING(c.name) FROM c"));

        await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

        var query = new QueryDefinition("SELECT c.cp_isStr FROM c");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();

        response.First()["cp_isStr"]!.Value<bool>().Should().BeTrue();
    }

    [Fact]
    public async Task ComputedProperty_ArrayFunction()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_tagCount", "SELECT VALUE ARRAY_LENGTH(c.tags) FROM c"));

        await SeedItems(container,
            new { id = "1", pk = "p", tags = new[] { "a", "b", "c" } });

        var query = new QueryDefinition("SELECT c.cp_tagCount FROM c");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();

        response.First()["cp_tagCount"]!.Value<int>().Should().Be(3);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Phase 4: Edge cases & lifecycle
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ComputedProperty_EmptyComputedPropertiesCollection()
    {
        var props = new ContainerProperties("test", "/pk")
        {
            ComputedProperties = new Collection<ComputedProperty>()
        };
        var container = new InMemoryContainer(props);

        await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

        var query = new QueryDefinition("SELECT c.name FROM c");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();

        response.Should().ContainSingle().Which["name"]!.ToString().Should().Be("Alice");
    }

    [Fact]
    public async Task ComputedProperty_EvaluatesPerItem()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_discounted", "SELECT VALUE c.price * 0.9 FROM c"));

        await SeedItems(container,
            new { id = "1", pk = "p", price = 100.0 },
            new { id = "2", pk = "p", price = 200.0 },
            new { id = "3", pk = "p", price = 300.0 });

        var query = new QueryDefinition(
            "SELECT c.id, c.cp_discounted FROM c ORDER BY c.id");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();
        var results = response.ToList();

        results[0]["cp_discounted"]!.Value<double>().Should().Be(90.0);
        results[1]["cp_discounted"]!.Value<double>().Should().Be(180.0);
        results[2]["cp_discounted"]!.Value<double>().Should().Be(270.0);
    }

    [Fact]
    public async Task ComputedProperty_ReEvaluatesAfterDocumentUpdate()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

        await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

        // Verify initial value
        var query = new QueryDefinition("SELECT c.cp_lowerName FROM c");
        var iter1 = container.GetItemQueryIterator<JObject>(query);
        (await iter1.ReadNextAsync()).First()["cp_lowerName"]!.ToString().Should().Be("alice");

        // Update the document
        await container.UpsertItemAsync(
            JObject.FromObject(new { id = "1", pk = "p", name = "Bob" }),
            new PartitionKey("p"));

        // CP should reflect the update
        var iter2 = container.GetItemQueryIterator<JObject>(query);
        (await iter2.ReadNextAsync()).First()["cp_lowerName"]!.ToString().Should().Be("bob");
    }

    [Fact]
    public async Task ComputedProperty_CrossPartitionQuery()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

        await SeedItems(container,
            new { id = "1", pk = "p1", name = "Alice" },
            new { id = "2", pk = "p2", name = "Bob" });

        // Cross-partition (no partition key filter)
        var query = new QueryDefinition("SELECT c.cp_lowerName FROM c ORDER BY c.cp_lowerName");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }

        results.Should().HaveCount(2);
        results[0]["cp_lowerName"]!.ToString().Should().Be("alice");
        results[1]["cp_lowerName"]!.ToString().Should().Be("bob");
    }

    [Fact]
    public async Task ComputedProperty_OldCPRemovedAfterReplace()
    {
        var container = CreateContainerWithComputedProperties(
            ("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

        await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

        // Replace with completely different CP
        var readResp = await container.ReadContainerAsync();
        var props = readResp.Resource;
        props.ComputedProperties = new Collection<ComputedProperty>
        {
            new() { Name = "cp_upperName", Query = "SELECT VALUE UPPER(c.name) FROM c" }
        };
        await container.ReplaceContainerAsync(props);

        // Old CP should return null
        var query = new QueryDefinition("SELECT c.cp_lowerName, c.cp_upperName FROM c");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();
        var doc = response.First();

        // Old CP should not evaluate to "alice" anymore — it is no longer a computed property
        var oldVal = doc["cp_lowerName"];
        if (oldVal is not null)
            oldVal.ToString().Should().NotBe("alice", "old CP should no longer be evaluated after replace");

        doc["cp_upperName"]!.ToString().Should().Be("ALICE");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Phase 5: Divergent behaviour
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact(Skip = "Emulator returns null instead of undefined for functions with missing-property " +
                 "inputs. LOWER(undefined) should evaluate to undefined (property absent), but " +
                 "emulator evaluates to null (property present with null value). Fixing requires " +
                 "UndefinedValue propagation through all 120+ SQL function implementations.")]
    public void ComputedProperty_UndefinedPropagation_RealCosmos() { }

    [Fact]
    public async Task ComputedProperty_UndefinedPropagation_EmulatorBehaviour()
    {
        // DIVERGENT: In real Cosmos DB, LOWER(undefined) → undefined (property absent)
        // EMULATOR: LOWER(undefined) → null (property present with null value)
        // Root cause: UndefinedValue.ToString() returns null → LOWER(null) → null
        var container = CreateContainerWithComputedProperties(
            ("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

        await SeedItems(container, new { id = "1", pk = "p", age = 30 }); // no "name" field

        var query = new QueryDefinition("SELECT c.id, c.cp_lowerName FROM c");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();
        var doc = response.First();

        // Emulator: property is present but null-valued
        doc["cp_lowerName"]!.Type.Should().Be(JTokenType.Null);
    }
}
