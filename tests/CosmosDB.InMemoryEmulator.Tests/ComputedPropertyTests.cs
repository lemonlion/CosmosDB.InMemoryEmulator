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
}
