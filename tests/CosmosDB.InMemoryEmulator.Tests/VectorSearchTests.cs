using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for the VECTORDISTANCE SQL function, which computes similarity/distance
/// between two vectors. Supports cosine similarity, dot product, and Euclidean distance.
/// Signature: VECTORDISTANCE(vector1, vector2 [, bool_bruteForce] [, {distanceFunction:'cosine'}])
/// </summary>
public class VectorSearchTests
{
    private static async Task<InMemoryContainer> CreateContainerWithVectors()
    {
        var container = new InMemoryContainer("vector-test", "/pk");

        // Unit vectors along each axis — easy to reason about cosine similarity
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "x", pk = "a", embedding = new[] { 1.0, 0.0, 0.0 } }),
            new PartitionKey("a"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "y", pk = "a", embedding = new[] { 0.0, 1.0, 0.0 } }),
            new PartitionKey("a"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "z", pk = "a", embedding = new[] { 0.0, 0.0, 1.0 } }),
            new PartitionKey("a"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "neg-x", pk = "a", embedding = new[] { -1.0, 0.0, 0.0 } }),
            new PartitionKey("a"));

        return container;
    }

    private static async Task<List<JObject>> RunQuery(InMemoryContainer container, string sql)
    {
        var iterator = container.GetItemQueryIterator<JObject>(sql,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());
        return results;
    }

    // ─── Cosine Similarity ───────────────────────────────────────────────

    [Fact]
    public async Task VectorDistance_Cosine_IdenticalVectors_ReturnsOne()
    {
        var container = await CreateContainerWithVectors();
        var results = await RunQuery(container,
            "SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c WHERE c.id = 'x'");

        results.Should().ContainSingle();
        results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9,
            "cosine similarity of identical vectors should be 1.0");
    }

    [Fact]
    public async Task VectorDistance_Cosine_OrthogonalVectors_ReturnsZero()
    {
        var container = await CreateContainerWithVectors();
        var results = await RunQuery(container,
            "SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c WHERE c.id = 'y'");

        results.Should().ContainSingle();
        results[0]["score"]!.Value<double>().Should().BeApproximately(0.0, 1e-9,
            "cosine similarity of orthogonal vectors should be 0.0");
    }

    [Fact]
    public async Task VectorDistance_Cosine_OppositeVectors_ReturnsNegativeOne()
    {
        var container = await CreateContainerWithVectors();
        var results = await RunQuery(container,
            "SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c WHERE c.id = 'neg-x'");

        results.Should().ContainSingle();
        results[0]["score"]!.Value<double>().Should().BeApproximately(-1.0, 1e-9,
            "cosine similarity of opposite vectors should be -1.0");
    }

    // ─── Dot Product ─────────────────────────────────────────────────────

    [Fact]
    public async Task VectorDistance_DotProduct_ReturnsCorrectValue()
    {
        var container = new InMemoryContainer("vector-test", "/pk");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 2.0, 3.0, 4.0 } }),
            new PartitionKey("a"));

        // dot([2,3,4], [1,5,2]) = 2*1 + 3*5 + 4*2 = 2 + 15 + 8 = 25
        var results = await RunQuery(container,
            "SELECT VectorDistance(c.embedding, [1.0, 5.0, 2.0], false, {distanceFunction:'dotproduct'}) AS score FROM c");

        results.Should().ContainSingle();
        results[0]["score"]!.Value<double>().Should().BeApproximately(25.0, 1e-9);
    }

    // ─── Euclidean Distance ──────────────────────────────────────────────

    [Fact]
    public async Task VectorDistance_Euclidean_ReturnsCorrectValue()
    {
        var container = new InMemoryContainer("vector-test", "/pk");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0, 0.0 } }),
            new PartitionKey("a"));

        // euclidean([1,0,0], [0,1,0]) = sqrt((1-0)^2 + (0-1)^2 + (0-0)^2) = sqrt(2)
        var results = await RunQuery(container,
            "SELECT VectorDistance(c.embedding, [0.0, 1.0, 0.0], false, {distanceFunction:'euclidean'}) AS score FROM c");

        results.Should().ContainSingle();
        results[0]["score"]!.Value<double>().Should().BeApproximately(Math.Sqrt(2), 1e-9);
    }

    // ─── SELECT Projection ───────────────────────────────────────────────

    [Fact]
    public async Task VectorDistance_InSelectProjection_ReturnsSimilarityScore()
    {
        var container = await CreateContainerWithVectors();
        var results = await RunQuery(container,
            "SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c");

        results.Should().HaveCount(4, "all 4 documents should have a score computed");
        results.Should().Contain(r => r["id"]!.ToString() == "x" && Math.Abs(r["score"]!.Value<double>() - 1.0) < 1e-9);
        results.Should().Contain(r => r["id"]!.ToString() == "y" && Math.Abs(r["score"]!.Value<double>() - 0.0) < 1e-9);
    }

    // ─── ORDER BY ────────────────────────────────────────────────────────

    [Fact]
    public async Task VectorDistance_InOrderBy_SortsByScore()
    {
        var container = await CreateContainerWithVectors();

        // ORDER BY VectorDistance DESC — most similar first (highest cosine similarity)
        var results = await RunQuery(container,
            "SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c ORDER BY VectorDistance(c.embedding, [1.0, 0.0, 0.0]) DESC");

        results.Should().HaveCount(4);
        results[0]["id"]!.ToString().Should().Be("x", "identical vector should be first (score 1.0)");
        results[^1]["id"]!.ToString().Should().Be("neg-x", "opposite vector should be last (score -1.0)");
    }

    [Fact]
    public async Task VectorDistance_TopN_WithOrderBy_ReturnsClosest()
    {
        var container = await CreateContainerWithVectors();

        var results = await RunQuery(container,
            "SELECT TOP 2 c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c ORDER BY VectorDistance(c.embedding, [1.0, 0.0, 0.0]) DESC");

        results.Should().HaveCount(2);
        results[0]["id"]!.ToString().Should().Be("x");
    }

    // ─── Combined with WHERE ─────────────────────────────────────────────

    [Fact]
    public async Task VectorDistance_WithWhereClause_FiltersAndScores()
    {
        var container = await CreateContainerWithVectors();

        var results = await RunQuery(container,
            "SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c WHERE c.id != 'neg-x' ORDER BY VectorDistance(c.embedding, [1.0, 0.0, 0.0]) DESC");

        results.Should().HaveCount(3);
        results.Should().NotContain(r => r["id"]!.ToString() == "neg-x");
    }

    // ─── Edge Cases ──────────────────────────────────────────────────────

    [Fact]
    public async Task VectorDistance_MismatchedDimensions_ReturnsNull()
    {
        var container = new InMemoryContainer("vector-test", "/pk");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0 } }),
            new PartitionKey("a"));

        // Document has 2D vector, query has 3D vector — should return null
        var results = await RunQuery(container,
            "SELECT VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c");

        results.Should().ContainSingle();
        results[0]["score"]!.Type.Should().Be(JTokenType.Null,
            "mismatched dimensions should return null, not throw");
    }

    [Fact]
    public async Task VectorDistance_MissingEmbedding_ReturnsNull()
    {
        var container = new InMemoryContainer("vector-test", "/pk");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", name = "no-embedding" }),
            new PartitionKey("a"));

        var results = await RunQuery(container,
            "SELECT VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c");

        results.Should().ContainSingle();
        results[0]["score"]!.Type.Should().Be(JTokenType.Null,
            "missing vector property should return null");
    }

    // ─── Optional Parameters ─────────────────────────────────────────────

    [Fact]
    public async Task VectorDistance_DistanceFunctionOverride_UsesEuclidean()
    {
        var container = new InMemoryContainer("vector-test", "/pk");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 3.0, 0.0, 0.0 } }),
            new PartitionKey("a"));

        // Euclidean distance between [3,0,0] and [0,0,0] = 3.0
        var results = await RunQuery(container,
            "SELECT VectorDistance(c.embedding, [0.0, 0.0, 0.0], false, {distanceFunction:'euclidean'}) AS score FROM c");

        results.Should().ContainSingle();
        results[0]["score"]!.Value<double>().Should().BeApproximately(3.0, 1e-9);
    }

    [Fact]
    public async Task VectorDistance_BoolBruteForceParam_AcceptedAndIgnored()
    {
        var container = await CreateContainerWithVectors();

        // 3rd argument (true = brute force) should be accepted but has no effect in emulator
        var results = await RunQuery(container,
            "SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0], true) AS score FROM c WHERE c.id = 'x'");

        results.Should().ContainSingle();
        results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9);
    }

    // ─── High-Dimensional Vectors ────────────────────────────────────────

    [Fact]
    public async Task VectorDistance_HighDimensional_1536Dimensions()
    {
        var container = new InMemoryContainer("vector-test", "/pk");

        // Create a 1536-dim unit vector along first axis
        var vec1 = new double[1536];
        vec1[0] = 1.0;
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", embedding = vec1 }),
            new PartitionKey("a"));

        // Query with same vector — cosine similarity should be 1.0
        var queryVec = string.Join(",", vec1.Select(v => v.ToString("F1")));
        var results = await RunQuery(container,
            $"SELECT VectorDistance(c.embedding, [{queryVec}]) AS score FROM c");

        results.Should().ContainSingle();
        results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9);
    }

    // ─── Default Distance Function (Cosine) ──────────────────────────────

    [Fact]
    public async Task VectorDistance_DefaultDistanceFunction_IsCosine()
    {
        var container = await CreateContainerWithVectors();

        // No 4th argument — should default to cosine
        // VectorDistance([1,0,0], [1,0,0]) with cosine = 1.0
        var results = await RunQuery(container,
            "SELECT VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c WHERE c.id = 'x'");

        results.Should().ContainSingle();
        results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9,
            "default distance function should be cosine similarity");
    }
}
