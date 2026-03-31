using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ════════════════════════════════════════════════════════════════════════════════
// Full-Text Search Functions — Approximate implementation using case-insensitive
// word matching instead of the real Cosmos DB NLP tokenizer + BM25 engine.
//
// Real Cosmos DB requires a full-text indexing policy on the container and performs
// stemming, stop-word removal, and BM25 relevance scoring.  The emulator uses
// simple case-insensitive Contains/word matching as an approximation so that
// queries exercising these functions compile and produce reasonable results.
//
// Reference: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/full-text-search
// ════════════════════════════════════════════════════════════════════════════════

public class FullTextContainsTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task FullTextContains_MatchingTerm_ReturnsDocument()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", description = "The quick brown fox jumps over the lazy dog" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE FullTextContains(c.description, 'fox')");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle();
        results[0]["id"]!.Value<string>().Should().Be("1");
    }

    [Fact]
    public async Task FullTextContains_NonMatchingTerm_ReturnsEmpty()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", description = "The quick brown fox" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE FullTextContains(c.description, 'elephant')");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task FullTextContains_IsCaseInsensitive()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", description = "Cosmos Database Service" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE FullTextContains(c.description, 'cosmos')");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle();
    }

    [Fact]
    public async Task FullTextContains_NullField_ReturnsEmpty()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE FullTextContains(c.description, 'test')");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task FullTextContains_MultipleDocuments_FiltersCorrectly()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", text = "Azure Cosmos DB is a NoSQL database" }),
            new PartitionKey("a"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", text = "SQL Server is a relational database" }),
            new PartitionKey("a"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "3", pk = "a", text = "Azure Functions is serverless" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE FullTextContains(c.text, 'database')");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().HaveCount(2);
        results.Select(r => r["id"]!.Value<string>()).Should().BeEquivalentTo(["1", "2"]);
    }

    [Fact]
    public async Task FullTextContains_WithPartitionKey_RespectsFilter()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", text = "Hello world" }),
            new PartitionKey("a"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "b", text = "Hello cosmos" }),
            new PartitionKey("b"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE FullTextContains(c.text, 'hello')",
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle();
        results[0]["id"]!.Value<string>().Should().Be("1");
    }
}

public class FullTextContainsAllTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task FullTextContainsAll_AllTermsPresent_ReturnsDocument()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", text = "Azure Cosmos DB is a fast NoSQL database" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE FullTextContainsAll(c.text, 'cosmos', 'database')");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle();
    }

    [Fact]
    public async Task FullTextContainsAll_SomeTermsMissing_ReturnsEmpty()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", text = "Azure Cosmos DB is fast" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE FullTextContainsAll(c.text, 'cosmos', 'elephant')");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task FullTextContainsAll_SingleTerm_WorksLikeContains()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", text = "Hello world" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE FullTextContainsAll(c.text, 'world')");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle();
    }

    [Fact]
    public async Task FullTextContainsAll_IsCaseInsensitive()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", text = "AZURE COSMOS DATABASE" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE FullTextContainsAll(c.text, 'azure', 'cosmos', 'database')");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle();
    }
}

public class FullTextContainsAnyTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task FullTextContainsAny_OneTermMatches_ReturnsDocument()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", text = "Azure Cosmos DB" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE FullTextContainsAny(c.text, 'elephant', 'cosmos')");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle();
    }

    [Fact]
    public async Task FullTextContainsAny_NoTermsMatch_ReturnsEmpty()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", text = "Azure Cosmos DB" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE FullTextContainsAny(c.text, 'elephant', 'giraffe')");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task FullTextContainsAny_AllTermsMatch_ReturnsDocument()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", text = "Azure Cosmos DB database" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE FullTextContainsAny(c.text, 'cosmos', 'database')");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle();
    }

    [Fact]
    public async Task FullTextContainsAny_FiltersMultipleDocumentsCorrectly()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", text = "NoSQL database on Azure" }),
            new PartitionKey("a"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", text = "SQL relational engine" }),
            new PartitionKey("a"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "3", pk = "a", text = "Graph processing system" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE FullTextContainsAny(c.text, 'database', 'engine')");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().HaveCount(2);
        results.Select(r => r["id"]!.Value<string>()).Should().BeEquivalentTo(["1", "2"]);
    }
}

public class FullTextScoreTests2
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task FullTextScore_ReturnsNumericValue()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", text = "Cosmos database is a great database" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT FullTextScore(c.text, ['database', 'cosmos']) AS score FROM c");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle();
        var score = results[0]["score"]!.Value<double>();
        score.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task FullTextScore_MoreMatchingTerms_HigherScore()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", text = "Cosmos database is fast" }),
            new PartitionKey("a"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", text = "Cosmos database is a great database service for cosmos" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT c.id, FullTextScore(c.text, ['database', 'cosmos']) AS score FROM c");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().HaveCount(2);
        var scores = results.ToDictionary(r => r["id"]!.Value<string>()!, r => r["score"]!.Value<double>());
        scores["2"].Should().BeGreaterThan(scores["1"],
            "document 2 has more occurrences of the search terms");
    }

    [Fact]
    public async Task FullTextScore_NoMatchingTerms_ReturnsZero()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", text = "Hello world" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT FullTextScore(c.text, ['elephant', 'giraffe']) AS score FROM c");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle();
        results[0]["score"]!.Value<double>().Should().Be(0);
    }
}

public class OrderByRankTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task OrderByRank_SortsByFullTextScoreDescending()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", text = "Azure cloud platform" }),
            new PartitionKey("a"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", text = "Cosmos database is a database" }),
            new PartitionKey("a"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "3", pk = "a", text = "Cosmos database service for database storage of database records" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c ORDER BY RANK FullTextScore(c.text, ['database', 'cosmos'])");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().HaveCount(3);
        // Document 3 has most occurrences (3x database + 1x cosmos = 4), should be first
        // Document 2 has (2x database + 1x cosmos = 3), should be second
        // Document 1 has 0 matches, should be last
        results[0]["id"]!.Value<string>().Should().Be("3");
        results[1]["id"]!.Value<string>().Should().Be("2");
        results[2]["id"]!.Value<string>().Should().Be("1");
    }

    [Fact]
    public async Task OrderByRank_WithWhereClause_FiltersAndSorts()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", text = "Cosmos DB overview", category = "docs" }),
            new PartitionKey("a"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", text = "Cosmos DB database guide for database", category = "docs" }),
            new PartitionKey("a"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "3", pk = "a", text = "Unrelated topic", category = "other" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE c.category = 'docs' ORDER BY RANK FullTextScore(c.text, ['database'])");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().HaveCount(2);
        results[0]["id"]!.Value<string>().Should().Be("2");
        results[1]["id"]!.Value<string>().Should().Be("1");
    }

    [Fact]
    public async Task OrderByRank_WithTopClause_LimitsResults()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", text = "Low relevance" }),
            new PartitionKey("a"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", text = "database database database" }),
            new PartitionKey("a"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "3", pk = "a", text = "database service" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT TOP 2 * FROM c ORDER BY RANK FullTextScore(c.text, ['database'])");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().HaveCount(2);
        results[0]["id"]!.Value<string>().Should().Be("2");
        results[1]["id"]!.Value<string>().Should().Be("3");
    }

    [Fact]
    public async Task OrderByRank_WithSelectScore_ReturnsSortedWithScores()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", text = "Cosmos database" }),
            new PartitionKey("a"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", text = "Cosmos database cosmos database cosmos" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT c.id, FullTextScore(c.text, ['cosmos', 'database']) AS score FROM c ORDER BY RANK FullTextScore(c.text, ['cosmos', 'database'])");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().HaveCount(2);
        results[0]["id"]!.Value<string>().Should().Be("2");
        var score1 = results[0]["score"]!.Value<double>();
        var score2 = results[1]["score"]!.Value<double>();
        score1.Should().BeGreaterThan(score2);
    }
}

public class FullTextCombinedTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    [Fact]
    public async Task FullTextContains_CombinedWithOtherWhereConditions()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", text = "Cosmos database", active = true }),
            new PartitionKey("a"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", text = "Cosmos database", active = false }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE FullTextContains(c.text, 'cosmos') AND c.active = true");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle();
        results[0]["id"]!.Value<string>().Should().Be("1");
    }

    [Fact]
    public async Task FullTextContainsAny_CombinedWithContainsAll()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", title = "Azure Cosmos", body = "A fast database" }),
            new PartitionKey("a"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "2", pk = "a", title = "Azure Functions", body = "Serverless compute" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE FullTextContainsAny(c.title, 'cosmos', 'functions') AND FullTextContainsAll(c.body, 'fast', 'database')");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        results.Should().ContainSingle();
        results[0]["id"]!.Value<string>().Should().Be("1");
    }
}

// ════════════════════════════════════════════════════════════════════════════════
// Divergent Behaviour Tests
//
// These tests document where the emulator's approximate full-text search
// intentionally differs from real Cosmos DB.
// ════════════════════════════════════════════════════════════════════════════════

public class FullTextSearchDivergentBehaviorTests
{
    private readonly InMemoryContainer _container = new("test-container", "/pk");

    /// <summary>
    /// DIVERGENT BEHAVIOUR #1 — No full-text indexing policy required.
    ///
    /// Real Cosmos DB: Calling FULLTEXTCONTAINS on a container without a full-text
    /// indexing policy throws a BadRequest error:
    ///   "Full-text search queries are not supported for accounts or containers
    ///    without a full-text index policy."
    ///
    /// InMemoryEmulator: Full-text functions work on any container without any
    /// indexing configuration. This is intentional — the emulator approximates
    /// the function behaviour using simple string matching so tests can exercise
    /// their query logic without needing to configure indexing policies.
    /// </summary>
    [Fact]
    public async Task FullTextContains_WorksWithoutFullTextIndexPolicy()
    {
        // No full-text index policy configured on this container — real Cosmos would reject this.
        var container = new InMemoryContainer("test", "/pk");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", text = "Hello world" }),
            new PartitionKey("a"));

        var iterator = container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE FullTextContains(c.text, 'hello')");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        // Emulator returns results; real Cosmos DB would throw BadRequest
        results.Should().ContainSingle();
    }

    /// <summary>
    /// DIVERGENT BEHAVIOUR #2 — Naive term-frequency scoring vs BM25.
    ///
    /// Real Cosmos DB: FULLTEXTSCORE uses the BM25 (Best Matching 25) algorithm
    /// which considers term frequency, inverse document frequency, and document
    /// length normalization. A term appearing 3x in a short document scores
    /// differently than 3x in a long document.
    ///
    /// InMemoryEmulator: Uses a simple case-insensitive count of how many times
    /// each search term appears in the field text. The score is the sum of all
    /// term occurrence counts. No IDF or length normalization is applied.
    ///
    /// This means:
    /// - Relative ordering is usually correct (more occurrences = higher score)
    /// - Absolute score values will differ significantly
    /// - Edge cases with document length variation may produce different orderings
    ///   since BM25 penalises very long documents
    /// </summary>
    [Fact]
    public async Task FullTextScore_UsesNaiveTermFrequency_NotBM25()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "short", pk = "a", text = "database database database" }),
            new PartitionKey("a"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "long", pk = "a",
                text = "database database database plus many other words to make this a much longer document that would score differently under BM25 length normalization" }),
            new PartitionKey("a"));

        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT c.id, FullTextScore(c.text, ['database']) AS score FROM c");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        // Both have 3 occurrences of "database" — emulator scores them equally.
        // Real BM25 would score the shorter document higher due to length normalization.
        var scores = results.ToDictionary(r => r["id"]!.Value<string>()!, r => r["score"]!.Value<double>());
        scores["short"].Should().Be(scores["long"],
            "the emulator uses naive term-frequency counting without BM25 length normalization");
    }

    /// <summary>
    /// DIVERGENT BEHAVIOUR #3 — No tokenization/stemming.
    ///
    /// Real Cosmos DB: The full-text engine tokenizes text and applies stemming,
    /// so searching for "running" would match "runs", "ran", "runner" etc.
    ///
    /// InMemoryEmulator: Uses literal case-insensitive substring matching, so
    /// "running" only matches text containing the exact substring "running".
    /// </summary>
    [Fact]
    public async Task FullTextContains_NoStemming_LiteralMatchOnly()
    {
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", text = "The runner runs quickly" }),
            new PartitionKey("a"));

        // Search for "running" — real Cosmos with stemming might match "runner"/"runs"
        var iterator = _container.GetItemQueryIterator<JObject>(
            "SELECT * FROM c WHERE FullTextContains(c.text, 'running')");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
            results.AddRange(await iterator.ReadNextAsync());

        // Emulator does literal matching — "running" is not a substring of "The runner runs quickly"
        results.Should().BeEmpty(
            "the emulator uses literal substring matching without stemming");
    }
}

public class FullTextSearchSkippedTests
{
    /// <summary>
    /// Real Cosmos DB requires a full-text indexing policy on the container definition.
    /// The emulator does not require or validate this configuration.
    ///
    /// In real Cosmos DB, you must configure the container with a full-text policy:
    /// <code>
    /// new ContainerProperties("container", "/pk")
    /// {
    ///     FullTextPolicy = new FullTextPolicy
    ///     {
    ///         DefaultLanguage = "en-US",
    ///         FullTextPaths = { new FullTextPath { Path = "/text", Language = "en-US" } }
    ///     },
    ///     IndexingPolicy = new IndexingPolicy
    ///     {
    ///         FullTextIndexes = { new FullTextIndexPath { Path = "/text" } }
    ///     }
    /// }
    /// </code>
    ///
    /// Without this configuration, all FULLTEXT* queries return HTTP 400.
    /// The emulator skips this validation entirely to keep test setup simple.
    /// </summary>
    [Fact(Skip = "Full-text indexing policy validation is not implemented. " +
        "Real Cosmos DB requires a FullTextPolicy and FullTextIndexes on the container's " +
        "IndexingPolicy. Without it, all FULLTEXT* queries return HTTP 400 BadRequest. " +
        "The emulator intentionally skips this validation so queries work without " +
        "indexing policy configuration. See FullTextSearchDivergentBehaviorTests for details.")]
    public async Task FullTextContains_WithoutFullTextIndex_ShouldThrow400()
    {
        var container = new InMemoryContainer("test", "/pk");
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", pk = "a", text = "Hello" }),
            new PartitionKey("a"));

        // In real Cosmos DB, this would throw CosmosException with StatusCode 400
        var act = async () =>
        {
            var iterator = container.GetItemQueryIterator<JObject>(
                "SELECT * FROM c WHERE FullTextContains(c.text, 'hello')");
            while (iterator.HasMoreResults)
                await iterator.ReadNextAsync();
        };

        await act.Should().ThrowAsync<CosmosException>();
    }
}
