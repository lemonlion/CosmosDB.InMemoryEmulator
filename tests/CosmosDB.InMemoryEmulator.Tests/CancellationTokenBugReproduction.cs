using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Bug: CancellationToken is ignored on query iterator ReadNextAsync.
///
/// Found while migrating ASOS/SimpleEventStore to use InMemoryEmulator.
/// See: https://github.com/McNultyyy/SimpleEventStore/tree/use-inmemory-emulator
///
/// Real Cosmos DB throws OperationCanceledException when ReadNextAsync is called
/// with a pre-cancelled CancellationToken. The InMemoryEmulator completes
/// synchronously and ignores the token entirely.
/// </summary>
public class CancellationTokenBugReproduction
{
    [Fact]
    public async Task QueryIterator_ShouldThrowWhenCancellationTokenIsCancelled()
    {
        var container = new InMemoryContainer("cancel-test", "/partitionKey");

        for (var i = 0; i < 10; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"item-{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));

        using var cts = new CancellationTokenSource();
        cts.Cancel(); // Pre-cancel the token

        var query = new QueryDefinition("SELECT * FROM c");
        var iterator = container.GetItemQueryIterator<TestDocument>(query);

        // Real Cosmos throws OperationCanceledException when given a cancelled token
        var act = async () =>
        {
            while (iterator.HasMoreResults)
                await iterator.ReadNextAsync(cts.Token);
        };

        await act.Should().ThrowAsync<OperationCanceledException>(
            "A pre-cancelled CancellationToken should cause OperationCanceledException");
    }
}
