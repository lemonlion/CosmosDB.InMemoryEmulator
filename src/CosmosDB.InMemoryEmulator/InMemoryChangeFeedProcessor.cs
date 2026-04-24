#nullable disable
using System.Diagnostics;
using System.Linq.Expressions;
using System.Net;
using System.Reflection;
using System.Runtime.CompilerServices;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using NSubstitute;

namespace CosmosDB.InMemoryEmulator;

// Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/change-feed-processor
//   "The delegate receives batches of changes as they are generated in the change feed and can process them."
//   The ChangeFeedProcessorContext provides LeaseToken, Diagnostics, Headers, and FeedRange per batch.
internal sealed class InMemoryChangeFeedProcessorContext : ChangeFeedProcessorContext
{
    public override string LeaseToken { get; } = "0";
    public override CosmosDiagnostics Diagnostics => null;
    public override Headers Headers { get; } = new();
    public override FeedRange FeedRange => FeedRange.FromPartitionKey(PartitionKey.None);
}

internal sealed class NoOpChangeFeedProcessor : ChangeFeedProcessor
{
    public override Task StartAsync() => Task.CompletedTask;
    public override Task StopAsync() => Task.CompletedTask;
}

// Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/change-feed-processor
//   "The change feed processor has four main components: The monitored container [...],
//    The lease container [...], The compute instance [...], The delegate [...]"
//   This class implements the processor lifecycle: polling, delegate invocation, and checkpointing.
internal sealed class InMemoryChangeFeedProcessor<T> : ChangeFeedProcessor
{
    private readonly InMemoryContainer _container;
    private readonly Func<ChangeFeedProcessorContext, IReadOnlyCollection<T>, CancellationToken, Task> _handler;
    private CancellationTokenSource _cts;
    private Task _pollingTask;
    private long _checkpoint;

    internal InMemoryChangeFeedProcessor(
        InMemoryContainer container,
        Container.ChangeFeedHandler<T> handler)
    {
        _container = container;
        _handler = (ctx, changes, ct) => handler(ctx, changes, ct);
    }

    internal InMemoryChangeFeedProcessor(
        InMemoryContainer container,
        Container.ChangesHandler<T> legacyHandler)
    {
        _container = container;
        _handler = (_, changes, ct) => legacyHandler(changes, ct);
    }

    // Ref: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.changefeedprocessor.startasync
    //   "Start listening for changes."
    public override Task StartAsync()
    {
        _checkpoint = _container.GetChangeFeedCheckpoint();
        _cts = new CancellationTokenSource();
        _pollingTask = PollAsync(_cts.Token);
        return Task.CompletedTask;
    }

    // Ref: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.changefeedprocessor.stopasync
    //   "Stops listening for changes."
    public override async Task StopAsync()
    {
        if (_cts != null)
        {
            await _cts.CancelAsync();
            try { await _pollingTask; }
            catch (OperationCanceledException) { }
            _cts.Dispose();
            _cts = null;
        }
    }

    // Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/change-feed-processor
    //   "Processing life cycle: 1. Read the change feed. 2. If there are no changes, sleep for a
    //    predefined amount of time (customizable by using WithPollInterval in the Builder) and go to
    //    step 1. 3. If there are changes, send them to the delegate. 4. When the delegate finishes
    //    processing the changes successfully, update the lease store with the latest processed point
    //    in time and go to step 1."
    private async Task PollAsync(CancellationToken cancellationToken)
    {
        var context = new InMemoryChangeFeedProcessorContext();

        while (!cancellationToken.IsCancellationRequested)
        {
            // Ref: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.changefeedprocessorbuilder.withpollinterval
            //   "Gets or sets the delay in between polling the change feed for new changes,
            //    after all current changes are drained."
            await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);

            var iterator = _container.GetChangeFeedIterator<T>(_checkpoint);
            var changes = new List<T>();

            while (iterator.HasMoreResults)
            {
                var response = await iterator.ReadNextAsync(cancellationToken);
                // Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/change-feed-pull-model
                //   "When you try to read the change feed and there are no new changes available,
                //    you receive a response with NotModified status."
                if (response.StatusCode == HttpStatusCode.NotModified)
                {
                    break;
                }

                changes.AddRange(response);
            }

            if (changes.Count > 0)
            {
                // Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/change-feed-modes
                //   "Only the most recent change for a specific item is included in the change feed.
                //    Intermediate changes might not be available." (latest version mode)
                // Deduplicate: keep only the latest version per item (by id),
                // matching LatestVersion change feed processor behavior.
                var deduped = DeduplicateByItemId(changes);
                try
                {
                    await _handler(context, deduped, cancellationToken);
                    // Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/change-feed-processor
                    //   "When the delegate finishes processing the changes successfully, update the
                    //    lease store with the latest processed point in time and go to step 1."
                    _checkpoint += changes.Count; // advance past ALL entries, including intermediates
                }
                // Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/change-feed-processor
                //   "If your delegate implementation has an unhandled exception [...] The new thread
                //    restarts from there, effectively sending the same batch of changes to the delegate.
                //    This behavior continues until your delegate processes the changes correctly, and
                //    it's the reason the change feed processor has an 'at least once' guarantee."
                catch (Exception) when (!cancellationToken.IsCancellationRequested)
                {
                    // Handler threw — do not advance checkpoint so the same batch is redelivered
                }
            }
        }
    }

    // Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/change-feed-modes
    //   "Only the most recent change for a specific item is included in the change feed.
    //    Intermediate changes might not be available." (latest version mode)
    private static IReadOnlyCollection<T> DeduplicateByItemId(List<T> changes)
    {
        var seen = new Dictionary<string, T>();
        foreach (var item in changes)
        {
            var id = ExtractId(item);
            if (id != null)
                seen[id] = item; // last write wins — keeps latest version per item
        }
        return seen.Count == changes.Count ? changes : seen.Values.ToList();
    }

    private static string ExtractId(T item)
    {
        if (item is JObject jo) return jo["id"]?.ToString();
        var prop = typeof(T).GetProperty("Id") ?? typeof(T).GetProperty("id");
        return prop?.GetValue(item)?.ToString();
    }
}

// Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/change-feed-processor
//   Stream-based variant of the change feed processor. The delegate receives a Stream of changes
//   instead of typed objects, allowing raw JSON processing without deserialization.
internal sealed class InMemoryChangeFeedStreamProcessor : ChangeFeedProcessor
{
    private readonly InMemoryContainer _container;
    private readonly Container.ChangeFeedStreamHandler _handler;
    private CancellationTokenSource _cts;
    private Task _pollingTask;
    private long _checkpoint;

    internal InMemoryChangeFeedStreamProcessor(
        InMemoryContainer container,
        Container.ChangeFeedStreamHandler handler)
    {
        _container = container;
        _handler = handler;
    }

    public override Task StartAsync()
    {
        _checkpoint = _container.GetChangeFeedCheckpoint();
        _cts = new CancellationTokenSource();
        _pollingTask = PollAsync(_cts.Token);
        return Task.CompletedTask;
    }

    public override async Task StopAsync()
    {
        if (_cts != null)
        {
            await _cts.CancelAsync();
            try { await _pollingTask; }
            catch (OperationCanceledException) { }
            _cts.Dispose();
            _cts = null;
        }
    }

    private async Task PollAsync(CancellationToken cancellationToken)
    {
        var context = new InMemoryChangeFeedProcessorContext();

        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);

            var iterator = _container.GetChangeFeedIterator<JObject>(_checkpoint);
            var changes = new List<JObject>();

            while (iterator.HasMoreResults)
            {
                var response = await iterator.ReadNextAsync(cancellationToken);
                if (response.StatusCode == HttpStatusCode.NotModified)
                    break;
                changes.AddRange(response);
            }

            if (changes.Count > 0)
            {
                try
                {
                    var array = new JArray(changes);
                    var bytes = System.Text.Encoding.UTF8.GetBytes(array.ToString());
                    using var stream = new MemoryStream(bytes);
                    await _handler(context, stream, cancellationToken);
                    _checkpoint += changes.Count;
                }
                catch (Exception) when (!cancellationToken.IsCancellationRequested)
                {
                    // Handler threw — do not advance checkpoint so the same batch is redelivered
                }
            }
        }
    }
}

// Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/change-feed
//   "Each change appears exactly once in the change feed, and the clients must manage the
//    checkpointing logic."
//   This variant provides manual checkpoint control: the checkpoint only advances when the
//   delegate explicitly invokes the checkpoint callback.
internal sealed class InMemoryManualCheckpointChangeFeedProcessor<T> : ChangeFeedProcessor
{
    private readonly InMemoryContainer _container;
    private readonly Container.ChangeFeedHandlerWithManualCheckpoint<T> _handler;
    private CancellationTokenSource _cts;
    private Task _pollingTask;
    private long _checkpoint;

    internal InMemoryManualCheckpointChangeFeedProcessor(
        InMemoryContainer container,
        Container.ChangeFeedHandlerWithManualCheckpoint<T> handler)
    {
        _container = container;
        _handler = handler;
    }

    public override Task StartAsync()
    {
        _checkpoint = _container.GetChangeFeedCheckpoint();
        _cts = new CancellationTokenSource();
        _pollingTask = PollAsync(_cts.Token);
        return Task.CompletedTask;
    }

    public override async Task StopAsync()
    {
        if (_cts != null)
        {
            await _cts.CancelAsync();
            try { await _pollingTask; }
            catch (OperationCanceledException) { }
            _cts.Dispose();
            _cts = null;
        }
    }

    private async Task PollAsync(CancellationToken cancellationToken)
    {
        var context = new InMemoryChangeFeedProcessorContext();

        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);

            var iterator = _container.GetChangeFeedIterator<T>(_checkpoint);
            var changes = new List<T>();

            while (iterator.HasMoreResults)
            {
                var response = await iterator.ReadNextAsync(cancellationToken);
                if (response.StatusCode == HttpStatusCode.NotModified)
                    break;
                changes.AddRange(response);
            }

            if (changes.Count > 0)
            {
                var pendingCheckpoint = _checkpoint + changes.Count;
                var checkpointed = false;
                try
                {
                    // Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/change-feed
                    //   "Each change appears exactly once in the change feed, and the clients must
                    //    manage the checkpointing logic."
                    //   The checkpoint delegate allows the handler to control when progress is committed.
                    await _handler(context, changes, async () => { checkpointed = true; await Task.CompletedTask; }, cancellationToken);
                }
                catch (Exception) when (!cancellationToken.IsCancellationRequested)
                {
                    // Handler threw — do not advance checkpoint
                }
                if (checkpointed)
                    _checkpoint = pendingCheckpoint;
            }
        }
    }
}

// Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/change-feed
//   "Each change appears exactly once in the change feed, and the clients must manage the
//    checkpointing logic."
//   Stream-based variant with manual checkpoint control.
internal sealed class InMemoryManualCheckpointStreamChangeFeedProcessor : ChangeFeedProcessor
{
    private readonly InMemoryContainer _container;
    private readonly Container.ChangeFeedStreamHandlerWithManualCheckpoint _handler;
    private CancellationTokenSource _cts;
    private Task _pollingTask;
    private long _checkpoint;

    internal InMemoryManualCheckpointStreamChangeFeedProcessor(
        InMemoryContainer container,
        Container.ChangeFeedStreamHandlerWithManualCheckpoint handler)
    {
        _container = container;
        _handler = handler;
    }

    public override Task StartAsync()
    {
        _checkpoint = _container.GetChangeFeedCheckpoint();
        _cts = new CancellationTokenSource();
        _pollingTask = PollAsync(_cts.Token);
        return Task.CompletedTask;
    }

    public override async Task StopAsync()
    {
        if (_cts != null)
        {
            await _cts.CancelAsync();
            try { await _pollingTask; }
            catch (OperationCanceledException) { }
            _cts.Dispose();
            _cts = null;
        }
    }

    private async Task PollAsync(CancellationToken cancellationToken)
    {
        var context = new InMemoryChangeFeedProcessorContext();

        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);

            var iterator = _container.GetChangeFeedIterator<JObject>(_checkpoint);
            var changes = new List<JObject>();

            while (iterator.HasMoreResults)
            {
                var response = await iterator.ReadNextAsync(cancellationToken);
                if (response.StatusCode == HttpStatusCode.NotModified)
                    break;
                changes.AddRange(response);
            }

            if (changes.Count > 0)
            {
                var pendingCheckpoint = _checkpoint + changes.Count;
                var checkpointed = false;
                try
                {
                    var array = new JArray(changes);
                    var bytes = System.Text.Encoding.UTF8.GetBytes(array.ToString());
                    using var stream = new MemoryStream(bytes);
                    await _handler(context, stream, async () => { checkpointed = true; await Task.CompletedTask; }, cancellationToken);
                }
                catch (Exception) when (!cancellationToken.IsCancellationRequested)
                {
                    // Handler threw — do not advance checkpoint
                }
                if (checkpointed)
                    _checkpoint = pendingCheckpoint;
            }
        }
    }
}

// Ref: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.changefeedprocessorbuilder
//   "Builds a new instance of the ChangeFeedProcessor with the specified configuration."
//   This factory creates a ChangeFeedProcessorBuilder pre-configured with our in-memory processor
//   via reflection into SDK internals, with an NSubstitute fallback if reflection fails.
//   Reflection targets private fields of ChangeFeedProcessorBuilder in the Cosmos SDK — these are
//   internal implementation details that may change across SDK versions. See IsReflectionCompatible().
internal static class ChangeFeedProcessorBuilderFactory
{
    private static readonly Assembly CosmosAssembly = typeof(Container).Assembly;
    private static readonly BindingFlags PrivateInstance = BindingFlags.NonPublic | BindingFlags.Instance;

    internal static readonly string[] RequiredFieldNames =
    {
        "changeFeedProcessor", "isBuilt", "changeFeedLeaseOptions",
        "changeFeedProcessorOptions", "monitoredContainer", "applyBuilderConfiguration"
    };

    internal static readonly string[] RequiredInternalTypes =
    {
        "Microsoft.Azure.Cosmos.ChangeFeed.Configuration.ChangeFeedLeaseOptions",
        "Microsoft.Azure.Cosmos.ChangeFeed.Configuration.ChangeFeedProcessorOptions",
        "Microsoft.Azure.Cosmos.ContainerInlineCore",
        "Microsoft.Azure.Cosmos.ContainerInternal",
        "Microsoft.Azure.Cosmos.ChangeFeed.LeaseManagement.DocumentServiceLeaseStoreManager"
    };

    internal static bool IsReflectionCompatible()
    {
        var builderType = typeof(ChangeFeedProcessorBuilder);

        foreach (var fieldName in RequiredFieldNames)
        {
            if (builderType.GetField(fieldName, PrivateInstance) == null)
            {
                return false;
            }
        }

        foreach (var typeName in RequiredInternalTypes)
        {
            if (CosmosAssembly.GetType(typeName) == null)
            {
                return false;
            }
        }

        return true;
    }

    internal static ChangeFeedProcessorBuilder Create(string processorName, ChangeFeedProcessor processor)
    {
        try
        {
            return CreateViaReflection(processorName, processor);
        }
        catch (Exception exception)
        {
            Trace.TraceWarning(
                "InMemoryContainer: ChangeFeedProcessorBuilder reflection failed " +
                $"({exception.GetType().Name}: {exception.Message}). " +
                "Falling back to NSubstitute stub — processor.Build() will return a " +
                "no-op processor that does not poll the change feed.");
            return CreateFallbackBuilder(processor);
        }
    }

    private static ChangeFeedProcessorBuilder CreateViaReflection(
        string processorName, ChangeFeedProcessor processor)
    {
        var builder = (ChangeFeedProcessorBuilder)RuntimeHelpers.GetUninitializedObject(
            typeof(ChangeFeedProcessorBuilder));

        var builderType = typeof(ChangeFeedProcessorBuilder);

        SetField(builderType, builder, "changeFeedProcessor", processor);
        SetField(builderType, builder, "isBuilt", false);

        var leaseOptionsType = GetInternalType(
            "Microsoft.Azure.Cosmos.ChangeFeed.Configuration.ChangeFeedLeaseOptions");
        var leaseOptions = Activator.CreateInstance(leaseOptionsType);
        leaseOptionsType.GetProperty("LeasePrefix")!.SetValue(leaseOptions, processorName);
        SetField(builderType, builder, "changeFeedLeaseOptions", leaseOptions);

        var processorOptionsType = GetInternalType(
            "Microsoft.Azure.Cosmos.ChangeFeed.Configuration.ChangeFeedProcessorOptions");
        var processorOptions = Activator.CreateInstance(processorOptionsType, nonPublic: true);
        SetField(builderType, builder, "changeFeedProcessorOptions", processorOptions);

        var containerInlineCore = GetInternalType("Microsoft.Azure.Cosmos.ContainerInlineCore");
        var fakeContainer = RuntimeHelpers.GetUninitializedObject(containerInlineCore);
        SetField(builderType, builder, "monitoredContainer", fakeContainer);

        var leaseStoreManagerType = GetInternalType(
            "Microsoft.Azure.Cosmos.ChangeFeed.LeaseManagement.DocumentServiceLeaseStoreManager");
        var containerInternalType = GetInternalType("Microsoft.Azure.Cosmos.ContainerInternal");

        var actionType = typeof(Action<,,,,,>).MakeGenericType(
            leaseStoreManagerType,
            containerInternalType,
            typeof(string),
            leaseOptionsType,
            processorOptionsType,
            containerInternalType);

        var parameters = actionType.GetMethod("Invoke")!.GetParameters()
            .Select(p => Expression.Parameter(p.ParameterType))
            .ToArray();
        var noopDelegate = Expression.Lambda(actionType, Expression.Empty(), parameters).Compile();
        SetField(builderType, builder, "applyBuilderConfiguration", noopDelegate);

        return builder;
    }

    // Ref: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.changefeedprocessorbuilder
    //   Fallback builder using NSubstitute when reflection into SDK internals is not possible.
    //   Stubs the fluent builder API methods: WithInstanceName, WithLeaseContainer, WithStartTime,
    //   WithPollInterval, WithMaxItems, and Build.
    private static ChangeFeedProcessorBuilder CreateFallbackBuilder(ChangeFeedProcessor processor)
    {
        var builder = Substitute.For<ChangeFeedProcessorBuilder>();
        // Ref: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.changefeedprocessorbuilder.withinstancename
        //   "Sets the compute instance name that will host the processor."
        builder.WithInstanceName(Arg.Any<string>()).Returns(builder);
        // Ref: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.changefeedprocessorbuilder.withleasecontainer
        //   "Sets the Cosmos Container to hold the leases state"
        builder.WithLeaseContainer(Arg.Any<Container>()).Returns(builder);
        // Ref: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.changefeedprocessorbuilder.withstarttime
        //   "Sets the time (exclusive) to start looking for changes after."
        builder.WithStartTime(Arg.Any<DateTime>()).Returns(builder);
        // Ref: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.changefeedprocessorbuilder.withpollinterval
        //   "Gets or sets the delay in between polling the change feed for new changes,
        //    after all current changes are drained."
        builder.WithPollInterval(Arg.Any<TimeSpan>()).Returns(builder);
        // Ref: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.changefeedprocessorbuilder.withmaxitems
        //   "Sets the maximum number of items to be returned in the enumeration operation."
        builder.WithMaxItems(Arg.Any<int>()).Returns(builder);
        // Ref: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.changefeedprocessorbuilder.build
        //   "Builds a new instance of the ChangeFeedProcessor with the specified configuration."
        builder.Build().Returns(processor);
        return builder;
    }

    private static void SetField(Type type, object target, string fieldName, object value)
    {
        var field = type.GetField(fieldName, PrivateInstance)
            ?? throw new MissingFieldException(type.Name, fieldName);
        field.SetValue(target, value);
    }

    private static Type GetInternalType(string typeName)
    {
        return CosmosAssembly.GetType(typeName)
            ?? throw new TypeLoadException(
                $"Internal type '{typeName}' not found in {CosmosAssembly.GetName().Name} " +
                $"v{CosmosAssembly.GetName().Version}.");
    }
}
