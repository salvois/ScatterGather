using System.Text.Json;

namespace ScatterGather;

public readonly record struct ScatterRequestId(string Value);

public readonly record struct ScatterPartId(string Value);

public interface IScatterGatherStore
{
    Task BeginScatter(ScatterRequestId requestId, string context);
    Task MarkScattered(ScatterRequestId requestId, IEnumerable<ScatterPartId> partIds);
    Task EndScatter(ScatterRequestId requestId, Func<string, Task> handleCompletion);
    Task MarkGathered(ScatterRequestId requestId, IReadOnlyCollection<ScatterPartId> partIds, Func<string, Task> handleCompletion);
}

public class ScatterGatherGateway(IScatterGatherStore store)
{
    public async Task<ScatterScope<TContext>> CreateScatterScopeAsync<TContext>(
        ScatterRequestId requestId,
        TContext context,
        Func<TContext, Task> handleCompletion)
    {
        await store.BeginScatter(requestId, JsonSerializer.Serialize(context));
        return new ScatterScope<TContext>(store, requestId, handleCompletion);
    }

    public async Task GatherAsync<TContext>(
        ScatterRequestId requestId,
        IReadOnlyCollection<ScatterPartId> partIds,
        Func<Task> process,
        Func<TContext, Task> handleCompletion)
    {
        await process();
        await store.MarkGathered(requestId, partIds, c => handleCompletion(JsonSerializer.Deserialize<TContext>(c)!));
    }
}

public sealed class ScatterScope<TContext> : IAsyncDisposable
{
    private readonly IScatterGatherStore _scatterGatherStore;
    private readonly ScatterRequestId _requestId;
    private readonly Func<TContext, Task> _handleCompletion;
    private bool _disposed;

    internal ScatterScope(
        IScatterGatherStore scatterGatherStore,
        ScatterRequestId requestId,
        Func<TContext, Task> handleCompletion)
    {
        _scatterGatherStore = scatterGatherStore;
        _requestId = requestId;
        _handleCompletion = handleCompletion;
    }

    public async Task AddAsync(IEnumerable<ScatterPartId> partIds, Func<Task> process)
    {
        await _scatterGatherStore.MarkScattered(_requestId, partIds);
        await process();
    }

    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            await _scatterGatherStore.EndScatter(_requestId, c => _handleCompletion(JsonSerializer.Deserialize<TContext>(c)!));
            _disposed = true;
        }
    }
}