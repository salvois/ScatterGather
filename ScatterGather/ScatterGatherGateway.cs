/*
ScatterGather - .NET library to implement the scatter-gather pattern
using a database to store distributed progress state

Copyright 2023-2026 Salvatore ISAJA. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED THE COPYRIGHT HOLDER ``AS IS'' AND ANY EXPRESS
OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN
NO EVENT SHALL THE COPYRIGHT HOLDER BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
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