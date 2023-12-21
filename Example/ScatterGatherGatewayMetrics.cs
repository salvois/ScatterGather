/*
ScatterGather - .NET library to implement the scatter-gather pattern
using a database to store distributed progress state

Copyright 2023 Salvatore ISAJA. All rights reserved.

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
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using ScatterGather;

namespace Example;

public class ScatterGatherGatewayMetrics : IScatterGatherGateway
{
    private int _beginScatterCount;
    private int _scatterCount;
    private int _endScatterCount;
    private int _gatherCount;
    private readonly IScatterGatherGateway _decoratee;

    public ScatterGatherGatewayMetrics(IScatterGatherGateway decoratee)
    {
        _decoratee = decoratee;
    }

    public async Task BeginScatter(ScatterRequestId requestId, string context)
    {
        var stopwatch = Stopwatch.StartNew();
        await _decoratee.BeginScatter(requestId, context);
        stopwatch.Stop();
        _beginScatterCount++;
        Console.WriteLine($"{nameof(BeginScatter)} #{_beginScatterCount} executed in {stopwatch.Elapsed.TotalMilliseconds} ms.");
    }

    public async Task Scatter(ScatterRequestId requestId, IEnumerable<ScatterPartId> partIds, Func<Task> callback)
    {
        var stopwatch = Stopwatch.StartNew();
        await _decoratee.Scatter(requestId, partIds, callback);
        stopwatch.Stop();
        _scatterCount++;
        Console.WriteLine($"{nameof(Scatter)} #{_scatterCount} executed in {stopwatch.Elapsed.TotalMilliseconds} ms.");
    }

    public async Task<T> Scatter<T>(ScatterRequestId requestId, IEnumerable<ScatterPartId> partIds, Func<Task<T>> callback)
    {
        var stopwatch = Stopwatch.StartNew();
        var result = await _decoratee.Scatter(requestId, partIds, callback);
        stopwatch.Stop();
        _scatterCount++;
        Console.WriteLine($"{nameof(Scatter)} #{_scatterCount} executed in {stopwatch.Elapsed.TotalMilliseconds} ms.");
        return result;
    }

    public async Task EndScatter(ScatterRequestId requestId, Func<string, Task> handleCompletion)
    {
        var stopwatch = Stopwatch.StartNew();
        await _decoratee.EndScatter(requestId, handleCompletion);
        stopwatch.Stop();
        _endScatterCount++;
        Console.WriteLine($"{nameof(EndScatter)} #{_endScatterCount} executed in {stopwatch.Elapsed.TotalMilliseconds} ms.");
    }

    public async Task Gather(ScatterRequestId requestId, IReadOnlyCollection<ScatterPartId> partIds, Func<string, Task> handleCompletion)
    {
        var stopwatch = Stopwatch.StartNew();
        await _decoratee.Gather(requestId, partIds, handleCompletion);
        stopwatch.Stop();
        _gatherCount++;
        Console.WriteLine($"{nameof(Gather)} #{_gatherCount} executed in {stopwatch.Elapsed.TotalMilliseconds} ms.");
    }
}