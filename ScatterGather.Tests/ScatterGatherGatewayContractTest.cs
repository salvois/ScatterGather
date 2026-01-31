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
OF MERCHANTABILITYMongoScatterGather AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN
NO EVENT SHALL THE COPYRIGHT HOLDER BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
using System;
using System.Threading.Tasks;
using NUnit.Framework;
using Shouldly;

namespace ScatterGather.Tests;

public abstract class ScatterGatherGatewayContractTest
{
    private static Task DoNothingCallback() =>
        Task.CompletedTask;

    protected abstract ScatterGatherGateway CreateScatterGatherGateway();

    [Test]
    public async Task NothingToScatter()
    {
        var gateway = CreateScatterGatherGateway();
        var completionHandler = new CompletionHandler();
        var scatterRequestId = new ScatterRequestId("requestId");

        await using (var scatterScope = await gateway.CreateScatterScopeAsync(scatterRequestId, "context", completionHandler.HandleCompletion))
        {
        }
        completionHandler.Completed.ShouldBeTrue();
        completionHandler.Context.ShouldBe("context");
    }

    [Test]
    public async Task SimpleScatterGather()
    {
        var gateway = CreateScatterGatherGateway();
        var completionHandler = new CompletionHandler();
        var scatterRequestId = new ScatterRequestId("requestId");

        await using (var scatterScope = await gateway.CreateScatterScopeAsync(scatterRequestId, "context", completionHandler.HandleCompletion))
        {
            await scatterScope.AddAsync([new ScatterPartId("lorem"), new ScatterPartId("ipsum")], DoNothingCallback);
        }
        completionHandler.Completed.ShouldBeFalse();

        await gateway.GatherAsync<string>(scatterRequestId, [new ScatterPartId("ipsum")], DoNothingCallback, completionHandler.HandleCompletion);
        completionHandler.Completed.ShouldBeFalse();
        await gateway.GatherAsync<string>(scatterRequestId, [new ScatterPartId("lorem")], DoNothingCallback, completionHandler.HandleCompletion);
        completionHandler.Completed.ShouldBeTrue();
        completionHandler.Context.ShouldBe("context");
    }

    [Test]
    public async Task GatherFasterThanScatter()
    {
        var gateway = CreateScatterGatherGateway();
        var completionHandler = new CompletionHandler();
        var scatterRequestId = new ScatterRequestId("requestId");
        await using (var scatterScope = await gateway.CreateScatterScopeAsync(scatterRequestId, "context", completionHandler.HandleCompletion))
        {
            await scatterScope.AddAsync([new ScatterPartId("lorem")], DoNothingCallback);
            await gateway.GatherAsync<string>(scatterRequestId, [new ScatterPartId("lorem")], DoNothingCallback, completionHandler.HandleCompletion);
            completionHandler.Completed.ShouldBeFalse();
        }
        completionHandler.Completed.ShouldBeTrue();
    }

    [Test]
    public async Task DuplicateBeforeCompletion()
    {
        var gateway = CreateScatterGatherGateway();
        var completionHandler = new CompletionHandler();
        var scatterRequestId = new ScatterRequestId("requestId");

        await using (var scatterScope = await gateway.CreateScatterScopeAsync(scatterRequestId, "context", completionHandler.HandleCompletion))
        {
            await scatterScope.AddAsync([new ScatterPartId("lorem"), new ScatterPartId("ipsum")], DoNothingCallback);
        }
        completionHandler.Completed.ShouldBeFalse();

        await gateway.GatherAsync<string>(scatterRequestId, [new ScatterPartId("ipsum")], DoNothingCallback, completionHandler.HandleCompletion);
        completionHandler.Completed.ShouldBeFalse();
        await gateway.GatherAsync<string>(scatterRequestId, [new ScatterPartId("ipsum")], DoNothingCallback, completionHandler.HandleCompletion);
        completionHandler.Completed.ShouldBeFalse();
        await gateway.GatherAsync<string>(scatterRequestId, [new ScatterPartId("lorem")], DoNothingCallback, completionHandler.HandleCompletion);
        completionHandler.Completed.ShouldBeTrue();
    }

    [Test]
    public async Task DuplicateAfterCompletion()
    {
        var gateway = CreateScatterGatherGateway();
        var completionHandler = new CompletionHandler();
        var scatterRequestId = new ScatterRequestId("requestId");

        await using (var scatterScope = await gateway.CreateScatterScopeAsync(scatterRequestId, "context", completionHandler.HandleCompletion))
        {
            await scatterScope.AddAsync([new ScatterPartId("lorem"), new ScatterPartId("ipsum")], DoNothingCallback);
        }
        completionHandler.Completed.ShouldBeFalse();

        await gateway.GatherAsync<string>(scatterRequestId, [new ScatterPartId("ipsum")], DoNothingCallback, completionHandler.HandleCompletion);
        completionHandler.Completed.ShouldBeFalse();
        await gateway.GatherAsync<string>(scatterRequestId, [new ScatterPartId("lorem")], DoNothingCallback, completionHandler.HandleCompletion);
        completionHandler.Completed.ShouldBeTrue();

        completionHandler = new CompletionHandler();
        await gateway.GatherAsync<string>(scatterRequestId, [new ScatterPartId("lorem")], DoNothingCallback, completionHandler.HandleCompletion);
        completionHandler.Completed.ShouldBeFalse();
    }

    [Test]
    public async Task ErrorOnCompletionHandlerAndRetry()
    {
        var gateway = CreateScatterGatherGateway();
        var completionHandler = new CompletionHandler();
        static Task HandleCompletionThrowing(string context) => throw new DivideByZeroException();
        var scatterRequestId = new ScatterRequestId("requestId");

        await using (var scatterScope = await gateway.CreateScatterScopeAsync(scatterRequestId, "context", completionHandler.HandleCompletion))
        {
            await scatterScope.AddAsync([new ScatterPartId("lorem"), new ScatterPartId("ipsum")], DoNothingCallback);
        }
        completionHandler.Completed.ShouldBeFalse();

        await gateway.GatherAsync<string>(scatterRequestId, [new ScatterPartId("ipsum")], DoNothingCallback, completionHandler.HandleCompletion);
        completionHandler.Completed.ShouldBeFalse();
        var act = () => gateway.GatherAsync<string>(scatterRequestId, [new ScatterPartId("lorem")], DoNothingCallback, HandleCompletionThrowing);
        await act.ShouldThrowAsync<DivideByZeroException>();
        await gateway.GatherAsync<string>(scatterRequestId, [new ScatterPartId("lorem")], DoNothingCallback, completionHandler.HandleCompletion);
        completionHandler.Completed.ShouldBeTrue();
    }

    [Test]
    public async Task RetryScatterWithNewIds()
    {
        var gateway = CreateScatterGatherGateway();
        var completionHandler = new CompletionHandler();
        var scatterRequestId = new ScatterRequestId("requestId");

        await using (var scatterScope = await gateway.CreateScatterScopeAsync(scatterRequestId, "context", completionHandler.HandleCompletion))
        {
            await scatterScope.AddAsync([new ScatterPartId("lorem"), new ScatterPartId("ipsum"), new ScatterPartId("dolor"), new ScatterPartId("consectetur")], DoNothingCallback);
        }
        completionHandler.Completed.ShouldBeFalse();

        await gateway.GatherAsync<string>(scatterRequestId, [new ScatterPartId("ipsum")], DoNothingCallback, completionHandler.HandleCompletion);
        completionHandler.Completed.ShouldBeFalse();
        await gateway.GatherAsync<string>(scatterRequestId, [new ScatterPartId("dolor")], DoNothingCallback, completionHandler.HandleCompletion);
        completionHandler.Completed.ShouldBeFalse();

        await using (var scatterScope = await gateway.CreateScatterScopeAsync(scatterRequestId, "context", completionHandler.HandleCompletion))
        {
            await scatterScope.AddAsync([new ScatterPartId("dolor"), new ScatterPartId("sit"), new ScatterPartId("amet")], DoNothingCallback);
        }
        completionHandler.Completed.ShouldBeFalse();

        await gateway.GatherAsync<string>(scatterRequestId, [new ScatterPartId("sit")], DoNothingCallback, completionHandler.HandleCompletion);
        completionHandler.Completed.ShouldBeFalse();
        await gateway.GatherAsync<string>(scatterRequestId, [new ScatterPartId("lorem")], DoNothingCallback, completionHandler.HandleCompletion);
        completionHandler.Completed.ShouldBeFalse();
        await gateway.GatherAsync<string>(scatterRequestId, [new ScatterPartId("dolor")], DoNothingCallback, completionHandler.HandleCompletion);
        completionHandler.Completed.ShouldBeFalse();
        await gateway.GatherAsync<string>(scatterRequestId, [new ScatterPartId("amet")], DoNothingCallback, completionHandler.HandleCompletion);
        completionHandler.Completed.ShouldBeTrue();
    }

    private class CompletionHandler
    {
        public bool Completed { get; private set; }
        public string Context { get; private set; } = "";

        public Task HandleCompletion(string context)
        {
            Completed = true;
            Context = context;
            return Task.CompletedTask;
        }
    }
}