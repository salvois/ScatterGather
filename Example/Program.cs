/*
DynamoScatterGather - .NET library to implement the scatter-gather pattern
using Amazon DynamoDB to store progress state

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
using System.Linq;
using System.Threading.Tasks;
using DynamoScatterGather;
using Example;

// This example uses two DynamoDB tables that are assumed to be already existing.
// They are used to store scatter requests and scattered parts respectively.
// The key of the request table must be a single string field named RequestId.
// The key of the part table must be a pair of string fields named RequestId and PartId.
// Here we decorate our ScatterGatherGateway to print duration and number of invocations.
var scatterGatherGateway = new ScatterGatherGatewayMetrics(
    new ScatterGatherGateway("DynamoScatterGather-example-requests", "DynamoScatterGather-example-parts"));

// The ScatterRequestId represents a single scatter-gather operation with its own progress.
var scatterRequestId = new ScatterRequestId("42");

// Each scatter-gather operation includes multiple sub-operation, each identified by a ScatterPartId.
var scatterPartIds = Enumerable.Range(0, 100).Select(i => new ScatterPartId(i.ToString())).ToList();

// BeginScatter initializes the state for a new scatter-gather request
await scatterGatherGateway.BeginScatter(scatterRequestId, "This is a custom text to identify this request, for debugging or troubleshooting");

// Sub-operations, that is scattered parts, can be added to the scatter-gather operation using Scatter.
// This may be called multiple times, for example because scatter parts are discovered while streaming an external resource
await scatterGatherGateway.Scatter(scatterRequestId, scatterPartIds, () =>
{
    Console.WriteLine($"Scattered {scatterPartIds.Count} parts.");
    return Task.FromResult(0);
});

// Call EndScatter once all scatter parts have been added to the scatter-gather operation.
// In case some processing have already occurred, and all scattered parts have been already
// gathered by some background worker, the HandleCompletion callback function is called,
// otherwise the scatter-gather operation is in progress, and HandleCompletion is not called.
await scatterGatherGateway.EndScatter(scatterRequestId, HandleCompletion);

// A worker will call Gather on one or more scatter parts after it finished processing them.
// This is usually done in a separate process or even application, even while scatter is still in progress.
// If the ScatterGatherGateway notices that it has just gathered the last part, it calls the
// HandleCompletion callback function.
foreach (var scatterPartId in scatterPartIds)
    await scatterGatherGateway.Gather(scatterRequestId, new[] { scatterPartId }, HandleCompletion);

// The completion function that will be called once all scattered parts have been gathered.
// This allows executing some action after the whole scatter-gather operation is completed.
static Task HandleCompletion()
{
    Console.WriteLine("All parts have been gathered.");
    return Task.CompletedTask;
}