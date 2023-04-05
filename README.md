# DynamoScatterGather - .NET library to implement the scatter-gather pattern using Amazon DynamoDB to store progress state

[![NuGet](https://img.shields.io/nuget/v/DynamoScatterGather.svg)](https://www.nuget.org/packages/DynamoScatterGather)

[Scatter-gather](https://www.enterpriseintegrationpatterns.com/patterns/messaging/BroadcastAggregate.html) is an enteprise integration pattern where a single big operation is split into a number of sub-operations, usually performed by separate workers, then some other operation must be carried on after all sub-operations have finished.

This library provides means to keep track of how many scattered sub-operations have been completed, that is gathered, using Amazon DynamoDB to store that state.

## Use case

A typical scenario involves a component that is asked to process a large operation, and a set of workers this component wants to delegate parts of that large operation. For example, the first component may want to send messages to workers using a message queue, keeping processing of parts asynchronous.

Once a worker completes processing of a part, it may send a message back to the first component, or even to a third one, which act as an aggregator for the processed parts. When all parts have been processed, the large operation may move forward.

The first component performs a *scatter* of the large operation, while each worker perform a *gather*.

With the `ScatterGatherGateway` provided by this libary, when the worker performs a *gather*, and the `ScatterGatherGateway` notices it was the last part left, it calls a callback function in the context of that worker, and only that worker.

## Comparison with AWS Step Functions

The scatter-gather pattern may be implemented on AWS using the [Map state](https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-map-state.html) of Step functions. Using Map, you can split an operation across multiple workers. If you need to process a large number of sub-operations, you can run a so-called Distributed Map state, which takes its input from a file saved in an S3 bucket, containing the list the parts to scatter.

The Map state of Step functions will wait for all workers to complete before moving forward. In case of errors, the Map state will produce reports containing the failed or pending parts (saving them into an S3 bucket, in case of a Distributed Map state), which you can use to fabricate a new input to resume the failed state machine.

As an alternative, you might want to use the `ScatterGatherGateway` provided by this library in the following cases:

- you want to decouple the scattering component and workers using a message queue, so that processing is asynchronous
- in case of errors, you want to take advantage of dead-letter queues so that you can restart a failing scatter-gather operation for the failed parts, using the same mechanics of non-scatter-gather operations
- you want more control over the progress state of the scatter-gather operation
- you don't want to create a file on S3 to list the scattered parts for a Distributed Map state
- you don't want to parse the result files saved by the Distributed Map state to S3 to know which parts failed and which parts did not even start

## Usage

The `ScatterGatherGateway` class implements a [gateway](https://martinfowler.com/articles/gateway-pattern.html) to DynamoDB to manage state of a scatter-gather operation.

Typically, the scattering component creates a unique `ScatterRequestId` and executes a `BeginScatter`/`Scatter`/`EndScatter` sequence, maybe calling `Scatter` multiple times to add more parts to the scatter-gather operation (that is, `ScatterGatherGateway` is "stream friendly").

`BeginScatter` initializes a new scatter-gather request, identified by a unique `ScatterRequestId`, and accepts an arbitrary context string that is associated with that request. This context string can contain any text meaningful to the application, perhaps even some JSON-encoded data, and it will be passed to the completion handler function. The size of the context string is limited by the size of a DynamoDB item, that is less than 400 kB including other request fields.

`Scatter` accepts a callback function to execute on the scattered part, for example to send a message to a worker. Each scattered part must be identified by a `ScatterPartId` that must be unique in that scatter-gather operation.

`EndScatter` signals that all parts have been scattered, thus it is now possible to expect completion of the whole scatter-gather operation. The `EndScatter` calls the completion handler function in case all parts, if any, have been gathered so fast that the scatter-gather operation is already completed.

A worker typically call `Gather` after processing its part (after is important for idempotency), to mark that part as complete, passing a completion handler function that will be executed if the `ScatterGatherGateway` notices that that was the last part to be gathered.

Note that you must first create two tables on DynamoDB:

- a table for scatter requests, that will contain an item for each `ScatterRequestId` that has been created, having a simple primary key composed of a `RequestId` string field as the partition key
- a table for scattered parts, that will contain an item for each `(ScatterRequestId, ScatterPartId)` pair, that is each scattered sub-operation of an operation, having a composite primary key composed of a `RequestId` string field as the partition key and a `PartId` string field as the sort key

Performance-wise, all methods of `ScatterGatherGateway` run time is proportional to the number of elements passed to that method, but irrespective of the number of elements in the whole scatter-gather operation.

Finally, note that only one worker will be able to call the completion handler function, because the `ScatterGatherGateway` will protect treat it as a critical section. Also note that, in case of errors during the completion handler function, restarting the worker that was processing the last `Gather` will restart the completion handler function (that is, the critical section is re-entrant).

Here is a complete example:

```csharp
// This example uses two DynamoDB tables that are assumed to be already existing.
// They are used to store scatter requests and scattered parts respectively.
// The key of the request table must be a single string field named RequestId.
// The key of the part table must be a pair of string fields named RequestId and PartId.
var scatterGatherGateway = new ScatterGatherGateway("DynamoScatterGather-example-requests", "DynamoScatterGather-example-parts"));

// The ScatterRequestId represents a single scatter-gather operation with its own progress.
var scatterRequestId = new ScatterRequestId("42");

// Each scatter-gather operation includes multiple sub-operation, each identified by a ScatterPartId.
var scatterPartIds = Enumerable.Range(0, 100).Select(i => new ScatterPartId(i.ToString())).ToList();

// BeginScatter initializes the state for a new scatter-gather request
await scatterGatherGateway.BeginScatter(scatterRequestId, "This is a custom text associated with this request");

// Sub-operations, that is scattered parts, can be added to the scatter-gather operation using Scatter.
// This may be called multiple times, for example because scatter parts are discovered while streaming an external resource.
await scatterGatherGateway.Scatter(scatterRequestId, scatterPartIds, () =>
{
    // In this callback you typically send a message to a worker through a message queue.
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
static Task HandleCompletion(string context)
{
    Console.WriteLine($"All parts have been gathered for context: {context}");
    return Task.CompletedTask;
}
```

## Testing locally

Automated tests are run against a container created from the [DynamoDB-local Docker image](https://hub.docker.com/r/amazon/dynamodb-local/). A docker-compose file is provided so that you can run `docker-compose up` to run the DynamoDB container before running tests. The DynamoDB container will be mapped on TCP port 8998 on the host.

## Special thanks

Thanks to [Matteo Pierangeli](https://github.com/matpierangeli) for his code review and comments!

## License

Permissive, [2-clause BSD style](https://opensource.org/licenses/BSD-2-Clause)

DynamoScatterGather - .NET library to implement the scatter-gather pattern using Amazon DynamoDB to store progress state

Copyright 2023 Salvatore ISAJA. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.