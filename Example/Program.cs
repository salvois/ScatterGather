// MIT License — Copyright (c) 2023–2026 Salvatore ISAJA
// Example file for ScatterGather library
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Driver;
using ScatterGather;

// Choose your scatter-gather gateway implementation
//var scatterGatherGateway = CreateDynamoScatterGatherGateway(); //46s scatter+47s gather for 3000 parts
//var scatterGatherGateway = CreateMongoScatterGatherGateway(); //6s scatter+30s gather for 3000 parts
var scatterGatherGateway = CreatePostgresScatterGatherGateway(); //7s scatter+11s gather for 3000 parts

// The ScatterRequestId represents a single scatter-gather operation with its own progress
var scatterRequestId = new ScatterRequestId("42");

// Each scatter-gather operation includes multiple sub-operation, each identified by a ScatterPartId
var parts = Enumerable.Range(0, 3000).Select(i => new Part(new ScatterPartId(i.ToString()))).ToList();

// Create a scatter scope that will collect all sub-operations, or scattered parts.
// Once the scope is disposed, the scatter operation is considered completed.
// In case some processing have already occurred, and all scattered parts have been already
// gathered by some background worker, the HandleCompletion callback function is called,
// otherwise the scatter-gather operation is in progress, and HandleCompletion is not called
var stopwatch = Stopwatch.StartNew();
await using (var scatterScope = await scatterGatherGateway.CreateScatterScopeAsync(
                 requestId: scatterRequestId,
                 context: new Context("This is a custom text associated with this request"),
                 handleCompletion: HandleCompletion))
{
    // AddAsync may be called multiple times, for example because scatter parts are discovered while streaming an external resource.
    // The "process" callback typically sends a message to a worker through a message queue
    foreach (var part in parts)
    {
        Console.WriteLine($"Adding part {part.PartId} to scatter scope");
        await scatterScope.AddAsync(partIds: [part.PartId], process: () => Task.CompletedTask);
    }
}
Console.WriteLine($"Done scattering in {stopwatch.Elapsed}");

// Here we are on the worker side of the scatter-gather operation.
// A worker will call GatherAsync on one or more scatter parts after it finished processing them.
// This is usually done in a separate process or even application, even while scatter is still in progress.
// If the ScatterGatherGateway notices that it has just gathered the last part, it calls the
// HandleCompletion callback function.
stopwatch.Restart();
foreach (var part in parts)
{
    Console.WriteLine($"Gathering part {part.PartId}");
    await scatterGatherGateway.GatherAsync<Context>(
        requestId: scatterRequestId,
        partIds: [part.PartId],
        process: () => Task.CompletedTask,
        handleCompletion: HandleCompletion);
}
Console.WriteLine($"Done gathering in {stopwatch.Elapsed}");


// This creates a scatter-gather gateway using two MongoDB collections in the specified database to store progress.
// These collections are named after the specified prefix, followed by .Requests and .Parts respectively.
// Here we decorate our ScatterGatherGateway to print duration and number of invocations.
static ScatterGatherGateway CreateMongoScatterGatherGateway()
{
    var mongoClient = new MongoClient(new MongoUrl("mongodb://localhost:27017/"));
    return ScatterGather.MongoDB.ScatterGatherFactory.Create(mongoClient.GetDatabase("MongoScatterGatherExample"), "ScatterGather");
}

// This creates a scatter-gather gateway using two DynamoDB tables to store progress.
// They are used to store scatter requests and scattered parts respectively.
// They are automatically created if they don't exist.
static ScatterGatherGateway CreateDynamoScatterGatherGateway()
{
    Environment.SetEnvironmentVariable("AWS_ACCESS_KEY_ID", "test");
    Environment.SetEnvironmentVariable("AWS_SECRET_ACCESS_KEY", "test");
    return ScatterGather.DynamoDB.ScatterGatherFactory.Create("http://localhost:8998", "DynamoScatterGather-example-requests", "DynamoScatterGather-example-parts");
}

// This creates a scatter-gather gateway using two PostgreSQL tables to store progress.
// They are used to store scatter requests and scattered parts respectively.
// They are automatically created if they don't exist.
static ScatterGatherGateway CreatePostgresScatterGatherGateway() =>
    ScatterGather.Postgres.ScatterGatherFactory.Create("Host=localhost;Username=postgres;Password=password;Database=postgres", "scatter_requests", "scatter_parts");

// The completion function that will be called once all scattered parts have been gathered.
// This allows executing some action after the whole scatter-gather operation is completed.
static Task HandleCompletion(Context context)
{
    Console.WriteLine($"All parts have been gathered for context: {context.Text}");
    return Task.CompletedTask;
}

record Part(ScatterPartId PartId);

record Context(string Text);