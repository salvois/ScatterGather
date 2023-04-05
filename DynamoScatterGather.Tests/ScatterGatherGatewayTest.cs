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
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2;
using FluentAssertions;
using NUnit.Framework;

namespace DynamoScatterGather.Tests;

[TestFixture]
public static class ScatterGatherGatewayTest
{
    private const string DynamoDbServiceUrl = "http://localhost:8998";
    private const string RequestTableName = "ScatterRequests";
    private const string PartTableName = "ScatterParts";

    private static Task<int> DoNothingCallback() =>
        Task.FromResult(0);

    private static AmazonDynamoDBClient CreateDynamoDbClient() =>
        new(new AmazonDynamoDBConfig { ServiceURL = DynamoDbServiceUrl });

    private static IScatterGatherGateway CreateScatterGatherGateway() =>
        new ScatterGatherGateway(DynamoDbServiceUrl, RequestTableName, PartTableName);

    [OneTimeSetUp]
    public static async Task OneTimeSetUp()
    {
        var client = CreateDynamoDbClient();
        await client.CreateTableAsync(
            tableName: RequestTableName,
            keySchema: new List<KeySchemaElement> { new("RequestId", KeyType.HASH) },
            attributeDefinitions: new List<AttributeDefinition> { new("RequestId", ScalarAttributeType.S) },
            provisionedThroughput: new ProvisionedThroughput(1L, 1L));
        await client.CreateTableAsync(
            tableName: PartTableName,
            keySchema: new List<KeySchemaElement> { new("RequestId", KeyType.HASH), new("PartId", KeyType.RANGE) },
            attributeDefinitions: new List<AttributeDefinition> { new("RequestId", ScalarAttributeType.S), new("PartId", ScalarAttributeType.S) },
            provisionedThroughput: new ProvisionedThroughput(1L, 1L));
    }

    [OneTimeTearDown]
    public static async Task OneTimeTearDown()
    {
        var client = CreateDynamoDbClient();
        await client.DeleteTableAsync(PartTableName);
        await client.DeleteTableAsync(RequestTableName);
    }

    [Test]
    public static async Task NothingToScatter()
    {
        var gateway = CreateScatterGatherGateway();
        var completionHandler = new CompletionHandler();
        var scatterRequestId = new ScatterRequestId("requestId");

        await gateway.BeginScatter(scatterRequestId, "context");
        await gateway.EndScatter(scatterRequestId, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeTrue();
        completionHandler.Context.Should().Be("context");
    }

    [Test]
    public static async Task SimpleScatterGather()
    {
        var gateway = CreateScatterGatherGateway();
        var completionHandler = new CompletionHandler();
        var scatterRequestId = new ScatterRequestId("requestId");

        await gateway.BeginScatter(scatterRequestId, "context");
        await gateway.Scatter(scatterRequestId, new ScatterPartId[] { new("lorem"), new("ipsum") }, DoNothingCallback);
        await gateway.EndScatter(scatterRequestId, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeFalse();

        await gateway.Gather(scatterRequestId, new ScatterPartId[] { new("ipsum") }, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeFalse();
        await gateway.Gather(scatterRequestId, new ScatterPartId[] { new("lorem") }, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeTrue();
        completionHandler.Context.Should().Be("context");
    }

    [Test]
    public static async Task GatherFasterThanScatter()
    {
        var gateway = CreateScatterGatherGateway();
        var completionHandler = new CompletionHandler();
        var scatterRequestId = new ScatterRequestId("requestId");
        await gateway.BeginScatter(scatterRequestId, "context");

        await gateway.Scatter(scatterRequestId, new ScatterPartId[] { new("lorem") }, DoNothingCallback);
        await gateway.Gather(scatterRequestId, new ScatterPartId[] { new("lorem") }, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeFalse();

        await gateway.EndScatter(scatterRequestId, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeTrue();
    }

    [Test]
    public static async Task DuplicateBeforeCompletion()
    {
        var gateway = CreateScatterGatherGateway();
        var completionHandler = new CompletionHandler();
        var scatterRequestId = new ScatterRequestId("requestId");

        await gateway.BeginScatter(scatterRequestId, "context");
        await gateway.Scatter(scatterRequestId, new ScatterPartId[] { new("lorem"), new("ipsum") }, DoNothingCallback);
        await gateway.EndScatter(scatterRequestId, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeFalse();

        await gateway.Gather(scatterRequestId, new ScatterPartId[] { new("ipsum") }, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeFalse();
        await gateway.Gather(scatterRequestId, new ScatterPartId[] { new("ipsum") }, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeFalse();
        await gateway.Gather(scatterRequestId, new ScatterPartId[] { new("lorem") }, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeTrue();
    }

    [Test]
    public static async Task DuplicateAfterCompletion()
    {
        var gateway = CreateScatterGatherGateway();
        var completionHandler = new CompletionHandler();
        var scatterRequestId = new ScatterRequestId("requestId");

        await gateway.BeginScatter(scatterRequestId, "context");
        await gateway.Scatter(scatterRequestId, new ScatterPartId[] { new("lorem"), new("ipsum") }, DoNothingCallback);
        await gateway.EndScatter(scatterRequestId, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeFalse();

        await gateway.Gather(scatterRequestId, new ScatterPartId[] { new("ipsum") }, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeFalse();
        await gateway.Gather(scatterRequestId, new ScatterPartId[] { new("lorem") }, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeTrue();

        completionHandler = new CompletionHandler();
        await gateway.Gather(scatterRequestId, new ScatterPartId[] { new("lorem") }, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeFalse();
    }

    [Test]
    public static async Task ErrorOnCompletionHandlerAndRetry()
    {
        var gateway = CreateScatterGatherGateway();
        var completionHandler = new CompletionHandler();
        static Task HandleCompletionThrowing(string context) => throw new DivideByZeroException();
        var scatterRequestId = new ScatterRequestId("requestId");

        await gateway.BeginScatter(scatterRequestId, "context");
        await gateway.Scatter(scatterRequestId, new ScatterPartId[] { new("lorem"), new("ipsum") }, DoNothingCallback);
        await gateway.EndScatter(scatterRequestId, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeFalse();

        await gateway.Gather(scatterRequestId, new ScatterPartId[] { new("ipsum") }, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeFalse();
        var act = () => gateway.Gather(scatterRequestId, new ScatterPartId[] { new("lorem") }, HandleCompletionThrowing);
        await act.Should().ThrowAsync<DivideByZeroException>();
        await gateway.Gather(scatterRequestId, new ScatterPartId[] { new("lorem") }, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeTrue();
    }

    [Test]
    public static async Task RetryScatterWithNewIds()
    {
        var gateway = CreateScatterGatherGateway();
        var completionHandler = new CompletionHandler();
        var scatterRequestId = new ScatterRequestId("requestId");

        await gateway.BeginScatter(scatterRequestId, "context");
        await gateway.Scatter(scatterRequestId, new ScatterPartId[] { new("lorem"), new("ipsum"), new("dolor"), new("consectetur") }, DoNothingCallback);
        await gateway.EndScatter(scatterRequestId, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeFalse();

        await gateway.Gather(scatterRequestId, new ScatterPartId[] { new("ipsum") }, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeFalse();
        await gateway.Gather(scatterRequestId, new ScatterPartId[] { new("dolor") }, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeFalse();

        await gateway.BeginScatter(scatterRequestId, "context");
        await gateway.Scatter(scatterRequestId, new ScatterPartId[] { new("dolor"), new("sit"), new("amet") }, DoNothingCallback);
        await gateway.EndScatter(scatterRequestId, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeFalse();

        await gateway.Gather(scatterRequestId, new ScatterPartId[] { new("sit") }, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeFalse();
        await gateway.Gather(scatterRequestId, new ScatterPartId[] { new("lorem") }, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeFalse();
        await gateway.Gather(scatterRequestId, new ScatterPartId[] { new("dolor") }, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeFalse();
        await gateway.Gather(scatterRequestId, new ScatterPartId[] { new("amet") }, completionHandler.HandleCompletion);
        completionHandler.Completed.Should().BeTrue();
    }

    private class CompletionHandler
    {
        public bool Completed { get; private set; }
        public string Context { get; private set; }

        public CompletionHandler()
        {
            Completed = false;
            Context = "";
        }

        public Task HandleCompletion(string context)
        {
            Completed = true;
            Context = context;
            return Task.CompletedTask;
        }
    }
}