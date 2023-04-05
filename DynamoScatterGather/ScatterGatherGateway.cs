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
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamoScatterGather;

public class ScatterGatherGateway : IScatterGatherGateway
{
    private const int MaxBatchSize = 25;
    private readonly string _requestTableName;
    private readonly string _partTableName;
    private readonly AmazonDynamoDBClient _dynamoDbClient;

    private static Dictionary<string, AttributeValue> RequestKey(ScatterRequestId requestId) =>
        new()
        {
            ["RequestId"] = new AttributeValue { S = requestId.Value }
        };

    private static Dictionary<string, AttributeValue> RequestItem(ScatterRequestId requestId, DateTime creationTime, string context) =>
        new()
        {
            ["RequestId"] = new AttributeValue { S = requestId.Value },
            ["CreationTime"] = new AttributeValue { S = creationTime.ToString("O") },
            ["Context"] = new AttributeValue { S = context }
        };

    private static Dictionary<string, AttributeValue> PartItem(ScatterRequestId requestId, ScatterPartId partId) =>
        new()
        {
            ["RequestId"] = new AttributeValue { S = requestId.Value },
            ["PartId"] = new AttributeValue { S = partId.Value }
        };

    public ScatterGatherGateway(string requestTableName, string partTableName)
    {
        _requestTableName = requestTableName;
        _partTableName = partTableName;
        _dynamoDbClient = new AmazonDynamoDBClient();
    }

    public ScatterGatherGateway(string dynamoDbServiceUrlOption, string requestTableName, string partTableName)
    {
        _requestTableName = requestTableName;
        _partTableName = partTableName;
        _dynamoDbClient = new AmazonDynamoDBClient(new AmazonDynamoDBConfig { ServiceURL = dynamoDbServiceUrlOption });
    }

    public async Task BeginScatter(ScatterRequestId requestId, string info)
    {
        await Cleanup(requestId);
        await _dynamoDbClient.PutItemAsync(_requestTableName, RequestItem(requestId, DateTime.UtcNow, info));
    }

    public async Task Scatter(ScatterRequestId requestId, IEnumerable<ScatterPartId> partIds, Func<Task> callback)
    {
        await PutParts(requestId, partIds);
        await callback();
    }

    public async Task<T> Scatter<T>(ScatterRequestId requestId, IEnumerable<ScatterPartId> partIds, Func<Task<T>> callback)
    {
        await PutParts(requestId, partIds);
        return await callback();
    }

    public async Task EndScatter(ScatterRequestId requestId, Func<string, Task> handleCompletion)
    {
        await _dynamoDbClient.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = _requestTableName,
            Key = RequestKey(requestId),
            AttributeUpdates = new Dictionary<string, AttributeValueUpdate>
            {
                ["ScatterCompleted"] = new(new AttributeValue { BOOL = true }, AttributeAction.PUT)
            }
        });
        await TryHandleCompletion(requestId, lockerId: $"{nameof(EndScatter)}-{requestId.Value}", handleCompletion);
    }

    public async Task Gather(ScatterRequestId requestId, IReadOnlyCollection<ScatterPartId> partIds, Func<string, Task> handleCompletion)
    {
        await DeleteParts(requestId, partIds);
        await TryHandleCompletion(requestId, lockerId: $"{nameof(Gather)}-{partIds.First().Value}", handleCompletion);
    }

    private async Task TryHandleCompletion(ScatterRequestId requestId, string lockerId, Func<string, Task> handleCompletion)
    {
        var completion = await CheckForCompletion(requestId, lockerId);
        await completion.Match(
            onCompleted: async context =>
            {
                await handleCompletion(context);
                await Cleanup(requestId);
            },
            onNotCompleted: () => Task.CompletedTask);
    }

    private async Task<Completion> CheckForCompletion(ScatterRequestId requestId, string lockerId)
    {
        var partIds = await QueryPartsByRequestId(requestId, firstOnly: true);
        if (partIds.Count != 0)
            return Completion.CreateNotCompleted();
        return await TryLockRequest(requestId, lockerId);
    }

    private async Task<Completion> TryLockRequest(ScatterRequestId requestId, string lockerId)
    {
        try
        {
            var updateItemResponse = await _dynamoDbClient.UpdateItemAsync(new UpdateItemRequest
            {
                TableName = _requestTableName,
                Key = RequestKey(requestId),
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":ScatterCompleted"] = new() { BOOL = true },
                    [":LockerId"] = new() { S = lockerId }
                },
                UpdateExpression = "SET LockerId = :LockerId",
                ConditionExpression = "ScatterCompleted = :ScatterCompleted AND (attribute_not_exists (LockerId) OR LockerId = :LockerId)",
                ReturnValues = ReturnValue.ALL_NEW
            });
            return Completion.CreateCompleted(updateItemResponse.Attributes["Context"].S);
        }
        catch (ConditionalCheckFailedException)
        {
            return Completion.CreateNotCompleted();
        }
    }

    private async Task Cleanup(ScatterRequestId requestId)
    {
        while (true)
        {
            var partIds = await QueryPartsByRequestId(requestId, firstOnly: false);
            if (partIds.Count == 0)
                break;
            await DeleteParts(requestId, partIds);
        }
        await _dynamoDbClient.DeleteItemAsync(_requestTableName, RequestKey(requestId));
    }

    private async Task<IReadOnlyCollection<ScatterPartId>> QueryPartsByRequestId(ScatterRequestId requestId, bool firstOnly)
    {
        var queryRequest = new QueryRequest
        {
            TableName = _partTableName,
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":RequestId"] = new() { S = requestId.Value }
            },
            KeyConditionExpression = "RequestId = :RequestId",
            ConsistentRead = true
        };
        if (firstOnly)
            queryRequest.Limit = 1;
        var queryResponse = await _dynamoDbClient.QueryAsync(queryRequest);
        return queryResponse.Items.Select(item => new ScatterPartId(item["PartId"].S)).ToList();
    }

    private async Task PutParts(ScatterRequestId requestId, IEnumerable<ScatterPartId> partIds)
    {
        foreach (var partIdChunk in partIds.Chunk(MaxBatchSize))
            await _dynamoDbClient.BatchWriteItemAsync(new BatchWriteItemRequest(new Dictionary<string, List<WriteRequest>>
            {
                [_partTableName] = partIdChunk
                    .Select(partId => new WriteRequest(new PutRequest(PartItem(requestId, partId))))
                    .ToList()
            }));
    }

    private async Task DeleteParts(ScatterRequestId requestId, IEnumerable<ScatterPartId> partIds)
    {
        foreach (var partIdChunk in partIds.Chunk(MaxBatchSize))
            await _dynamoDbClient.BatchWriteItemAsync(new BatchWriteItemRequest(new Dictionary<string, List<WriteRequest>>
            {
                [_partTableName] = partIdChunk
                    .Select(partId => new WriteRequest(new DeleteRequest(PartItem(requestId, partId))))
                    .ToList()
            }));
    }

    private readonly record struct Completion(bool Completed, string Context)
    {
        public static Completion CreateCompleted(string context) => new(true, context);
        public static Completion CreateNotCompleted() => new(false, "");
        public T Match<T>(Func<string, T> onCompleted, Func<T> onNotCompleted) =>
            this switch
            {
                { Completed: true, Context: var c } => onCompleted(c),
                { Completed: false } => onNotCompleted()
            };
    }
}