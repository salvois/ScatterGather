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
using System.Diagnostics;
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

    private static Dictionary<string, AttributeValue> PartKey(ScatterRequestId requestId, ScatterPartId partId) =>
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
        await _dynamoDbClient.PutItemAsync(_requestTableName, new Dictionary<string, AttributeValue>
        {
            ["RequestId"] = new() { S = requestId.Value },
            ["CreationTime"] = new() { S = DateTime.UtcNow.ToString("O") },
            ["Info"] = new() { S = info }
        });
    }

    public async Task<T> Scatter<T>(ScatterRequestId requestId, IEnumerable<ScatterPartId> partIds, Func<Task<T>> callback)
    {
        foreach (var partIdChunk in partIds.Chunk(MaxBatchSize))
            await _dynamoDbClient.BatchWriteItemAsync(new BatchWriteItemRequest(new Dictionary<string, List<WriteRequest>>
            {
                [_partTableName] = partIdChunk
                    .Select(partId => new WriteRequest(new PutRequest(PartKey(requestId, partId))))
                    .ToList()
            }));
        return await callback();
    }

    public async Task EndScatter(ScatterRequestId requestId, Func<Task> handleCompletion)
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
        if (await ShouldHandleCompletion(requestId, $"{nameof(EndScatter)}-{requestId.Value}"))
        {
            await handleCompletion();
            await Cleanup(requestId);
        }
    }

    public async Task Gather(ScatterRequestId requestId, IReadOnlyCollection<ScatterPartId> partIds, Func<Task> handleCompletion)
    {
        foreach (var partIdChunk in partIds.Chunk(MaxBatchSize))
            await _dynamoDbClient.BatchWriteItemAsync(new BatchWriteItemRequest(new Dictionary<string, List<WriteRequest>>
            {
                [_partTableName] = partIdChunk
                    .Select(partId => new WriteRequest(new DeleteRequest(PartKey(requestId, partId))))
                    .ToList()
            }));
        var shouldHandleCompletion = await ShouldHandleCompletion(requestId, $"{nameof(Gather)}-{partIds.First().Value}");
        if (shouldHandleCompletion)
        {
            await handleCompletion();
            await Cleanup(requestId);
        }
    }

    private async Task<bool> ShouldHandleCompletion(ScatterRequestId requestId, string lockerId) =>
        !await StillHasParts(requestId) && await TryLockRequest(requestId, lockerId);

    private async Task<bool> StillHasParts(ScatterRequestId requestId)
    {
        var queryResponse = await _dynamoDbClient.QueryAsync(new QueryRequest
        {
            TableName = _partTableName,
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":RequestId"] = new() { S = requestId.Value }
            },
            KeyConditionExpression = "RequestId = :RequestId",
            ConsistentRead = true,
            Limit = 1
        });
        return queryResponse.Count != 0;
    }

    private async Task<bool> TryLockRequest(ScatterRequestId requestId, string lockerId)
    {
        try
        {
            await _dynamoDbClient.UpdateItemAsync(new UpdateItemRequest
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
            });
            return true;
        }
        catch (ConditionalCheckFailedException)
        {
            return false;
        }
    }

    private async Task Cleanup(ScatterRequestId requestId)
    {
        while (true)
        {
            var queryResponse = await _dynamoDbClient.QueryAsync(new QueryRequest
            {
                TableName = _partTableName,
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":RequestId"] = new() { S = requestId.Value }
                },
                KeyConditionExpression = "RequestId = :RequestId",
                ConsistentRead = true
            });
            if (queryResponse.Count == 0)
                break;
            foreach (var itemChunk in queryResponse.Items.Chunk(MaxBatchSize))
                await _dynamoDbClient.BatchWriteItemAsync(new BatchWriteItemRequest(new Dictionary<string, List<WriteRequest>>
                {
                    [_partTableName] = itemChunk
                        .Select(part => new WriteRequest(new DeleteRequest(PartKey(requestId, new ScatterPartId(part["PartId"].S)))))
                        .ToList()
                }));
        }
        await _dynamoDbClient.DeleteItemAsync(_requestTableName, RequestKey(requestId));
    }
}