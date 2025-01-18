/*
ScatterGather - .NET library to implement the scatter-gather pattern
using a database to store distributed progress state

Copyright 2023-2025 Salvatore ISAJA. All rights reserved.

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
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace ScatterGather.DynamoDB;

internal static class DynamoDbUtil
{
    public static async Task SafeCreateTable(AmazonDynamoDBClient dynamoDbClient, CreateTableRequest createTableRequest)
    {
        TableStatus tableStatus;
        try
        {
            var createTableResponse = await dynamoDbClient.CreateTableAsync(createTableRequest);
            tableStatus = createTableResponse.TableDescription.TableStatus;
        }
        catch (ResourceInUseException)
        {
            return;
        }
        for (var i = 0; i < 60; i++)
        {
            if (tableStatus == TableStatus.ACTIVE)
                return;
            await Task.Delay(TimeSpan.FromSeconds(1));
            try
            {
                var describeTableResponse = await dynamoDbClient.DescribeTableAsync(createTableRequest.TableName);
                tableStatus = describeTableResponse.Table.TableStatus;
            }
            catch (ResourceNotFoundException)
            {
            }
        }
        throw new ResourceNotFoundException($"Could not create table {createTableRequest.TableName}");
    }
}