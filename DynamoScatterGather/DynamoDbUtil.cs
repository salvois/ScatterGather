using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2;
using System.Threading.Tasks;
using System;

namespace DynamoScatterGather;

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