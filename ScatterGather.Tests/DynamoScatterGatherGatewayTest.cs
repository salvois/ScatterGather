using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using NUnit.Framework;
using ScatterGather.DynamoDB;

namespace ScatterGather.Tests;

[TestFixture]
public class DynamoScatterGatherGatewayTest : ScatterGatherGatewayContractTest
{
    private const string DynamoDbServiceUrl = "http://localhost:8998";
    private const string RequestTableName = "ScatterRequests";
    private const string PartTableName = "ScatterParts";

    protected override ScatterGatherGateway CreateScatterGatherGateway() =>
        ScatterGatherFactory.Create(DynamoDbServiceUrl, RequestTableName, PartTableName);

    [OneTimeTearDown]
    public static async Task OneTimeTearDown()
    {
        var client = new AmazonDynamoDBClient(new AmazonDynamoDBConfig { ServiceURL = DynamoDbServiceUrl });
        await client.DeleteTableAsync(PartTableName);
        await client.DeleteTableAsync(RequestTableName);
    }
}