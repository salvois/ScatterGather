using MongoDB.Driver;
using NUnit.Framework;
using ScatterGather.MongoDB;

namespace ScatterGather.Tests;

[TestFixture]
public class MongoScatterGatherGatewayTest : ScatterGatherGatewayContractTest
{
    protected override ScatterGatherGateway CreateScatterGatherGateway()
    {
        var client = new MongoClient(new MongoUrl("mongodb://localhost:27017/"));
        return ScatterGatherFactory.Create(client.GetDatabase("ScatterGatherGatewayTest"), "ScatterGather");
    }
}