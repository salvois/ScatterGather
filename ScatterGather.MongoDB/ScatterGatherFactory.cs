using MongoDB.Driver;

namespace ScatterGather.MongoDB;

public static class ScatterGatherFactory
{
    public static ScatterGatherGateway Create(IMongoDatabase mongoDatabase, string collectionNamePrefix) =>
        new(new Store(mongoDatabase, collectionNamePrefix));
}