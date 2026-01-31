namespace ScatterGather.DynamoDB;

public static class ScatterGatherFactory
{
    public static ScatterGatherGateway Create(string requestTableName, string partTableName) =>
        new(new Store(requestTableName, partTableName));

    public static ScatterGatherGateway Create(string dynamoDbServiceUrlOption, string requestTableName, string partTableName) =>
        new(new Store(dynamoDbServiceUrlOption, requestTableName, partTableName));
}