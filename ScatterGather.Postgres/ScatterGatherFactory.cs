namespace ScatterGather.Postgres;

public static class ScatterGatherFactory
{
    public static ScatterGatherGateway Create(string connectionString, string requestTableName, string partTableName) =>
        new(new Store(connectionString, requestTableName, partTableName));
}