using System.Threading.Tasks;
using Npgsql;
using NUnit.Framework;
using ScatterGather.Postgres;

namespace ScatterGather.Tests;

[TestFixture]
public class PostgresScatterGatherGatewayTest : ScatterGatherGatewayContractTest
{
    private const string ConnectionString = "Host=localhost;Username=postgres;Password=password;Database=postgres";
    private const string RequestTableName = "scatter_requests";
    private const string PartTableName = "scatter_parts";

    protected override ScatterGatherGateway CreateScatterGatherGateway() =>
        ScatterGatherFactory.Create(ConnectionString, RequestTableName, PartTableName);

    [OneTimeTearDown]
    public static async Task OneTimeTearDown()
    {
        await using var conn = new NpgsqlConnection(ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            $"""
             DROP TABLE IF EXISTS {PartTableName};
             DROP TABLE IF EXISTS {RequestTableName};
             """, conn);
        await cmd.ExecuteNonQueryAsync();
    }
}