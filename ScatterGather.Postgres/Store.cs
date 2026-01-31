using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ScatterGather.Postgres;

public class Store(string connectionString, string requestTableName, string partTableName) : IScatterGatherStore
{
    private const int MaxBatchSize = 100;

    public async Task BeginScatter(ScatterRequestId requestId, string context)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await CreateTables(conn);
        await Cleanup(conn, requestId);
        await using var cmd = new NpgsqlCommand($"""
                                                 INSERT INTO {requestTableName} (request_id, scatter_completed, locker_id, context)
                                                 VALUES (@requestId, false, null, @context);
                                                 """, conn);
        cmd.Parameters.AddWithValue("requestId", requestId.Value);
        cmd.Parameters.AddWithValue("context", context);
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task MarkScattered(ScatterRequestId requestId, IEnumerable<ScatterPartId> partIds)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await PutParts(conn, requestId, partIds);
    }

    public async Task EndScatter(ScatterRequestId requestId, Func<string, Task> handleCompletion)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand($"UPDATE {requestTableName} SET scatter_completed=true WHERE request_id=@requestId;", conn);
        cmd.Parameters.AddWithValue("requestId", requestId.Value);
        await cmd.ExecuteNonQueryAsync();
        await TryHandleCompletion(conn, requestId, lockerId: $"{nameof(EndScatter)}-{requestId.Value}", handleCompletion);
    }

    public async Task MarkGathered(ScatterRequestId requestId, IReadOnlyCollection<ScatterPartId> partIds, Func<string, Task> handleCompletion)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await DeleteParts(conn, requestId, partIds);
        await TryHandleCompletion(conn, requestId, lockerId: $"{nameof(MarkGathered)}-{partIds.First().Value}", handleCompletion);
    }

    private async Task CreateTables(NpgsqlConnection conn)
    {
        await using var cmd = new NpgsqlCommand(
            $"""
             CREATE TABLE IF NOT EXISTS {requestTableName} (
                 request_id text NOT NULL PRIMARY KEY,
                 creation_time timestamp with time zone NOT NULL DEFAULT now(),
                 scatter_completed boolean NOT NULL,
                 locker_id text,
                 context text NOT NULL
             );
             CREATE TABLE IF NOT EXISTS {partTableName} (
                 request_id text NOT NULL REFERENCES scatter_requests (request_id),
                 part_id text NOT NULL,
                 PRIMARY KEY (request_id, part_id)
             );
             """, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task TryHandleCompletion(NpgsqlConnection conn, ScatterRequestId requestId, string lockerId, Func<string, Task> handleCompletion)
    {
        var completion = await CheckForCompletion(conn, requestId, lockerId);
        await completion.Match(
            onCompleted: async context =>
            {
                await handleCompletion(context);
                await Cleanup(conn, requestId);
            },
            onNotCompleted: () => Task.CompletedTask);
    }

    private async Task<Completion> CheckForCompletion(NpgsqlConnection conn, ScatterRequestId requestId, string lockerId)
    {
        bool updated;
        await using (var cmd = new NpgsqlCommand($"SELECT EXISTS (SELECT 1 FROM {partTableName} WHERE request_id=@requestId);", conn))
        {
            cmd.Parameters.AddWithValue("requestId", requestId.Value);
            await using var reader = await cmd.ExecuteReaderAsync();
            await reader.ReadAsync();
            updated = reader.GetBoolean(0);
        }
        return updated
            ? Completion.CreateNotCompleted()
            : await TryLockRequest(conn, requestId, lockerId);
    }

    private async Task<Completion> TryLockRequest(NpgsqlConnection conn, ScatterRequestId requestId, string lockerId)
    {
        await using var cmd = new NpgsqlCommand($"""
                                                 UPDATE {requestTableName} SET locker_id=@lockerId
                                                 WHERE request_id=@requestId AND scatter_completed=true AND (locker_id IS NULL OR locker_id=@lockerId)
                                                 RETURNING context;
                                                 """, conn);
        cmd.Parameters.AddWithValue("lockerId", lockerId);
        cmd.Parameters.AddWithValue("requestId", requestId.Value);
        await using var reader = await cmd.ExecuteReaderAsync();
        return await reader.ReadAsync()
            ? Completion.CreateCompleted(reader.GetString(0))
            : Completion.CreateNotCompleted();
    }

    private async Task PutParts(NpgsqlConnection conn, ScatterRequestId requestId, IEnumerable<ScatterPartId> partIds)
    {
        foreach (var partIdChunk in partIds.Chunk(MaxBatchSize))
        {
            var rows = string.Join(", ", partIdChunk.Select((_, i) => $"(@requestId, @partId{i})"));
            await using var cmd = new NpgsqlCommand($"INSERT INTO {partTableName} (request_id, part_id) VALUES {rows};", conn);
            cmd.Parameters.AddWithValue("requestId", requestId.Value);
            var i = 0;
            foreach (var partId in partIdChunk)
            {
                cmd.Parameters.AddWithValue($"partId{i}", partId.Value);
                i++;
            }
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task DeleteParts(NpgsqlConnection conn, ScatterRequestId requestId, IEnumerable<ScatterPartId> partIds)
    {
        foreach (var partIdChunk in partIds.Chunk(MaxBatchSize))
        {
            await using var cmd = new NpgsqlCommand($"DELETE FROM {partTableName} WHERE request_id=@requestId AND part_id=ANY(@partIds);", conn);
            cmd.Parameters.AddWithValue("requestId", requestId.Value);
            cmd.Parameters.AddWithValue("partIds", partIdChunk.Select(p => p.Value).ToArray());
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task Cleanup(NpgsqlConnection conn, ScatterRequestId requestId)
    {
        await using var cmd = new NpgsqlCommand($"""
                                                 DELETE FROM {partTableName} WHERE request_id=@requestId;
                                                 DELETE FROM {requestTableName} WHERE request_id=@requestId;
                                                 """, conn);
        cmd.Parameters.AddWithValue("requestId", requestId.Value);
        await cmd.ExecuteNonQueryAsync();
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