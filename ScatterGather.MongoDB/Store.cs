using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace ScatterGather.MongoDB;

internal class Store : IScatterGatherStore
{
    private record RequestDoc(string Id, DateTime CreationTime, bool ScatterCompleted, string? LockerId, string Context);
    private record PartDocId(string PartId, string RequestId);
    private record PartDoc(PartDocId Id);

    private const int MaxBatchSize = 25;
    private readonly IMongoCollection<RequestDoc> _requestCollection;
    private readonly IMongoCollection<PartDoc> _partCollection;


    public Store(IMongoDatabase mongoDatabase, string collectionNamePrefix)
    {
        _requestCollection = mongoDatabase.GetCollection<RequestDoc>($"{collectionNamePrefix}.Requests");
        _partCollection = mongoDatabase.GetCollection<PartDoc>($"{collectionNamePrefix}.Parts");
    }

    public async Task BeginScatter(ScatterRequestId requestId, string context)
    {
        await EnsureIndexes();
        await Cleanup(requestId);
        await _requestCollection.InsertOneAsync(new RequestDoc(Id: requestId.Value, CreationTime: DateTime.UtcNow, ScatterCompleted: false, LockerId: null, Context: context));
    }

    public Task MarkScattered(ScatterRequestId requestId, IEnumerable<ScatterPartId> partIds) =>
        PutParts(requestId, partIds);

    public async Task EndScatter(ScatterRequestId requestId, Func<string, Task> handleCompletion)
    {
        await _requestCollection.UpdateOneAsync(
            r => r.Id == requestId.Value,
            Builders<RequestDoc>.Update.Set(r => r.ScatterCompleted, true));
        await TryHandleCompletion(requestId, lockerId: $"{nameof(EndScatter)}-{requestId.Value}", handleCompletion);
    }

    public async Task MarkGathered(ScatterRequestId requestId, IReadOnlyCollection<ScatterPartId> partIds, Func<string, Task> handleCompletion)
    {
        await DeleteParts(requestId, partIds);
        await TryHandleCompletion(requestId, lockerId: $"{nameof(MarkGathered)}-{partIds.First().Value}", handleCompletion);
    }

    private Task EnsureIndexes() =>
        _partCollection.Indexes.CreateOneAsync(new CreateIndexModel<PartDoc>(Builders<PartDoc>.IndexKeys.Ascending(p => p.Id.RequestId)));

    private async Task TryHandleCompletion(ScatterRequestId requestId, string lockerId, Func<string, Task> handleCompletion)
    {
        var completion = await CheckForCompletion(requestId, lockerId);
        await completion.Match(
            onCompleted: async context =>
            {
                await handleCompletion(context);
                await Cleanup(requestId);
            },
            onNotCompleted: () => Task.CompletedTask);
    }

    private async Task<Completion> CheckForCompletion(ScatterRequestId requestId, string lockerId)
    {
        var firstPart = await _partCollection.Find(p => p.Id.RequestId == requestId.Value).FirstOrDefaultAsync();
        return firstPart != null
            ? Completion.CreateNotCompleted()
            : await TryLockRequest(requestId, lockerId);
    }

    private async Task<Completion> TryLockRequest(ScatterRequestId requestId, string lockerId)
    {
        var updated = await _requestCollection.FindOneAndUpdateAsync(
            r => r.Id == requestId.Value && r.ScatterCompleted == true && (r.LockerId == null || r.LockerId == lockerId),
            Builders<RequestDoc>.Update.Set(r => r.LockerId, lockerId));
        return updated != null
            ? Completion.CreateCompleted(updated.Context)
            : Completion.CreateNotCompleted();
    }

    private async Task Cleanup(ScatterRequestId requestId)
    {
        await _partCollection.DeleteManyAsync(p => p.Id.RequestId == requestId.Value);
        await _requestCollection.DeleteOneAsync(r => r.Id == requestId.Value);
    }

    private async Task PutParts(ScatterRequestId requestId, IEnumerable<ScatterPartId> partIds)
    {
        foreach (var partIdChunk in partIds.Chunk(MaxBatchSize))
            await _partCollection.InsertManyAsync(partIdChunk.Select(partId => new PartDoc(new PartDocId(partId.Value, requestId.Value))));
    }

    private async Task DeleteParts(ScatterRequestId requestId, IEnumerable<ScatterPartId> partIds)
    {
        foreach (var partIdChunk in partIds.Chunk(MaxBatchSize))
        {
            var partItemIds = partIdChunk.Select(partId => new PartDocId(partId.Value, requestId.Value)).ToList();
            await _partCollection.DeleteManyAsync(p => partItemIds.Contains(p.Id));
        }
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