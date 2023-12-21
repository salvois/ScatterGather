/*
ScatterGather - .NET library to implement the scatter-gather pattern
using a database to store distributed progress state

Copyright 2023 Salvatore ISAJA. All rights reserved.

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
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace ScatterGather.MongoDB;

public class ScatterGatherGateway : IScatterGatherGateway
{
    private record RequestDoc(string Id, DateTime CreationTime, bool ScatterCompleted, string? LockerId, string Context);
    private record PartDocId(string PartId, string RequestId);
    private record PartDoc(PartDocId Id);

    private const int MaxBatchSize = 25;
    private readonly IMongoCollection<RequestDoc> _requestCollection;
    private readonly IMongoCollection<PartDoc> _partCollection;


    public ScatterGatherGateway(IMongoDatabase mongoDatabase, string collectionNamePrefix)
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

    public async Task Scatter(ScatterRequestId requestId, IEnumerable<ScatterPartId> partIds, Func<Task> callback)
    {
        await PutParts(requestId, partIds);
        await callback();
    }

    public async Task<T> Scatter<T>(ScatterRequestId requestId, IEnumerable<ScatterPartId> partIds, Func<Task<T>> callback)
    {
        await PutParts(requestId, partIds);
        return await callback();
    }

    public async Task EndScatter(ScatterRequestId requestId, Func<string, Task> handleCompletion)
    {
        await _requestCollection.UpdateOneAsync(
            r => r.Id == requestId.Value,
            Builders<RequestDoc>.Update.Set(r => r.ScatterCompleted, true));
        await TryHandleCompletion(requestId, lockerId: $"{nameof(EndScatter)}-{requestId.Value}", handleCompletion);
    }

    public async Task Gather(ScatterRequestId requestId, IReadOnlyCollection<ScatterPartId> partIds, Func<string, Task> handleCompletion)
    {
        await DeleteParts(requestId, partIds);
        await TryHandleCompletion(requestId, lockerId: $"{nameof(Gather)}-{partIds.First().Value}", handleCompletion);
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