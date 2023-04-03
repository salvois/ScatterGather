/*
DynamoScatterGather - .NET library to implement the scatter-gather pattern
using Amazon DynamoDB to store progress state

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
using System.Threading.Tasks;

namespace DynamoScatterGather;

public readonly record struct ScatterRequestId(string Value);

public readonly record struct ScatterPartId(string Value);

public interface IScatterGatherGateway
{
    Task BeginScatter(ScatterRequestId requestId, string info);
    Task Scatter(ScatterRequestId requestId, IEnumerable<ScatterPartId> partIds, Func<Task> callback);
    Task<T> Scatter<T>(ScatterRequestId requestId, IEnumerable<ScatterPartId> partIds, Func<Task<T>> callback);
    Task EndScatter(ScatterRequestId requestId, Func<Task> handleCompletion);
    Task Gather(ScatterRequestId requestId, IReadOnlyCollection<ScatterPartId> partIds, Func<Task> handleCompletion);
}