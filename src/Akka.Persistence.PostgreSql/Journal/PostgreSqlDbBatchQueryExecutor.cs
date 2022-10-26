//-----------------------------------------------------------------------
// <copyright file="PostgreSqlQueryExecutor.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Actor;
using Akka.Persistence.Sql.Common.Journal;
using Akka.Serialization;
using Akka.Util;
using Newtonsoft.Json;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Immutable;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Akka.Event;

namespace Akka.Persistence.PostgreSql.Journal
{
    public class PostgreSqlDbBatchQueryExecutor : PostgreSqlQueryExecutor
    {
        protected readonly ILoggingAdapter Log;
        public PostgreSqlDbBatchQueryExecutor(PostgreSqlQueryConfiguration configuration, Akka.Serialization.Serialization serialization, ITimestampProvider timestampProvider, ILoggingAdapter loggingAdapter)
            : base(configuration, serialization, timestampProvider)
        {
            Log = loggingAdapter;
        }

        protected void AddParameter(NpgsqlBatchCommand command, string parameterName, DbType parameterType, object value)
        {
            var parameter = new NpgsqlParameter()
            {
                ParameterName = parameterName,
                DbType = parameterType,
                Value = value
            };
            
            command.Parameters.Add(parameter);
        }
        protected void WriteEvent(NpgsqlBatchCommand command, IPersistentRepresentation e, IImmutableSet<string> tags)
        {
            var serializationResult = _serialize(e);
            var serializer = serializationResult.Serializer;
            var hasSerializer = serializer != null;

            string manifest = "";
            if (hasSerializer && serializer is SerializerWithStringManifest)
                manifest = ((SerializerWithStringManifest)serializer).Manifest(e.Payload);
            else if (hasSerializer && serializer.IncludeManifest)
                manifest = QualifiedName(e);
            else
                manifest = string.IsNullOrEmpty(e.Manifest) ? QualifiedName(e) : e.Manifest;
            
            AddParameter(command, "@PersistenceId", DbType.String, e.PersistenceId);
            AddParameter(command, "@SequenceNr", DbType.Int64, e.SequenceNr);
            AddParameter(command, "@Timestamp", DbType.Int64, TimestampProvider.GenerateTimestamp(e));
            AddParameter(command, "@IsDeleted", DbType.Boolean, false);
            AddParameter(command, "@Manifest", DbType.String, manifest);

            if (hasSerializer)
            {
                AddParameter(command, "@SerializerId", DbType.Int32, serializer.Identifier);
            }
            else
            {
                AddParameter(command, "@SerializerId", DbType.Int32, DBNull.Value);
            }

            command.Parameters.Add(new NpgsqlParameter("@Payload", serializationResult.DbType) { Value = serializationResult.Payload });

            if (tags.Count != 0)
            {
                var tagBuilder = new StringBuilder(";", tags.Sum(x => x.Length) + tags.Count + 1);
                foreach (var tag in tags)
                {
                    tagBuilder.Append(tag).Append(';');
                }

                AddParameter(command, "@Tag", DbType.String, tagBuilder.ToString());
            }
            else AddParameter(command, "@Tag", DbType.String, DBNull.Value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static string QualifiedName(IPersistentRepresentation e)
            => e.Payload.GetType().TypeQualifiedName();

        
        public override async Task InsertBatchAsync(DbConnection connection, CancellationToken cancellationToken, WriteJournalBatch write)
        {
            using (var tx = ((NpgsqlConnection)connection).BeginTransaction())
            {
                using (var batch = new NpgsqlBatch(((NpgsqlConnection)connection)))
                {
                    
                    batch.Transaction = tx;

                    foreach (var entry in write.EntryTags)
                    {
                        NpgsqlBatchCommand command = (NpgsqlBatchCommand)batch.CreateBatchCommand();
                        command.CommandText = this.InsertEventSql;

                        var evt = entry.Key;
                        var tags = entry.Value;

                        WriteEvent(command, evt, tags);
                        batch.BatchCommands.Add(command);
                    }
                    await batch.PrepareAsync(cancellationToken);
                    await batch.ExecuteNonQueryAsync(cancellationToken);
                    await tx.CommitAsync(cancellationToken);
                }
            }
        }
    }
}
