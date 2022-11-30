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

namespace Akka.Persistence.PostgreSql.Journal
{
    public class PostgreSqlQueryExecutor : AbstractQueryExecutor
    {
        private readonly Func<IPersistentRepresentation, SerializationResult> _serialize;
        private readonly Func<Type, object, string, int?, object> _deserialize;

        public PostgreSqlQueryExecutor(PostgreSqlQueryConfiguration configuration, Akka.Serialization.Serialization serialization, ITimestampProvider timestampProvider)
            : base(configuration, serialization, timestampProvider)
        {
            var storedAs = configuration.StoredAs.ToString().ToUpperInvariant();
            var tagsColumnSize = configuration.TagsColumnSize;
            
            CreateEventsJournalSql =  $@"
                CREATE TABLE IF NOT EXISTS {Configuration.FullJournalTableName} (
                    {Configuration.OrderingColumnName} {(configuration.UseBigIntPrimaryKey ? "BIGINT GENERATED ALWAYS AS IDENTITY" : "BIGSERIAL")} NOT NULL PRIMARY KEY,
                    {Configuration.PersistenceIdColumnName} VARCHAR(255) NOT NULL,
                    {Configuration.SequenceNrColumnName} BIGINT NOT NULL,
                    {Configuration.IsDeletedColumnName} BOOLEAN NOT NULL,
                    {Configuration.TimestampColumnName} BIGINT NOT NULL,
                    {Configuration.ManifestColumnName} VARCHAR(500) NOT NULL,
                    {Configuration.PayloadColumnName} {storedAs} NOT NULL,
                    {Configuration.TagsColumnName} VARCHAR({tagsColumnSize}) NULL,
                    {Configuration.SerializerIdColumnName} INTEGER NULL,
                    CONSTRAINT {Configuration.JournalEventsTableName}_uq UNIQUE ({Configuration.PersistenceIdColumnName}, {Configuration.SequenceNrColumnName})
                );";

            CreateMetaTableSql = $@"
                CREATE TABLE IF NOT EXISTS {Configuration.FullMetaTableName} (
                    {Configuration.PersistenceIdColumnName} VARCHAR(255) NOT NULL,
                    {Configuration.SequenceNrColumnName} BIGINT NOT NULL,
                    CONSTRAINT {Configuration.MetaTableName}_pk PRIMARY KEY ({Configuration.PersistenceIdColumnName}, {Configuration.SequenceNrColumnName})
                );";

            HighestSequenceNrSql = $@"
                SELECT MAX(u.SeqNr) as SequenceNr 
                FROM (
                    SELECT MAX(e.{Configuration.SequenceNrColumnName}) as SeqNr FROM {Configuration.FullJournalTableName} e WHERE e.{Configuration.PersistenceIdColumnName} = @PersistenceId
                    UNION
                    SELECT MAX(m.{Configuration.SequenceNrColumnName}) as SeqNr FROM {Configuration.FullMetaTableName} m WHERE m.{Configuration.PersistenceIdColumnName} = @PersistenceId) as u";

            // As per https://github.com/akkadotnet/Akka.Persistence.PostgreSql/pull/72, apparently PostgreSQL does not like
            // it when you chain two deletes in a single command, so we have to split it into two.
            // The performance penalty should be minimal, depending on the network speed
            DeleteBatchSql = $@"
                DELETE FROM {Configuration.FullJournalTableName}
                WHERE {Configuration.PersistenceIdColumnName} = @PersistenceId AND {Configuration.SequenceNrColumnName} <= @ToSequenceNr;";

            DeleteBatchSqlMetadata = $@"DELETE FROM {Configuration.FullMetaTableName}
                WHERE {Configuration.PersistenceIdColumnName} = @PersistenceId AND {Configuration.SequenceNrColumnName} <= @ToSequenceNr;";

            switch (configuration.StoredAs)
            {
                case StoredAsType.ByteA:
                    _serialize = e =>
                    {
                        var payloadType = e.Payload.GetType();
                        var serializer = Serialization.FindSerializerForType(payloadType, Configuration.DefaultSerializer);

                        // TODO: hack. Replace when https://github.com/akkadotnet/akka.net/issues/3811
                        var binary = Akka.Serialization.Serialization.WithTransport(Serialization.System, () => serializer.ToBinary(e.Payload));

                        return new SerializationResult(NpgsqlDbType.Bytea, binary, serializer);
                    };
                    _deserialize = (type, payload, manifest, serializerId) =>
                    {
                        if (serializerId.HasValue)
                        {
                            // TODO: hack. Replace when https://github.com/akkadotnet/akka.net/issues/3811
                            return Serialization.Deserialize((byte[])payload, serializerId.Value, manifest);
                        }
                        else
                        {
                            // Support old writes that did not set the serializer id
                            var deserializer = Serialization.FindSerializerForType(type, Configuration.DefaultSerializer);

                            // TODO: hack. Replace when https://github.com/akkadotnet/akka.net/issues/3811
                            return Akka.Serialization.Serialization.WithTransport(Serialization.System, () => deserializer.FromBinary((byte[])payload, type));
                        }
                    }; 
                    break;
                case StoredAsType.JsonB:
                    _serialize = e => new SerializationResult(NpgsqlDbType.Jsonb, JsonConvert.SerializeObject(e.Payload, configuration.JsonSerializerSettings), null);
                    _deserialize = (type, serialized, manifest, serializerId) => JsonConvert.DeserializeObject((string)serialized, type, configuration.JsonSerializerSettings);
                    break;
                case StoredAsType.Json:
                    _serialize = e => new SerializationResult(NpgsqlDbType.Json, JsonConvert.SerializeObject(e.Payload, configuration.JsonSerializerSettings), null);
                    _deserialize = (type, serialized, manifest, serializerId) => JsonConvert.DeserializeObject((string)serialized, type, configuration.JsonSerializerSettings);
                    break;
                default:
                    throw new NotSupportedException($"{configuration.StoredAs} is not supported Db type for a payload");
            }
        }

        protected override DbCommand CreateCommand(DbConnection connection) => ((NpgsqlConnection)connection).CreateCommand();
        protected override string CreateEventsJournalSql { get; }
        protected override string CreateMetaTableSql { get; }
        protected override string HighestSequenceNrSql { get; }
        protected override string DeleteBatchSql { get; }
        protected virtual string DeleteBatchSqlMetadata { get; }

        protected override void WriteEvent(DbCommand command, IPersistentRepresentation e, IImmutableSet<string> tags)
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
            AddParameter(command, "@Timestamp", DbType.Int64, e.Timestamp);
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

        protected override IPersistentRepresentation ReadEvent(DbDataReader reader)
        {
            var persistenceId = reader.GetString(PersistenceIdIndex);
            var sequenceNr = reader.GetInt64(SequenceNrIndex);
            var timestamp = reader.GetInt64(TimestampIndex);
            var isDeleted = reader.GetBoolean(IsDeletedIndex);
            var manifest = reader.GetString(ManifestIndex);
            var raw = reader[PayloadIndex];

            int? serializerId = null;
            Type type = null;
            if (reader.IsDBNull(SerializerIdIndex))
            {
                type = Type.GetType(manifest, true);
            }
            else
            {
                serializerId = reader.GetInt32(SerializerIdIndex);
            }

            var deserialized = _deserialize(type, raw, manifest, serializerId);

            return new Persistent(deserialized, sequenceNr, persistenceId, manifest, isDeleted, ActorRefs.NoSender, null, timestamp);
        }

        public override async Task DeleteBatchAsync(DbConnection connection, CancellationToken cancellationToken, string persistenceId, long toSequenceNr)
        {
            using (var deleteCommand = GetCommand(connection, DeleteBatchSql))
            using (var deleteMetadataCommand = GetCommand(connection, DeleteBatchSqlMetadata))
            using (var highestSeqNrCommand = GetCommand(connection, HighestSequenceNrSql))
            {
                AddParameter(highestSeqNrCommand, "@PersistenceId", DbType.String, persistenceId);

                AddParameter(deleteCommand, "@PersistenceId", DbType.String, persistenceId);
                AddParameter(deleteCommand, "@ToSequenceNr", DbType.Int64, toSequenceNr);

                AddParameter(deleteMetadataCommand, "@PersistenceId", DbType.String, persistenceId);
                AddParameter(deleteMetadataCommand, "@ToSequenceNr", DbType.Int64, toSequenceNr);

                using (var tx = connection.BeginTransaction())
                {
                    deleteCommand.Transaction = tx;
                    deleteMetadataCommand.Transaction = tx;
                    highestSeqNrCommand.Transaction = tx;

                    var res = await highestSeqNrCommand.ExecuteScalarAsync(cancellationToken);
                    var highestSeqNr = res is long ? Convert.ToInt64(res) : 0L;

                    await deleteCommand.ExecuteNonQueryAsync(cancellationToken);
                    await deleteMetadataCommand.ExecuteNonQueryAsync(cancellationToken);

                    if (highestSeqNr <= toSequenceNr)
                    {
                        using (var updateCommand = GetCommand(connection, UpdateSequenceNrSql))
                        {
                            updateCommand.Transaction = tx;

                            AddParameter(updateCommand, "@PersistenceId", DbType.String, persistenceId);
                            AddParameter(updateCommand, "@SequenceNr", DbType.Int64, highestSeqNr);

                            await updateCommand.ExecuteNonQueryAsync(cancellationToken);
                            tx.Commit();
                        }
                    }
                    else tx.Commit();
                }
            }
        }
    }
    
    public class PostgreSqlQueryConfiguration : QueryConfiguration
    {
        public readonly StoredAsType StoredAs;
        public readonly JsonSerializerSettings JsonSerializerSettings;
        public readonly bool UseBigIntPrimaryKey;
        public readonly int TagsColumnSize;

        public PostgreSqlQueryConfiguration(
            string schemaName,
            string journalEventsTableName,
            string metaTableName,
            string persistenceIdColumnName,
            string sequenceNrColumnName,
            string payloadColumnName,
            string manifestColumnName,
            string timestampColumnName,
            string isDeletedColumnName,
            string tagsColumnName,
            string orderingColumn,
            string serializerIdColumnName,
            TimeSpan timeout,
            StoredAsType storedAs,
            string defaultSerializer,
            JsonSerializerSettings jsonSerializerSettings = null,
            bool useSequentialAccess = true,
            bool useBigIntPrimaryKey = false)
            : this(schemaName, journalEventsTableName, metaTableName, persistenceIdColumnName, sequenceNrColumnName,
                payloadColumnName, manifestColumnName, timestampColumnName, isDeletedColumnName, tagsColumnName,
                orderingColumn, serializerIdColumnName, timeout, storedAs, defaultSerializer, 100, jsonSerializerSettings,
                useSequentialAccess, useBigIntPrimaryKey)
        {
        }
        
        public PostgreSqlQueryConfiguration(
            string schemaName,
            string journalEventsTableName,
            string metaTableName,
            string persistenceIdColumnName,
            string sequenceNrColumnName,
            string payloadColumnName,
            string manifestColumnName,
            string timestampColumnName,
            string isDeletedColumnName,
            string tagsColumnName,
            string orderingColumn,
            string serializerIdColumnName,
            TimeSpan timeout,
            StoredAsType storedAs,
            string defaultSerializer,
            int tagsColumnSize = 2000, 
            JsonSerializerSettings jsonSerializerSettings = null, 
            bool useSequentialAccess = true, 
            bool useBigIntPrimaryKey = false)
            : base(schemaName, journalEventsTableName, metaTableName, persistenceIdColumnName, sequenceNrColumnName,
                  payloadColumnName, manifestColumnName, timestampColumnName, isDeletedColumnName, tagsColumnName, orderingColumn, 
                serializerIdColumnName, timeout, defaultSerializer, useSequentialAccess)
        {
            StoredAs = storedAs;
            UseBigIntPrimaryKey = useBigIntPrimaryKey;
            JsonSerializerSettings = jsonSerializerSettings ?? new JsonSerializerSettings
            {
                ContractResolver = new AkkaContractResolver()
            };
            TagsColumnSize = tagsColumnSize;
        }
    }
}
