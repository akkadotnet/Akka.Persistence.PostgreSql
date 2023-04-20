//-----------------------------------------------------------------------
// <copyright file="PostgreSqlQueryExecutor.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Persistence.Sql.Common.Snapshot;
using Akka.Serialization;
using Akka.Util;
using Newtonsoft.Json;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;

namespace Akka.Persistence.PostgreSql.Snapshot
{
    public class PostgreSqlQueryExecutor : AbstractQueryExecutor
    {
        private readonly Func<object, SerializationResult> _serialize;
        private readonly Func<Type, object, string, int?, object> _deserialize;
        public PostgreSqlQueryExecutor(PostgreSqlQueryConfiguration configuration, Akka.Serialization.Serialization serialization) : base(configuration, serialization)
        {
            CreateSnapshotTableSql = $@"
                DO
                $do$
                BEGIN
                IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{Configuration.SchemaName}' AND TABLE_NAME = '{Configuration.SnapshotTableName}') THEN
                    CREATE TABLE {Configuration.FullSnapshotTableName} (
                        {Configuration.PersistenceIdColumnName} VARCHAR(255) NOT NULL,
                        {Configuration.SequenceNrColumnName} BIGINT NOT NULL,
                        {Configuration.TimestampColumnName} BIGINT NOT NULL,
                        {Configuration.ManifestColumnName} VARCHAR(500) NOT NULL,
                        {Configuration.PayloadColumnName} {configuration.StoredAs.ToString().ToUpperInvariant()} NOT NULL,
                        {Configuration.SerializerIdColumnName} INTEGER NULL,
                        CONSTRAINT {Configuration.SnapshotTableName}_pk PRIMARY KEY ({Configuration.PersistenceIdColumnName}, {Configuration.SequenceNrColumnName})
                    );
                    CREATE INDEX {Configuration.SnapshotTableName}_{Configuration.SequenceNrColumnName}_idx ON {Configuration.FullSnapshotTableName}({Configuration.SequenceNrColumnName});
                    CREATE INDEX {Configuration.SnapshotTableName}_{Configuration.TimestampColumnName}_idx ON {Configuration.FullSnapshotTableName}({Configuration.TimestampColumnName});
                END IF;
                END
                $do$";

            InsertSnapshotSql = $@"
                WITH upsert AS (
                    UPDATE {Configuration.FullSnapshotTableName} 
                    SET 
                        {Configuration.TimestampColumnName} = @Timestamp, 
                        {Configuration.PayloadColumnName} = @Payload 
                    WHERE {Configuration.PersistenceIdColumnName} = @PersistenceId
                    AND {Configuration.SequenceNrColumnName} = @SequenceNr 
                    RETURNING *) 
                INSERT INTO {Configuration.FullSnapshotTableName} (
                    {Configuration.PersistenceIdColumnName}, 
                    {Configuration.SequenceNrColumnName}, 
                    {Configuration.TimestampColumnName}, 
                    {Configuration.ManifestColumnName}, 
                    {Configuration.PayloadColumnName},
                    {Configuration.SerializerIdColumnName})
                SELECT @PersistenceId, @SequenceNr, @Timestamp, @Manifest, @Payload, @SerializerId
                WHERE NOT EXISTS (SELECT * FROM upsert)";

            switch (configuration.StoredAs)
            {
                case StoredAsType.ByteA:
                    _serialize = ss =>
                    {
                        var serializer = Serialization.FindSerializerFor(ss);
                        // TODO: hack. Replace when https://github.com/akkadotnet/akka.net/issues/3811
                        var binary = Akka.Serialization.Serialization
                            .WithTransport(Serialization.System, () => serializer.ToBinary(ss));
                        return new SerializationResult(NpgsqlDbType.Bytea, binary, serializer);
                    };
                    _deserialize = (type, serialized, manifest, serializerId) =>
                    {
                        if (serializerId.HasValue)
                        {
                            return Serialization.Deserialize((byte[])serialized, serializerId.Value, manifest);
                        }
                        else
                        {
                            // Support old writes that did not set the serializer id
                            var deserializer = Serialization.FindSerializerForType(type, Configuration.DefaultSerializer);
                            return deserializer.FromBinary((byte[])serialized, type);
                        }
                    };
                    break;
                case StoredAsType.JsonB:
                    _serialize = ss => new SerializationResult(NpgsqlDbType.Jsonb, JsonConvert.SerializeObject(ss, configuration.JsonSerializerSettings), null);
                    _deserialize = (type, serialized, manifest, serializerId) => JsonConvert.DeserializeObject((string)serialized, type, configuration.JsonSerializerSettings);
                    break;
                case StoredAsType.Json:
                    _serialize = ss => new SerializationResult(NpgsqlDbType.Json, JsonConvert.SerializeObject(ss, configuration.JsonSerializerSettings), null);
                    _deserialize = (type, serialized, manifest, serializerId) => JsonConvert.DeserializeObject((string)serialized, type, configuration.JsonSerializerSettings);
                    break;
                default:
                    throw new NotSupportedException($"{configuration.StoredAs} is not supported Db type for a payload");
            }
        }

        protected override string InsertSnapshotSql { get; }

        protected override DbCommand CreateCommand(DbConnection connection)
        {
            return ((NpgsqlConnection)connection).CreateCommand();
        }

        protected override void SetTimestampParameter(DateTime timestamp, DbCommand command) => AddParameter(command, "@Timestamp", DbType.Int64, timestamp.Ticks);
        protected override void SetPayloadParameter(object snapshot, DbCommand command)
        {
            var serializationResult = _serialize(snapshot);
            command.Parameters.Add(new NpgsqlParameter("@Payload", serializationResult.DbType) { Value = serializationResult.Payload });
        }

        protected override void SetManifestParameters(object snapshot, DbCommand command)
        {
            var serializationResult = _serialize(snapshot);
            var serializer = serializationResult.Serializer;
            var hasSerializer = serializer != null;

            string manifest = "";
            if (hasSerializer && serializer is SerializerWithStringManifest)
                manifest = ((SerializerWithStringManifest)serializer).Manifest(serializationResult.Payload);
            else if (hasSerializer && serializer.IncludeManifest)
                manifest = QualifiedName(serializationResult.Payload);
            else
                manifest = QualifiedName(snapshot);

            AddParameter(command, "@Manifest", DbType.String, manifest);

            if (hasSerializer)
            {
                AddParameter(command, "@SerializerId", DbType.Int32, serializer.Identifier);
            }
            else
            {
                AddParameter(command, "@SerializerId", DbType.Int32, DBNull.Value);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static string QualifiedName(object payload)
            => payload.GetType().TypeQualifiedName();

        protected override SelectedSnapshot ReadSnapshot(DbDataReader reader)
        {
            var persistenceId = reader.GetString(0);
            var sequenceNr = reader.GetInt64(1);
            var timestamp = new DateTime(reader.GetInt64(2));
            var manifest = reader.GetString(3);
            var payloadObject = reader[4];

            int? serializerId = null;
            Type type = null;
            if (reader.IsDBNull(5))
            {
                type = Type.GetType(manifest, true);
            }
            else
            {
                type = Type.GetType(manifest, false);
                serializerId = reader.GetInt32(5);
            }

            var snapshot = _deserialize(type, payloadObject, manifest, serializerId);

            var metadata = new SnapshotMetadata(persistenceId, sequenceNr, timestamp);
            return new SelectedSnapshot(metadata, snapshot);
        }

        protected override string CreateSnapshotTableSql { get; }
    }

    [Serializable]
    public class PostgreSqlQueryConfiguration : QueryConfiguration
    {
        public readonly StoredAsType StoredAs;
        public readonly JsonSerializerSettings JsonSerializerSettings;

        public PostgreSqlQueryConfiguration(
            string schemaName,
            string snapshotTableName,
            string persistenceIdColumnName,
            string sequenceNrColumnName,
            string payloadColumnName,
            string manifestColumnName,
            string timestampColumnName,
            string serializerIdColumnName,
            TimeSpan timeout,
            StoredAsType storedAs,
            string defaultSerializer,
            JsonSerializerSettings jsonSerializerSettings = null,
            bool useSequentialAccess = true)
            : base(schemaName, snapshotTableName, persistenceIdColumnName, sequenceNrColumnName, payloadColumnName, 
                manifestColumnName, timestampColumnName, serializerIdColumnName, timeout, defaultSerializer, useSequentialAccess)
        {
            StoredAs = storedAs;
            JsonSerializerSettings = jsonSerializerSettings ?? new JsonSerializerSettings
            {
                ContractResolver = new AkkaContractResolver()
            };
        }
    }
}