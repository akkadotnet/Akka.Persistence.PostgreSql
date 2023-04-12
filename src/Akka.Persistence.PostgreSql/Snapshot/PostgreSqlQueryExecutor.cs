//-----------------------------------------------------------------------
// <copyright file="PostgreSqlQueryExecutor.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Persistence.Sql.Common.Snapshot;
using Akka.Serialization;
using Akka.Util;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Data;
using System.Data.Common;
using System.Text;

namespace Akka.Persistence.PostgreSql.Snapshot
{
    public class PostgreSqlQueryExecutor : AbstractQueryExecutor
    {
        private readonly NpgsqlDbType _payloadDbType;
        private readonly NewtonSoftJsonSerializer _jsonSerializer;
        private readonly PostgreSqlQueryConfiguration _config;
        
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

            _jsonSerializer = new NewtonSoftJsonSerializer(Serialization.System);
            _config = configuration;
            _payloadDbType = _config.StoredAs switch
            {
                StoredAsType.ByteA => NpgsqlDbType.Bytea,
                StoredAsType.JsonB => NpgsqlDbType.Jsonb,
                StoredAsType.Json => NpgsqlDbType.Json,
                _ => throw new NotSupportedException($"{_config.StoredAs} is not supported Db type for a payload")
            };
        }

        protected override string InsertSnapshotSql { get; }

        protected override DbCommand CreateCommand(DbConnection connection)
        {
            return ((NpgsqlConnection)connection).CreateCommand();
        }

        protected override void SetTimestampParameter(DateTime timestamp, DbCommand command) => AddParameter(command, "@Timestamp", DbType.Int64, timestamp.Ticks);
        protected override void SetPayloadParameter(object snapshot, DbCommand command)
        {
            var serializer = GetSerializerFor(snapshot);
            // TODO: hack. Replace when https://github.com/akkadotnet/akka.net/issues/3811
            object payload = _payloadDbType switch
            {
                NpgsqlDbType.Bytea => Akka.Serialization.Serialization.WithTransport(Serialization.System, () => serializer.ToBinary(snapshot)),
                _ => Akka.Serialization.Serialization.WithTransport(Serialization.System, () =>
                {
                    var bytes = _jsonSerializer.ToBinary(snapshot);
                    return Encoding.UTF8.GetString(bytes);
                })
            };
            command.Parameters.Add(new NpgsqlParameter("@Payload", _payloadDbType) { Value = payload });
        }

        protected override void SetManifestParameters(object snapshot, DbCommand command)
        {
            var serializer = GetSerializerFor(snapshot);
            var manifest = serializer switch
            {
                SerializerWithStringManifest stringManifest => stringManifest.Manifest(snapshot),
                _ => snapshot.GetType().TypeQualifiedName(),
            };
            AddParameter(command, "@Manifest", DbType.String, manifest);
            AddParameter(command, "@SerializerId", DbType.Int32, serializer.Identifier);
        }

        private Serializer GetSerializerFor(object snapshot)
        {
            return _payloadDbType switch
            {
                NpgsqlDbType.Bytea => Serialization.FindSerializerFor(snapshot, _config.DefaultSerializer),
                NpgsqlDbType.Jsonb => _jsonSerializer,
                NpgsqlDbType.Json => _jsonSerializer,
                _ => throw new NotSupportedException($"{_payloadDbType} is not supported Db type for a payload")
            };
        }
        
        protected override SelectedSnapshot ReadSnapshot(DbDataReader reader)
        {
            var persistenceId = reader.GetString(0);
            var sequenceNr = reader.GetInt64(1);
            var timestamp = new DateTime(reader.GetInt64(2));
            var manifest = reader.GetString(3);
            var payloadObject = reader[4];

            int? serializerId;
            Type type;
            if (reader.IsDBNull(5))
            {
                type = Type.GetType(manifest, true);
                serializerId = null;
            }
            else
            {
                type = Type.GetType(manifest, false);
                serializerId = reader.GetInt32(5);
            }

            object snapshot;
            if (serializerId is { })
            {
                if (_payloadDbType == NpgsqlDbType.Bytea)
                {
                    snapshot = Serialization.Deserialize((byte[])payloadObject, serializerId.Value, manifest);
                }
                else
                {
                    var bytes = Encoding.UTF8.GetBytes((string)payloadObject);
                    snapshot = _jsonSerializer.FromBinary(bytes, type);
                }
            }
            else
            {
                // Support old writes that did not set the serializer id
                var deserializer = Serialization.FindSerializerForType(type, Configuration.DefaultSerializer);
                snapshot = deserializer.FromBinary((byte[])payloadObject, type);
            }

            var metadata = new SnapshotMetadata(persistenceId, sequenceNr, timestamp);
            return new SelectedSnapshot(metadata, snapshot);
        }

        protected override string CreateSnapshotTableSql { get; }
    }

    [Serializable]
    public class PostgreSqlQueryConfiguration : QueryConfiguration
    {
        public readonly StoredAsType StoredAs;

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
            bool useSequentialAccess = true)
            : base(schemaName, snapshotTableName, persistenceIdColumnName, sequenceNrColumnName, payloadColumnName, 
                manifestColumnName, timestampColumnName, serializerIdColumnName, timeout, defaultSerializer, useSequentialAccess)
        {
            StoredAs = storedAs;
        }
    }
}