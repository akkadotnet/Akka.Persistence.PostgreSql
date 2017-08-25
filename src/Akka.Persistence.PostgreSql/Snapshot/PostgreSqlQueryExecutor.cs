//-----------------------------------------------------------------------
// <copyright file="PostgreSqlQueryExecutor.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Persistence.Sql.Common.Snapshot;
using Newtonsoft.Json;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace Akka.Persistence.PostgreSql.Snapshot
{
    public class PostgreSqlQueryExecutor : AbstractQueryExecutor
    {
        private readonly Func<object, KeyValuePair<NpgsqlDbType, object>> _serialize;
        private readonly Func<Type, object, object> _deserialize;
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
                    {Configuration.PayloadColumnName})
                SELECT @PersistenceId, @SequenceNr, @Timestamp, @Manifest, @Payload
                WHERE NOT EXISTS (SELECT * FROM upsert)";

            switch (configuration.StoredAs)
            {
                case StoredAsType.ByteA:
                    _serialize = e => new KeyValuePair<NpgsqlDbType, object>(NpgsqlDbType.Bytea, serialization.FindSerializerFor(e).ToBinary(e));
                    _deserialize = (type, serialized) => serialization.FindSerializerForType(type).FromBinary((byte[])serialized, type);
                    break;
                case StoredAsType.JsonB:
                    _serialize = e => new KeyValuePair<NpgsqlDbType, object>(NpgsqlDbType.Jsonb, JsonConvert.SerializeObject(e, configuration.JsonSerializerSettings));
                    _deserialize = (type, serialized) => JsonConvert.DeserializeObject((string)serialized, type, configuration.JsonSerializerSettings);
                    break;
                case StoredAsType.Json:
                    _serialize = e => new KeyValuePair<NpgsqlDbType, object>(NpgsqlDbType.Json, JsonConvert.SerializeObject(e, configuration.JsonSerializerSettings));
                    _deserialize = (type, serialized) => JsonConvert.DeserializeObject((string)serialized, type, configuration.JsonSerializerSettings);
                    break;
                default:
                    throw new NotSupportedException($"{configuration.StoredAs} is not supported Db type for a payload");
            }
        }

        protected override string InsertSnapshotSql { get; }

        protected override DbCommand CreateCommand(DbConnection connection)
        {
            return ((NpgsqlConnection) connection).CreateCommand();
        }

        protected override void SetTimestampParameter(DateTime timestamp, DbCommand command) => AddParameter(command, "@Timestamp", DbType.Int64, timestamp.Ticks);
        protected override void SetPayloadParameter(object snapshot, DbCommand command)
        {
            var t = _serialize(snapshot);
            command.Parameters.Add(new NpgsqlParameter("@Payload", t.Key) { Value = t.Value });
        }

        protected override SelectedSnapshot ReadSnapshot(DbDataReader reader)
        {
            var persistenceId = reader.GetString(0);
            var sequenceNr = reader.GetInt64(1);
            var timestamp = new DateTime(reader.GetInt64(2));
            var type = Type.GetType(reader.GetString(3), true);
            var snapshot = _deserialize(type, reader[4]);
            
            return new SelectedSnapshot(new SnapshotMetadata(persistenceId, sequenceNr, timestamp), snapshot);
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
            TimeSpan timeout, 
            StoredAsType storedAs,
            string defaultSerializer,
            JsonSerializerSettings jsonSerializerSettings = null) 
            : base(schemaName, snapshotTableName, persistenceIdColumnName, sequenceNrColumnName, payloadColumnName, manifestColumnName, timestampColumnName, timeout, defaultSerializer)
        {
            StoredAs = storedAs;
            JsonSerializerSettings = jsonSerializerSettings ?? new JsonSerializerSettings
            {
                ContractResolver = new AkkaContractResolver()
            };
        }
    }
}