//-----------------------------------------------------------------------
// <copyright file="PostgreSqlQueryExecutor.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Actor;
using Akka.Persistence.Sql.Common.Journal;
using Akka.Util;
using Newtonsoft.Json;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace Akka.Persistence.PostgreSql.Journal
{
    public class PostgreSqlQueryExecutor : AbstractQueryExecutor
    {
        private readonly PostgreSqlQueryConfiguration _configuration;
        private readonly Func<IPersistentRepresentation, KeyValuePair<NpgsqlDbType, object>> _serialize;
        private readonly Func<Type, object, object> _deserialize;

        public PostgreSqlQueryExecutor(PostgreSqlQueryConfiguration configuration, Akka.Serialization.Serialization serialization, ITimestampProvider timestampProvider)
            : base(configuration, serialization, timestampProvider)
        {
            _configuration = configuration;
            var storedAs = configuration.StoredAs.ToString().ToUpperInvariant();
            
            CreateEventsJournalSql = $@"
                CREATE TABLE IF NOT EXISTS {Configuration.FullJournalTableName} (
                    {Configuration.OrderingColumnName} BIGSERIAL NOT NULL PRIMARY KEY,
                    {Configuration.PersistenceIdColumnName} VARCHAR(255) NOT NULL,
                    {Configuration.SequenceNrColumnName} BIGINT NOT NULL,
                    {Configuration.IsDeletedColumnName} BOOLEAN NOT NULL,
                    {Configuration.TimestampColumnName} BIGINT NOT NULL,
                    {Configuration.ManifestColumnName} VARCHAR(500) NOT NULL,
                    {Configuration.PayloadColumnName} {storedAs} NOT NULL,
                    {Configuration.TagsColumnName} VARCHAR(100) NULL,
                    CONSTRAINT {Configuration.JournalEventsTableName}_uq UNIQUE ({Configuration.PersistenceIdColumnName}, {Configuration.SequenceNrColumnName})
                );
                ";

            CreateMetaTableSql = $@"
                CREATE TABLE IF NOT EXISTS {Configuration.FullMetaTableName} (
                    {Configuration.PersistenceIdColumnName} VARCHAR(255) NOT NULL,
                    {Configuration.SequenceNrColumnName} BIGINT NOT NULL,
                    CONSTRAINT {Configuration.MetaTableName}_pk PRIMARY KEY ({Configuration.PersistenceIdColumnName}, {Configuration.SequenceNrColumnName})
                );";

            switch (_configuration.StoredAs)
            {
                case StoredAsType.ByteA:
                    _serialize = e => new KeyValuePair<NpgsqlDbType, object>(NpgsqlDbType.Bytea, Serialization.FindSerializerFor(e.Payload).ToBinary(e.Payload));
                    _deserialize = (type, serialized) => Serialization.FindSerializerForType(type).FromBinary((byte[])serialized, type);
                    break;
                case StoredAsType.JsonB:
                    _serialize = e => new KeyValuePair<NpgsqlDbType, object>(NpgsqlDbType.Jsonb, JsonConvert.SerializeObject(e.Payload, _configuration.JsonSerializerSettings));
                    _deserialize = (type, serialized) => JsonConvert.DeserializeObject((string)serialized, type, _configuration.JsonSerializerSettings);
                    break;
                case StoredAsType.Json:
                    _serialize = e => new KeyValuePair<NpgsqlDbType, object>(NpgsqlDbType.Json, JsonConvert.SerializeObject(e.Payload, _configuration.JsonSerializerSettings));
                    _deserialize = (type, serialized) => JsonConvert.DeserializeObject((string)serialized, type, _configuration.JsonSerializerSettings);
                    break;
                default:
                    throw new NotSupportedException($"{_configuration.StoredAs} is not supported Db type for a payload");
            }
        }

        protected override DbCommand CreateCommand(DbConnection connection) => ((NpgsqlConnection)connection).CreateCommand();
        protected override string CreateEventsJournalSql { get; }
        protected override string CreateMetaTableSql { get; }

        protected override void WriteEvent(DbCommand command, IPersistentRepresentation e, IImmutableSet<string> tags)
        {
            var manifest = string.IsNullOrEmpty(e.Manifest) ? QualifiedName(e) : e.Manifest;
            var t = _serialize(e);

            AddParameter(command, "@PersistenceId", DbType.String, e.PersistenceId);
            AddParameter(command, "@SequenceNr", DbType.Int64, e.SequenceNr);
            AddParameter(command, "@Timestamp", DbType.Int64, TimestampProvider.GenerateTimestamp(e));
            AddParameter(command, "@IsDeleted", DbType.Boolean, false);
            AddParameter(command, "@Manifest", DbType.String, manifest);
            command.Parameters.Add(new NpgsqlParameter("@Payload", t.Key) { Value = t.Value });

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

        private static string QualifiedName(IPersistentRepresentation e)
        {
            var type = e.Payload.GetType();
            return type.TypeQualifiedName();
        }

        protected override IPersistentRepresentation ReadEvent(DbDataReader reader)
        {
            var persistenceId = reader.GetString(PersistenceIdIndex);
            var sequenceNr = reader.GetInt64(SequenceNrIndex);
            //var timestamp = reader.GetDateTime(TimestampIndex);
            var isDeleted = reader.GetBoolean(IsDeletedIndex);
            var manifest = reader.GetString(ManifestIndex);
            var raw = reader[PayloadIndex];
            var type = Type.GetType(manifest, true);

            var deserialized = _deserialize(type, raw);

            return new Persistent(deserialized, sequenceNr, persistenceId, manifest, isDeleted, ActorRefs.NoSender, null);
        }
    }
    
    public class PostgreSqlQueryConfiguration : QueryConfiguration
    {
        public readonly StoredAsType StoredAs;
        public readonly JsonSerializerSettings JsonSerializerSettings;

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
            TimeSpan timeout,
            StoredAsType storedAs,
            string defaultSerializer,
            JsonSerializerSettings jsonSerializerSettings = null)
            : base(schemaName, journalEventsTableName, metaTableName, persistenceIdColumnName, sequenceNrColumnName,
                  payloadColumnName, manifestColumnName, timestampColumnName, isDeletedColumnName, tagsColumnName, orderingColumn, timeout, defaultSerializer)
        {
            StoredAs = storedAs;
            JsonSerializerSettings = jsonSerializerSettings ?? new JsonSerializerSettings
            {
                ContractResolver = new AkkaContractResolver()
            };
        }
    }
}