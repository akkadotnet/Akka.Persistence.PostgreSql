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
using System.Text;

namespace Akka.Persistence.PostgreSql.Journal
{
    public class PostgreSqlQueryExecutor : AbstractQueryExecutor
    {
        private readonly PostgreSqlQueryConfiguration _configuration;
        private readonly Func<IPersistentRepresentation, SerializationResult> _serialize;
        private readonly Func<Type, object, string, int?, object> _deserialize;

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
                    {Configuration.SerializerIdColumnName} INTEGER NULL,
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
                    _serialize = e =>
                    {
                        var serializer = Serialization.FindSerializerFor(e.Payload);
                        return new SerializationResult(NpgsqlDbType.Bytea, serializer.ToBinary(e.Payload), serializer);
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
                    _serialize = e => new SerializationResult(NpgsqlDbType.Jsonb, JsonConvert.SerializeObject(e.Payload, _configuration.JsonSerializerSettings), null);
                    _deserialize = (type, serialized, manifest, serializerId) => JsonConvert.DeserializeObject((string)serialized, type, _configuration.JsonSerializerSettings);
                    break;
                case StoredAsType.Json:
                    _serialize = e => new SerializationResult(NpgsqlDbType.Json, JsonConvert.SerializeObject(e.Payload, _configuration.JsonSerializerSettings), null);
                    _deserialize = (type, serialized, manifest, serializerId) => JsonConvert.DeserializeObject((string)serialized, type, _configuration.JsonSerializerSettings);
                    break;
                default:
                    throw new NotSupportedException($"{_configuration.StoredAs} is not supported Db type for a payload");
            }
        }

        protected override DbCommand CreateCommand(DbConnection connection) => ((NpgsqlConnection)connection).CreateCommand();
        protected override string CreateEventsJournalSql { get; }
        protected override string CreateMetaTableSql { get; }
        // Fetching all distinct persistence IDs takes a long time and is not always necessary
        protected override string AllPersistenceIdsSql => $@"";

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
            string serializerIdColumnName,
            TimeSpan timeout,
            StoredAsType storedAs,
            string defaultSerializer,
            JsonSerializerSettings jsonSerializerSettings = null, 
            bool useSequentialAccess = true)
            : base(schemaName, journalEventsTableName, metaTableName, persistenceIdColumnName, sequenceNrColumnName,
                  payloadColumnName, manifestColumnName, timestampColumnName, isDeletedColumnName, tagsColumnName, orderingColumn, 
                serializerIdColumnName, timeout, defaultSerializer, useSequentialAccess)
        {
            StoredAs = storedAs;
            JsonSerializerSettings = jsonSerializerSettings ?? new JsonSerializerSettings
            {
                ContractResolver = new AkkaContractResolver()
            };
        }
    }
}