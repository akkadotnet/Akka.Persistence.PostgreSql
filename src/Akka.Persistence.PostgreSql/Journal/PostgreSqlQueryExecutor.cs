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

namespace Akka.Persistence.PostgreSql.Journal
{
    using System.Threading;
    using System.Threading.Tasks;

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

            var allEventColumnNames = $@"
                e.{Configuration.PersistenceIdColumnName} as PersistenceId, 
                e.{Configuration.SequenceNrColumnName} as SequenceNr, 
                e.{Configuration.TimestampColumnName} as Timestamp, 
                e.{Configuration.IsDeletedColumnName} as IsDeleted, 
                e.{Configuration.ManifestColumnName} as Manifest, 
                e.{Configuration.PayloadColumnName} as Payload,
                e.{Configuration.SerializerIdColumnName} as SerializerId";

            CreateEventsJournalSql = $@"
                CREATE TABLE IF NOT EXISTS {Configuration.FullJournalTableName} (
                    {Configuration.OrderingColumnName} BIGSERIAL NOT NULL PRIMARY KEY,
                    {Configuration.PersistenceIdColumnName} VARCHAR(255) NOT NULL,
                    {Configuration.SequenceNrColumnName} BIGINT NOT NULL,
                    {Configuration.IsDeletedColumnName} BOOLEAN NOT NULL,
                    {Configuration.TimestampColumnName} BIGINT NOT NULL,
                    {Configuration.ManifestColumnName} VARCHAR(500) NOT NULL,
                    {Configuration.PayloadColumnName} {storedAs} NOT NULL,
                    {Configuration.TagsColumnName} VARCHAR(100)[] NULL,
                    {Configuration.SerializerIdColumnName} INTEGER NULL,
                    CONSTRAINT {Configuration.JournalEventsTableName}_uq UNIQUE ({Configuration.PersistenceIdColumnName}, {Configuration.SequenceNrColumnName})
                );

                CREATE INDEX IF NOT EXISTS idx_{Configuration.FullJournalTableName.Replace('.', '_')}_{Configuration.TagsColumnName}_gin
                    ON {Configuration.FullJournalTableName} USING gin ({Configuration.TagsColumnName});
                ";

            CreateMetaTableSql = $@"
                CREATE TABLE IF NOT EXISTS {Configuration.FullMetaTableName} (
                    {Configuration.PersistenceIdColumnName} VARCHAR(255) NOT NULL,
                    {Configuration.SequenceNrColumnName} BIGINT NOT NULL,
                    CONSTRAINT {Configuration.MetaTableName}_pk PRIMARY KEY ({Configuration.PersistenceIdColumnName}, {Configuration.SequenceNrColumnName})
                );";

            HighestTagOrderingSql =
                $@"
                SELECT MAX(e.{Configuration.OrderingColumnName}) as Ordering
                FROM {Configuration.FullJournalTableName} e
                WHERE e.{Configuration.OrderingColumnName} > @Ordering AND e.{Configuration.TagsColumnName} @> @Tag";

            ByTagSql =
                $@"
                SELECT {allEventColumnNames}, e.{Configuration.OrderingColumnName} as Ordering
                FROM {Configuration.FullJournalTableName} e
                WHERE e.{Configuration.OrderingColumnName} > @Ordering AND e.{Configuration.TagsColumnName} @> @Tag
                ORDER BY {Configuration.OrderingColumnName} ASC";

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
        protected override string HighestTagOrderingSql { get; }
        protected override string ByTagSql { get; }

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

            command.Parameters.Add(tags.Count != 0
                ? new NpgsqlParameter("@Tag", NpgsqlDbType.Array | NpgsqlDbType.Varchar) {Value = tags.ToArray()}
                : new NpgsqlParameter("@Tag", NpgsqlDbType.Array | NpgsqlDbType.Varchar) {Value = DBNull.Value});
        }

                /// <summary>
        /// TBD
        /// </summary>
        /// <param name="connection">TBD</param>
        /// <param name="cancellationToken">TBD</param>
        /// <param name="tag">TBD</param>
        /// <param name="fromOffset">TBD</param>
        /// <param name="toOffset">TBD</param>
        /// <param name="max">TBD</param>
        /// <param name="callback">TBD</param>
        /// <returns>TBD</returns>
        public override async Task<long> SelectByTagAsync(DbConnection connection, CancellationToken cancellationToken, string tag, long fromOffset, long toOffset, long max,
            Action<ReplayedTaggedMessage> callback)
        {
            using (var command = GetCommand(connection, ByTagSql))
            {
                var take = Math.Min(toOffset - fromOffset, max);
                command.Parameters.Add(new NpgsqlParameter("@Tag", NpgsqlDbType.Array | NpgsqlDbType.Varchar) { Value =  new[] { tag }});
                AddParameter(command, "@Ordering", DbType.Int64, fromOffset);
                AddParameter(command, "@Take", DbType.Int64, take);

                CommandBehavior commandBehavior;

                if (Configuration.UseSequentialAccess)
                {
                    commandBehavior = CommandBehavior.SequentialAccess;
                }
                else
                {
                    commandBehavior = CommandBehavior.Default;
                }

                using (var reader = await command.ExecuteReaderAsync(commandBehavior, cancellationToken))
                {
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        var persistent = ReadEvent(reader);
                        var ordering = reader.GetInt64(OrderingIndex);
                        callback(new ReplayedTaggedMessage(persistent, tag, ordering));
                    }
                }
            }

            using (var command = GetCommand(connection, HighestTagOrderingSql))
            {
                command.Parameters.Add(new NpgsqlParameter("@Tag", NpgsqlDbType.Array | NpgsqlDbType.Varchar) { Value = new[] { tag } });
                AddParameter(command, "@Ordering", DbType.Int64, fromOffset);
                var maxOrdering = (await command.ExecuteScalarAsync(cancellationToken)) as long? ?? 0L;
                return maxOrdering;
            }
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