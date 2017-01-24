using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Configuration;
using System.Data;
using System.Data.Common;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Sql.Common.Journal;
using Akka.Serialization;
using Akka.Util;
using Newtonsoft.Json;
using Npgsql;
using NpgsqlTypes;
using ConfigurationException = Akka.Configuration.ConfigurationException;

namespace Akka.Persistence.PostgreSql.Journal
{
    public sealed class BatchingPostgresJournalSetup : BatchingSqlJournalSetup
    {
        public readonly StoredAsType StoredAs;
        public readonly JsonSerializerSettings JsonSerializerSettings;

        public static BatchingPostgresJournalSetup Create(Config config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config), "Postgres journal settings cannot be initialized, because required HOCON section couldn't been found");

            var connectionString = config.GetString("connection-string");
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                connectionString = ConfigurationManager
                    .ConnectionStrings[config.GetString("connection-string-name", "DefaultConnection")]
                    .ConnectionString;
            }

            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentException("No connection string for Sql Event Journal was specified");

            StoredAsType storedAs;
            var storedAsString = config.GetString("stored-as", "JsonB");
            if (!Enum.TryParse(storedAsString, true, out storedAs))
            {
                throw new ConfigurationException($"Value [{storedAsString}] of the 'stored-as' HOCON config key is not valid. Valid values: bytea, json, jsonb.");
            }

            IsolationLevel level;
            switch (config.GetString("isolation-level", "unspecified"))
            {
                case "chaos": level = IsolationLevel.Chaos; break;
                case "read-committed": level = IsolationLevel.ReadCommitted; break;
                case "read-uncommitted": level = IsolationLevel.ReadUncommitted; break;
                case "repeatable-read": level = IsolationLevel.RepeatableRead; break;
                case "serializable": level = IsolationLevel.Serializable; break;
                case "snapshot": level = IsolationLevel.Snapshot; break;
                case "unspecified": level = IsolationLevel.Unspecified; break;
                default: throw new ArgumentException("Unknown isolation-level value. Should be one of: chaos | read-committed | read-uncommitted | repeatable-read | serializable | snapshot | unspecified");
            }

            return new BatchingPostgresJournalSetup(
                connectionString: connectionString,
                maxConcurrentOperations: config.GetInt("max-concurrent-operations", 64),
                maxBatchSize: config.GetInt("max-batch-size", 100),
                maxBufferSize: config.GetInt("max-buffer-size", 500000),
                autoInitialize: config.GetBoolean("auto-initialize", false),
                connectionTimeout: config.GetTimeSpan("connection-timeout", TimeSpan.FromSeconds(30)),
                isolationLevel: level,
                circuitBreakerSettings: new CircuitBreakerSettings(config.GetConfig("circuit-breaker")),
                replayFilterSettings: new ReplayFilterSettings(config.GetConfig("replay-filter")), 
                namingConventions: new QueryConfiguration(
                    schemaName: config.GetString("schema-name", "public"),
                    journalEventsTableName: config.GetString("table-name", "event_journal"),
                    metaTableName: config.GetString("metadata-table-name", "metadata"),
                    persistenceIdColumnName: "persistence_id",
                    sequenceNrColumnName: "sequence_nr",
                    payloadColumnName: "payload",
                    manifestColumnName: "manifest",
                    timestampColumnName: "created_at",
                    isDeletedColumnName: "is_deleted",
                    tagsColumnName: "tags",
                    orderingColumnName: "ordering",
                    timeout: config.GetTimeSpan("connection-timeout", TimeSpan.FromSeconds(30))),
                storedAs: storedAs,
                jsonSerializerSettings: new JsonSerializerSettings
                {
                    ContractResolver = new NewtonSoftJsonSerializer.AkkaContractResolver()
                });
        }

        public BatchingPostgresJournalSetup(string connectionString, int maxConcurrentOperations, int maxBatchSize, int maxBufferSize, bool autoInitialize, TimeSpan connectionTimeout, IsolationLevel isolationLevel, CircuitBreakerSettings circuitBreakerSettings, ReplayFilterSettings replayFilterSettings, QueryConfiguration namingConventions, StoredAsType storedAs, JsonSerializerSettings jsonSerializerSettings) 
            : base(connectionString, maxConcurrentOperations, maxBatchSize, maxBufferSize, autoInitialize, connectionTimeout, isolationLevel, circuitBreakerSettings, replayFilterSettings, namingConventions)
        {
            StoredAs = storedAs;
            JsonSerializerSettings = jsonSerializerSettings;
        }
    }

    public class BatchingPostgreSqlJournal : BatchingSqlJournal<NpgsqlConnection, NpgsqlCommand>
    {
        private readonly Func<IPersistentRepresentation, KeyValuePair<NpgsqlDbType, object>> serialize;
        private readonly Func<Type, object, object> deserialize;

        public BatchingPostgreSqlJournal(Config config) : this(BatchingPostgresJournalSetup.Create(config))
        {
        }

        public BatchingPostgreSqlJournal(BatchingPostgresJournalSetup setup) : base(setup)
        {
            var conventions = Setup.NamingConventions;
            Initializers = ImmutableDictionary.CreateRange(new[]
            {
                new KeyValuePair<string, string>("CreateJournalSql", $@"
                CREATE TABLE IF NOT EXISTS {conventions.FullJournalTableName} (
                    {conventions.OrderingColumnName} BIGSERIAL NOT NULL PRIMARY KEY,
                    {conventions.PersistenceIdColumnName} VARCHAR(255) NOT NULL,
                    {conventions.SequenceNrColumnName} BIGINT NOT NULL,
                    {conventions.IsDeletedColumnName} BOOLEAN NOT NULL,
                    {conventions.TimestampColumnName} BIGINT NOT NULL,
                    {conventions.ManifestColumnName} VARCHAR(500) NOT NULL,
                    {conventions.PayloadColumnName} {setup.StoredAs} NOT NULL,
                    {conventions.TagsColumnName} VARCHAR(100) NULL,
                    CONSTRAINT {conventions.JournalEventsTableName}_uq UNIQUE ({conventions.PersistenceIdColumnName}, {conventions.SequenceNrColumnName})
                );"),
                new KeyValuePair<string, string>("CreateMetadataSql", $@"
                CREATE TABLE IF NOT EXISTS {conventions.FullMetaTableName} (
                    {conventions.PersistenceIdColumnName} VARCHAR(255) NOT NULL,
                    {conventions.SequenceNrColumnName} BIGINT NOT NULL,
                    CONSTRAINT {conventions.MetaTableName}_pk PRIMARY KEY ({conventions.PersistenceIdColumnName}, {conventions.SequenceNrColumnName})
                );"),
            });

            switch (setup.StoredAs)
            {
                case StoredAsType.ByteA:
                    var serialization = Context.System.Serialization;
                    serialize = e => new KeyValuePair<NpgsqlDbType, object>(NpgsqlDbType.Bytea, serialization.FindSerializerFor(e.Payload).ToBinary(e.Payload));
                    deserialize = (type, serialized) => serialization.FindSerializerForType(type).FromBinary((byte[])serialized, type);
                    break;
                case StoredAsType.JsonB:
                    serialize = e => new KeyValuePair<NpgsqlDbType, object>(NpgsqlDbType.Jsonb, JsonConvert.SerializeObject(e.Payload, setup.JsonSerializerSettings));
                    deserialize = (type, serialized) => JsonConvert.DeserializeObject((string)serialized, type, setup.JsonSerializerSettings);
                    break;
                case StoredAsType.Json:
                    serialize = e => new KeyValuePair<NpgsqlDbType, object>(NpgsqlDbType.Json, JsonConvert.SerializeObject(e.Payload, setup.JsonSerializerSettings));
                    deserialize = (type, serialized) => JsonConvert.DeserializeObject((string)serialized, type, setup.JsonSerializerSettings);
                    break;
                default:
                    throw new NotSupportedException($"{setup.StoredAs} is not supported Db type for a payload");
            }
        }

        protected override NpgsqlConnection CreateConnection(string connectionString) =>
            new NpgsqlConnection(connectionString);

        protected override ImmutableDictionary<string, string> Initializers { get; }

        protected override void WriteEvent(NpgsqlCommand command, IPersistentRepresentation persistent, string tags = "")
        {
            var payloadType = persistent.Payload.GetType();
            var manifest = string.IsNullOrEmpty(persistent.Manifest)
                ? payloadType.TypeQualifiedName()
                : persistent.Manifest;
            var t = serialize(persistent);

            AddParameter(command, "@PersistenceId", DbType.String, persistent.PersistenceId);
            AddParameter(command, "@SequenceNr", DbType.Int64, persistent.SequenceNr);
            AddParameter(command, "@Timestamp", DbType.Int64, 0L);
            AddParameter(command, "@IsDeleted", DbType.Boolean, false);
            AddParameter(command, "@Manifest", DbType.String, manifest);
            command.Parameters.Add(new NpgsqlParameter("@Payload", t.Key) { Value = t.Value });
            AddParameter(command, "@Tag", DbType.String, tags);
        }

        protected override IPersistentRepresentation ReadEvent(DbDataReader reader)
        {
            var persistenceId = reader.GetString(PersistenceIdIndex);
            var sequenceNr = reader.GetInt64(SequenceNrIndex);
            var isDeleted = reader.GetBoolean(IsDeletedIndex);
            var manifest = reader.GetString(ManifestIndex);
            var payload = reader[PayloadIndex];

            var type = Type.GetType(manifest, true);
            var deserialized = deserialize(type, payload);

            var persistent = new Persistent(deserialized, sequenceNr, persistenceId, manifest, isDeleted, ActorRefs.NoSender, null);
            return persistent;
        }
    }
}