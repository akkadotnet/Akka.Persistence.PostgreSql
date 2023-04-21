//-----------------------------------------------------------------------
// <copyright file="PostgreSqlJournal.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Configuration;
using Akka.Persistence.Sql.Common.Journal;
using Npgsql;
using System;
using System.Data.Common;
using System.Runtime.CompilerServices;
using Akka.Annotations;
using Akka.Persistence.Sql.Common;

namespace Akka.Persistence.PostgreSql.Journal
{
    /// <summary>
    /// Persistent journal actor using PostgreSQL as persistence layer. It processes write requests
    /// one by one in synchronous manner, while reading results asynchronously.
    /// </summary>

    public class PostgreSqlJournal : SqlJournal
    {
        public readonly PostgreSqlPersistence Extension = PostgreSqlPersistence.Get(Context.System);
        public PostgreSqlJournalSettings JournalSettings { get; }
        public PostgreSqlJournal(Config journalConfig) : base(journalConfig)
        {
            var config = journalConfig.WithFallback(Extension.DefaultJournalConfig);

            QueryExecutor = new PostgreSqlQueryExecutor(
                CreateQueryConfiguration(config, Settings),
                    Context.System.Serialization,
                    GetTimestampProvider(config.GetString("timestamp-provider")));

            JournalSettings = new PostgreSqlJournalSettings(config);
        }

        [InternalApi]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static PostgreSqlQueryConfiguration CreateQueryConfiguration(Config config, JournalSettings settings)
        {
            return new PostgreSqlQueryConfiguration(
                schemaName: config.GetString("schema-name"),
                journalEventsTableName: config.GetString("table-name"),
                metaTableName: config.GetString("metadata-table-name"),
                persistenceIdColumnName: "persistence_id",
                sequenceNrColumnName: "sequence_nr",
                payloadColumnName: "payload",
                manifestColumnName: "manifest",
                timestampColumnName: "created_at",
                isDeletedColumnName: "is_deleted",
                tagsColumnName: "tags",
                orderingColumn: "ordering",
                serializerIdColumnName: "serializer_id",
                timeout: config.GetTimeSpan("connection-timeout"),
                storedAs: config.GetStoredAsType("stored-as"),
                defaultSerializer: config.GetString("serializer"),
                readIsolationLevel: settings.ReadIsolationLevel,
                writeIsolationLevel: settings.WriteIsolationLevel,
                useSequentialAccess: config.GetBoolean("sequential-access"),
                useBigIntPrimaryKey: config.GetBoolean("use-bigint-identity-for-ordering-column"),
                tagsColumnSize: config.GetInt("tags-column-size"));
        }
        
        public override IJournalQueryExecutor QueryExecutor { get; }
        protected override string JournalConfigPath => PostgreSqlJournalSettings.JournalConfigPath;
        protected override DbConnection CreateDbConnection(string connectionString)
        {
            return new NpgsqlConnection(connectionString);
        }
    }
}