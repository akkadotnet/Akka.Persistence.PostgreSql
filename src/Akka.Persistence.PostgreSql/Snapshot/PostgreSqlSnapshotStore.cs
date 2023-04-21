//-----------------------------------------------------------------------
// <copyright file="PostgreSqlSnapshotStore.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Configuration;
using Akka.Persistence.Sql.Common.Snapshot;
using Npgsql;
using System;
using System.Data.Common;
using System.Runtime.CompilerServices;
using Akka.Annotations;
using Akka.Persistence.Sql.Common;

namespace Akka.Persistence.PostgreSql.Snapshot
{
    /// <summary>
    /// Actor used for storing incoming snapshots into persistent snapshot store backed by PostgreSQL database.
    /// </summary>
    public class PostgreSqlSnapshotStore : SqlSnapshotStore
    {
        public readonly PostgreSqlPersistence Extension = PostgreSqlPersistence.Get(Context.System);
        public PostgreSqlSnapshotStoreSettings SnapshotSettings { get; }
        public PostgreSqlSnapshotStore(Config snapshotConfig) : base(snapshotConfig)
        {
            var config = snapshotConfig.WithFallback(Extension.DefaultSnapshotConfig);

            QueryExecutor = new PostgreSqlQueryExecutor(
                    CreateQueryConfiguration(config, Settings),
                    Context.System.Serialization);

            SnapshotSettings = new PostgreSqlSnapshotStoreSettings(config);
        }

        [InternalApi]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static PostgreSqlQueryConfiguration CreateQueryConfiguration(Config config, SnapshotStoreSettings settings)
        {
            return new PostgreSqlQueryConfiguration(
                schemaName: config.GetString("schema-name"),
                snapshotTableName: config.GetString("table-name"),
                persistenceIdColumnName: "persistence_id",
                sequenceNrColumnName: "sequence_nr",
                payloadColumnName: "payload",
                manifestColumnName: "manifest",
                timestampColumnName: "created_at",
                serializerIdColumnName: "serializer_id",
                timeout: config.GetTimeSpan("connection-timeout"),
                storedAs: config.GetStoredAsType("stored-as"),
                defaultSerializer: config.GetString("serializer"),
                useSequentialAccess: config.GetBoolean("sequential-access"),
                readIsolationLevel: settings.ReadIsolationLevel,
                writeIsolationLevel: settings.WriteIsolationLevel);
        }

        protected override DbConnection CreateDbConnection(string connectionString)
        {
            return new NpgsqlConnection(connectionString);
        }

        public override ISnapshotQueryExecutor QueryExecutor { get; }
    }
}