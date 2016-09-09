//-----------------------------------------------------------------------
// <copyright file="Extension.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Sql.Common;

namespace Akka.Persistence.PostgreSql
{
    public enum StoredAsType
    {
        ByteA,
        Json,
        JsonB
    }

    /// <summary>
    /// Configuration settings representation targeting PostgreSql journal actor.
    /// </summary>
    public class PostgreSqlJournalSettings : JournalSettings
    {
        public const string JournalConfigPath = "akka.persistence.journal.postgresql";
        
        /// <summary>
        /// Specifies Postgres data type for payload column.
        /// </summary>
        public StoredAsType StoredAs { get; set; }

        public PostgreSqlJournalSettings(Config config)
            : base(config)
        {
            StoredAs = (StoredAsType)Enum.Parse(typeof(StoredAsType), config.GetString("stored-as"), true);
        }
    }

    /// <summary>
    /// Configuration settings representation targeting PostgreSql snapshot store actor.
    /// </summary>
    public class PostgreSqlSnapshotStoreSettings : SnapshotStoreSettings
    {
        public const string SnapshotStoreConfigPath = "akka.persistence.snapshot-store.postgresql";

        public PostgreSqlSnapshotStoreSettings(Config config)
            : base(config)
        {
        }
    }

    /// <summary>
    /// An actor system extension initializing support for PostgreSql persistence layer.
    /// </summary>
    public class PostgreSqlPersistence : IExtension
    {
        public readonly Config DefaultJournalConfig;
        public readonly Config DefaultSnapshotConfig;

        /// <summary>
        /// Returns a default configuration for akka persistence SQLite-based journals and snapshot stores.
        /// </summary>
        /// <returns></returns>
        public static Config DefaultConfiguration()
        {
            return ConfigurationFactory.FromResource<PostgreSqlPersistence>("Akka.Persistence.PostgreSql.postgresql.conf");
        }

        public static PostgreSqlPersistence Get(ActorSystem system)
        {
            return system.WithExtension<PostgreSqlPersistence, PostgreSqlPersistenceProvider>();
        }

        public PostgreSqlPersistence(ExtendedActorSystem system)
        {
            system.Settings.InjectTopLevelFallback(DefaultConfiguration());

            DefaultJournalConfig = system.Settings.Config.GetConfig(PostgreSqlJournalSettings.JournalConfigPath);
            DefaultSnapshotConfig = system.Settings.Config.GetConfig(PostgreSqlSnapshotStoreSettings.SnapshotStoreConfigPath);
        }
    }

    /// <summary>
    /// Singleton class used to setup PostgreSQL backend for akka persistence plugin.
    /// </summary>
    public class PostgreSqlPersistenceProvider : ExtensionIdProvider<PostgreSqlPersistence>
    {
        /// <summary>
        /// Creates an actor system extension for akka persistence PostgreSQL support.
        /// </summary>
        /// <param name="system"></param>
        /// <returns></returns>
        public override PostgreSqlPersistence CreateExtension(ExtendedActorSystem system)
        {
            return new PostgreSqlPersistence(system);
        }
    }
}