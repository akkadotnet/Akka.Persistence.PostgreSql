using System;
using System.Configuration;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Sql.Common;

namespace Akka.Persistence.PostgreSql
{
    /// <summary>
    /// Configuration settings representation targeting PostgreSql journal actor.
    /// </summary>
    public class PostgreSqlJournalSettings : JournalSettings
    {
        public const string JournalConfigPath = "akka.persistence.journal.postgresql";

        /// <summary>
        /// Flag determining in case of event journal table missing, it should be automatically initialized.
        /// </summary>
        public bool AutoInitialize { get; private set; }

        public PostgreSqlJournalSettings(Config config)
            : base(config)
        {
            AutoInitialize = config.GetBoolean("auto-initialize");
        }
    }

    /// <summary>
    /// Configuration settings representation targeting PostgreSql snapshot store actor.
    /// </summary>
    public class PostgreSqlSnapshotStoreSettings : SnapshotStoreSettings
    {
        public const string SnapshotStoreConfigPath = "akka.persistence.snapshot-store.postgresql";

        /// <summary>
        /// Flag determining in case of snapshot store table missing, it should be automatically initialized.
        /// </summary>
        public bool AutoInitialize { get; private set; }

        public PostgreSqlSnapshotStoreSettings(Config config)
            : base(config)
        {
            AutoInitialize = config.GetBoolean("auto-initialize");
        }
    }

    /// <summary>
    /// An actor system extension initializing support for PostgreSql persistence layer.
    /// </summary>
    public class PostgreSqlPersistence : IExtension
    {
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

        /// <summary>
        /// Journal-related settings loaded from HOCON configuration.
        /// </summary>
        public readonly PostgreSqlJournalSettings JournalSettings;

        /// <summary>
        /// Snapshot store related settings loaded from HOCON configuration.
        /// </summary>
        public readonly PostgreSqlSnapshotStoreSettings SnapshotSettings;

        public PostgreSqlPersistence(ExtendedActorSystem system)
        {
            system.Settings.InjectTopLevelFallback(DefaultConfiguration());

            JournalSettings = new PostgreSqlJournalSettings(system.Settings.Config.GetConfig(PostgreSqlJournalSettings.JournalConfigPath));
            SnapshotSettings = new PostgreSqlSnapshotStoreSettings(system.Settings.Config.GetConfig(PostgreSqlSnapshotStoreSettings.SnapshotStoreConfigPath));

            if (JournalSettings.AutoInitialize)
            {
                var connectionString = string.IsNullOrEmpty(JournalSettings.ConnectionString)
                    ? ConfigurationManager.ConnectionStrings[JournalSettings.ConnectionStringName].ConnectionString
                    : JournalSettings.ConnectionString;

                PostgreSqlInitializer.CreatePostgreSqlJournalTables(connectionString, JournalSettings.SchemaName, JournalSettings.TableName);
            }

            if (SnapshotSettings.AutoInitialize)
            {
                var connectionString = string.IsNullOrEmpty(SnapshotSettings.ConnectionString)
                    ? ConfigurationManager.ConnectionStrings[SnapshotSettings.ConnectionStringName].ConnectionString
                    : SnapshotSettings.ConnectionString;

                PostgreSqlInitializer.CreatePostgreSqlSnapshotStoreTables(connectionString, SnapshotSettings.SchemaName, SnapshotSettings.TableName);
            }
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