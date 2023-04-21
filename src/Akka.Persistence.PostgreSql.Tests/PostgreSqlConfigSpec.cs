//-----------------------------------------------------------------------
// <copyright file="PostgreSqlConfigSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Data;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.PostgreSql.Journal;
using Akka.Persistence.PostgreSql.Snapshot;
using Akka.Persistence.Sql.Common.Extensions;
using Akka.Persistence.Sql.Common.Journal;
using FluentAssertions;
using FluentAssertions.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.PostgreSql.Tests
{
    public class PostgreSqlConfigSpec : Akka.TestKit.Xunit2.TestKit
    {
        private static readonly Config Config = "akka.persistence.journal.plugin = akka.persistence.journal.postgresql";

        private readonly PersistenceExtension _extension;
        
        public PostgreSqlConfigSpec(ITestOutputHelper helper):base(Config, nameof(PostgreSqlConfigSpec), helper)
        {
            PostgreSqlPersistence.Get(Sys);
            _extension = Persistence.Instance.Apply((ExtendedActorSystem)Sys);
        }
        
        [Fact]
        public void Should_PostgreSql_journal_has_default_config()
        {
            PostgreSqlPersistence.Get(Sys);

            var config = Sys.Settings.Config.GetConfig("akka.persistence.journal.postgresql");
            
            Assert.NotNull(config);
            Assert.Equal("Akka.Persistence.PostgreSql.Journal.PostgreSqlJournal, Akka.Persistence.PostgreSql", config.GetString("class"));
            Assert.Equal("akka.actor.default-dispatcher", config.GetString("plugin-dispatcher"));
            Assert.Equal(string.Empty, config.GetString("connection-string"));
            Assert.Equal(string.Empty, config.GetString("connection-string-name"));
            Assert.Equal(TimeSpan.FromSeconds(30), config.GetTimeSpan("connection-timeout"));
            Assert.Equal("public", config.GetString("schema-name"));
            Assert.Equal("event_journal", config.GetString("table-name"));
            Assert.Equal("metadata", config.GetString("metadata-table-name"));
            Assert.False(config.GetBoolean("auto-initialize"));
            Assert.Equal("Akka.Persistence.Sql.Common.Journal.DefaultTimestampProvider, Akka.Persistence.Sql.Common", config.GetString("timestamp-provider"));
            Assert.False(config.GetBoolean("sequential-access"));
            Assert.Equal(2000, config.GetInt("tags-column-size"));
            Assert.Equal("unspecified", config.GetString("read-isolation-level"));
            Assert.Equal("unspecified", config.GetString("write-isolation-level"));
        }

        [Fact]
        public void PostgreSql_journal_should_have_default_settings()
        {
            var hoconConfig = PostgreSqlPersistence.DefaultConfiguration()
                    .GetConfig(PostgreSqlJournalSettings.JournalConfigPath)
                    .WithFallback(Persistence.DefaultConfig().GetConfig("akka.persistence.journal-plugin-fallback"));
            var settings = new PostgreSqlJournalSettings(hoconConfig);
            var config = PostgreSqlJournal.CreateQueryConfiguration(hoconConfig, settings);
            
            // values should be correct
            settings.ConnectionString.Should().BeEmpty();
            settings.ConnectionStringName.Should().BeEmpty();
            settings.ConnectionTimeout.Should().Be(30.Seconds());
            settings.SchemaName.Should().Be("public");
            settings.JournalTableName.Should().Be("event_journal");
            settings.AutoInitialize.Should().BeFalse();
            settings.MetaTableName.Should().Be("metadata");
            settings.StoredAs.Should().Be(StoredAsType.ByteA);
            Type.GetType(settings.TimestampProvider).Should().Be(typeof(DefaultTimestampProvider));
            settings.ReadIsolationLevel.Should().Be(IsolationLevel.Unspecified);
            settings.WriteIsolationLevel.Should().Be(IsolationLevel.Unspecified);

            config.StoredAs.Should().Be(StoredAsType.ByteA);
            config.TagsColumnSize.Should().Be(2000);
            config.UseBigIntPrimaryKey.Should().BeFalse();
            config.Timeout.Should().Be(30.Seconds());
            config.SchemaName.Should().Be("public");
            config.MetaTableName.Should().Be("metadata");
            config.UseSequentialAccess.Should().BeFalse();
            config.JournalEventsTableName.Should().Be("event_journal");
#pragma warning disable CS0618
            config.DefaultSerializer.Should().Be("json");
#pragma warning restore CS0618
            
            // values should reflect configuration
            settings.ConnectionString.Should().Be(hoconConfig.GetString("connection-string"));
            settings.ConnectionStringName.Should().Be(hoconConfig.GetString("connection-string-name"));
            settings.ConnectionTimeout.Should().Be(hoconConfig.GetTimeSpan("connection-timeout"));
            settings.JournalTableName.Should().Be(hoconConfig.GetString("table-name"));
            settings.SchemaName.Should().Be(hoconConfig.GetString("schema-name"));
            settings.MetaTableName.Should().Be(hoconConfig.GetString("metadata-table-name"));
            settings.TimestampProvider.Should().Be(hoconConfig.GetString("timestamp-provider"));
            settings.ReadIsolationLevel.Should().Be(hoconConfig.GetIsolationLevel("read-isolation-level"));
            settings.WriteIsolationLevel.Should().Be(hoconConfig.GetIsolationLevel("write-isolation-level"));
            settings.AutoInitialize.Should().Be(hoconConfig.GetBoolean("auto-initialize"));
            
            config.StoredAs.Should().Be(hoconConfig.GetStoredAsType("stored-as"));
            config.TagsColumnSize.Should().Be(hoconConfig.GetInt("tags-column-size"));
            config.UseBigIntPrimaryKey.Should().Be(hoconConfig.GetBoolean("use-bigint-identity-for-ordering-column"));
            config.Timeout.Should().Be(hoconConfig.GetTimeSpan("connection-timeout"));
            config.SchemaName.Should().Be(hoconConfig.GetString("schema-name"));
            config.MetaTableName.Should().Be(hoconConfig.GetString("metadata-table-name"));
            config.UseSequentialAccess.Should().Be(hoconConfig.GetBoolean("sequential-access"));
            config.JournalEventsTableName.Should().Be(hoconConfig.GetString("table-name"));
#pragma warning disable CS0618
            config.DefaultSerializer.Should().Be(hoconConfig.GetString("serializer"));
#pragma warning restore CS0618
        }

        [Fact]
        public void Modified_PostgreSql_JournalSettings_should_contain_proper_config()
        {
            var fullConfig = ConfigurationFactory.ParseString(@"
akka.persistence.journal {
	postgresql {
		connection-string = ""a""
		connection-string-name = ""b""
		connection-timeout = 3s
		table-name = ""c""
		auto-initialize = on
		metadata-table-name = ""d""
        schema-name = ""e""
	    serializer = ""f""
		read-isolation-level = snapshot
		write-isolation-level = snapshot
        sequential-access = on
	}
}")
                .WithFallback(PostgreSqlPersistence.DefaultConfiguration())
                .WithFallback(Persistence.DefaultConfig());

            var config = fullConfig.GetConfig("akka.persistence.journal.postgresql");
            var settings = new PostgreSqlJournalSettings(config);
            var executorConfig = PostgreSqlJournal.CreateQueryConfiguration(config, settings);

            // values should be correct
            settings.ConnectionString.Should().Be("a");
            settings.ConnectionStringName.Should().Be("b");
            settings.JournalTableName.Should().Be("c");
            settings.MetaTableName.Should().Be("d");
            settings.SchemaName.Should().Be("e");
            settings.ConnectionTimeout.Should().Be(TimeSpan.FromSeconds(3));
            settings.ReadIsolationLevel.Should().Be(IsolationLevel.Snapshot);
            settings.WriteIsolationLevel.Should().Be(IsolationLevel.Snapshot);
            settings.AutoInitialize.Should().BeTrue();

            executorConfig.JournalEventsTableName.Should().Be("c");
            executorConfig.MetaTableName.Should().Be("d");
            executorConfig.SchemaName.Should().Be("e");
#pragma warning disable CS0618
            executorConfig.DefaultSerializer.Should().Be("f");
#pragma warning restore CS0618
            executorConfig.Timeout.Should().Be(TimeSpan.FromSeconds(3));
            executorConfig.ReadIsolationLevel.Should().Be(IsolationLevel.Snapshot);
            executorConfig.WriteIsolationLevel.Should().Be(IsolationLevel.Snapshot);
            executorConfig.UseSequentialAccess.Should().BeTrue();

            // values should reflect configuration
            settings.ConnectionString.Should().Be(config.GetString("connection-string"));
            settings.ConnectionStringName.Should().Be(config.GetString("connection-string-name"));
            settings.ConnectionTimeout.Should().Be(config.GetTimeSpan("connection-timeout"));
            settings.JournalTableName.Should().Be(config.GetString("table-name"));
            settings.SchemaName.Should().Be(config.GetString("schema-name"));
            settings.MetaTableName.Should().Be(config.GetString("metadata-table-name"));
            settings.ReadIsolationLevel.Should().Be(config.GetIsolationLevel("read-isolation-level"));
            settings.WriteIsolationLevel.Should().Be(config.GetIsolationLevel("write-isolation-level"));
            settings.AutoInitialize.Should().Be(config.GetBoolean("auto-initialize"));

            executorConfig.JournalEventsTableName.Should().Be(config.GetString("table-name"));
            executorConfig.MetaTableName.Should().Be(config.GetString("metadata-table-name"));
            executorConfig.SchemaName.Should().Be(config.GetString("schema-name"));
#pragma warning disable CS0618
            executorConfig.DefaultSerializer.Should().Be(config.GetString("serializer"));
#pragma warning restore CS0618
            executorConfig.Timeout.Should().Be(config.GetTimeSpan("connection-timeout"));
            executorConfig.ReadIsolationLevel.Should().Be(config.GetIsolationLevel("read-isolation-level"));
            executorConfig.WriteIsolationLevel.Should().Be(config.GetIsolationLevel("write-isolation-level"));
            executorConfig.UseSequentialAccess.Should().Be(config.GetBoolean("sequential-access"));
        }        

        [Fact]
        public void Should_PostgreSql_snapshot_has_default_config()
        {
            PostgreSqlPersistence.Get(Sys);

            var config = Sys.Settings.Config.GetConfig("akka.persistence.snapshot-store.postgresql");

            Assert.NotNull(config);
            Assert.Equal("Akka.Persistence.PostgreSql.Snapshot.PostgreSqlSnapshotStore, Akka.Persistence.PostgreSql", config.GetString("class"));
            Assert.Equal("akka.actor.default-dispatcher", config.GetString("plugin-dispatcher"));
            Assert.Equal(string.Empty, config.GetString("connection-string"));
            Assert.Equal(string.Empty, config.GetString("connection-string-name"));
            Assert.Equal(TimeSpan.FromSeconds(30), config.GetTimeSpan("connection-timeout"));
            Assert.Equal("public", config.GetString("schema-name"));
            Assert.Equal("snapshot_store", config.GetString("table-name"));
            Assert.False(config.GetBoolean("auto-initialize"));
            Assert.False(config.GetBoolean("sequential-access"));
        }
        
        [Fact]
        public void PostgreSql_SnapshotStoreSettings_default_should_contain_default_config()
        {
            var config = PostgreSqlPersistence.Get(Sys).DefaultSnapshotConfig;
            var settings = new PostgreSqlSnapshotStoreSettings(config);

            // values should be correct
            settings.ConnectionString.Should().Be(string.Empty);
            settings.ConnectionStringName.Should().Be(string.Empty);
            settings.ConnectionTimeout.Should().Be(TimeSpan.FromSeconds(30));
            settings.SchemaName.Should().Be("public");
            settings.TableName.Should().Be("snapshot_store");
            settings.AutoInitialize.Should().BeFalse();
#pragma warning disable CS0618
            settings.DefaultSerializer.Should().BeNullOrEmpty();
#pragma warning restore CS0618
            settings.ReadIsolationLevel.Should().Be(IsolationLevel.Unspecified);
            settings.WriteIsolationLevel.Should().Be(IsolationLevel.Unspecified);
            settings.FullTableName.Should().Be($"{settings.SchemaName}.{settings.TableName}");

            // values should reflect configuration
            settings.ConnectionString.Should().Be(config.GetString("connection-string"));
            settings.ConnectionStringName.Should().Be(config.GetString("connection-string-name"));
            settings.ConnectionTimeout.Should().Be(config.GetTimeSpan("connection-timeout"));
            settings.SchemaName.Should().Be(config.GetString("schema-name"));
            settings.TableName.Should().Be(config.GetString("table-name"));
            settings.ReadIsolationLevel.Should().Be(config.GetIsolationLevel("read-isolation-level"));
            settings.WriteIsolationLevel.Should().Be(config.GetIsolationLevel("write-isolation-level"));
            settings.AutoInitialize.Should().Be(config.GetBoolean("auto-initialize"));
#pragma warning disable CS0618
            settings.DefaultSerializer.Should().Be(config.GetString("serializer"));
#pragma warning restore CS0618
        }
        
        [Fact]
        public void Modified_PostgreSql_SnapshotStoreSettings_should_contain_proper_config()
        {
            var fullConfig = ConfigurationFactory.ParseString(@"
akka.persistence.snapshot-store.postgresql 
{
	connection-string = ""a""
	connection-string-name = ""b""
	connection-timeout = 3s
	table-name = ""c""
	auto-initialize = on
	serializer = ""d""
    schema-name = ""e""
	sequential-access = on
	read-isolation-level = snapshot
	write-isolation-level = snapshot
}")
                .WithFallback(PostgreSqlPersistence.DefaultConfiguration())
                .WithFallback(Persistence.DefaultConfig());
            
            var config = fullConfig.GetConfig("akka.persistence.snapshot-store.postgresql");
            var settings = new PostgreSqlSnapshotStoreSettings(config);
            var executorConfig = PostgreSqlSnapshotStore.CreateQueryConfiguration(config, settings);

            // values should be correct
            settings.ConnectionString.Should().Be("a");
            settings.ConnectionStringName.Should().Be("b");
            settings.ConnectionTimeout.Should().Be(TimeSpan.FromSeconds(3));
            settings.TableName.Should().Be("c");
#pragma warning disable CS0618
            settings.DefaultSerializer.Should().Be("d");
#pragma warning restore CS0618
            settings.SchemaName.Should().Be("e");
            settings.AutoInitialize.Should().BeTrue();
            settings.ReadIsolationLevel.Should().Be(IsolationLevel.Snapshot);
            settings.WriteIsolationLevel.Should().Be(IsolationLevel.Snapshot);

            executorConfig.SnapshotTableName.Should().Be("c");
#pragma warning disable CS0618
            executorConfig.DefaultSerializer.Should().Be("d");
#pragma warning restore CS0618
            executorConfig.SchemaName.Should().Be("e");
            executorConfig.Timeout.Should().Be(TimeSpan.FromSeconds(3));
            executorConfig.ReadIsolationLevel.Should().Be(IsolationLevel.Snapshot);
            executorConfig.WriteIsolationLevel.Should().Be(IsolationLevel.Snapshot);
            executorConfig.UseSequentialAccess.Should().BeTrue();
            
            // values should reflect configuration
            settings.ConnectionString.Should().Be(config.GetString("connection-string"));
            settings.ConnectionStringName.Should().Be(config.GetString("connection-string-name"));
            settings.ConnectionTimeout.Should().Be(config.GetTimeSpan("connection-timeout"));
            settings.TableName.Should().Be(config.GetString("table-name"));
#pragma warning disable CS0618
            settings.DefaultSerializer.Should().Be(config.GetString("serializer"));
#pragma warning restore CS0618
            settings.SchemaName.Should().Be(config.GetString("schema-name"));
            settings.AutoInitialize.Should().Be(config.GetBoolean("auto-initialize"));
            settings.ReadIsolationLevel.Should().Be(config.GetIsolationLevel("read-isolation-level"));
            settings.WriteIsolationLevel.Should().Be(config.GetIsolationLevel("write-isolation-level"));

            executorConfig.SnapshotTableName.Should().Be(config.GetString("table-name"));
#pragma warning disable CS0618
            executorConfig.DefaultSerializer.Should().Be(config.GetString("serializer"));
#pragma warning restore CS0618
            executorConfig.SchemaName.Should().Be(config.GetString("schema-name"));
            executorConfig.Timeout.Should().Be(config.GetTimeSpan("connection-timeout"));
            executorConfig.ReadIsolationLevel.Should().Be(config.GetIsolationLevel("read-isolation-level"));
            executorConfig.WriteIsolationLevel.Should().Be(config.GetIsolationLevel("write-isolation-level"));
            executorConfig.UseSequentialAccess.Should().Be(config.GetBoolean("sequential-access"));
        }
    }
}
