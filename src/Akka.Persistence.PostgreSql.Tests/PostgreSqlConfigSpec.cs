//-----------------------------------------------------------------------
// <copyright file="PostgreSqlConfigSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Reflection;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.PostgreSql.Journal;
using Akka.Persistence.Sql.Common.Journal;
using Akka.TestKit;
using FluentAssertions;
using FluentAssertions.Extensions;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Akka.Persistence.PostgreSql.Tests
{
    [Collection("PostgreSqlSpec")]
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
        }

        [Fact]
        public void PostgreSql_journal_should_have_default_settings()
        {
            var fieldInfo = typeof(ActorCell).GetField("_actor", BindingFlags.Instance | BindingFlags.NonPublic);
            if (fieldInfo is null)
                throw new XunitException($"Failed to reflect _actor field in {nameof(ActorCell)} Type. Have the source code been changed?");
            
            var journalRef = (RepointableActorRef) _extension.JournalFor(null);
            AwaitCondition(
                () => journalRef.IsStarted, 
                TimeSpan.FromSeconds(1), 
                TimeSpan.FromMilliseconds(10));

            var cell = journalRef.Underlying;
            
            AwaitCondition(
                () => fieldInfo.GetValue(cell) != null,
                TimeSpan.FromSeconds(1),
                TimeSpan.FromMilliseconds(10));
            
            var actor = (PostgreSqlJournal) fieldInfo.GetValue(cell);
            var settings = actor.JournalSettings;
            var config = (PostgreSqlQueryConfiguration) actor.QueryExecutor.Configuration;

            settings.ConnectionString.Should().BeEmpty();
            settings.ConnectionStringName.Should().BeEmpty();
            settings.ConnectionTimeout.Should().Be(30.Seconds());
            settings.SchemaName.Should().Be("public");
            settings.JournalTableName.Should().Be("event_journal");
            settings.AutoInitialize.Should().BeFalse();
            settings.MetaTableName.Should().Be("metadata");
            settings.StoredAs.Should().Be(StoredAsType.ByteA);
            Type.GetType(settings.TimestampProvider).Should().Be(typeof(DefaultTimestampProvider));

            config.StoredAs.Should().Be(StoredAsType.ByteA);
            config.TagsColumnSize.Should().Be(2000);
            config.UseBigIntPrimaryKey.Should().BeFalse();
            config.Timeout.Should().Be(30.Seconds());
            config.SchemaName.Should().Be("public");
            config.MetaTableName.Should().Be("metadata");
            config.UseSequentialAccess.Should().BeFalse();
            config.JournalEventsTableName.Should().Be("event_journal");
            config.DefaultSerializer.Should().Be("json");
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
    }
}
