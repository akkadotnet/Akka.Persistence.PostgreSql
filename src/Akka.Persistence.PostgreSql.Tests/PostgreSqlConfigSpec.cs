using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Akka.Persistence.PostgreSql.Tests
{
    public class PostgreSqlConfigSpec : Akka.TestKit.Xunit2.TestKit
    {
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
            Assert.Equal(false, config.GetBoolean("auto-initialize"));
            Assert.Equal("Akka.Persistence.Sql.Common.Journal.DefaultTimestampProvider, Akka.Persistence.Sql.Common", config.GetString("timestamp-provider"));
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
            Assert.Equal(false, config.GetBoolean("auto-initialize"));
        }
    }
}
