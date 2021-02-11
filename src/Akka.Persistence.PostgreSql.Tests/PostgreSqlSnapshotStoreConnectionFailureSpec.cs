//-----------------------------------------------------------------------
// <copyright file="PostgreSqlSnapshotStoreSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Configuration;
using Akka.Persistence.Snapshot;
using Akka.Persistence.Sql.TestKit;
using Akka.Persistence.TCK.Snapshot;
using Akka.TestKit;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.PostgreSql.Tests
{
    [Collection("PostgreSqlSpec")]
    public class PostgreSqlSnapshotStoreConnectionFailureSpec : SqlSnapshotConnectionFailureSpec
    {
        private static Config Initialize(PostgresFixture fixture, string connectionString)
        {
            //need to make sure db is created before the tests start
            DbUtils.Initialize(fixture);

            var config = @"
                akka.persistence {
                    publish-plugin-commands = on
                    snapshot-store {
                        plugin = ""akka.persistence.snapshot-store.postgresql""
                        postgresql {
                            class = ""Akka.Persistence.PostgreSql.Snapshot.PostgreSqlSnapshotStore, Akka.Persistence.PostgreSql""
                            plugin-dispatcher = ""akka.actor.default-dispatcher""
                            table-name = snapshot_store
                            schema-name = public
                            auto-initialize = on
                            connection-string = """ + connectionString + @"""
                        }
                    }
                }";

            return ConfigurationFactory.ParseString(config);
        }

        public PostgreSqlSnapshotStoreConnectionFailureSpec(ITestOutputHelper output, PostgresFixture fixture)
            : base(Initialize(fixture, DefaultInvalidConnectionString), output: output)
        {
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            DbUtils.Clean();
        }
    }
}