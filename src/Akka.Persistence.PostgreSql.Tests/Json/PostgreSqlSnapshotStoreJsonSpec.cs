//-----------------------------------------------------------------------
// <copyright file="PostgreSqlSnapshotStoreJsonSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Configuration;
using Akka.Persistence.TCK.Snapshot;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.PostgreSql.Tests.Json
{
    [Collection("PostgreSqlSpec")]
    public class PostgreSqlSnapshotStoreJsonSpec : SnapshotStoreSpec
    {
        private static Config Initialize(PostgresFixture fixture)
        {
            //need to make sure db is created before the tests start
            DbUtils.Initialize(fixture);

            return ConfigurationFactory.ParseString(@"
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
                            connection-string = """ + DbUtils.ConnectionString + @"""
                            stored-as = ""JSONB""
                        }
                    }
                }
                akka.test.single-expect-default = 10s");
        }

        
        public PostgreSqlSnapshotStoreJsonSpec(ITestOutputHelper output, PostgresFixture fixture)
            : base(Initialize(fixture), "PostgreSqlSnapshotStoreJsonSpec", output: output)
        {
            Initialize();
        }

        // TODO: hack. Replace when https://github.com/akkadotnet/akka.net/issues/3811
        protected override bool SupportsSerialization => false;

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            DbUtils.Clean();
        }
    }
}