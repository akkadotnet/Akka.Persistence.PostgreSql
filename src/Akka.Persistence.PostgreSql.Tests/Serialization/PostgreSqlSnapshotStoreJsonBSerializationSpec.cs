//-----------------------------------------------------------------------
// <copyright file="PostgreSqlSnapshotStoreSerializationSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.TCK.Serialization;
using Akka.Util.Internal;
using FluentAssertions;
using Newtonsoft.Json;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.PostgreSql.Tests.Serialization
{
    [Collection("PostgreSqlSpec")]
    public class PostgreSqlSnapshotStoreJsonBSerializationSpec : SnapshotStoreSerializationSpec
    {
        public PostgreSqlSnapshotStoreJsonBSerializationSpec(ITestOutputHelper output, PostgresFixture fixture)
            : base(CreateSpecConfig(fixture), "PostgreSqlSnapshotStoreSerializationSpec", output)
        {
        }

        private static Config CreateSpecConfig(PostgresFixture fixture)
        {
            //need to make sure db is created before the tests start
            DbUtils.Initialize(fixture);

            return ConfigurationFactory.ParseString($@"
                akka.persistence {{
                    publish-plugin-commands = on
                    journal {{
                        plugin = ""akka.persistence.journal.postgresql""
                        postgresql {{
                            stored-as = jsonb
                            connection-string = ""{DbUtils.ConnectionString}""
                            auto-initialize = on
                        }}
                    }}
                    snapshot-store {{
                        plugin = ""akka.persistence.snapshot-store.postgresql""
                        postgresql {{
                            stored-as = jsonb
                            connection-string = ""{DbUtils.ConnectionString}""
                            auto-initialize = on
                        }}
                    }}
                }}
                akka.test.single-expect-default = 10s")
                .WithFallback(PostgreSqlPersistence.DefaultConfiguration());
        }

        [Fact]
        public override void SnapshotStore_should_serialize_Payload()
        {
            var probe = CreateTestProbe();

            var snapshot = new Test.MySnapshot("a");

            var metadata = new SnapshotMetadata(Pid, 1);
            SnapshotStore.Tell(new SaveSnapshot(metadata, snapshot), probe.Ref);
            probe.ExpectMsg<SaveSnapshotSuccess>();

            SnapshotStore.Tell(new LoadSnapshot(Pid, SnapshotSelectionCriteria.Latest, long.MaxValue), probe.Ref);
            probe.ExpectMsg<LoadSnapshotResult>(s => 
                s.Snapshot.Snapshot is Test.MySnapshot
                && s.Snapshot.Snapshot.AsInstanceOf<Test.MySnapshot>().Data.Equals("a"));
        }

        [Fact]
        public override void SnapshotStore_should_serialize_Payload_with_string_manifest()
        {
            var probe = CreateTestProbe();

            var snapshot = new Test.MySnapshot2("a");

            var metadata = new SnapshotMetadata(Pid, 1);
            SnapshotStore.Tell(new SaveSnapshot(metadata, snapshot), probe.Ref);
            probe.ExpectMsg<SaveSnapshotSuccess>();

            SnapshotStore.Tell(new LoadSnapshot(Pid, SnapshotSelectionCriteria.Latest, long.MaxValue), probe.Ref);
            probe.ExpectMsg<LoadSnapshotResult>(s => 
                s.Snapshot.Snapshot is Test.MySnapshot2
                && s.Snapshot.Snapshot.AsInstanceOf<Test.MySnapshot2>().Data.Equals("a"));
        }
    }
}
