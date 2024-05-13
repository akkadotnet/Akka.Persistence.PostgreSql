//-----------------------------------------------------------------------
// <copyright file="PostgreSqlSnapshotStoreSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Configuration;
using Akka.Persistence.TCK.Snapshot;
using Akka.TestKit;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.PostgreSql.Tests
{
    [Collection("PostgreSqlSpec")]
    public sealed class PostgreSqlByteASnapshotStoreSpec : PostgreSqlSnapshotStoreSpec
    {
        public PostgreSqlByteASnapshotStoreSpec(ITestOutputHelper output, PostgresFixture fixture) 
            : base(output, fixture, "bytea")
        { }
    }
    
    [Collection("PostgreSqlSpec")]
    public sealed class PostgreSqlJsonBSnapshotStoreSpec : PostgreSqlSnapshotStoreSpec
    {
        public PostgreSqlJsonBSnapshotStoreSpec(ITestOutputHelper output, PostgresFixture fixture) 
            : base(output, fixture, "jsonb")
        { }
        
        [Fact(Skip = "Akka.Persistence.PostgreSql in JsonB mode does not support ISurrogate serialization")]
        public override void ShouldSerializeSnapshots()
        { }
    }
    
    [Collection("PostgreSqlSpec")]
    public sealed class PostgreSqlJsonSnapshotStoreSpec : PostgreSqlSnapshotStoreSpec
    {
        public PostgreSqlJsonSnapshotStoreSpec(ITestOutputHelper output, PostgresFixture fixture) 
            : base(output, fixture, "json")
        { }
        
        [Fact(Skip = "Akka.Persistence.PostgreSql in Json mode does not support ISurrogate serialization")]
        public override void ShouldSerializeSnapshots()
        { }
    }
    
    public abstract class PostgreSqlSnapshotStoreSpec : SnapshotStoreSpec
    {
        private static Config Initialize(PostgresFixture fixture, string storedAs)
        {
            //need to make sure db is created before the tests start
            DbUtils.Initialize(fixture);

            var config = @$"
                akka.persistence {{
                    publish-plugin-commands = on
                    snapshot-store {{
                        plugin = ""akka.persistence.snapshot-store.postgresql""
                        postgresql {{
                            class = ""Akka.Persistence.PostgreSql.Snapshot.PostgreSqlSnapshotStore, Akka.Persistence.PostgreSql""
                            plugin-dispatcher = ""akka.actor.default-dispatcher""
                            table-name = snapshot_store
                            schema-name = public
                            auto-initialize = on
                            connection-string = ""{DbUtils.ConnectionString}""
                            stored-as = {storedAs}
                        }}
                    }}
                }}
                akka.test.single-expect-default = 10s";

            return ConfigurationFactory.ParseString(config);
        }

        protected PostgreSqlSnapshotStoreSpec(ITestOutputHelper output, PostgresFixture fixture, string storedAs)
            : base(Initialize(fixture, storedAs), "PostgreSqlSnapshotStoreSpec", output: output)
        {
            Initialize();
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            DbUtils.Clean();
        }

        [Fact]
        public void SnapshotStore_should_save_and_overwrite_snapshot_with_same_sequence_number_unskipped()
        {
            TestProbe _senderProbe = CreateTestProbe();
            var md = Metadata[4];
            SnapshotStore.Tell(new SaveSnapshot(md, "s-5-modified"), _senderProbe.Ref);
            var md2 = _senderProbe.ExpectMsg<SaveSnapshotSuccess>().Metadata;
            Assert.Equal(md.SequenceNr, md2.SequenceNr);
            SnapshotStore.Tell(new LoadSnapshot(Pid, new SnapshotSelectionCriteria(md.SequenceNr), long.MaxValue), _senderProbe.Ref);
            var result = _senderProbe.ExpectMsg<LoadSnapshotResult>();
            Assert.Equal("s-5-modified", result.Snapshot.Snapshot.ToString());
            Assert.Equal(md.SequenceNr, result.Snapshot.Metadata.SequenceNr);
            // metadata timestamp may have been changed
        }
    }
}