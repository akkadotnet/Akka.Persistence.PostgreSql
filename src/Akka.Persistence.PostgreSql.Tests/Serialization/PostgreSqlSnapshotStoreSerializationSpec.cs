//-----------------------------------------------------------------------
// <copyright file="PostgreSqlSnapshotStoreSerializationSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Configuration;
using Akka.Persistence.TCK.Serialization;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.PostgreSql.Tests.Serialization
{
    [Collection("PostgreSqlSpec")]
    public abstract class PostgreSqlByteASnapshotStoreSerializationSpec : PostgreSqlSnapshotStoreSerializationSpec
    {
        protected PostgreSqlByteASnapshotStoreSerializationSpec(ITestOutputHelper output, PostgresFixture fixture)
            : base(output, fixture, "bytea")
        { }
    }
    
    [Collection("PostgreSqlSpec")]
    public abstract class PostgreSqlJsonBSnapshotStoreSerializationSpec : PostgreSqlSnapshotStoreSerializationSpec
    {
        protected PostgreSqlJsonBSnapshotStoreSerializationSpec(ITestOutputHelper output, PostgresFixture fixture)
            : base(output, fixture, "jsonb")
        { }
    }
    
    [Collection("PostgreSqlSpec")]
    public abstract class PostgreSqlJsonSnapshotStoreSerializationSpec : PostgreSqlSnapshotStoreSerializationSpec
    {
        protected PostgreSqlJsonSnapshotStoreSerializationSpec(ITestOutputHelper output, PostgresFixture fixture)
            : base(output, fixture, "json")
        { }
    }
    
    public abstract class PostgreSqlSnapshotStoreSerializationSpec : SnapshotStoreSerializationSpec
    {
        protected PostgreSqlSnapshotStoreSerializationSpec(ITestOutputHelper output, PostgresFixture fixture, string storedAs)
            : base(CreateSpecConfig(fixture, storedAs), "PostgreSqlSnapshotStoreSerializationSpec", output)
        {
        }

        private static Config CreateSpecConfig(PostgresFixture fixture, string storedAs)
        {
            //need to make sure db is created before the tests start
            DbUtils.Initialize(fixture);

            return ConfigurationFactory.ParseString($@"
                akka.persistence {{
                    publish-plugin-commands = on
                    journal {{
                        plugin = ""akka.persistence.journal.postgresql""
                        postgresql {{
                            class = ""Akka.Persistence.PostgreSql.Journal.PostgreSqlJournal, Akka.Persistence.PostgreSql""
                            plugin-dispatcher = ""akka.actor.default-dispatcher""
                            table-name = event_journal
                            schema-name = public
                            auto-initialize = on
                            connection-string = ""{DbUtils.ConnectionString}""
                            stored-as = {storedAs}
                        }}
                    }}
                }}
                akka.test.single-expect-default = 10s");
        }
    }
}
