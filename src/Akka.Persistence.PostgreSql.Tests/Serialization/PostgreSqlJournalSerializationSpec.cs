//-----------------------------------------------------------------------
// <copyright file="PostgreSqlJournalSerializationSpec.cs" company="Akka.NET Project">
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
    public class PostgreSqlJournalSerializationSpec : JournalSerializationSpec
    {
        public PostgreSqlJournalSerializationSpec(ITestOutputHelper output, PostgresFixture fixture)
            : base(CreateSpecConfig(fixture), "PostgreSqlJournalSerializationSpec", output)
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
                            stored-as = bytea
                            connection-string = ""{DbUtils.ConnectionString}""
                            auto-initialize = on
                        }}
                    }}
                    snapshot-store {{
                        plugin = ""akka.persistence.snapshot-store.postgresql""
                        postgresql {{
                            stored-as = bytea
                            connection-string = ""{DbUtils.ConnectionString}""
                            auto-initialize = on
                        }}
                    }}
                }}
                akka.test.single-expect-default = 10s")
                .WithFallback(PostgreSqlPersistence.DefaultConfiguration());
        }

        [Fact(Skip = "Sql plugin does not support EventAdapter.Manifest")]
        public override void Journal_should_serialize_Persistent_with_EventAdapter_manifest()
        {
        }
    }
}
