//-----------------------------------------------------------------------
// <copyright file="BatchingPostgreSqlAllPersistenceIdsSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Typesafe Inc. <http://www.typesafe.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Configuration;
using Akka.Persistence.Sql.TestKit;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.PostgreSql.Tests.Batching
{
    [Collection("PostgreSqlSpec")]
    public class BatchingPostgreSqlAllPersistenceIdsSpec : AllPersistenceIdsSpec
    {
        public static Config SpecConfig => ConfigurationFactory.ParseString($@"
            akka.loglevel = INFO
            akka.persistence.journal.plugin = ""akka.persistence.journal.postgresql""
            akka.persistence.journal.postgresql {{
                class = ""Akka.Persistence.PostgreSql.Journal.BatchingPostgreSqlJournal, Akka.Persistence.PostgreSql""
                plugin-dispatcher = ""akka.actor.default-dispatcher""
                table-name = event_journal
                auto-initialize = on
                connection-string-name = ""TestDb""
                refresh-interval = 1s
            }}
            akka.test.single-expect-default = 10s");

        public BatchingPostgreSqlAllPersistenceIdsSpec(ITestOutputHelper output) : base(SpecConfig, output)
        {
            DbUtils.Initialize();
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            DbUtils.Clean();
        }
    }
}