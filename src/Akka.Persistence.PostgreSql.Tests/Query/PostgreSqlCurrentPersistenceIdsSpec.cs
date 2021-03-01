//-----------------------------------------------------------------------
// <copyright file="PostgreSqlPersistenceIdsSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Configuration;
using Akka.Persistence.Query;
using Akka.Persistence.Query.Sql;
using Akka.Persistence.TCK.Query;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.PostgreSql.Tests.Query
{
    [Collection("PostgreSqlSpec")]
    public class PostgreSqlCurrentPersistenceIdsSpec : CurrentPersistenceIdsSpec
    {
        private static Config Initialize(PostgresFixture fixture)
        {
            //need to make sure db is created before the tests start
            DbUtils.Initialize(fixture);

            return ConfigurationFactory.ParseString($@"
            akka.loglevel = INFO
            akka.test.single-expect-default = 10s
            akka.persistence.journal.plugin = ""akka.persistence.journal.postgresql""
            akka.persistence.journal.postgresql {{
                class = ""Akka.Persistence.PostgreSql.Journal.PostgreSqlJournal, Akka.Persistence.PostgreSql""
                plugin-dispatcher = ""akka.actor.default-dispatcher""
                table-name = event_journal
                auto-initialize = on
                connection-string = ""{DbUtils.ConnectionString}""
                refresh-interval = 1s
            }}
            akka.test.single-expect-default = 15s")
                .WithFallback(PostgreSqlPersistence.DefaultConfiguration())
                .WithFallback(SqlReadJournal.DefaultConfiguration())
                .WithFallback(Persistence.DefaultConfig());
        }

        public PostgreSqlCurrentPersistenceIdsSpec(ITestOutputHelper output, PostgresFixture fixture) 
            : base(Initialize(fixture), nameof(PostgreSqlCurrentPersistenceIdsSpec), output)
        {
            ReadJournal = Sys.ReadJournalFor<SqlReadJournal>(SqlReadJournal.Identifier);
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            DbUtils.Clean();
        }
    }
}