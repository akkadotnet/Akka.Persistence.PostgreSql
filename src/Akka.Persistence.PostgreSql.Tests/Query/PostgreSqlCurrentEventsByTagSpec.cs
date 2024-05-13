//-----------------------------------------------------------------------
// <copyright file="PostgreSqlEventsByTagSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Configuration;
using Akka.Persistence.Journal;
using Akka.Persistence.Query;
using Akka.Persistence.Query.Sql;
using Akka.Persistence.TCK.Query;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.PostgreSql.Tests.Query
{
    [Collection("PostgreSqlSpec")]
    public sealed class PostgreSqlByteACurrentEventsByTagSpec : PostgreSqlCurrentEventsByTagSpec
    {
        public PostgreSqlByteACurrentEventsByTagSpec(ITestOutputHelper output, PostgresFixture fixture) 
            : base(output, fixture, "bytea")
        { }
    }

    [Collection("PostgreSqlSpec")]
    public sealed class PostgreSqlJsonBCurrentEventsByTagSpec : PostgreSqlCurrentEventsByTagSpec
    {
        public PostgreSqlJsonBCurrentEventsByTagSpec(ITestOutputHelper output, PostgresFixture fixture) 
            : base(output, fixture, "jsonb")
        { }
    }

    [Collection("PostgreSqlSpec")]
    public sealed class PostgreSqlJsonCurrentEventsByTagSpec : PostgreSqlCurrentEventsByTagSpec
    {
        public PostgreSqlJsonCurrentEventsByTagSpec(ITestOutputHelper output, PostgresFixture fixture) 
            : base(output, fixture, "json")
        { }
    }

    public abstract class PostgreSqlCurrentEventsByTagSpec : CurrentEventsByTagSpec
    {
        private static Config Initialize(PostgresFixture fixture, string storedAs)
        {
            //need to make sure db is created before the tests start
            DbUtils.Initialize(fixture);

            return ConfigurationFactory.ParseString($@"
            akka.persistence.journal.plugin = ""akka.persistence.journal.postgresql""
            akka.persistence.journal.postgresql {{
                event-adapters {{
                  color-tagger  = ""Akka.Persistence.TCK.Query.ColorFruitTagger, Akka.Persistence.TCK""
                }}
                event-adapter-bindings = {{
                  ""System.String"" = color-tagger
                }}
                class = ""Akka.Persistence.PostgreSql.Journal.PostgreSqlJournal, Akka.Persistence.PostgreSql""
                plugin-dispatcher = ""akka.actor.default-dispatcher""
                table-name = event_journal
                auto-initialize = on
                connection-string = ""{DbUtils.ConnectionString}""
                refresh-interval = 1s
                stored-as = {storedAs}
            }}
            akka.test.single-expect-default = 10s")
                .WithFallback(PostgreSqlPersistence.DefaultConfiguration())
                .WithFallback(SqlReadJournal.DefaultConfiguration())
                .WithFallback(Persistence.DefaultConfig());
        }

        protected PostgreSqlCurrentEventsByTagSpec(ITestOutputHelper output, PostgresFixture fixture, string storedAs)
            : base(Initialize(fixture, storedAs), nameof(PostgreSqlCurrentEventsByTagSpec), output)
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