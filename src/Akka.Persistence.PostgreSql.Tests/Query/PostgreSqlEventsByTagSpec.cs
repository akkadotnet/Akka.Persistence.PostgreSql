//-----------------------------------------------------------------------
// <copyright file="PostgreSqlEventsByTagSpec.cs" company="Akka.NET Project">
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
    public sealed class PostgreSqlByteAEventsByTagSpec : PostgreSqlEventsByTagSpec
    {
        public PostgreSqlByteAEventsByTagSpec(ITestOutputHelper output, PostgresFixture fixture) 
            : base(output, fixture, "bytea")
        { }
    }
    
    [Collection("PostgreSqlSpec")]
    public sealed class PostgreSqlJsonBEventsByTagSpec : PostgreSqlEventsByTagSpec
    {
        public PostgreSqlJsonBEventsByTagSpec(ITestOutputHelper output, PostgresFixture fixture) 
            : base(output, fixture, "jsonb")
        { }
    }
    
    [Collection("PostgreSqlSpec")]
    public sealed class PostgreSqlJsonEventsByTagSpec : PostgreSqlEventsByTagSpec
    {
        public PostgreSqlJsonEventsByTagSpec(ITestOutputHelper output, PostgresFixture fixture) 
            : base(output, fixture, "json")
        { }
    }
    
    public abstract class PostgreSqlEventsByTagSpec : EventsByTagSpec
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

        protected PostgreSqlEventsByTagSpec(ITestOutputHelper output, PostgresFixture fixture, string storedAs)
            : base(Initialize(fixture, storedAs), nameof(PostgreSqlEventsByTagSpec), output)
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