//-----------------------------------------------------------------------
// <copyright file="PostgreSqlJournalSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Configuration;
using Akka.Persistence.Sql.TestKit;
using Akka.Persistence.TCK.Journal;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.PostgreSql.Tests
{
    [Collection("PostgreSqlSpec")]
    public class PostgreSqlJournalConnectionFailureSpec : SqlJournalConnectionFailureSpec
    {
        private static Config Initialize(PostgresFixture fixture, string connectionString) 
        {
            //need to make sure db is created before the tests start
            DbUtils.Initialize(fixture);

            var config = @"
                akka.persistence {
                    publish-plugin-commands = on
                    journal {
                        plugin = ""akka.persistence.journal.postgresql""
                        postgresql {
                            class = ""Akka.Persistence.PostgreSql.Journal.PostgreSqlJournal, Akka.Persistence.PostgreSql""
                            plugin-dispatcher = ""akka.actor.default-dispatcher""
                            table-name = event_journal
                            schema-name = public
                            auto-initialize = on
                            connection-string = """ + connectionString + @"""
                        }
                    }
                }";

            return ConfigurationFactory.ParseString(config);
        }

        public PostgreSqlJournalConnectionFailureSpec(ITestOutputHelper output, PostgresFixture fixture)
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