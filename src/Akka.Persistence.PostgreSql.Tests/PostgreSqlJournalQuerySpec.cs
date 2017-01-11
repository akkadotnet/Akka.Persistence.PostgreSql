//-----------------------------------------------------------------------
// <copyright file="PostgreSqlJournalQuerySpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System.Configuration;
using Akka.Configuration;
using Akka.Persistence.Sql.TestKit;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.PostgreSql.Tests
{
    [Collection("PostgreSqlSpec")]
    public class PostgreSqlJournalQuerySpec : SqlJournalQuerySpec
    {
        private static readonly Config SpecConfig;

        static PostgreSqlJournalQuerySpec()
        {
            var specString = @"
                akka.persistence {
                    publish-plugin-commands = on
                    journal {
                        plugin = ""akka.persistence.journal.postgresql""
                        postgresql {
                            class = ""Akka.Persistence.PostgreSql.Journal.PostgreSqlJournal, Akka.Persistence.PostgreSql""
                            plugin-dispatcher = ""akka.actor.default-dispatcher""
                            table-name = event_journal
                            auto-initialize = on
                            connection-string-name = ""TestDb""
                        }
                    }
                } " + TimestampConfig("akka.persistence.journal.postgresql");

            SpecConfig = ConfigurationFactory.ParseString(specString);

            //need to make sure db is created before the tests start
            DbUtils.Initialize();
        }

        public PostgreSqlJournalQuerySpec(ITestOutputHelper output)
            : base(SpecConfig, "PostgreSqlJournalQuerySpec", output)
        {
            Initialize();
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            DbUtils.Clean();
        }
    }
}
