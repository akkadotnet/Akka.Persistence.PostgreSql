using System.Configuration;
using Akka.Configuration;
using Akka.Persistence.Sql.Common.TestKit;
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
            var connectionString = ConfigurationManager.ConnectionStrings["TestDb"].ConnectionString;

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
                            connection-string = """ + connectionString + @"""
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
