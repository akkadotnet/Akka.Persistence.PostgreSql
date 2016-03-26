using System.Configuration;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.TestKit.Journal;
using Akka.TestKit;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.PostgreSql.Tests
{
    [Collection("PostgreSqlSpec")]
    public class PostgreSqlJournalSpec : JournalSpec
    {
        private static readonly Config SpecConfig;

        static PostgreSqlJournalSpec() 
        {
            var connectionString = ConfigurationManager.ConnectionStrings["TestDb"].ConnectionString;

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

            SpecConfig = ConfigurationFactory.ParseString(config);

            //need to make sure db is created before the tests start
            DbUtils.Initialize();
        }

        public PostgreSqlJournalSpec(ITestOutputHelper output)
            : base(SpecConfig, "PostgreSqlJournalSpec", output: output)
        {
            Initialize();
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            DbUtils.Clean();
        }

        [Fact]
        public void Journal_should_not_reset_HighestSequenceNr_after_journal_cleanup_unskipped()
        {
            TestProbe _receiverProbe = CreateTestProbe();
            Journal.Tell(new ReplayMessages(0, long.MaxValue, long.MaxValue, Pid, _receiverProbe.Ref));
            for (int i = 1; i <= 5; i++) _receiverProbe.ExpectMsg<ReplayedMessage>(m => IsReplayedMessage(m, i));
            _receiverProbe.ExpectMsg<RecoverySuccess>(m => m.HighestSequenceNr == 5L);

            Journal.Tell(new DeleteMessagesTo(Pid, long.MaxValue, _receiverProbe.Ref));
            _receiverProbe.ExpectMsg<DeleteMessagesSuccess>(m => m.ToSequenceNr == long.MaxValue);

            Journal.Tell(new ReplayMessages(0, long.MaxValue, long.MaxValue, Pid, _receiverProbe.Ref));
            _receiverProbe.ExpectMsg<RecoverySuccess>(m => m.HighestSequenceNr == 5L);
        }
    }
}