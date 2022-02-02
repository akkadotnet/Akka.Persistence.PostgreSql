using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.TestKit;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.PostgreSql.Tests
{
    [Collection("PostgreSqlSpec")]
    public class PostgreSqlJournalRecoverySpec: Akka.TestKit.Xunit2.TestKit
    {
        private const int TotalActors = 50;
        private const int TotalPersistedMessages = 100;
        
        private static Config Initialize(PostgresFixture fixture) 
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
                            connection-string = """ + DbUtils.ConnectionString + @"""
                        }
                    }
                }
                akka.test.single-expect-default = 10s";

            return ConfigurationFactory.ParseString(config);
        }
        
        public PostgreSqlJournalRecoverySpec(ITestOutputHelper output, PostgresFixture fixture)
            : base(Initialize(fixture), "PostgreSqlJournalSpec", output: output)
        {
        }
        
        [Fact]
        public async Task Recovery_should_emit_messages_in_the_right_order()
        {
            var probes = new TestProbe[TotalActors];
            var actors = new IActorRef[TotalActors];
            var states = new int[TotalActors];

            // Create persistent actors and populate them with data 
            for (var i = 0; i < TotalActors; i++)
            {
                probes[i] = CreateTestProbe();
                actors[i] = Sys.ActorOf(Props.Create(() => new PersistentActor($"test_id_{i}", probes[i])));
                
                probes[i].ExpectMsg("Recovered. State: 0");
                
                foreach (var data in Enumerable.Range(1, TotalPersistedMessages))
                {
                    states[i] += await actors[i].Ask<int>(data);
                }
            }

            // Kill all actors
            for (var i = 0; i < TotalActors; i++)
            {
                probes[i].Watch(actors[i]);
                actors[i].Tell(PoisonPill.Instance);
                probes[i].ExpectTerminated(actors[i]);
                probes[i].Unwatch(actors[i]);
            }
            
            // Restore all actors
            for (var i = 0; i < TotalActors; i++)
            {
                actors[i] = Sys.ActorOf(Props.Create(() => new PersistentActor($"test_id_{i}", probes[i])));
            }
            
            // All actors should restore fine
            for (var i = 0; i < TotalActors; i++)
            {
                probes[i].ExpectMsg($"Recovered. State: {states[i]}");
            }
        }
        
        internal class PersistentActor : ReceivePersistentActor
        {
            private bool _recoveryCompleted;
            private readonly IActorRef _probe;
            private int _state;
            public override string PersistenceId { get; }

            public PersistentActor(string persistenceId, IActorRef probe)
            {
                PersistenceId = persistenceId;
                _probe = probe;
                
                Command<int>(l =>
                {
                    var sender = Sender;
                    Persist(new Persisted(l), p =>
                    {
                        _state += p.Data;
                        sender.Tell(p.Data);
                    });
                });
                
                Recover<Persisted>(p =>
                {
                    if(_recoveryCompleted)
                    {
                        _probe.Tell($"Persisted {p.Data} arrived after recovery was completed.");
                        return;
                    }
                    
                    _state += p.Data;
                });
            }
            
            protected override void Unhandled(object message)
            {
                switch (message)
                {
                    case RecoveryCompleted _:
                        _recoveryCompleted = true;
                        _probe.Tell($"Recovered. State: {_state}");
                        return;
                    case Persisted p:
                        _probe.Tell($"Persisted {p.Data} was unhandled");
                        break;
                }
                base.Unhandled(message);
            }
        }

        internal class Persisted
        {
            public Persisted(int data)
            {
                Data = data;
            }

            public int Data { get; }
        }
    }
}