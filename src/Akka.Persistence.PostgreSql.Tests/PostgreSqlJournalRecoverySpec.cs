using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.TestKit;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.PostgreSql.Tests
{
    [Collection("PostgreSqlSpec")]
    public class PostgreSqlJournalRecoverySpec: Akka.TestKit.Xunit2.TestKit
    {
        private const int TotalActors = 50;
        private const int TotalPersistedMessages = 100;
        private static readonly int Seed;
        private static readonly Random Rnd;

        static PostgreSqlJournalRecoverySpec()
        {
            // Generate seed from DateTime
            var now = DateTime.Now;
            Seed = now.Year + now.Month + now.Day + now.Hour + now.Minute + now.Second + now.Millisecond;
            Rnd = new Random(Seed);
        }
        
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
            var probe = CreateTestProbe();
            //var probes = new TestProbe[TotalActors];
            var actors = new IActorRef[TotalActors];
            var states = new int[TotalActors];

            // Create persistent actors and populate them with data 
            for (var i = 0; i < TotalActors; i++)
            {
                actors[i] = Sys.ActorOf(Props.Create(() => new PersistentActor(i, probe)));
                var m = probe.ExpectMsg<(int, string)>();
                var (idx, msg) = m;
                idx.Should().Be(i);
                msg.Should().Contain("Recovered. State: 0");
            }
            
            // Chaotically populate actors with data
            var lastData = new int[TotalActors];
            var actorIndices = new List<int>();
            for (var i = 0; i < TotalActors; i++)
            {
                actorIndices.Add(i);
            }

            while (actorIndices.Count > 0)
            {
                var i = actorIndices[Rnd.Next(0, actorIndices.Count)];
                states[i] += await actors[i].Ask<int>(++lastData[i]);
                if (lastData[i] == TotalPersistedMessages)
                    actorIndices.Remove(i);
            }

            // Kill all actors
            for (var i = 0; i < TotalActors; i++)
            {
                probe.Watch(actors[i]);
                actors[i].Tell(PoisonPill.Instance);
                probe.ExpectTerminated(actors[i]);
                probe.Unwatch(actors[i]);
            }

            // Restore all actors
            probe = CreateTestProbe();
            for (var i = 0; i < TotalActors; i++)
            {
                actors[i] = Sys.ActorOf(Props.Create(() => new PersistentActor(i, probe)));
            }
            
            // All actors should restore fine
            for (var i = 0; i < TotalActors; i++)
            {
                var m = probe.ExpectMsg<(int, string)>();
                var (index, msg) = m;
                msg.Should().Contain($"Recovered. State: {states[index]}");
                Output.WriteLine($"{index}. {msg}");
            }
            
            // No message should arrive after
            probe.ExpectNoMsg(TimeSpan.FromSeconds(2));
        }
        
        internal class PersistentActor : ReceivePersistentActor
        {
            private bool _recoveryCompleted;
            private readonly IActorRef _probe;
            private readonly int _index;
            private int _state;
            public override string PersistenceId => $"test_id_{_index}";

            public PersistentActor(int index, IActorRef probe)
            {
                _index = index;
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
                        _probe.Tell($"{PersistenceId}: Persisted {p.Data} arrived after recovery was completed. Random seed: {Seed}");
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
                        _probe.Tell((_index, $"Recovered. State: {_state}"));
                        return;
                    case Persisted p:
                        _probe.Tell($"{PersistenceId}: Persisted {p.Data} was unhandled. Random seed: {Seed}");
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