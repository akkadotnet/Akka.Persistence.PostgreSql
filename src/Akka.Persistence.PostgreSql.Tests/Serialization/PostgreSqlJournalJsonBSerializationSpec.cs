//-----------------------------------------------------------------------
// <copyright file="PostgreSqlJournalSerializationSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.TCK.Serialization;
using Akka.Util.Internal;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.PostgreSql.Tests.Serialization
{
    [Collection("PostgreSqlSpec")]
    public class PostgreSqlJournalJsonBSerializationSpec : JournalSerializationSpec
    {
        public PostgreSqlJournalJsonBSerializationSpec(ITestOutputHelper output, PostgresFixture fixture)
            : base(CreateSpecConfig(fixture), "PostgreSqlJournalSerializationSpec", output)
        {
        }

        private static Config CreateSpecConfig(PostgresFixture fixture)
        {
            //need to make sure db is created before the tests start
            DbUtils.Initialize(fixture);

            return ConfigurationFactory.ParseString($@"
                akka.persistence {{
                    publish-plugin-commands = on
                    journal {{
                        plugin = ""akka.persistence.journal.postgresql""
                        postgresql {{
                            stored-as = jsonb
                            connection-string = ""{DbUtils.ConnectionString}""
                            auto-initialize = on
                        }}
                    }}
                    snapshot-store {{
                        plugin = ""akka.persistence.snapshot-store.postgresql""
                        postgresql {{
                            stored-as = jsonb
                            connection-string = ""{DbUtils.ConnectionString}""
                            auto-initialize = on
                        }}
                    }}
                }}
                akka.test.single-expect-default = 10s")
                .WithFallback(PostgreSqlPersistence.DefaultConfiguration());
        }

        [Fact(Skip = "Sql plugin does not support EventAdapter.Manifest")]
        public override void Journal_should_serialize_Persistent_with_EventAdapter_manifest()
        {
        }
        
        [Fact]
        public override void Journal_should_serialize_Persistent_with_string_manifest()
        {
            var probe = CreateTestProbe();
            var persistentEvent = new Persistent(new TestJournal.MyPayload2("b", 5), 1L, Pid, null, false, null, WriterGuid);

            var messages = new List<AtomicWrite>
            {
                new AtomicWrite(persistentEvent)
            };

            Journal.Tell(new WriteMessages(messages, probe.Ref, ActorInstanceId));
            probe.ExpectMsg<WriteMessagesSuccessful>();
            probe.ExpectMsg<WriteMessageSuccess>(m => m.ActorInstanceId == ActorInstanceId && m.Persistent.PersistenceId == Pid);

            Journal.Tell(new ReplayMessages(0, long.MaxValue, long.MaxValue, Pid, probe.Ref));
            probe.ExpectMsg<ReplayedMessage>(s => s.Persistent.PersistenceId == persistentEvent.PersistenceId
                                                  && s.Persistent.SequenceNr == persistentEvent.SequenceNr
                                                  && s.Persistent.Payload.AsInstanceOf<TestJournal.MyPayload2>().Data.Equals("b"));
            probe.ExpectMsg<RecoverySuccess>();
        }

        [Fact]
        public override void Journal_should_serialize_Persistent()
        {
            var probe = CreateTestProbe();
            var persistentEvent = new Persistent(new TestJournal.MyPayload("a"), 1L, Pid, null, false, null, WriterGuid);

            var messages = new List<AtomicWrite>
            {
                new AtomicWrite(persistentEvent)
            };

            Journal.Tell(new WriteMessages(messages, probe.Ref, ActorInstanceId));
            probe.ExpectMsg<WriteMessagesSuccessful>();
            probe.ExpectMsg<WriteMessageSuccess>(m => m.ActorInstanceId == ActorInstanceId && m.Persistent.PersistenceId == Pid);

            Journal.Tell(new ReplayMessages(0, long.MaxValue, long.MaxValue, Pid, probe.Ref));
            probe.ExpectMsg<ReplayedMessage>(s => s.Persistent.PersistenceId == Pid
                                                  && s.Persistent.SequenceNr == persistentEvent.SequenceNr
                                                  && s.Persistent.Payload.AsInstanceOf<TestJournal.MyPayload>().Data.Equals("a"));
            probe.ExpectMsg<RecoverySuccess>();
        }
    }
}
