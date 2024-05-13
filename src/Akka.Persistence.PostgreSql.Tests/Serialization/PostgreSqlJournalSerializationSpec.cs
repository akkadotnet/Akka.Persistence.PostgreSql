//-----------------------------------------------------------------------
// <copyright file="PostgreSqlJournalSerializationSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using Akka.Configuration;
using Akka.Persistence.TCK.Serialization;
using Akka.Util.Internal;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.PostgreSql.Tests.Serialization
{
    [Collection("PostgreSqlSpec")]
    public sealed class PostgreSqlByteAJournalSerializationSpec : PostgreSqlJournalSerializationSpec
    {
        public PostgreSqlByteAJournalSerializationSpec(ITestOutputHelper output, PostgresFixture fixture) 
            : base(output, fixture, "bytea")
        { }
    }
    
    [Collection("PostgreSqlSpec")]
    public sealed class PostgreSqlJsonBJournalSerializationSpec : PostgreSqlJsonBasedJournalSerializationSpec
    {
        public PostgreSqlJsonBJournalSerializationSpec(ITestOutputHelper output, PostgresFixture fixture) 
            : base(output, fixture, "jsonb")
        { }
    }
    
    [Collection("PostgreSqlSpec")]
    public sealed class PostgreSqlJsonJournalSerializationSpec : PostgreSqlJsonBasedJournalSerializationSpec
    {
        public PostgreSqlJsonJournalSerializationSpec(ITestOutputHelper output, PostgresFixture fixture) 
            : base(output, fixture, "json")
        { }
    }

    public abstract class PostgreSqlJsonBasedJournalSerializationSpec : PostgreSqlJournalSerializationSpec
    {
        protected PostgreSqlJsonBasedJournalSerializationSpec(
            ITestOutputHelper output,
            PostgresFixture fixture,
            string storedAs) 
            : base(output, fixture, storedAs)
        {
        }

        [Fact]
        public override void Journal_should_serialize_Persistent()
        {
            var probe = CreateTestProbe();
            var persistentEvent = new Persistent(new JsonTestJournal.MyPayload("a"), 1L, Pid, null, false, null, WriterGuid);

            var messages = new List<AtomicWrite>
            {
                new(persistentEvent)
            };

            Journal.Tell(new WriteMessages(messages, probe.Ref, ActorInstanceId), TestActor);
            probe.ExpectMsg<WriteMessagesSuccessful>();
            probe.ExpectMsg<WriteMessageSuccess>(m => m.ActorInstanceId == ActorInstanceId && m.Persistent.PersistenceId == Pid);

            Journal.Tell(new ReplayMessages(0, long.MaxValue, long.MaxValue, Pid, probe.Ref), TestActor);
            probe.ExpectMsg<ReplayedMessage>(s => s.Persistent.PersistenceId == Pid
                                                  && s.Persistent.SequenceNr == persistentEvent.SequenceNr
                                                  && s.Persistent.Payload.AsInstanceOf<JsonTestJournal.MyPayload>().Data.Equals("a"));
            probe.ExpectMsg<RecoverySuccess>();
        }

        [Fact(Skip = "Json based persistence does not support string manifest")]
        public override void Journal_should_serialize_Persistent_with_string_manifest()
        {
        }
    }
    
    internal static class JsonTestJournal
    {
        public class MyPayload
        {
            public MyPayload(string data) => Data = data;

            public string Data { get; }
        }

        public class MyPayload2
        {
            public MyPayload2(string data, int n)
            {
                Data = data;
                N = n;
            }

            public string Data { get; }
            public int N { get; }
        }

        public class MyPayload3
        {
            public MyPayload3(string data) => Data = data;

            public string Data { get; }
        }
    }
    
    
    public abstract class PostgreSqlJournalSerializationSpec : JournalSerializationSpec
    {
        protected PostgreSqlJournalSerializationSpec(ITestOutputHelper output, PostgresFixture fixture, string storedAs)
            : base(CreateSpecConfig(fixture, storedAs), "PostgreSqlJournalSerializationSpec", output)
        {
        }

        private static Config CreateSpecConfig(PostgresFixture fixture, string storedAs)
        {
            //need to make sure db is created before the tests start
            DbUtils.Initialize(fixture);

            return ConfigurationFactory.ParseString($@"
                akka.persistence {{
                    publish-plugin-commands = on
                    journal {{
                        plugin = ""akka.persistence.journal.postgresql""
                        postgresql {{
                            class = ""Akka.Persistence.PostgreSql.Journal.PostgreSqlJournal, Akka.Persistence.PostgreSql""
                            plugin-dispatcher = ""akka.actor.default-dispatcher""
                            table-name = event_journal
                            schema-name = public
                            auto-initialize = on
                            connection-string = ""{DbUtils.ConnectionString}""
                            stored-as = {storedAs}
                        }}
                    }}
                }}
                akka.test.single-expect-default = 10s");
        }

        [Fact(Skip = "Sql plugin does not support EventAdapter.Manifest")]
        public override void Journal_should_serialize_Persistent_with_EventAdapter_manifest()
        {
        }
    }
}
