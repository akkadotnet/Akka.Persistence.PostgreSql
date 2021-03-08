using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.Persistence.Journal;
using Akka.Persistence.Query;
using Akka.Persistence.Query.Sql;
using Akka.Streams;
using Akka.Streams.TestKit;
using Akka.TestKit;
using Akka.Util;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.PostgreSql.Tests.Performance
{
    [Collection("PostgreSqlSpec")]
    public class PostgreSqlEventsByTagPerfSpec : Akka.TestKit.Xunit2.TestKit
    {
        private static Config Initialize(PostgresFixture fixture)
        {
            //need to make sure db is created before the tests start
            DbUtils.Initialize(fixture);

            return ConfigurationFactory.ParseString(@"
            akka.persistence.journal.plugin = ""akka.persistence.journal.postgresql""
            akka.persistence.journal.postgresql {
                event-adapters {
                  color-tagger  = ""Akka.Persistence.TCK.Query.ColorFruitTagger, Akka.Persistence.TCK""
                }
                event-adapter-bindings = {
                  ""System.String"" = color-tagger
                }
                auto-initialize = on
                connection-string = """ + DbUtils.ConnectionString + @"""
            }
            akka.test.single-expect-default = 10s")
                .WithFallback(PostgreSqlPersistence.DefaultConfiguration())
                .WithFallback(SqlReadJournal.DefaultConfiguration())
                .WithFallback(Persistence.DefaultConfig());
        }

        private readonly TestProbe _testProbe;

        protected ActorMaterializer Materializer { get; }
        protected IReadJournal ReadJournal { get; }
        protected int MeasurementIterations { get; }
        protected TimeSpan ExpectDuration { get; }
        protected int EventsCount { get; }

        public PostgreSqlEventsByTagPerfSpec(ITestOutputHelper output, PostgresFixture fixture)
            : base(Initialize(fixture), nameof(PostgreSqlEventsByTagPerfSpec), output)
        {
            MeasurementIterations = 10;
            EventsCount = 2_000_000;
            ExpectDuration = TimeSpan.FromHours(2);
            ReadJournal = Sys.ReadJournalFor<SqlReadJournal>(SqlReadJournal.Identifier);
            Materializer = Sys.Materializer();
            _testProbe = CreateTestProbe();
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            DbUtils.Clean();
        }

        // This is a VERY expensive test, unless you're testing this specifically, ALWAYS skip it.
        [Fact(Skip = "This is a VERY expensive test, unless you're testing this specifically, ALWAYS skip it")]
        public async Task PersistenceActor_performance_must_measure_EventsByTag()
        {
            // Create events
            var commands = new List<string>();
            for (var i = 0; i < EventsCount; ++i)
            {
                commands.Add(i % 2 == 0 ? "untagged" : "green black blue yellow gray red magenta");
            }
            
            // Measure persist performance
            var p1 = Sys.ActorOf(Props.Create(() =>
                new BenchActor("pid", _testProbe, EventsCount, false))); 

            Measure(
                d => $"Persist()-ing {EventsCount} took {d.TotalMilliseconds} ms",
                () =>
                {
                    commands.ForEach(c => p1.Tell(c));
                    _testProbe.ExpectMsg<string>(ExpectDuration).Should().Be(commands.Last());
                    p1.Tell(ResetCounter.Instance);
                });

            var queryMeasurements = new List<TimeSpan>(MeasurementIterations);
            for (var i = 0; i < MeasurementIterations; ++i)
            {
                // Do full garbage collection
                GC.Collect(3);
                GC.WaitForPendingFinalizers();
                GC.Collect(3);
                GC.WaitForFullGCComplete();

                // wait for everything to settle down
                await Task.Delay(500);

                // do query measurement
                queryMeasurements.Add(Measure(
                    d => $"Querying took {d.TotalMilliseconds} ms",
                    () =>
                    {
                        var query = (ICurrentEventsByTagQuery) ReadJournal;
                        var src = query.CurrentEventsByTag("green", Offset.NoOffset());
                        var probe = src.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
                        probe.Request(EventsCount);
                        for (var j = 0; j < EventsCount / 2; ++j)
                        {
                            probe.ExpectNext<EventEnvelope>(e => true);
                        }

                        probe.ExpectComplete();
                    }));
            }

            var avgTime = queryMeasurements
                .Select(c => c.TotalMilliseconds).Sum() / MeasurementIterations;
            var msgPerSec = ((int) (EventsCount / 2) / (double) avgTime) * 1000;

            Output.WriteLine($"Query average time: {avgTime} ms, {msgPerSec} msg/sec");
        }

        /// <summary>
        /// Executes a block of code multiple times (no warm-up)
        /// </summary>
        internal TimeSpan Measure(Func<TimeSpan, string> msg, Action block)
        {
            var sw = Stopwatch.StartNew();
            block();
            sw.Stop();
            Output.WriteLine(msg(sw.Elapsed));
            return sw.Elapsed;
        }

        private class BenchActor : UntypedPersistentActor
        {
            private int _counter = 0;
            private const int BatchSize = 100;
            private List<string> _batch = new List<string>(BatchSize);
            private readonly ILoggingAdapter _log;

            public BenchActor(string persistenceId, IActorRef replyTo, int replyAfter, bool isGroup = false)
            {
                PersistenceId = isGroup
                    ? persistenceId +
                      MurmurHash.StringHash(
                          Context.Parent.Path.Name + Context.Self.Path.Name)
                    : persistenceId;
                ReplyTo = replyTo;
                ReplyAfter = replyAfter;

                _log = Context.GetLogger();
            }

            public override string PersistenceId { get; }
            public IActorRef ReplyTo { get; }
            public int ReplyAfter { get; }

            protected override void OnRecover(object message)
            { }

            protected override void OnCommand(object message)
            {
                switch (message)
                {
                    case string c:
                        Persist(c, d =>
                        {
                            switch (_counter % 2)
                            {
                                case 0 when c != "untagged":
                                    throw new ArgumentException($"Expected to receive [untagged] yet got: [{c}]");
                                case 1 when c != "green black blue yellow gray red magenta":
                                    throw new ArgumentException(
                                        $"Expected to receive [green black blue yellow gray red magenta] yet got: [{c}]");
                            }

                            _counter++;
                            if (_counter == ReplyAfter) ReplyTo.Tell(c);
                        });
                        break;
                    case ResetCounter _:
                        _counter = 0;
                        break;
                    case CleanDatabase _:
                        DeleteMessages(long.MaxValue);
                        break;
                    case DeleteMessagesSuccess _:
                        ReplyTo.Tell("clean");
                        break;
                    case DeleteMessagesFailure _:
                        ReplyTo.Tell("clean failed");
                        break;
                    default:
                        Unhandled(message);
                        break;
                }
            }
        }

        private class CleanDatabase
        {
            public static CleanDatabase Instance { get; } = new CleanDatabase();
            private CleanDatabase() { }
        }
        

        private class ResetCounter
        {
            public static ResetCounter Instance { get; } = new ResetCounter();
            private ResetCounter() { }
        }
    }

    public class ColorFTagger : IWriteEventAdapter
    {
        public static IImmutableSet<string> Colors { get; } = ImmutableHashSet.Create("green", "black", "blue", "yellow", "gray", "red", "magenta");

        public string Manifest(object evt) => string.Empty;

        public object ToJournal(object evt)
        {
            if (evt is string s)
            {
                var colorTags = Colors.Aggregate(ImmutableHashSet<string>.Empty, (acc, color) => s.Contains(color) ? acc.Add(color) : acc);
                return colorTags.IsEmpty
                    ? evt
                    : new Tagged(evt, colorTags);
            }

            return evt;
        }
    }
}