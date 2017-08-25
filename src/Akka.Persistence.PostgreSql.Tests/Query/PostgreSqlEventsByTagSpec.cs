//-----------------------------------------------------------------------
// <copyright file="PostgreSqlEventsByTagSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Configuration;
using Akka.Persistence.Journal;
using Akka.Persistence.Query;
using Akka.Persistence.Query.Sql;
using Akka.Persistence.TCK.Query;
using System.Collections.Immutable;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.PostgreSql.Tests.Query
{
    [Collection("PostgreSqlSpec")]
    public class PostgreSqlEventsByTagSpec : EventsByTagSpec
    {
        public static Config SpecConfig => ConfigurationFactory.ParseString($@"
            akka.loglevel = DEBUG
            akka.persistence.journal.plugin = ""akka.persistence.journal.postgresql""
            akka.persistence.journal.postgresql {{
                event-adapters {{
                  color-tagger  = ""Akka.Persistence.PostgreSql.Tests.Query.ColorTagger, Akka.Persistence.PostgreSql.Tests""
                }}
                event-adapter-bindings = {{
                  ""System.String"" = color-tagger
                }}
                class = ""Akka.Persistence.PostgreSql.Journal.PostgreSqlJournal, Akka.Persistence.PostgreSql""
                plugin-dispatcher = ""akka.actor.default-dispatcher""
                table-name = event_journal
                auto-initialize = on
                connection-string = """ + DbUtils.ConnectionString + @"""
                refresh-interval = 1s
            }}
            akka.test.single-expect-default = 10s");


        static PostgreSqlEventsByTagSpec()
        {
            DbUtils.Initialize();
        }

        public PostgreSqlEventsByTagSpec(ITestOutputHelper output) : base(SpecConfig, nameof(PostgreSqlEventsByTagSpec), output)
        {
            ReadJournal = Sys.ReadJournalFor<SqlReadJournal>(SqlReadJournal.Identifier);
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            DbUtils.Clean();
        }
    }

    // This ColorTagger class was previously in Akka.Persistence.Sql.TestKit but has gone
    public class ColorTagger : IWriteEventAdapter
    {
        public static readonly IImmutableSet<string> Colors = ImmutableHashSet.CreateRange(new[] { "green", "black", "blue" });
        public string Manifest(object evt) => string.Empty;

        public object ToJournal(object evt)
        {
            var s = evt as string;
            if (s != null)
            {
                var tags = Colors.Aggregate(ImmutableHashSet<string>.Empty, (acc, color) => s.Contains(color) ? acc.Add(color) : acc);
                return tags.IsEmpty
                    ? evt
                    : new Tagged(evt, tags);
            }
            else return evt;
        }
    }
}