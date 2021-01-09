//-----------------------------------------------------------------------
// <copyright file="PostgreSqlJournalJsonSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Configuration;
using Akka.Persistence.TCK.Journal;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.PostgreSql.Tests.Json
{
    [Collection("PostgreSqlSpec")]
    public class PostgreSqlJournalJsonSpec : JournalSpec
    {
        private static readonly Config SpecConfig;

        static PostgreSqlJournalJsonSpec()
        {
            //need to make sure db is created before the tests start
            DbUtils.Initialize();

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
                            stored-as = ""jsonb""
                        }
                    }
                }";

            SpecConfig = ConfigurationFactory.ParseString(config);
        }

        public PostgreSqlJournalJsonSpec(ITestOutputHelper output)
            : base(SpecConfig, "PostgreSqlJournalJsonSpec", output: output)
        {
            Initialize();
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            DbUtils.Clean();
        }
        
        protected override bool SupportsSerialization { get; } = false;
    }
}