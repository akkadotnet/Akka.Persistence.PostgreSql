//-----------------------------------------------------------------------
// <copyright file="PostgreSqlJournalPerfSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Configuration;
using Akka.Persistence.TestKit.Performance;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.PostgreSql.Tests.Performance
{
    [Collection("PostgreSqlSpec")]
    public class PostgreSqlDbBatchJournalPerfSpec : JournalPerfSpec
    {
        public PostgreSqlDbBatchJournalPerfSpec(ITestOutputHelper output, PostgresFixture fixture)
            : base(CreateSpecConfig(fixture), "PostgreSqlJournalPerfSpec", output)
        {
            EventsCount = 5 * 1000;
            ExpectDuration = TimeSpan.FromSeconds(60);
        }

        private static Config CreateSpecConfig(PostgresFixture fixture)
        {
            //need to make sure db is created before the tests start
            DbUtils.Initialize(fixture);

            return ConfigurationFactory.ParseString(@"
            akka.persistence.journal.plugin = ""akka.persistence.journal.postgresql""
            akka.persistence.journal.postgresql {
                class = ""Akka.Persistence.PostgreSql.Journal.PostgreDbBatchSqlJournal, Akka.Persistence.PostgreSql""
                auto-initialize = on
                connection-string = """ + DbUtils.ConnectionString + @"""
            }
            akka.test.single-expect-default = 10s")
                .WithFallback(PostgreSqlPersistence.DefaultConfiguration())
                .WithFallback(Persistence.DefaultConfig());
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            DbUtils.Clean();
        }
    }
}
