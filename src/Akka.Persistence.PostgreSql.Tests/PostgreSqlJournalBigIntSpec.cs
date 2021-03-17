//-----------------------------------------------------------------------
// <copyright file="PostgreSqlJournalSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.TCK;
using Akka.Persistence.TCK.Journal;
using FluentAssertions;
using Npgsql;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.PostgreSql.Tests
{
    [Collection("PostgreSqlSpec")]
    public class PostgreSqlJournalBigIntSpec : JournalSpec
    {
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
                            use-bigint-identity-for-ordering-column = on
                        }
                    }
                }
                akka.test.single-expect-default = 10s";

            return ConfigurationFactory.ParseString(config);
        }

        // TODO: hack. Replace when https://github.com/akkadotnet/akka.net/issues/3811
        protected override bool SupportsSerialization => false;

        public PostgreSqlJournalBigIntSpec(ITestOutputHelper output, PostgresFixture fixture)
            : base(Initialize(fixture), "PostgreSqlJournalBigIntSpec", output)
        {
            Initialize();
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            DbUtils.Clean();
        }

        [Fact]
        public async Task BigInt_Journal_ordering_column_data_type_should_be_BigInt()
        {
            using (var conn = new NpgsqlConnection(DbUtils.ConnectionString))
            {
                conn.Open();

                var sql = $@"
                SELECT column_name, column_default, data_type, is_identity, identity_generation
                FROM information_schema.columns
                WHERE table_schema = 'public'
                    AND table_name = 'event_journal'
                    AND ordinal_position = 1";

                using (var cmd = new NpgsqlCommand(sql, conn))
                {
                    var reader = await cmd.ExecuteReaderAsync();
                    await reader.ReadAsync();

                    // these are the "fingerprint" of BIGINT ... GENERATED ALWAYS AS IDENTITY
                    reader.GetString(0).Should().Be("ordering");
                    reader[1].Should().BeOfType<DBNull>();
                    reader.GetString(2).Should().Be("bigint");
                    reader.GetString(3).Should().Be("YES");
                    reader.GetString(4).Should().Be("ALWAYS");
                }
            }
        }
    }
}