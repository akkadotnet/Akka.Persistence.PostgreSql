using Akka.Configuration;
using Akka.Persistence.TCK.Snapshot;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.PostgreSql.Tests;

[Collection("PostgreSqlSpec")]
public class PostgreSqlSnapshotStoreSaveSnapshotSpec: SnapshotStoreSaveSnapshotSpec
{
    private static Config Initialize(PostgresFixture fixture)
    {
        //need to make sure db is created before the tests start
        DbUtils.Initialize(fixture);

        var config = @$"
                akka.persistence {{
                    publish-plugin-commands = on
                    snapshot-store {{
                        plugin = ""akka.persistence.snapshot-store.postgresql""
                        postgresql {{
                            class = ""Akka.Persistence.PostgreSql.Snapshot.PostgreSqlSnapshotStore, Akka.Persistence.PostgreSql""
                            plugin-dispatcher = ""akka.actor.default-dispatcher""
                            table-name = snapshot_store
                            schema-name = public
                            auto-initialize = on
                            connection-string = ""{DbUtils.ConnectionString}""
                            stored-as = bytea
                        }}
                    }}
                }}
                akka.test.single-expect-default = 10s";

        return ConfigurationFactory.ParseString(config);
    }

    public PostgreSqlSnapshotStoreSaveSnapshotSpec(ITestOutputHelper output, PostgresFixture fixture)
        : base(Initialize(fixture), nameof(PostgreSqlSnapshotStoreSaveSnapshotSpec), output: output)
    {
    }
}