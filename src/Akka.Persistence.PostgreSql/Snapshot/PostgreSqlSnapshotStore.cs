using System.Data.Common;
using Akka.Persistence.Sql.Common;
using Akka.Persistence.Sql.Common.Snapshot;
using Npgsql;

namespace Akka.Persistence.PostgreSql.Snapshot
{
    /// <summary>
    /// Actor used for storing incoming snapshots into persistent snapshot store backed by PostgreSQL database.
    /// </summary>
    public class PostgreSqlSnapshotStore : SqlSnapshotStore
    {
        private readonly PostgreSqlPersistence _extension = PostgreSqlPersistence.Get(Context.System);

        public PostgreSqlSnapshotStore()
        {
            QueryBuilder = new PostgreSqlSnapshotQueryBuilder(_extension.SnapshotSettings);
            QueryMapper = new PostgreSqlSnapshotQueryMapper(Context.System.Serialization);
        }


        protected override DbConnection CreateDbConnection(string connectionString)
        {
            return new NpgsqlConnection(connectionString);
        }

        protected override SnapshotStoreSettings Settings { get { return _extension.SnapshotSettings; } }
    }
}