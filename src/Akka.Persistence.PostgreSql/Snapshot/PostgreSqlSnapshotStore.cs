using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Akka.Persistence.Snapshot;
using Npgsql;
using Akka.Persistence.Sql.Common.Snapshot;
using Akka.Persistence.Sql.Common;
using System;
using System.Data.Common;

namespace Akka.Persistence.PostgreSql.Snapshot
{
    /// <summary>
    /// Actor used for storing incoming snapshots into persistent snapshot store backed by PostgreSQL database.
    /// </summary>
    public class PostgreSqlSnapshotStore : SqlSnapshotStore
    {
        private readonly PostgreSqlPersistence _extension;

        public PostgreSqlSnapshotStore()
        {
            _extension = PostgreSqlPersistence.Get(Context.System);
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