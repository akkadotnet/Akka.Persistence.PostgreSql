using System;
using System.Text;
using Npgsql;
using NpgsqlTypes;
using Akka.Persistence.Sql.Common.Snapshot;
using System.Data.Common;

namespace Akka.Persistence.PostgreSql.Snapshot
{
    internal class PostgreSqlSnapshotQueryBuilder : ISnapshotQueryBuilder
    {
        private readonly string _deleteSql;
        private readonly string _insertSql;
        private readonly string _selectSql;

        public PostgreSqlSnapshotQueryBuilder(PostgreSqlSnapshotStoreSettings settings)
        {
            var tableName = settings.TableName;
            var schemaName = settings.SchemaName;
            _deleteSql =@"DELETE FROM {0}.{1} WHERE persistence_id = :persistence_id ".QuoteSchemaAndTable(schemaName, tableName);
            _selectSql = "SELECT persistence_id, sequence_nr, created_at, manifest, snapshot FROM {0}.{1} WHERE persistence_id = :persistence_id ".QuoteSchemaAndTable(schemaName, tableName);
            _insertSql = "WITH upsert AS (UPDATE {0}.{1} SET created_at = :created_at, snapshot = :snapshot WHERE persistence_id = :persistence_id AND sequence_nr = :sequence_nr RETURNING *) INSERT INTO {0}.{1} (persistence_id, sequence_nr, created_at, manifest, snapshot) SELECT :persistence_id, :sequence_nr, :created_at, :manifest, :snapshot WHERE NOT EXISTS (SELECT * FROM upsert)".QuoteSchemaAndTable(schemaName, tableName);
        }

        public DbCommand DeleteOne(string persistenceId, long sequenceNr, DateTime timestamp)
        {
            var sqlCommand = new NpgsqlCommand();
            sqlCommand.Parameters.Add(new NpgsqlParameter(":persistence_id", NpgsqlDbType.Varchar, persistenceId.Length)
            {
                Value = persistenceId
            });
            var sb = new StringBuilder(_deleteSql);

            if (sequenceNr < long.MaxValue && sequenceNr > 0)
            {
                sb.Append(@"AND sequence_nr = :sequence_nr ");
                sqlCommand.Parameters.Add(new NpgsqlParameter(":sequence_nr", NpgsqlDbType.Bigint) { Value = sequenceNr });
            }

            if (timestamp > DateTime.MinValue && timestamp < DateTime.MaxValue)
            {
                sb.Append(@"AND created_at = :created_at");
                sqlCommand.Parameters.Add(new NpgsqlParameter(":created_at", NpgsqlDbType.Bigint)
                {
                    Value = timestamp.Ticks
                });
            }

            sqlCommand.CommandText = sb.ToString();

            return sqlCommand;
        }

        public DbCommand DeleteMany(string persistenceId, long maxSequenceNr, DateTime maxTimestamp)
        {
            var sqlCommand = new NpgsqlCommand();
            sqlCommand.Parameters.Add(new NpgsqlParameter(":persistence_id", NpgsqlDbType.Varchar, persistenceId.Length)
            {
                Value = persistenceId
            });
            var sb = new StringBuilder(_deleteSql);

            if (maxSequenceNr < long.MaxValue && maxSequenceNr > 0)
            {
                sb.Append(@" AND sequence_nr <= :sequence_nr ");
                sqlCommand.Parameters.Add(new NpgsqlParameter(":sequence_nr", NpgsqlDbType.Bigint)
                {
                    Value = maxSequenceNr
                });
            }

            if (maxTimestamp > DateTime.MinValue && maxTimestamp < DateTime.MaxValue)
            {
                sb.Append(@" AND created_at <= :created_at");
                sqlCommand.Parameters.Add(new NpgsqlParameter(":created_at", NpgsqlDbType.Bigint)
                {
                    Value = maxTimestamp.Ticks
                });
            }

            sqlCommand.CommandText = sb.ToString();

            return sqlCommand;
        }

        public DbCommand InsertSnapshot(SnapshotEntry entry)
        {
            var sqlCommand = new NpgsqlCommand(_insertSql)
            {
                Parameters =
                {
                    new NpgsqlParameter(":persistence_id", NpgsqlDbType.Varchar, entry.PersistenceId.Length) { Value = entry.PersistenceId },
                    new NpgsqlParameter(":sequence_nr", NpgsqlDbType.Bigint) { Value = entry.SequenceNr },
                    new NpgsqlParameter(":created_at", NpgsqlDbType.Bigint) { Value = entry.Timestamp.Ticks },
                    new NpgsqlParameter(":manifest", NpgsqlDbType.Varchar, entry.SnapshotType.Length) { Value = entry.SnapshotType },
                    new NpgsqlParameter(":snapshot", NpgsqlDbType.Bytea, entry.Snapshot.Length) { Value = entry.Snapshot }
                }
            };

            return sqlCommand;
        }

        public DbCommand SelectSnapshot(string persistenceId, long maxSequenceNr, DateTime maxTimestamp)
        {
            var sqlCommand = new NpgsqlCommand();
            sqlCommand.Parameters.Add(new NpgsqlParameter(":persistence_id", NpgsqlDbType.Varchar, persistenceId.Length)
            {
                Value = persistenceId
            });

            var sb = new StringBuilder(_selectSql);
            if (maxSequenceNr > 0 && maxSequenceNr < long.MaxValue)
            {
                sb.Append(" AND sequence_nr <= :sequence_nr ");
                sqlCommand.Parameters.Add(new NpgsqlParameter(":sequence_nr", NpgsqlDbType.Bigint)
                {
                    Value = maxSequenceNr
                });
            }

            if (maxTimestamp > DateTime.MinValue && maxTimestamp < DateTime.MaxValue)
            {
                sb.Append(@" AND (created_at <= :created_at) ");
                sqlCommand.Parameters.Add(new NpgsqlParameter(":created_at", NpgsqlDbType.Bigint)
                {
                    Value = maxTimestamp.Ticks
                });
            }

            sb.Append(" ORDER BY sequence_nr DESC LIMIT 1");
            sqlCommand.CommandText = sb.ToString();
            return sqlCommand;
        }
    }
}