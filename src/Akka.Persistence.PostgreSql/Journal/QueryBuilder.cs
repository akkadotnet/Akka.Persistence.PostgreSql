using System;
using System.Collections.Generic;
using System.Text;
using Npgsql;
using NpgsqlTypes;
using Akka.Persistence.Sql.Common.Journal;
using System.Data.Common;
using System.Linq;
using Akka.Persistence.Sql.Common.Queries;

namespace Akka.Persistence.PostgreSql.Journal
{
    internal class PostgreSqlJournalQueryBuilder : IJournalQueryBuilder
    {
        private readonly string _schemaName;
        private readonly string _tableName;

        private readonly string _selectHighestSequenceNrSql;
        private readonly string _insertMessagesSql;

        public PostgreSqlJournalQueryBuilder(string tableName, string schemaName)
        {
            _tableName = tableName;
            _schemaName = schemaName;

            _insertMessagesSql = "INSERT INTO {0}.{1} (persistence_id, sequence_nr, is_deleted, manifest, payload, created_at) VALUES (:persistence_id, :sequence_nr, :is_deleted, :manifest, :payload, :created_at)"
                .QuoteSchemaAndTable(_schemaName, _tableName);
            _selectHighestSequenceNrSql = @"SELECT MAX(sequence_nr) FROM {0}.{1} WHERE persistence_id = :persistence_id".QuoteSchemaAndTable(_schemaName, _tableName);
        }

        public DbCommand SelectEvents(IEnumerable<IHint> hints)
        {
            var sqlCommand = new NpgsqlCommand();

            var sqlized = hints
                .Select(h => HintToSql(h, sqlCommand))
                .Where(x => !string.IsNullOrEmpty(x));

            var where = string.Join(" AND ", sqlized);
            var sql = new StringBuilder("SELECT persistence_id, sequence_nr, is_deleted, manifest, payload, created_at FROM {0}.{1} ".QuoteSchemaAndTable(_schemaName, _tableName));
            if (!string.IsNullOrEmpty(where))
            {
                sql.Append(" WHERE ").Append(where);
            }

            sqlCommand.CommandText = sql.ToString();
            return sqlCommand;
        }

        private string HintToSql(IHint hint, NpgsqlCommand command)
        {
            if (hint is TimestampRange)
            {
                var range = (TimestampRange)hint;
                var sb = new StringBuilder();

                if (range.From.HasValue)
                {
                    sb.Append(" created_at >= :TimestampFrom ");
                    command.Parameters.AddWithValue("@TimestampFrom", range.From.Value);
                }
                if (range.From.HasValue && range.To.HasValue) sb.Append("AND");
                if (range.To.HasValue)
                {
                    sb.Append(" created_at < :TimestampTo ");
                    command.Parameters.AddWithValue("@TimestampTo", range.To.Value);
                }

                return sb.ToString();
            }
            if (hint is PersistenceIdRange)
            {
                var range = (PersistenceIdRange)hint;
                var sb = new StringBuilder(" persistence_id IN (");
                var i = 0;
                foreach (var persistenceId in range.PersistenceIds)
                {
                    var paramName = ":Pid" + (i++);
                    sb.Append(paramName).Append(',');
                    command.Parameters.AddWithValue(paramName, persistenceId);
                }
                return range.PersistenceIds.Count == 0
                    ? string.Empty
                    : sb.Remove(sb.Length - 1, 1).Append(')').ToString();
            }
            else if (hint is WithManifest)
            {
                var manifest = (WithManifest)hint;
                command.Parameters.AddWithValue(":Manifest", manifest.Manifest);
                return " manifest = :Manifest";
            }
            else throw new NotSupportedException(string.Format("PostgreSql journal doesn't support query with hint [{0}]", hint.GetType()));
        }

        public DbCommand SelectMessages(string persistenceId, long fromSequenceNr, long toSequenceNr, long max)
        {
            var sql = BuildSelectMessagesSql(fromSequenceNr, toSequenceNr, max);
            var command = new NpgsqlCommand(sql)
            {
                Parameters = { PersistenceIdToSqlParam(persistenceId) }
            };

            return command;
        }

        public DbCommand SelectHighestSequenceNr(string persistenceId)
        {
            var command = new NpgsqlCommand(_selectHighestSequenceNrSql)
            {
                Parameters = { PersistenceIdToSqlParam(persistenceId) }
            };

            return command;
        }

        public DbCommand InsertBatchMessages(IPersistentRepresentation[] messages)
        {
            var command = new NpgsqlCommand(_insertMessagesSql);
            command.Parameters.Add(":persistence_id", NpgsqlDbType.Varchar);
            command.Parameters.Add(":sequence_nr", NpgsqlDbType.Bigint);
            command.Parameters.Add(":is_deleted", NpgsqlDbType.Boolean);
            command.Parameters.Add(":created_at", NpgsqlDbType.Timestamp);
            command.Parameters.Add(":manifest", NpgsqlDbType.Varchar);
            command.Parameters.Add(":payload", NpgsqlDbType.Bytea);

            return command;
        }

        public DbCommand DeleteBatchMessages(string persistenceId, long toSequenceNr, bool permanent)
        {
            var sql = BuildDeleteSql(toSequenceNr, permanent);
            var command = new NpgsqlCommand(sql)
            {
                Parameters = { PersistenceIdToSqlParam(persistenceId) }
            };

            return command;
        }

        private string BuildDeleteSql(long toSequenceNr, bool permanent)
        {
            var sqlBuilder = new StringBuilder();

            if (permanent)
            {
                sqlBuilder.Append("DELETE FROM {0}.{1} ".QuoteSchemaAndTable(_schemaName, _tableName));
            }
            else
            {
                sqlBuilder.Append("UPDATE {0}.{1} SET is_deleted = true ".QuoteSchemaAndTable(_schemaName, _tableName));
            }

            sqlBuilder.Append("WHERE persistence_id = :persistence_id");

            if (toSequenceNr != long.MaxValue)
            {
                sqlBuilder.Append(" AND sequence_nr <= ").Append(toSequenceNr);
            }

            var sql = sqlBuilder.ToString();
            return sql;
        }

        private string BuildSelectMessagesSql(long fromSequenceNr, long toSequenceNr, long max)
        {
            var sqlBuilder = new StringBuilder();
            sqlBuilder.AppendFormat(
                @"SELECT
                    persistence_id,
                    sequence_nr,
                    is_deleted,
                    manifest,
                    payload,
                    created_at ")
                .Append(" FROM {0}.{1} WHERE persistence_id = :persistence_id".QuoteSchemaAndTable(_schemaName, _tableName));

            // since we guarantee type of fromSequenceNr, toSequenceNr and max
            // we can inline them without risk of SQL injection

            if (fromSequenceNr > 0)
            {
                if (toSequenceNr != long.MaxValue)
                    sqlBuilder.Append(" AND sequence_nr BETWEEN ")
                        .Append(fromSequenceNr)
                        .Append(" AND ")
                        .Append(toSequenceNr);
                else
                    sqlBuilder.Append(" AND sequence_nr >= ").Append(fromSequenceNr);
            }

            if (toSequenceNr != long.MaxValue)
                sqlBuilder.Append(" AND sequence_nr <= ").Append(toSequenceNr);

            if (max != long.MaxValue)
            {
                sqlBuilder.AppendFormat(" LIMIT {0}", max);
            }

            var sql = sqlBuilder.ToString();
            return sql;
        }

        private static NpgsqlParameter PersistenceIdToSqlParam(string persistenceId, string paramName = null)
        {
            return new NpgsqlParameter(paramName ?? ":persistence_id", NpgsqlDbType.Varchar, persistenceId.Length) { Value = persistenceId };
        }
    }
}