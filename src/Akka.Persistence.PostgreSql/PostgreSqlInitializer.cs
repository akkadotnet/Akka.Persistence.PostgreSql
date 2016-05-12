using System;
using Npgsql;

namespace Akka.Persistence.PostgreSql
{
    internal static class PostgreSqlInitializer
    {
        private const string SqlJournalFormat = @"
            DO
            $do$
            BEGIN
            IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{2}' AND TABLE_NAME = '{3}') THEN
                CREATE TABLE {0}.{1} (
                    persistence_id VARCHAR(255) NOT NULL,
                    sequence_nr BIGINT NOT NULL,
                    is_deleted BOOLEAN NOT NULL,
                    created_at BIGINT NOT NULL,
                    manifest VARCHAR(500) NOT NULL,
                    payload BYTEA NOT NULL,
                    CONSTRAINT {3}_pk PRIMARY KEY (persistence_id, sequence_nr)
                );
                CREATE INDEX {3}_sequence_nr_idx ON {0}.{1}(sequence_nr);
                CREATE INDEX {3}_created_at_idx ON {0}.{1}(created_at);
            END IF;
            END
            $do$
            ";

        private const string SqlSnapshotStoreFormat = @"
            DO
            $do$
            BEGIN
            IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{2}' AND TABLE_NAME = '{3}') THEN
                CREATE TABLE {0}.{1} (
                    persistence_id VARCHAR(255) NOT NULL,
                    sequence_nr BIGINT NOT NULL,
                    created_at BIGINT NOT NULL,
                    manifest VARCHAR(500) NOT NULL,
                    snapshot BYTEA NOT NULL,
                    CONSTRAINT {3}_pk PRIMARY KEY (persistence_id, sequence_nr)
                );
                CREATE INDEX {3}_sequence_nr_idx ON {0}.{1}(sequence_nr);
                CREATE INDEX {3}_created_at_idx ON {0}.{1}(created_at);
            END IF;
            END
            $do$
            ";

        private const string SqlMetadataFormat = @"
            DO
            $do$
            BEGIN
            IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{2}' AND TABLE_NAME = '{3}') THEN
                CREATE TABLE {0}.{1} (
                    persistence_id VARCHAR(255) NOT NULL,
                    sequence_nr BIGINT NOT NULL,
                    CONSTRAINT {3}_pk PRIMARY KEY (persistence_id, sequence_nr)
                );
            END IF;
            END
            $do$
            ";

        /// <summary>
        /// Initializes a PostgreSQL journal-related tables according to 'schema-name', 'table-name' 
        /// and 'connection-string' values provided in 'akka.persistence.journal.postgresql' config.
        /// </summary>
        internal static void CreatePostgreSqlJournalTables(string connectionString, string schemaName, string tableName)
        {
            var sql = InitJournalSql(tableName, schemaName);
            ExecuteSql(connectionString, sql);
        }

        /// <summary>
        /// Initializes a PostgreSQL snapshot store related tables according to 'schema-name', 'table-name' 
        /// and 'connection-string' values provided in 'akka.persistence.snapshot-store.postgresql' config.
        /// </summary>
        internal static void CreatePostgreSqlSnapshotStoreTables(string connectionString, string schemaName, string tableName)
        {
            var sql = InitSnapshotStoreSql(tableName, schemaName);
            ExecuteSql(connectionString, sql);
        }

        /// <summary>
        /// Initializes a PostgreSQL metadata table according to 'schema-name', 'metadata-table-name' 
        /// and 'connection-string' values provided in 'akka.persistence.snapshot-store.postgresql' config.
        /// </summary>
        internal static void CreatePostgreSqlMetadataTables(string connectionString, string schemaName, string metadataTableName)
        {
            var sql = InitMetadataSql(metadataTableName, schemaName);
            ExecuteSql(connectionString, sql);
        }

        private static string InitJournalSql(string tableName, string schemaName = null)
        {
            if (string.IsNullOrEmpty(tableName)) throw new ArgumentNullException("tableName", "Akka.Persistence.PostgreSql journal table name is required");
            schemaName = schemaName ?? "public";

            var cb = new NpgsqlCommandBuilder();
            return string.Format(SqlJournalFormat, cb.QuoteIdentifier(schemaName), cb.QuoteIdentifier(tableName), cb.UnquoteIdentifier(schemaName), cb.UnquoteIdentifier(tableName));
        }

        private static string InitSnapshotStoreSql(string tableName, string schemaName = null)
        {
            if (string.IsNullOrEmpty(tableName)) throw new ArgumentNullException("tableName", "Akka.Persistence.PostgreSql snapshot store table name is required");
            schemaName = schemaName ?? "public";

            var cb = new NpgsqlCommandBuilder();
            return string.Format(SqlSnapshotStoreFormat, cb.QuoteIdentifier(schemaName), cb.QuoteIdentifier(tableName), cb.UnquoteIdentifier(schemaName), cb.UnquoteIdentifier(tableName));
        }

        private static string InitMetadataSql(string metadataTable, string schemaName)
        {
            if (string.IsNullOrEmpty(metadataTable)) throw new ArgumentNullException("metadataTable", "Akka.Persistence.PostgreSql metadata table name is required");
            schemaName = schemaName ?? "public";

            var cb = new NpgsqlCommandBuilder();
            return string.Format(SqlMetadataFormat, cb.QuoteIdentifier(schemaName), cb.QuoteIdentifier(metadataTable), cb.UnquoteIdentifier(schemaName), cb.UnquoteIdentifier(metadataTable));
        }

        private static void ExecuteSql(string connectionString, string sql)
        {
            using (var conn = new NpgsqlConnection(connectionString))
            using (var command = conn.CreateCommand())
            {
                conn.Open();

                command.CommandText = sql;
                command.ExecuteNonQuery();
            }
        }
    }
}