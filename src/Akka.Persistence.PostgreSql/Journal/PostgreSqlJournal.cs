using System.Data.Common;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Persistence.Sql.Common.Journal;
using Npgsql;
using NpgsqlTypes;

namespace Akka.Persistence.PostgreSql.Journal
{
    public class PostgreSqlJournalEngine : JournalDbEngine
    {
        public readonly PostgreSqlJournalSettings PostgreSqlJournalSettings;

        public PostgreSqlJournalEngine(ActorSystem system)
            : base(system)
        {
            PostgreSqlJournalSettings = new PostgreSqlJournalSettings(system.Settings.Config.GetConfig(PostgreSqlJournalSettings.JournalConfigPath));

            QueryBuilder = new PostgreSqlJournalQueryBuilder(Settings.TableName, Settings.SchemaName, PostgreSqlJournalSettings.MetadataTableName);
        }

        protected override string JournalConfigPath { get { return PostgreSqlJournalSettings.JournalConfigPath; } }

        protected override DbConnection CreateDbConnection(string connectionString)
        {
            return new NpgsqlConnection(connectionString);
        }

        protected override void CopyParamsToCommand(DbCommand sqlCommand, JournalEntry entry)
        {
            sqlCommand.Parameters[":persistence_id"].Value = entry.PersistenceId;
            sqlCommand.Parameters[":sequence_nr"].Value = entry.SequenceNr;
            sqlCommand.Parameters[":is_deleted"].Value = entry.IsDeleted;
            sqlCommand.Parameters[":created_at"].Value = entry.Timestamp.Ticks;
            sqlCommand.Parameters[":manifest"].Value = entry.Manifest;
            sqlCommand.Parameters[":payload"].Value = entry.Payload;
        }
    }

    /// <summary>
    /// Persistent journal actor using PostgreSQL as persistence layer. It processes write requests
    /// one by one in synchronous manner, while reading results asynchronously.
    /// </summary>
    public class PostgreSqlJournal : SqlJournal
    {
        public readonly PostgreSqlPersistence Extension = PostgreSqlPersistence.Get(Context.System);

        private readonly string _updateSequenceNrSql;

        public PostgreSqlJournal() : base(new PostgreSqlJournalEngine(Context.System))
        {
            string schemaName = Extension.JournalSettings.SchemaName;
            string tableName = Extension.JournalSettings.MetadataTableName;

            _updateSequenceNrSql = @"WITH upsert AS (UPDATE {0}.{1} SET sequence_nr = :sequence_nr WHERE persistence_id = :persistence_id RETURNING *) INSERT INTO {0}.{1} (persistence_id, sequence_nr) SELECT :persistence_id, :sequence_nr WHERE NOT EXISTS (SELECT * FROM upsert)".QuoteSchemaAndTable(schemaName, tableName);
        }

        protected override async Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            long highestSequenceNr = await DbEngine.ReadHighestSequenceNrAsync(persistenceId, 0);
            await base.DeleteMessagesToAsync(persistenceId, toSequenceNr);

            if (highestSequenceNr <= toSequenceNr)
            {
                await UpdateSequenceNr(persistenceId, highestSequenceNr);
            }
        }

        private async Task UpdateSequenceNr(string persistenceId, long toSequenceNr)
        {
            using (DbConnection connection = DbEngine.CreateDbConnection())
            {
                await connection.OpenAsync();
                using (DbCommand sqlCommand = new NpgsqlCommand(_updateSequenceNrSql))
                {
                    sqlCommand.Parameters.Add(new NpgsqlParameter(":persistence_id", NpgsqlDbType.Varchar, persistenceId.Length)
                    {
                        Value = persistenceId
                    });
                    sqlCommand.Parameters.Add(new NpgsqlParameter(":sequence_nr", NpgsqlDbType.Bigint)
                    {
                        Value = toSequenceNr
                    });

                    sqlCommand.Connection = connection;
                    sqlCommand.CommandTimeout = (int)Extension.JournalSettings.ConnectionTimeout.TotalMilliseconds;
                    await sqlCommand.ExecuteNonQueryAsync();
                }
            }
        }
    }
}