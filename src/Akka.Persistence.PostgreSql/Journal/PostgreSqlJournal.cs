using System.Data.Common;
using Akka.Actor;
using Akka.Persistence.Sql.Common.Journal;
using Npgsql;

namespace Akka.Persistence.PostgreSql.Journal
{
    public class PostgreSqlJournalEngine : JournalDbEngine
    {
        public PostgreSqlJournalEngine(ActorSystem system)
            : base(system)
        {
            QueryBuilder = new PostgreSqlJournalQueryBuilder(Settings.TableName, Settings.SchemaName);
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
            sqlCommand.Parameters[":created_at"].Value = entry.Timestamp;
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
        private readonly PostgreSqlPersistence _extension = PostgreSqlPersistence.Get(Context.System);

        public PostgreSqlJournal() : base(new PostgreSqlJournalEngine(Context.System))
        {
        }
    }
}