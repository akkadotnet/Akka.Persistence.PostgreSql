## Akka.Persistence.PostgreSql

Akka Persistence journal and snapshot store backed by PostgreSql database.

### Configuration

Both journal and snapshot store share the same configuration keys (however they resides in separate scopes, so they are definied distinctly for either journal or snapshot store):

Remember that connection string must be provided separately to Journal and Snapshot Store.

```hocon
akka.persistence{
	journal {
		plugin = "akka.persistence.journal.postgresql"
		postgresql {
			# qualified type name of the PostgreSql persistence journal actor
			class = "Akka.Persistence.PostgreSql.Journal.PostgreSqlJournal, Akka.Persistence.PostgreSql"

			# dispatcher used to drive journal actor
			plugin-dispatcher = "akka.actor.default-dispatcher"

			# connection string used for database access
			connection-string = ""

			# default SQL commands timeout
			connection-timeout = 30s

			# PostgreSql schema name to table corresponding with persistent journal
			schema-name = public

			# PostgreSql table corresponding with persistent journal
			table-name = event_journal

			# should corresponding journal table be initialized automatically
			auto-initialize = off
			
			# timestamp provider used for generation of journal entries timestamps
			timestamp-provider = "Akka.Persistence.Sql.Common.Journal.DefaultTimestampProvider, Akka.Persistence.Sql.Common"
		
			# metadata table
			metadata-table-name = metadata

			# defines column db type used to store payload. Available option: BYTEA (default), JSON, JSONB
			stored-as = BYTEA

			# Setting used to toggle sequential read access when loading large objects
			# from journals and snapshot stores.
			sequential-access = off

			# When turned on, persistence will use `BIGINT` and `GENERATED ALWAYS AS IDENTITY`
			# for journal table schema creation.
			# NOTE: This only affects newly created tables, as such, it should not affect any
			#       existing database.
			#
			# !!!!! WARNING !!!!!
			# To use this feature, you have to have PorsgreSql version 10 or above
			use-bigint-identity-for-ordering-column = off

			# Setting used to change size of the tags column in persistent journal table
			tags-column-size = 2000
		}
	}

	snapshot-store {
		plugin = "akka.persistence.snapshot-store.postgresql"
		postgresql {
			# qualified type name of the PostgreSql persistence journal actor
			class = "Akka.Persistence.PostgreSql.Snapshot.PostgreSqlSnapshotStore, Akka.Persistence.PostgreSql"

			# dispatcher used to drive journal actor
			plugin-dispatcher = ""akka.actor.default-dispatcher""

			# connection string used for database access
			connection-string = ""

			# default SQL commands timeout
			connection-timeout = 30s

			# PostgreSql schema name to table corresponding with persistent journal
			schema-name = public

			# PostgreSql table corresponding with persistent journal
			table-name = snapshot_store

			# should corresponding journal table be initialized automatically
			auto-initialize = off
			
			# defines column db type used to store payload. Available option: BYTEA (default), JSON, JSONB
			stored-as = BYTEA

			# Setting used to toggle sequential read access when loading large objects
			# from journals and snapshot stores.
			sequential-access = off
		}
	}
}
```
### Table Schema

PostgreSql persistence plugin defines a default table schema used for journal, snapshot store and metadate table.

```SQL
CREATE TABLE {your_journal_table_name} (
	ordering BIGSERIAL NOT NULL PRIMARY KEY,
    persistence_id VARCHAR(255) NOT NULL,
    sequence_nr BIGINT NOT NULL,
    is_deleted BOOLEAN NOT NULL,
    created_at BIGINT NOT NULL,
    manifest VARCHAR(500) NOT NULL,
    payload BYTEA NOT NULL,
    tags VARCHAR(100) NULL,
    serializer_id INTEGER NULL,
    CONSTRAINT {your_journal_table_name}_uq UNIQUE (persistence_id, sequence_nr)
);

CREATE TABLE {your_snapshot_table_name} (
    persistence_id VARCHAR(255) NOT NULL,
    sequence_nr BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    manifest VARCHAR(500) NOT NULL,
    payload BYTEA NOT NULL,
    serializer_id INTEGER NULL,
    CONSTRAINT {your_snapshot_table_name}_pk PRIMARY KEY (persistence_id, sequence_nr)
);

CREATE TABLE {your_metadata_table_name} (
    persistence_id VARCHAR(255) NOT NULL,
    sequence_nr BIGINT NOT NULL,
    CONSTRAINT {your_metadata_table_name}_pk PRIMARY KEY (persistence_id, sequence_nr)
);
```

Note that if you turn on the `akka.persistence.journal.postgresql.use-bigint-identity-for-ordering-column` flag, the journal table schema will be altered to the latest recommended primary key setting.
```
CREATE TABLE {your_journal_table_name} (
	ordering BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    persistence_id VARCHAR(255) NOT NULL,
    sequence_nr BIGINT NOT NULL,
    is_deleted BOOLEAN NOT NULL,
    created_at BIGINT NOT NULL,
    manifest VARCHAR(500) NOT NULL,
    payload BYTEA NOT NULL,
    tags VARCHAR(100) NULL,
    serializer_id INTEGER NULL,
    CONSTRAINT {your_journal_table_name}_uq UNIQUE (persistence_id, sequence_nr)
);
```
Since this script is only run once during table generation, we will not provide any migration path for this change, any migration is left as an exercise for the user.

### Migration

#### From 1.1.0 to 1.3.1
```SQL
ALTER TABLE {your_journal_table_name} ADD COLUMN serializer_id INTEGER NULL;
ALTER TABLE {your_snapshot_table_name} ADD COLUMN serializer_id INTEGER NULL;
```

#### From 1.0.6 to 1.1.0
```SQL
CREATE TABLE {your_metadata_table_name} (
    persistence_id VARCHAR(255) NOT NULL,
    sequence_nr BIGINT NOT NULL,
    CONSTRAINT {your_metadata_table_name}_pk PRIMARY KEY (persistence_id, sequence_nr)
);

ALTER TABLE {your_journal_table_name} DROP CONSTRAINT {your_journal_table_name}_pk;
ALTER TABLE {your_journal_table_name} ADD COLUMN ordering BIGSERIAL NOT NULL PRIMARY KEY;
ALTER TABLE {your_journal_table_name} ADD COLUMN tags VARCHAR(100) NULL;
ALTER TABLE {your_journal_table_name} ADD CONSTRAINT {your_journal_table_name}_uq UNIQUE (persistence_id, sequence_nr);
ALTER TABLE {your_journal_table_name} ADD COLUMN created_at_temp BIGINT NOT NULL;

UPDATE {your_journal_table_name} SET created_at_temp=extract(epoch from create_at);

ALTER TABLE {your_journal_table_name} DROP COLUMN create_at;
ALTER TABLE {your_journal_table_name} DROP COLUMN created_at_ticks;
ALTER TABLE {your_journal_table_name} RENAME COLUMN created_at_temp TO create_at;
```

### Tests

The PostgreSql tests are packaged and run as part of the default "All" build task.

In order to run the tests, you must do the following things:

1. Download and install PostgreSql from: http://www.postgresql.org/download/
2. Install PostgreSql with the default settings.  The default connection string uses the following credentials:
  1. Username: postgres
  2. Password: postgres
3. A custom app.config file can be used and needs to be placed in the same folder as the dll

or run postgres in docker

```
docker run -d --rm --name=akka-postgres-db -p 5432:5432 -l deployer=akkadotnet -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres postgres:9.6
```
