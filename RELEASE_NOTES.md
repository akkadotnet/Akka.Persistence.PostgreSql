#### 1.3.8 July 3 2018 ####
Upgraded to support Akka.NET 1.3.8 and to take advantage of some performance improvements that have been added to Akka.Persistence for loading large snapshots, which you can read more about here: https://github.com/akkadotnet/akka.net/issues/3422

#### 1.3.1 September 11 2017 ####
Support for Akka.NET 1.3, .NET Standard 1.6, and the first stable RTM release of Akka.Persistence.

Migration from 1.1.0-beta Up**
The event journal and snapshot store schema has changed with this release.  In order to keep existing stores compatible with this release, you **must** add a column to both stores for `SerializerId` like so:
```sql
ALTER TABLE {your_journal_table_name} ADD COLUMN SerializerId INTEGER NULL
ALTER TABLE {your_snapshot_table_name} ADD COLUMN SerializerId INTEGER NULL
```

#### 1.1.2 January 2017 ####

Updated for Akka.NET 1.1.2.

#### 1.0.6 December 10 2015 ####

#### 1.0.5 August 08 2015 ####

- Changed tables schema: renamed payload_type column to manifest for journal and snapshot tables
- Changed tables schema: added created_at column to journal table
- Added compatibility with Persistent queries API
- Added ability to specify connection string stored in \*.config files

#### 1.0.4 August 07 2015 ####

#### 1.0.3 June 12 2015 ####
**Bugfix release for Akka.NET v1.0.2.**

This release addresses an issue with Akka.Persistence.SqlServer and Akka.Persistence.PostgreSql where both packages were missing a reference to Akka.Persistence.Sql.Common.

In Akka.NET v1.0.3 we've packaged Akka.Persistence.Sql.Common into its own NuGet package and referenced it in the affected packages.

#### 1.0.2 June 2 2015
Initial Release of Akka.Persistence.PostgreSql

Fixes & Changes - Akka.Persistence
* [Renamed GuaranteedDelivery classes to AtLeastOnceDelivery](https://github.com/akkadotnet/akka.net/pull/984)
* [Changes in Akka.Persistence SQL backend](https://github.com/akkadotnet/akka.net/pull/963)
* [PostgreSQL persistence plugin for both event journal and snapshot store](https://github.com/akkadotnet/akka.net/pull/971)
* [Cassandra persistence plugin](https://github.com/akkadotnet/akka.net/pull/995)

**New Features:**

**Akka.Persistence.PostgreSql** and **Akka.Persistence.Cassandra**
Akka.Persistence now has two additional concrete implementations for PostgreSQL and Cassandra! You can install either of the packages using the following commandline:

[Akka.Persistence.PostgreSql Configuration Docs](https://github.com/akkadotnet/akka.net/tree/dev/src/contrib/persistence/Akka.Persistence.PostgreSql)
```
PM> Install-Package Akka.Persistence.PostgreSql
```
