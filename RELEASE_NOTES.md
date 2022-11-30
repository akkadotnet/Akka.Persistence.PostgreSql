### 1.4.45 October 21 2022 ###
* [Update Akka.NET to v1.4.45](https://github.com/akkadotnet/akka.net/releases/tag/1.4.45)
* [Bump Npgsql from 6.0.2 to 6.0.7](https://github.com/akkadotnet/Akka.Persistence.PostgreSql/pull/163)

### 1.4.35 March 23 2022 ###
* [Update Akka.NET to v1.4.35](https://github.com/akkadotnet/akka.net/releases/tag/1.4.35)

### 1.4.32 January 18 2022 ###
- Upgraded to [Akka.NET v1.4.32](https://github.com/akkadotnet/akka.net/releases/tag/1.4.32)
- [Upgraded Npgsql to 6.0.2](https://github.com/akkadotnet/Akka.Persistence.PostgreSql/pull/128)
- [Changed Npgsql package version from explicit to ranged](https://github.com/akkadotnet/Akka.Persistence.PostgreSql/pull/126)

 In 1.4.31, we bumped the Npgsql package to 6.0.1 and introduced a regression bug for users who uses Entity Framework by locking them from using .NET Core 3.1. We're fixing this bug in 1.4.32 by using ranged versioning to allow users to use Npgsql 5.0.11 and not forced to update to .NET 6.0.  

### 1.4.31 December 20 2021 ###
- Upgraded to [Akka.NET v1.4.31](https://github.com/akkadotnet/akka.net/releases/tag/1.4.31)

### 1.4.29 December 15 2021 ###
- Upgraded to [Akka.NET v1.4.29](https://github.com/akkadotnet/akka.net/releases/tag/1.4.29)
- [Upgraded Npgsql to 5.0.10](https://github.com/akkadotnet/Akka.Persistence.PostgreSql/pull/106)


### 1.4.25 September 9 2021 ###
- Upgraded to [Akka.NET v1.4.25](https://github.com/akkadotnet/akka.net/releases/tag/1.4.25)

### 1.4.19 June 16 2021 ###

- [Bugfix: Snapshot manifest isn't serialized properly](https://github.com/akkadotnet/Akka.Persistence.PostgreSql/pull/92)
- Upgraded to [Akka.NET v1.4.19](https://github.com/akkadotnet/akka.net/releases/tag/1.4.19)
- Upgraded Npgsql to 5.0.7

### 1.4.17 March 13 2021 ###
Major upgrade and modernization for Akka.Persistence.PostgreSql

- Upgraded to Akka.NET v1.4.17
- Implemented all Akka.Persistence.Query's correctly
- [Lots of other fixes and modernizations, which you can read here](https://github.com/akkadotnet/Akka.Persistence.PostgreSql/projects/1).

Akka.Persistence.PostgreSql is now under the umbrella of the Akka.NET project again and will be maintained at roughly the same cadence as the other officially supported Akka.NET plugins.

#### 1.3.9 August 29 2018 ####
Upgraded for Akka.NET v1.3.9.

**Other Fixes and Improvements**
* [Bugfix: Loading shapshot error](https://github.com/AkkaNetContrib/Akka.Persistence.PostgreSql/issues/57)

#### 1.3.8 July 6 2018 ####
Upgraded to support Akka.NET 1.3.8 and to take advantage of some performance improvements that have been added to Akka.Persistence for loading large snapshots, which you can read more about here: https://github.com/akkadotnet/akka.net/issues/3422

Note that this feature is currently disabled by default in Akka.Persistence.PostgreSql due to https://github.com/AkkaNetContrib/Akka.Persistence.PostgreSql/issues/53

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
