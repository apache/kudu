---
title: Kudu FAQ
layout: single_col
active_nav: faq
single_col_extra_classes: faq
---

## Frequently Asked Questions
{:.no_toc}

- TOC
{:toc}

### Project Motivation

#### Why use column storage format? Would a row-wise format increase performance?

Analytic use-cases almost exclusively use a subset of the columns in the queried
table and generally aggregate values over a broad range of rows. This access pattern
is greatly accelerated by column oriented data. Operational use-cases are more
likely to access most or all of the columns in a row, and might be more appropriately
served by row oriented storage. A column oriented storage format was chosen for
Kudu because it's primarily targeted at analytic use-cases.

There's nothing that precludes Kudu from providing a row-oriented option, and it
could be included in a potential release.

#### Why build a new storage engine? Why not just improve Apache HBase to increase its scan speed?

Kudu shares some characteristics with HBase. Like HBase, it is a real-time store
that supports key-indexed record lookup and mutation.

However, Kudu's design differs from HBase in some fundamental ways:

* Kudu's data model is more traditionally relational, while HBase is schemaless.
* Kudu's on-disk representation is truly columnar and follows an entirely different
  storage design than HBase/BigTable.

Making these fundamental changes in HBase would require a massive redesign, as opposed
to a series of simple changes. HBase is the right design for many classes of
applications and use cases and will continue to be the best storage engine for those
workloads.

### Project Status

#### Is Apache Kudu ready to be deployed into production yet?

Yes! Although Kudu is still relatively new, as far as storage engines are considered,
it is ready for production workloads.

#### Is Kudu open source?

Yes, Kudu is open source and licensed under the Apache Software License, version 2.0.
Apache Kudu is a top level project (TLP) under the umbrella of the Apache Software Foundation.

#### Why was Kudu developed internally at Cloudera before its release?

We believe strongly in the value of open source for the long-term sustainable
development of a project. We also believe that it is easier to work with a small
group of colocated developers when a project is very young. Being in the same
organization allowed us to move quickly during the initial design and development
of the system.

Now that Kudu is public and is part of the Apache Software Foundation, we look
forward to working with a larger community during its next phase of development.


### Getting Started

#### Is training available?

Training is not provided by the Apache Software Foundation, but may be provided
by third-party vendors.

As of January 2016, Cloudera offers an
[on-demand training course](https://university.cloudera.com/content/cloudera-university-ondemand-introduction-to-apache-kudu)
entitled "Introduction to Apache Kudu".
This training covers what Kudu is, and how it compares to other Hadoop-related
storage systems, use cases that will benefit from using Kudu, and how to create,
store, and access data in Kudu tables with Apache Impala.

Aside from training, you can also get help with using Kudu through
[documentation](docs/index.html),
the [mailing lists](community.html),
the [Kudu chat room](https://getkudu-slack.herokuapp.com/), and the
[Cloudera beta release forum](https://community.cloudera.com/t5/Beta-Releases-Kudu-RecordService/bd-p/Beta).

#### Is there a quickstart VM?

Yes. Instructions on getting up and running on Kudu via a VM are provided in Kudu's
[quickstart guide](http://kudu.apache.org/docs/quickstart.html).


### Storage Details

#### How does Kudu store its data? Is the underlying data storage readable without going through Kudu?

Kudu accesses storage devices through the local filesystem, and works best with Ext4 or
XFS. Kudu handles striping across <abbr title="just a bunch of disks">JBOD</abbr> mount
points, and does not require <abbr title="redundant array of inexpensive disks">RAID</abbr>.
Kudu's write-ahead logs (WALs) can be stored on separate locations from the data files,
which means that WALs can be stored on <abbr title="solid state drives">SSDs</abbr> to
enable lower-latency writes on systems with both SSDs and magnetic disks.

Kudu's on-disk data format closely resembles Parquet, with a few differences to
support efficient random access as well as updates. The underlying data is not
directly queryable without using the Kudu client APIs. The Kudu developers have worked hard
to ensure that Kudu's scan performance is performant, and has focused on storing data
efficiently without making the trade-offs that would be required to allow direct access
to the data files.

#### Is Kudu an in-memory database?

Kudu is not an
[in-memory database](https://en.wikipedia.org/wiki/In-memory_database)
since it primarily relies on disk storage. This should not be confused with Kudu's
experimental use of
[persistent memory](https://en.wikipedia.org/wiki/Non-volatile_memory)
which is integrated in the block cache. In the future, this integration this will
allow the cache to survive tablet server restarts, so that it never starts "cold".

In addition, Kudu's C++ implementation can scale to very large heaps. Coupled
with its CPU-efficient design, Kudu's heap scalability offers outstanding
performance for data sets that fit in memory.

#### Does Kudu run its own format type or does it use Parquet? What is the compression recommendation?

Kudu's on-disk data format closely resembles Parquet, with a few differences to
support efficient random access as well as updates. The underlying data is not
directly queryable without using the Kudu client APIs. The Kudu developers have worked
hard to ensure that Kudu's scan performance is performant, and has focused on
storing data efficiently without making the trade-offs that would be required to
allow direct access to the data files.

The recommended compression codec is dependent on the appropriate trade-off
between cpu utilization and storage efficiency and is therefore use-case dependent.

#### Should compactions be managed?

Compactions in Kudu are designed to be small and to always be running in the
background. They operate under a (configurable) budget to prevent tablet servers
from unexpectedly attempting to rewrite tens of GB of data at a time. Since compactions
are so predictable, the only tuning knob available is the number of threads dedicated
to flushes and compactions in the *maintenance manager*.

#### What is the compaction performance like?

Kudu runs a background compaction process that incrementally and constantly
compacts data. Constant small compactions provide predictable latency by avoiding
major compaction operations that could monopolize CPU and IO resources.

#### Is there a time-to-live property as in HBase to delete a record automatically?

No, Kudu does not currently support such a feature.

#### Do the tablet servers require a Linux filesystem or control the storage devices directly?

The tablet servers store data on the Linux filesystem. We recommend ext4 or xfs
mount points for the storage directories. Typically, a Kudu tablet server will
share the same partitions as existing HDFS datanodes.

#### Are there chances of region server hotspotting like with HBase and how does Kudu mitigate this?

Hotspotting in HBase is an attribute inherited from the distribution strategy used.

By default, HBase uses range based distribution. Range based partitioning stores
ordered values that fit within a specified range of a provided key contiguously
on disk. Range based partitioning is efficient when there are large numbers of
concurrent small queries, as only servers in the cluster that have values within
the range specified by the query will be recruited to process that query. Range
partitioning is susceptible to hotspots, either because the key(s) used to
specify the range exhibits "data skew" (the number of rows within each range
is not uniform), or some data is queried more frequently creating "workload
skew".

In contrast, hash based distribution specifies a certain number of "buckets"
and distribution keys are passed to a hash function that produces the value of
the bucket that the row is assigned to. If the distribution key is chosen
carefully (a unique key with no business meaning is ideal) hash distribution
will result in each server in the cluster having a uniform number of rows. Hash
based distribution protects against both data skew and workload skew.
Additionally, it provides the highest possible throughput for any individual
query because all servers are recruited in parallel as data will be evenly
spread across every server in the cluster. However, optimizing for throughput by
recruiting every server in the cluster for every query comes compromises the
maximum concurrency that the cluster can achieve. HBase can use hash based
distribution by "salting" the row key.

Kudu supports both approaches, giving you the ability choose to emphasize
concurrency at the expense of potential data and workload skew with range
partitioning, or query throughput at the expense of concurrency through hash
partitioning.

#### Is there any limitation to the size of data that can be added to a column?

There is no hard limit imposed by Kudu, but large values (10s of KB and above)
are likely to perform poorly and may cause stability issues in current releases.

#### Does Kudu support dynamic partitioning?

Kudu is a storage engine, not a SQL engine. Dynamic partitions are created at
execution time rather than at query time, but in either case the process will
look the same from Kudu's perspective: the query engine will pass down
partition keys to Kudu.

### Consistency and CAP Theorem

#### What is Kudu's consistency model? Is Kudu a CP or AP system?

In the parlance of the CAP theorem, Kudu is a
<abbr title="consistent (but not available) under network partitions">CP</abbr>
type of storage engine. Writing to a tablet will be delayed if the server that hosts that
tablet's leader replica fails until a quorum of servers is able to elect a new leader and
acknowledge a given write request.

Kudu gains the following properties by using Raft consensus:

* Leader elections are fast. As soon as the leader misses 3 heartbeats (half a second each), the
  remaining followers will elect a new leader which will start accepting operations right away.
  This whole process usually takes less than 10 seconds.
* Follower replicas don't allow writes, but they do allow reads when fully up-to-date data is not
  required. Thus, queries against historical data (even just a few minutes old) can be
  sent to any of the replicas. If that replica fails, the query can be sent to another
  replica immediately.

In current releases, some of these properties are not be fully implemented and
may suffer from some deficiencies. See the answer to
"[Is Kudu's consistency level tunable?](#is-kudus-consistency-level-tunable)"
for more information.

#### Is Kudu's consistency level tunable?

Yes, Kudu's consistency level is partially tunable, both for writes and reads (scans):

* Writes to a single tablet are always internally consistent. When writing to multiple tablets,
  with multiple clients, the user has a choice between no consistency (the default) and
  enforcing "external consistency" in two different ways: one that optimizes for latency
  requires the user to perform additional work and another that requires no additional
  work but can result in some additional latency.
* Scans have "Read Committed" consistency by default. If the user requires strict-serializable
  scans it can choose the `READ_AT_SNAPSHOT` mode and, optionally, provide a timestamp. The default
  option is non-blocking but the `READ_AT_SNAPSHOT` option may block when reading from non-leader
  replicas.

Kudu's transactional semantics are a work in progress, see
[Kudu Transaction Semantics](docs/transaction_semantics.html) for
further information and caveats.

#### How does Kudu handle dirty reads?

Neither "read committed" nor "READ_AT_SNAPSHOT" consistency modes permit dirty reads.

#### Where is Kudu's Jepsen report?

No one has yet run [Jepsen](https://github.com/aphyr/jepsen) on Kudu, but it
would be a very welcome contribution. However, we know of
[several issues](docs/transaction_semantics.html#known_issues)
that need to be resolved before Kudu can pass a Jepsen serializable consistency
test. We are committed to passing Jepsen, as well as other consistency-focused stress
tests, before releasing Kudu 1.0.

### Working With Other Storage Systems

#### Can data be loaded directly into Kudu? What ingest tools can be used?

Kudu provides direct access via Java and C++ APIs. An experimental Python API is
also available and is expected to be fully supported in the future. The easiest
way to load data into Kudu is to use a `CREATE TABLE ... AS SELECT * FROM ...`
statement in Impala. Although Kudu has not been extensively tested to work with
ingest tools such as Flume, Sqoop, or Kafka, several of these have been
experimentally tested. Explicit support for these ingest tools is expected with
Kudu's first generally available release.

#### What's the most efficient way to bulk load data into Kudu?

The easiest way to load data into Kudu is if the data is already managed by Impala.
In this case, a simple `INSERT INTO TABLE some_kudu_table SELECT * FROM some_csv_table`
does the trick.

You can also use Kudu's MapReduce OutputFormat to load data from HDFS, HBase, or
any other data store that has an InputFormat.

No tool is provided to load data directly into Kudu's on-disk data format. We
have found that for many workloads, the insert performance of Kudu is comparable
to bulk load performance of other systems.


#### What kinds of data can Kudu store? Can it accept JSON?

Kudu uses typed storage and currently does not have a specific type for semi-
structured data such as JSON. Semi-structured data can be stored in a STRING or
BINARY column, but large values (10s of KB or more) are likely to cause
performance or stability problems in current versions.

Fuller support for semi-structured types like JSON and protobuf will be added in
the future, contingent on demand from early adopters.

#### Is there a JDBC driver available?

Kudu is not a SQL engine. The availability of JDBC and ODBC drivers will be
dictated by the SQL engine used in combination with Kudu.

#### Do you need Hadoop to run Kudu?

Kudu does not rely on any Hadoop components if it is accessed using its
programmatic APIs. However, most usage of Kudu will include at least one Hadoop
component such as MapReduce, Spark, or Impala. Components that have been
modified to take advantage of Kudu storage, such as Impala, might have Hadoop
dependencies.

#### What is the relationship between Kudu and HDFS? Does Kudu require HDFS?

Kudu is a separate storage system. It does not rely on or run on top of HDFS.
Kudu can coexist with HDFS on the same cluster.

#### Why doesn't Kudu store its data in HDFS?

We considered a design which stored data on HDFS, but decided to go in a different
direction, for the following reasons:

* Kudu handles replication at the logical level using Raft consensus, which makes
  HDFS replication redundant. We could have mandated a replication level of 1, but
  that is not HDFS's best use case.
* Filesystem-level snapshots provided by HDFS do not directly translate to Kudu support for
  snapshots, because it is hard to predict when a given piece of data will be flushed
  from memory. In addition, snapshots only make sense if they are provided on a per-table
  level, which would be difficult to orchestrate through a filesystem-level snapshot.
* HDFS security doesn't translate to table- or column-level ACLs. Similar to HBase
  ACLs, Kudu would need to implement its own security system and would not get much
  benefit from the HDFS security model.
* Kudu's scan performance is already within the same ballpark as Parquet files stored
  on HDFS, so there's no need to accomodate reading Kudu's data files directly.

#### What frameworks are integrated with Kudu for data access?

Kudu is already integrated with Impala, MapReduce, and Spark. Additional
frameworks are expected for GA with Hive being the current highest priority
addition.

#### Can I colocate Kudu with HDFS on the same servers?

Kudu can be colocated with HDFS on the same data disk mount points. This is similar
to colocating Hadoop and HBase workloads. Kudu has been extensively tested
in this type of configuration, with no stability issues. For latency-sensitive workloads,
consider dedicating an SSD to Kudu's WAL files.


### Hardware and Operations

#### What are Kudu's runtime dependencies?

Kudu itself doesn't have any service dependencies and can run on a cluster without Hadoop,
Impala, Spark, or any other project.

If you want to use Impala, note that Impala depends on Hive's metadata server, which has
its own dependencies on Hadoop. It is not currently possible to have a pure Kudu+Impala
deployment.

#### Should the master node have more RAM than worker nodes?

For small clusters with fewer than 100 nodes, with reasonable numbers of tables
and tablets, the master node requires very little RAM, typically 1 GB or less.
For workloads with large numbers of tables or tablets, more RAM will be
required, but not more RAM than typical Hadoop worker nodes.

#### Is the master node a single point of failure?

No. Kudu includes support for running multiple Master nodes, using the same Raft
consensus algorithm that is used for durability of data.

#### Does Kudu require the use of SSDs?

No, SSDs are not a requirement of Kudu. Kudu is designed to take full advantage
of fast storage and large amounts of memory if present, but neither is required.

#### Can a Kudu deployment be geo-distributed?

We don't recommend geo-distributing tablet servers this time because of the possibility
of higher write latencies. In addition, Kudu is not currently aware of data placement.
This could lead to a situation where the master might try to put all replicas
in the same datacenter. We plan to implement the necessary features for geo-distribution
in a future release.

#### Where is the Kudu shell?

Kudu doesn't yet have a command-line shell. If the Kudu-compatible version of Impala is
installed on your cluster then you can use it as a replacement for a shell. See also the
docs for the [Kudu Impala Integration](docs/kudu_impala_integration.html).

#### Is the Kudu Master a bottleneck?

Although the Master is not sharded, it is not expected to become a bottleneck for
the following reasons.

* Like many other systems, the master is not on the hot path once the tablet
  locations are cached.
* The Kudu master process is extremely efficient at keeping everything in memory.
  In our testing on an 80-node cluster, the 99.99th percentile latency for getting
  tablet locations was on the order of hundreds of microseconds (not a typo).

#### What operating systems does Kudu support?

Linux is required to run Kudu. See the [installation
guide](docs/installation.html#_prerequisites_and_requirements) for details. OSX
is supported as a development platform in Kudu 0.6.0 and newer. The Java client
can be used on any JVM 7+ platform.

#### What Linux-based operating systems are known NOT to work with Kudu?

**RHEL 5**: the kernel is missing critical features for handling disk space
reclamation (such as hole punching), and it is not possible to run applications
which use C++11 language features.

**Debian 7**: ships with gcc 4.7.2 which produces broken Kudu optimized code,
and there is insufficient support for applications which use C++11 language
features.

**SLES 11**: it is not possible to run applications which use C++11 language
features.

#### How can I back up my Kudu data?

Kudu doesn't yet have a built-in backup mechanism. Similar to bulk loading data,
Impala can help if you have it available. You can use it to copy your data into
Parquet format using a statement like:

    INSERT INTO TABLE some_parquet_table SELECT * FROM kudu_table

then use [distcp](http://hadoop.apache.org/docs/r1.2.1/distcp2.html)
to copy the Parquet data to another cluster.

#### Can the WAL transaction logs be used to build a disaster recovery site?

Currently, Kudu does not support any mechanism for shipping or replaying WALs
between sites.

#### Is there a single WAL per tablet or per table?

There is one WAL per tablet.

### Security

#### How is security handled in Kudu?

Kudu has no security features at the moment, but this is a goal for GA.

### Schema Design

#### Can Kudu tolerate changing schemas?

Yes, Kudu provides the ability to add, drop, and rename columns/tables.
Currently it is not possible to change the type of a column in-place, though
this is expected to be added to a subsequent Kudu release.

#### Are there best practices in terms of data modeling?

Kudu tables must have a unique primary key. Kudu has not been tested with
columns containing large values (10s of KB and higher) and performance problems
when using large values are anticipated. See
[Schema Design](http://kudu.apache.org/docs/schema_design.html).

#### Can Kudu be used to replace Lambda Architectures?

In many cases Kudu's combination of real-time and analytic performance will
allow the complexity inherent to Lambda architectures to be simplified through
the use of a single storage engine.

#### Is there a way to force the order of execution of a list statement? (ie force an update on table A after a previous insert on table B)?

When using the Kudu API, users can choose to perform synchronous operations.
If a sequence of synchronous operations is made, Kudu guarantees that timestamps
are assigned in a corresponding order.

#### Should I use Kudu for OLTP-type workloads? How does Kudu relate to Spanner from an OLTP standpoint?

Kudu is inspired by Spanner in that it uses a consensus-based replication design and
timestamps for consistency control, but the on-disk layout is pretty different.

Kudu was designed and optimized for OLAP workloads and lacks features such as multi-row
transactions and secondary indexing typically needed to support OLTP.

As a true column store, Kudu is not as efficient for OLTP as a row store would be. There are also
currently some implementation issues that hurt Kudu's performance on Zipfian distribution
updates (see the YCSB results in the performance evaluation of our [draft paper](kudu.pdf).

We anticipate that future releases will continue to improve performance for these workloads,
but Kudu is not designed to be a full replacement for OLTP stores for all workloads. Please
consider other storage engines such as Apache HBase or a traditional RDBMS.

### Indexes

#### Can multi-column indexes be created?

Kudu supports compound primary keys. Secondary indexes, compound or not, are not
currently supported.

#### Does Kudu support secondary indexes?

No, Kudu does not support secondary indexes. Random access is only possible through the
primary key. For analytic drill-down queries, Kudu has very fast single-column scans which
allow it to produce sub-second results when querying across billions of rows on small
clusters.

#### Are index updates maintained automatically?

Kudu's primary key is automatically maintained. Secondary indexes, manually or
automatically maintained, are not currently supported.

#### Is there a concept like partitioning keys like with Cassandra (primary and secondary index concepts)?

Kudu's primary key can be either simple (a single column) or compound
(multiple columns). Within any tablet, rows are written in the sort order of the
primary key. In the case of a compound key, sorting is determined by the order
that the columns in the key are declared. For hash-based distribution, a hash of
the entire key is used to determine the "bucket" that values will be placed in.

With either type of partitioning, it is possible to partition based on only a
subset of the primary key column. For example, a primary key of "(host, timestamp)"
could be range-partitioned on only the timestamp column.

#### Does Kudu have relational features like autoincrement column, PK/FK constraints, or built-in indexes?

Kudu tables have a primary key that is used for uniqueness as well as providing
quick access to individual rows. Auto-incrementing columns, foreign key constraints,
and secondary indexes are not currently supported, but could be added in subsequent
Kudu releases.


### Transactions

#### Does Kudu support multi-row transactions?

No, Kudu does not support multi-row transactions at this time. However, single row
operations are atomic within that row.

#### Does Kudu offer ACID compliance?

Kudu is designed to eventually be fully ACID compliant. However, multi-row
transactions are not yet implemented. The single-row transaction guarantees it
currently provides are very similar to HBase.

#### Is a rollback concept supported?

Kudu does not currently support transaction rollback.
