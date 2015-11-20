---
title: Kudu FAQ
layout: single_col
active_nav: faq
single_col_extra_classes: faq
---

## Frequently Asked Questions
{:.no_toc}

1. TOC
{:toc}

### Is Kudu ready to be deployed into production yet?
We don't yet recommend this -- Kudu is currently in Beta. During the next few months we
might have to break backwards compatibility with our current on-the-wire or on-disk
formats or change public APIs. There is also still work to do to improve data durability
and consistency. Kudu is currently in a good state for using in a proof-of-concept
scenario for a future deployment.

For more details on the Kudu Beta period, see the
[release notes](docs/release_notes.html#_about_the_kudu_public_beta).

### When can I expect GA or version 1.0?

There is no set timeline for general availability (GA). We are looking forward to working
with the community to continue stabilizing and improving Kudu. We want to implement some
critical missing features such as security and fill in other gaps before releasing Kudu
1.0.

### Why build a new storage engine? Why not just improve Apache HBase to increase its scan speed?

Kudu shares some characteristics with HBase. Like HBase, it is a real-time store
that supports key-indexed record lookup and mutation.

However, Kudu's design differs from HBase in some fundamental ways:

* Kudu’s data model is more traditionally relational, while HBase is schemaless.
* Kudu’s on-disk representation is truly columnar and follows an entirely different
  storage design than HBase/BigTable.

Making these fundamental changes in HBase would require a massive redesign, as opposed
to a series of simple changes. HBase is the right design for many classes of
applications and use cases and will continue to be the best storage engine for those
workloads.

### How does Kudu store its data? Is the underlying data storage readable without going through Kudu?

Kudu accesses storage devices through the local filesystem, and works best with Ext4 or
XFS. Kudu handles striping across <abbr title="just a bunch of disks">JBOD</abbr> mount
points, and does not require <abbr title="redundant array of inexpensive disks">RAID</abbr>.
Kudu's write-ahead logs (WALs) can be stored on separate locations from the data files,
which means that WALs can be stored on <abbr title="solid state drives">SSDs</abbr> to
enable lower-latency writes on systems with both SSDs and magnetic disks.

Kudu's on-disk data format closely resembles Parquet, with a few differences to
support efficient random access as well as updates. The underlying data is not
directly queryable without using the Kudu client APIs. The Kudu team has worked hard
to ensure that Kudu’s scan performance is performant, and has focused on storing data
efficiently without making the trade-offs that would be required to allow direct access
to the data files.

### Why doesn’t Kudu store its data in HDFS?

We considered a design which stored data on HDFS, but decided to go in a different
direction, for the following reasons:

* Kudu handles replication at the logical level using Raft consensus, which makes
  HDFS replication redundant. We could have mandated a replication level of 1, but
  that is not HDFS's best use case.
* Filesystem-level snapshots provided by HDFS do not directly translate to Kudu support for
  snapshots, because it is hard to predict when a given piece of data will be flushed
  from memory. In addition, snapshots only make sense if they are provided on a per-table
  level, which would be difficult to orchestrate through a filesystem-level snapshot.
* HDFS security doesn’t translate to table- or column-level ACLs. Similar to HBase
  ACLs, Kudu would need to implement its own security system and would not get much
  benefit from the HDFS security model.
* Kudu’s scan performance is already within the same ballpark as Parquet files stored
  on HDFS, so there’s no need to accomodate reading Kudu’s data files directly.

### Can I colocate Kudu with HDFS on the same servers?

Kudu can be colocated with HDFS on the same data disk mount points. This is similar
to colocating Hadoop and HBase workloads. Kudu has been extensively tested
in this type of configuration, with no stability issues. For latency-sensitive workloads,
consider dedicating an SSD to Kudu's WAL files.

### Does Kudu support multi-row transactions?

No, Kudu does not support multi-row transactions at this time. However, single row
operations are atomic within that row.

### Does Kudu support secondary indexes?

No, Kudu does not support secondary indexes. Random access is only possible through the
primary key. For analytic drill-down queries, Kudu has very fast single-column scans which
allow it to produce sub-second results when querying across billions of rows on small
clusters.

### Can a Kudu deployment be geo-distributed?

We don't recommend geo-distributing tablet servers this time because of the possibility
of higher write latencies. In addition, Kudu is not currently aware of data placement.
This could lead to a situation where the master might try to put all replicas
in the same datacenter. We plan to implement the necessary features for geo-distribution
in a future release.

### Is Kudu an in-memory database?

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

### Where is the Kudu shell?

Kudu doesn’t yet have a command-line shell. If the Kudu-compatible version of Impala is
installed on your cluster then you can use it as a replacement for a shell. See also the
docs for the [Kudu Impala Integration](docs/kudu_impala_integration.html).

### Is Kudu open source?

Yes, Kudu is open source and licensed under the Apache Software License, version 2.0. The
plan is to donate the project to the Apache Software Foundation for incubation. However,
there is currently no committed timeline for this.

### Why was Kudu developed internally at Cloudera before its release?

We believe strongly in the value of open source for the long-term sustainable
development of a project. We also believe that it is easier to work with a small
group of colocated developers when a project is very young. Being in the same
organization allowed us to move quickly during the initial design and development
of the system.

Now that Kudu is public, we look forward to working with a larger community during
its next phase of development.

### Is the Kudu Master a bottleneck?

Although the Master is not sharded, it is not expected to become a bottleneck for
the following reasons.

* Like many other systems, the master is not on the hot path once the tablet
  locations are cached.
* The Kudu master process is extremely efficient at keeping everything in memory.
  In our testing on an 80-node cluster, the 99.99th percentile latency for getting
  tablet locations was on the order of hundreds of microseconds (not a typo).

### Should compactions be managed?

Compactions in Kudu are designed to be small and to always be running in the
background. They operate under a (configurable) budget to prevent tablet servers
from unexpectedly attempting to rewrite tens of GB of data at a time. Since compactions
are so predictable, the only tuning knob available is the number of threads dedicated
to flushes and compactions in the *maintenance manager*.

### How is security handled in Kudu?

Kudu has no security features at the moment, but we would like to have it ready for GA.

### Is Kudu a CP or AP system?

In the parlance of the CAP theorem, Kudu is a
<abbr title="consistent (but not available) under network partitions">CP</abbr>
type of storage engine. Writing to a tablet will be delayed if the server that hosts that
tablet's leader replica fails until a quorum of servers is able to elect a new leader and
acknowledge a given write request.

Kudu gains the following properties by using Raft consensus:

* Leader elections are fast. As soon as the leader misses 3 heartbeats (half a second each), the
  remaining followers will elect a new leader which will start accepting operations right away.
  This whole process usually takes less than 10 seconds.
* Follower replicas don’t allow writes, but they do allow reads when fully up-to-date data is not
  required. Thus, queries against historical data (even just a few minutes old) can be
  sent to any of the replicas. If that replica fails, the query can be sent to another
  replica immediately.

During Kudu's beta period, those properties maybe not be fully implemented and
may suffer from some deficiencies. See the answer to
"[Is Kudu's consistency level tunable?](#is-kudus-consistency-level-tunable)"
for more information.

### Is Kudu’s consistency level tunable?

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

### Where is Kudu’s Jepsen report?

No one has yet run [Jepsen](https://github.com/aphyr/jepsen) on Kudu, but it
would be a very welcome contribution. However, we know of
[several issues](docs/transaction_semantics.html#known_issues)
that need to be resolved before Kudu can pass a Jepsen serializable consistency
test. We are committed to passing Jepsen, as well as other consistency-focused stress
tests, before releasing Kudu 1.0.

### What are Kudu’s runtime dependencies?

Kudu itself doesn’t have any service dependencies and can run on a cluster without Hadoop,
Impala, Spark, or any other project.

If you want to use Impala, note that Impala depends on Hive’s metadata server, which has
its own dependencies on Hadoop. It is not currently possible to have a pure Kudu+Impala
deployment.

### What’s the most efficient way to bulk load data into Kudu?

The easiest way to load data into Kudu is if the data is already managed by Impala.
In this case, a simple `INSERT INTO TABLE some_kudu_table SELECT * FROM some_csv_table`
does the trick.

You can also use Kudu’s MapReduce OutputFormat to load data from HDFS, HBase, or
any other data store that has an InputFormat.

No tool is provided to load data directly into Kudu’s on-disk data format. We
have found that for many workloads, the insert performance of Kudu is comparable
to bulk load performance of other systems.

### Should I use Kudu for OLTP-type workloads? How does Kudu relate to Spanner from an OLTP standpoint?

Kudu is inspired by Spanner in that it uses a consensus-based replication design and
timestamps for consistency control, but the on-disk layout is pretty different.

Kudu was designed and optimized for OLAP workloads and lacks features such as multi-row
transactions and secondary indexing typically needed to support OLTP.

As a true column store, Kudu is not as efficient for OLTP as a row store would be. There are also
currently some implementation issues that hurt Kudu's performance on Zipfian distribution
updates (see the YCSB results in the performance evaluation of our [draft paper](kudu.pdf).

Our intention with the Kudu beta is that if you have a _light_ OLTP-like workload,
it should work, but if you have mostly random accesses something like HBase is probably better.

### How can I back up my Kudu data?

Kudu doesn’t yet have a built-in backup mechanism. Similar to bulk loading data,
Impala can help if you have it available. You can use it to copy your data into
Parquet format using a statement like:

    INSERT INTO TABLE some_parquet_table SELECT * FROM kudu_table

then use [distcp](http://hadoop.apache.org/docs/r1.2.1/distcp2.html)
to copy the Parquet data to another cluster. While Kudu is in beta, we’re not
expecting people to deploy mission-critical workloads on it yet.

### What operating systems does Kudu support?

Linux is required to run Kudu. See the
[installation guide](docs/installation.html#_prerequisites_and_requirements)
for details. Work is underway to add
[support for Kudu on OSX](https://issues.cloudera.org/browse/KUDU-1185)
as a development platform. The Java client can be used on any JVM platform.
