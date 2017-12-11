---
layout: post
title: Apache Kudu 1.6.0 released
author: Mike Percy
---

The Apache Kudu team is happy to announce the release of Kudu 1.6.0!

Apache Kudu 1.6.0 is a minor release that offers new features, performance
optimizations, incremental improvements, and bug fixes.

Release highlights:

<!--more-->

1. Kudu servers can now tolerate short interruptions in NTP clock
   synchronization. NTP synchronization is still required when any Kudu daemon
   starts up.
2. Tablet servers will no longer crash when a disk containing data blocks
   fails, unless that disk also stores WAL segments or tablet metadata. Instead
   of crashing, the tablet server will shut down any tablets that may have lost
   data locally and Kudu will re-replicate the affected tablets to another
   tablet server. More information can be found in the documentation under
   [Recovering from Disk Failure](/releases/1.6.0/docs/administration.html#disk_failure_recovery).
3. Tablet server startup time has been improved significantly on servers
   containing large numbers of blocks.
4. The Spark DataSource integration now can take advantage of scan locality for
   better scan performance. The scan will take place at the closest replica
   instead of going to the leader.
5. Support for Spark 1 has been removed in Kudu 1.6.0 and now only Spark 2 is
   supported. Spark 1 support was deprecated in Kudu 1.5.0.
6. HybridTime timestamp propagation now works in the Java client when using
   scan tokens.
7. Tablet servers now consider the health of all replicas of a tablet before
   deciding to evict one. This can improve the stability of the Kudu cluster
   when multiple servers temporarily go down at the same time.
8. A bug in the C++ client was fixed that could cause tablets to be erroneously
   pruned, or skipped, during certain scans, resulting in fewer results than
   expected being returned from queries. The bug only affected tables whose
   range partition columns are a proper prefix of the primary key.
   See [KUDU-2173](https://issues.apache.org/jira/browse/KUDU-2173) for more
   information.

For more details, and the complete list of changes in Kudu 1.6.0, please see
the [Kudu 1.6.0 release notes](/releases/1.6.0/docs/release_notes.html).

The Apache Kudu project only publishes source code releases. To build Kudu
1.6.0, follow these steps:

1. Download the [Kudu 1.6.0 source release](/releases/1.6.0/).
2. Follow the instructions in the documentation to
   [build Kudu 1.6.0 from source](/releases/1.6.0/docs/installation.html#build_from_source).

For your convenience, binary JAR files for the Kudu Java client library, Spark
DataSource, Flume sink, and other Java integrations are published to the ASF
Maven repository and are
[now available](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.kudu%22%20AND%20v%3A%221.6.0%22).

