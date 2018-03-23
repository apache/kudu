---
layout: post
title: Apache Kudu 1.7.0 released
author: Grant Henke
---

The Apache Kudu team is happy to announce the release of Kudu 1.7.0!

Apache Kudu 1.7.0 is a minor release that offers new features, performance
optimizations, incremental improvements, and bug fixes.

Release highlights:

<!--more-->

1. Kudu now supports the decimal column type. The decimal type is a numeric
   data type with fixed scale and precision suitable for financial and other
   arithmetic calculations where the imprecise representation and rounding
   behavior of float and double make those types impractical. The decimal type
   is also useful for integers larger than int64 and cases with fractional values
   in a primary key. See [Decimal Type](/releases/1.7.0/docs/schema_design.html#decimal)
   for more details.
2. The strategy Kudu uses for automatically healing tablets which have lost a
   replica due to server or disk failures has been improved. The new re-replication
   strategy, or replica management scheme, first adds a replacement tablet replica
   before evicting the failed one.
3. A new scan read mode READ_YOUR_WRITES. Users can specify READ_YOUR_WRITES when
   creating a new scanner in C++, Java and Python clients. If this mode is used,
   the client will perform a read such that it follows all previously known writes
   and reads from this client. Reads in this mode ensure read-your-writes and
   read-your-reads session guarantees, while minimizing latency caused by waiting
   for outstanding write transactions to complete. Note that this is still an
   experimental feature which may be stabilized in future releases.
4. The tablet server web UI scans dashboard (/scans) has been improved with several
   new features, including: showing the most recently completed scans, a pseudo-SQL
   scan descriptor that concisely shows the selected columns and applied predicates,
   and more complete and better documented scan statistics.
5. Kudu daemons now expose a web page /stacks which dumps the current stack trace of
   every thread running in the server. This information can be helpful when diagnosing
   performance issues.
6. By default, each tablet replica will now stripe data blocks across 3 data directories
   instead of all data directories. This decreases the likelihood that any given tablet
   will be affected in the event of a single disk failure.
7. The Java client now uses a predefined prioritized list of TLS ciphers when
   establishing an encrypted connection to Kudu servers. This cipher list matches the
   list of ciphers preferred for server-to-server communication and ensures that the
   most efficient and secure ciphers are preferred. When the Kudu client is running on
   Java 8 or newer, this provides a substantial speed-up to read and write performance.
8. The performance of inserting rows containing many string or binary columns has been
   improved, especially in the case of highly concurrent write workloads.
9. The Java client will now automatically attempt to re-acquire Kerberos credentials
   from the ticket cache when the prior credentials are about to expire. This allows
   client instances to persist longer than the expiration time of a single Kerberos
   ticket so long as some other process renews the credentials in the ticket cache.

For more details, and the complete list of changes in Kudu 1.7.0, please see
the [Kudu 1.7.0 release notes](/releases/1.7.0/docs/release_notes.html).

The Apache Kudu project only publishes source code releases. To build Kudu
1.7.0, follow these steps:

1. Download the [Kudu 1.7.0 source release](/releases/1.7.0/).
2. Follow the instructions in the documentation to
   [build Kudu 1.7.0 from source](/releases/1.7.0/docs/installation.html#build_from_source).

For your convenience, binary JAR files for the Kudu Java client library, Spark
DataSource, Flume sink, and other Java integrations are published to the ASF
Maven repository and are
[now available](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.kudu%22%20AND%20v%3A%221.7.0%22).

