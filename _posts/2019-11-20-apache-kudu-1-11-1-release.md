---
layout: post
title: Apache Kudu 1.11.1 released
author: Alexey Serbin
---

The Apache Kudu team is happy to announce the release of Kudu 1.11.1!

This release contains a fix which addresses a critical issue discovered in
1.10.0 and 1.11.0 and adds several new features and improvements since 1.10.0.

<!--more-->

Apache Kudu 1.11.1 is a bug fix release which fixes critical issues discovered
in Apache Kudu 1.11.0. In particular, this release fixes a licensing issue with
distributing libnuma library with the kudu-binary JAR artifact. Users of
Kudu 1.11.0 are encouraged to upgrade to 1.11.1 as soon as possible.

Apache Kudu 1.11.1 adds several new features and improvements since
Apache Kudu 1.10.0, including the following:
- Kudu now supports putting tablet servers into maintenance mode: while in this
  mode, the tablet server’s replicas will not be re-replicated if the server
  fails. Administrative CLI are added to orchestrate tablet server maintenance
  (see [KUDU-2069](https://issues.apache.org/jira/browse/KUDU-2069)).
- Kudu now has a built-in NTP client which maintains the internal wallclock
  time used for generation of HybridTime timestamps. When enabled, system clock
  synchronization for nodes running Kudu is no longer necessary,
  (see [KUDU-2935](https://issues.apache.org/jira/browse/KUDU-2935)).
- Aggregated table statistics are now available to Kudu clients. This allows
  for various query optimizations. For example, Spark now uses it to perform
  join optimizations
  (see [KUDU-2797](https://issues.apache.org/jira/browse/KUDU-2797) and
  [KUDU-2721](https://issues.apache.org/jira/browse/KUDU-2721)).
- The kudu CLI tool now supports creating and dropping range partitions
  for a table
  (see [KUDU-2881](https://issues.apache.org/jira/browse/KUDU-2881)).
- The kudu CLI tool now supports altering and dropping table columns.
- The kudu CLI tool now supports getting and setting extra configuration
  properties for a table
  (see [KUDU-2514](https://issues.apache.org/jira/browse/KUDU-2514)).

See the [release notes](/releases/1.11.1/docs/release_notes.html) for details.

The Apache Kudu project only publishes source code releases. To build Kudu
1.11.1, follow these steps:

- Download the Kudu [1.11.1 source release](/releases/1.11.1)
- Follow the instructions in the documentation to build Kudu [1.11.1 from
  source](/releases/1.11.1/docs/installation.html#build_from_source)

For your convenience, binary JAR files for the Kudu Java client library, Spark
DataSource, Flume sink, and other Java integrations are published to the ASF
Maven repository and are [now
available](https://search.maven.org/search?q=g:org.apache.kudu%20AND%20v:1.11.1).

The Python client source is also available on
[PyPI](https://pypi.org/project/kudu-python/).

Additionally experimental Docker images are published to
[Docker Hub](https://hub.docker.com/r/apache/kudu).
