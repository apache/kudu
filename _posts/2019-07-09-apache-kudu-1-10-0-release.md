---
layout: post
title: Apache Kudu 1.10.0 Released
author: Grant Henke
---

The Apache Kudu team is happy to announce the release of Kudu 1.10.0!

The new release adds several new features and improvements, including the
following:

<!--more-->

- Kudu now supports both full and incremental table backups via a job
  implemented using Apache Spark. Additionally it supports restoring
  tables from full and incremental backups via a restore job implemented using
  Apache Spark. See the
  [backup documentation](/releases/1.10.0/docs/administration.html#backup)
  for more details.
- Kudu can now synchronize its internal catalog with the Apache Hive Metastore,
  automatically updating Hive Metastore table entries upon table creation,
  deletion, and alterations in Kudu. See the
  [HMS synchronization documentation](/releases/1.10.0/docs/hive_metastore.html#metadata_sync)
  for more details.
- Kudu now supports native fine-grained authorization via integration with
  Apache Sentry. Kudu may now enforce access control policies defined for Kudu
  tables and columns, as well as policies defined on Hive servers and databases
  that may store Kudu tables. See the
  [authorization documentation](/releases/1.10.0/docs/security.html#fine_grained_authz)
  for more details.
- Kudu’s web UI now supports SPNEGO, a protocol for securing HTTP requests with
  Kerberos by passing negotiation through HTTP headers. To enable, set the
  `--webserver_require_spnego` command line flag.
- Column comments can now be stored in Kudu tables, and can be updated using
  the AlterTable API
  (see [KUDU-1711](https://issues.apache.org/jira/browse/KUDU-1711)).
- The performance of mutations (i.e. UPDATE, DELETE, and re-INSERT) to
  not-yet-flushed Kudu data has been significantly optimized
  (see [KUDU-2826]https://issues.apache.org/jira/browse/KUDU-2826) and
  [f9f9526d3](https://github.com/apache/kudu/commit/f9f9526d3)).
- Predicate performance for primitive columns and IS NULL and IS NOT NULL
  has been optimized
  (see [KUDU-2846](https://issues.apache.org/jira/browse/KUDU-2846)).

The above is just a list of the highlights, for a more complete list of new
features, improvements and fixes please refer to the [release
notes](/releases/1.10.0/docs/release_notes.html).

The Apache Kudu project only publishes source code releases. To build Kudu
1.10.0, follow these steps:

- Download the Kudu [1.10.0 source release](/releases/1.10.0)
- Follow the instructions in the documentation to build Kudu [1.10.0 from
  source](/releases/1.10.0/docs/installation.html#build_from_source)

For your convenience, binary JAR files for the Kudu Java client library, Spark
DataSource, Flume sink, and other Java integrations are published to the ASF
Maven repository and are [now
available](https://search.maven.org/search?q=g:org.apache.kudu%20AND%20v:1.10.0).

The Python client source is also available on
[PyPI](https://pypi.org/project/kudu-python/).

Additionally experimental Docker images are published to
[Docker Hub](https://hub.docker.com/r/apache/kudu).
