---
layout: post
title: Apache Kudu 1.14.0 Released
author: Grant Henke
---

The Apache Kudu team is happy to announce the release of Kudu 1.14.0!

The new release adds several new features and improvements, including the
following:

<!--more-->

- Full support for `INSERT_IGNORE`, `UPDATE_IGNORE`, and `DELETE_IGNORE` operations
  was added. The `INSERT_IGNORE` operation will insert a row if one matching the key
  does not exist and ignore the operation if one already exists. The `UPDATE_IGNORE`
  operation will update the row if one matching the key exists and ignore the operation
  if one does not exist. The `DELETE_IGNORE` operation will delete the row if one matching
  the key exists and ignore the operation if one does not exist. These operations are
  particularly useful in situations where retries or duplicate operations could occur and
  you do not want to handle the errors that could result manually or you do not want to cause
  unnecessary writes and compaction work as a result of using the `UPSERT` operation.
  The Java client can check if the cluster it is communicating with supports these operations
  by calling the `supportsIgnoreOperations()` method on the KuduClient. See
  link:https://issues.apache.org/jira/browse/KUDU-1563[KUDU-1563] for more details.

- Spark 3 compatible JARs compiled for Scala 2.12 are now published for the Kudu Spark integration.
  See link:https://issues.apache.org/jira/browse/KUDU-3202[KUDU-3202] for more details.

- Every Kudu cluster now has an automatically generated cluster Id that can be used to uniquely
  identify a cluster. The cluster Id is shown in the masters web-UI, the `kudu master list` tool,
  and in master server logs. See link:https://issues.apache.org/jira/browse/KUDU-2574[KUDU-2574]
  for more details.

- Downloading the WAL data and data blocks when copying tablets to another tablet server is now
  parallelized, resulting in much faster tablet copy operations. These operations occur when
  recovering from a down tablet server or when running the cluster rebalancer. See
  link:https://issues.apache.org/jira/browse/KUDU-1728[KUDU-1728] and
  link:https://issues.apache.org/jira/browse/KUDU-3214[KUDU-3214] for more details.

- The HMS integration now supports multiple Kudu clusters associated with a single HMS
  including Kudu clusters that do not have HMS synchronization enabled. This is possible,
  because the Kudu master will now leverage the cluster Id to ignore notifications from
  tables in a different cluster. Additionally, the HMS plugin will check if the Kudu cluster
  associated with a table has HMS synchronization enabled.
  See link:https://issues.apache.org/jira/browse/KUDU-3192[KUDU-3192] and
  link:https://issues.apache.org/jira/browse/KUDU-3187[KUDU-3187] for more details.

- DeltaMemStores will now be flushed as long as any DMS in a tablet is older than the point
  defined by `--flush_threshold_secs`, rather than flushing once every `--flush_threshold_secs`
  period. This can reduce memory pressure under update- or delete-heavy workloads, and lower tablet
  server restart times following such workloads. See
  link:https://issues.apache.org/jira/browse/KUDU-3195[KUDU-3195] for more details.

The above is just a list of the highlights, for a more complete list of new
features, improvements and fixes please refer to the [release
notes](/releases/1.14.0/docs/release_notes.html).

The Apache Kudu project only publishes source code releases. To build Kudu
1.14.0, follow these steps:

- Download the Kudu [1.14.0 source release](/releases/1.14.0)
- Follow the instructions in the documentation to build Kudu [1.14.0 from
  source](/releases/1.14.0/docs/installation.html#build_from_source)

For your convenience, binary JAR files for the Kudu Java client library, Spark
DataSource, Flume sink, and other Java integrations are published to the ASF
Maven repository and are [now
available](https://search.maven.org/search?q=g:org.apache.kudu%20AND%20v:1.14.0).

The Python client source is also available on
[PyPI](https://pypi.org/project/kudu-python/).

Additionally, experimental Docker images are published to
[Docker Hub](https://hub.docker.com/r/apache/kudu), including for AArch64-based
architectures (ARM).
