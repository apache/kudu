---
layout: post
title: Apache Kudu 1.13.0 released
author: Attila Bukor
---

The Apache Kudu team is happy to announce the release of Kudu 1.13.0!

The new release adds several new features and improvements, including the
following:

<!--more-->

- Added table ownership support. All newly created tables are automatically
  owned by the user creating them. It is also possible to change the owner by
  altering the table. You can also assign privileges to table owners via Apache
  Ranger.
- An experimental feature is added to Kudu that allows it to automatically
  rebalance tablet replicas among tablet servers. The background task can be
  enabled by setting the `--auto_rebalancing_enabled` flag on the Kudu masters.
  Before starting auto-rebalancing on an existing cluster, the CLI rebalancer
  tool should be run first.
- Bloom filter column predicate pushdown has been added to allow optimized
  execution of filters which match on a set of column values with a
  false-positive rate. Support for Impala queries utilizing Bloom filter
  predicate is available yielding performance improvements of 19% to 30% in TPC-H
  benchmarks and around 41% improvement for distributed joins across large
  tables. Support for Spark is not yet available.
- ARM-based architectures are now supported.

The above is just a list of the highlights, for a more complete list of new
features, improvements and fixes please refer to the [release
notes](/releases/1.13.0/docs/release_notes.html).

The Apache Kudu project only publishes source code releases. To build Kudu
1.13.0, follow these steps:

- Download the Kudu [1.13.0 source release](/releases/1.13.0)
- Follow the instructions in the documentation to build Kudu [1.13.0 from
  source](/releases/1.13.0/docs/installation.html#build_from_source)

For your convenience, binary JAR files for the Kudu Java client library, Spark
DataSource, Flume sink, and other Java integrations are published to the ASF
Maven repository and are [now
available](https://search.maven.org/search?q=g:org.apache.kudu%20AND%20v:1.13.0).

The Python client source is also available on
[PyPI](https://pypi.org/project/kudu-python/).

Additionally, experimental Docker images are published to
[Docker Hub](https://hub.docker.com/r/apache/kudu), including for AArch64-based
architectures (ARM).
