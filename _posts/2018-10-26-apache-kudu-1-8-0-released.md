---
layout: post
title: Apache Kudu 1.8.0 Released
author: Attila Bukor
---

The Apache Kudu team is happy to announce the release of Kudu 1.8.0!

The new release adds several new features and improvements, including the
following:

<!--more-->

- Introduced manual data rebalancer tool which can be used to redistribute
  table replicas among tablet servers
- Added support for `IS NULL` and `IS NOT NULL` predicates to the Kudu Python
  client
- Multiple tooling improvements make diagnostics and troubleshooting simpler
- The Kudu Spark connector now supports Spark Streaming DataFrames
- Added Pandas support to the Python client

The above is just a list of the highlights, for a more complete list of new
features, improvements and fixes please refer to the [release
notes](/releases/1.8.0/docs/release_notes.html).

The Apache Kudu project only publishes source code releases. To build Kudu
1.8.0, follow these steps:

- Download the Kudu [1.8.0 source release](/releases/1.8.0)
- Follow the instructions in the documentation to build Kudu [1.8.0 from
  source](/releases/1.8.0/docs/installation.html#build_from_source)

For your convenience, binary JAR files for the Kudu Java client library, Spark
DataSource, Flume sink, and other Java integrations are published to the ASF
Maven repository and are [now
available](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.kudu%22%20AND%20v%3A%221.8.0%22).

The Python client source is also available on
[PyPI](https://pypi.org/project/kudu-python/).
