---
layout: post
title: Apache Kudu 1.9.0 Released
author: Andrew Wong
---

The Apache Kudu team is happy to announce the release of Kudu 1.9.0!

The new release adds several new features and improvements, including the
following:

<!--more-->

- Added support for location awareness for placement of tablet replicas.
- Introduced docker scripts to facilitate building and running Kudu on various
  operating systems.
- Introduced an experimental feature to allow users to run tests against a Kudu
  mini cluster without having to first locally build or install Kudu.
- Updated the compaction policy to favor reducing the number of rowsets, which
  can lead to significantly faster scans and bootup times in certain workloads.
- Multiple tooling enhancements have been made to improve visibility into Kudu
  tables.

The above is just a list of the highlights, for a more complete list of new
features, improvements and fixes please refer to the [release
notes](/releases/1.9.0/docs/release_notes.html).

The Apache Kudu project only publishes source code releases. To build Kudu
1.9.0, follow these steps:

- Download the Kudu [1.9.0 source release](/releases/1.9.0)
- Follow the instructions in the documentation to build Kudu [1.9.0 from
  source](/releases/1.9.0/docs/installation.html#build_from_source)

For your convenience, binary JAR files for the Kudu Java client library, Spark
DataSource, Flume sink, and other Java integrations are published to the ASF
Maven repository and are [now
available](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.kudu%22%20AND%20v%3A%221.9.0%22).

The Python client source is also available on
[PyPI](https://pypi.org/project/kudu-python/).
