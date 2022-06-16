---
layout: post
title: Apache Kudu 1.16.0 Released
author: Attila Bukor
---

The Apache Kudu team is happy to announce the release of Kudu 1.16.0!

The new release adds several new features and improvements, including the
following:

<!--more-->

* Kudu has a new `/startup` page on the web UI where admins can easily track
  startup progress of a Kudu server.

* It is now possible to change the replication of an existing table, and to
  require a minimum replication factor across a Kudu cluster.

The above is just a list of the highlights, for a more complete list of new
features, improvements and fixes please refer to the [release
notes](/releases/1.16.0/docs/release_notes.html).

The Apache Kudu project only publishes source code releases. To build Kudu
1.16.0, follow these steps:

- Download the Kudu [1.16.0 source release](/releases/1.16.0)
- Follow the instructions in the documentation to build Kudu [1.16.0 from
  source](/releases/1.16.0/docs/installation.html#build_from_source)

For your convenience, binary JAR files for the Kudu Java client library, Spark
DataSource, Flume sink, and other Java integrations are published to the ASF
Maven repository and are [now
available](https://search.maven.org/search?q=g:org.apache.kudu%20AND%20v:1.16.0).

The Python client source is also available on
[PyPI](https://pypi.org/project/kudu-python/).

Additionally, experimental Docker images are published to
[Docker Hub](https://hub.docker.com/r/apache/kudu).
