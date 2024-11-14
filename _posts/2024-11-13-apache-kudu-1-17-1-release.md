---
layout: post
title: Apache Kudu 1.17.1 Released
author: Abhishek Chennaka
---

The Apache Kudu team is happy to announce the release of Kudu 1.17.1!

This maintenance release fixes critical bugs and build issues discovered in Apache
Kudu 1.17.0. Users of Kudu 1.17.0 are encouraged to upgrade to 1.17.1 as soon as
possible.

The changes include the following:

<!--more-->

* Upgraded several thirdparty components either to address CVEs or fix build
  issues discovered with contemporary releases.

* Fixed file descriptor leak in encryption-at-rest
  (see https://issues.apache.org/jira/browse/KUDU-3520[KUDU-3520]).

* Fixed bug in range-aware tablet replica placement causing Kudu master to crash
  (see https://issues.apache.org/jira/browse/KUDU-3532[KUDU-3532]).

The above is just a few of the fixes, for a more complete list of  improvements and
fixes please refer to the [release notes](/releases/1.17.1/docs/release_notes.html).

The Apache Kudu project only publishes source code releases. To build Kudu
1.17.1, follow these steps:

- Download the Kudu [1.17.1 source release](/releases/1.17.1)
- Follow the instructions in the documentation to build Kudu [1.17.1 from
  source](/releases/1.17.1/docs/installation.html#build_from_source)

For your convenience, binary JAR files for the Kudu Java client library, Spark
DataSource, Flume sink, and other Java integrations are published to the ASF
Maven repository and are [now
available](https://search.maven.org/search?q=g:org.apache.kudu%20AND%20v:1.17.1).

The Python client source is also available on
[PyPI](https://pypi.org/project/kudu-python/).

Additionally, experimental Docker images are published to
[Docker Hub](https://hub.docker.com/r/apache/kudu).
