---
layout: post
title: Apache Kudu 1.10.1 released
author: Alexey Serbin
---

The Apache Kudu team is happy to announce the release of Kudu 1.10.1!

Apache Kudu 1.10.1 is a bug fix release which fixes critical issues discovered
in Apache Kudu 1.10.0. In particular, this fixes a licensing issue with
distributing libnuma library with the kudu-binary JAR artifact. Users of
Kudu 1.10.0 are encouraged to upgrade to 1.10.1 as soon as possible. See the
[release notes](/releases/1.10.1/docs/release_notes.html) for details.

<!--more-->

The Apache Kudu project only publishes source code releases. To build Kudu
1.10.1, follow these steps:

- Download the Kudu [1.10.1 source release](/releases/1.10.1)
- Follow the instructions in the documentation to build Kudu [1.10.1 from
  source](/releases/1.10.1/docs/installation.html#build_from_source)

For your convenience, binary JAR files for the Kudu Java client library, Spark
DataSource, Flume sink, and other Java integrations are published to the ASF
Maven repository and are [now
available](https://search.maven.org/search?q=g:org.apache.kudu%20AND%20v:1.10.1).

The Python client source is also available on
[PyPI](https://pypi.org/project/kudu-python/).
