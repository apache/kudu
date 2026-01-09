---
layout: post
title: Apache Kudu 1.18.1 Released
author: Abhishek Chennaka
---

The Apache Kudu team is happy to announce the release of Kudu 1.18.1!

This maintenance release fixes critical bugs and packaging issues discovered in Apache Kudu
 1.18.0. Users of Kudu 1.18.0 are encouraged to upgrade to 1.18.1 as soon as possible.

This release includes the following incremental updates since Apache Kudu 1.18.0:
<!--more-->

* Improved Java and Spark ecosystem reliability through fixes to dependency publishing, Spark
  class packaging, and data source discovery.

* Enhanced TLS support for the embedded webserver, including configurable minimum TLS versions
  up to TLS 1.3 and control over TLSv1.3 cipher suites.

* Upgraded Apache Spark support to Spark 3.5.

* Increased stability of the CLI tools and the C++ client library, fixing crash in the `kudu` CLI
  binary upon exiting and crash in the C++ client library with verbose logging enabled. The
  latter was a regression introduced in the 1.18.0 release.

* Improved compatibility with newer system libraries, including OpenSSL 3.4+ and platforms with
  64KB memory pages.

The above is just a list of the highlights, for a more complete list of new
features, improvements and fixes please refer to the [release
notes](/releases/1.18.1/docs/release_notes.html).

The Apache Kudu project only publishes source code releases. To build Kudu
1.18.1, follow these steps:

- Download the Kudu [1.18.1 source release](/releases/1.18.1)
- Follow the instructions in the documentation to build Kudu [1.18.1 from
  source](/releases/1.18.1/docs/installation.html#build_from_source)

For your convenience, binary JAR files for the Kudu Java client library, Spark
DataSource, and other Java integrations are published to the ASF
Maven repository and are [now
available](https://search.maven.org/search?q=g:org.apache.kudu%20AND%20v:1.18.1).

The Python client source is also available on
[PyPI](https://pypi.org/project/kudu-python/).

Additionally, experimental Docker images are published to
[Docker Hub](https://hub.docker.com/r/apache/kudu).
