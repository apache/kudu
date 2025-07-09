---
layout: post
title: Apache Kudu 1.18.0 Released
author: Abhishek Chennaka
---

The Apache Kudu team is happy to announce the release of Kudu 1.18.0!

The new release adds several new features and improvements, including the following:

<!--more-->

* **Segmented LRU Block Cache (Experimental):**
  Helps protect hot data from eviction during large scans.

* **Embedded RocksDB for Metadata (Experimental):**
  A new option to store metadata in embedded RocksDB for better scalability.

* **Auto-Incrementing Columns Enhancements:**
  These columns are now correctly restored on backup/restore and initialized on bootstrap.

* **Improved Metrics & Observability:**
  Added metrics for timeouts, tablet scans, RPC backlogs, and more as well as Prometheus
  support for tablet-level metrics.

* **Build and Platform Updates:**
  Kudu now supports **AArch64 Ubuntu/RedHat**, **Ubuntu24.04** and builds on **macOS Sonoma (Xcode 15)**.

* **Java & Python Client Improvements:**
  - Python: simplified installation, UPSERT IGNORE, soft-deletes, immutable columns
  - Java: configurable buffer size, better schema update handling

* **Security Enhancements:**
  - Improvements in JWT authentication
  - Dedicated SPNEGO keytab for embedded webserver
  - HTTP headers for enhanced security of the embedded webserver

* **Kudu CLI & Tooling Improvements:**
  Better error messages and handling in `kudu table copy`, `scan`, and others.

The above is just a list of the highlights, for a more complete list of new
features, improvements and fixes please refer to the [release
notes](/releases/1.18.0/docs/release_notes.html).

The Apache Kudu project only publishes source code releases. To build Kudu
1.18.0, follow these steps:

- Download the Kudu [1.18.0 source release](/releases/1.18.0)
- Follow the instructions in the documentation to build Kudu [1.18.0 from
  source](/releases/1.18.0/docs/installation.html#build_from_source)

For your convenience, binary JAR files for the Kudu Java client library, Spark
DataSource, and other Java integrations are published to the ASF
Maven repository and are [now
available](https://search.maven.org/search?q=g:org.apache.kudu%20AND%20v:1.18.0).

The Python client source is also available on
[PyPI](https://pypi.org/project/kudu-python/).

Additionally, experimental Docker images are published to
[Docker Hub](https://hub.docker.com/r/apache/kudu).
