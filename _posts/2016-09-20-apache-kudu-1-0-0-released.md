---
layout: post
title: Apache Kudu 1.0.0 released
author: Todd Lipcon
---

The Apache Kudu team is happy to announce the release of Kudu 1.0.0!

This latest version adds several new features, including:

<!--more-->

- Removal of multiversion concurrency control (MVCC) history is now supported.
This allows Kudu to reclaim disk space, where previously Kudu would keep a full
history of all changes made to a given table since the beginning of time.

- Most of Kudu’s command line tools have been consolidated under a new
top-level `kudu` tool. This reduces the number of large binaries distributed
with Kudu and also includes much-improved help output.

- Administrative tools including `kudu cluster ksck` now support running
against multi-master Kudu clusters.

- The C++ client API now supports writing data in `AUTO_FLUSH_BACKGROUND` mode.
This can provide higher throughput for ingest workloads.

This release also includes many bug fixes, optimizations, and other
improvements, detailed in the [release notes](/releases/1.0.0/docs/release_notes.html).

* Download the [Kudu 1.0.0 source release](/releases/1.0.0/)
* Convenience binary artifacts for the Java client and various Java
integrations (eg Spark, Flume) are also now available via the ASF Maven
repository.
