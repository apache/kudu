---
layout: post
title: Apache Kudu 1.4.0 released
author: Todd Lipcon
---

The Apache Kudu team is happy to announce the release of Kudu 1.4.0!

Apache Kudu 1.4.0 is a minor release which offers several new features,
improvements, optimizations, and bug fixes.

Highlights include:

<!--more-->

- ability to alter storage attributes and default values for existing columns
- a new C++ client API to efficiently map primary keys to their associated partitions
  and hosts
- support for long-running fault-tolerant scans in the Java client
- a new `kudu fs check` command which can perform offline consistency checks
  and repairs on the local on-disk storage of a Tablet Server or Master.
- many optimizations to reduce disk space usage, improve write throughput,
  and improve throughput of background maintenance operations.

The above list of changes is non-exhaustive. Please refer to the
[release notes](/releases/1.4.0/docs/release_notes.html)
for an expanded list of important improvements, bug fixes, and
incompatible changes before upgrading.

* Download the [Kudu 1.4.0 source release](/releases/1.4.0/)
* Convenience binary artifacts for the Java client and various Java
integrations (eg Spark, Flume) are also now available via the ASF Maven
repository.
