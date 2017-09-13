---
layout: post
title: Apache Kudu 1.5.0 released
author: Dan Burkert
---

The Apache Kudu team is happy to announce the release of Kudu 1.5.0!

Apache Kudu 1.5.0 is a minor release which offers several new features,
improvements, optimizations, and bug fixes.

Highlights include:

<!--more-->

- optimizations to improve write throughput and failover recovery times
- the Raft consensus implementation has been made more resilient and flexible
  through "tombstoned voting", which allows Kudu to self-heal in more edge-case
  scenarios
- the number of threads used by Kudu servers has been further reduced, with
  additional reductions planned for the future
- a new configuration dashboard on the web UI which provides a high-level
  summary of important configuration values
- a new `kudu tablet move` command which moves a tablet replica from one tablet
  server to another
- a new `kudu local_replica data_size` command which summarizes the space usage
  of a local tablet
- all on-disk data is now checksummed by default, which provides error detection
  for improved confidence when running Kudu on unreliable hardware

The above list of changes is non-exhaustive. Please refer to the
[release notes](/releases/1.5.0/docs/release_notes.html)
for an expanded list of important improvements, bug fixes, and
incompatible changes before upgrading.

* Download the [Kudu 1.5.0 source release](/releases/1.5.0/)
* Convenience binary artifacts for the Java client and various Java
integrations (eg Spark, Flume) are also now available via the ASF Maven
repository.
