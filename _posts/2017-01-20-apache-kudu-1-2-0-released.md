---
layout: post
title: Apache Kudu 1.2.0 released
author: Todd Lipcon
---

The Apache Kudu team is happy to announce the release of Kudu 1.2.0!

The new release adds several new features and improvements, including:

<!--more-->

- User data such as row contents is now redacted from logging statements.
- Kudu's ability to provide strong consistency guarantees has been substantially improved.
- Various performance improvements in metadata management as well as optimizations for BITSHUFFLE encoding on AVX2-capable hosts.

Additionally, 1.2.0 fixes a number of important bugs, including:

- Kudu now automatically limits its usage of file descriptors, preventing crashes due to ulimit exhaustion.
- Fixed a long-standing issue which could cause ext4 file system corruption on RHEL 6.
- Fixed a disk space leak.
- Several fixes for correctness in various edge cases.

The above list of changes is non-exhaustive. Please refer to the
[release notes](/releases/1.2.0/docs/release_notes.html)
for an expanded list of important improvements, bug fixes, and
incompatible changes before upgrading.

* Download the [Kudu 1.2.0 source release](/releases/1.2.0/)
* Convenience binary artifacts for the Java client and various Java
integrations (eg Spark, Flume) are also now available via the ASF Maven
repository.
