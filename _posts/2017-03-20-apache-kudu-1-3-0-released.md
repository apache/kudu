---
layout: post
title: Apache Kudu 1.3.0 released
author: Todd Lipcon
---

The Apache Kudu team is happy to announce the release of Kudu 1.3.0!

Apache Kudu 1.3 is a minor release which adds various new features,
improvements, bug fixes, and optimizations on top of Kudu
1.2. Highlights include:

<!--more-->

- significantly improved support for security, including Kerberos
  authentication, TLS encryption, and coarse-grained (cluster-level)
  authorization
- automatic garbage collection of historical versions of data
- lower space consumption and better performance in default
  configurations.

The above list of changes is non-exhaustive. Please refer to the
[release notes](/releases/1.3.0/docs/release_notes.html)
for an expanded list of important improvements, bug fixes, and
incompatible changes before upgrading.

Thanks to the 25 developers who contributed code or documentation to
this release!

* Download the [Kudu 1.3.0 source release](/releases/1.3.0/)
* Convenience binary artifacts for the Java client and various Java
integrations (eg Spark, Flume) are also now available via the ASF Maven
repository.
