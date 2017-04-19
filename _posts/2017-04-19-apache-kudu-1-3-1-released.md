---
layout: post
title: Apache Kudu 1.3.1 released
author: Todd Lipcon
---

The Apache Kudu team is happy to announce the release of Kudu 1.3.1!

Apache Kudu 1.3.1 is a bug fix release which fixes critical issues discovered
in Apache Kudu 1.3.0. In particular, this fixes a bug in which data could be
incorrectly deleted after certain sequences of node failures. Several other
bugs are also fixed. See the release notes for details.

Users of Kudu 1.3.0 are encouraged to upgrade to 1.3.1 immediately.

* Download the [Kudu 1.3.1 source release](/releases/1.3.1/)
* Convenience binary artifacts for the Java client and various Java
integrations (eg Spark, Flume) are also now available via the ASF Maven
repository.
