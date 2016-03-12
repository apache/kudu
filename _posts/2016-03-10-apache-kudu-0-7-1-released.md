---
layout: post
title: Apache Kudu (incubating) 0.7.1 released
author: Jean-Daniel Cryans
---
The Apache Kudu (incubating) team is happy to announce the release of Kudu
0.7.1!

This latest release fixes several bugs found during and after the release
of 0.7.0. Special thanks are due to several community users who reported
bugs fixed in this release:
<!--more-->

* Chris George, for reporting a critical bug in the Java client preventing it from scanning large partitioned tables with single scanner object instances. (KUDU-1325)
* Bruce Song Zhang, for running cluster stress tests which identified a potential data corruption in certain random access write workloads involving frequent row deletions (KUDU-1341)
* Nick Wolf, for reporting a bug where Kudu's internal clock could jump backwards causing a server to crash and fail to restart. (KUDU-1345)

Thanks again to Jean-Daniel Cryans for managing this release!

* Read the detailed [Kudu 0.7.1 release notes](http://getkudu.io/releases/0.7.1/docs/release_notes.html)
* Download the [Kudu 0.7.1 source release](http://getkudu.io/releases/0.7.1/)
