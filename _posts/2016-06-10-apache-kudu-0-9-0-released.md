---
layout: post
title: Apache Kudu (incubating) 0.9.0 released
author: Jean-Daniel Cryans
---
The Apache Kudu (incubating) team is happy to announce the release of Kudu
0.9.0!

This latest version adds basic UPSERT functionality and an improved Spark Data Source
that doesn't rely on the MapReduce I/O formats. It also improves Tablet Server
restart time as well as write performance under high load. Finally, Kudu now enforces
the specification of a partitioning scheme for new tables.

* Read the detailed [Kudu 0.9.0 release notes](http://getkudu.io/releases/0.9.0/docs/release_notes.html)
* Download the [Kudu 0.9.0 source release](http://getkudu.io/releases/0.9.0/)
