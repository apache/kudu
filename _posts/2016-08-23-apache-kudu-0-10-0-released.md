---
layout: post
title: Apache Kudu 0.10.0 released
author: Todd Lipcon
---

The Apache Kudu team is happy to announce the release of Kudu 0.10.0!

This latest version adds several new features, including:
<!--more-->

- Users may now manually manage the partitioning of a range-partitioned table
  by adding or dropping range partitions after a table has been created. This
  can be particularly helpful for time-series workloads. Dan Burkert posted
  an [in-depth blog](/2016/08/23/new-range-partitioning-features.html) today
  detailing the new feature.

- Multi-master (HA) Kudu clusters are now significantly more stable.

- Administrators may now reserve a certain amount of disk space on each of its
  configured data directories.

- Kudu's integration with Spark has been substantially improved and is much
  more flexible.

This release also includes many bug fixes and other improvements, detailed in
the release notes below.

* Read the detailed [Kudu 0.10.0 release notes](http://kudu.apache.org/releases/0.10.0/docs/release_notes.html)
* Download the [Kudu 0.10.0 source release](http://kudu.apache.org/releases/0.9.0/)
