---
layout: post
title: Apache Kudu (incubating) Weekly Update June 1, 2016
author: Jean-Daniel Cryans
---
Welcome to the eleventh edition of the Kudu Weekly Update. This weekly blog post
covers ongoing development and news in the Apache Kudu (incubating) project.

<!--more-->

If you find this post useful, please let us know by emailing the
[kudu-user mailing list](mailto:user@kudu.incubator.apache.org) or
tweeting at [@ApacheKudu](https://twitter.com/ApacheKudu). Similarly, if you're
aware of some Kudu news we missed, let us know so we can cover it in
a future post.

## Development discussions and code in progress

* Jean-Daniel Cryans, the release manager for 0.9.0, [indicated](http://mail-archives.apache.org/mod_mbox/incubator-kudu-dev/201605.mbox/%3CCAGpTDNe_gV5TTsJQSjx_Q-hSGjK9TesWkyP-k9rnhd0mBtYAYg%40mail.gmail.com%3E)
  that the release is almost ready and the first release candidate will be put up for vote this
  week.

* Dan Burkert pushed [a change](http://gerrit.cloudera.org:8080/3131) that disallows default
  partitioning when creating a new table. This is due to many reports from users experiencing bad
  performance because their table was created with only one tablet. Kudu will now force users to
  partition their tables.

* Todd Lipcon ran YCSB stress tests on a cluster and discovered that compactions were taking hours
  instead of seconds. He pushed [a change](http://gerrit.cloudera.org:8080/#/c/3221/) that solves
  the issue as part of our [general effort](https://issues.apache.org/jira/browse/KUDU-749) to
  improve performance for zipfian update workloads.

* Todd also [changed](http://gerrit.cloudera.org:8080/#/c/3186/) some flush-related defaults to
  encourage parallel IO and larger flushes. This is based on his previous work that he documented
  in this [blog post](http://getkudu.io/2016/04/26/ycsb.html).

* Will Berkeley made a few improvements last week, but [one](http://gerrit.cloudera.org:8080/3199)
  we'd like to call out is that he removed the Java's kudu-mapreduce module dependency on Hadoop's
  hadoop-common test jar. This solved build issues while also removing a nasty dependency.