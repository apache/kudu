---
layout: post
title: Apache Kudu (incubating) Weekly Update April 4, 2016
author: Todd Lipcon
---
Welcome to the third edition of the Kudu Weekly Update. This weekly blog post
covers ongoing development and news in the Apache Kudu (incubating) project.

<!--more-->

If you find this post useful, please let us know by emailing the
[kudu-user mailing list](mailto:user@kudu.incubator.apache.org) or
tweeting at [@ApacheKudu](https://twitter.com/ApacheKudu). Similarly, if you're
aware of some Kudu news we missed, let us know so we can cover it in
a future post.

## Development discussions and code in progress

* The 0.8.0 release train is progressing nicely. Jean-Daniel Cryans posted a first
  release candidate [VOTE thread](http://mail-archives.apache.org/mod_mbox/incubator-kudu-dev/201604.mbox/%3CCAGpTDNfA-hsv6xkeNcvwBGarP1sri%2BvBYqYNt70YWeH44_QPSw%40mail.gmail.com%3E)
  on the dev mailing list. Please download the release candidate and give it a try.

* Ara Ebrahimi's implementation of a [Kudu sink for Apache Flume](https://github.com/apache/incubator-kudu/blob/master/java/kudu-flume-sink/)
  was completed this week. Mike Percy worked on some follow-up around documentation and a shaded
  _jar-with-dependencies_ for easier consumption.

* Mike also worked on and committed an [improvement to the Java API](http://gerrit.cloudera.org:8080/#/c/2640/)
  so that error results for failed write operations are easier to handle. Previously,
  clients had to perform string comparisons to know whether an insert failed due
  to something like a duplicate key constraint rather than some other less expected
  error.

* After some early cluster testing of the new implementation of scanner predicates,
  a few tricky bugs were discovered. The first was around proper handling of inequality
  predicates on floating point columns. The second involved handling predicates like
  'my_int8_col <= 127': for non-nullable columns, this predicate is a tautology
  and can be eliminated. However, for nullable columns, such a predicate is equivalent
  to 'my_int8_col IS NOT NULL'. In order to fix this, Dan Burkert added an internal
  [implementation of 'IS NOT NULL'](http://gerrit.cloudera.org:8080/#/c/2671/).

  These bugs also exposed a few gaps in test coverage around predicate handling. Dan
  added a couple thousand lines worth of new test coverage both from [C++](http://gerrit.cloudera.org:8080/2677)
  and [Java](http://gerrit.cloudera.org:8080/#/c/2591/).

  These exhaustive tests also identified a [gap in Kudu's handling of NaN
  float values](https://issues.apache.org/jira/browse/KUDU-1386). The team
  elected to leave this as a known issue for now, since usage of NaN is relatively
  rare.

* Todd Lipcon fixed a [bug in Kudu's implementation of Raft configuration
  change](http://gerrit.cloudera.org:8080/#/c/2483/).
  This bug could cause tablet replicas to become "stuck" after certain types of network
  partitions. The fix will be included in the upcoming 0.8 release.

* Mike Percy has been working on a [bug](http://gerrit.cloudera.org:8080/#/c/2595/)
  where tablet servers fail to start up after their write-ahead logs (WALs) have been
  truncated. This can happen on certain types of machine crashes, or if the disks
  fill up under a write workload.

* Todd Lipcon spent time this week testing Kudu on a small cluster with a 3TB
  TPC-H dataset. In particular, he was focusing on concurrent query workloads,
  including scenarios with multiple read-only users in addition to combining
  a query workload with a write workload. As a result, he identified a few
  issues around Kudu's handling of RPCs in overload conditions.

  In order to improve the behavior on the server side, Todd changed the RPC
  scheduling algorithm to use an [earliest-deadline-first](https://en.wikipedia.org/wiki/Earliest_deadline_first_scheduling)
  policy. This has the effect of preventing query timeouts: a query which
  is closest to experiencing a timeout will be scheduled with higher priority
  over those which have plenty of time left.

  In addition, this work identified a few bugs in the Kudu C++ client.
  In particular, in the case where the server was overloaded, the client
  would sometimes [incorrectly rewind to the start of the current
  tablet](http://gerrit.cloudera.org:8080/#/c/2654/) resulting in incorrect
  results. In other cases, the client would end up in a [tight loop sending
  RPCs to the master](http://gerrit.cloudera.org:8080/#/c/2709/). Fixes
  for both of these issues will be in the upcoming 0.8 release.


## Upcoming talks and meetups

* Todd Lipcon will be presenting an introductory Kudu talk at [DataEngConf](http://www.dataengconf.com/schedule/)
  on Friday, April 8th.
