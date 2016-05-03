---
layout: post
title: Apache Kudu (incubating) Weekly Update May 3, 2016
author: Todd Lipcon
---
Welcome to the seventh edition of the Kudu Weekly Update. This weekly blog post
covers ongoing development and news in the Apache Kudu (incubating) project.

<!--more-->

If you find this post useful, please let us know by emailing the
[kudu-user mailing list](mailto:user@kudu.incubator.apache.org) or
tweeting at [@ApacheKudu](https://twitter.com/ApacheKudu). Similarly, if you're
aware of some Kudu news we missed, let us know so we can cover it in
a future post.

## Development discussions and code in progress

* Chris George completed his
  [improved Spark DataSource implementation for Kudu](http://gerrit.cloudera.org:8080/#/c/2848/)
  and the new feature will be available in the upcoming 0.9.0 release.
  The improved DataSource supports features such as predicate pushdown
  and Chris reports that it is able to perform subsecond queries on
  millions of rows of data.

* As mentioned last week, Binglin Chang has been working on and off
  on a new [Get API](https://issues.apache.org/jira/browse/KUDU-1235) for
  Kudu. The purpose of this new API is to provide an optimized path for
  looking up a single row.

  In some initial benchmarking of the feature, it became apparent that Kudu's
  RPC mechanism was a scalability bottleneck on servers with 12 or more CPU
  cores. With some more prototype optimizations of the RPC system, Binglin
  has been able to push approximately 220K random reads per second on a single
  server.

  It may be a few more weeks before this work progresses from prototype stage
  to completion, but initial results are looking quite promising.

* Discussion continued on the design document for the upcoming
  [http://gerrit.cloudera.org:8080/#/c/2642/](Replay Cache) feature. This feature
  was previously introduced in an [earlier weekly update
  post](http://getkudu.io/2016/04/11/weekly-update.html)
  and development is now fully under way.

## On the Kudu blog

* Todd Lipcon wrote a post about [benchmarking Kudu insert performance with
  YCSB](http://getkudu.io/2016/04/26/ycsb.html). This post was quite popular,
  so Todd is currently working on a follow-up which will include more experiments
  around Kudu configuration tuning.

## Upcoming talks and meetups

* ApacheCon Big Data will be next week in Vancouver. As always, you can check
  the [Kudu Community page](http://getkudu.io/community.html) for an up-to-date
  list of conferenace sessions and meetups near you.
