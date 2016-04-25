---
layout: post
title: Apache Kudu (incubating) Weekly Update April 25, 2016
author: Todd Lipcon
---
Welcome to the sixth edition of the Kudu Weekly Update. This weekly blog post
covers ongoing development and news in the Apache Kudu (incubating) project.

<!--more-->

If you find this post useful, please let us know by emailing the
[kudu-user mailing list](mailto:user@kudu.incubator.apache.org) or
tweeting at [@ApacheKudu](https://twitter.com/ApacheKudu). Similarly, if you're
aware of some Kudu news we missed, let us know so we can cover it in
a future post.

## Development discussions and code in progress

* Chris George continued to iterate on his
  [improved Spark DataSource implementation for Kudu](http://gerrit.cloudera.org:8080/#/c/2848/).
  Chris reports that basic functionality like pushing down predicates
  is now working properly, and the main work remaining is around
  writing automated tests.

* Dan Burkert finished the implementation of the Scan Token API described
  in previous weeks' blog posts. Both
  [Java](http://gerrit.cloudera.org:8080/#/c/2592/) and
  [C++](http://gerrit.cloudera.org:8080/#/c/2757/) implementations
  were committed this past week, and will be available in the upcoming
  0.9.0 release.

* Todd Lipcon committed a five-patch series implementing many of the
  ideas listed in [KUDU-1410](https://issues.apache.org/jira/browse/KUDU-1410).
  This new set of improvements will make it easier for operators and
  developers to diagnose performance issues and timeouts in Kudu clusters.

  Look out for an upcoming blog post about this new feature.

* Mike Percy has spent the last couple of weeks working on
  [KUDU-1377](https://issues.apache.org/jira/browse/KUDU-1377), a subtle
  issue where various types of disk drive errors or system
  crashes could cause the Kudu tablet server to be unable to properly
  recover. This week he committed the
  [final patch](http://gerrit.cloudera.org:8080/#/c/2595/) in this series
  which should prevent the issue in the future.

* For the last couple of months, Binglin Chang has been working on and off
  on a new [Get API](https://issues.apache.org/jira/browse/KUDU-1235) for
  Kudu. The purpose of this new API is to provide an optimized path for
  looking up a single row.

  Binglin has been working with Todd on analyzing where CPU time is spent
  in these code paths, and in initial prototypes has achieved a significant
  speedup on single-server tests: up to around 90K random reads per
  second compared to a starting point of around 35K with the current
  Scan API.

* Currently,  Kudu provides the ability to read at any arbitrary point in the past.
  Some would consider this a feature, and others would consider it a bug --
  namely, Kudu never reclaims space from deleted rows.

  Mike Percy posted an initial [design document](http://gerrit.cloudera.org:8080/#/c/2853/)
  for garbage collection deleted rows and past versions of updated rows.

* Dan Burkert started working on the implementation of the
  [non-covering range partitions](http://gerrit.cloudera.org:8080/#/c/2772/)
  feature that was first mentioned last week. A
  [first patch](http://gerrit.cloudera.org:8080/#/c/2806/) starts implementing
  the master side of the feature.

* Zhen Zhang posted an initial patch for [KUDU-1415](https://issues.apache.org/jira/browse/KUDU-1415),
  a new feature that proposes to collect basic operation statistics in the Java client.
  This would include things such as the number of operations, number of bytes read and
  written, etc. Jean-Daniel Cryans has already provided a first pass review.


## On the Kudu blog

* Dan Burkert wrote a post about [improvements to predicate handling in
  Kudu 0.8](http://getkudu.io/2016/04/19/kudu-0-8-0-predicate-improvements.html).
