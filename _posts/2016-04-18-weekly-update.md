---
layout: post
title: Apache Kudu (incubating) Weekly Update April 18, 2016
author: Todd Lipcon
---
Welcome to the fifth edition of the Kudu Weekly Update. This weekly blog post
covers ongoing development and news in the Apache Kudu (incubating) project.

<!--more-->

If you find this post useful, please let us know by emailing the
[kudu-user mailing list](mailto:user@kudu.incubator.apache.org) or
tweeting at [@ApacheKudu](https://twitter.com/ApacheKudu). Similarly, if you're
aware of some Kudu news we missed, let us know so we can cover it in
a future post.

## Project news

* Cloudera announced that it has posted [binary packages](http://markmail.org/thread/tghwcux5k4qvcsep)
  for the recent 0.8.0 release. These are not official packages from
  the Apache Kudu (incubating) project, but users who prefer not to
  build from source may find them convenient.

* Jean-Daniel Cryans has volunteered to continue to act as release manager for
  the 0.x release line, and has start a [discussion](http://mail-archives.apache.org/mod_mbox/incubator-kudu-dev/201604.mbox/%3CCAGpTDNcfTOcp%2Beb39h5j%3DoxttZNhOBZ7v%2B%2B6hxRtWCh3t_psbQ%40mail.gmail.com%3E)
  detailing what features and improvements he expects will be ready
  for an 0.9 release in June.

## Development discussions and code in progress

* Chris George posted a [work in progress patch](http://gerrit.cloudera.org:8080/#/c/2754/)
  for a native Kudu RDD implementation for Spark. Kudu already ships an RDD
  based on the generic HadoopRDD and Kudu's MapReduce integration, but Chris's
  new version paves the way for new features like pushing down predicates.


* Todd Lipcon has been working on [KUDU-1410](https://issues.apache.org/jira/browse/KUDU-1410),
  a small project which makes it easier to diagnose performance issues on a Kudu
  cluster.

  The first feature proposed by this JIRA is the idea of collecting
  "exemplar" traces: for each type of RPC (e.g. _Write_, _Scan_, etc.)
  the RPC system will collect a few _exemplar_ RPCs in different
  latency buckets and retain their traces.  This makes it easier for
  an operator to see what might have caused a slow response from a
  server even after the request has been finished for some time.

  The second new feature is the collection of per-RPC-request metrics
  such as lock acquisition time, time spent waiting on disk, and other
  metrics specific to each type of RPC. In combination with the
  exemplar trace feature above, this should make it easy to root-cause
  whether a request is slow due to underlying hardware issues,
  Kudu-specific issues, or a particular workload characteristic.

  Todd posted a work-in-progress implementation of these features on gerrit
  in a five-part patch series:
  [(1)](http://gerrit.cloudera.org:8080/#/c/2794/)
  [(2)](http://gerrit.cloudera.org:8080/#/c/2795/)
  [(3)](http://gerrit.cloudera.org:8080/#/c/2796/)
  [(4)](http://gerrit.cloudera.org:8080/#/c/2797/)
  [(5)](http://gerrit.cloudera.org:8080/#/c/2798/)

* Dan Burkert continued working on the [Java implementation of the Scan Token API](http://gerrit.cloudera.org:8080/#/c/2592/)
  described in previous weekly updates, with reviews this week from Jean-Daniel
  Cryans and Adar Dembo. He also posted a patch for the [C++ implementation](http://gerrit.cloudera.org:8080/#/c/2757/)
  which has seen some review action as well.

* Dan also posted a [design document for non-covering range partitioning](http://gerrit.cloudera.org:8080/#/c/2772/).
  This new feature will allow Kudu operators to add or drop tablets to
  an existing range-partitioned table. This is very important for time
  series use cases where new partitions may need to be added daily,
  and old partitions potentially dropped in order to achieve a
  "sliding window" table. Read the design document for more details on
  use cases and the expected semantics.

## On the Kudu blog

* Pat Patterson wrote a post about [Ingesting JSON Data into Apache Kudu with StreamSets
  Data Collector](http://getkudu.io/2016/04/14/ingesting-json-apache-kudu-streamsets-data-collector.html).

