---
layout: post
title: Apache Kudu (incubating) Weekly Update March 28, 2016
author: Todd Lipcon
---
Welcome to the second edition of the Kudu Weekly Update. As with last week's
inaugural post, we'll cover ongoing development and news in the Apache Kudu
project on a weekly basis.

<!--more-->

If you find this post useful, please let us know by emailing the
[kudu-user mailing list](mailto:user@kudu.incubator.apache.org) or
tweeting at [@ApacheKudu](https://twitter.com/ApacheKudu). Similarly, if you're
aware of some Kudu news we missed, let us know so we can cover it in
a future post.

## Development discussions and code in progress

* Binglin Chang's patch for [per-tablet write quotas](http://gerrit.cloudera.org:8080/#/c/2327/),
  described in last week's post, was committed this week. This new experimental feature will be
  available in the upcoming 0.8.0 release.

* Development discussion around improving the stability of the high-availability
  Kudu master continued on the [design doc review](http://gerrit.cloudera.org:8080/#/c/2527/).

* Dan Burkert has been working on a [design doc](http://gerrit.cloudera.org:8080/#/c/2443/)
  for a "scan tokens" API as described in the [KUDU-1312](https://issues.apache.org/jira/browse/KUDU-1312)
  JIRA. He also posted a preliminary patch series for the
  [http://gerrit.cloudera.org:8080/#/c/2592/](Java implementation) of the API.

  This work will make it easier to integrate query and execution engines such as
  Spark, MapReduce, and Drill with Kudu. In particular, the API aims to support features
  such as partition pruning based on Kudu's distribution schemas. See the design document
  for more details.

* Ara Ebrahimi has been working on a Kudu sink for Flume. This will make it easier for
  users of Flume to build streaming ingest pipelines into Kudu tables. The
  [code review](http://gerrit.cloudera.org:8080/#/c/2600/) is making steady progress
  with reviews by Mike Percy and Harsh J.

* Jean-Daniel Cryans was the release manager for the past couple of releases and is
  acting as RM again for the upcoming 0.8.0 release. This week he reminded developers
  that [the 0.8.0 train will soon be departing the station](http://markmail.org/thread/syfg6icqhfgxlcvi).
  JD is trying to follow the "train" release model: if a feature is ready when the release's
  time comes, it will be included. Otherwise, it will have to wait for the next release
  to depart.

  The thread hasn't generated much discussion, as it's just a reminder of the schedule
  that JD proposed a few months back. However, the focus this coming week will likely
  be on fixing blockers and getting the last bits and pieces of in-flight features
  in before a release candidate is cut.

## Upcoming talks and meetups

* This week, O'Reilly and Cloudera will be hosting the Strata/Hadoop World
in San Jose. The conference will feature two talks on Kudu:

1. Thu, Mar 31, 2016. [Strata Hadoop World](http://conferences.oreilly.com/strata/hadoop-big-data-ca/public/schedule/detail/47055). San Jose, CA, USA.<br/>
   _Fast data made easy with Apache Kafka and Apache Kudu (incubating)_. Presented by Ted Malaska and Jeff Holoman.
1. Wed, Mar 30, 2016. [Strata Hadoop World](http://conferences.oreilly.com/strata/hadoop-big-data-ca/public/schedule/detail/46916). San Jose, CA, USA.<br/>
   _Hadoop's storage gap: Resolving transactional-access and analytic-performance tradeoffs with Apache Kudu (incubating)_. Presented by Todd Lipcon.

Datanami has posted a [preview of Todd Lipcon's talk](http://www.datanami.com/2016/03/28/strata-preview-resolving-hadoops-storage-gap/).
