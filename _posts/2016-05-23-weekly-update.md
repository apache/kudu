---
layout: post
title: Apache Kudu (incubating) Weekly Update May 23, 2016
author: Todd Lipcon
---
Welcome to the tenth edition of the Kudu Weekly Update. This weekly blog post
covers ongoing development and news in the Apache Kudu (incubating) project.

<!--more-->

If you find this post useful, please let us know by emailing the
[kudu-user mailing list](mailto:user@kudu.incubator.apache.org) or
tweeting at [@ApacheKudu](https://twitter.com/ApacheKudu). Similarly, if you're
aware of some Kudu news we missed, let us know so we can cover it in
a future post.

## Kudu related podcast

* Two committers, Mike Percy and Dan Burkert, appeared on the
  [IBM New Builders podcast](https://developer.ibm.com/tv/apachecon-apache-projects/)
  to talk about Apache Kudu, how they got involved, and what sort of
  workloads it is best suited for.

## Development discussions and code in progress

* Jean-Daniel Cryans is again acting as the release manager for the upcoming
  0.9.0 release. The git branch for 0.9 has now been cut, and only bug fixes
  or small improvements will be committed to that branch between now and the
  first release candidate.

* Since Kudu's initial release, one of the most commonly requested features
  has been support for the `UPSERT` operation. `UPSERT`, known in some other
  databases as `INSERT ... ON DUPLICATE KEY UPDATE`. This operation has the
  semantics of an `INSERT` if no key already exists with the provided primary
  key. Otherwise, it replaces the existing row with the new values.

  This week, several developers collaborated to add support for this operation.
  Todd Lipcon implemented
  [support on the server side](http://gerrit.cloudera.org:8080/#/c/3101/),
  C++ client, and [Python client](http://gerrit.cloudera.org:8080/#/c/3128/).
  Jean-Daniel Cryans added support in the
  [Java client](http://gerrit.cloudera.org:8080/#/c/3123/). Ara Ebrahimi
  and Will Berkeley have started working on
  [integrating upsert support into the Flume sink](http://gerrit.cloudera.org:8080/#/c/3145/).

* Mike Percy started working on support for [basic disk
  space reservations](http://gerrit.cloudera.org:8080/#/c/3135/)
  in the tablet server. This feature will cause the tablet server to stop
  writing to a disk before it's full, preventing crashes due to running
  out of space.

* Chris George and Andy Grove collaborated on support for [insertions and
  updates in the Spark DataSource](http://gerrit.cloudera.org:8080/#/c/2992/),
  and the patch was committed towards the end of the week. Brent Gardner
  has also been helping with the Spark integration, and fixed an important
  [connection leak bug](https://issues.apache.org/jira/browse/KUDU-1453)
  in the initial implementation.

* David Alves worked on reviving a 7-month old patch by Jingkai Yuan which
  implements a [integer delta encoding scheme](http://gerrit.cloudera.org:8080/#/c/1210/)
  that is meant to be efficient both in terms of CPU and disk space. This
  encoding scheme is also designed to take advantage of modern CPU instruction sets
  such as AVX and AVX2.


## Upcoming talks and meetups

* Ryan Bosshart will be presenting Kudu at the [Dallas/Fort Worth
  Cloudera User Group](http://www.meetup.com/DFW-Cloudera-User-Group/events/230547045/).

