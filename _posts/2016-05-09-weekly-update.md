---
layout: post
title: Apache Kudu (incubating) Weekly Update May 9, 2016
author: Jean-Daniel Cryans
---
Welcome to the eighth edition of the Kudu Weekly Update. This weekly blog post
covers ongoing development and news in the Apache Kudu (incubating) project.

<!--more-->

If you find this post useful, please let us know by emailing the
[kudu-user mailing list](mailto:user@kudu.incubator.apache.org) or
tweeting at [@ApacheKudu](https://twitter.com/ApacheKudu). Similarly, if you're
aware of some Kudu news we missed, let us know so we can cover it in
a future post.

## Development discussions and code in progress

* Sameer Abhyankar posted a [patch](http://gerrit.cloudera.org:8080/#/c/2986/)
  for [KUDU-1363](https://issues.apache.org/jira/browse/KUDU-1363)
  that adds the ability to specify
  [IN](http://www.w3schools.com/sql/sql_in.asp)-like predicates on column values.

* Chris George and Andy Grove have both been adding new features in Kudu's
  Spark module such as methods to [create/delete tables](http://gerrit.cloudera.org:8080/#/c/2981/)
  and [insert/update rows in the DataSource](http://gerrit.cloudera.org:8080/#/c/2992/).

* Todd Lipcon [fixed a bug](https://issues.apache.org/jira/browse/KUDU-1437) in RLE
  encoding that was reported by Przemyslaw Maciolek. Thank you Przemyslaw for
  reporting it and providing an easy way to reproduce it!

* Adar Dembo is currently working on addressing the
  [issues with multi-master](https://github.com/cloudera/kudu/blob/master/docs/design-docs/multi-master-1.0.md)
  and early last week he got [a](http://gerrit.cloudera.org:8080/2879)
  [few](http://gerrit.cloudera.org:8080/2928) [patches](http://gerrit.cloudera.org:8080/2891)
  in that address some race conditions.

* Zhen Zhang got [a first contribution](http://gerrit.cloudera.org:8080/#/c/2858/)
  in with a patch that adds statistics in the Java client. In 0.9.0 it will be
  possible to query the client to get things like the number of bytes written or
  how many write operations were sent.

## Upcoming talks and meetups

* Dan Burkert and Mike Percy will present Kudu at the
  [Vancouver Spark Meetup](http://www.meetup.com/Vancouver-Spark/events/229692936/) in Vancouver, BC,
  on May 10.
