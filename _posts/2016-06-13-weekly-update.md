---
layout: post
title: Apache Kudu (incubating) Weekly Update June 13, 2016
author: Jean-Daniel Cryans
---
Welcome to the thirteenth edition of the Kudu Weekly Update. This weekly blog post
covers ongoing development and news in the Apache Kudu (incubating) project.

<!--more-->

If you find this post useful, please let us know by emailing the
[kudu-user mailing list](mailto:user@kudu.incubator.apache.org) or
tweeting at [@ApacheKudu](https://twitter.com/ApacheKudu). Similarly, if you're
aware of some Kudu news we missed, let us know so we can cover it in
a future post.

## Development discussions and code in progress

* The IPMC vote for 0.9.0 RC1 passed and Kudu 0.9.0 is now
  [officially released](http://kudu.apache.org/2016/06/10/apache-kudu-0-9-0-released.html). Per the
  lazily agreed-upon [plan](http://mail-archives.apache.org/mod_mbox/kudu-dev/201602.mbox/%3CCAGpTDNcMBWwX8p+yGKzHfL2xcmKTScU-rhLcQFSns1UVSbrXhw@mail.gmail.com%3E),
  the next release will be 1.0.0 in about two months.

* Adar Dembo has been cleaning up and improving the Master process's code. Last week he
  [finished](https://gerrit.cloudera.org/#/c/2887/) removing the per-tablet replica locations cache.

* Alexey Serbin contributed his first patch last week by [fixing](https://gerrit.cloudera.org/#/c/3360/)
  most of the unit tests that were failing on OSX.

* Sameer Abhyankar is nearly finished adding support for "in-list" predicates,
  follow [this link](https://gerrit.cloudera.org/#/c/2986/) to the gerrit
  review. This will enable specifying predicates in the style of "column IN (list, of, values)".

* Mike Percy posted a few patches that remove LocalConsensus for single-node tablets, with the actual
  removal happening in this [patch](https://gerrit.cloudera.org/#/c/3350/).

## Slides and recordings

* Todd Lipcon presented Kudu at Berlin Buzzwords earlier this month. The recording is available
  [here](https://berlinbuzzwords.de/session/apache-kudu-incubating-fast-analytics-fast-data).
