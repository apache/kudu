---
layout: post
title: Apache Kudu (incubating) Weekly Update July 18, 2016
author: Todd Lipcon
---
Welcome to the seventeenth edition of the Kudu Weekly Update. This weekly blog post
covers ongoing development and news in the Apache Kudu (incubating) project.

<!--more-->

## Development discussions and code in progress


* Dan Burkert has continued making progress on support for non-covering range partitioned
  tables. This past week, he posted a code review for
  [adding and dropping range partitions to the master](https://gerrit.cloudera.org/#/c/3648/)
  and another for [handling non-covering ranges in the C++ client](https://gerrit.cloudera.org/#/c/3581/).

* Adar Dembo continued working on addressing multi-master issues, as he explained in this
  [blog post](http://kudu.apache.org/2016/06/24/multi-master-1-0-0.html). This past week
  he worked on [tackling various race conditions](https://gerrit.cloudera.org/#/c/3550/)
  that were possible when master operations were submitted concurrent with a master leader election.

  Adar also posted patches for most of the remaining known server-side issues, including
  posting a comprehensive [stress test](https://gerrit.cloudera.org/#/c/3611/) which issues
  client DDL operations concurrent with triggering master crashes and associated failovers.

  As always, Adar's commit messages are instructive and fun reads for those interested in
  following along.

* As mentioned last week, David Alves has been making a lot of progress on the implementation
  of the replay cache. Many patches landed in master this week, including:

** [RPC system integration](https://gerrit.cloudera.org/#/c/3192/)
** [Integration with replicated writes](https://gerrit.cloudera.org/#/c/3449/)
** [Correctness/stress tests](https://gerrit.cloudera.org/#/c/3519/)

  Currently, this new feature is disabled by default, as the support for evicting elements
  from the cache is not yet complete. This last missing feature is now
  [up for review](https://gerrit.cloudera.org/#/c/3628/).

* Alexey Serbin has been working on adding Doxygen-based documentation for the public
  C++ API. This was originally [proposed on the mailing list](https://mail-archives.apache.org/mod_mbox/incubator-kudu-dev/201606.mbox/%3CCANbMB4wtMz=JKwgKMNPvkjWX3t9NxCeGt04NmL=SyESyzUMWJg@mail.gmail.com%3E)
 a couple of weeks ago, and last week, Alexey posted the
 [initial draft of the implementation](https://gerrit.cloudera.org/#/c/3619/).

## Project news

* The [discussion](http://mail-archives.apache.org/mod_mbox/incubator-kudu-dev/201607.mbox/%3CCAGpTDNesxH43C-Yt5fNwpEpAxfb2P62Xpdi8AqT8jfvjeqnu0w%40mail.gmail.com%3E)
  on the dev mailing list about having an intermediate release, called 0.10.0, before 1.0.0,
  has wound down. The consensus seems to be that the development team is in favor of this
  release. Accordingly, the version number in the master branch has been changed back to
  0.10.0-SNAPSHOT.

Want to learn more about a specific topic from this blog post? Shoot an email to the
[kudu-user mailing list](mailto:user@kudu.incubator.apache.org) or
tweet at [@ApacheKudu](https://twitter.com/ApacheKudu). Similarly, if you're
aware of some Kudu news we missed, let us know so we can cover it in
a future post.
