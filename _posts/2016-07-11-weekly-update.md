---
layout: post
title: Apache Kudu (incubating) Weekly Update July 11, 2016
author: Jean-Daniel Cryans
---
Welcome to the sixteenth edition of the Kudu Weekly Update. This weekly blog post
covers ongoing development and news in the Apache Kudu (incubating) project.

<!--more-->

## Development discussions and code in progress

* Todd Lipcon [changed the default](https://gerrit.cloudera.org/#/c/3517/)
  bloom filter false positive (FP) ratio from 1% to 0.01%. The original value
  wasn't scientifically chosen, but testing with billions of rows on a 5
  node cluster showed a 2x insert throughput improvement at the cost of
  some more disk space.

* J-D Cryans has been fixing some recently introduced bugs in the Java client.
  For example, see [this patch](https://gerrit.cloudera.org/#/c/3541/) and
  [that one](https://gerrit.cloudera.org/#/c/3586/). Testability is a major
  concern right now in the Java client since triggering those issues requires
  a lot of time and data.

* Dan Burkert has been making progress on the non-convering range partitioned
  tables front. The Java client [now supports](https://gerrit.cloudera.org/#/c/3388/)
  such tables and Dan is currently implementing new functionality to add and remove
  tablets via simple APIs.

* David Alves is also making a lot of progress on the replay cache, this new component
  on the server-side which makes it possible for tablets to identify client write
  operations and enable exactly-once semantics. The main patch is up for review
  [here](https://gerrit.cloudera.org/#/c/3449/).

* Adar Dembo is working on addressing multi-master issues, as he explained in this
  [blog post](http://kudu.apache.org/2016/06/24/multi-master-1-0-0.html). He just put
  up for review patches that enable tablet servers to heartbeat to all masters. Part
  one is [available here](https://gerrit.cloudera.org/#/c/3609/).

* Misty prepared a document with J-D that contains instructions on how to release a
  new Kudu version. It is [up for review here](https://gerrit.cloudera.org/#/c/3614/)
  if you are curious or want to learn more about this process.

## Project news

* The [vote](https://s.apache.org/l6Tw) to graduate Kudu from the ASF's Incubator passed!
  The next step is for the ASF Board to vote on the resolution at their next meeting.

* There's [a discussion](http://mail-archives.apache.org/mod_mbox/incubator-kudu-dev/201607.mbox/%3CCAGpTDNesxH43C-Yt5fNwpEpAxfb2P62Xpdi8AqT8jfvjeqnu0w%40mail.gmail.com%3E)
  on the dev mailing list about having an intermediate release, called 0.10.0, before 1.0.0.
  The current issue is that the version in the code is current "1.0.0-SNAPSHOT" which doesn't
  leave room for another other release, but the bigger issue is that code is still churning a
  lot which doesn't make for a stable 1.0 release.

Want to learn more about a specific topic from this blog post? Shoot an email to the
[kudu-user mailing list](mailto:user@kudu.incubator.apache.org) or
tweet at [@ApacheKudu](https://twitter.com/ApacheKudu). Similarly, if you're
aware of some Kudu news we missed, let us know so we can cover it in
a future post.
