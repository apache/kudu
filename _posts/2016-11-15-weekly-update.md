---
layout: post
title: Apache Kudu Weekly Update November 15th, 2016
author: David Alves
---
Welcome to the twenty-third edition of the Kudu Weekly Update. This weekly blog post
covers ongoing development and news in the Apache Kudu project.

<!--more-->

## Project news

* The first release candidate for Kudu 1.1.0 is [now available](http://mail-archives.apache.org/mod_mbox/kudu-dev/201611.mbox/%3CCADY20s7ZKZkPmUEcTexW%3D%2B_%2BLnDY2hABZg0-UZD3jvWAs9-pog%40mail.gmail.com%3E).

  Noteworthy new features/improvements:
  : ^
  * The Python client has been brought to feature parity with the C++ and Java clients.
  * IN LIST predicates.
  * Java client now features client-side tracing.
  * Kudu now publishes jar files for Spark 2.0 compiled with Scala 2.11.
  * Kudu's Raft implementation now features pre-elections. In our tests this has greatly improved stability.

  Community developers and users are encouraged to download the source
  tarball and vote on the release.

  For more information on what's new, check out the
  [release notes](https://github.com/apache/kudu/blob/branch-1.1.x/docs/release_notes.adoc).
  *Note:* some links from these in-progress release notes will not be live until the
  release itself is published.

* On November 7th, the Kudu PMC announced that Jordan Birdsell, from State Farm, had been voted
  in as a new committer and PMC member.

  Jordan's contributions include extensive work on the python client, throwing it some much needed
  love, and bringing it to feature parity with the other clients.

  Besides his extensive code contributions Jordan has also been active in reviewing other
  developer's patches and helping the community in general, on slack and other channels.

  Jordan has been doing great work and the Kudu PMC was pleased to recognize his contributions
  with committership.

* Mike Percy will be presenting Kudu Wednesday 16th November at [Apache Big Data Europe, in Seville](https://apachebigdataeu2016.sched.org/).

* Congratulations to Haijie Hong for his [first contribution to Kudu!](https://gerrit.cloudera.org/#/c/4822/).
  Haijie fixed some edge cases in BitWriter that were blocking RLE usage for 64 bit types.

* Congratulations to Maxim Smyatkin for his [first contributions to Kudu!](https://gerrit.cloudera.org/#/q/Maxim).
  Maxim has contributed several patches helping with debug and cleanup.

## Development discussions and code in progress

* A lot of progress has been done towards the goals that were set in the scope docs introduced in
  the last couple of posts. Specifically:

  * Dan Burkert, Todd Lipcon and Alexey Serbin have doubled down on the security effort. They have
    been working on enabling Kerberos authentication and rpc encryption. The [security scope doc](https://docs.google.com/document/d/1cPNDTpVkIUo676RlszpTF1gHZ8l0TdbB7zFBAuOuYUw/edit#heading=h.gsibhnd5dyem)
    has been updated with the latest plans for security and many patches have been merged already.

  * David Alves has continued the work on [consistency](https://s.apache.org/7VCo). Up for review
    and partially pushed is a patch series to address row history loss if a row is deleted and then
    re-inserted. Also in progress is work to make sure that scans at a snapshot from followers
    always return same data as if they were executed on the leader. This helps with Read-Your-Writes
    when reading from lagging replicas.

  * Adar Dembo has been making good progress [addressing issues seen with the LogBlockManager](https://s.apache.org/uOOt).
    A series of patches have been merged with various fixes to block managers in general and to the
    log block manager in particular.

  * Dinesh Bhat has been working on improving the manual recovery tools for Kudu. Namely, he has
    added a tool to force a remote replica copy to a destination server, and a tool to delete a
    local replica of a tablet. The latter is useful when a tablet cannot come up due to bad state.

  * Jean-Daniel Cryans has implemented RPC tracing for the java client, greatly improving
    debuggability. JD also has added ReplicaSelection to the java client, allowing to perform
    scans on replicas other than the leader, which should be of great help for load-balancing.

  * Besides the feature parity contributions, Jordan Birdsell has laid out a
    [roadmap for Python client work](http://mail-archives.apache.org/mod_mbox/kudu-dev/201611.mbox/%3CCAGaaj_VKfB4mhu6eExHCWo0%3D6Qd0HFWy7bg9e39JgOaFPGJ1nQ%40mail.gmail.com%3E)
    for the 1.2 release. Feedback from other Python client users is certainly appreciated.

Want to learn more about a specific topic from this blog post? Shoot an email to the
[kudu-user mailing list](mailto:user@kudu.apache.org) or
tweet at [@ApacheKudu](https://twitter.com/ApacheKudu). Similarly, if you're
aware of some Kudu news we missed, let us know so we can cover it in
a future post.
