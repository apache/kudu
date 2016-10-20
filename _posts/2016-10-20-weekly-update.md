---
layout: post
title: Apache Kudu Weekly Update October 20th, 2016
author: Todd Lipcon
---
Welcome to the twenty-second edition of the Kudu Weekly Update. This weekly blog post
covers ongoing development and news in the Apache Kudu project.

<!--more-->

## Project news

* Kudu 1.0.1 was [released](http://mail-archives.apache.org/mod_mbox/kudu-user/201610.mbox/%3CCALo2W-UgTa%2BX15_q_9FQpRUPWN53eyqFS10C5MXK1KpsFgqcyQ%40mail.gmail.com%3E)
  on October 11th. This is a bug fix release which fixes several bugs found
  in 1.0.0. See the [Kudu 1.0.1 release notes](http://kudu.apache.org/releases/1.0.1/docs/release_notes.html)
  for more details.

* Todd Lipcon has proposed a [release plan](https://lists.apache.org/thread.html/4c94d313e28381bb107682ffaf43adfd38bd7fb3b03c98e3c86c52e2@%3Cdev.kudu.apache.org%3E)
  for the next few months. The proposal is to have a 1.1 release in mid-November and
  a 1.2 release in mid-January. These would be time-based releases rather than
  gated on any particular feature scope; however, it's anticipated that several
  new features and improvements will be ready in time for these releases.

* Happy fourth birthday to the Kudu project! The initial commit was made
  on October 11th, 2012! Since then we've had 4888 more commits by 60
  authors!

## Development discussions and code in progress

* As mentioned last week, a lot of contributors have been collaborating on
  design documents for upcoming work. Here's the complete list of in-flight
  documents, along with the primary authors of these docs:
  * [Security features](https://docs.google.com/document/d/1cPNDTpVkIUo676RlszpTF1gHZ8l0TdbB7zFBAuOuYUw/edit#heading=h.gsibhnd5dyem) (Todd Lipcon)
  * [Improved disk-failure handling](https://goo.gl/wP5BJb) (Dinesh Bhat)
  * [Tools for manual recovery from corruption](https://s.apache.org/7K48) (Mike Percy and Dinesh Bhat)
  * [Addressing issues seen with the LogBlockManager](https://s.apache.org/uOOt) (Adar Dembo)
  * [Providing proper snapshot/serializable consistency](https://s.apache.org/7VCo) (David Alves)
  * [Improving re-replication of under-replicated tablets](https://s.apache.org/ARUP) (Mike Percy)
  * [Avoiding Raft election storms](https://docs.google.com/document/d/1066W63e2YUTNnecmfRwgAHghBPnL1Pte_gJYAaZ_Bjo/edit) (Todd Lipcon)
  * [Backup and bulk load](https://s.apache.org/kudu-backup-scope) (Dan Burkert)
  * [Improving diagnosability of client errors](https://s.apache.org/SM6V) (Alexey Serbin)

  In many cases, work is now progressing on implementation of these ideas,
  but these are considered living documents. It's not too late to add your
  comments or volunteer to help out.

* JD Cryans has been working on cleaning up the Java client. Several complex pieces
  of code were completely removed, and other parts were refactored into new
  standalone classes for better modularity. Along the way, JD also
  [reduced lock contention](http://gerrit.cloudera.org:8080/4706) on a frequently-accessed
  data structure.

* Todd Lipcon implemented and committed Raft "pre-elections" as described in the
  [election storm mitigation design document]((https://docs.google.com/document/d/1066W63e2YUTNnecmfRwgAHghBPnL1Pte_gJYAaZ_Bjo/edit).
  Initial experiments, detailed in the document, indicate that this will substantially
  improve leader stability on clusters with overloaded disks and lots of tablets.

  Following this patch, Todd worked on some cleanup and refactor of the Consensus
  implementation, removing a bunch of dead code and splitting some classes up
  into smaller pieces. This is preparing for some improvements in locking
  granularity also described in the same document.

* Dan Burkert and Todd Lipcon have started submitting patches to integrate Kerberos
  authentication with Kudu's RPC system. Dan posted a
  [patch](https://gerrit.cloudera.org/#/c/4752/) which adds "MiniKDC", some test
  infrastructure for starting and stopping a standalone Kerberos service in
  the context of a test. Todd worked on adding
  [support for Kerberos authentication](https://gerrit.cloudera.org/#/c/4763/)
  during RPC negotiation.

  These patches are just the beginning of the security work, but form an important
  base to build on top of. The design uses Kerberos both as a mechanism to authenticate
  clients as well as a way to mutually authenticate tablet servers with the master.


Want to learn more about a specific topic from this blog post? Shoot an email to the
[kudu-user mailing list](mailto:user@kudu.apache.org) or
tweet at [@ApacheKudu](https://twitter.com/ApacheKudu). Similarly, if you're
aware of some Kudu news we missed, let us know so we can cover it in
a future post.
