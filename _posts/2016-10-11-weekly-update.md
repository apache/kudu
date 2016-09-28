---
layout: post
title: Apache Kudu Weekly Update October 11th, 2016
author: Todd Lipcon
---
Welcome to the twenty-first edition of the Kudu Weekly Update. Astute
readers will notice that the weekly blog posts have been not-so-weekly
of late -- in fact, it has been nearly two months since the previous post
as I and others have focused on releases, conferences, etc.

So, rather than covering just this past week, this post will cover highlights
of the progress since the 1.0 release in mid-September. If you're interested
in learning about progress prior to that release, check the
[release notes](http://kudu.apache.org/releases/1.0.0/docs/release_notes.html).

<!--more-->

## Project news

* On September 12th, the Kudu PMC announced that Alexey Serbin and Will
  Berkeley had been voted as new committers and PMC members.

  Alexey's contributions prior to committership included
  [AUTO_FLUSH_BACKGROUND](https://gerrit.cloudera.org/#/c/3952/) support
  in C++ as well as [API documentation](http://kudu.apache.org/apidocs/)
  for the C++ client API.

  Will's contributions include several fixes to the web UIs, large
  improvements the Flume integration, and a lot of good work
  burning down long-standing bugs.

  Both contributors were "acting the part" and the PMC was pleased to
  recognize their contributions with committership.

* Kudu 1.0.0 was [released](https://kudu.apache.org/2016/09/20/apache-kudu-1-0-0-released.html)
  on September 19th. Most community members have upgraded by this point
  and have been reporting improved stability and performance.

* Dan Burkert has been managing a Kudu 1.0.1 release to address a few
  important bugs discovered since 1.0.0. The vote passed on Monday
  afternoon, so the release should be made officially available
  later this week.


## Development discussions and code in progress

* After the 1.0 release, many contributors have gone into a design phase
  for upcoming work. Over the last couple of weeks, developers have posted
  scoping and design documents for topics including:
  * [Security features](https://docs.google.com/document/d/1cPNDTpVkIUo676RlszpTF1gHZ8l0TdbB7zFBAuOuYUw/edit#heading=h.gsibhnd5dyem) (Todd Lipcon)
  * [Improved disk-failure handling](https://goo.gl/wP5BJb) (Dinesh Bhat)
  * [Tools for manual recovery from corruption](https://s.apache.org/7K48) (Mike Percy and Dinesh Bhat)
  * [Addressing issues seen with the LogBlockManager](https://s.apache.org/uOOt) (Adar Dembo)
  * [Providing proper snapshot/serializable consistency](https://s.apache.org/7VCo) (David Alves)
  * [Improving re-replication of under-replicated tablets](https://s.apache.org/ARUP) (Mike Percy)
  * [Avoiding Raft election storms](https://docs.google.com/document/d/1066W63e2YUTNnecmfRwgAHghBPnL1Pte_gJYAaZ_Bjo/edit) (Todd Lipcon)

  The development community has no particular rule that all work must be
  accompanied by such a document, but in the past they have proven useful
  for fleshing out ideas around a design before beginning implementation.
  As Kudu matures, we can probably expect to see more of this kind of planning
  and design discussion.

  If any of the above work areas sounds interesting to you, please take a
  look and leave your comments! Similarly, if you are interested in contributing
  in any of these areas, please feel free to volunteer on the mailing list.
  Help of all kinds (coding, documentation, testing, etc) is welcomed.

* Adar Dembo spent a chunk of time re-working the `thirdparty` directory
  that contains most of Kudu's native dependencies. The major resulting
  changes are:
  * Build directories are now cleanly isolated from source directories,
    improving cleanliness of re-builds.
  * ThreadSanitizer (TSAN) builds now use `libc++` instead of `libstdcxx`
    for C++ library support. The `libc++` library has better support for
    sanitizers, is easier to build in isolation, and solves some compatibility
    issues that Adar was facing with GCC 5 on Ubuntu Xenial.
  * All of the thirdparty dependencies now build with TSAN instrumentation,
    which improves our coverage of this very effective tooling.

  The impact to most developers is that, if you have an old source checkout,
  it's highly likely you will need to clean and re-build the thirdparty
  directory.

* Many contributors spent time in recent weeks trying to address the
  flakiness of various test cases. The Kudu project uses a
  [dashboard](http://dist-test.cloudera.org:8080/) to track the flakiness
  of each test case, and [distributed test infrastructure](http://dist-test.cloudera.org/)
  to facilitate reproducing test flakes.    <!-- spaces cause line break -->
  As might be expected, some of the flaky tests were due to bugs or
  timing assumptions in the tests themselves. However, this effort
  also identified several real bugs:
  * A [tight retry loop](http://gerrit.cloudera.org:8080/4570]) in the
    Java client.
  * A [memory leak](http://gerrit.cloudera.org:8080/4395) due to circular
    references in the C++ client.
  * A [crash](http://gerrit.cloudera.org:8080/4551) which could affect
    tools used for problem diagnosis.
  * A [divergence bug](http://gerrit.cloudera.org:8080/4409) in Raft consensus
    under particularly torturous scenarios.
  * A potential [crash during tablet server startup](http://gerrit.cloudera.org:8080/4394).
  * A case in which [thread startup could be delayed](http://gerrit.cloudera.org:8080/4626)
    by built-in monitoring code.


  As a result of these efforts, the failure rate of these flaky tests has
  decreased significantly and the stability of Kudu releases continues
  to increase.

* Dan Burkert picked up work originally started by Sameer Abhyankar on
  [KUDU-1363](https://issues.apache.org/jira/browse/KUDU-1363), which adds
  support for adding `IN (...)` predicates to scanners. Dan committed the
  [main patch](http://gerrit.cloudera.org:8080/2986) as well as corresponding
  [support in the Java client](http://gerrit.cloudera.org:8080/4530).
  Jordan Birdsell quickly added corresponding support in [Python](http://gerrit.cloudera.org:8080/4548).
  This new feature will be available in an upcoming release.

* Work continues on the `kudu` command line tool. Dinesh Bhat added
  the ability to ask a tablet's leader to [step down](http://gerrit.cloudera.org:8080/4533)
  and Alexey Serbin added a [tool to insert random data into a
  table](http://gerrit.cloudera.org:8080/4412).

* Jordan Birdsell continues to be on a tear improving the Python client.
  The patches are too numerous to mention, but highlights include Python 3
  support as well as near feature parity with the C++ client.

* Todd Lipcon has been doing some refactoring and cleanup in the Raft
  consensus implementation. In addition to simplifying and removing code,
  he committed [KUDU-1567](https://issues.apache.org/jira/browse/KUDU-1567),
  which improves write performance in many cases by a factor of three
  or more while also improving stability.

* Brock Noland is working on support for [INSERT IGNORE](https://gerrit.cloudera.org/#/c/4491/)
  as a first-class part of the Kudu API. Of course this functionality
  can already be done by simply performing normal inserts and ignoring any
  resulting errors, but pushing it to the server prevents the server
  from counting such operations as errors.

* Congratulations to Ninad Shringarpure for contributing his first patches
  to Kudu. Ninad contributed two documentation fixes and improved
  formatting on the Kudu web UI.



Want to learn more about a specific topic from this blog post? Shoot an email to the
[kudu-user mailing list](mailto:user@kudu.apache.org) or
tweet at [@ApacheKudu](https://twitter.com/ApacheKudu). Similarly, if you're
aware of some Kudu news we missed, let us know so we can cover it in
a future post.
