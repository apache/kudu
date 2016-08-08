---
layout: post
title: Apache Kudu Weekly Update August 8th, 2016
author: Todd Lipcon
---
Welcome to the nineteenth edition of the Kudu Weekly Update. This weekly blog post
covers ongoing development and news in the Apache Kudu project.

<!--more-->

## Development discussions and code in progress

* After a couple months of work, Dan Burkert finished
  [adding add/remove range partition support](https://gerrit.cloudera.org/#/c/3648/)
  in the C++ client and in the master.

  Dan also posted a patch for review which [adds support for this
  feature](https://gerrit.cloudera.org/#/c/3854/) to the Java client. Dan is
  expecting that this will be finished in time for the upcoming Kudu 0.10.0
  release.

  Misty Stanley-Jones started working on [documentation for this
  feature](https://gerrit.cloudera.org/#/c/3796/). Readers of this
  blog are encouraged to check out the docs and provide feedback!

* Adar Dembo also completed fixing most of the issues related to high availability
  using multiple Kudu master processes. The upcoming Kudu 0.10.0 release will support
  running multiple masters and transparently handling a transient failure of any
  master process.

  Although multi-master should now be stable, some work remains in this area. Namely,
  Adar is working on a [design for handling permanent failure of a machine hosting
  a master](https://gerrit.cloudera.org/#/c/3393/). In this case, the administrator
  will need to use some new tools to create a new master replica by copying data from
  an existing one.

* Todd Lipcon started a
  [discussion](https://mail-archives.apache.org/mod_mbox/incubator-kudu-dev/201607.mbox/%3CCADY20s5WdR7KmB%3DEAHJwvzELhe9PXfnnGMLV%2B4t%3D%3Defw%3Dix8uw%40mail.gmail.com%3E)
  on the dev mailing list about renaming the Kudu feature which creates new
  replicas of tablets after they become under-replicated.  Since its initial
  introduction, this feature was called "remote bootstrap", but Todd pointed out
  that this naming caused some confusion with the other "bootstrap" term used to
  describe the process by which a tablet loads itself at startup.

  The discussion concluded with an agreement to rename the process to "Tablet Copy".
  Todd provided patches to perform this rename, which were committed at the end of the
  week last week.

* Congratulations to Attila Bukor for his first commit to Kudu! Attila
  [fixed an error in the quick-start documentation](https://gerrit.cloudera.org/#/c/3820/).

## News and articles from around the web

* The New Stack published an [introductory article about Kudu](http://thenewstack.io/apache-kudu-fast-columnar-data-store-hadoop/).
  The article was based on a recent interview with Todd Lipcon
  and covers topics such as the origin of the name "Kudu", where Kudu fits into the
  Apache Hadoop ecosystem, and goals for the upcoming 1.0 release.

Want to learn more about a specific topic from this blog post? Shoot an email to the
[kudu-user mailing list](mailto:user@kudu.apache.org) or
tweet at [@ApacheKudu](https://twitter.com/ApacheKudu). Similarly, if you're
aware of some Kudu news we missed, let us know so we can cover it in
a future post.
