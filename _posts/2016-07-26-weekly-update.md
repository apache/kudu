---
layout: post
title: Apache Kudu Weekly Update July 26, 2016
author: Jean-Daniel Cryans
---
Welcome to the eighteenth edition of the Kudu Weekly Update. This weekly blog post
covers ongoing development and news in the Apache Kudu project.

<!--more-->

## Project news

* Kudu has graduated from the Apache Incubator and is now a Top-Level Project! All the details
  are in this [blog post](http://kudu.apache.org/2016/07/25/asf-graduation.html).
  Mike Percy and Todd Lipcon made a few updates to the website to reflect the project’s
  new name and status.

## Development discussions and code in progress

* Dan Burkert contributed a few patches that repackage the Java client under `org.apache.kudu`
  in place of `org.kududb`. This was done in a **backward-incompatible** way, meaning that import
  statements will have to be modified in existing Java code to compile against a newer Kudu JAR
  version (from 0.10.0 onward). This stems from [a discussion](http://mail-archives.apache.org/mod_mbox/kudu-dev/201605.mbox/%3CCAGpTDNcJohQBgjzXafXJQdqmBB4sL495p5V_BJRXk_nAGWbzhA@mail.gmail.com%3E)
  initiated in May. It won’t have an impact on C++ or Python users, and it isn’t affecting wire
  compatibility.

* Still on the Java-side, J-D Cryans pushed [a patch](https://gerrit.cloudera.org/#/c/3055/)
  that completely changes how Exceptions are managed. Before this change, users had to introspect
  generic Exception objects, making it a guessing game and discouraging good error handling.
  Now, the synchronous client’s methods throw `KuduException` which packages a `Status` object
  that can be interrogated. This is very similar to how the C++ API works.

  Existing code that uses the new Kudu JAR should still compile since this change replaces generic
  `Exception` with a more specific `KuduException`. Error handling done by string-matching the
  exception messages should now use the provided `Status` object.

* Alexey Serbin’s [patch](https://gerrit.cloudera.org/#/c/3619/) that adds Doxygen-based
  documentation was pushed and the new API documentation for C++ developers will be available
  with the next release.

* Todd has made many improvements to the `ksck` tool over the last week. Building upon Will
  Berkeley’s [WIP patch for KUDU-1516](https://gerrit.cloudera.org/#/c/3632/), `ksck` can
  now detect more problematic situations like if a tablet doesn’t have a majority of replicas on
  live tablet servers, or if those replicas aren’t in a good state.
  `ksck` is also [now faster](https://gerrit.cloudera.org/#/c/3705/) when run against a large
  cluster with a lot of tablets, among other improvements.

* As mentioned last week, Dan has been working on [adding add/remove range partition support](https://gerrit.cloudera.org/#/c/3648/)
  in the C++ client and in the master. The patch has been through many rounds of review and
  testing and it’s getting close to completion. Meanwhile, J-D started looking at adding support
  for this functionality in the [Java client](https://gerrit.cloudera.org/#/c/3731/).

* Adar Dembo is also hard at work on the master. The [series](https://gerrit.cloudera.org/#/c/3609/)
  [of](https://gerrit.cloudera.org/#/c/3610/) [patches](https://gerrit.cloudera.org/#/c/3611/) to
  have the tablet servers heartbeat to all the masters that he published earlier this month is
  getting near the finish line.

Want to learn more about a specific topic from this blog post? Shoot an email to the
[kudu-user mailing list](mailto:user@kudu.incubator.apache.org) or
tweet at [@ApacheKudu](https://twitter.com/ApacheKudu). Similarly, if you're
aware of some Kudu news we missed, let us know so we can cover it in
a future post.
