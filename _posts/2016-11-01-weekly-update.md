---
layout: post
title: Apache Kudu Weekly Update November 1st, 2016
author: Todd Lipcon
---
Welcome to the twenty-third edition of the Kudu Weekly Update. This weekly blog post
covers ongoing development and news in the Apache Kudu project.

<!--more-->

## Development discussions and code in progress

* Dan Burkert committed a piece of test infrastructure
  called "MiniKDC" for both Java and C++. The MiniKDC sets up a short-lived
  Kerberos environment in the context of a single test case, making it
  easy to build tests of security features without requiring any special
  infrastructure on the part of the developer.

* Todd Lipcon added support for Kerberos (GSSAPI) support to Kudu's
  RPC system, allowing servers to authenticate the user principal of
  any inbound RPC connection. He also integrated Kudu's C++ "MiniCluster"
  test infrastructure to allow starting a Kerberized cluster in the
  context of a test.

* Dan, Todd, and Alexey Serbin have been iterating on a more detailed
  [design doc](https://docs.google.com/document/d/1Yu4iuIhaERwug1vS95yWDd_WzrNRIKvvVGUb31y-_mY/edit#)
  for authentication in Kudu. This doc outlines the various non-Kerberos
  methods that Kudu will use for authentication as well as how TLS will
  be used to encrypt and authenticate some types of connections.

* Part of the above design document involves Kudu servers generating and
  signing X509 certificates on the fly to use for authenticated TLS.
  Alexey has been working on a large [patch](https://gerrit.cloudera.org/#/c/4799/)
  which uses OpenSSL to provide key generation and signing functionality.

* Sailesh Mukil has been working on adding support for
  [TLS in Kudu's RPC system](https://gerrit.cloudera.org/#/c/4789/). The TLS
  support is a critical part of the overall design for security. This patch
  has gone through several rounds of review and nearing completion.

* JD Cryans has been continuing to improve the Java client, including adding
  the ability to specify that the client would like to read the "closest"
  replica (e.g. reading from a local copy if possible). Additionally,
  JD has been working on some basic [tracing support](https://gerrit.cloudera.org/#/c/4781/)
  within the Java client. This tracing aims to make timeouts easier to understand
  and diagnose.

* Jordan Birdsell committed 9 more patches to the Python client, bringing it
  very close to feature parity with C++. Jordan has a few more patches in flight
  which should complete this long-running effort.

* Congrats to new contributor Haijie Hong who committed his first patch this week.
  Haijie added support for [run-length encoding 64-bit integers](https://gerrit.cloudera.org/#/c/4822/).

* Will Berkeley picked back up work on [improving the capability of ALTER
  TABLE](https://gerrit.cloudera.org/#/c/4310/). His in-flight patch adds support
  for changing the default value of a column as well as changing storage attributes
  such as desired block size, encoding, and compression.

* Adar Dembo has been working on a series of patches for the Block Manager, the
  component of Kudu which is responsible for laying out blocks on the local
  file system. His patch series consists of a number of refactors to clean up
  and improve the code structure, followed by an [improvement to reduce file system
  fragmentation](https://gerrit.cloudera.org/#/c/4848/).

* David Alves has been working on a [patch series](https://gerrit.cloudera.org/#/c/4819/)
  which adds support for storing 'REINSERT' deltas on disk. These records are
  generated if a user inserts a row, deletes it, and inserts a new row with the
  same primary key. Current versions of Kudu lose track of the history of the
  prior version of the row in this scenario, which prevents correct snapshot reads.
  David's patch series fixes this.



Want to learn more about a specific topic from this blog post? Shoot an email to the
[kudu-user mailing list](mailto:user@kudu.apache.org) or
tweet at [@ApacheKudu](https://twitter.com/ApacheKudu). Similarly, if you're
aware of some Kudu news we missed, let us know so we can cover it in
a future post.
