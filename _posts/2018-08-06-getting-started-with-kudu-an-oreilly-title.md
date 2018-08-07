---
layout: post
title: Getting Started with Kudu an O'Reilly Title
author: Brock Noland
---

The following article by Brock Noland was reposted from the
[phData](https://www.phdata.io/getting-started-with-kudu/)
blog with their permission.

Five years ago, enabling Data Science and Advanced Analytics on the
Hadoop platform was hard. Organizations required strong Software Engineering
capabilities to successfully implement complex Lambda architectures or even
simply implement continuous ingest. Updating or deleting data, were simplify
nightmare. General Data Protection Regulation (GDPR) would have been an extreme
challenge at that time.

<!-- more -->

In that context, on October 11th 2012 Todd Lipcon perform Apache Kudu's initial
commit. The commit message was:

    Code for writing cfiles seems to basically work
    Need to write code for reading cfiles, still

And Kudu development was off and running. Around this same time Todd, on his
internal Wiki page, started listing out the papers he was reading to develop
the theoretical background for creating Kudu. I followed along, reading as many
as I could, understanding little, because I knew Todd was up to something
important. About a year after that initial commit, I got my
[Kudu first commit](https://github.com/apache/kudu/commit/1d7e6864b4a31d3fe6897e4cb484dfcda6608d43),
documenting the upper bound of a library. This is a small contribution of which I am still
proud.

In the meantime, I was lucky enough to be a founder of a Hadoop Managed Services
and Consulting company known as [phData](http://phdata.io/). We found that a majority
of our customers had use cases which Kudu vastly simplified. Whether it's Change Data
Capture (CDC) from thousands of source tables to Internet of Things (IoT) ingest, Kudu
makes life much easier as both an operator of a Hadoop cluster and a developer providing
business value on the platform.

Through this work, I was lucky enough to be a co-author of
[Getting Started with Kudu(http://shop.oreilly.com/product/0636920065739.do).
The book is a summation of mine and our co-authors, Jean-Marc Spaggiari, Mladen
Kovacevic, and Ryan Bosshart,  learnings while cutting our teeth on early versions
of Kudu. Specifically you will learn:

* Theoretical understanding of Kudu concepts in simple plain spoken words and simple diagrams
* Why, for many use cases, using Kudu is so much easier than other ecosystem storage technologies
* How Kudu enables Hybrid Transactional/Analytical Processing (HTAP) use cases
* How to design IoT, Predictive Modeling, and Mixed Platform Solutions using Kudu
* How to design Kudu Schemas

![Getting Started with Kudu Cover]({{ site.github.url }}/img/2018-08-06-getting-started-with-kudu-an-oreilly-title.gif){: .img-responsive}

Looking forward, I am excited to see Kudu gain additional features and adoption
and eventually the second revision of this title. In the meantime, if you have
feedback or questions, please reach out on the `#getting-started-kudu` channel of
the [Kudu Slack](https://getkudu-slack.herokuapp.com/) or if you prefer non-real-time
communication, please use the user@ mailing list!
