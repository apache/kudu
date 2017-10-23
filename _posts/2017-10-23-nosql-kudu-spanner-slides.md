---
layout: post
title: "Slides: A brave new world in mutable big data: Relational storage"
author: Todd Lipcon
---
Since the Apache Kudu project made its debut in 2015, there have been
a few common questions that kept coming up at every presentation:

* Is Kudu an open source version of Google's Spanner system?
* Is Kudu NoSQL or SQL?
* Why does Kudu have a relational data model? Isn't SQL dead?

<!--more-->

A few of these questions are addressed in the
[Kudu FAQ](https://kudu.apache.org/faq.html), but I thought they were
interesting enough that I decided to give a talk on these subjects
at [Strata Data Conference NYC 2017](https://conferences.oreilly.com/strata/strata-ny).

Preparing this talk was particularly interesting, since Google recently released
Spanner to the public in SaaS form as [Google Cloud Spanner](https://cloud.google.com/spanner/).
This meant that I was able to compare Kudu vs Spanner not just qualitatively
based on some academic papers, but quantitatively as well.

To summarize the key points of the presentation:

* Despite the growing popularity of "NoSQL" from 2009 through 2013, SQL has
  once again become the access mechanism of choice for the majority of
  analytic applications. NoSQL has become "Not Only SQL".

* Spanner and Kudu share a lot of common features. However:

  * Spanner offers a superior feature set and performance for Online
   Transactional Processing (OLTP) workloads, including ACID transactions and
   secondary indexing.

  * Kudu offers a superior feature set and performance for Online
   Analytical Processing (OLAP) and Hybrid Transactional/Analytic Processing
   (HTAP) workloads, including more complete SQL support and orders of
   magnitude better performance on large queries.

For more details and for the full benchmark numbers, check out the slide deck
below:

<iframe src="//www.slideshare.net/slideshow/embed_code/key/loQpO2vzlwGGgz" width="595" height="485" frameborder="0" marginwidth="0" marginheight="0" scrolling="no" style="border:1px solid #CCC; border-width:1px; margin-bottom:5px; max-width: 100%;" allowfullscreen> </iframe> <div style="margin-bottom:15px"> <strong> <a href="//www.slideshare.net/ToddLipcon/a-brave-new-world-in-mutable-big-data-relational-storage-strata-nyc-2017" title="A brave new world in mutable big data relational storage (Strata NYC 2017)" target="_blank">A brave new world in mutable big data relational storage (Strata NYC 2017)</a> </strong> from <strong><a href="https://www.slideshare.net/ToddLipcon" target="_blank">Todd Lipcon</a></strong> </div>

Questions or comments? Join the [Apache Kudu Community](/community.html) to discuss.
