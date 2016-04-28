---
layout: post
title: Ingesting JSON Data Into Apache Kudu with StreamSets Data Collector
author: Pat Patterson
---
At the [Hadoop Summit in Dublin](http://2016.hadoopsummit.org/dublin/) 
this week, [Ted Malaska](https://twitter.com/TedMalaska), Principal 
Solutions Architect at Cloudera, and I presented *Ingest and Stream 
Processing - What Will You Choose?*, looking at the big data streaming 
landscape with a focus on ingest. The session closed with a demo of 
StreamSets Data Collector, the open source graphical IDE for building 
ingest pipelines.

In the demo, I built a pipeline to read JSON data from Apache Kafka, 
augmented the data in JavaScript, and wrote the resulting records to 
both Apache Kudu (incubating) for analysis and Apache Kafka for 
visualization.

<!--more-->

Here's a recording of the session:

[![YouTube video](https://img.youtube.com/vi/LTONR-L40Xg/0.jpg)](https://www.youtube.com/watch?v=LTONR-L40Xg)

The Apache Kudu destination is new in StreamSets Data Collector 1.3.0.0, 
[released this week](https://streamsets.com/blog/announcing-data-collector-ver-1-3-0-0/) 
and [available for download](https://streamsets.com/opensource/).

Learn more about StreamSets at 
[https://streamsets.com/](https://streamsets.com/).

About the Author
----------------

[Pat Patterson](https://about.me/patpatterson) was recently hired as 
Community Champion at StreamSets. Prior to StreamSets, Pat was a 
developer evangelist at Salesforce, focused on identity, integration and 
the Internet of Things and, before that, managed the OpenSSO community 
at Sun Microsystems. Pat enjoys hacking code at every level - from 
kernel drivers in C to web front ends in JavaScript.