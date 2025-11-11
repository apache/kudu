---
title: Ecosystem
layout: single_col
active_nav: community
single_col_extra_classes: ecosystem
---

## Apache Kudu Ecosystem

While the Apache Kudu project provides client bindings that allow users to
mutate and fetch data, more complex access patterns are often written via SQL
and compute engines. This is a non-exhaustive list of projects that integrate
with Kudu to enhance ingest, querying capabilities, and orchestration.

### Frequently used

The following integrations are among the most commonly used with Apache Kudu
(sorted alphabetically).

* [Apache Impala](#apache-impala)
* [Apache Nifi](#apache-nifi)
* [Apache Spark SQL](#apache-spark-sql)
* [Presto](#presto)

### SQL

#### [Apache Drill](https://drill.apache.org/)
Apache Drill provides schema-free SQL Query Engine for Hadoop, NoSQL and Cloud
Storage. See the [Drill Kudu API
documentation](https://drill.apache.org/apidocs/org/apache/drill/exec/store/kudu/package-summary.html)
for more details.

#### [Apache Hive](https://hive.apache.org/)
The Apache Hive ™ data warehouse software facilitates reading, writing, and
managing large datasets residing in distributed storage using SQL. See the
[Hive Kudu integration
documentation](https://cwiki.apache.org/confluence/display/Hive/Kudu+Integration)
for more details.

#### [Apache Impala](https://impala.apache.org/)
Apache Impala is the open source, native analytic database for Apache Hadoop.
See the [Kudu Impala integration
documentation](https://kudu.apache.org/docs/kudu_impala_integration.html) for
more details.

#### [Apache Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html)
Spark SQL is a Spark module for structured data processing. See the [Kudu Spark
integration
documentation](https://kudu.apache.org/docs/developing.html#_kudu_integration_with_spark)
for more details.

#### [Presto](https://prestodb.io/)
Presto is an open source distributed SQL query engine for running interactive
analytic queries against data sources of all sizes ranging from gigabytes to
petabytes. See the [Presto Kudu connector
documentation](https://prestodb.io/docs/current/connector/kudu.html) for more
details.

### Computation

#### [Apache Beam](https://beam.apache.org/)
Apache Beam is a unified model for defining both batch and streaming
data-parallel processing pipelines, as well as a set of language-specific SDKs
for constructing pipelines and Runners for executing them on distributed
processing backends. See the [Beam Kudu source and sink
documentation](https://beam.apache.org/releases/javadoc/2.23.0/org/apache/beam/sdk/io/kudu/KuduIO.html)
for more details.

#### [Apache Spark](https://spark.apache.org/)
Apache Spark is a unified analytics engine for large-scale data processing. See
the [Kudu Spark integration
documentation](https://kudu.apache.org/docs/developing.html#_kudu_integration_with_spark)
for more details.

#### [Pandas](https://pandas.pydata.org/)
Pandas is an open source, BSD-licensed library providing high-performance,
easy-to-use data structures and data analysis tools for the Python programming
language. Kudu Python scanners can be converted to Pandas DataFrames. See
[Kudu's Python
tests](https://github.com/apache/kudu/blob/master/python/kudu/tests/test_scanner.py)
for example usage.

### [Talend Big Data](https://www.talend.com/products/big-data/)
Talend simplifies and automates big data integration projects with on demand
Serverless Spark and machine learning. See [Talend's Kudu component
documentation](https://help.talend.com/reader/SuRq3Ek0vdlxbl_OV_wVFQ/iC3nZLaM7f49tf0mYTetIA)
for more details.

### Ingest

#### [Akka](https://akka.io/)
Akka facilitates building highly concurrent, distributed, and resilient
message-driven applications on the JVM. See the [Alpakka Kudu connector
documentation](https://doc.akka.io/docs/alpakka/current/kudu.html) for more
details.

#### [Apache Flink](https://flink.apache.org/)
Apache Flink is a framework and distributed processing engine for stateful
computations over unbounded and bounded data streams.  See the [Flink Kudu
connector
documentation](https://github.com/apache/flink-connector-kudu)
for more details.

#### [Apache Nifi](https://nifi.apache.org/)
Apache NiFi supports powerful and scalable directed graphs of data routing,
transformation, and system mediation logic. See the [PutKudu processor
documentation](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-kudu-nar/1.5.0/org.apache.nifi.processors.kudu.PutKudu/)
for more details.

#### [Apache Spark Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
Spark Streaming is an extension of the core Spark API that enables scalable,
high-throughput, fault-tolerant stream processing of live data streams.
See [Kudu's Spark Streaming
tests](https://github.com/apache/kudu/blob/master/java/kudu-spark/src/test/scala/org/apache/kudu/spark/kudu/StreamingTest.scala)
for example usage.

#### [Confluent Platform Kafka](https://www.confluent.io/product/confluent-platform)
Apache Kafka is an open-source distributed event streaming platform used by
thousands of companies for high-performance data pipelines, streaming
analytics, data integration, and mission-critical applications. See the [Kafka
Kudu connector
documentation](https://docs.confluent.io/current/connect/kafka-connect-kudu/index.html)
for more details.

#### [StreamSets Data Collector](https://streamsets.com/products/dataops-platform/data-collector/)
StreamSets Data Collector is a lightweight, powerful engine that streams data
in real time. See the [StreamSets Data Collector Kudu destination
documentation](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Destinations/Kudu.html).

#### [Striim](https://www.striim.com/)
Striim is real-time data integration software that enables continuous data
ingestion, in-flight stream processing, and delivery. See the [Striim Kudu
Writer
documentation](https://www.striim.com/docs/archive/390/en/kuduwriter.html) for
more details.

#### [TIBCO StreamBase](https://www.tibco.com/resources/datasheet/tibco-streambase)
TIBCO StreamBase® is an event processing platform for applying mathematical and
relational processing to real-time data streams. See the [StreamBase Kudu
operator
documentation](https://docs.tibco.com/pub/sfire-sfds/latest/doc/html/authoring/kuduoperator.html)
for more details.

#### [Informatica PowerExchange](https://docs.informatica.com/data-integration/powerexchange-for-cdc-and-mainframe/10-4-1/reference-manual/introduction-to-powerexchange.html)
Informatica® PowerExchange® is a family of products that enables retrieval of a variety of data
sources without having to develop custom data-access programs. See the
[PowerExchange for Kudu documentation](https://docs.informatica.com/data-integration/powerexchange-adapters-for-informatica/10-5/powerexchange-for-kudu-user-guide/preface.html)
for more details.

### Deployment and Orchestration

#### [Apache Camel](https://camel.apache.org/)
Camel is an open source integration framework that empowers you to quickly and
easily integrate various systems consuming or producing data. See the [Camel
Kudu component
documentation](https://camel.apache.org/components/latest/kudu-component.html)
for more details.

#### [Cloudera Manager](https://www.cloudera.com/products/product-components/cloudera-manager.html)
Cloudera Manager is an end-to-end application for managing CDH clusters. See
the [Cloudera Manager documentation for
Kudu](https://docs.cloudera.com/runtime/latest/administering-kudu/topics/kudu-managing-kudu.html)
for more details.

#### [Docker](https://www.docker.com/)
Docker facilitates packaging software into standardized units for development,
shipment, and deployment. See the official [Apache Kudu
Dockerhub](https://hub.docker.com/r/apache/kudu) and the [Apache Kudu Docker
Quickstart](https://kudu.apache.org/docs/quickstart.html) for more details.

#### [Wavefront](https://docs.wavefront.com/wavefront_introduction.html)
Wavefront is a high-performance streaming analytics platform that supports 3D
observability. See the [Wavefront Kudu integration
documentation](https://docs.wavefront.com/kudu.html) for more details.

### Visualization

#### [Zoomdata](https://www.zoomdata.com/)
Zoomdata provides a high-performance BI engine and visually engaging,
interactive dashboards. See [Zoomdata's Kudu
page](https://www.zoomdata.com/product/big-data/big-data-analytics-kudu/) for
more details.

## Distribution and Support

While Kudu is an Apache-licensed open source project, software vendors may
package and license it with other components to facilitate consumption. These
offerings are typically bundled with support to tune and facilitate
administration.

* [Cloudera CDH](https://www.cloudera.com/products/open-source/apache-hadoop/apache-kudu.html)
* [phData](https://www.phdata.io/getting-started-with-kudu/)
