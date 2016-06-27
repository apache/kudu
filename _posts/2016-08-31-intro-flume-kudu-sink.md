---
layout: post
title: "An Introduction to Kudu Flume Sink"
author: Ara Abrahamian
---
This post discusses the Kudu Flume Sink. First, I'll give some background on why we considered
using Kudu, what Flume does for us, and how Flume fits with Kudu in our project.

Why Kudu
========
Traditionally in the Hadoop ecosystem we've dealt with various _batch processing_ technologies such
as MapReduce and the many libraries and tools built on top of it in various languages (Apache Pig,
Apache Hive, Apache Oozie and many others). The main problem with this approach is that it needs to
process the whole data set in batches, again and again, as soon as new data gets added. Things get
really complicated when a few such tasks need to get chained together, or when the same data set
needs to be processed in various ways by different jobs, while all compete for the shared cluster
resources.

The opposite of this approach is _stream processing_: process the data as soon as it arrives, not
in batches. Streaming systems such as Spark Streaming, Storm, Kafka Streams, and many others make
this possible. But writing streaming services is not trivial. The streaming systems are becoming
more and more capable and support more complex constructs, but they are not yet easy to use. All
queries and processes need to be carefully planned and implemented.

To summarize, _batch processing_ is:

- file-based
- a paradigm that processes large chunks of data as a group
- high latency and high throughput, both for ingest and query
- typically easy to program, but hard to orchestrate
- well suited for writing ad-hoc queries, although they are typically high latency

While _stream processing_ is:

- a totally different paradigm, which involves single events and time windows instead of large groups of events
- still file-based and not a long-term database
- not batch-oriented, but incremental
- ultra-fast ingest and ultra-fast query (query results basically pre-calculated)
- not so easy to program, relatively easy to orchestrate
- impossible to write ad-hoc queries

And a Kudu-based _near real-time_ approach is:

- flexible and expressive, thanks to SQL support via Apache Impala (incubating)
- a table-oriented, mutable data store that feels like a traditional relational database
- very easy to program, you can even pretend it's good old MySQL
- low-latency and relatively high throughput, both for ingest and query

At Argyle Data, we're dealing with complex fraud detection scenarios. We need to ingest massive
amounts of data, run machine learning algorithms and generate reports. When we created our current
architecture two years ago we decided to opt for a database as the backbone of our system. That
database is Apache Accumulo. It's a key-value based database which runs on top of Hadoop HDFS,
quite similar to HBase but with some important improvements such as cell level security and ease
of deployment and management. To enable querying of this data for quite complex reporting and
analytics, we used Presto, a distributed query engine with a pluggable architecture open-sourced
by Facebook. We wrote a connector for it to let it run queries against the Accumulo database. This
architecture has served us well, but there were a few problems:

- we need to ingest even more massive volumes of data in real-time
- we need to perform complex machine-learning calculations on even larger data-sets
- we need to support ad-hoc queries, plus long-term data warehouse functionality

So, we've started gradually moving the core machine-learning pipeline to a streaming based
solution. This way we can ingest and process larger data-sets faster in the real-time. But then how
would we take care of ad-hoc queries and long-term persistence? This is where Kudu comes in. While
the machine learning pipeline ingests and processes real-time data, we store a copy of the same
ingested data in Kudu for long-term access and ad-hoc queries. Kudu is our _data warehouse_. By
using Kudu and Impala, we can retire our in-house Presto connector and rely on Impala's
super-fast query engine. 
  
But how would we make sure data is reliably ingested into the streaming pipeline _and_ the
Kudu-based data warehouse? This is where Apache Flume comes in.

Why Flume
=========
According to their [website](http://flume.apache.org/) "Flume is a distributed, reliable, and
available service for efficiently collecting, aggregating, and moving large amounts of log data.
It has a simple and flexible architecture based on streaming data flows. It is robust and fault
tolerant with tunable reliability mechanisms and many failover and recovery mechanisms." As you
can see, nowhere is Hadoop mentioned but Flume is typically used for ingesting data to Hadoop
clusters. 

![png](https://blogs.apache.org/flume/mediaresource/ab0d50f6-a960-42cc-971e-3da38ba3adad)

Flume has an extensible architecture. An instance of Flume, called an _agent_, can have multiple
_channels_, with each having multiple _sources_ and _sinks_ of various types. Sources queue data
in channels, which in turn write out data to sinks. Such _pipelines_ can be chained together to
create even more complex ones. There may be more than one agent and agents can be configured to
support failover and recovery. 

Flume comes with a bunch of built-in types of channels, sources and sinks. Memory channel is the
default (an in-memory queue with no persistence to disk), but other options such as Kafka- and
File-based channels are also provided. As for the sources, Avro, JMS, Thrift, spooling directory
source are some of the built-in ones. Flume also ships with many sinks, including sinks for writing
data to HDFS, HBase, Hive, Kafka, as well as to other Flume agents.
 
In the rest of this post I'll go over the Kudu Flume sink and show you how to configure Flume to
write ingested data to a Kudu table. The sink has been part of the Kudu distribution since the 0.8
release and the source code can be found [here](https://github.com/apache/kudu/tree/master/java/kudu-flume-sink).

Configuring the Kudu Flume Sink
===============================
Here is a sample flume configuration file:

```
agent1.sources  = source1
agent1.channels = channel1
agent1.sinks = sink1

agent1.sources.source1.type = exec
agent1.sources.source1.command = /usr/bin/vmstat 1
agent1.sources.source1.channels = channel1

agent1.channels.channel1.type = memory
agent1.channels.channel1.capacity = 10000
agent1.channels.channel1.transactionCapacity = 1000

agent1.sinks.sink1.type = org.apache.flume.sink.kudu.KuduSink
agent1.sinks.sink1.masterAddresses = localhost
agent1.sinks.sink1.tableName = stats
agent1.sinks.sink1.channel = channel1
agent1.sinks.sink1.batchSize = 50
agent1.sinks.sink1.producer = org.apache.kudu.flume.sink.SimpleKuduEventProducer
```

We define a source called `source1` which simply executes a `vmstat` command to continuously generate
virtual memory statistics for the machine and queue events into an in-memory `channel1` channel,
which in turn is used for writing these events to a Kudu table called `stats`. We are using
`org.apache.kudu.flume.sink.SimpleKuduEventProducer` as the producer. `SimpleKuduEventProducer` is
the built-in and default producer, but it's implemented as a showcase for how to write Flume
events into Kudu tables. For any serious functionality we'd have to write a custom producer. We
need to make this producer and the `KuduSink` class available to Flume. We can do that by simply
copying the `kudu-flume-sink-<VERSION>.jar` jar file from the Kudu distribution to the
`$FLUME_HOME/plugins.d/kudu-sink/lib` directory in the Flume installation. The jar file contains
`KuduSink` and all of its dependencies (including Kudu java client classes).

At a minimum, the Kudu Flume Sink needs to know where the Kudu masters are
(`agent1.sinks.sink1.masterAddresses = localhost`) and which Kudu table should be used for writing
Flume events to (`agent1.sinks.sink1.tableName = stats`). The Kudu Flume Sink doesn't create this
table, it has to be created before the Kudu Flume Sink is started.

You may also notice the `batchSize` parameter. Batch size is used for batching up to that many
Flume events and flushing the entire batch in one shot. Tuning batchSize properly can have a huge
impact on ingest performance of the Kudu cluster.

Here is a complete list of KuduSink parameters:

| Parameter Name      | Default                                            | Description                                                                                  |
| ------------------- | -------------------------------------------------- | ---------------------------------------------------------------------------------------------|
| masterAddresses     | N/A                                                | Comma-separated list of "host:port" pairs of the masters (port optional)                     |
| tableName           | N/A                                                | The name of the table in Kudu to write to                                                    |
| producer            | org.apache.kudu.flume.sink.SimpleKuduEventProducer | The fully qualified class name of the Kudu event producer the sink should use                |
| batchSize           | 100                                                | Maximum number of events the sink should take from the channel per transaction, if available |
| timeoutMillis       | 30000                                              | Timeout period for Kudu operations, in milliseconds                                          |
| ignoreDuplicateRows | true                                               | Whether to ignore errors indicating that we attempted to insert duplicate rows into Kudu     |

Let's take a look at the source code for the built-in producer class:

```
public class SimpleKuduEventProducer implements KuduEventProducer {
  private byte[] payload;
  private KuduTable table;
  private String payloadColumn;

  public SimpleKuduEventProducer(){
  }

  @Override
  public void configure(Context context) {
    payloadColumn = context.getString("payloadColumn","payload");
  }

  @Override
  public void configure(ComponentConfiguration conf) {
  }

  @Override
  public void initialize(Event event, KuduTable table) {
    this.payload = event.getBody();
    this.table = table;
  }

  @Override
  public List<Operation> getOperations() throws FlumeException {
    try {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addBinary(payloadColumn, payload);

      return Collections.singletonList((Operation) insert);
    } catch (Exception e){
      throw new FlumeException("Failed to create Kudu Insert object!", e);
    }
  }

  @Override
  public void close() {
  }
}
```

`SimpleKuduEventProducer` implements the `org.apache.kudu.flume.sink.KuduEventProducer` interface,
which itself looks like this:

```
public interface KuduEventProducer extends Configurable, ConfigurableComponent {
  /**
   * Initialize the event producer.
   * @param event to be written to Kudu
   * @param table the KuduTable object used for creating Kudu Operation objects
   */
  void initialize(Event event, KuduTable table);

  /**
   * Get the operations that should be written out to Kudu as a result of this
   * event. This list is written to Kudu using the Kudu client API.
   * @return List of {@link org.kududb.client.Operation} which
   * are written as such to Kudu
   */
  List<Operation> getOperations();

  /*
   * Clean up any state. This will be called when the sink is being stopped.
   */
  void close();
}
```

`public void configure(Context context)` is called when an instance of our producer is instantiated
by the KuduSink. SimpleKuduEventProducer's implementation looks for a producer parameter named
`payloadColumn` and uses its value ("payload" if not overridden in Flume configuration file) as the
column which will hold the value of the Flume event payload. If you recall from above, we had
configured the KuduSink to listen for events generated from the `vmstat` command. Each output row
from that command will be stored as a new row containing a `payload` column in the `stats` table.
`SimpleKuduEventProducer` does not have any configuration parameters, but if it had any we would
define them by prefixing it with `producer.` (`agent1.sinks.sink1.producer.parameter1` for
example). 

The main producer logic resides in the `public List<Operation> getOperations()` method. In
SimpleKuduEventProducer's implementation we simply insert the binary body of the Flume event into
the Kudu table. Here we call Kudu's `newInsert()` to initiate an insert, but could have used
`Upsert` if updating an existing row was also an option, in fact there's another producer
implementation available for doing just that: `SimpleKeyedKuduEventProducer`. Most probably you
will need to write your own custom producer in the real world, but you can base your implementation
on the built-in ones.

In the future, we plan to add more flexible event producer implementations so that creation of a
custom event producer is not required to write data to Kudu. See
[here](https://gerrit.cloudera.org/#/c/4034/) for a work-in-progress generic event producer for
Avro-encoded Events.

Conclusion
====
Kudu is a scalable data store which lets us ingest insane amounts of data per second. Apache Flume
helps us aggregate data from various sources, and the Kudu Flume Sink lets us easily store
the aggregated Flume events into Kudu. Together they enable us to create a data warehouse out of
disparate sources.

_Ara Abrahamian is a software engineer at Argyle Data building fraud detection systems using
sophisticated machine learning methods. Ara is the original author of the Flume Kudu Sink that
is included in the Kudu distribution. You can follow him on Twitter at @ara_e._