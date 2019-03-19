---
layout: post
title: Testing Apache Kudu Applications on the JVM
author: Grant Henke & Mike Percy
---

Note: This is a cross-post from the Cloudera Engineering Blog
[Testing Apache Kudu Applications on the JVM](https://blog.cloudera.com/blog/2019/03/testing-apache-kudu-applications-on-the-jvm/)

Although the Kudu server is written in C++ for performance and efficiency, developers can write
client applications in C++, Java, or Python. To make it easier for Java developers to create
reliable client applications, we've added new utilities in Kudu 1.9.0 that allow you to write tests
using a Kudu cluster without needing to build Kudu yourself, without any knowledge of C++,
and without any complicated coordination around starting and stopping Kudu clusters for each test.
This post describes how the new testing utilities work and how you can use them in your application
tests.

<!--more-->

## User Guide

Note: It is possible this blog post could become outdated – for the latest documentation on using
the JVM testing utilities see the
[Kudu documentation](https://kudu.apache.org/docs/developing.html#_jvm_based_integration_testing).

### Requirements

In order to use the new testing utilities, the following requirements must be met:

- OS
  - macOS El Capitan (10.11) or later
  - CentOS 6.6+, Ubuntu 14.04+, or another recent distribution of Linux
 [supported by Kudu](https://kudu.apache.org/docs/installation.html#_prerequisites_and_requirements)
- JVM
  - Java 8+
  - Note: Java 7+ is deprecated, but still supported
- Build Tool
  - Maven 3.1 or later, required to support the
  [os-maven-plugin](https://github.com/trustin/os-maven-plugin)
  - Gradle 2.1 or later, to support the
  [osdetector-gradle-plugin](https://github.com/google/osdetector-gradle-plugin)
  - Any other build tool that can download the correct jar from Maven

### Build Configuration

In order to use the Kudu testing utilities, add two dependencies to your classpath:

- The `kudu-test-utils` dependency
- The `kudu-binary` dependency

The `kudu-test-utils` dependency has useful utilities for testing applications that use Kudu.
Primarily, it provides the
[KuduTestHarness class](https://github.com/apache/kudu/blob/master/java/kudu-test-utils/src/main/java/org/apache/kudu/test/KuduTestHarness.java)
to manage the lifecycle of a Kudu cluster for each test. The `KuduTestHarness` is a
[JUnit TestRule](https://junit.org/junit4/javadoc/4.12/org/junit/rules/TestRule.html)
that not only starts and stops a Kudu cluster for each test, but also has methods to manage the
cluster and get pre-configured `KuduClient` instances for use while testing.

The `kudu-binary` dependency contains the native Kudu (server and command-line tool) binaries for
the specified operating system. In order to download the right artifact for the running operating
system it is easiest to use a plugin, such as the
[os-maven-plugin](https://github.com/trustin/os-maven-plugin) or
[osdetector-gradle-plugin](https://github.com/google/osdetector-gradle-plugin), to detect the
current runtime environment. The `KuduTestHarness` will automatically find and use the `kudu-binary`
jar on the classpath.

WARNING: The `kudu-binary` module should only be used to run Kudu for integration testing purposes.
It should never be used to run an actual Kudu service, in production or development, because the
`kudu-binary` module includes native security-related dependencies that have been copied from the
build system and will not be patched when the operating system on the runtime host is patched.

#### Maven Configuration

If you are using Maven to build your project, add the following entries to your project’s
`pom.xml` file:

{% highlight XML %}
<build>
  <extensions>
    <!-- Used to find the right kudu-binary artifact with the Maven
         property ${os.detected.classifier} -->
    <extension>
      <groupId>kr.motd.maven</groupId>
      <artifactId>os-maven-plugin</artifactId>
      <version>1.6.2</version>
    </extension>
  </extensions>
</build>

<dependencies>
  <dependency>
    <groupId>org.apache.kudu</groupId>
    <artifactId>kudu-test-utils</artifactId>
    <version>1.9.0</version>
    <scope>test</scope>
  </dependency>
  <dependency>
    <groupId>org.apache.kudu</groupId>
    <artifactId>kudu-binary</artifactId>
    <version>1.9.0</version>
    <classifier>${os.detected.classifier}</classifier>
    <scope>test</scope>
  </dependency>
</dependencies>
{% endhighlight %}

#### Gradle Configuration

If you are using Gradle to build your project, add the following entries to your project’s
`build.gradle` file:

{% highlight Groovy %}
plugins {
  // Used to find the right kudu-binary artifact with the Gradle
  // property ${osdetector.classifier}
  id "com.google.osdetector" version "1.6.2"
}

dependencies {
   testCompile "org.apache.kudu:kudu-test-utils:1.9.0"
   testCompile "org.apache.kudu:kudu-binary:1.9.0:${osdetector.classifier}"
}
{% endhighlight %}

## Test Setup

Once your project is configured correctly, you can start writing tests using the `kudu-test-utils`
and `kudu-binary` artifacts. One line of code will ensure that each test automatically starts and
stops a real Kudu cluster and that cluster logging is output through `slf4j`:

{% highlight Java %}
@Rule public KuduTestHarness harness = new KuduTestHarness();
{% endhighlight %}

The [KuduTestHarness](https://github.com/apache/kudu/blob/master/java/kudu-test-utils/src/main/java/org/apache/kudu/test/KuduTestHarness.java)
has methods to get pre-configured clients, start and stop servers, and more. Below is an example
test to showcase some of the capabilities:

{% highlight Java %}
import org.apache.kudu.*;
import org.apache.kudu.client.*;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.*;

import java.util.Arrays;
import java.util.Collections;

public class MyKuduTest {

    @Rule
    public KuduTestHarness harness = new KuduTestHarness();

    @Test
    public void test() throws Exception {
        // Get a KuduClient configured to talk to the running mini cluster.
        KuduClient client = harness.getClient();

        // Some of the other most common KuduTestHarness methods include:
        AsyncKuduClient asyncClient = harness.getAsyncClient();
        String masterAddresses= harness.getMasterAddressesAsString();
        List<HostAndPort> masterServers = harness.getMasterServers();
        List<HostAndPort> tabletServers = harness.getTabletServers();
        harness.killLeaderMasterServer();
        harness.killAllMasterServers();
        harness.startAllMasterServers();
        harness.killAllTabletServers();
        harness.startAllTabletServers();

        // Create a new Kudu table.
        String tableName = "myTable";
        Schema schema = new Schema(Arrays.asList(
            new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build(),
            new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).key(true).build()
        ));
        CreateTableOptions opts = new CreateTableOptions()
            .setRangePartitionColumns(Collections.singletonList("key"));
        client.createTable(tableName, schema, opts);
        KuduTable table = client.openTable(tableName);

        // Write a few rows to the table
        KuduSession session = client.newSession();
        for(int i = 0; i < 10; i++) {
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            row.addInt("key", i);
            row.addString("value", String.valueOf(i));
            session.apply(insert);
        }
        session.close();

        // ... Continue the test. Read and validate the rows, alter the table, etc.
    }
}
{% endhighlight %}

For a complete example of a project using the `KuduTestHarness`, see the
[java-example](https://github.com/apache/kudu/tree/master/examples/java/java-example) project in
the Kudu source code repository. The Kudu project itself uses the `KuduTestHarness` for all of its
own integration tests. For more complex examples, you can explore the various
[Kudu integration](https://github.com/apache/kudu/tree/master/java/kudu-client/src/test/java/org/apache/kudu/client)
tests in the Kudu source code repository.

## Feedback

Kudu 1.9.0 is the first release to have these testing utilities available. Although these utilities
simplify testing of Kudu applications, there is always room for improvement.
Please report any issues, ideas, or feedback to the Kudu user mailing list, Jira, or Slack channel
and we will try to incorporate your feedback quickly. See the
[Kudu community page](https://kudu.apache.org/community.html) for details.

## Thank You

We would like to give a special thank you to everyone who helped contribute to the `kudu-test-utils`
and `kudu-binary` artifacts. We would especially like to thank
[Brian McDevitt](https://www.linkedin.com/in/brian-mcdevitt-1136a08/) at [phData](https://www.phdata.io/)
and
[Tim Robertson](https://twitter.com/timrobertson100) at [GBIF](https://www.gbif.org/) who helped us
tremendously.
