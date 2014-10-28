Copyright (c) 2014, Cloudera, inc.
Confidential Cloudera Information: Covered by NDA.

Asynchronous Native Java Client for Kudu

System Requirements
------------------------------------------------------------

- Java 6
- Maven 3
- protobuf 2.5.0


Building the Client
------------------------------------------------------------

$ mvn package -DskipTests

The client jar will can then be found at kudu-client/target.


Running the Tests
------------------------------------------------------------

Most of the unit tests will start their own cluster but it
is also possible to provide your own.

By default, the unit tests will start a master and a tablet
server using the flags file located in the tests' resource
directory. Make sure that both "kudu-master" and
"kudu-tablet_server" are in the path. The build script does
the following:

$ export PATH=$(pwd)/build/latest/:$PATH

Once everything is setup correctly, run:

$ mvn test

In order to point the unit tests to an existing cluster,
you need to use a command line like this one:

$ mvn test -DstartCluster=false

Since by default the test will look for a master on
localhost:64000, you may want to override this by passing
-DmasterAddress and/or -DmasterPort.

To use a different flags file, pass the path to
-DflagsPath.


State of Eclipse integration
------------------------------------------------------------

Maven projects can be integrated with Eclipse in one of two
ways:

1. Import a Maven project using Eclipse's m2e plugin.
2. Generate Eclipse project files using maven's
   maven-eclipse-plugin plugin.

Each approach has its own pros and cons.

## m2e integration (Eclipse to Maven)

The m2e approach is generally recommended as m2e is still
under active development, unlike maven-eclipse-plugin. Much
of the complexity comes from how m2e maps maven lifecycle
phases to Eclipse build actions. The problem is that m2e
must be told what to do with each maven plugin, and this
information is either conveyed through explicit mapping
metadata found in pom.xml, or in an m2e "extension". m2e
ships with extensions for some of the common maven plugins,
but not for maven-antrun-plugin or maven-protoc-plugin. The
explicit metadata mapping found in kudu-client/pom.xml has
placated m2e in both cases (in Eclipse see
kudu-client->Properties->Maven->Lifecycle Mapping).
Nevertheless, maven-protoc-plugin isn't being run correctly.

To work around this, you can download, build, and install a
user-made m2e extension for maven-protoc-plugin:

  http://www.masterzen.fr/2011/12/25/protobuf-maven-m2e-and-eclipse-are-on-a-boat

See http://wiki.eclipse.org/M2E_plugin_execution_not_covered
for far more excruciating detail.

## maven-eclipse-plugin (Maven to Eclipse)

The maven-eclipse-plugin approach, despite being old
fashioned and largely unsupported, is easier to use. The
very first time you want to use it, run the following:

$ mvn -Declipse.workspace=<path-to-eclipse-workspace> eclipse:configure-workspace

This will add the M2_REPO classpath variable to Eclipse. You
can verify this in
Preferences->Java->Build Path->Classpath Variables. It
should be set to `/home/<user>/.m2/repository`.

To generate the Eclipse project files, run:

$ mvn eclipse:eclipse

If you want to look at Javadoc/source in Eclipse for
dependent artifacts, run:

$ mvn eclipse:eclipse -DdownloadJavadocs=true -DdownloadSources=true

So what's the problem with maven-eclipse-plugin? The issue
lies with maven-protoc-plugin. Because all of our .proto
files are in src/kudu, the "resource path" in
maven-protoc-plugin must be absolute and prefixed with
${project.baseDir). This absolute path is copied verbatim
to an Eclipse .classpath <classpathentry/>, and Eclipse
doesn't know what to do with it, causing it avoid building
kudu-client altogether. Other plugins (like
maven-avro-plugin) don't seem to have this problem, so it's
likely a bug in maven-protoc-plugin.

There's a simple workaround: delete the errant folder within
Eclipse and refresh the kudu-client project.
