Asynchronous Native Java Client for Kudu

System Requirements
------------------------------------------------------------

- Java 6
- Maven 3
- protobuf 2.5.0


Building the Client
------------------------------------------------------------

$ ./kudu-client/dev-support/build-proto.sh
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

# mvn test -Dstartcluster=false

Since by default the test will look for a master on
localhost:64000, you may want to override this by passing
-DmasterAddress and/or -DmasterPort.

To use a different flags file, pass the path to
-DflagsPath.
