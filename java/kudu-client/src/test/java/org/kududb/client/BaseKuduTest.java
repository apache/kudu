// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.master.Master;
import org.kududb.util.NetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

public class BaseKuduTest {

  private static final Logger LOG = LoggerFactory.getLogger(BaseKuduTest.class);

  private static final int DEFAULT_MASTER_RPC_PORT = 7051;
  private static final String START_CLUSTER = "startCluster";
  private static final String NUM_MASTERS_PROP = "NUM_MASTERS";

  // TS and Master ports will be assigned starting with this one.
  private static final int PORT_START = 64030;

  // Comma separate describing the master addresses and ports.
  protected static String masterAddresses;
  protected static List<HostAndPort> masterHostPorts;

  protected static final int DEFAULT_SLEEP = 50000;
  protected static final int NUM_TABLET_SERVERS = 3;
  protected static final int DEFAULT_NUM_MASTERS = 3;

  static final List<Thread> PROCESS_INPUT_PRINTERS = new ArrayList<>();

  // Number of masters that will be started for this test if we're starting
  // a cluster.
  protected static final int NUM_MASTERS =
      Integer.getInteger(NUM_MASTERS_PROP, DEFAULT_NUM_MASTERS);

  // Map of ports to master servers.
  static final Map<Integer, Process> MASTERS = new ConcurrentHashMap<>(NUM_MASTERS);

  // Map of ports to tablet servers.
  static final Map<Integer, Process> TABLET_SERVERS =
      new ConcurrentHashMap<Integer, Process>(NUM_TABLET_SERVERS);

  // We create both versions of the client for ease of use.
  protected static AsyncKuduClient client;
  protected static KuduClient syncClient;
  protected static Schema basicSchema = getBasicSchema();
  protected static Schema allTypesSchema = getSchemaWithAllTypes();
  protected static boolean startCluster;

  private static List<String> tableNames = new ArrayList<>();
  private static List<String> pathsToDelete = new ArrayList<>();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    LOG.info("Setting up before class...");
    // The following props are set via kudu-client's pom.
    String baseDirPath = TestUtils.getBaseDir();
    startCluster = Boolean.parseBoolean(System.getProperty(START_CLUSTER, "true"));

    if (startCluster) {
      long now = System.currentTimeMillis();
      LOG.info("Starting {} masters...", NUM_MASTERS);
      int port = startMasters(PORT_START, NUM_MASTERS, baseDirPath);
      LOG.info("Starting {} tablet servers...", NUM_TABLET_SERVERS);
      for (int i = 0; i < NUM_TABLET_SERVERS; i++) {
        port = TestUtils.findFreePort(port);
        String dataDirPath = baseDirPath + "/ts-" + i + "-" + now;
        String flagsPath = TestUtils.getFlagsPath();
        String[] tsCmdLine = {
            TestUtils.findBinary("kudu-tserver"),
            "--flagfile=" + flagsPath,
            "--fs_wal_dir=" + dataDirPath,
            "--fs_data_dirs=" + dataDirPath,
            "--tserver_master_addrs=" + masterAddresses,
            "--rpc_bind_addresses=127.0.0.1:" + port};
        TABLET_SERVERS.put(port, configureAndStartProcess(tsCmdLine));
        port++;

        if (flagsPath.startsWith(baseDirPath)) {
          // We made a temporary copy of the flags; delete them later.
          pathsToDelete.add(flagsPath);
        }
        pathsToDelete.add(dataDirPath);
      }
    } else {
      masterAddresses = TestUtils.getMasterAddresses();
      masterHostPorts = NetUtil.parseStrings(masterAddresses, DEFAULT_MASTER_RPC_PORT);
    }
    LOG.info("Creating new Kudu client...");
    client = new AsyncKuduClient.AsyncKuduClientBuilder(masterAddresses).build();
    syncClient = new KuduClient(client);
    LOG.info("Waiting for tablet servers...");
    if (!waitForTabletServers(NUM_TABLET_SERVERS)) {
      fail("Couldn't get " + NUM_MASTERS + " tablet servers running, aborting");
    }
  }

  /**
   * Start the specified number of master servers with ports starting from a specified
   * number. Finds free web and RPC ports up front for all of the masters first, then
   * starts them on those ports, populating 'masters' map.
   * @param masterStartPort The starting of the port range for the masters.
   * @param numMasters Number of masters to start.
   * @param baseDirPath Kudu base directory.
   * @return Next free port.
   * @throws Exception If we are unable to start the masters.
   */
  static int startMasters(int masterStartPort, int numMasters,
                          String baseDirPath) throws Exception {
    LOG.info("Starting {} masters...", numMasters);
    // Get the list of web and RPC ports to use for the master consensus configuration:
    // request NUM_MASTERS * 2 free ports as we want to also reserve the web
    // ports for the consensus configuration.
    List<Integer> ports = TestUtils.findFreePorts(masterStartPort, numMasters * 2);
    int lastFreePort = ports.get(ports.size() - 1);
    List<Integer> masterRpcPorts = Lists.newArrayListWithCapacity(numMasters);
    List<Integer> masterWebPorts = Lists.newArrayListWithCapacity(numMasters);
    masterHostPorts = Lists.newArrayListWithCapacity(numMasters);
    for (int i = 0; i < numMasters * 2; i++) {
      if (i % 2 == 0) {
        masterRpcPorts.add(ports.get(i));
        masterHostPorts.add(HostAndPort.fromParts("127.0.0.1", ports.get(i)));
      } else {
        masterWebPorts.add(ports.get(i));
      }
    }
    masterAddresses = NetUtil.hostsAndPortsToString(masterHostPorts);
    for (int i = 0; i < numMasters; i++) {
      long now = System.currentTimeMillis();
      String dataDirPath = baseDirPath + "/master-" + i + "-" + now;
      String flagsPath = TestUtils.getFlagsPath();
      // The web port must be reserved in the call to findFreePorts above and specified
      // to avoid the scenario where:
      // 1) findFreePorts finds RPC ports a, b, c for the 3 masters.
      // 2) start master 1 with RPC port and let it bind to any (specified as 0) web port.
      // 3) master 1 happens to bind to port b for the web port, as master 2 hasn't been
      // started yet and findFreePort(s) is "check-time-of-use" (it does not reserve the
      // ports, only checks that when it was last called, these ports could be used).
      List<String> masterCmdLine = Lists.newArrayList(
          TestUtils.findBinary("kudu-master"),
          "--flagfile=" + flagsPath,
          "--fs_wal_dir=" + dataDirPath,
          "--fs_data_dirs=" + dataDirPath,
          "--rpc_bind_addresses=127.0.0.1:" + masterRpcPorts.get(i),
          "--webserver_port=" + masterWebPorts.get(i));
      if (numMasters > 1) {
        masterCmdLine.add("--master_addresses=" + masterAddresses);
      }
      MASTERS.put(masterRpcPorts.get(i),
          configureAndStartProcess(masterCmdLine.toArray(new String[masterCmdLine.size()])));

      if (flagsPath.startsWith(baseDirPath)) {
        // We made a temporary copy of the flags; delete them later.
        pathsToDelete.add(flagsPath);
      }
      pathsToDelete.add(dataDirPath);
    }
    return lastFreePort + 1;
  }

  /**
   * Wait up to DEFAULT_SLEEP for an expected count of TS to connect to the master
   * @param expected How many TS are expected
   * @return true if there are at least as many TS as expected, otherwise false
   */
  static boolean waitForTabletServers(int expected) throws Exception {
    int count = 0;
    Stopwatch stopwatch = new Stopwatch().start();
    while (count < expected && stopwatch.elapsedMillis() < DEFAULT_SLEEP) {
      Thread.sleep(200);
      Deferred<ListTabletServersResponse> d = client.listTabletServers();
      d.addErrback(defaultErrorCB);
      count = d.join(DEFAULT_SLEEP).getTabletServersCount();
    }
    return count >= expected;
  }

  /**
   * Starts a process using the provided command and configures it to be daemon,
   * redirects the stderr to stdout, and starts a thread that will read from the process' input
   * stream and redirect that to LOG.
   * @param command Process and options
   * @return The started process
   * @throws Exception Exception if an error prevents us from starting the process,
   * or if we were able to start the process but noticed that it was then killed (in which case
   * we'll log the exit value).
   */
  static Process configureAndStartProcess(String[] command) throws Exception {
    LOG.info("Starting process: {}", Joiner.on(" ").join(command));
    ProcessBuilder processBuilder = new ProcessBuilder(command);
    processBuilder.redirectErrorStream(true);
    Process proc = processBuilder.start();
    ProcessInputStreamLogPrinterRunnable printer =
        new ProcessInputStreamLogPrinterRunnable(proc.getInputStream());
    Thread thread = new Thread(printer);
    thread.setDaemon(true);
    thread.setName(command[0]);
    PROCESS_INPUT_PRINTERS.add(thread);
    thread.start();

    Thread.sleep(300);
    try {
      int ev = proc.exitValue();
      throw new Exception("We tried starting a process (" + command[0] + ") but it exited with " +
          "value=" + ev);
    } catch (IllegalThreadStateException ex) {
      // This means the process is still alive, it's like reverse psychology.
    }
    return proc;
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    try {
      if (client != null) {
        Deferred<ArrayList<Void>> d = client.shutdown();
        d.addErrback(defaultErrorCB);
        d.join(DEFAULT_SLEEP);
        // No need to explicitly shutdown the sync client,
        // shutting down the async client effectively does that.
      }
    } finally {
      if (startCluster) {
        for (Iterator<Process> masterIter = MASTERS.values().iterator(); masterIter.hasNext(); ) {
          masterIter.next().destroy();
          masterIter.remove();
        }
        for (Iterator<Process> tsIter = TABLET_SERVERS.values().iterator(); tsIter.hasNext(); ) {
          tsIter.next().destroy();
          tsIter.remove();
        }
        for (Thread thread : PROCESS_INPUT_PRINTERS) {
          thread.interrupt();
        }
      }
      for (String path : pathsToDelete) {
        try {
          File f = new File(path);
          if (f.isDirectory()) {
            FileUtils.deleteDirectory(f);
          } else {
            f.delete();
          }
        } catch (Exception e) {
          LOG.warn("Could not delete path {}", path, e);
        }
      }
    }
  }

  protected static void createTable(String tableName, Schema schema, CreateTableBuilder builder) {
    LOG.info("Creating table: {}", tableName);
    Deferred<CreateTableResponse> d = client.createTable(tableName, schema, builder);
    final AtomicBoolean gotError = new AtomicBoolean(false);
    d.addErrback(new Callback<Object, Object>() {
      @Override
      public Object call(Object arg) throws Exception {
        gotError.set(true);
        LOG.error("Error : " + arg);
        return null;
      }
    });
    try {
      d.join(DEFAULT_SLEEP);
    } catch (Exception e) {
      fail("Timed out");
    }
    if (gotError.get()) {
      fail("Got error during table creation, is the Kudu master running at " +
          masterAddresses + "?");
    }
    tableNames.add(tableName);
  }

  /**
   * Counts the rows from the {@code scanner} until exhaustion. It doesn't require the scanner to
   * be new, so it can be used to finish scanning a previously-started scan.
   */
  protected static int countRowsInScan(AsyncKuduScanner scanner)
      throws Exception {
    final AtomicInteger counter = new AtomicInteger();

    Callback<Object, RowResultIterator> cb = new Callback<Object, RowResultIterator>() {
      @Override
      public Object call(RowResultIterator arg) throws Exception {
        if (arg == null) return null;
        counter.addAndGet(arg.getNumRows());
        return null;
      }
    };

    while (scanner.hasMoreRows()) {
      Deferred<RowResultIterator> data = scanner.nextRows();
      data.addCallbacks(cb, defaultErrorCB);
      data.join(DEFAULT_SLEEP);
    }

    Deferred<RowResultIterator> closer = scanner.close();
    closer.addCallbacks(cb, defaultErrorCB);
    closer.join(DEFAULT_SLEEP);
    return counter.get();
  }

  protected List<String> scanTableToStrings(KuduTable table) throws Exception {
    List<String> rowStrings = Lists.newArrayList();
    KuduScanner scanner = syncClient.newScannerBuilder(table).build();
    while (scanner.hasMoreRows()) {
      RowResultIterator rows = scanner.nextRows();
      for (RowResult r : rows) {
        rowStrings.add(r.rowToString());
      }
    }
    Collections.sort(rowStrings);
    return rowStrings;
  }

  private static final int[] KEYS = new int[] {10, 20, 30};
  protected static KuduTable createFourTabletsTableWithNineRows(String tableName) throws
      Exception {
    CreateTableBuilder builder = new CreateTableBuilder();
    PartialRow splitRow = basicSchema.newPartialRow();
    for (int i : KEYS) {
      splitRow.addInt(0, i);
      builder.addSplitRow(splitRow);
    }
    createTable(tableName, basicSchema, builder);
    AsyncKuduSession session = client.newSession();

    // create a table with on empty tablet and 3 tablets of 3 rows each
    KuduTable table = openTable(tableName);
    for (int key1 : KEYS) {
      for (int key2 = 1; key2 <= 3; key2++) {
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        row.addInt(0, key1 + key2);
        row.addInt(1, key1);
        row.addInt(2, key2);
        row.addString(3, "a string");
        row.addBoolean(4, true);
        session.apply(insert).join(DEFAULT_SLEEP);
      }
    }
    session.close().join(DEFAULT_SLEEP);
    return table;
  }

  public static Schema getSchemaWithAllTypes() {
    List<ColumnSchema> columns =
        ImmutableList.of(
            new ColumnSchema.ColumnSchemaBuilder("int8", Type.INT8).key(true).build(),
            new ColumnSchema.ColumnSchemaBuilder("int16", Type.INT16).build(),
            new ColumnSchema.ColumnSchemaBuilder("int32", Type.INT32).build(),
            new ColumnSchema.ColumnSchemaBuilder("int64", Type.INT64).build(),
            new ColumnSchema.ColumnSchemaBuilder("bool", Type.BOOL).build(),
            new ColumnSchema.ColumnSchemaBuilder("float", Type.FLOAT).build(),
            new ColumnSchema.ColumnSchemaBuilder("double", Type.DOUBLE).build(),
            new ColumnSchema.ColumnSchemaBuilder("string", Type.STRING).build(),
            new ColumnSchema.ColumnSchemaBuilder("binary", Type.BINARY).build(),
            new ColumnSchema.ColumnSchemaBuilder("null", Type.STRING).nullable(true).build(),
            new ColumnSchema.ColumnSchemaBuilder("timestamp", Type.TIMESTAMP).build());

    return new Schema(columns);
  }

  public static Schema getBasicSchema() {
    ArrayList<ColumnSchema> columns = new ArrayList<ColumnSchema>(4);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("column1_i", Type.INT32).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("column2_i", Type.INT32).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("column3_s", Type.STRING)
        .nullable(true)
        .desiredBlockSize(4096)
        .encoding(ColumnSchema.Encoding.DICT_ENCODING)
        .build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("column4_b", Type.BOOL).build());
    return new Schema(columns);
  }

  protected Insert createBasicSchemaInsert(KuduTable table, int key) {
    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();
    row.addInt(0, key);
    row.addInt(1, 2);
    row.addInt(2, 3);
    row.addString(3, "a string");
    row.addBoolean(4, true);
    return insert;
  }

  static Callback<Object, Object> defaultErrorCB = new Callback<Object, Object>() {
    @Override
    public Object call(Object arg) throws Exception {
      if (arg == null) return null;
      if (arg instanceof Exception) {
        LOG.warn("Got exception", (Exception) arg);
      } else {
        LOG.warn("Got an error response back " + arg);
      }
      return new Exception("Can't recover from error, see previous WARN");
    }
  };

  /**
   * Helper method to open a table. It sets the default sleep time when joining on the Deferred.
   * @param name Name of the table
   * @return A KuduTable
   * @throws Exception MasterErrorException if the table doesn't exist
   */
  protected static KuduTable openTable(String name) throws Exception {
    Deferred<KuduTable> d = client.openTable(name);
    return d.join(DEFAULT_SLEEP);
  }

  /**
   * Helper method to easily kill a tablet server that serves the given table's only tablet's
   * leader. The currently running test case will be failed if there's more than one tablet,
   * if the tablet has no leader after some retries, or if the tablet server was already killed.
   *
   * This method is thread-safe.
   * @param table a KuduTable which will get its single tablet's leader killed.
   * @throws Exception
   */
  protected static void killTabletLeader(KuduTable table) throws Exception {
    LocatedTablet.Replica leader = null;
    DeadlineTracker deadlineTracker = new DeadlineTracker();
    deadlineTracker.setDeadline(DEFAULT_SLEEP);
    while (leader == null) {
      if (deadlineTracker.timedOut()) {
        fail("Timed out while trying to find a leader for this table: " + table.getName());
      }
      List<LocatedTablet> tablets = table.getTabletsLocations(DEFAULT_SLEEP);
      if (tablets.isEmpty() || tablets.size() > 1) {
        fail("Currently only support killing leaders for tables containing 1 tablet, table " +
            table.getName() + " has " + tablets.size());
      }
      LocatedTablet tablet = tablets.get(0);
      if (tablet.getReplicas().size() == 1) {
        fail("Table " + table.getName() + " only has 1 tablet, please enable replication");
      }
      leader = tablet.getLeaderReplica();
      if (leader == null) {
        LOG.info("Sleeping while waiting for a tablet LEADER to arise, currently slept " +
            deadlineTracker.getElapsedMillis() + "ms");
        Thread.sleep(50);
      }
    }

    Integer port = leader.getRpcPort();
    Process ts = TABLET_SERVERS.get(port);
    if (ts == null) {
      // The TS is already dead, good.
      return;
    }
    LOG.info("Killing server at port " + port);
    ts.destroy();
    ts.waitFor();
    TABLET_SERVERS.remove(port);
  }

  /**
   * Helper method to easily kill the leader master.
   *
   * This method is thread-safe.
   * @throws Exception If there is an error finding or killing the leader master.
   */
  protected static void killMasterLeader() throws Exception {
    int leaderPort = findLeaderMasterPort();
    Process master = MASTERS.get(leaderPort);
    if (master == null) {
      // The master is already dead, good.
      return;
    }
    LOG.info("Killing master at port " + leaderPort);
    master.destroy();
    master.waitFor();
    MASTERS.remove(leaderPort);
  }

  /**
   * Find the port of the leader master in order to retrieve it from the port to process map.
   * @return The port of the leader master.
   * @throws Exception If we are unable to find the leader master.
   */
  protected static int findLeaderMasterPort() throws Exception {
    Stopwatch sw = new Stopwatch().start();
    int leaderPort = -1;
    while (leaderPort == -1 && sw.elapsedMillis() < DEFAULT_SLEEP) {
      Deferred<Master.GetTableLocationsResponsePB> masterLocD = client.getMasterTableLocationsPB();
      Master.GetTableLocationsResponsePB r = masterLocD.join(DEFAULT_SLEEP);
      leaderPort = r.getTabletLocations(0)
          .getReplicas(0)
          .getTsInfo()
          .getRpcAddresses(0)
          .getPort();
    }
    if (leaderPort == -1) {
      fail("No leader master found after " + DEFAULT_SLEEP + " ms.");
    }
    return leaderPort;
  }

  /**
   * Return the comma-separated list of "host:port" pairs that describes the master
   * config for this cluster.
   * @return The master config string.
   */
  protected static String getMasterAddresses() {
    return masterAddresses;
  }

  /**
   * Helper runnable that can log what the processes are sending on their stdout and stderr that
   * we'd otherwise miss.
   */
  static class ProcessInputStreamLogPrinterRunnable implements Runnable {

    private final InputStream is;

    public ProcessInputStreamLogPrinterRunnable(InputStream is) {
      this.is = is;
    }

    @Override
    public void run() {
      try {
        String line;
        BufferedReader in = new BufferedReader(new InputStreamReader(is));
        while ((line = in.readLine()) != null) {
          LOG.info(line);
        }
        in.close();
      }
      catch (Exception e) {
        if (!e.getMessage().contains("Stream closed")) {
          LOG.error("Caught error while reading a process' output", e);
        }
      }
    }
  }

}
