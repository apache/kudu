/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.kudu.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kudu.util.NetUtil;
import org.apache.kudu.util.SecurityUtil;

/**
 * Utility class to start and manipulate Kudu clusters. Relies on being IN the Kudu source code with
 * both the kudu-master and kudu-tserver binaries already compiled. {@link BaseKuduTest} should be
 * extended instead of directly using this class in almost all cases.
 */
public class MiniKuduCluster implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(MiniKuduCluster.class);

  // TS and Master ports will be assigned starting with this one.
  private static final int PORT_START = 64030;

  // List of threads that print
  private final List<Thread> PROCESS_INPUT_PRINTERS = new ArrayList<>();

  // Map of ports to master servers.
  private final Map<Integer, Process> masterProcesses = new ConcurrentHashMap<>();

  // Map of ports to tablet servers.
  private final Map<Integer, Process> tserverProcesses = new ConcurrentHashMap<>();

  // Map of ports to process command lines. Never removed from. Used to restart processes.
  private final Map<Integer, List<String>> commandLines = new ConcurrentHashMap<>();

  private final List<String> pathsToDelete = new ArrayList<>();
  private final List<HostAndPort> masterHostPorts = new ArrayList<>();
  private final List<Integer> tserverPorts = new ArrayList<>();
  private final ImmutableList<String> extraTserverFlags;
  private final ImmutableList<String> extraMasterFlags;

  // Client we can use for common operations.
  private KuduClient syncClient;
  private final int defaultTimeoutMs;

  private String masterAddresses;

  private final String bindHost = TestUtils.getUniqueLocalhost();
  private Path keytab;
  private MiniKdc miniKdc;

  private MiniKuduCluster(final int defaultTimeoutMs,
                          final List<String> extraTserverFlags,
                          final List<String> extraMasterFlags) {
    this.defaultTimeoutMs = defaultTimeoutMs;
    this.extraTserverFlags = ImmutableList.copyOf(extraTserverFlags);
    this.extraMasterFlags = ImmutableList.copyOf(extraMasterFlags);
  }

  /**
   * Enable Kerberos security for this cluster, start the MiniKdc, and log in
   * the required subjects.
   */
  private void startKerberos() throws Exception {
    miniKdc = MiniKdc.withDefaults();
    miniKdc.start();

    keytab = miniKdc.createServiceKeytab("kudu/" + bindHost);

    miniKdc.createUserPrincipal("testuser");
    miniKdc.kinit("testuser");
    System.setProperty("java.security.krb5.conf",
        miniKdc.getEnvVars().get("KRB5_CONFIG"));
    System.setProperty(SecurityUtil.KUDU_TICKETCACHE_PROPERTY,
        miniKdc.getEnvVars().get("KRB5CCNAME"));
  }

  /**
   * Start the master and tablet server processes.
   * @param numMasters the number of masters to start.
   * @param numTservers the number of tablet servers to start.
   */
  private void start(int numMasters, int numTservers) throws Exception {
    startCluster(numMasters, numTservers);

    KuduClient.KuduClientBuilder kuduClientBuilder =
        new KuduClient.KuduClientBuilder(getMasterAddresses());
    kuduClientBuilder.defaultAdminOperationTimeoutMs(defaultTimeoutMs);
    kuduClientBuilder.defaultOperationTimeoutMs(defaultTimeoutMs);
    syncClient = kuduClientBuilder.build();
  }

  /**
   * Wait up to this instance's "default timeout" for an expected count of TS to
   * connect to the master.
   * @param expected How many TS are expected
   * @return true if there are at least as many TS as expected, otherwise false
   */
  public boolean waitForTabletServers(int expected) throws Exception {
    int count = 0;
    Stopwatch stopwatch = Stopwatch.createStarted();
    while (count < expected && stopwatch.elapsed(TimeUnit.MILLISECONDS) < defaultTimeoutMs) {
      Thread.sleep(200);
      count = syncClient.listTabletServers().getTabletServersCount();
    }
    return count >= expected;
  }

  /**
   * Starts a Kudu cluster composed of the provided masters and tablet servers.
   * @param numMasters how many masters to start
   * @param numTservers how many tablet servers to start
   */
  private void startCluster(int numMasters, int numTservers) throws Exception {
    Preconditions.checkArgument(numMasters > 0, "Need at least one master");
    // The following props are set via kudu-client's pom.
    String baseDirPath = TestUtils.getBaseDir();

    long now = System.currentTimeMillis();
    LOG.info("Starting {} masters...", numMasters);
    int startPort = startMasters(PORT_START, numMasters, baseDirPath, bindHost);

    LOG.info("Starting {} tablet servers...", numTservers);
    List<Integer> ports = TestUtils.findFreePorts(startPort, numTservers * 2);
    for (int i = 0; i < numTservers; i++) {
      int rpcPort = ports.get(i * 2);
      tserverPorts.add(rpcPort);
      String tsBaseDirPath = baseDirPath + "/ts-" + i + "-" + now;
      new File(tsBaseDirPath).mkdir();
      String logDirPath = tsBaseDirPath + "/logs";
      new File(logDirPath).mkdir();
      String dataDirPath = tsBaseDirPath + "/data";
      String flagsPath = TestUtils.getFlagsPath();

      List<String> commandLine = Lists.newArrayList(
          TestUtils.findBinary("kudu-tserver"),
          "--flagfile=" + flagsPath,
          "--log_dir=" + logDirPath,
          "--fs_wal_dir=" + dataDirPath,
          "--fs_data_dirs=" + dataDirPath,
          "--flush_threshold_mb=1",
          "--ipki_server_key_size=1024",
          "--tserver_master_addrs=" + masterAddresses,
          "--webserver_interface=" + bindHost,
          "--local_ip_for_outbound_sockets=" + bindHost,
          "--webserver_port=" + (rpcPort + 1),
          "--rpc_bind_addresses=" + bindHost + ":" + rpcPort);

      if (miniKdc != null) {
        commandLine.add("--keytab_file=" + keytab);
        commandLine.add("--principal=kudu/" + bindHost);
        commandLine.add("--rpc_authentication=required");
        commandLine.add("--superuser_acl=testuser");
      }

      commandLine.addAll(extraTserverFlags);

      tserverProcesses.put(rpcPort, configureAndStartProcess(rpcPort, commandLine));
      commandLines.put(rpcPort, commandLine);

      if (flagsPath.startsWith(baseDirPath)) {
        // We made a temporary copy of the flags; delete them later.
        pathsToDelete.add(flagsPath);
      }
      pathsToDelete.add(tsBaseDirPath);
    }
  }

  /**
   * Start the specified number of master servers with ports starting from a specified
   * number. Finds free web and RPC ports up front for all of the masters first, then
   * starts them on those ports, populating 'masters' map.
   * @param masterStartPort the starting point of the port range for the masters
   * @param numMasters number of masters to start
   * @param baseDirPath the base directory where the mini cluster stores its data
   * @return the next free port
   * @throws Exception if we are unable to start the masters
   */
  private int startMasters(int masterStartPort,
                           int numMasters,
                           String baseDirPath,
                           String bindHost) throws Exception {
    // Get the list of web and RPC ports to use for the master consensus configuration:
    // request NUM_MASTERS * 2 free ports as we want to also reserve the web
    // ports for the consensus configuration.
    List<Integer> ports = TestUtils.findFreePorts(masterStartPort, numMasters * 2);
    int lastFreePort = ports.get(ports.size() - 1);
    List<Integer> masterRpcPorts = Lists.newArrayListWithCapacity(numMasters);
    List<Integer> masterWebPorts = Lists.newArrayListWithCapacity(numMasters);
    for (int i = 0; i < numMasters * 2; i++) {
      if (i % 2 == 0) {
        masterRpcPorts.add(ports.get(i));
        masterHostPorts.add(HostAndPort.fromParts(bindHost, ports.get(i)));
      } else {
        masterWebPorts.add(ports.get(i));
      }
    }
    masterAddresses = NetUtil.hostsAndPortsToString(masterHostPorts);
    long now = System.currentTimeMillis();
    for (int i = 0; i < numMasters; i++) {
      int port = masterRpcPorts.get(i);
      String masterBaseDirPath = baseDirPath + "/master-" + i + "-" + now;
      new File(masterBaseDirPath).mkdir();
      String logDirPath = masterBaseDirPath + "/logs";
      new File(logDirPath).mkdir();
      String dataDirPath = masterBaseDirPath + "/data";
      String flagsPath = TestUtils.getFlagsPath();
      // The web port must be reserved in the call to findFreePorts above and specified
      // to avoid the scenario where:
      // 1) findFreePorts finds RPC ports a, b, c for the 3 masters.
      // 2) start master 1 with RPC port and let it bind to any (specified as 0) web port.
      // 3) master 1 happens to bind to port b for the web port, as master 2 hasn't been
      // started yet and findFreePort(s) is "check-time-of-use" (it does not reserve the
      // ports, only checks that when it was last called, these ports could be used).
      List<String> commandLine = Lists.newArrayList(
          TestUtils.findBinary("kudu-master"),
          "--flagfile=" + flagsPath,
          "--log_dir=" + logDirPath,
          "--fs_wal_dir=" + dataDirPath,
          "--fs_data_dirs=" + dataDirPath,
          "--ipki_ca_key_size=1024",
          "--ipki_server_key_size=1024",
          "--tsk_num_rsa_bits=512",
          "--webserver_interface=" + bindHost,
          "--local_ip_for_outbound_sockets=" + bindHost,
          "--rpc_bind_addresses=" + bindHost + ":" + port,
          "--webserver_port=" + masterWebPorts.get(i),
          "--raft_heartbeat_interval_ms=200"); // make leader elections faster for faster tests

      if (numMasters > 1) {
        commandLine.add("--master_addresses=" + masterAddresses);
      }

      if (miniKdc != null) {
        commandLine.add("--keytab_file=" + keytab);
        commandLine.add("--principal=kudu/" + bindHost);
        commandLine.add("--rpc_authentication=required");
        commandLine.add("--superuser_acl=testuser");
      }

      commandLine.addAll(extraMasterFlags);

      masterProcesses.put(port, configureAndStartProcess(port, commandLine));
      commandLines.put(port, commandLine);

      if (flagsPath.startsWith(baseDirPath)) {
        // We made a temporary copy of the flags; delete them later.
        pathsToDelete.add(flagsPath);
      }
      pathsToDelete.add(masterBaseDirPath);
    }
    return lastFreePort + 1;
  }

  /**
   * Starts a process using the provided command and configures it to be daemon,
   * redirects the stderr to stdout, and starts a thread that will read from the process' input
   * stream and redirect that to LOG.
   * @param port rpc port used to identify the process
   * @param command process and options
   * @return The started process
   * @throws Exception Exception if an error prevents us from starting the process,
   * or if we were able to start the process but noticed that it was then killed (in which case
   * we'll log the exit value).
   */
  private Process configureAndStartProcess(int port, List<String> command) throws Exception {
    ProcessBuilder processBuilder = new ProcessBuilder(command);
    processBuilder.redirectErrorStream(true);
    if (miniKdc != null) {
      processBuilder.environment().putAll(miniKdc.getEnvVars());
    }
    Process proc = processBuilder.start();
    ProcessInputStreamLogPrinterRunnable printer =
        new ProcessInputStreamLogPrinterRunnable(proc.getInputStream());
    Thread thread = new Thread(printer);
    thread.setDaemon(true);
    thread.setName(Iterables.getLast(Splitter.on(File.separatorChar).split(command.get(0))) + ":" + port);
    PROCESS_INPUT_PRINTERS.add(thread);
    thread.start();

    Thread.sleep(300);
    try {
      int ev = proc.exitValue();
      throw new Exception(String.format(
          "We tried starting a process (%s) but it exited with value=%s", command.get(0), ev));
    } catch (IllegalThreadStateException ex) {
      // This means the process is still alive, it's like reverse psychology.
    }
    return proc;
  }

  /**
   * Starts a previously killed master process on the specified port.
   * @param port which port the master was listening on for RPCs
   * @throws Exception
   */
  public void restartDeadMasterOnPort(int port) throws Exception {
    restartDeadProcessOnPort(port, masterProcesses);
  }

  /**
   * Starts a previously killed tablet server process on the specified port.
   * @param port which port the TS was listening on for RPCs
   * @throws Exception
   */
  public void restartDeadTabletServerOnPort(int port) throws Exception {
    restartDeadProcessOnPort(port, tserverProcesses);
  }

  private void restartDeadProcessOnPort(int port, Map<Integer, Process> map) throws Exception {
    if (!commandLines.containsKey(port)) {
      String message = "Cannot start process on unknown port " + port;
      LOG.warn(message);
      throw new RuntimeException(message);
    }

    if (map.containsKey(port)) {
      String message = "Process already exists on port " + port;
      LOG.warn(message);
      throw new RuntimeException(message);
    }

    map.put(port, configureAndStartProcess(port, commandLines.get(port)));
  }

  /**
   * Kills the TS listening on the provided port. Doesn't do anything if the TS was already killed.
   * @param port port on which the tablet server is listening on
   * @throws InterruptedException
   */
  public void killTabletServerOnPort(int port) throws InterruptedException {
    Process ts = tserverProcesses.remove(port);
    if (ts == null) {
      // The TS is already dead, good.
      return;
    }
    LOG.info("Killing server at port " + port);
    destroyAndWaitForProcess(ts);
  }

  /**
   * Kills all tablet servers.
   * @throws InterruptedException
   */
  public void killTabletServers() throws InterruptedException {
    for (Process tserver : tserverProcesses.values()) {
      destroyAndWaitForProcess(tserver);
    }
    tserverProcesses.clear();
  }

  /**
   * Restarts any tablet servers which were previously killed.
   */
  public void restartDeadTabletServers() throws Exception {
    for (int port : tserverPorts) {
      if (tserverProcesses.containsKey(port)) continue;
      restartDeadTabletServerOnPort(port);
    }
  }

  /**
   * Kills the master listening on the provided port. Doesn't do anything if the master was
   * already killed.
   * @param port port on which the master is listening on
   * @throws InterruptedException
   */
  public void killMasterOnPort(int port) throws InterruptedException {
    Process master = masterProcesses.remove(port);
    if (master == null) {
      // The master is already dead, good.
      return;
    }
    LOG.info("Killing master at port " + port);
    destroyAndWaitForProcess(master);
  }

  /** {@override} */
  @Override
  public void close() {
    shutdown();
  }

  /**
   * Stops all the processes and deletes the folders used to store data and the flagfile.
   */
  public void shutdown() {
    for (Iterator<Process> masterIter = masterProcesses.values().iterator(); masterIter.hasNext(); ) {
      try {
        destroyAndWaitForProcess(masterIter.next());
      } catch (InterruptedException e) {
        // Need to continue cleaning up.
      }
      masterIter.remove();
    }

    for (Iterator<Process> tsIter = tserverProcesses.values().iterator(); tsIter.hasNext(); ) {
      try {
        destroyAndWaitForProcess(tsIter.next());
      } catch (InterruptedException e) {
        // Need to continue cleaning up.
      }
      tsIter.remove();
    }

    // Whether we were interrupted or not above we still destroyed all the processes, so the input
    // printers will hit EOFs and stop.
    for (Thread thread : PROCESS_INPUT_PRINTERS) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        // Need to continue cleaning up.
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
        LOG.warn(String.format("Could not delete path %s", path), e);
      }
    }

    if (miniKdc != null) {
      try {
        miniKdc.close();
      } catch (IOException e) {
        LOG.warn("Unable to close MiniKdc", e);
      }
    }
  }

  private void destroyAndWaitForProcess(Process process) throws InterruptedException {
    process.destroy();
    process.waitFor();
  }

  /**
   * Returns the comma-separated list of master addresses.
   * @return master addresses
   */
  public String getMasterAddresses() {
    return masterAddresses;
  }

  /**
   * Returns a list of master addresses.
   * @return master addresses
   */
  public List<HostAndPort> getMasterHostPorts() {
    return masterHostPorts;
  }

  /**
   * Returns an unmodifiable map of all tablet servers in pairs of RPC port - > Process.
   * @return an unmodifiable map of all tablet servers
   */
  @VisibleForTesting
  Map<Integer, Process> getTabletServerProcesses() {
    return Collections.unmodifiableMap(tserverProcesses);
  }

  /**
   * Returns an unmodifiable map of all masters in pairs of RPC port - > Process.
   * @return an unmodifiable map of all masters
   */
  @VisibleForTesting
  Map<Integer, Process> getMasterProcesses() {
    return Collections.unmodifiableMap(masterProcesses);
  }

  /**
   * Helper runnable that receives stdout and logs it along with the process' identifier.
   */
  public static class ProcessInputStreamLogPrinterRunnable implements Runnable {

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

  public static class MiniKuduClusterBuilder {

    private int numMasters = 1;
    private int numTservers = 3;
    private int defaultTimeoutMs = 50000;
    private boolean enableKerberos = false;
    private List<String> extraTserverFlags = new ArrayList<>();
    private List<String> extraMasterFlags = new ArrayList<>();

    public MiniKuduClusterBuilder numMasters(int numMasters) {
      this.numMasters = numMasters;
      return this;
    }

    public MiniKuduClusterBuilder numTservers(int numTservers) {
      this.numTservers = numTservers;
      return this;
    }

    /**
     * Configures the internal client to use the given timeout for all operations. Also uses the
     * timeout for tasks like waiting for tablet servers to check in with the master.
     * @param defaultTimeoutMs timeout in milliseconds
     * @return this instance
     */
    public MiniKuduClusterBuilder defaultTimeoutMs(int defaultTimeoutMs) {
      this.defaultTimeoutMs = defaultTimeoutMs;
      return this;
    }

    /**
     * Enables Kerberos on the mini cluster and acquire client credentials for this process.
     * @return this instance
     */
    public MiniKuduClusterBuilder enableKerberos() {
      enableKerberos = true;
      return this;
    }

    /**
     * Adds a new flag to be passed to the Tablet Server daemons on start.
     * @return this instance
     */
    public MiniKuduClusterBuilder addTserverFlag(String flag) {
      this.extraTserverFlags.add(flag);
      return this;
    }

    /**
     * Adds a new flag to be passed to the Master daemons on start.
     * @return this instance
     */
    public MiniKuduClusterBuilder addMasterFlag(String flag) {
      this.extraMasterFlags.add(flag);
      return this;
    }

    public MiniKuduCluster build() throws Exception {
      MiniKuduCluster cluster =
          new MiniKuduCluster(defaultTimeoutMs, extraTserverFlags, extraMasterFlags);
      try {
        if (enableKerberos) {
          cluster.startKerberos();
        }
        cluster.start(numMasters, numTservers);
      } catch (Exception e) {
        // MiniKuduCluster.close should not throw, so no need for a nested try/catch.
        cluster.close();
        throw e;
      }
      return cluster;
    }
  }
}
