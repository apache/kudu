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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;

import org.apache.kudu.Common.HostPortPB;
import org.apache.kudu.tools.Tool.ControlShellRequestPB;
import org.apache.kudu.tools.Tool.ControlShellResponsePB;
import org.apache.kudu.tools.Tool.CreateClusterRequestPB.MiniKdcOptionsPB;
import org.apache.kudu.tools.Tool.CreateClusterRequestPB;
import org.apache.kudu.tools.Tool.DaemonIdentifierPB;
import org.apache.kudu.tools.Tool.DaemonInfoPB;
import org.apache.kudu.tools.Tool.GetKDCEnvVarsRequestPB;
import org.apache.kudu.tools.Tool.GetMastersRequestPB;
import org.apache.kudu.tools.Tool.GetTServersRequestPB;
import org.apache.kudu.tools.Tool.KdestroyRequestPB;
import org.apache.kudu.tools.Tool.KinitRequestPB;
import org.apache.kudu.tools.Tool.StartClusterRequestPB;
import org.apache.kudu.tools.Tool.StartDaemonRequestPB;
import org.apache.kudu.tools.Tool.StopDaemonRequestPB;
import org.apache.kudu.util.SecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to start and manipulate Kudu clusters. Depends on precompiled
 * kudu, kudu-master, and kudu-tserver binaries. {@link BaseKuduTest} should be
 * extended instead of directly using this class in almost all cases.
 */
public class MiniKuduCluster implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(MiniKuduCluster.class);

  // Control shell process.
  private Process miniCluster;

  // Request channel to the control shell.
  private DataOutputStream miniClusterStdin;

  // Response channel from the control shell.
  private DataInputStream miniClusterStdout;

  // Thread that reads and logs stderr from the control shell.
  private Thread miniClusterErrorPrinter;

  private class DaemonInfo {
    DaemonIdentifierPB id;
    boolean isRunning;
  }

  // Map of master addresses to daemon information.
  private final Map<HostAndPort, DaemonInfo> masters = Maps.newHashMap();

  // Map of tserver addresses to daemon information.
  private final Map<HostAndPort, DaemonInfo> tservers = Maps.newHashMap();

  // Builder-provided cluster configuration state.
  private final boolean enableKerberos;
  private final int numMasters;
  private final int numTservers;
  private final ImmutableList<String> extraTserverFlags;
  private final ImmutableList<String> extraMasterFlags;

  private MiniKdcOptionsPB kdcOptionsPb;

  private MiniKuduCluster(boolean enableKerberos,
      int numMasters,
      int numTservers,
      List<String> extraTserverFlags,
      List<String> extraMasterFlags,
      MiniKdcOptionsPB kdcOptionsPb) {
    this.enableKerberos = enableKerberos;
    this.numMasters = numMasters;
    this.numTservers = numTservers;
    this.extraTserverFlags = ImmutableList.copyOf(extraTserverFlags);
    this.extraMasterFlags = ImmutableList.copyOf(extraMasterFlags);
    this.kdcOptionsPb = kdcOptionsPb;
  }

  /**
   * Sends a command to the control shell and receives its response.
   * <p>
   * The method is synchronized to prevent interleaving of requests and responses.
   * @param req control shell request
   * @return control shell response
   * @throws IOException if there was some kind of transport error, or if the
   *                     response indicates an error
   */
  private synchronized ControlShellResponsePB sendRequestToCluster(ControlShellRequestPB req)
      throws IOException {
    // Send the request's size (4 bytes, big endian) followed by the request.
    LOG.debug("Request: {}", req);
    miniClusterStdin.writeInt(req.getSerializedSize());
    miniClusterStdin.write(req.toByteArray());
    miniClusterStdin.flush();

    // Read the response's size (4 bytes, big endian) followed by the response.
    int respLength = miniClusterStdout.readInt();
    byte[] respBody = new byte[respLength];
    miniClusterStdout.readFully(respBody);
    ControlShellResponsePB resp = ControlShellResponsePB.parseFrom(respBody);
    LOG.debug("Response: {}", resp);

    // Convert any error into an exception.
    if (resp.hasError()) {
      throw new NonRecoverableException(Status.fromPB(resp.getError()));
    }
    return resp;
  }

  /**
   * Starts this Kudu cluster.
   * @throws IOException if something went wrong in transit
   */
  private void start() throws IOException {
    Preconditions.checkArgument(numMasters > 0, "Need at least one master");

    // Start the control shell and the communication channel to it.
    List<String> commandLine = Lists.newArrayList(
        TestUtils.findBinary("kudu"),
        "test",
        "mini_cluster",
        "--serialization=pb");
    LOG.info("Starting process: {}", commandLine);
    ProcessBuilder processBuilder = new ProcessBuilder(commandLine);
    miniCluster = processBuilder.start();
    miniClusterStdin = new DataOutputStream(miniCluster.getOutputStream());
    miniClusterStdout = new DataInputStream(miniCluster.getInputStream());

    // Set up a thread that logs stderr from the control shell; this will
    // include all cluster logging.
    ProcessInputStreamLogPrinterRunnable printer =
        new ProcessInputStreamLogPrinterRunnable(miniCluster.getErrorStream());
    miniClusterErrorPrinter = new Thread(printer);
    miniClusterErrorPrinter.setDaemon(true);
    miniClusterErrorPrinter.setName("cluster stderr printer");
    miniClusterErrorPrinter.start();

    // Create and start the cluster.
    sendRequestToCluster(
        ControlShellRequestPB.newBuilder()
        .setCreateCluster(CreateClusterRequestPB.newBuilder()
            .setNumMasters(numMasters)
            .setNumTservers(numTservers)
            .setEnableKerberos(enableKerberos)
            .addAllExtraMasterFlags(extraMasterFlags)
            .addAllExtraTserverFlags(extraTserverFlags)
            .setMiniKdcOptions(kdcOptionsPb)
            .build())
        .build());
    sendRequestToCluster(
        ControlShellRequestPB.newBuilder()
        .setStartCluster(StartClusterRequestPB.newBuilder().build())
        .build());

    // If the cluster is Kerberized, retrieve the KDC's environment variables
    // and adapt them into certain security-related system properties.
    if (enableKerberos) {
      ControlShellResponsePB resp = sendRequestToCluster(
          ControlShellRequestPB.newBuilder()
          .setGetKdcEnvVars(GetKDCEnvVarsRequestPB.newBuilder().build())
          .build());
      for (Map.Entry<String, String> e : resp.getGetKdcEnvVars().getEnvVarsMap().entrySet()) {
        if (e.getKey().equals("KRB5_CONFIG")) {
          System.setProperty("java.security.krb5.conf", e.getValue());
        } else if (e.getKey().equals("KRB5CCNAME")) {
          System.setProperty(SecurityUtil.KUDU_TICKETCACHE_PROPERTY, e.getValue());
        }
      }
    }

    // Initialize the maps of masters and tservers.
    ControlShellResponsePB resp = sendRequestToCluster(
        ControlShellRequestPB.newBuilder()
        .setGetMasters(GetMastersRequestPB.newBuilder().build())
        .build());
    for (DaemonInfoPB info : resp.getGetMasters().getMastersList()) {
      DaemonInfo d = new DaemonInfo();
      d.id = info.getId();
      d.isRunning = true;
      masters.put(hostAndPortFromPB(info.getBoundRpcAddress()), d);
    }
    resp = sendRequestToCluster(
        ControlShellRequestPB.newBuilder()
        .setGetTservers(GetTServersRequestPB.newBuilder().build())
        .build());
    for (DaemonInfoPB info : resp.getGetTservers().getTserversList()) {
      DaemonInfo d = new DaemonInfo();
      d.id = info.getId();
      d.isRunning = true;
      tservers.put(hostAndPortFromPB(info.getBoundRpcAddress()), d);
    }
  }

  /**
   * Restarts a master identified by hostname and port. The master must already be dead.
   * @param hp unique hostname and port identifying the master
   * @throws IOException if the master is believed to be alive
   */
  public void restartDeadMasterOnHostPort(HostAndPort hp) throws IOException {
    DaemonInfo d = masters.get(hp);
    if (d == null) {
      throw new IOException(String.format("Master %s not found", hp));
    }
    if (d.isRunning) {
      throw new IOException(String.format("Master %s is already running", hp));
    }
    sendRequestToCluster(ControlShellRequestPB.newBuilder()
        .setStartDaemon(StartDaemonRequestPB.newBuilder().setId(d.id).build())
        .build());
    d.isRunning = true;
  }

  /**
   * Kills a master identified by hostname and port. Does nothing if the master
   * was already dead.
   * @param hp unique hostname and port identifying the master
   * @throws IOException if something went wrong in transit
   */
  public void killMasterOnHostPort(HostAndPort hp) throws IOException {
    DaemonInfo d = masters.get(hp);
    if (d == null) {
      throw new IOException(String.format("Master %s not found", hp));
    }
    if (!d.isRunning) {
      return;
    }
    sendRequestToCluster(ControlShellRequestPB.newBuilder()
        .setStopDaemon(StopDaemonRequestPB.newBuilder().setId(d.id).build())
        .build());
    d.isRunning = false;
  }

  /**
   * Restarts a tserver identified by hostname and port. The tserver must already be dead.
   * @param hp unique hostname and port identifying the tserver
   * @throws IOException if the tserver is believed to be alive
   */
  public void restartDeadTabletServerOnHostPort(HostAndPort hp) throws IOException {
    DaemonInfo d = tservers.get(hp);
    if (d == null) {
      throw new IOException(String.format("Tserver %s not found", hp));
    }
    if (d.isRunning) {
      throw new IOException(String.format("Tserver %s is already running", hp));
    }
    sendRequestToCluster(ControlShellRequestPB.newBuilder()
        .setStartDaemon(StartDaemonRequestPB.newBuilder().setId(d.id).build())
        .build());
    d.isRunning = true;
  }

  /**
   * Kills a tserver identified by hostname and port. Does nothing if the tserver
   * was already dead.
   * @param hp unique hostname and port identifying the tserver
   * @throws IOException if something went wrong in transit
   */
  public void killTabletServerOnHostPort(HostAndPort hp) throws IOException {
    DaemonInfo d = tservers.get(hp);
    if (d == null) {
      throw new IOException(String.format("Tserver %s not found", hp));
    }
    if (!d.isRunning) {
      return;
    }
    LOG.info("Killing tserver {}", hp);
    sendRequestToCluster(ControlShellRequestPB.newBuilder()
        .setStopDaemon(StopDaemonRequestPB.newBuilder().setId(d.id).build())
        .build());
    d.isRunning = false;
  }

  /**
   * Kills all masters not already stopped.
   * @throws IOException if something went wrong in transit
   */
  public void killMasters() throws IOException {
    List<HostAndPort> toKill = Lists.newArrayList();
    for (Map.Entry<HostAndPort, DaemonInfo> e : masters.entrySet()) {
      if (e.getValue().isRunning) {
        toKill.add(e.getKey());
      }
    }
    for (HostAndPort hp : toKill) {
      killMasterOnHostPort(hp);
    }
  }

  /**
   * Starts all currently stopped masters.
   * @throws IOException if something went wrong in transit
   */
  public void restartDeadMasters() throws IOException {
    List<HostAndPort> toRestart = Lists.newArrayList();
    for (Map.Entry<HostAndPort, DaemonInfo> e : masters.entrySet()) {
      if (!e.getValue().isRunning) {
        toRestart.add(e.getKey());
      }
    }
    for (HostAndPort hp : toRestart) {
      restartDeadMasterOnHostPort(hp);
    }
  }

  /**
   * Kills all tservers not already stopped.
   * @throws IOException if something went wrong in transit
   */
  public void killTservers() throws IOException {
    List<HostAndPort> toKill = Lists.newArrayList();
    for (Map.Entry<HostAndPort, DaemonInfo> e : tservers.entrySet()) {
      if (e.getValue().isRunning) {
        toKill.add(e.getKey());
      }
    }
    for (HostAndPort hp : toKill) {
      killTabletServerOnHostPort(hp);
    }
  }

  /**
   * Starts all currently stopped tservers.
   * @throws IOException if something went wrong in transit
   */
  public void restartDeadTservers() throws IOException {
    List<HostAndPort> toRestart = Lists.newArrayList();
    for (Map.Entry<HostAndPort, DaemonInfo> e : tservers.entrySet()) {
      if (!e.getValue().isRunning) {
        toRestart.add(e.getKey());
      }
    }
    for (HostAndPort hp : toRestart) {
      restartDeadTabletServerOnHostPort(hp);
    }
  }

  /**
   * Removes all credentials for all principals from the Kerberos credential cache.
   */
  public void kdestroy() throws IOException {
    sendRequestToCluster(ControlShellRequestPB.newBuilder()
                                              .setKdestroy(KdestroyRequestPB.getDefaultInstance())
                                              .build());
  }

  /**
   * Re-initialize Kerberos credentials for the given username, writing them
   * into the Kerberos credential cache.
   * @param username the username to kinit as
   */
  public void kinit(String username) throws IOException {
    sendRequestToCluster(ControlShellRequestPB.newBuilder()
        .setKinit(KinitRequestPB.newBuilder().setUsername(username).build())
        .build());
  }


  /** {@override} */
  @Override
  public void close() {
    shutdown();
  }

  /**
   * Shuts down a Kudu cluster.
   */
  public void shutdown() {
    // Closing stdin should cause the control shell process to terminate.
    if (miniClusterStdin != null) {
      try {
        miniClusterStdin.close();
      } catch (IOException e) {
        LOG.info("Caught exception while closing minicluster stdin", e);
      }
    }
    if (miniClusterStdout != null) {
      try {
        miniClusterStdout.close();
      } catch (IOException e) {
        LOG.info("Caught exception while closing minicluster stdout", e);
      }
    }
    if (miniClusterErrorPrinter != null) {
      try {
        miniClusterErrorPrinter.join();
      } catch (InterruptedException e) {
        LOG.info("Caught exception while closing minicluster stderr", e);
      }
    }
    if (miniCluster != null) {
      try {
        miniCluster.waitFor();
      } catch (InterruptedException e) {
        LOG.warn("Minicluster process did not exit, destroying");
        miniCluster.destroy();
      }
    }
  }

  /**
   * @return comma-separated list of master addresses
   */
  public String getMasterAddresses() {
    return Joiner.on(',').join(masters.keySet());
  }

  /**
   * @return list of all masters, uniquely identified by hostname and port
   */
  public List<HostAndPort> getMasterHostPorts() {
    return new ArrayList<>(masters.keySet());
  }

  /**
   * @return list of all tservers, uniquely identified by hostname and port
   */
  public List<HostAndPort> getTserverHostPorts() {
    return new ArrayList<>(tservers.keySet());
  }

  /**
   * Helper runnable that receives stderr and logs it along with the process' identifier.
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
      } catch (Exception e) {
        if (!e.getMessage().contains("Stream closed")) {
          LOG.error("Caught error while reading a process' output", e);
        }
      }
    }
  }

  /**
   * TODO(KUDU-2186): If used directly from {@link ProtobufHelper}, tests from
   * other modules break when run by Gradle.
   */
  private static HostAndPort hostAndPortFromPB(HostPortPB hostPortPB) {
    return HostAndPort.fromParts(hostPortPB.getHost(), hostPortPB.getPort());
  }

  /**
   * Builder for {@link MiniKuduCluster}
   */
  public static class MiniKuduClusterBuilder {

    private int numMasters = 1;
    private int numTservers = 3;
    private boolean enableKerberos = false;
    private final List<String> extraTserverFlags = new ArrayList<>();
    private final List<String> extraMasterFlags = new ArrayList<>();

    private MiniKdcOptionsPB.Builder kdcOptionsPb =
        MiniKdcOptionsPB.newBuilder();

    public MiniKuduClusterBuilder numMasters(int numMasters) {
      this.numMasters = numMasters;
      return this;
    }

    public MiniKuduClusterBuilder numTservers(int numTservers) {
      this.numTservers = numTservers;
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

    public MiniKuduClusterBuilder kdcTicketLifetime(String lifetime) {
      this.kdcOptionsPb.setTicketLifetime(lifetime);
      return this;
    }

    public MiniKuduClusterBuilder kdcRenewLifetime(String lifetime) {
      this.kdcOptionsPb.setRenewLifetime(lifetime);
      return this;
    }

    /**
     * Builds and starts a new {@link MiniKuduCluster} using builder state.
     * @return the newly started {@link MiniKuduCluster}
     * @throws IOException if something went wrong starting the cluster
     */
    public MiniKuduCluster build() throws IOException {
      MiniKuduCluster cluster =
          new MiniKuduCluster(enableKerberos,
              numMasters, numTservers,
              extraTserverFlags, extraMasterFlags,
              kdcOptionsPb.build());
      try {
        cluster.start();
      } catch (IOException e) {
        // MiniKuduCluster.close should not throw, so no need for a nested try/catch.
        cluster.close();
        throw e;
      }
      return cluster;
    }
  }
}
