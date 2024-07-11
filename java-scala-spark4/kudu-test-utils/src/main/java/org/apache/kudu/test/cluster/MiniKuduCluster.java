// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.kudu.test.cluster;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Paths;
import java.security.Security;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.Common;
import org.apache.kudu.client.HostAndPort;
import org.apache.kudu.client.ProtobufHelper;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.TempDirUtils;
import org.apache.kudu.tools.Tool.ControlShellRequestPB;
import org.apache.kudu.tools.Tool.ControlShellResponsePB;
import org.apache.kudu.tools.Tool.CreateClusterRequestPB;
import org.apache.kudu.tools.Tool.CreateClusterRequestPB.JwksOptionsPB;
import org.apache.kudu.tools.Tool.CreateClusterRequestPB.MiniKdcOptionsPB;
import org.apache.kudu.tools.Tool.CreateClusterRequestPB.MiniOidcOptionsPB;
import org.apache.kudu.tools.Tool.CreateJwtRequestPB;
import org.apache.kudu.tools.Tool.DaemonIdentifierPB;
import org.apache.kudu.tools.Tool.DaemonInfoPB;
import org.apache.kudu.tools.Tool.GetKDCEnvVarsRequestPB;
import org.apache.kudu.tools.Tool.GetMastersRequestPB;
import org.apache.kudu.tools.Tool.GetTServersRequestPB;
import org.apache.kudu.tools.Tool.KdestroyRequestPB;
import org.apache.kudu.tools.Tool.KinitRequestPB;
import org.apache.kudu.tools.Tool.PauseDaemonRequestPB;
import org.apache.kudu.tools.Tool.ResumeDaemonRequestPB;
import org.apache.kudu.tools.Tool.SetDaemonFlagRequestPB;
import org.apache.kudu.tools.Tool.StartClusterRequestPB;
import org.apache.kudu.tools.Tool.StartDaemonRequestPB;
import org.apache.kudu.tools.Tool.StopDaemonRequestPB;
import org.apache.kudu.util.SecurityUtil;

/**
 * Utility class to start and manipulate Kudu clusters. Depends on precompiled
 * kudu, kudu-master, and kudu-tserver binaries. {@link KuduTestHarness}
 * should be used instead of directly using this class in almost all cases.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class MiniKuduCluster implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(MiniKuduCluster.class);

  // Control shell process.
  private Process miniCluster;

  // Request channel to the control shell.
  private DataOutputStream miniClusterStdin;

  // Response channel from the control shell.
  private DataInputStream miniClusterStdout;

  // Thread that reads and logs stderr from the control shell.
  private Thread miniClusterErrorPrinter;

  private static class DaemonInfo {
    DaemonIdentifierPB id;
    boolean isRunning;
    boolean isPaused;
    String webServerAddress;
  }

  // Map of master addresses to daemon information.
  private final Map<HostAndPort, DaemonInfo> masterServers = Maps.newHashMap();

  // Map of tserver addresses to daemon information.
  private final Map<HostAndPort, DaemonInfo> tabletServers = Maps.newHashMap();

  // Builder-provided cluster configuration state.
  private final boolean enableKerberos;
  private final int numMasters;
  private final int numTservers;
  private final ImmutableList<String> extraTserverFlags;
  private final ImmutableList<String> extraMasterFlags;
  private final ImmutableList<String> locationInfo;
  private final String clusterRoot;
  private final String principal;

  private MiniKdcOptionsPB kdcOptionsPb;
  private final Common.HmsMode hmsMode;
  private MiniOidcOptionsPB oidcOptionsPb;

  private MiniKuduCluster(boolean enableKerberos,
      int numMasters,
      int numTservers,
      List<String> extraTserverFlags,
      List<String> extraMasterFlags,
      List<String> locationInfo,
      MiniKdcOptionsPB kdcOptionsPb,
      String clusterRoot,
      Common.HmsMode hmsMode,
      String principal,
      MiniOidcOptionsPB oidcOptionsPb) {
    this.enableKerberos = enableKerberos;
    this.numMasters = numMasters;
    this.numTservers = numTservers;
    this.extraTserverFlags = ImmutableList.copyOf(extraTserverFlags);
    this.extraMasterFlags = ImmutableList.copyOf(extraMasterFlags);
    this.locationInfo = ImmutableList.copyOf(locationInfo);
    this.kdcOptionsPb = kdcOptionsPb;
    this.principal = principal;
    this.hmsMode = hmsMode;
    this.oidcOptionsPb = oidcOptionsPb;

    if (clusterRoot == null) {
      // If a cluster root was not set, create a unique temp directory to use.
      // The mini cluster will clean this directory up on exit.
      try {
        File tempRoot = TempDirUtils.makeTempDirectory("mini-kudu-cluster",
            TempDirUtils.DeleteOnExit.NO_DELETE_ON_EXIT);
        this.clusterRoot = tempRoot.toString();
      } catch (IOException ex) {
        throw new RuntimeException("Could not create cluster root directory", ex);
      }
    } else {
      this.clusterRoot = clusterRoot;
    }

    // Default Java security settings are restrictive with regard to RSA key
    // length. Since Kudu masters and tablet servers in MiniKuduCluster use
    // smaller RSA keys to shorten runtime of tests, it's necessary to override
    // those default security settings to allow for using relaxed cryptography,
    // particularly smaller RSA keys.
    Security.setProperty("jdk.certpath.disabledAlgorithms", "MD2, RC4, MD5");
    Security.setProperty("jdk.tls.disabledAlgorithms", "SSLv3, RC4, MD5");
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
      throw new IOException(resp.getError().getMessage());
    }
    return resp;
  }

  /**
   * Starts this Kudu cluster.
   * @throws IOException if something went wrong in transit
   */
  private synchronized void start() throws IOException {
    Preconditions.checkArgument(numMasters > 0, "Need at least one master");

    // Start the control shell and the communication channel to it.
    KuduBinaryLocator.ExecutableInfo exeInfo = KuduBinaryLocator.findBinary("kudu");
    List<String> commandLine = Lists.newArrayList(exeInfo.exePath(),
                                                  "test",
                                                  "mini_cluster",
                                                  "--serialization=pb");
    LOG.info("Starting process: {}", commandLine);
    ProcessBuilder processBuilder = new ProcessBuilder(commandLine);
    processBuilder.environment().putAll(exeInfo.environment());

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

    CreateClusterRequestPB.Builder createClusterRequestBuilder = CreateClusterRequestPB.newBuilder()
        .setNumMasters(numMasters)
        .setNumTservers(numTservers)
        .setEnableKerberos(enableKerberos)
        .setHmsMode(hmsMode)
        .addAllExtraMasterFlags(extraMasterFlags)
        .addAllExtraTserverFlags(extraTserverFlags)
        .setMiniKdcOptions(kdcOptionsPb)
        .setClusterRoot(clusterRoot)
        .setPrincipal(principal)
        .setMiniOidcOptions(oidcOptionsPb);

    // Set up the location mapping command flag if there is location info.
    if (!locationInfo.isEmpty()) {
      List<String> locationMappingCmd = new ArrayList<>();
      locationMappingCmd.add(getClass().getResource("/assign-location.py").getFile());
      String locationMappingCmdPath =
          Paths.get(clusterRoot, "location-assignment.state").toString();
      locationMappingCmd.add("--state_store=" + locationMappingCmdPath);
      for (String location : locationInfo) {
        locationMappingCmd.add("--map " + location);
      }
      String locationMappingCmdFlag = "--location_mapping_cmd=" +
          Joiner.on(" ").join(locationMappingCmd);
      createClusterRequestBuilder.addExtraMasterFlags(locationMappingCmdFlag);
    }

    // Create and start the cluster.
    sendRequestToCluster(
        ControlShellRequestPB.newBuilder()
        .setCreateCluster(createClusterRequestBuilder.build())
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

    // Initialize the maps of master and tablet servers.
    ControlShellResponsePB resp = sendRequestToCluster(
        ControlShellRequestPB.newBuilder()
        .setGetMasters(GetMastersRequestPB.newBuilder().build())
        .build());
    for (DaemonInfoPB info : resp.getGetMasters().getMastersList()) {
      DaemonInfo d = new DaemonInfo();
      d.id = info.getId();
      d.isRunning = true;
      d.isPaused = false;
      d.webServerAddress = String.join(":", info.getBoundHttpAddress().getHost(),
                                       Integer.toString(info.getBoundHttpAddress().getPort()));
      masterServers.put(ProtobufHelper.hostAndPortFromPB(info.getBoundRpcAddress()), d);
    }
    resp = sendRequestToCluster(
        ControlShellRequestPB.newBuilder()
        .setGetTservers(GetTServersRequestPB.newBuilder().build())
        .build());
    for (DaemonInfoPB info : resp.getGetTservers().getTserversList()) {
      DaemonInfo d = new DaemonInfo();
      d.id = info.getId();
      d.isRunning = true;
      d.isPaused = false;
      d.webServerAddress = String.join(":", info.getBoundHttpAddress().getHost(),
                                       Integer.toString(info.getBoundHttpAddress().getPort()));
      tabletServers.put(ProtobufHelper.hostAndPortFromPB(info.getBoundRpcAddress()), d);
    }
  }

  /**
   * @return a comma-separated list of RPC addresses of all masters in the cluster
   */
  public String getMasterAddressesAsString() {
    return Joiner.on(',').join(masterServers.keySet());
  }

  /**
   * @return a comma-separated list of webserver addresses of all masters in the cluster
   */
  public String getMasterWebServerAddressesAsString() {
    List<String> addresses = new ArrayList<String>();
    masterServers.forEach((hp, daemonInfo) -> {
      addresses.add(daemonInfo.webServerAddress);
    });

    return Joiner.on(',').join(addresses);
  }

  /**
   * @return the list of master servers
   */
  public List<HostAndPort> getMasterServers() {
    return new ArrayList<>(masterServers.keySet());
  }

  /**
   * @return the list of tablet servers
   */
  public List<HostAndPort> getTabletServers() {
    return new ArrayList<>(tabletServers.keySet());
  }

  /**
   * @return the service principal name
   */
  public String getPrincipal() {
    return principal;
  }

  public String createJwtFor(String accountId, String subject, boolean isValid) throws IOException {
    ControlShellResponsePB resp = sendRequestToCluster(ControlShellRequestPB.newBuilder()
        .setCreateJwt(CreateJwtRequestPB
            .newBuilder()
            .setAccountId(accountId)
            .setSubject(subject)
            .setIsValidKey(isValid)
            .build())
        .build());
    return resp.getCreateJwt().getJwt();
  }

  /**
   * Starts a master identified by a host and port.
   * Does nothing if the server was already running.
   *
   * @param hp unique host and port identifying the server
   * @throws IOException if something went wrong in transit
   */
  public void startMasterServer(HostAndPort hp) throws IOException {
    DaemonInfo d = getMasterServer(hp);
    if (d.isRunning) {
      return;
    }
    LOG.info("Starting master server {}", hp);
    sendRequestToCluster(ControlShellRequestPB.newBuilder()
        .setStartDaemon(StartDaemonRequestPB.newBuilder().setId(d.id).build())
        .build());
    d.isRunning = true;
  }

  /**
   * Kills a master identified identified by an host and port.
   * Does nothing if the master was already dead.
   *
   * @param hp unique host and port identifying the server
   * @throws IOException if something went wrong in transit
   */
  public void killMasterServer(HostAndPort hp) throws IOException {
    DaemonInfo d = getMasterServer(hp);
    if (!d.isRunning) {
      return;
    }
    LOG.info("Killing master server {}", hp);
    sendRequestToCluster(ControlShellRequestPB.newBuilder()
        .setStopDaemon(StopDaemonRequestPB.newBuilder().setId(d.id).build())
        .build());
    d.isRunning = false;
  }

  /**
   * Pauses a master identified identified by the specified host and port.
   * Does nothing if the master is already paused.
   *
   * @param hp unique host and port identifying the server
   * @throws IOException if something went wrong in transit
   */
  public void pauseMasterServer(HostAndPort hp) throws IOException {
    DaemonInfo d = getMasterServer(hp);
    if (d.isPaused) {
      return;
    }
    LOG.info("pausing master server {}", hp);
    sendRequestToCluster(ControlShellRequestPB.newBuilder()
        .setPauseDaemon(PauseDaemonRequestPB.newBuilder().setId(d.id).build())
        .build());
    d.isPaused = true;
  }

  /**
   * Resumes a master identified identified by the specified host and port.
   * Does nothing if the master isn't paused.
   *
   * @param hp unique host and port identifying the server
   * @throws IOException if something went wrong in transit
   */
  public void resumeMasterServer(HostAndPort hp) throws IOException {
    DaemonInfo d = getMasterServer(hp);
    if (!d.isPaused) {
      return;
    }
    LOG.info("resuming master server {}", hp);
    sendRequestToCluster(ControlShellRequestPB.newBuilder()
        .setResumeDaemon(ResumeDaemonRequestPB.newBuilder().setId(d.id).build())
        .build());
    d.isPaused = false;
  }

  /**
   * Starts a tablet server identified by an host and port.
   * Does nothing if the server was already running.
   *
   * @param hp unique host and port identifying the server
   * @throws IOException if something went wrong in transit
   */
  public void startTabletServer(HostAndPort hp) throws IOException {
    DaemonInfo d = getTabletServer(hp);
    if (d.isRunning) {
      return;
    }
    LOG.info("Starting tablet server {}", hp);
    sendRequestToCluster(ControlShellRequestPB.newBuilder()
        .setStartDaemon(StartDaemonRequestPB.newBuilder().setId(d.id).build())
        .build());
    d.isRunning = true;
  }

  /**
   * Kills a tablet server identified by an host and port.
   * Does nothing if the tablet server was already dead.
   *
   * @param hp unique host and port identifying the server
   * @throws IOException if something went wrong in transit
   */
  public void killTabletServer(HostAndPort hp) throws IOException {
    DaemonInfo d = getTabletServer(hp);
    if (!d.isRunning) {
      return;
    }
    LOG.info("Killing tablet server {}", hp);
    sendRequestToCluster(ControlShellRequestPB.newBuilder()
        .setStopDaemon(StopDaemonRequestPB.newBuilder().setId(d.id).build())
        .build());
    d.isRunning = false;
  }

  /**
   * Pauses a tablet server identified by the specified host and port.
   * Does nothing if the tablet server is already paused.
   *
   * @param hp unique host and port identifying the server
   * @throws IOException if something went wrong in transit
   */
  public void pauseTabletServer(HostAndPort hp) throws IOException {
    DaemonInfo d = getTabletServer(hp);
    if (d.isPaused) {
      return;
    }
    LOG.info("pausing tablet server {}", hp);
    sendRequestToCluster(ControlShellRequestPB.newBuilder()
        .setPauseDaemon(PauseDaemonRequestPB.newBuilder().setId(d.id).build())
        .build());
    d.isPaused = true;
  }

  /**
   * Resumes a tablet server identified by the specified host and port.
   * Does nothing if the tablet server isn't paused.
   *
   * @param hp unique host and port identifying the server
   * @throws IOException if something went wrong in transit
   */
  public void resumeTabletServer(HostAndPort hp) throws IOException {
    DaemonInfo d = getTabletServer(hp);
    if (!d.isPaused) {
      return;
    }
    LOG.info("resuming tablet server {}", hp);
    sendRequestToCluster(ControlShellRequestPB.newBuilder()
        .setResumeDaemon(ResumeDaemonRequestPB.newBuilder().setId(d.id).build())
        .build());
    d.isPaused = true;
  }

  /**
   * Kills all the master servers.
   * Does nothing to the servers that are already dead.
   *
   * @throws IOException if something went wrong in transit
   */
  public void killAllMasterServers() throws IOException {
    for (Map.Entry<HostAndPort, DaemonInfo> e : masterServers.entrySet()) {
      killMasterServer(e.getKey());
    }
  }

  /**
   * Starts all the master servers.
   * Does nothing to the servers that are already running.
   *
   * @throws IOException if something went wrong in transit
   */
  public void startAllMasterServers() throws IOException {
    for (Map.Entry<HostAndPort, DaemonInfo> e : masterServers.entrySet()) {
      startMasterServer(e.getKey());
    }
  }

  /**
   * Kills all tablet servers.
   * Does nothing to the servers that are already dead.
   *
   * @throws IOException if something went wrong in transit
   */
  public void killAllTabletServers() throws IOException {
    for (Map.Entry<HostAndPort, DaemonInfo> e : tabletServers.entrySet()) {
      killTabletServer(e.getKey());
    }
  }

  /**
   * Starts all the tablet servers.
   * Does nothing to the servers that are already running.
   *
   * @throws IOException if something went wrong in transit
   */
  public void startAllTabletServers() throws IOException {
    for (Map.Entry<HostAndPort, DaemonInfo> e : tabletServers.entrySet()) {
      startTabletServer(e.getKey());
    }
  }

  /**
   * Set flag for the specified master.
   *
   * @param hp unique host and port identifying the target master
   * @throws IOException if something went wrong in transit
   */
  public void setMasterFlag(HostAndPort hp, String flag, String value)
      throws IOException {
    DaemonInfo d = getMasterServer(hp);
    LOG.info("Setting flag for master at {}", hp);
    sendRequestToCluster(ControlShellRequestPB.newBuilder()
        .setSetDaemonFlag(SetDaemonFlagRequestPB.newBuilder()
            .setId(d.id)
            .setFlag(flag)
            .setValue(value)
            .build())
        .build());
  }

  /**
   * Set flag for the specified tablet server.
   *
   * @param hp unique host and port identifying the target tablet server
   * @throws IOException if something went wrong in transit
   */
  public void setTServerFlag(HostAndPort hp, String flag, String value)
      throws IOException {
    DaemonInfo d = getTabletServer(hp);
    LOG.info("Setting flag for tserver at {}", hp);
    sendRequestToCluster(ControlShellRequestPB.newBuilder()
        .setSetDaemonFlag(SetDaemonFlagRequestPB.newBuilder()
            .setId(d.id)
            .setFlag(flag)
            .setValue(value)
            .build())
        .build());
  }

  /**
   * Removes all credentials for all principals from the Kerberos credential cache.
   */
  public void kdestroy() throws IOException {
    LOG.info("Destroying all Kerberos credentials");
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
    LOG.info("Running kinit for user {}", username);
    sendRequestToCluster(ControlShellRequestPB.newBuilder()
        .setKinit(KinitRequestPB.newBuilder().setUsername(username).build())
        .build());
  }

  @Override
  public void close() {
    shutdown();
  }

  /**
   * Shuts down a Kudu cluster.
   */
  public synchronized void shutdown() {
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
   * Returns a master server identified by an address.
   *
   * @param hp unique host and port identifying the server
   * @return the DaemonInfo of the server
   * @throws RuntimeException if the server is not found
   */
  private DaemonInfo getMasterServer(HostAndPort hp) throws RuntimeException {
    DaemonInfo d = masterServers.get(hp);
    if (d == null) {
      throw new RuntimeException(String.format("Master server %s not found", hp));
    }
    return d;
  }

  /**
   * Returns a tablet server identified by an address.
   *
   * @param hp unique host and port identifying the server
   * @return the DaemonInfo of the server
   * @throws RuntimeException if the server is not found
   */
  private DaemonInfo getTabletServer(HostAndPort hp) throws RuntimeException {
    DaemonInfo d = tabletServers.get(hp);
    if (d == null) {
      throw new RuntimeException(String.format("Tablet server %s not found", hp));
    }
    return d;
  }

  /**
   * @return path to the mini cluster root directory
   */
  public String getClusterRoot() {
    return clusterRoot;
  }

  /**
   * @return cluster's CA certificate in DER format or an empty array
   */
  public byte[] getCACertDer() throws IOException {
    String masterHttpAddr = Iterables.get(Splitter.on(',')
                                     .split(getMasterWebServerAddressesAsString()), 0);
    URL url = new URL("http://" + masterHttpAddr + "/ipki-ca-cert-der");
    HttpURLConnection connection = (HttpURLConnection)url.openConnection();
    connection.setRequestMethod("GET");
    connection.connect();

    if (connection.getResponseCode() != 200) {
      connection.disconnect();
      return new byte[0];
    }

    InputStream urlData = connection.getInputStream();
    int contentSize = connection.getContentLength();
    byte[] data = new byte[contentSize];
    int numBytesRead = urlData.read(data);
    if (numBytesRead != contentSize) {
      connection.disconnect();
      return new byte[0];
    }
    return data;
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
        BufferedReader in = new BufferedReader(
            new InputStreamReader(is, UTF_8));
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
   * Builder for {@link MiniKuduCluster}
   */
  public static class MiniKuduClusterBuilder {

    private int numMasterServers = 1;
    private int numTabletServers = 3;
    private boolean enableKerberos = false;
    private final List<String> extraTabletServerFlags = new ArrayList<>();
    private final List<String> extraMasterServerFlags = new ArrayList<>();
    private final List<String> locationInfo = new ArrayList<>();
    private String clusterRoot = null;
    private String principal = "kudu";

    private MiniKdcOptionsPB.Builder kdcOptionsPb = MiniKdcOptionsPB.newBuilder();
    private MiniOidcOptionsPB.Builder oidcOptionsPb = MiniOidcOptionsPB.newBuilder();
    private Common.HmsMode hmsMode = Common.HmsMode.NONE;

    public MiniKuduClusterBuilder numMasterServers(int numMasterServers) {
      this.numMasterServers = numMasterServers;
      return this;
    }

    public MiniKuduClusterBuilder numTabletServers(int numTabletServers) {
      this.numTabletServers = numTabletServers;
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

    public MiniKuduClusterBuilder enableHiveMetastoreIntegration() {
      hmsMode = Common.HmsMode.ENABLE_METASTORE_INTEGRATION;
      return this;
    }

    /**
     * Adds a new flag to be passed to the Tablet Server daemons on start.
     * @return this instance
     */
    public MiniKuduClusterBuilder addTabletServerFlag(String flag) {
      this.extraTabletServerFlags.add(flag);
      return this;
    }

    /**
     * Adds a new flag to be passed to the Master daemons on start.
     * @return this instance
     */
    public MiniKuduClusterBuilder addMasterServerFlag(String flag) {
      this.extraMasterServerFlags.add(flag);
      return this;
    }

    /**
     * Adds one location to the minicluster configuration, consisting of a
     * location and the total number of tablet servers and clients that
     * can be assigned to the location. The 'location' string should be
     * in the form 'location:number'. For example,
     *     "/L0:2"
     * will add a location "/L0" that will accept up to two clients or
     * tablet servers registered in it.
     * @return this instance
     */
    public MiniKuduClusterBuilder addLocation(String location) {
      locationInfo.add(location);
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
     * Sets the directory where the cluster's data and logs should be placed.
     * @return this instance
     */
    public MiniKuduClusterBuilder clusterRoot(String clusterRoot) {
      this.clusterRoot = clusterRoot;
      return this;
    }

    public MiniKuduClusterBuilder principal(String principal) {
      this.principal = principal;
      return this;
    }

    public MiniKuduClusterBuilder addJwks(String accountId, boolean isValid) {
      this.oidcOptionsPb.addJwksOptions(
          JwksOptionsPB.newBuilder()
              .setAccountId(accountId)
              .setIsValidKey(isValid)
              .build());
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
              numMasterServers, numTabletServers,
              extraTabletServerFlags, extraMasterServerFlags, locationInfo,
              kdcOptionsPb.build(), clusterRoot, hmsMode, principal,
              oidcOptionsPb.build());
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
