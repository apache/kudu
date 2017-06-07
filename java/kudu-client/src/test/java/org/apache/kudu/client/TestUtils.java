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
package org.apache.kudu.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.sun.security.auth.module.UnixSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.management.VMManagement;

import org.apache.kudu.Common;
import org.apache.kudu.consensus.Metadata;
import org.apache.kudu.master.Master;

/**
 * A grouping of methods that help unit testing.
 */
public class TestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

  // Used by pidOfProcess()
  private static String UNIX_PROCESS_CLS_NAME =  "java.lang.UNIXProcess";
  private static Set<String> VALID_SIGNALS =  ImmutableSet.of("STOP", "CONT", "TERM", "KILL");

  private static final String BIN_DIR_PROP = "binDir";

  /**
   * @return the path of the flags file to pass to daemon processes
   * started by the tests
   */
  public static String getFlagsPath() {
    URL u = BaseKuduTest.class.getResource("/flags");
    if (u == null) {
      throw new RuntimeException("Unable to find 'flags' file");
    }
    if (u.getProtocol() == "file") {
      return urlToPath(u);
    }
    // If the flags are inside a JAR, extract them into our temporary
    // test directory.
    try {
      // Somewhat unintuitively, createTempFile() actually creates the file,
      // not just the path, so we have to use REPLACE_EXISTING below.
      Path tmpFile = Files.createTempFile(
          Paths.get(getBaseDir()), "kudu-flags", ".flags");
      Files.copy(BaseKuduTest.class.getResourceAsStream("/flags"), tmpFile,
          StandardCopyOption.REPLACE_EXISTING);
      return tmpFile.toAbsolutePath().toString();
    } catch (IOException e) {
      throw new RuntimeException("Unable to extract flags file into tmp", e);
    }
  }

  /**
   * Return the path portion of a file URL, after decoding the escaped
   * components. This fixes issues when trying to build within a
   * working directory with special characters.
   */
  private static String urlToPath(URL u) {
    try {
      return URLDecoder.decode(u.getPath(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Find the nearest directory up in the path hierarchy that is a git source directory.
   */
  private static File findParentGitDir(File dir) {
    while (dir != null) {
      if (new File(dir, ".git").exists()) {
        return dir;
      }
      dir = dir.getParentFile();
    }
    return null;
  }

  /**
   * Find the binary directory within the build tree.
   */
  private static String findBinDir() {
    // First check the system property, which is our standard override.
    String binDirProp = System.getProperty(BIN_DIR_PROP);
    if (binDirProp != null) {
      LOG.info("Using binary directory specified by property: {}", binDirProp);
      return binDirProp;
    }

    // Next, attempt to traverse from the location of the class file.
    URL codeSrcUrl = BaseKuduTest.class.getProtectionDomain().getCodeSource().getLocation();
    File srcDir = findParentGitDir(new File(urlToPath(codeSrcUrl)));
    if (srcDir != null) {
      return new File(srcDir, "build/latest/bin").getAbsolutePath();
    }

    // Note: It has been discussed in the past whether to also search from the current working
    // directory to find the source directory. At this time we have elected *not* to support this,
    // instead relying on setting -DbinDir for cases where the test libs may be in the Maven repo.
    // See the following code reviews for the discussion: https://gerrit.cloudera.org/5328 and
    // https://gerrit.cloudera.org/4630

    LOG.warn("Unable to find bin dir! codeSrcUrl={}", codeSrcUrl);
    return null;
  }

  /**
   * @param binName the binary to look for (eg 'kudu-tserver')
   * @return the absolute path of that binary
   * @throws FileNotFoundException if no such binary is found
   */
  public static String findBinary(String binName) throws FileNotFoundException {
    String binDir = findBinDir();

    File candidate = new File(binDir, binName);
    if (candidate.canExecute()) {
      return candidate.getAbsolutePath();
    }
    throw new FileNotFoundException("Cannot find binary " + binName +
        " in binary directory " + binDir);
  }

  /**
   * @return the base directory within which we will store server data
   */
  public static String getBaseDir() {
    String s = System.getenv("TEST_TMPDIR");
    if (s == null) {
      s = String.format("/tmp/kudutest-%d", new UnixSystem().getUid());
    }
    File f = new File(s);
    f.mkdirs();
    return f.getAbsolutePath();
  }

  /**
   * Finds the next free port, starting with the one passed. Keep in mind the
   * time-of-check-time-of-use nature of this method, the returned port might become occupied
   * after it was checked for availability.
   * @param startPort first port to be probed
   * @return a currently usable port
   * @throws IOException IOE is thrown if we can't close a socket we tried to open or if we run
   * out of ports to try
   */
  public static int findFreePort(int startPort) throws IOException {
    ServerSocket ss;
    for(int i = startPort; i < 65536; i++) {
      try {
        ss = new ServerSocket();
        SocketAddress address = new InetSocketAddress(getUniqueLocalhost(), i);
        ss.bind(address);
      } catch (IOException e) {
        continue;
      }
      ss.close();
      return i;
    }
    throw new IOException("Ran out of ports.");
  }

  /**
   * Finds the next free UDP port, starting with the one passed. Keep in mind the
   * time-of-check-time-of-use nature of this method, the returned port might become occupied
   * after it was checked for availability.
   * @param startPort first port to be probed
   * @return a currently usable port
   * @throws IOException IOE is thrown if we can't close a socket we tried to open or if we run
   * out of ports to try
   */
  public static int findFreeUdpPort(int startPort) throws IOException {
    DatagramSocket ds;
    for (int i = startPort; i < 65536; i++) {
      try {
        SocketAddress address = new InetSocketAddress(getUniqueLocalhost(), i);
        ds = new DatagramSocket(address);
      } catch (SocketException e) {
        continue;
      }
      ds.close();
      return i;
    }
    throw new IOException("Ran out of ports");
  }

  /**
   * Finds a specified number of parts, starting with one passed. Keep in mind the
   * time-of-check-time-of-use nature of this method.
   * @see {@link #findFreePort(int)}
   * @param startPort First port to be probed.
   * @param numPorts Number of ports to reserve.
   * @return A list of currently usable ports.
   * @throws IOException IOE Is thrown if we can't close a socket we tried to open or if run
   * out of ports to try.
   */
  public static List<Integer> findFreePorts(int startPort, int numPorts) throws IOException {
    List<Integer> ports = Lists.newArrayListWithCapacity(numPorts);
    for (int i = 0; i < numPorts; i++) {
      startPort = findFreePort(startPort);
      ports.add(startPort++);
    }
    return ports;
  }

  /**
   * Gets the pid of a specified process. Relies on reflection and only works on
   * UNIX process, not guaranteed to work on JDKs other than Oracle and OpenJDK.
   * @param proc The specified process.
   * @return The process UNIX pid.
   * @throws IllegalArgumentException If the process is not a UNIXProcess.
   * @throws Exception If there are other getting the pid via reflection.
   */
  static int pidOfProcess(Process proc) throws Exception {
    Class<?> procCls = proc.getClass();
    if (!procCls.getName().equals(UNIX_PROCESS_CLS_NAME)) {
      throw new IllegalArgumentException("stopProcess() expects objects of class " +
          UNIX_PROCESS_CLS_NAME + ", but " + procCls.getName() + " was passed in instead!");
    }
    Field pidField = procCls.getDeclaredField("pid");
    pidField.setAccessible(true);
    return (Integer) pidField.get(proc);
  }

  /**
   * Send a code specified by its string representation to the specified process.
   * TODO: Use a JNR/JNR-Posix instead of forking the JVM to exec "kill".
   * @param proc The specified process.
   * @param sig The string representation of the process (e.g., STOP for SIGSTOP).
   * @throws IllegalArgumentException If the signal type is not supported.
   * @throws IllegalStateException If we are unable to send the specified signal.
   */
  static void signalProcess(Process proc, String sig) throws Exception {
    if (!VALID_SIGNALS.contains(sig)) {
      throw new IllegalArgumentException(sig + " is not a supported signal, only " +
              Joiner.on(",").join(VALID_SIGNALS) + " are supported");
    }
    int pid = pidOfProcess(proc);
    int rv = Runtime.getRuntime()
            .exec(String.format("kill -%s %d", sig, pid))
            .waitFor();
    if (rv != 0) {
      throw new IllegalStateException(String.format("unable to send SIG%s to process %s(pid=%d): " +
              "expected return code from kill, but got %d instead", sig, proc, pid, rv));
    }
  }

  /**
   * Pause the specified process by sending a SIGSTOP using the kill command.
   * @param proc The specified process.
   * @throws Exception If error prevents us from pausing the process.
   */
  static void pauseProcess(Process proc) throws Exception {
    signalProcess(proc, "STOP");
  }

  /**
   * Resumes the specified process by sending a SIGCONT using the kill command.
   * @param proc The specified process.
   * @throws Exception If error prevents us from resuming the process.
   */
  static void resumeProcess(Process proc) throws Exception {
    signalProcess(proc, "CONT");
  }

  /**
   * This is used to generate unique loopback IPs for parallel test running.
   * @return the local PID of this process
   */
  static int getPid() {
    try {
      RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
      java.lang.reflect.Field jvm = runtime.getClass().getDeclaredField("jvm");
      jvm.setAccessible(true);
      VMManagement mgmt = (VMManagement)jvm.get(runtime);
      Method pid_method = mgmt.getClass().getDeclaredMethod("getProcessId");
      pid_method.setAccessible(true);

      return (Integer)pid_method.invoke(mgmt);
    } catch (Exception e) {
      LOG.warn("Cannot get PID", e);
      return 1;
    }
  }

  /**
   * The generated IP is based on pid, so this requires that the parallel tests
   * run in separate VMs.
   *
   * On OSX, the above trick doesn't work, so we can't run parallel tests on OSX.
   * Given that, we just return the normal localhost IP.
   *
   * @return a unique loopback IP address for this PID. This allows running
   * tests in parallel, since 127.0.0.0/8 all act as loopbacks on Linux.
   */
  static String getUniqueLocalhost() {
    if ("Mac OS X".equals(System.getProperty("os.name"))) {
      return "127.0.0.1";
    }

    int pid = getPid();
    return "127." + ((pid & 0xff00) >> 8) + "." + (pid & 0xff) + ".1";
  }

  /**
   * Get a PartitionPB with empty start and end keys.
   * @return a fake partition
   */
  static Common.PartitionPB.Builder getFakePartitionPB() {
    Common.PartitionPB.Builder partition = Common.PartitionPB.newBuilder();
    partition.setPartitionKeyStart(ByteString.EMPTY);
    partition.setPartitionKeyEnd(ByteString.EMPTY);
    return partition;
  }

  /**
   * Create a ReplicaPB based on the passed information.
   * @param uuid server's identifier
   * @param host server's hostname
   * @param port server's port
   * @param role server's role in the configuration
   * @return a fake ReplicaPB
   */
  static Master.TabletLocationsPB.ReplicaPB.Builder getFakeTabletReplicaPB(
      String uuid, String host, int port, Metadata.RaftPeerPB.Role role) {
    Master.TSInfoPB.Builder tsInfoBuilder = Master.TSInfoPB.newBuilder();
    Common.HostPortPB.Builder hostBuilder = Common.HostPortPB.newBuilder();
    hostBuilder.setHost(host);
    hostBuilder.setPort(port);
    tsInfoBuilder.addRpcAddresses(hostBuilder);
    tsInfoBuilder.setPermanentUuid(ByteString.copyFromUtf8(uuid));
    Master.TabletLocationsPB.ReplicaPB.Builder replicaBuilder =
        Master.TabletLocationsPB.ReplicaPB.newBuilder();
    replicaBuilder.setTsInfo(tsInfoBuilder);
    replicaBuilder.setRole(role);
    return replicaBuilder;
  }
}
