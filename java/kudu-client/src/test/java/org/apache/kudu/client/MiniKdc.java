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

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kudu.annotations.InterfaceAudience;

/**
 * A managed Kerberos Key Distribution Center.
 *
 * Provides utility functions to create users and services which can authenticate
 * to the KDC.
 *
 * The KDC is managed as an external process, using the krb5 binaries installed on the system.
 *
 * For debugging Kerberos client issues, it can be helpful to add
 * {@code -Dsun.security.krb5.debug=true} to the JVM properties.
 */
@InterfaceAudience.Private
@NotThreadSafe
public class MiniKdc implements Closeable {

  // The KDC port will be assigned starting from this value.
  private static final int PORT_START = 64530;

  private static final Logger LOG = LoggerFactory.getLogger(MiniKuduCluster.class);

  private final Options options;

  private Process kdcProcess;

  private Thread kdcProcessLogRedirector;

  /**
   * Options for the MiniKdc.
   */
  public static class Options {
    private final String realm;
    private final Path dataRoot;
    private final int port;

    public Options(String realm, Path dataRoot, int port) {
      Preconditions.checkArgument(port > 0);
      this.realm = realm;
      this.dataRoot = dataRoot;
      this.port = port;
    }

    public String getRealm() {
      return realm;
    }

    public Path getDataRoot() {
      return dataRoot;
    }

    public int getPort() {
      return port;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
                        .add("realm", realm)
                        .add("dataRoot", dataRoot)
                        .add("port", port)
                        .toString();
    }
  }

  /**
   * Creates a MiniKdc with explicit options.
   */
  public MiniKdc(Options options) {
    this.options = options;
  }

  /**
   * Creates a MiniKdc with default options.
   */
  public static MiniKdc withDefaults() throws IOException {
    return new MiniKdc(
        new Options("KRBTEST.COM",
                    Paths.get(TestUtils.getBaseDir(), "krb5kdc-" + System.currentTimeMillis()),
                    TestUtils.findFreeUdpPort(PORT_START)));
  }

  /**
   * Start the MiniKdc.
   */
  public void start() throws IOException {
    Preconditions.checkState(kdcProcess == null);
    LOG.debug("starting KDC {}", options);

    File dataRootDir = options.dataRoot.toFile();
    if (!dataRootDir.exists()) {
      if (!dataRootDir.mkdir()) {
        throw new RuntimeException(String.format("unable to create krb5 state directory: %s",
                                                 dataRootDir));
      }

      createKdcConf();
      createKrb5Conf();

      // Create the KDC database using the kdb5_util tool.
      checkReturnCode(
          startProcessWithKrbEnv(
              getBinaryPath("kdb5_util"),
              "create",
              "-s", // Stash the master password.
              "-P", "masterpw", // Set a password.
              "-W" // Use weak entropy (since we don't need real security).
          ), "kdb5_util", true);
    }

    kdcProcess = startProcessWithKrbEnv(getBinaryPath("krb5kdc"),
                                        "-n"); // Do not daemonize.

    // Redirect the KDC output to SLF4J.
    kdcProcessLogRedirector = new Thread(
        new MiniKuduCluster.ProcessInputStreamLogPrinterRunnable(kdcProcess.getInputStream()),
        "krb5kdc:" + options.port);
    kdcProcessLogRedirector.setDaemon(true);
    kdcProcessLogRedirector.start();

    // The C++ MiniKdc defaults to binding the KDC to an ephemeral port, which
    // it then finds using lsof at this point. Java is unable to do that since
    // the Process API does not expose the subprocess PID. As a result, this
    // MiniKdc doesn't support binding to an ephemeral port, and we use the
    // race-prone TestUtils.findFreePort instead. The upside is that we
    // don't have to rewrite the config files.
  }

  /**
   * Creates a new Kerberos user with the given username.
   * @param username the new user
   */
  void createUserPrincipal(String username) throws IOException {
    checkReturnCode(
        startProcessWithKrbEnv(
            getBinaryPath("kadmin.local"),
            "-q",
            String.format("add_principal -pw %s %s", username, username)
        ), "kadmin.local", true);
  }

  /**
   * Kinit a user with the mini KDC.
   * @param username the user to kinit
   */
  void kinit(String username) throws IOException {
    Process proc = startProcessWithKrbEnv(getBinaryPath("kinit"), username);
    proc.getOutputStream().write(username.getBytes());
    proc.getOutputStream().close();
    checkReturnCode(proc, "kinit", true);
  }

  /**
   * Returns the output from the 'klist' utility. This is useful for logging the
   * local ticket cache state.
   */
  String klist() throws IOException {
    Process proc = startProcessWithKrbEnv(getBinaryPath("klist"), "-A");
    checkReturnCode(proc, "klist", false);
    return CharStreams.toString(new InputStreamReader(proc.getInputStream()));
  }

  /**
   * Creates a new service principal and associated keytab, returning its path.
   * @param spn the desired service principal name (e.g. "kudu/foo.example.com").
   *            If the principal already exists, its key will be reset and a new
   *            keytab will be generated.
   * @return the path to the new services' keytab file.
   */
  Path createServiceKeytab(String spn) throws IOException {
    Path kt_path = options.dataRoot.resolve(spn.replace('/', '_') + ".keytab");
    String kadmin = getBinaryPath("kadmin.local");
    checkReturnCode(startProcessWithKrbEnv(kadmin,
                                           "-q",
                                           String.format("add_principal -randkey %s", spn)),
                    "kadmin.local", true);

    checkReturnCode(startProcessWithKrbEnv(kadmin,
                                           "-q",
                                           String.format("ktadd -k %s %s", kt_path, spn)),
                    "kadmin.local", true);
    return kt_path;
  }

  private void createKrb5Conf() throws IOException {
    List<String> contents = ImmutableList.of(
        "[logging]",
        "   kdc = FILE:/dev/stderr",

        "[libdefaults]",
        "   default_realm = " + options.realm,
        "   dns_lookup_kdc = false",
        "   dns_lookup_realm = false",
        "   forwardable = true",
        "   renew_lifetime = 7d",
        "   ticket_lifetime = 24h",

        // Disable aes256, since Java does not support it without JCE, see
        // https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/jgss-features.html
        "   default_tkt_enctypes = aes128-cts des3-cbc-sha1",
        "   default_tgs_enctypes = aes128-cts des3-cbc-sha1",
        "   permitted_enctypes = aes128-cts des3-cbc-sha1",

        // In miniclusters, we start daemons on local loopback IPs that
        // have no reverse DNS entries. So, disable reverse DNS.
        "   rdns = false",
        "   ignore_acceptor_hostname = true",

        "[realms]",
        options.realm + " = {",
        "   kdc = 127.0.0.1:" + options.port,
        "}");

    Files.write(options.dataRoot.resolve("krb5.conf"), contents, Charsets.UTF_8);
  }

  private void createKdcConf() throws IOException {
    List<String> contents = ImmutableList.of(
        "[kdcdefaults]",
        "   kdc_ports = " + options.port,

        "[realms]",
        options.realm + " = {",
        "   acl_file = " + options.dataRoot.resolve("kadm5.acl"),
        "   admin_keytab = " + options.dataRoot.resolve("kadm5.keytab"),
        "   database_name = " + options.dataRoot.resolve("principal"),
        "   key_stash_file = " + options.dataRoot.resolve(".k5." + options.realm),
        "   max_renewable_life = 7d 0h 0m 0s",
        "}");

    Files.write(options.dataRoot.resolve("kdc.conf"), contents, Charsets.UTF_8);
  }

  /**
   * Stop the MiniKdc.
   */
  public void stop() throws IOException {
    Preconditions.checkState(kdcProcess != null);
    LOG.debug("stopping KDC {}", options);
    try {
      kdcProcess.destroy();
      kdcProcess.waitFor();
      kdcProcessLogRedirector.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      kdcProcess = null;
      kdcProcessLogRedirector = null;
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    LOG.debug("closing KDC {}", options);
    try {
      if (kdcProcess != null) {
        stop();
      }
    } finally {
      FileUtils.deleteDirectory(options.dataRoot.toFile());
    }
  }

  private static final List<String> KRB5_BINARY_PATHS = ImmutableList.of(
      "/usr/local/opt/krb5/sbin", // Homebrew
      "/usr/local/opt/krb5/bin", // Homebrew
      "/opt/local/sbin", // Macports
      "/opt/local/bin", // Macports
      "/usr/lib/mit/sbin", // SLES
      "/usr/sbin" // Linux
  );

  public Map<String, String> getEnvVars() {
    return ImmutableMap.of(
        "KRB5_CONFIG", options.dataRoot.resolve("krb5.conf").toString(),
        "KRB5_KDC_PROFILE", options.dataRoot.resolve("kdc.conf").toString(),
        "KRB5CCNAME", getTicketCachePath(),
        "KUDU_ENABLE_KRB5_REALM_FIX", "yes"
    );
  }

  /**
   * @return the path of the Kerberos ticket/credential cache
   */
  public String getTicketCachePath() {
    return options.dataRoot.resolve("krb5cc").toString();
  }

  private Process startProcessWithKrbEnv(String... argv) throws IOException {

    ProcessBuilder procBuilder = new ProcessBuilder(argv);
    procBuilder.environment().putAll(getEnvVars());
    LOG.debug("executing '{}', env: '{}'",
              Joiner.on(" ").join(procBuilder.command()),
              Joiner.on(", ").withKeyValueSeparator("=").join(procBuilder.environment()));
    return procBuilder.redirectErrorStream(true).start();
  }

  /**
   * Waits for the process to exit, checking the return code. Any output to the
   * process' stdout is optionally logged to SLF4J.
   * @param process the process to check
   * @param name the name of the process
   * @param log whether to log the process' stdout.
   */
  private static void checkReturnCode(Process process, String name, boolean log) throws IOException {
    int ret;
    try {
      ret = process.waitFor();
      if (log) {
        // Reading the output *after* waiting for the process to close can deadlock
        // if the process overwhelms the output buffer, however none of the krb5
        // utilities are known to do that.
        try (BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
          String line;
          while ((line = in.readLine()) != null) {
            LOG.debug(line);
          }
        }
      }
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new IOException(String.format("process '%s' interrupted", name));
    }
    if (ret != 0) {
      throw new IOException(String.format("process '%s' failed: %s", name, ret));
    }
  }

  private static String getBinaryPath(String executable) throws IOException {
    return getBinaryPath(executable, KRB5_BINARY_PATHS);
  }

  private static String getBinaryPath(String executable,
                                      List<String> searchPaths) throws IOException {
    for (String path : searchPaths) {
      File f = Paths.get(path).resolve(executable).toFile();
      if (f.exists() && f.canExecute()) {
        return f.getPath();
      }
    }

    Process which = new ProcessBuilder().command("which", executable).start();
    checkReturnCode(which, "which", false);
    return CharStreams.toString(new InputStreamReader(which.getInputStream())).trim();
  }
}
