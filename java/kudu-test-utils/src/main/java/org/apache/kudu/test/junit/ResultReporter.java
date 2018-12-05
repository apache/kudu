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
package org.apache.kudu.test.junit;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.http.StatusLine;
import org.apache.http.util.EntityUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;

/** Class to report test results to the flaky test server. */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ResultReporter {
  public enum Result {
    SUCCESS,
    FAILURE
  }

  public static class Options {
    private boolean reportResults = true;
    private String httpEndpoint;
    private String buildTag;
    private String revision;
    private String hostname;
    private String buildConfig;

    public Options reportResults(boolean reportResults) {
      this.reportResults = reportResults;
      return this;
    }
    public Options httpEndpoint(String httpEndpoint) {
      this.httpEndpoint = httpEndpoint;
      return this;
    }
    public Options buildTag(String buildTag) {
      this.buildTag = buildTag;
      return this;
    }
    public Options revision(String revision) {
      this.revision = revision;
      return this;
    }
    public Options hostname(String hostname) {
      this.hostname = hostname;
      return this;
    }
    public Options buildConfig(String buildConfig) {
      this.buildConfig = buildConfig;
      return this;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(ResultReporter.class);
  private static final String KUDU_REPORT_TEST_RESULTS_VAR = "KUDU_REPORT_TEST_RESULTS";
  private static final String TEST_RESULT_SERVER_VAR = "TEST_RESULT_SERVER";
  private static final String BUILD_TAG_VAR = "BUILD_TAG";
  private static final String GIT_REVISION_VAR = "GIT_REVISION";
  private static final String BUILD_CONFIG_VAR = "BUILD_CONFIG";

  private final Options options;

  public ResultReporter() {
    this(new Options()
        .reportResults(isReportingConfigured())
        .httpEndpoint(getEnvStringWithDefault(TEST_RESULT_SERVER_VAR,
                                              "localhost:8080"))
        .buildTag(System.getenv(BUILD_TAG_VAR))
        .revision(System.getenv(GIT_REVISION_VAR))
        .buildConfig(System.getenv(BUILD_CONFIG_VAR))
        .hostname(getLocalHostname()));
  }

  @InterfaceAudience.LimitedPrivate("Test")
  ResultReporter(Options options) {
    this.options = options;
  }

  private static boolean isVarSetAndNonEmpty(String name) {
    String var = System.getenv(name);
    return var != null && !var.equals("");
  }

  private static boolean areRequiredReportingVarsSetAndNonEmpty() {
    return isVarSetAndNonEmpty(BUILD_TAG_VAR) &&
           isVarSetAndNonEmpty(GIT_REVISION_VAR) &&
           isVarSetAndNonEmpty(BUILD_CONFIG_VAR);
  }

  private static String reportingVarDump() {
    List<String> vars = new ArrayList<>();
    for (String var : ImmutableList.of(TEST_RESULT_SERVER_VAR,
                                       BUILD_TAG_VAR,
                                       GIT_REVISION_VAR,
                                       BUILD_CONFIG_VAR)) {
      vars.add(var + ": \"" + System.getenv(var) + "\"");
    }
    return Joiner.on(", ").join(vars);
  }

  private static boolean isReportingConfigured() {
    if (getEnvIntegerWithDefault(KUDU_REPORT_TEST_RESULTS_VAR, 0) == 0) {
      return false;
    }
    if (!areRequiredReportingVarsSetAndNonEmpty()) {
      throw new IllegalStateException("Not all required variables are set: " +
                                      reportingVarDump());
    }
    return true;
  }

  private static String getEnvStringWithDefault(String name,
                                                String defaultValue) {
    String value = System.getenv(name);
    if (value == null || value.isEmpty()) {
      return defaultValue;
    }
    return value;
  }

  private static int getEnvIntegerWithDefault(String name, int defaultValue) {
    return Integer.parseInt(getEnvStringWithDefault(
        name, String.valueOf(defaultValue)));
  }

  /**
   * Invokes the `hostname` UNIX utility to retrieve the machine's hostname.
   *
   * Note: this is not the same as InetAddress.getLocalHost().getHostName(),
   * which performs a reverse DNS lookup and may return a different result,
   * depending on the machine's networking configuration. The equivalent C++
   * code uses `hostname`, so it's important we do the same here for parity.
   *
   * @returns the local hostname
   */
  @InterfaceAudience.LimitedPrivate("Test")
  static String getLocalHostname() {
    ProcessBuilder pb = new ProcessBuilder("hostname");
    try {
      Process p = pb.start();
      try (InputStreamReader isr = new InputStreamReader(p.getInputStream(), UTF_8);
           BufferedReader br = new BufferedReader(isr)) {
        int rv = p.waitFor();
        if (rv != 0) {
          throw new IllegalStateException(String.format(
              "Process 'hostname' exited with exit status %d", rv));
        }
        return br.readLine();
      }
    } catch (InterruptedException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Reports a test result to the flaky test server.
   *
   * @param testName the display name of the JUnit test
   * @param result success or failure
   * @param logFile optionally, file containing log messages generated by the test
   * @throws IOException if test reporting failed
   */
  public void reportResult(String testName, Result result, File logFile)
      throws IOException {
    if (!options.reportResults) return;

    try (CloseableHttpClient client = HttpClients.createDefault()) {
      HttpPost post = new HttpPost("http://" + options.httpEndpoint + "/add_result");

      // Set up the request with all form parts.
      MultipartEntityBuilder meb = MultipartEntityBuilder.create();
      // In the backend, the BUILD_TAG field is called 'build_id', but we can't use
      // that as an env variable because it'd collide with Jenkins' BUILD_ID.
      meb.addTextBody("build_id", options.buildTag);
      meb.addTextBody("hostname", options.hostname);
      meb.addTextBody("revision", options.revision);
      meb.addTextBody("build_config", options.buildConfig);
      meb.addTextBody("test_name", testName);
      // status=0 indicates success, status=1 indicates failure.
      meb.addTextBody("status", Integer.toString(result == Result.SUCCESS ? 0 : 1));
      if (logFile != null) {
        meb.addBinaryBody("log", logFile, ContentType.APPLICATION_OCTET_STREAM,
                          testName + ".txt.gz");
      }
      post.setEntity(meb.build());

      // Send the request and process the response.
      try (CloseableHttpResponse resp = client.execute(post)) {
        StatusLine sl = resp.getStatusLine();
        if (sl.getStatusCode() != 200) {
          throw new IOException("Bad response from server: " + sl.getStatusCode() + ": " +
                                EntityUtils.toString(resp.getEntity(), UTF_8));
        }
      }
    }
  }

  /**
   * Same as {@link #reportResult(String, Result)} but never throws an exception.
   * Logs a warning message on failure.
   */
  public void tryReportResult(String testName, Result result, File logFile) {
    try {
      reportResult(testName, result, logFile);
    } catch (IOException ex) {
      LOG.warn("Failed to record test result for {} as {}", testName, result, ex);
    }
  }

  /**
   * @return whether result reporting is enabled for this reporter
   */
  public boolean isReportingEnabled() {
    return options.reportResults;
  }
}
