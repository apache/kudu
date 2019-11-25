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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unit test for ResultReporter. */
public class TestResultReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestResultReporter.class);
  private static final String BIND_ADDR = "127.0.0.1";
  private Server server;
  private MockFlakyTestServlet flakyTestServlet;

  @Rule
  public RetryRule retryRule = new RetryRule();

  /** Record of a specific test run. */
  private static class TestRecord {
    public String testName;
    public String buildTag;
    public String revision;
    public String hostname;
    public String buildConfig;
    public int status;
    public String log;

    public TestRecord(Map<String, String> params) {
      testName = params.get("test_name");
      buildTag = params.get("build_id");
      revision = params.get("revision");
      hostname = params.get("hostname");
      buildConfig = params.get("build_config");
      status = Integer.parseInt(params.get("status"));
      log = params.get("log");
    }

    @Override
    public String toString() {
      List<String> required = ImmutableList.of(
          testName, buildTag, revision, hostname, buildConfig, Integer.toString(status));
      List<String> all = new ArrayList<>(required);
      if (log != null) {
        all.add(log);
      }
      return Joiner.on(" ").join(all);
    }
  }

  /**
   * Mock implementation of the flaky test server.
   *
   * Must be a servlet (not just a Jetty handler) to support multipart forms.
   */
  private static class MockFlakyTestServlet extends HttpServlet {
    private static final Logger LOG = LoggerFactory.getLogger(MockFlakyTestServlet.class);
    private static final long serialVersionUID = 1L;
    private final transient List<TestRecord> records = new ArrayList<>();

    List<TestRecord> getRecords() {
      return records;
    }

    @Override
    protected void doPost(HttpServletRequest request,
                          HttpServletResponse response) throws IOException, ServletException {
      LOG.debug("Handling request {}: ", request);

      // Process the form parts into key/value pairs.
      Map<String, String> params = new HashMap<>();
      for (Part p : request.getParts()) {
        params.put(p.getName(), IOUtils.toString(p.getInputStream(), UTF_8));
      }

      // We're done processing the request.
      records.add(new TestRecord(params));
      response.setContentType("text/html; charset=utf-8");
      response.setStatus(HttpServletResponse.SC_OK);
    }
  }

  @Before
  public void setup() throws Exception {
    flakyTestServlet = new MockFlakyTestServlet();

    // This Enterprise Java nonsense is to enable multipart form submission. The
    // servlet is configured to only spill parts to disk if they exceed 1 MB in
    // size, which isn't a concern for this test.
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");
    ServletHolder holder = new ServletHolder(flakyTestServlet);
    holder.getRegistration().setMultipartConfig(new MultipartConfigElement(
        "",            // location
        1024 * 1024,   // maxFileSize
        1024 * 1024,   // maxRequestSize
        1024 * 1024)); // fileSizeThreshold
    context.addServlet(holder, "/*");

    server = new Server(new InetSocketAddress(BIND_ADDR, 0));
    server.setHandler(context);
    server.start();
  }

  @After
  public void teardown() throws Exception {
    server.stop();
    server.join();
  }

  @Test
  public void testRoundTrip() throws IOException {
    final ResultReporter.Options options = new ResultReporter.Options();
    assertNotNull(server);
    assertTrue(server.isStarted());
    assertNotNull(server.getURI());
    options.httpEndpoint(BIND_ADDR + ":" + server.getURI().getPort())
           .buildTag("shark")
           .revision("do")
           .hostname("do-do")
           .buildConfig("do-do-do");
    ResultReporter.Result[] expectedResults = {
        ResultReporter.Result.SUCCESS, ResultReporter.Result.FAILURE };
    String[] testNames = { "baby", "mommy", "daddy"};
    String logFormat = "%s: a log message";
    ResultReporter reporter = new ResultReporter(options);
    int expectedRecords = 0;
    for (ResultReporter.Result result : expectedResults) {
      for (String testName : testNames) {
        File tempLogFile = null;
        if (result == ResultReporter.Result.FAILURE) {
          tempLogFile = File.createTempFile("test_log", ".txt");
          tempLogFile.deleteOnExit();
          FileOutputStream fos = new FileOutputStream(tempLogFile);
          IOUtils.write(String.format(logFormat, testName), fos, UTF_8);
        }
        reporter.reportResult(testName, result, tempLogFile);
        expectedRecords++;
      }
    }
    assertEquals(expectedRecords, flakyTestServlet.getRecords().size());
    Iterator<TestRecord> iterator = flakyTestServlet.getRecords().iterator();
    for (ResultReporter.Result result : expectedResults) {
      for (String testName : testNames) {
        assertTrue(iterator.hasNext());
        TestRecord record = iterator.next();
        LOGGER.info(record.toString());
        assertEquals(testName, record.testName);
        assertEquals(result == ResultReporter.Result.SUCCESS ? 0 : 1, record.status);
        assertEquals(result == ResultReporter.Result.FAILURE ?
                     String.format(logFormat, testName) : null, record.log);
      }
    }
  }

  @Test
  public void testHostName() {
    // Just tests that this doesn't throw an exception.
    LOGGER.info(ResultReporter.getLocalHostname());
  }
}
