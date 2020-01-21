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

package org.apache.kudu.subprocess.echo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.subprocess.MessageIO;
import org.apache.kudu.subprocess.OutboundResponse;
import org.apache.kudu.subprocess.Subprocess.EchoResponsePB;
import org.apache.kudu.subprocess.Subprocess.SubprocessMetricsPB;
import org.apache.kudu.subprocess.Subprocess.SubprocessResponsePB;
import org.apache.kudu.subprocess.SubprocessExecutor;
import org.apache.kudu.subprocess.SubprocessTestUtil;
import org.apache.kudu.test.junit.RetryRule;

/**
 * Tests for subprocess that handles EchoRequest messages in various conditions.
 */
public class TestEchoSubprocess extends SubprocessTestUtil {
  private static final Logger LOG = LoggerFactory.getLogger(TestEchoSubprocess.class);
  private static final String MESSAGE = "We are one. We are many.";

  @Rule
  public RetryRule retryRule = new RetryRule();

  // Given that executors run multiple threads, the exceptions that we expect
  // may not necessarily be the first thrown. This checks for the expected
  // error on all thrown exceptions, including suppressed ones.
  <T extends Throwable> void assertIncludingSuppressedThrows(Class<T> expectedThrowable,
                                                             String errorMessage,
                                                             ThrowingRunnable runnable) {
    try {
      runnable.run();
    } catch (Throwable actualThrown) {
      if (expectedThrowable.isInstance(actualThrown) &&
          actualThrown.toString().contains(errorMessage)) {
        return;
      }
      LOG.info(actualThrown.toString());
      for (Throwable s : actualThrown.getSuppressed()) {
        if (s.getClass() == expectedThrowable && s.toString().contains(errorMessage)) {
          return;
        }
        LOG.info(s.toString());
      }
      throw new AssertionError(String.format("No errors that match %s with message: %s",
                                             expectedThrowable.toString(), errorMessage));
    }
    throw new AssertionError("Didn't throw an exception");
  }

  /**
   * Test a regular old message. There should be no exceptions of any kind.
   * We should also see some metrics that make sense.
   */
  @Test(expected = TimeoutException.class)
  public void testBasicMsg() throws Exception {
    SubprocessExecutor executor =
        setUpExecutorIO(NO_ERR, /*injectIOError*/false);
    sendRequestToPipe(createEchoSubprocessRequest(MESSAGE));

    executor.run(NO_ARGS, new EchoProtocolHandler(), TIMEOUT_MS);
    SubprocessResponsePB spResp = receiveResponse();
    EchoResponsePB echoResp = spResp.getResponse().unpack(EchoResponsePB.class);
    Assert.assertEquals(MESSAGE, echoResp.getData());

    SubprocessMetricsPB spMetrics = spResp.getMetrics();
    // We only sent one request, so by the time the executor sent the message,
    // the queues should have been empty.
    Assert.assertTrue(spMetrics.hasInboundQueueLength());
    Assert.assertTrue(spMetrics.hasOutboundQueueLength());
    Assert.assertEquals(0, spMetrics.getInboundQueueLength());
    Assert.assertEquals(0, spMetrics.getOutboundQueueLength());

    // The recorded times should be non-zero.
    Assert.assertTrue(spMetrics.hasInboundQueueTimeMs());
    Assert.assertTrue(spMetrics.hasOutboundQueueTimeMs());
    Assert.assertTrue(spMetrics.hasExecutionTimeMs());
    Assert.assertTrue(spMetrics.getInboundQueueTimeMs() >= 0);
    Assert.assertTrue(spMetrics.getOutboundQueueTimeMs() >= 0);
    Assert.assertTrue(spMetrics.getExecutionTimeMs() >= 0);
  }

  /**
   * Test to see what happens when the execution is the bottleneck. We should
   * see it in the execution time and inbound queue time and length metrics.
   */
  @Test(expected = TimeoutException.class)
  public void testSlowExecutionMetrics() throws Exception {
    SubprocessExecutor executor =
      setUpExecutorIO(NO_ERR, /*injectIOError*/false);
    final int SLEEP_MS = 200;
    sendRequestToPipe(createEchoSubprocessRequest(MESSAGE, SLEEP_MS));
    sendRequestToPipe(createEchoSubprocessRequest(MESSAGE, SLEEP_MS));
    sendRequestToPipe(createEchoSubprocessRequest(MESSAGE, SLEEP_MS));

    // Run the executor with a single parser thread so we can make stronger
    // assumptions about timing.
    executor.run(new String[]{"-p", "1"}, new EchoProtocolHandler(), TIMEOUT_MS);

    SubprocessMetricsPB m = receiveResponse().getMetrics();
    long inboundQueueLength = m.getInboundQueueLength();
    long inboundQueueTimeMs = m.getInboundQueueTimeMs();
    long executionTimeMs = m.getExecutionTimeMs();
    // By the time the first request is written, the second should be sleeping,
    // and the third should be waiting in the inbound queue. That said, the
    // second could also be in the queue if the parser thread is slow to pick
    // up the second request.
    Assert.assertTrue(
        String.format("Got an unexpected inbound queue length: %s", inboundQueueLength),
        inboundQueueLength == 1 || inboundQueueLength == 2);
    Assert.assertEquals(0, m.getOutboundQueueLength());

    // We can't make many guarantees about how long the first request was
    // waiting in the queues.
    Assert.assertTrue(
        String.format("Expected a positive inbound queue time: %s", inboundQueueTimeMs),
        inboundQueueTimeMs >= 0);

    // It should've taken longer than our sleep to execute.
    Assert.assertTrue(
        String.format("Expected a longer execution time than %s ms: %s ms",
                      SLEEP_MS, executionTimeMs),
        executionTimeMs >= SLEEP_MS);

    // The second request should've spent the duration of the first sleep waiting
    // in the inbound queue.
    m = receiveResponse().getMetrics();
    Assert.assertTrue(
        String.format("Expected a higher inbound queue time: %s ms", m.getInboundQueueTimeMs()),
        m.getInboundQueueTimeMs() >= SLEEP_MS);

    // The last should've spent the duration of the first two sleeps waiting.
    m = receiveResponse().getMetrics();
    Assert.assertTrue(
        String.format("Expected a higher inbound queue time: %s", m.getInboundQueueTimeMs()),
        m.getInboundQueueTimeMs() >= 2 * SLEEP_MS);
  }

  /**
   * Test to see what happens when writing is the bottleneck. We should see it
   * in the outbound queue metrics.
   */
  @Test(expected = TimeoutException.class)
  public void testSlowWriterMetrics() throws Exception {
    SubprocessExecutor executor =
      setUpExecutorIO(NO_ERR, /*injectIOError*/false);
    final int BLOCK_MS = 200;
    executor.blockWriteMs(BLOCK_MS);
    sendRequestToPipe(createEchoSubprocessRequest(MESSAGE));
    sendRequestToPipe(createEchoSubprocessRequest(MESSAGE));
    sendRequestToPipe(createEchoSubprocessRequest(MESSAGE));
    executor.run(NO_ARGS, new EchoProtocolHandler(), TIMEOUT_MS);

    // In writing the first request, the other two requests should've been
    // close behind, likely both in the outbound queue.
    SubprocessMetricsPB m = receiveResponse().getMetrics();
    Assert.assertEquals(2, m.getOutboundQueueLength());

    m = receiveResponse().getMetrics();
    Assert.assertEquals(1, m.getOutboundQueueLength());
    Assert.assertTrue(
      String.format("Expected a higher outbound queue time: %s ms", m.getOutboundQueueTimeMs()),
      m.getOutboundQueueTimeMs() >= BLOCK_MS);

    m = receiveResponse().getMetrics();
    Assert.assertEquals(0, m.getOutboundQueueLength());
    Assert.assertTrue(
      String.format("Expected a higher outbound queue time: %s ms", m.getOutboundQueueTimeMs()),
      m.getOutboundQueueTimeMs() >= 2 * BLOCK_MS);
  }

  /**
   * Test what happens when we send a message that is completely empty (i.e.
   * not an empty SubprocessRequestPB message -- no message at all).
   */
  @Test(expected = TimeoutException.class)
  public void testMsgWithEmptyMessage() throws Exception {
    SubprocessExecutor executor = setUpExecutorIO(NO_ERR,
                                                  /*injectIOError*/false);
    requestSenderPipe.write(MessageIO.intToBytes(0));
    executor.run(NO_ARGS, new EchoProtocolHandler(), TIMEOUT_MS);

    // We should see no bytes land in the receiver pipe.
    Assert.assertEquals(-1, responseReceiverPipe.read());
  }

  /**
   * Test what happens when we send a message that isn't protobuf.
   */
  @Test
  public void testMalformedPB() throws Exception {
    SubprocessExecutor executor = setUpExecutorIO(NO_ERR, /*injectIOError*/false);
    requestSenderPipe.write("malformed".getBytes(StandardCharsets.UTF_8));
    Throwable thrown = Assert.assertThrows(ExecutionException.class,
        () -> executor.run(NO_ARGS, new EchoProtocolHandler(), TIMEOUT_MS));
    Assert.assertTrue(thrown.getMessage().contains("Unable to read the protobuf message"));
  }

  /**
   * Try injecting an <code>IOException</code> to the pipe that gets written to
   * by the SubprocessExecutor. We should exit with a
   * <code>KuduSubprocessException</code>
   */
  @Test
  public void testInjectIOException() throws Exception {
    SubprocessExecutor executor =
        setUpExecutorIO(HAS_ERR, /*injectIOError*/true);
    sendRequestToPipe(createEchoSubprocessRequest(MESSAGE));
    // NOTE: we don't expect the ExecutionException from the MessageWriter's
    // CompletableFuture because, in waiting for completion, the MessageReader
    // times out before CompletableFuture.get() is called on the writer.
    assertIncludingSuppressedThrows(IOException.class,
      "Unable to write to print stream",
      () -> executor.run(NO_ARGS, new EchoProtocolHandler(), TIMEOUT_MS));
  }

  /**
   * Parses message with <code>InterruptedException</code> injected should exit
   * with <code>KuduSubprocessException</code>.
   */
  @Test
  public void testInjectInterruptedException() throws Exception {
    SubprocessExecutor executor =
        setUpExecutorIO(HAS_ERR, /*injectIOError*/false);
    executor.interrupt();
    sendRequestToPipe(createEchoSubprocessRequest(MESSAGE));
    assertIncludingSuppressedThrows(ExecutionException.class,
        "Unable to put the message to the queue",
        () -> executor.run(NO_ARGS, new EchoProtocolHandler(), TIMEOUT_MS));
  }

  /**
   * Check that even if the writer is blocked writing, the
   * <code>MessageParser</code> tasks can continue making progress.
   */
  @Test
  public void testMessageParser() throws Exception  {
    SubprocessExecutor executor =
        setUpExecutorIO(NO_ERR, /*injectIOError*/false);
    sendRequestToPipe(createEchoSubprocessRequest("a"));
    sendRequestToPipe(createEchoSubprocessRequest("b"));
    executor.blockWriteMs(1000);
    Assert.assertThrows(TimeoutException.class,
        () -> executor.run(NO_ARGS, new EchoProtocolHandler(), /*timeoutMs*/500));

    // We should see a single message in the outbound queue. The other one is
    // blocked writing.
    BlockingQueue<OutboundResponse> outboundQueue = executor.getOutboundQueue();
    Assert.assertEquals(1, outboundQueue.size());
  }
}
