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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import com.google.common.primitives.Bytes;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.subprocess.KuduSubprocessException;
import org.apache.kudu.subprocess.MessageIO;
import org.apache.kudu.subprocess.MessageTestUtil;
import org.apache.kudu.subprocess.Subprocess.SubprocessResponsePB;
import org.apache.kudu.subprocess.SubprocessExecutor;
import org.apache.kudu.test.junit.RetryRule;

/**
 * Tests for subprocess that handles EchoRequest messages in various conditions.
 */
public class TestEchoSubprocess {
  private static final Logger LOG = LoggerFactory.getLogger(TestEchoSubprocess.class);
  private static final Function<Throwable, Void> NO_ERR = e -> {
    LOG.error(String.format("Unexpected error: %s", e.getMessage()));
    Assert.fail();
    return null;
  };
  private static final Function<Throwable, Void> HAS_ERR = e -> {
    Assert.assertTrue(e instanceof KuduSubprocessException);
    return null;
  };

  @Rule
  public RetryRule retryRule = new RetryRule();

  public static class PrintStreamWithIOException extends PrintStream {
    public PrintStreamWithIOException(OutputStream out, boolean autoFlush, String encoding)
            throws UnsupportedEncodingException {
      super(out, autoFlush, encoding);
    }

    @Override
    public boolean checkError() {
      return true;
    }
  }

  void runEchoSubprocess(InputStream in,
                         PrintStream out,
                         String[] args,
                         Function<Throwable, Void> errorHandler,
                         boolean injectInterrupt)
      throws InterruptedException, ExecutionException, TimeoutException {
    System.setIn(in);
    System.setOut(out);
    SubprocessExecutor subprocessExecutor = new SubprocessExecutor(errorHandler);
    EchoProtocolHandler protocolProcessor = new EchoProtocolHandler();
    if (injectInterrupt) {
      subprocessExecutor.interrupt();
    }
    subprocessExecutor.run(args, protocolProcessor, /* timeoutMs= */1000);
  }

  /**
   * Parses non-malformed message should exit normally without any exceptions.
   */
  @Test(expected = TimeoutException.class)
  public void testBasicMsg() throws Exception {
    final String message = "data";
    final byte[] messageBytes = MessageTestUtil.serializeMessage(
        MessageTestUtil.createEchoSubprocessRequest(message));
    final InputStream in = new ByteArrayInputStream(messageBytes);
    final PrintStream out =
            new PrintStream(new ByteArrayOutputStream(), false, "UTF-8");
    final String[] args = {""};
    runEchoSubprocess(in, out, args, NO_ERR, /* injectInterrupt= */false);
  }

  /**
   * Parses message with empty payload should exit normally without any exceptions.
   */
  @Test(expected = TimeoutException.class)
  public void testMsgWithEmptyPayload() throws Exception {
    final byte[] emptyPayload = MessageIO.intToBytes(0);
    final InputStream in = new ByteArrayInputStream(emptyPayload);
    final PrintStream out =
            new PrintStream(new ByteArrayOutputStream(), false, "UTF-8");
    final String[] args = {""};
    runEchoSubprocess(in, out, args, NO_ERR, /* injectInterrupt= */false);
  }

  /**
   * Parses malformed message should cause <code>IOException</code>.
   */
  @Test
  public void testMalformedMsg() throws Exception {
    final byte[] messageBytes = "malformed".getBytes(StandardCharsets.UTF_8);
    final InputStream in = new ByteArrayInputStream(messageBytes);
    final PrintStream out =
            new PrintStream(new ByteArrayOutputStream(), false, "UTF-8");
    Throwable thrown = Assert.assertThrows(ExecutionException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Exception {
        final String[] args = {""};
        runEchoSubprocess(in, out, args, HAS_ERR, /* injectInterrupt= */false);
      }
    });
    Assert.assertTrue(thrown.getMessage().contains("Unable to read the protobuf message"));
  }

  /**
   * Parses message with <code>IOException</code> injected should exit with
   * <code>KuduSubprocessException</code>.
   */
  @Test
  public void testInjectIOException() throws Exception {
    final String message = "data";
    final byte[] messageBytes = MessageTestUtil.serializeMessage(
        MessageTestUtil.createEchoSubprocessRequest(message));
    final InputStream in = new ByteArrayInputStream(messageBytes);
    final PrintStream out =
            new PrintStreamWithIOException(new ByteArrayOutputStream(), false, "UTF-8");
    Throwable thrown = Assert.assertThrows(ExecutionException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Exception {
        final String[] args = {""};
        runEchoSubprocess(in, out, args, HAS_ERR, /* injectInterrupt= */false);
      }
    });
    Assert.assertTrue(thrown.getMessage().contains("Unable to write the protobuf messag"));
  }

  /**
   * Parses message with <code>InterruptedException</code> injected should exit
   * with <code>KuduSubprocessException</code>.
   */
  @Test
  public void testInjectInterruptedException() throws Exception {
    final String message = "data";
    final byte[] messageBytes = MessageTestUtil.serializeMessage(
        MessageTestUtil.createEchoSubprocessRequest(message));
    final InputStream in = new ByteArrayInputStream(messageBytes);
    final PrintStream out =
        new PrintStream(new ByteArrayOutputStream(), false, "UTF-8");
    Throwable thrown = Assert.assertThrows(ExecutionException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Exception {
        final String[] args = {""};
        runEchoSubprocess(in, out, args, HAS_ERR, /* injectInterrupt= */true);
      }
    });
    Assert.assertTrue(thrown.getMessage().contains("Unable to put the message to the queue"));
  }

  /**
   * Verifies when <code>MessageWriter</code> task is blocking on writing the message,
   * <code>MessageParser</code> task can continue processing the requests without
   * blocking.
   */
  @Test
  public void testMessageParser() throws Exception  {
    final byte[] messageBytes = Bytes.concat(
        MessageTestUtil.serializeMessage(MessageTestUtil.createEchoSubprocessRequest("a")),
        MessageTestUtil.serializeMessage(MessageTestUtil.createEchoSubprocessRequest("b")));
    final InputStream in = new ByteArrayInputStream(messageBytes);
    final PrintStream out =
        new PrintStream(new ByteArrayOutputStream(), false, "UTF-8");
    final SubprocessExecutor[] executors = new SubprocessExecutor[1];
    System.setIn(in);
    System.setOut(out);
    Assert.assertThrows(TimeoutException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Exception {
        final String[] args = {""};
        executors[0] = new SubprocessExecutor(NO_ERR);
        // Block the message write for 1000 Ms.
        executors[0].blockWriteMs(1000);
        executors[0].run(args, new EchoProtocolHandler(), /* timeoutMs= */500);
      }
    });

    // Verify that the message have been processed and placed to outbound queue.
    BlockingQueue<SubprocessResponsePB> outboundQueue = executors[0].getOutboundQueue();
    Assert.assertEquals(1, outboundQueue.size());
  }
}
