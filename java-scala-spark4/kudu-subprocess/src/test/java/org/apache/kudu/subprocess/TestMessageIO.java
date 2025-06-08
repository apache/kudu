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

package org.apache.kudu.subprocess;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;

import com.google.common.primitives.Bytes;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import org.apache.kudu.subprocess.Subprocess.SubprocessRequestPB;
import org.apache.kudu.test.junit.RetryRule;

/**
 * Tests for reading and writing protobuf message.
 */
public class TestMessageIO {

  @Rule
  public RetryRule retryRule = new RetryRule();

  public static class PrintStreamOverload extends PrintStream {
    public PrintStreamOverload(OutputStream out) {
      super(out);
    }

    /**
     * Expands the visibility of setError() for the tests to call it.
     */
    @Override
    public void setError() {
      super.setError();
    }
  }

  /**
   * Serializes a subprocess message that wraps EchoRequestPB and de-serializes
   * it to verify the content.
   */
  @Test
  public void testBasicEchoMessage() throws Exception {
    final String data = "data";
    final SubprocessRequestPB request = SubprocessTestUtil.createEchoSubprocessRequest(data);
    final byte[] message = SubprocessTestUtil.serializeMessage(request);
    final SubprocessRequestPB actualRequest = SubprocessTestUtil.deserializeMessage(
        message, SubprocessRequestPB.parser());
    Assert.assertEquals(request, actualRequest);
  }

  /**
   * Verifies that writing messages via <code>SubprocessOutputStream</code> can
   * catch errors thrown from underlying <code>PrintStream</code> and re-throws
   * <code>IOException</code>.
   */
  @Test
  public void testSubprocessOutputStream() {
    final String data = "data";
    final SubprocessRequestPB request = SubprocessTestUtil.createEchoSubprocessRequest(data);
    final PrintStreamOverload printStreamOverload =
        new PrintStreamOverload(new ByteArrayOutputStream());
    final BufferedOutputStream out = new BufferedOutputStream(
        new SubprocessOutputStream(printStreamOverload));
    final MessageIO messageIO = new MessageIO(
        SubprocessConfiguration.MAX_MESSAGE_BYTES_DEFAULT, /* in= */null, out);
    Throwable thrown = Assert.assertThrows(IOException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Exception {
        printStreamOverload.setError();
        messageIO.writeMessage(request);
      }
    });
    Assert.assertTrue(thrown.getMessage().contains(SubprocessOutputStream.WRITE_ERR));
  }

  /**
   * Verifies that reading malformed messages that has mismatched size
   * and body (not enough data in the body) should cause expected error.
   */
  @Test
  public void testMalformedMessageMismatchSize() {
    byte[] size = MessageIO.intToBytes(100);
    byte[] body = new byte[10];
    Arrays.fill(body, (byte)0);
    byte[] malformedMessage = Bytes.concat(size, body);
    BufferedInputStream in = new BufferedInputStream(new ByteArrayInputStream(malformedMessage));
    MessageIO messageIO = new MessageIO(SubprocessConfiguration.MAX_MESSAGE_BYTES_DEFAULT,
                                        in, /* out= */null);
    Throwable thrown = Assert.assertThrows(IOException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Exception {
        messageIO.readBytes();
      }
    });
    Assert.assertTrue(thrown.getMessage().contains("unable to receive message"));
  }

  /**
   * Verify that KuduSubprocessException is thrown by MessageIO.readBytes() when
   * an oversized message is detected in the input stream. After the oversized
   * message is read and discarded, next message can be read from the stream.
   */
  @Test
  public void testOversizedMessage() throws Exception {
    final int maxMessageSize = 32;
    byte[] size0 = MessageIO.intToBytes(maxMessageSize + 1);
    byte[] body0 = new byte[maxMessageSize + 1];
    Arrays.fill(body0, (byte) 0);
    byte[] msg0 = Bytes.concat(size0, body0);

    byte[] size1 = MessageIO.intToBytes(maxMessageSize);
    byte[] body1 = new byte[maxMessageSize];
    Arrays.fill(body1, (byte) 1);
    byte[] msg1 = Bytes.concat(size1, body1);

    byte[] msg = Bytes.concat(msg0, msg1);

    BufferedInputStream in = new BufferedInputStream(new ByteArrayInputStream(msg));

    MessageIO messageIO = new MessageIO(maxMessageSize, in, /* out= */null);
    Throwable thrown = Assert.assertThrows(KuduSubprocessException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Exception {
        messageIO.readBytes();
      }
    });
    Assert.assertTrue(thrown.getMessage().contains(
        "message size (33) exceeds maximum message size (32): message is discarded"));

    byte[] readMsg = messageIO.readBytes();
    Assert.assertEquals(maxMessageSize, readMsg.length);
    Assert.assertArrayEquals(body1, readMsg);
  }
}
