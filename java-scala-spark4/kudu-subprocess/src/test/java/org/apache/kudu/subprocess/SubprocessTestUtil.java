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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.function.Function;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.subprocess.Subprocess.EchoRequestPB;
import org.apache.kudu.subprocess.Subprocess.SubprocessRequestPB;

/**
 * Utility class of common functions used for testing subprocess.
 */
public class SubprocessTestUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SubprocessTestUtil.class);
  protected static final String[] NO_ARGS = {""};
  protected static final int TIMEOUT_MS = 1000;

  // Helper functors that can be passed around to ensure we either see an error
  // or not.
  protected static final Function<Throwable, Void> NO_ERR = e -> {
    LOG.error(String.format("Unexpected error: %s", e.getMessage()));
    fail();
    return null;
  };
  protected static final Function<Throwable, Void> HAS_ERR = e -> {
    assertTrue(e instanceof KuduSubprocessException);
    return null;
  };

  // Pipe that we can write to that will feed requests to the subprocess's
  // input pipe.
  protected PipedOutputStream requestSenderPipe;

  // Pipe that we can read from that will receive responses from the
  // subprocess's output pipe. We'll read from it via BufferedInputStream,
  // so wrap the pipe here.
  protected final PipedInputStream responseReceiverPipe = new PipedInputStream();
  private final BufferedInputStream bufferedInputStream =
      new BufferedInputStream(responseReceiverPipe);

  public static class PrintStreamWithIOException extends PrintStream {
    public PrintStreamWithIOException(OutputStream out, boolean autoFlush, String encoding)
            throws UnsupportedEncodingException {
      super(out, autoFlush, encoding);
    }

    @Override
    public boolean checkError() {
      // Always say that we've got an error.
      return true;
    }
  }

  // Sends a SubprocessRequestPB to the sender pipe, serializing it as
  // appropriate.
  public void sendRequestToPipe(Subprocess.SubprocessRequestPB req) throws IOException {
    requestSenderPipe.write(SubprocessTestUtil.serializeMessage(req));
  }

  // Receives a response from the receiver pipe and deserializes it into a
  // SubprocessResponsePB.
  public Subprocess.SubprocessResponsePB receiveResponse() throws IOException {
    return SubprocessTestUtil.deserializeMessage(bufferedInputStream,
                                                 Subprocess.SubprocessResponsePB.parser());
  }

  // Sets up and returns a SubprocessExecutor with the given error handler and
  // IO error injection behavior. The SubprocessExecutor will do IO to and from
  // 'requestSenderPipe' and 'responseReceiverPipe'.
  public SubprocessExecutor setUpExecutorIO(Function<Throwable, Void> errorHandler,
                                            boolean injectIOError) throws IOException {
    // Initialize the pipe that we'll push requests to; feed it into the
    // executor's input pipe.
    PipedInputStream inputPipe = new PipedInputStream();
    requestSenderPipe = new PipedOutputStream(inputPipe);
    System.setIn(inputPipe);

    // Initialize the pipe that the executor will write to; feed it into the
    // response pipe that we can read from.
    PipedOutputStream outputPipe = new PipedOutputStream(responseReceiverPipe);
    if (injectIOError) {
      System.setOut(new PrintStreamWithIOException(outputPipe, /*autoFlush*/false, "UTF-8"));
    } else {
      System.setOut(new PrintStream(outputPipe, /*autoFlush*/false, "UTF-8"));
    }
    return new SubprocessExecutor(errorHandler);
  }

  /**
   * Constructs a SubprocessRequestPB message of echo request with the
   * given payload and sleep.
   *
   * @param payload the message payload
   * @param sleepMs the amount of time to sleep
   * @return a SubprocessRequestPB message
   */
  public static SubprocessRequestPB createEchoSubprocessRequest(String payload,
                                                                int sleepMs) {
    SubprocessRequestPB.Builder builder = SubprocessRequestPB.newBuilder();
    EchoRequestPB.Builder echoBuilder = EchoRequestPB.newBuilder();
    echoBuilder.setData(payload);
    if (sleepMs > 0) {
      echoBuilder.setSleepMs(sleepMs);
    }
    builder.setRequest(Any.pack(echoBuilder.build()));
    return builder.build();
  }

  /**
   * Constructs a SubprocessRequestPB message of echo request with the
   * given payload.
   *
   * @param payload the message payload
   * @return a SubprocessRequestPB message
   */
  public static SubprocessRequestPB createEchoSubprocessRequest(String payload) {
    return createEchoSubprocessRequest(payload, 0);
  }

  /**
   * Serializes the given message to a byte array.
   *
   * @param message the message
   * @return a serialized message in byte array
   * @throws IOException if an I/O error occurs
   */
  public static byte[] serializeMessage(Message message) throws IOException {
    ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
    MessageIO messageIO = new MessageIO(
        SubprocessConfiguration.MAX_MESSAGE_BYTES_DEFAULT,
        /* in= */null, new BufferedOutputStream(byteOutputStream));
    messageIO.writeMessage(message);
    return byteOutputStream.toByteArray();
  }

  /**
   * Deserializes a message from the byte array.
   *
   * @param bytes the serialized message in byte array
   * @param parser the parser for the message
   * @return a message
   * @throws IOException if an I/O error occurs
   */
  public static <T extends Message> T deserializeMessage(byte[] bytes, Parser<T> parser)
      throws IOException {
    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    return deserializeMessage(new BufferedInputStream(inputStream), parser);
  }

  /**
   * Deserializes a message from the input stream.
   *
   * @param inputStream the input stream from which to deserialize the message
   * @param parser the parser for the message
   * @return a message
   * @throws IOException if an I/O error occurs
   */
  public static <T extends Message> T deserializeMessage(BufferedInputStream inputStream,
                                                         Parser<T> parser) throws IOException {
    MessageIO messageIO = new MessageIO(
        SubprocessConfiguration.MAX_MESSAGE_BYTES_DEFAULT, inputStream, /*out*/null);
    byte[] data = messageIO.readBytes();
    return parser.parseFrom(data);
  }
}
