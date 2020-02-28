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

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import org.apache.kudu.subprocess.Subprocess.EchoRequestPB;
import org.apache.kudu.subprocess.Subprocess.SubprocessRequestPB;

/**
 * Utility class of common functions used for testing subprocess
 * message processing.
 */
public class MessageTestUtil {

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
  static <T extends Message> T deserializeMessage(byte[] bytes, Parser<T> parser)
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
