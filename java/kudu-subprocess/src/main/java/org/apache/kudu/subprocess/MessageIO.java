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
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Bytes;
import com.google.protobuf.Message;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Util class for reading and writing protobuf messages.
 */
@InterfaceAudience.Private
public class MessageIO {
  private final int maxMessageBytes;
  private final BufferedInputStream in;
  private final BufferedOutputStream out;

  public MessageIO(int maxMessageBytes,
                   BufferedInputStream in,
                   BufferedOutputStream out) {
    this.maxMessageBytes = maxMessageBytes;
    this.in = in;
    this.out = out;
  }

  /**
   * Reads a protobuf message, if any, from the underlying buffered input
   * stream. The read is blocking and not atomic (partial reads can occur
   * if exceptions occur). As such, users should ensure this is not called
   * from multiple threads concurrently.
   *
   * @return the message in a byte array.
   * @throws EOFException if the end of the stream has been reached
   * @throws IOException if this input stream has been closed, an I/O
   *                     error occurs, or fail to read the message
   *                     properly
   */
  @VisibleForTesting
  byte[] readBytes() throws EOFException, IOException {
    Preconditions.checkNotNull(in);
    // Read four bytes of the message to get the size of the body.
    byte[] sizeBytes = new byte[Integer.BYTES];
    doRead(sizeBytes, Integer.BYTES);
    int size = bytesToInt(sizeBytes);
    if (size > maxMessageBytes) {
      throw new IOException(
          String.format("message size (%d) exceeds maximum message size (%d)",
                        size, maxMessageBytes));
    }
    // Read the body based on the size.
    byte[] dataBytes = new byte[size];
    doRead(dataBytes, size);
    return dataBytes;
  }

  /**
   * Reads <code>size</code> bytes of data from the underlying buffered input
   * stream into the specified byte array, starting at the offset <code>0</code>.
   * If it fails to read the specified size, <code>IOException</code> is thrown.
   *
   * @return the message in byte array.
   * @throws EOFException if the end of the stream has been reached
   * @throws IOException if this input stream has been closed, an I/O
   *                     error occurs, or fail to read the specified size
   */
  private void doRead(byte bytes[], int size) throws EOFException, IOException {
    Preconditions.checkNotNull(bytes);
    int read = in.read(bytes, 0, size);
    if (read == -1) {
      throw new EOFException("the end of the stream has been reached");
    } else if (read != size)
      throw new IOException(
          String.format("unable to receive message, expected (%d) bytes " +
                        "but read (%d) bytes", size, read));
  }

  /**
   * Writes a protobuf message to the buffered output stream. Since we flush
   * after writing each message, with the underlying buffer size being the
   * maximum bytes of a message, the write is atomic. That is if any exceptions
   * occur, no partial message will be written to the underlying output stream.
   *
   * @param message the protobuf message
   * @throws IOException if an I/O error occurs
   */
  @VisibleForTesting
  void writeMessage(Message message) throws IOException {
    Preconditions.checkNotNull(out);
    byte[] size = intToBytes(message.getSerializedSize());
    byte[] body = message.toByteArray();
    synchronized (out) {
      out.write(Bytes.concat(size, body));
      // Always do a flush after write to ensure no partial message is written.
      out.flush();
    }
  }

  /**
   * Converts a four-byte array in big endian order to a 32-bit integer.
   * @param data a four-byte array in big endian order
   * @return a 32-bit integer
   */
  static int bytesToInt(byte[] data) {
    return ByteBuffer.wrap(data)
                     .order(ByteOrder.BIG_ENDIAN)
                     .getInt();
  }

  /**
   * Converts a 32-bit integer to a four bytes array in big endian order.
   * @param value a 32-bit integer
   * @return a four bytes array in big endian order
   */
  @VisibleForTesting
  public static byte[] intToBytes(int value) {
    return ByteBuffer.allocate(Integer.BYTES)
                     .order(ByteOrder.BIG_ENDIAN)
                     .putInt(value)
                     .array();
  }
}
