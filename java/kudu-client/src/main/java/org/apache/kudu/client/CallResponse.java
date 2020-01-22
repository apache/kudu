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

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DefaultByteBufHolder;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.kudu.rpc.RpcHeader;
import org.apache.kudu.util.Slice;

/**
 * This class handles information received from an RPC response, providing
 * access to sidecars and decoded protobufs from the message.
 */
@InterfaceAudience.Private
final class CallResponse extends DefaultByteBufHolder {
  private final ByteBuf buf;
  private final RpcHeader.ResponseHeader header;
  private final int totalResponseSize;

  // Non-header main message slice is generated upon request and cached.
  private Slice message = null;

  /**
   * Performs some sanity checks on the sizes recorded in the packet
   * referred to by {@code buf}. Assumes that {@code buf} has not been
   * read from yet, and will only be accessed by this class.
   *
   * Afterwards, this constructs the RpcHeader from the buffer.
   * @param buf Byte buffer which call response reads from.
   * @throws IndexOutOfBoundsException if any length prefix inside the
   * response points outside the bounds of the buffer.
   */
  CallResponse(final ByteBuf buf) {
    super(buf);
    this.buf = buf;

    this.totalResponseSize = buf.readableBytes();
    final int headerSize = Bytes.readVarInt32(buf);
    // No needs to bounds-check the size since 'buf' is already sized appropriately.
    final Slice headerSlice = nextBytes(buf, headerSize);
    RpcHeader.ResponseHeader.Builder builder = RpcHeader.ResponseHeader.newBuilder();
    KuduRpc.readProtobuf(headerSlice, builder);
    this.header = builder.build();
  }

  /**
   * @return the parsed header
   */
  public RpcHeader.ResponseHeader getHeader() {
    return this.header;
  }

  /**
   * @return the total response size
   */
  public int getTotalResponseSize() {
    return this.totalResponseSize;
  }

  /**
   * @return A slice pointing to the section of the packet reserved for the main
   * protobuf message.
   * @throws IllegalStateException If the offset for the main protobuf message
   * is not valid.
   */
  public Slice getPBMessage() {
    cacheMessage();
    final int mainLength = this.header.getSidecarOffsetsCount() == 0 ?
        this.message.length() : this.header.getSidecarOffsets(0);
    if (mainLength < 0 || mainLength > this.message.length()) {
      throw new IllegalStateException("Main protobuf message invalid. " +
          "Length is " + mainLength + " while the size of the message " +
          "excluding the header is " + this.message.length());
    }
    return subslice(this.message, 0, mainLength);
  }

  /**
   * @param sidecar The index of the sidecar to retrieve.
   * @return A slice pointing to the desired sidecar.
   * @throws IllegalStateException If the sidecar offsets specified in the
   * header response PB are not valid offsets for the array.
   * @throws IllegalArgumentException If the sidecar with the specified index
   * does not exist.
   */
  public Slice getSidecar(int sidecar) {
    cacheMessage();

    List<Integer> sidecarList = this.header.getSidecarOffsetsList();
    if (sidecar < 0 || sidecar > sidecarList.size()) {
      throw new IllegalArgumentException("Sidecar " + sidecar +
          " not valid, response has " + sidecarList.size() + " sidecars");
    }

    final int prevOffset = sidecarList.get(sidecar);
    final int nextOffset = sidecar + 1 == sidecarList.size() ?
        this.message.length() : sidecarList.get(sidecar + 1);
    final int length = nextOffset - prevOffset;

    if (prevOffset < 0 || length < 0 || prevOffset + length > this.message.length()) {
      throw new IllegalStateException("Sidecar " + sidecar + " invalid " +
          "(offset = " + prevOffset + ", length = " + length + "). The size " +
          "of the message " + "excluding the header is " + this.message.length());
    }

    return subslice(this.message, prevOffset, length);
  }

  // Reads the message after the header if not read yet
  private void cacheMessage() {
    if (this.message != null) {
      return;
    }
    final int length = Bytes.readVarInt32(buf);
    this.message = nextBytes(buf, length);
  }

  // Accounts for a parent slice's offset when making a new one with relative offsets.
  private static Slice subslice(Slice parent, int offset, int length) {
    return new Slice(parent.getRawArray(), parent.getRawOffset() + offset, length);
  }

  // After checking the length, generates a slice for the next 'length'
  // bytes of 'buf'. Advances the buffer's read index by 'length' bytes.
  private static Slice nextBytes(final ByteBuf buf, final int length) {
    byte[] payload;
    int offset;
    if (buf.hasArray()) {  // Zero copy.
      payload = buf.array();
      offset = buf.arrayOffset() + buf.readerIndex();
      buf.skipBytes(length);
    } else {  // We have to copy the entire payload out of the buffer :(
      payload = new byte[length];
      buf.readBytes(payload);
      offset = 0;
    }
    return new Slice(payload, offset, length);
  }

  /**
   * Netty decoder which receives incoming frames (ByteBuf)
   * and constructs CallResponse objects.
   */
  static class Decoder extends ByteToMessageDecoder {

    Decoder() {
      // Only one message is decoded on each read.
      setSingleDecode(true);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) {
      // Increase the reference count because CallResponse holds onto and uses the ByteBuf.
      // https://netty.io/wiki/reference-counted-objects.html
      msg.retain();
      out.add(new CallResponse(msg));
    }
  }

}
