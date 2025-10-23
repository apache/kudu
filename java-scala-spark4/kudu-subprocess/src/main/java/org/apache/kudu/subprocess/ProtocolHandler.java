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

import com.google.common.base.Preconditions;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.kudu.subprocess.Subprocess.SubprocessRequestPB;
import org.apache.kudu.subprocess.Subprocess.SubprocessResponsePB;

/**
 * Protocol that represents how to handle a protobuf message.
 *
 * @param <RequestT> The request protobuf message
 * @param <ResponseT> The response protobuf message
 */
@InterfaceAudience.Private
public abstract class ProtocolHandler<RequestT extends Message,
                                      ResponseT extends Message> {

  /**
   * Unpacks the SubprocessRequestPB message according to the expected request
   * type and returns a SubprocessResponsePB builder with the results.
   *
   * @param request a SubprocessRequestPB message
   * @return a SubprocessResponsePB.Builder
   * @throws InvalidProtocolBufferException if the protocol message being parsed is invalid
   */
  SubprocessResponsePB.Builder unpackAndExecuteRequest(SubprocessRequestPB request)
      throws InvalidProtocolBufferException {
    Preconditions.checkNotNull(request);
    SubprocessResponsePB.Builder builder = SubprocessResponsePB.newBuilder();
    builder.setId(request.getId());
    Class<RequestT> requestType = getRequestClass();
    ResponseT resp = executeRequest(request.getRequest().unpack(requestType));
    builder.setResponse(Any.pack(resp));
    return builder;
  }

  /**
   * Executes the request and creates a response.
   *
   * @param request the request message
   * @return a response
   */
  protected abstract ResponseT executeRequest(RequestT request);

  /**
   * Gets the class instance of request message.
   *
   * @return the request class instance
   */
  protected abstract Class<RequestT> getRequestClass();
}
