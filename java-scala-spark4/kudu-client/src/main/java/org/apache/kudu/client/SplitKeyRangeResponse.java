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

import org.apache.yetus.audience.InterfaceAudience;

import org.apache.kudu.Common.KeyRangePB;

/**
 * Response type for SplitKeyRangeRequest.
 */
@InterfaceAudience.Private
public class SplitKeyRangeResponse extends KuduRpcResponse {

  private List<KeyRangePB> keyRanges;

  SplitKeyRangeResponse(long elapsedMillis, String tsUUID, List<KeyRangePB> keyRanges) {
    super(elapsedMillis, tsUUID);
    this.keyRanges = keyRanges;
  }

  /**
   * Get the list of primary key ranges key as specified in the request.
   * @return a list of key ranges
   */
  public List<KeyRangePB> getKeyRanges() {
    return keyRanges;
  }
}

