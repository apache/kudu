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

import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * This is the same class as src/kudu/rpc/request_tracker.h.
 */
@InterfaceAudience.Private
public class RequestTracker {
  private final AtomicLong sequenceIdTracker = new AtomicLong();
  private final Queue<Long> incompleteRpcs = new PriorityBlockingQueue<>();

  static final long NO_SEQ_NO = -1;

  private final String clientId;

  /**
   * Create a new request tracker for the given client id.
   * @param clientId identifier for the client this tracker belongs to
   */
  public RequestTracker(String clientId) {
    this.clientId = clientId;
  }

  /**
   * Generates a new sequence number and tracks it.
   * @return a new sequence number
   */
  public long newSeqNo() {
    Long next = sequenceIdTracker.incrementAndGet();
    incompleteRpcs.add(next);
    return next;
  }

  /**
   * Returns the oldest sequence number that wasn't marked as completed. If there is no incomplete
   * RPC then {@link RequestTracker#NO_SEQ_NO} is returned.
   * @return the first incomplete sequence number
   */
  public long firstIncomplete() {
    Long peek = incompleteRpcs.peek();
    return peek == null ? NO_SEQ_NO : peek;
  }

  /**
   * Marks the given sequence id as complete. This operation is idempotent.
   * @param sequenceId the sequence id to mark as complete
   */
  public void rpcCompleted(long sequenceId) {
    incompleteRpcs.remove(sequenceId);
  }

  public String getClientId() {
    return clientId;
  }
}
