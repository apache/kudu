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

import java.util.TreeSet;
import javax.annotation.concurrent.GuardedBy;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * This is the same class as src/kudu/rpc/request_tracker.h.
 */
@InterfaceAudience.Private
public class RequestTracker {
  private final Object lock = new Object();

  @GuardedBy("lock")
  private long nextSeqNo = 1;
  @GuardedBy("lock")
  private final TreeSet<Long> incompleteRpcs = new TreeSet<>();

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
    synchronized (lock) {
      long seq = nextSeqNo++;
      incompleteRpcs.add(seq);
      return seq;
    }
  }

  /**
   * Returns the oldest sequence number that wasn't marked as completed. If there is no incomplete
   * RPC then {@link RequestTracker#NO_SEQ_NO} is returned.
   * @return the first incomplete sequence number
   */
  public long firstIncomplete() {
    synchronized (lock) {
      if (incompleteRpcs.isEmpty()) {
        return NO_SEQ_NO;
      }
      return incompleteRpcs.first();
    }
  }

  /**
   * Marks the given sequence id as complete. The provided sequence ID must be a valid
   * number that was previously returned by {@link #newSeqNo()}. It is illegal to call
   * this method twice with the same sequence number.
   * @param sequenceId the sequence id to mark as complete
   */
  public void rpcCompleted(long sequenceId) {
    assert sequenceId != NO_SEQ_NO;
    synchronized (lock) {
      boolean removed = incompleteRpcs.remove(sequenceId);
      assert (removed) : "Could not remove seqid " + sequenceId + " from request tracker";
    }
  }

  public String getClientId() {
    return clientId;
  }
}
