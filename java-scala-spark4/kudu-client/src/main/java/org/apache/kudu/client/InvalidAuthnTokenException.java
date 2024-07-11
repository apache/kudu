/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.kudu.client;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Exception for notifying on invalid authn token. In most use cases in the Kudu Java client code,
 * 'invalid authn token' means 'expired authn token'. Receiving this exception means the current
 * authentication token is no longer valid and a new one is needed to establish connections to
 * the Kudu servers for sending RPCs.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class InvalidAuthnTokenException extends RecoverableException {
  /**
   * @param status status object containing the reason for the exception trace
   */
  InvalidAuthnTokenException(Status status) {
    super(status);
  }
}
