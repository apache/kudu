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
 * Exception for notifying of an invalid authorization token. In most use cases
 * in the Kudu Java client code, 'invalid authz token' means 'expired authz
 * token'. Receiving this exception means the authorization token used to make a
 * request is no longer valid and a new one is needed to make requests that
 * access data.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class InvalidAuthzTokenException extends RecoverableException {
  /**
   * @param status status object containing the reason for the exception trace
   */
  InvalidAuthzTokenException(Status status) {
    super(status);
  }
}
