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

import org.apache.kudu.annotations.InterfaceAudience;
import org.apache.kudu.rpc.RpcHeader;

@InterfaceAudience.Private
public class RpcRemoteException extends NonRecoverableException {
  private static final long serialVersionUID = 1941395686030839186L;
  private final RpcHeader.ErrorStatusPB errPb;

  RpcRemoteException(Status status, RpcHeader.ErrorStatusPB errPb) {
    super(status);
    this.errPb = errPb;
  }

  @InterfaceAudience.Private
  public RpcHeader.ErrorStatusPB getErrPB() {
    return errPb;
  }

}
