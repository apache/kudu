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

package org.apache.kudu.subprocess.ranger;

import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.ranger.Ranger;
import org.apache.kudu.ranger.Ranger.RangerRequestListPB;
import org.apache.kudu.ranger.Ranger.RangerResponseListPB;
import org.apache.kudu.ranger.Ranger.RangerResponsePB;
import org.apache.kudu.subprocess.ProtocolHandler;
import org.apache.kudu.subprocess.ranger.authorization.RangerKuduAuthorizer;

/**
 * Class that sends requests to Ranger and gets authorization decision
 * (e.g. allow or deny) as a response.
 */
@InterfaceAudience.Private
class RangerProtocolHandler extends ProtocolHandler<RangerRequestListPB,
                                                    RangerResponseListPB> {
  // The Ranger Kudu authorizer plugin. This field is not final
  // as it is used in the mock test.
  @InterfaceAudience.LimitedPrivate("Test")
  static RangerKuduAuthorizer authz = new RangerKuduAuthorizer();

  RangerProtocolHandler(String servicePrincipal, String keytab) {
    authz.init(servicePrincipal, keytab);
  }

  @Override
  protected RangerResponseListPB executeRequest(RangerRequestListPB requests) {
    RangerResponseListPB.Builder responses = authz.authorize(requests);
    if (requests.hasControlRequest()) {
      if (requests.getControlRequest().getRefreshPolicies()) {
        authz.refreshPolicies();
        responses.setControlResponse(Ranger.RangerControlResponsePB.newBuilder()
            .setSuccess(true).build());
      }
    }
    return responses.build();
  }

  @Override
  protected Class<RangerRequestListPB> getRequestClass() {
    return RangerRequestListPB.class;
  }
}
