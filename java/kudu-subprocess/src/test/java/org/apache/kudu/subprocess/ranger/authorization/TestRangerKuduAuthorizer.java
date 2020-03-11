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

package org.apache.kudu.subprocess.ranger.authorization;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.kudu.test.junit.RetryRule;

/**
 * Tests for the Ranger authorizer.
 */
public class TestRangerKuduAuthorizer {

  @Rule
  public RetryRule retryRule = new RetryRule();

  /**
   * Generates a few ranger authorization results and verifies the
   * Ranger authorizer work as expected.
   */
  @Test
  public void testBasicRangerAuthorizer() {
    RangerKuduAuthorizer authz = new RangerKuduAuthorizer();
    authz.plugin = Mockito.mock(RangerBasePlugin.class);
    // We have to mock RangerAccessRequestImpl as it does not implement equals().
    // Mock with a positive authz result.
    RangerAccessRequestImpl mockUpdateRequest = Mockito.mock(RangerAccessRequestImpl.class);
    final RangerAccessResult updateResult = new RangerAccessResult(
        /* policyType= */1, "kudu",
        new RangerServiceDef(), mockUpdateRequest);
    updateResult.setIsAllowed(true);

    // Mock with a negative authz result.
    RangerAccessRequestImpl mockCreateRequest = Mockito.mock(RangerAccessRequestImpl.class);
    final RangerAccessResult createResult = new RangerAccessResult(
        /* policyType= */1, "kudu",
        new RangerServiceDef(), mockCreateRequest);
    createResult.setIsAllowed(false);

    Collection<RangerAccessRequest> requests = new ArrayList<>();
    requests.add(mockUpdateRequest);
    requests.add(mockCreateRequest);
    Collection<RangerAccessResult> results = new ArrayList<>();
    results.add(updateResult);
    results.add(createResult);

    Mockito.when(authz.plugin.isAccessAllowed(requests))
           .thenReturn(results);
    Iterator<RangerAccessResult> actualResultsIter = authz.authorize(requests).iterator();
    assertTrue(actualResultsIter.next().getIsAllowed());
    assertFalse(actualResultsIter.next().getIsAllowed());
  }
}