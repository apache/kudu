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

package org.apache.kudu.test;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.client.KuduMetrics;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MetricTestUtils {

  /**
   * @return the total sum of "rpc.request" metrics
   */
  public static long totalRequestCount() {
    return (long) KuduMetrics.totalCount(KuduMetrics.RPC_REQUESTS_METRIC);
  }

  /**
   * Validates that the count change in the matching "rpc.request" metrics matches
   * the expectedCount when the callable f is called.
   *
   * @param expectedCount the expected count
   * @param clientId the clientId to filter on
   * @param f the callable to call and validate
   * @param <T> the return type
   * @return the return value from f
   * @throws Exception when f throws an exception
   */
  public static <T> T validateRequestCount(int expectedCount, String clientId,
                                           Callable<T> f) throws Exception {
    return validateRequestCount(expectedCount, clientId, Collections.emptyList(), f);
  }

  /**
   * Validates that the count change in the matching "rpc.request" metrics matches
   * the expectedCount when the callable f is called.
   *
   * @param expectedCount the expected count
   * @param clientId the clientId to filter on
   * @param rpcMethodName the rpc method name to filter on
   * @param f the callable to call and validate
   * @param <T> the return type
   * @return the return value from f
   * @throws Exception when f throws an exception
   */
  public static <T> T validateRequestCount(int expectedCount, String clientId,
                                           String rpcMethodName, Callable<T> f) throws Exception {
    return validateRequestCount(expectedCount, clientId,
        Collections.singletonList(rpcMethodName), f);
  }

  /**
   * Validates that the count change in the matching "rpc.request" metrics matches
   * the expectedCount when the callable f is called.
   *
   * @param expectedCount the expected count
   * @param clientId the clientId to filter on
   * @param rpcMethodNames the rpc method names to filter on
   * @param f the callable to call and validate
   * @param <T> the return type
   * @return the return value from f
   * @throws Exception when f throws an exception
   */
  public static <T> T validateRequestCount(int expectedCount, String clientId,
                                           List<String> rpcMethodNames, Callable<T> f)
      throws Exception {
    Map<String, Long> beforeMap = new HashMap<>();
    for (String rpcMethodName : rpcMethodNames) {
      beforeMap.put(rpcMethodName,
          (long) KuduMetrics.totalCount(KuduMetrics.RPC_REQUESTS_METRIC,
              KuduMetrics.CLIENT_ID_TAG, clientId, KuduMetrics.METHOD_NAME_TAG, rpcMethodName));
    }
    T t = f.call();
    long count = 0;
    for (Map.Entry<String, Long> entry : beforeMap.entrySet()) {
      String rpcMethodName = entry.getKey();
      long before = entry.getValue();
      long after = (long) KuduMetrics.totalCount(KuduMetrics.RPC_REQUESTS_METRIC,
          KuduMetrics.CLIENT_ID_TAG, clientId, KuduMetrics.METHOD_NAME_TAG, rpcMethodName);
      count += after - before;
    }
    assertEquals(expectedCount, count);
    return t;
  }

}
