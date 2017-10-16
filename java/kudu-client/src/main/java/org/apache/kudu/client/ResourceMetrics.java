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

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.protobuf.Descriptors.FieldDescriptor;

import org.apache.kudu.tserver.Tserver.ResourceMetricsPB;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * A container for scanner resource metrics.
 * <p>
 * This class wraps a mapping from metric name to metric value for server-side
 * metrics associated with a scanner.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ResourceMetrics {
  private Map<String, LongAdder> metrics = new ConcurrentHashMap<>();

  /**
   * Returns a copy of this ResourceMetrics's underlying map of metric name to
   * metric value.
   * @return a map of metric name to metric value
   */
  public Map<String, Long> get() {
      return metrics.entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().sum()));
  }

  /**
   * Returns the value of the metric named by 'name', or 0 if there is no such metric.
   * @param name the name of the metric to get the value for
   * @return the value of the named metric; if the metric is not found, returns 0
   */
  public long getMetric(String name) {
      return metrics.getOrDefault(name, new LongAdder()).sum();
  }

  /**
   * Increment this instance's metric values with those found in 'resourceMetricsPb'.
   * @param resourceMetricsPb resource metrics protobuf object to be used to update this object
   */
  void update(ResourceMetricsPB resourceMetricsPb) {
    Preconditions.checkNotNull(resourceMetricsPb);
    for (Map.Entry<FieldDescriptor, Object> entry : resourceMetricsPb.getAllFields().entrySet()) {
      FieldDescriptor field = entry.getKey();
      if (field.getJavaType() == JavaType.LONG) {
        increment(field.getName(), (Long) entry.getValue());
      }
    }
  }

  /**
   * Increment the metric value by the specific amount.
   * @param name the name of the metric whose value is to be incremented
   * @param amount the amount to increment the value by
   */
  private void increment(String name, long amount) {
    metrics.computeIfAbsent(name, k -> new LongAdder()).add(amount);
  }
}
