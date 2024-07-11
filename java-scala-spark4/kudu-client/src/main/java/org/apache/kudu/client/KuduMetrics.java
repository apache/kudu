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

import static java.util.stream.Collectors.joining;

import java.util.Comparator;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A static utility class to contain constants and methods for working with
 * Kudu Java client metrics.
 *
 * NOTE: The metrics are not considered public API yet. We should not expose
 * micrometer objects/classes through any public interface or method, even
 * when we do make them public.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class KuduMetrics {
  private static final Logger LOG = LoggerFactory.getLogger(KuduMetrics.class);

  public static final String[] EMPTY_TAGS = new String[]{};

  // RPC Metrics
  public static final KuduMetricId RPC_REQUESTS_METRIC =
      new KuduMetricId("rpc.requests", "A count of the sent request RPCs", "requests");
  public static final KuduMetricId RPC_RETRIES_METRIC =
      new KuduMetricId("rpc.retries", "A count of the retried request RPCs", "retries");
  public static final KuduMetricId RPC_RESPONSE_METRIC =
      new KuduMetricId("rpc.responses", "A count of the RPC responses received", "responses");

  // Common Tags
  public static final String CLIENT_ID_TAG = "client.id";
  public static final String SERVER_ID_TAG = "server.id";
  public static final String SERVICE_NAME_TAG = "service.name";
  public static final String METHOD_NAME_TAG = "method.name";

  // TODO(KUDU-3148): After extensive testing consider enabling metrics by default.
  private static boolean enabled = false;
  private static CompositeMeterRegistry registry = createDisabledRegistry();

  /**
   * This class is meant to be used statically.
   */
  private KuduMetrics() {
  }

  /**
   * Enable or disable metric tracking.
   * Disabling the metrics will discard any previously recorded metrics.
   *
   * @param enable If true, metric tracking is enabled.
   */
  public static synchronized void setEnabled(boolean enable) {
    if (enable && !enabled) {
      CompositeMeterRegistry oldRegistry = registry;
      registry = createRegistry();
      enabled = true;
      oldRegistry.close();
    } else if (!enable && enabled) {
      CompositeMeterRegistry oldRegistry = registry;
      registry = createDisabledRegistry();
      enabled = false;
      oldRegistry.close();
    }
  }

  private static CompositeMeterRegistry createRegistry() {
    CompositeMeterRegistry registry = new CompositeMeterRegistry();
    // This is the default naming convention that separates lowercase words
    // with a '.' (dot) character.
    registry.config().namingConvention(NamingConvention.dot);
    // Use the minimal meter registry. Once this is used/useful for more than tests
    // we may want to consider something more exposed such as JMX.
    registry.add(new SimpleMeterRegistry());
    return registry;
  }

  private static CompositeMeterRegistry createDisabledRegistry() {
    CompositeMeterRegistry registry = createRegistry();
    // Add a filter to deny all meters. When a meter is used with this registry,
    // the registry will return a NOOP version of that meter. Anything recorded
    // to it is discarded immediately with minimal overhead.
    registry.config().meterFilter(MeterFilter.deny());
    return registry;
  }

  /**
   * @return the total number of registered metrics.
   */
  static int numMetrics() {
    return registry.getMeters().size();
  }

  /**
   * @param id the metric id
   * @return the number of all the matching metrics.
   */
  static int numMetrics(KuduMetricId id) {
    return numMetrics(id, EMPTY_TAGS);
  }

  /**
   * @param id the metric id
   * @param tags tags must be an even number of arguments representing key/value pairs of tags.
   * @return the sum of all the matching metrics.
   */
  static int numMetrics(KuduMetricId id, String... tags) {
    return Search.in(registry).name(id.name).tags(tags).counters().size();
  }

  /**
   * @param tags tags must be an even number of arguments representing key/value pairs of tags.
   * @return the sum of all the matching metrics.
   */
  static int numMetrics(String... tags) {
    return Search.in(registry).tags(tags).counters().size();
  }

  /**
   * Returns the counter meter for the given metric id and tags.
   * If the meter is already registered, it will lookup the existing meter and
   * return it. Otherwise it will register a new meter and return that.
   *
   * @param id the metric id
   * @param tags tags must be an even number of arguments representing key/value pairs of tags.
   * @return a counter
   */
  static Counter counter(KuduMetricId id, String... tags) {
    return Counter.builder(id.getName())
        .description(id.getDescription())
        .baseUnit(id.getUnit())
        .tags(tags)
        .register(registry);
  }

  /**
   * @param id the metric id
   * @return the sum of all the matching metrics.
   */
  public static double totalCount(KuduMetricId id) {
    return totalCount(id, EMPTY_TAGS);
  }

  /**
   * @param id the metric id
   * @param tags tags must be an even number of arguments representing key/value pairs of tags.
   * @return the sum of all the matching metrics.
   */
  public static double totalCount(KuduMetricId id, String... tags) {
    return Search.in(registry).name(id.name).tags(tags).counters().stream()
        .mapToDouble(Counter::count).sum();
  }

  /**
   * Logs the metric values at the INFO level one metric per line.
   * The output format for each metric is:
   *  <name> {<tag.key>=<tag.value>,...} : <value> <unit>
   */
  static void logMetrics() {
    registry.getMeters().stream()
        // Sort by id to ensure the same order each time.
        .sorted(Comparator.comparing(m -> m.getId().toString()))
        .forEach(m -> {
          // Generate tags string as {k=v,...}
          String tags = m.getId().getTags().stream()
              .sorted()
              .map(t -> t.getKey() + "=" + t.getValue())
              .collect(joining(",", "{", "}"));
          String key = m.getId().getName() + " " + tags;
          String value = "unknown";
          if (m instanceof Counter) {
            value = ((Counter) m).count() + " " + m.getId().getBaseUnit();
          }
          LOG.info(key + " : " + value);
        });
  }

  private static class KuduMetricId {
    private final String name;
    private final String description;
    private final String unit;

    private KuduMetricId(String name, String description, String unit) {
      this.name = name;
      this.description = description;
      this.unit = unit;
    }

    public String getName() {
      return name;
    }

    public String getDescription() {
      return description;
    }

    public String getUnit() {
      return unit;
    }
  }
}
