#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
This script parses a set of metrics logs output from a tablet server,
and outputs a TSV file including some metrics.

This isn't meant to be used standalone as written, but rather as a template
which is edited based on whatever metrics you'd like to extract. The set
of metrics described below are just a starting point to work from.
"""

from collections import defaultdict
import gzip
try:
  import simplejson as json
except:
  import json
import sys

# Even if the input data has very frequent metrics, for graphing purposes we
# may not want to look at such fine-grained data. This constant can be set
# to drop samples which were measured within a given number of seconds
# from the prior sample.
GRANULARITY_SECS = 30

# These metrics will be extracted "as-is" into the TSV.
# The first element of each tuple is the metric name.
# The second is the name that will be used in the TSV header line.
DEFAULT_SIMPLE_METRICS = [
   ("server.generic_current_allocated_bytes", "heap_allocated"),
   ("server.log_block_manager_bytes_under_management", "bytes_on_disk"),
   ("tablet.memrowset_size", "mrs_size"),
   ("server.block_cache_usage", "bc_usage"),
]

# These metrics will be extracted as per-second rates into the TSV.
DEFAULT_RATE_METRICS = [
   ("server.block_manager_total_bytes_read", "bytes_r_per_sec"),
   ("server.block_manager_total_bytes_written", "bytes_w_per_sec"),
   ("server.block_cache_lookups", "bc_lookups_per_sec"),
   ("server.cpu_utime", "cpu_utime"),
   ("server.cpu_stime", "cpu_stime"),
   ("server.involuntary_context_switches", "invol_cs"),
   ("server.voluntary_context_switches", "vol_cs"),
   ("tablet.rows_inserted", "inserts_per_sec"),
   ("tablet.rows_upserted", "upserts_per_sec"),
   ("tablet.leader_memory_pressure_rejections", "mem_rejections"),
]

# These metrics will be extracted as percentile metrics into the TSV.
# Each metric will generate several columns in the output TSV, with
# percentile numbers suffixed to the column name provided here (foo_p95,
# foo_p99, etc)
DEFAULT_HISTOGRAM_METRICS = [
   ("server.op_apply_run_time", "apply_run_time"),
   ("server.handler_latency_kudu_tserver_TabletServerService_Write", "write"),
   ("server.handler_latency_kudu_consensus_ConsensusService_UpdateConsensus", "cons_update"),
   ("server.handler_latency_kudu_consensus_ConsensusService_RequestVote", "vote"),
   ("server.handler_latency_kudu_tserver_TabletCopyService_FetchData", "fetch_data"),
   ("tablet.bloom_lookups_per_op", "bloom_lookups"),
   ("tablet.log_append_latency", "log"),
   ("tablet.op_prepare_run_time", "prep"),
   ("tablet.write_op_duration_client_propagated_consistency", "op_dur")
]

NaN = float('nan')
UNKNOWN_PERCENTILES = dict(p50=0, p95=0, p99=0, p999=0, max=0)


def strip_metric(m):
  """
  Strip the input metric string, returning only the values and counts for the
  metric, as appropriate.
  """
  if 'value' in m:
    return m['value']

  if 'counts' in m:
    return dict(zip(m.get('values', []), m.get('counts')))
  return NaN


def json_to_map(j, parse_metric_keys):
  """
  Parse the JSON structure in the log into a python dictionary of the form:
    { <entity>.<metric name>:String => { <entity id>:String => <values> } }

  Only returns the metrics listed in PARSE_METRIC_KEYS.
  """
  ret = defaultdict(dict)
  for entity in j:
    for m in entity['metrics']:
      entity_id = entity['id']
      metric_key = entity['type'] + "." + m['name']
      if metric_key not in parse_metric_keys:
        continue
        # Add the metric_id to the metrics map.
      ret[metric_key][entity_id] = strip_metric(m)
  return ret

def delta(prev, cur, m):
  """ Compute the delta in metric 'm' between two metric snapshots. """
  if m not in prev or m not in cur:
    return 0

  return cur[m] - prev[m]

def aggregate_metrics(metric_to_eid_to_vals):
  """ Aggregates metrics across entity ids """
  ret = {}
  for metric_name in metric_to_eid_to_vals:
    if metric_name == "ts":
      ret['ts'] = metric_to_eid_to_vals['ts']
      continue

    eid_to_vals = metric_to_eid_to_vals[metric_name]
    # Iterate through all the entities for this metric.
    for eid in eid_to_vals:
      # Aggregate the bucket counts for histogram metrics.
      vals = eid_to_vals[eid]
      if isinstance(vals, dict):
        if metric_name not in ret:
          # Insert the counts if none exist for the metric.
          ret[metric_name] = vals.copy()
        else:
          # Otherwise, add the counts to what's there.
          for val, count in vals.items():
            if val in ret[metric_name]:
              ret[metric_name][val] += count
            else:
              ret[metric_name][val] = count
      else:
        # Sum the values for normal metrics.
        if metric_name in ret:
          ret[metric_name] += vals
        else:
          ret[metric_name] = vals
  return ret

def histogram_stats(aggregated_prev, aggregated_cur, m):
  """
  Compute percentile stats for the metric 'm' in the window between two
  metric snapshots.
  """
  if m not in aggregated_prev or m not in aggregated_cur or not isinstance(aggregated_cur, dict):
    return UNKNOWN_PERCENTILES

  prev = aggregated_prev[m]
  cur = aggregated_cur[m]

  # Determine the total count we should expect between the current and previous
  # snapshots.
  delta_total = sum([val for _, val in cur.items()]) - \
      sum([val for _, val in prev.items()])

  if delta_total == 0:
    return UNKNOWN_PERCENTILES
  res = dict()
  cum_count = 0


  # Iterate over all of the buckets for the current and previous snapshots,
  # summing them up, and assigning percentiles to the bucket as appropriate.
  for cur_val, cur_count in sorted(aggregated_cur[m].items()):
    prev_count = prev.get(cur_val, 0)
    delta_count = cur_count - prev_count
    cum_count += delta_count

    # Determine which percentiles this bucket belongs to.
    percentile = float(cum_count) / delta_total
    if 'p50' not in res and percentile > 0.50:
      res['p50'] = cur_val
    if 'p95' not in res and percentile > 0.95:
      res['p95'] = cur_val
    if 'p99' not in res and percentile > 0.99:
      res['p99'] = cur_val
    if 'p999' not in res and percentile > 0.999:
      res['p999'] = cur_val
    if cum_count == delta_total:
      res['max'] = cur_val
  return res

def cache_hit_ratio(aggregated_prev, aggregated_cur):
  """
  Calculate the cache hit ratio between the two samples.
  If there were no cache hits or misses, this returns NaN.
  """
  delta_hits = delta(aggregated_prev, aggregated_cur, 'server.block_cache_hits_caching')
  delta_misses = delta(aggregated_prev, aggregated_cur, 'server.block_cache_misses_caching')
  if delta_hits + delta_misses > 0:
    cache_ratio = float(delta_hits) / (delta_hits + delta_misses)
  else:
    cache_ratio = NaN
  return cache_ratio

def process(aggregated_prev, aggregated_cur, simple_metrics, rate_metrics, histogram_metrics):
  """ Process a pair of metric snapshots, outputting a line of TSV. """
  if not aggregated_prev:
    aggregated_prev = aggregate_metrics(aggregated_prev)

  delta_ts = aggregated_cur['ts'] - aggregated_prev['ts']
  calc_vals = []
  cache_ratio = cache_hit_ratio(aggregated_prev, aggregated_cur)
  for metric, _ in simple_metrics:
    if metric in aggregated_cur:
      calc_vals.append(aggregated_cur[metric])
    else:
      calc_vals.append(0)

  calc_vals.extend((delta(aggregated_prev, aggregated_cur, metric))/delta_ts \
      for metric, _ in rate_metrics)
  for metric, _ in histogram_metrics:
    stats = histogram_stats(aggregated_prev, aggregated_cur, metric)
    calc_vals.extend([stats['p50'], stats['p95'], stats['p99'], stats['p999'], stats['max']])

  return tuple([(aggregated_cur['ts'] + aggregated_prev['ts'])/2, cache_ratio] + calc_vals)

class MetricsLogParser(object):
  def __init__(self, paths,
               simple_metrics=DEFAULT_SIMPLE_METRICS,
               rate_metrics=DEFAULT_RATE_METRICS,
               histogram_metrics=DEFAULT_HISTOGRAM_METRICS):
    self.paths = paths
    self.simple_metrics = simple_metrics
    self.rate_metrics = rate_metrics
    self.histogram_metrics = histogram_metrics
    # Get the set of metrics we actually want to bother parsing from the log.
    self.parse_metric_keys = set(key for (key, _) in (simple_metrics + rate_metrics + histogram_metrics))
    # The script always reports cache-hit metrics.
    self.parse_metric_keys.add("server.block_cache_hits_caching")
    self.parse_metric_keys.add("server.block_cache_misses_caching")

  def column_names(self):
    simple_headers = [header for _, header in self.simple_metrics + self.rate_metrics]
    for _, header in self.histogram_metrics:
      simple_headers.append(header + "_p50")
      simple_headers.append(header + "_p95")
      simple_headers.append(header + "_p99")
      simple_headers.append(header + "_p999")
      simple_headers.append(header + "_max")
    return tuple(["time", "cache_hit_ratio"] + simple_headers)

  def __iter__(self):
    prev_data = None
    aggregated_prev = None

    for path in sorted(self.paths):
      if path.endswith(".gz"):
        f = gzip.GzipFile(path)
      else:
        f = open(path)
      for line_number, line in enumerate(f, start=1):
        # Only parse out the "metrics" lines.
        try:
          (_, _, log_type, ts, metrics_json) = line.split(" ")
        except ValueError:
          continue
        if log_type != "metrics":
          continue
        ts = float(ts) / 1000000.0
        prev_ts = prev_data['ts'] if prev_data else 0
        # Enforce that the samples come in time-sorted order.
        if ts <= prev_ts:
          raise Exception("timestamps must be in ascending order (%f <= %f at %s:%d)"
                          % (ts, prev_ts, path, line_number))
        if prev_data and ts < prev_ts + GRANULARITY_SECS:
          continue

        # Parse the metrics json into a map of the form:
        #   { metric key => { entity id => metric value } }
        data = json_to_map(json.loads(metrics_json), self.parse_metric_keys)
        data['ts'] = ts
        if prev_data:
          # Copy missing metrics from prev_data.
          for m, prev_eid_to_vals in prev_data.items():
            if m is 'ts':
              continue
            # The metric was missing entirely; copy it over.
            if m not in data:
              data[m] = prev_eid_to_vals
            else:
              # If the metric was missing for a specific entity, copy the metric
              # from the previous snapshot.
              for eid, prev_vals in prev_eid_to_vals.items():
                if eid not in data[m]:
                  data[m][eid] = prev_vals

        aggregated_cur = aggregate_metrics(data)
        if prev_data:
          if not aggregated_prev:
            aggregated_prev = aggregate_metrics(prev_data)
          yield process(aggregated_prev, aggregated_cur,
                        self.simple_metrics, self.rate_metrics, self.histogram_metrics)

        prev_data = data
        aggregated_prev = aggregated_cur

def main(argv):
  parser = MetricsLogParser(argv[1:], DEFAULT_SIMPLE_METRICS, DEFAULT_RATE_METRICS, DEFAULT_HISTOGRAM_METRICS)
  for line in parser:
    print(line)

if __name__ == "__main__":
  main(sys.argv)
