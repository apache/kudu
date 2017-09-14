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
Uncomment the ones you are interested in, or add new ones.
"""

from collections import Counter
import gzip
import heapq
import itertools
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
SIMPLE_METRICS = [
  #  ("server.generic_current_allocated_bytes", "heap_allocated"),
  #  ("server.log_block_manager_bytes_under_management", "bytes_on_disk"),
  #  ("tablet.memrowset_size", "mrs_size"),
  #  ("server.block_cache_usage", "bc_usage"),
]

# These metrics will be extracted as per-second rates into the TSV.
RATE_METRICS = [
  #  ("server.block_manager_total_bytes_read", "bytes_r_per_sec"),
  #  ("server.block_manager_total_bytes_written", "bytes_w_per_sec"),
  #  ("server.block_cache_lookups", "bc_lookups_per_sec"),
  #  ("server.cpu_utime", "cpu_utime"),
  #  ("server.cpu_stime", "cpu_stime"),
  #  ("server.involuntary_context_switches", "invol_cs"),
  #  ("server.voluntary_context_switches", "vol_cs"),
  #  ("tablet.rows_inserted", "inserts_per_sec"),
  #  ("tablet.rows_upserted", "upserts_per_sec"),
  #  ("server.tcmalloc_contention_time", "tcmalloc_contention_time")
]

# These metrics will be extracted as percentile metrics into the TSV.
# Each metric will generate several columns in the output TSV, with
# percentile numbers suffixed to the column name provided here (foo_p95,
# foo_p99, etc)
HISTOGRAM_METRICS = [
  #  ("server.op_apply_run_time", "apply_run_time"),
  #  ("server.handler_latency_kudu_tserver_TabletServerService_Write", "write"),
  #  ("server.handler_latency_kudu_consensus_ConsensusService_UpdateConsensus", "cons_update"),
  #  ("server.handler_latency_kudu_consensus_ConsensusService_RequestVote", "vote"),
  #  ("server.handler_latency_kudu_tserver_TabletCopyService_FetchData", "fetch_data"),
  #  ("tablet.bloom_lookups_per_op", "bloom_lookups"),
  #  ("tablet.log_append_latency", "log"),
  #  ("tablet.op_prepare_run_time", "prep"),
  #  ("tablet.write_op_duration_client_propagated_consistency", "op_dur")
]

# Get the set of metrics we actuall want to bother parsing from the log.
PARSE_METRIC_KEYS = set(key for (key, _) in (SIMPLE_METRICS + RATE_METRICS + HISTOGRAM_METRICS))

# The script always reports cache-hit metrics.
PARSE_METRIC_KEYS.add("server.block_cache_hits_caching")
PARSE_METRIC_KEYS.add("server.block_cache_misses_caching")

NaN = float('nan')
UNKNOWN_PERCENTILES = dict(p50=NaN, p95=NaN, p99=NaN, p999=NaN, max=NaN)

def merge_delta(m, delta):
  """
  Update (in-place) the metrics entry 'm' by merging another entry 'delta'.

  Counts and sums are simply added.
  Histograms require more complex processing: the 'values' array needs to be
  merged and the then the delta's counts added to the corresponding buckets.
  """

  for k, v in delta.iteritems():
    if k in ('name', 'values', 'counts', 'min', 'max', 'mean'):
      continue
    m[k] += v

  # Merge counts.
  if 'counts' in delta:
    m_zip = itertools.izip(m.get('values', []), m.get('counts', []))
    d_zip = itertools.izip(delta.get('values', []), delta.get('counts', []))
    new_values = []
    new_counts = []
    i = 0
    for value, counts in itertools.groupby(heapq.merge(m_zip, d_zip), lambda x: x[0]):
      new_values[i] = value
      new_counts[i] = sum(c for v, c in counts)
      i += 1
    m['counts'] = new_counts
    m['values'] = new_values


def json_to_map(j):
  """
  Parse the JSON structure in the log into a python dictionary
  keyed by <entity>.<metric name>.

  The entity ID is currently ignored. If there is more than one
  entity of a given type (eg tablets), the metrics will be summed
  together using 'merge_delta' above.
  """
  ret = {}
  for entity in j:
    for m in entity['metrics']:
      key = entity['type'] + "." + m['name']
      if key not in PARSE_METRIC_KEYS:
        continue
      if key in ret:
        merge_delta(ret[key], m)
      else:
        ret[key] = m
  return ret

def delta(prev, cur, m):
  """ Compute the delta in metric 'm' between two metric snapshots. """
  if m not in prev or m not in cur:
    return 0
  return cur[m]['value'] - prev[m]['value']

def histogram_stats(prev, cur, m):
  """
  Compute percentile stats for the metric 'm' in the window between two
  metric snapshots.
  """
  if m not in prev or m not in cur or 'values' not in cur[m]:
    return UNKNOWN_PERCENTILES
  prev = prev[m]
  cur = cur[m]

  p_dict = dict(zip(prev.get('values', []),
                    prev.get('counts', [])))
  c_zip = zip(cur.get('values', []),
              cur.get('counts', []))
  delta_total = cur['total_count'] - prev['total_count']
  if delta_total == 0:
    return UNKNOWN_PERCENTILES
  res = dict()
  cum_count = 0
  for cur_val, cur_count in c_zip:
    prev_count = p_dict.get(cur_val, 0)
    delta_count = cur_count - prev_count
    cum_count += delta_count
    percentile = float(cum_count) / delta_total
    if 'p50' not in res and percentile > 0.50:
      res['p50'] = cur_val
    if 'p95' not in res and percentile > 0.95:
      res['p95'] = cur_val
    if 'p99' not in res and percentile > 0.99:
      res['p99'] = cur_val
    if 'p999' not in res and percentile > 0.999:
      res['p999'] = cur_val
    if cum_count == delta_total and delta_count != 0:
      res['max'] = cur_val
  return res

def cache_hit_ratio(prev, cur):
  """
  Calculate the cache hit ratio between the two samples.
  If there were no cache hits or misses, this returns NaN.
  """
  delta_hits = delta(prev, cur, 'server.block_cache_hits_caching')
  delta_misses = delta(prev, cur, 'server.block_cache_misses_caching')
  if delta_hits + delta_misses > 0:
    cache_ratio = float(delta_hits) / (delta_hits + delta_misses)
  else:
    cache_ratio = NaN
  return cache_ratio

def process(prev, cur):
  """ Process a pair of metric snapshots, outputting a line of TSV. """
  delta_ts = cur['ts'] - prev['ts']
  cache_ratio = cache_hit_ratio(prev, cur)
  calc_vals = []
  for metric, _ in SIMPLE_METRICS:
    if metric in cur:
      calc_vals.append(cur[metric]['value'])
    else:
      calc_vals.append(NaN)
  calc_vals.extend(delta(prev, cur, metric)/delta_ts for (metric, _) in RATE_METRICS)
  for metric, _ in HISTOGRAM_METRICS:
    stats = histogram_stats(prev, cur, metric)
    calc_vals.extend([stats['p50'], stats['p95'], stats['p99'], stats['p999'], stats['max']])

  print (cur['ts'] + prev['ts'])/2, \
        cache_ratio, \
        " ".join(str(x) for x in calc_vals)

def main(argv):
  prev_data = None

  simple_headers = [header for _, header in SIMPLE_METRICS + RATE_METRICS]
  for _, header in HISTOGRAM_METRICS:
    simple_headers.append(header + "_p50")
    simple_headers.append(header + "_p95")
    simple_headers.append(header + "_p99")
    simple_headers.append(header + "_p999")
    simple_headers.append(header + "_max")

  print "time cache_hit_ratio", " ".join(simple_headers)

  for path in sorted(argv[1:]):
    if path.endswith(".gz"):
      f = gzip.GzipFile(path)
    else:
      f = file(path)
    for line_number, line in enumerate(f, start=1):
      (_, ts, metrics_json) = line.split(" ", 2)
      ts = float(ts) / 1000000.0
      prev_ts = prev_data['ts'] if prev_data else 0
      # Enforce that the samples come in time-sorted order.
      if ts <= prev_ts:
        raise Exception("timestamps must be in ascending order (%f <= %f at %s:%d)"
                        % (ts, prev_ts, path, line_number))
      if prev_data and ts < prev_ts + GRANULARITY_SECS:
        continue
      data = json_to_map(json.loads(metrics_json))
      data['ts'] = ts
      if prev_data:
        process(prev_data, data)
      prev_data = data

if __name__ == "__main__":
  main(sys.argv)
