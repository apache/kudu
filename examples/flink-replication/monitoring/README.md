<!-- Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License. -->

# Monitoring

Prometheus + Grafana stack for the Kudu Replication Demo.

## Start

Prometheus and Grafana start automatically with `make up`.

```
Prometheus : http://localhost:9090
Grafana    : http://localhost:3000  (admin / admin)
```

## Components

| File | Purpose |
|------|---------|
| `prometheus.yml` | Scrape config for Flink, Kudu tservers, and the json_exporter |
| `kudu_tablet_info.yml` | json_exporter config — maps tablet IDs to table names |
| `grafana/datasources/prometheus.yml` | Auto-provisions Prometheus as Grafana data source |
| `grafana/dashboards/dashboards.yml` | Tells Grafana where to find dashboard JSON files |
| `grafana/dashboards/replication.json` | The replication monitoring dashboard |

## Kudu Prometheus integration — version note

This setup was built against **Apache Kudu 1.18.1**.

Kudu's `/metrics_prometheus` endpoint embeds the tablet ID directly in each
metric name:

```
kudu_tablet_<32-char-hex-id>_rows_upserted
```

This is non-standard — Prometheus expects a stable metric name with labels for
dimensions. To work around it, `prometheus.yml` uses `metric_relabel_configs`
to extract the tablet ID into a `tablet_id` label and normalize the metric name:

```
kudu_tablet_rows_upserted{tablet_id="<id>", ...}
```

A second scrape job uses `json_exporter` to read Kudu's JSON metrics endpoint
and build a `kudu_tablet_info` metric that maps `tablet_id` -> `table_name`.
PromQL dashboard queries then join on `tablet_id` to filter by table.

If Kudu's Prometheus integration is normalized in a future release (stable
metric names + label-based dimensions), this relabeling pipeline and the
json_exporter job can be removed.

There is also no service discovery — tserver addresses are hardcoded in
`prometheus.yml`. Once Kudu exposes a discovery endpoint, the static target lists can be
replaced with a dynamic scrape config.
