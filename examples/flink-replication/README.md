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

# Kudu Replication Demo Environment

A Docker Compose–based end-to-end demo for near real-time Kudu-to-Kudu
replication using Apache Flink.

Two lean Kudu clusters (source + sink) run locally in Docker. A Flink job
continuously reads from the source cluster via diff scans and upserts changed
rows into the sink cluster. An ingest simulator continuously applies INSERT,
UPDATE, DELETE and UPSERT operations to the source, making all operation types
observable through replication.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Docker network: kudu-demo                     │
│                                                                      │
│  ┌──────────────────────┐       ┌─────────────────────────────────┐  │
│  │   Source Cluster     │ diff  │         Flink Cluster           │  │
│  │  src-kudu-master-1   │◄──────│  KuduSource ──► replication     │  │
│  │  src-kudu-tserver-1  │  scan │                    job          │  │
│  │  src-kudu-tserver-2  │       │                     │           │  │
│  └────────▲─────────────┘       │                  KuduSink       │  │
│           │ row ops             └─────────────────────┬───────────┘  │
│  ┌────────┴──────────────┐                            │              │
│  │  kudu-ingest          │                            ▼              │
│  │  (kudu-python image)  │      ┌──────────────────────────┐         │
│  └───────────────────────┘      │   Sink Cluster           │         │
│                                 │  snk-kudu-master-1       │         │
│                                 │  snk-kudu-tserver-1      │         │
│                                 │  snk-kudu-tserver-2      │         │
│                                 └──────────────────────────┘         │
└──────────────────────────────────────────────────────────────────────┘
```

### Port map (host -> container)

| Service              | RPC (host) | Web UI (host)         |
|----------------------|-----------:|-----------------------|
| Source master        | 7051       | http://localhost:8051 |
| Source tserver 1     | 7050       | http://localhost:8050 |
| Source tserver 2     | 7150       | http://localhost:8150 |
| Sink master          | 17051      | http://localhost:18051|
| Sink tserver 1       | 17050      | http://localhost:18050|
| Sink tserver 2       | 17150      | http://localhost:18150|
| Flink JobManager     | —          | http://localhost:8081 |

---

## Prerequisites

| Tool               | Minimum version | Notes                                      |
|--------------------|----------------:|--------------------------------------------|
| Docker             | 24.x            | Docker Desktop or Docker Engine            |
| Docker Compose     | v2              | `docker compose` (not `docker-compose`)    |
| Java               | 17              | Only needed for `make build-jar`           |

---

## Quick start

### 1. Build the Flink replication JAR

This step compiles the `java/kudu-replication` module and places the fat JAR
into `examples/flink-replication/flink/jars/` where it is picked up by the Flink containers.

The build requires `flatc` from Kudu's thirdparty. A full thirdparty build is
not necessary — only download and build the flatbuffers component:

```bash
./thirdparty/download-thirdparty.sh
./thirdparty/build-thirdparty.sh flatbuffers
```

Then build the JAR:

```bash
cd examples/flink-replication/
make build-jar
```

> Skip this step only if you already have the JAR from a previous build.

### 2. Start the infrastructure

```bash
make up
```

This starts both Kudu clusters, the Flink cluster, and the monitoring stack
(Prometheus, Grafana, json-exporter), then polls until all services are healthy
and prints a readiness summary.  The ingest simulator is **not** started
automatically; use `make start-ingest` in the next step.

### 3. Start the ingest simulator

```bash
make start-ingest
```

Starts `ingest/ingest.py` inside the `apache/kudu:kudu-python-1.18.1-ubuntu`
container — no host Python setup required.  Creates `demo_table` with schema
(`id INT64`, `event_ts UNIXTIME_MICROS`, `payload STRING`) and continuously
applies a mix of INSERT (40%), UPSERT (25%), UPDATE (20%), and DELETE (15%)
operations at 5 ops/s (configurable via `.env`).

```bash
make logs-ingest    # tail output
make stop-ingest    # pause ingestion (simulates source going offline)
make start-ingest   # resume
```

### 4. Submit the Flink replication job

```bash
make submit-job
```

Submits the replication job to the Flink JobManager with:
- Source: `src-kudu-master-1:7051`
- Sink: `snk-kudu-master-1:7051`
- Table: `demo_table`
- `--job.createTable=true` — mirrors the source schema to the sink
- Discovery interval: 10 s (configurable via `DISCOVERY_INTERVAL_SECONDS` in `.env`)
- Checkpoint interval: 5 s (configurable via `CHECKPOINT_INTERVAL_MS` in `.env`)

Monitor the job at http://localhost:8081.

### 5. Verify replication

```bash
make verify
```

Compares approximate row counts on source and sink.  The sink lags the
source by up to one discovery interval (~10 s by default).

---

## All Makefile targets

```
make build-jar        Build the Flink replication shadow JAR
make up               Start all containers and wait for readiness
make down             Stop all services
make clean            Stop services and remove all Docker volumes

make start-ingest     Start the ingest container (INSERT/UPDATE/DELETE mix)
make stop-ingest      Pause ingestion (simulate source going offline)
make submit-job       Submit (or resume from savepoint) the Flink job
make stop-job         Stop the job and write a savepoint
make verify           Compare source vs sink row counts

make logs             Tail all container logs
make logs-ingest      Tail ingest container logs
make logs-flink       Tail Flink container logs
make ui               Print all web UI URLs (Kudu, Flink, Prometheus, Grafana)
```

---

## Ingest simulator notes

`ingest/ingest.py` runs inside the `apache/kudu:kudu-python-1.18.1-ubuntu`
container — no host Python setup needed.

It creates `demo_table` with schema (`id INT64`, `event_ts UNIXTIME_MICROS`,
`payload STRING`) and applies a continuous mix of operations:

| Operation | Default weight | What it does |
|-----------|---------------:|---|
| INSERT    | 0.40           | Adds a new row to an in-memory ID pool |
| UPSERT    | 0.25           | Inserts or overwrites a row |
| UPDATE    | 0.20           | Rewrites payload + timestamp of a random existing row |
| DELETE    | 0.15           | Removes a random row from the table and pool |

The pool is capped at 2000 IDs; once full, only updates and deletes are issued.

```bash
make start-ingest   # start
make stop-ingest    # pause (simulate source going offline — container kept)
make start-ingest   # resume
make logs-ingest    # tail output
```

All rates and the interval are configurable in `.env` or via shell env vars:

```dotenv
INGEST_INTERVAL=0.2   # seconds between operations (5 ops/sec)
INSERT_RATE=0.4       # relative weight (ratios matter, not absolute values)
UPSERT_RATE=0.25
UPDATE_RATE=0.2
DELETE_RATE=0.15
```

---

## Configuration

Edit `examples/flink-replication/.env` to change defaults without modifying source files:

```dotenv
KUDU_VERSION=1.18.1-ubuntu          # apache/kudu image tag
FLINK_VERSION=1.19.2-java17         # Flink image tag
DEMO_TABLE=demo_table               # replication table name
DISCOVERY_INTERVAL_SECONDS=10       # KuduSource diff-scan period
CHECKPOINT_INTERVAL_MS=5000         # Flink checkpoint interval
```

All values can also be overridden by exporting environment variables in your
shell before running `make` or `docker compose`.

---

## Observability

Prometheus and Grafana start automatically with `make up` — no extra step
required.

| Service    | URL                                        |
|------------|--------------------------------------------|
| Prometheus | http://localhost:9090                      |
| Grafana    | http://localhost:3000  (`admin` / `admin`) |

Flink exposes metrics via its built-in Prometheus reporter, already configured
in `docker-compose.yml` (port 9250 for the jobmanager, 9251 for the
taskmanager).

Kudu masters and tservers expose Prometheus-format metrics at their respective
`/metrics_prometheus` endpoints (e.g. `http://localhost:8051/metrics_prometheus`)
with no additional flags required.

---


## Replication mechanics

The `java/kudu-replication` module implements the replication pipeline:

```
KuduSource (CONTINUOUS_UNBOUNDED, diff scan every DISCOVERY_INTERVAL_SECONDS)
  │  CustomReplicationRowResultConverter  (RowResult -> Flink Row)
  ▼
KuduSink (UPSERT via CustomReplicationOperationMapper)
```

Key properties:
- **AT_LEAST_ONCE** checkpointing: UPSERT semantics make the pipeline idempotent.
- **Diff scans**: Each discovery cycle scans only rows modified since the last
  checkpoint, keeping throughput proportional to the change rate rather than
  total table size.
- **Auto table creation**: With `--job.createTable=true`, the Flink job reads the
  source table schema (columns, primary key, hash/range partitions) and
  recreates it exactly on the sink cluster.

Known limitations:
- **No schema drift detection**: Schema changes on the source table (columns
  added, removed, or renamed) are not detected or propagated automatically.
  To perform a schema change:
  1. `make stop-ingest` — stop new writes to the source.
  2. Wait for the replication pipeline to drain.
  3. `make stop-job` — stop the Flink job and write a savepoint.
  4. Apply the schema change on both the source and sink clusters.
  5. `make submit-job` — resume the job from the savepoint.
