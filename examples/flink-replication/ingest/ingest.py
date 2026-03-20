#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

"""Continuous Kudu ingest simulator for the replication demo.

Creates the demo table if it does not already exist, then continuously
applies a mix of INSERT, UPDATE, and DELETE operations so the replication
pipeline is observable across all operation types.

Run in a container (no host setup needed):

    docker compose --profile ingest up -d kudu-ingest
    docker compose --profile ingest stop kudu-ingest   # pause ingestion
    docker compose --profile ingest start kudu-ingest  # resume ingestion

Or via make:

    make start-ingest
    make stop-ingest
    make start-ingest  # resume

Table schema:

    id        INT64            primary key, hash-partitioned (16 buckets)
    event_ts  UNIXTIME_MICROS  timestamp of the last write
    payload   STRING (LZ4)     random 32-character string

Environment variables (all optional, CLI flags take precedence):

    KUDU_MASTER      host:port of the source Kudu master  (default: localhost:7051)
    KUDU_TABLE       table name                            (default: demo_table)
    INGEST_INTERVAL  seconds between operations            (default: 1.0)
    INSERT_RATE      relative weight for INSERT ops        (default: 0.4)
    UPDATE_RATE      relative weight for UPDATE ops        (default: 0.2)
    DELETE_RATE      relative weight for DELETE ops        (default: 0.15)
    UPSERT_RATE      relative weight for UPSERT ops        (default: 0.25)

Rates are normalised automatically so only the ratios matter.
"""

import argparse
import logging
import os
import random
import string
import sys
import time
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ingest] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

try:
    import kudu
    from kudu.client import Partitioning
except ImportError:
    log.error(
        "kudu-python is not installed. "
        "Use the kudu-ingest container or install kudu-python."
    )
    sys.exit(1)


TABLE_NAME_DEFAULT = "demo_table"
MASTER_DEFAULT = "localhost:7051"
INTERVAL_DEFAULT = 1.0
# Number of hash partitions for demo_table.
# 16 tablets give the KuduSource enumerator more splits per scan cycle,
# making enumerator activity visible in Prometheus/Grafana.
NUM_PARTITIONS = 16

# Maximum IDs held in the in-memory pool.  When the pool is full no new
# inserts are issued until deletes free up space.
POOL_MAX = 2000


def _rand_payload():
    return "".join([random.choice(string.ascii_lowercase) for _ in range(32)])


def _weighted_choice(choices, weights):
    """Pick one item from choices according to weights. Python 2/3 compatible."""
    r = random.random() * sum(weights)
    for choice, w in zip(choices, weights):
        r -= w
        if r <= 0:
            return choice
    return choices[-1]


def _parse_master(master_str):
    """Split 'host:port' into (host, port). Returns port=7051 if omitted."""
    host, _, port_str = master_str.rpartition(":")
    if not host:
        return master_str, 7051
    return host, int(port_str)


def create_table_if_missing(client, table_name):
    if client.table_exists(table_name):
        log.info("Table '%s' already exists -- skipping creation.", table_name)
        return

    log.info("Creating table '%s' ...", table_name)
    builder = kudu.schema_builder()
    builder.add_column("id").type(kudu.int64).nullable(False).primary_key()
    builder.add_column("event_ts", type_=kudu.unixtime_micros, nullable=False)
    builder.add_column("payload", type_=kudu.string, nullable=True, compression="lz4")
    schema = builder.build()

    partitioning = Partitioning().add_hash_partitions(
        column_names=["id"], num_buckets=NUM_PARTITIONS
    )
    client.create_table(table_name, schema, partitioning)
    log.info("Table '%s' created (hash partitions: %d).", table_name, NUM_PARTITIONS)


def ingest_loop(client, table_name, interval, insert_w, update_w, delete_w, upsert_w):
    """Apply a continuous mix of INSERT / UPDATE / DELETE / UPSERT operations."""
    table = client.table(table_name)
    session = client.new_session()
    session.set_flush_mode(kudu.FLUSH_AUTO_SYNC)

    # In-memory pool of IDs that currently exist in the table.
    # Required so UPDATE, DELETE, and pool-targeting UPSERTs can target real rows.
    id_pool = []
    next_id = int(time.time() * 1000) % (2 ** 48)

    counts = {"insert": 0, "update": 0, "delete": 0, "upsert": 0}
    ops_since_log = 0
    LOG_EVERY = 10

    log.info(
        "Ingest running on '%s'  interval=%.1fs  "
        "insert=%.0f%%  update=%.0f%%  delete=%.0f%%  upsert=%.0f%%",
        table_name, interval,
        insert_w * 100, update_w * 100, delete_w * 100, upsert_w * 100,
    )

    while True:
        # Decide operation type based on pool state.
        if not id_pool:
            op_type = "insert"
        elif len(id_pool) >= POOL_MAX:
            op_type = _weighted_choice(
                ["update", "delete", "upsert"],
                [update_w, delete_w, upsert_w],
            )
        else:
            op_type = _weighted_choice(
                ["insert", "update", "delete", "upsert"],
                [insert_w, update_w, delete_w, upsert_w],
            )

        try:
            if op_type == "insert":
                row_id = next_id
                next_id += 1
                op = table.new_insert({
                    "id": row_id,
                    "event_ts": datetime.utcnow(),
                    "payload": _rand_payload(),
                })
                session.apply(op)
                session.flush()
                id_pool.append(row_id)

            elif op_type == "update":
                row_id = random.choice(id_pool)
                op = table.new_update({
                    "id": row_id,
                    "event_ts": datetime.utcnow(),
                    "payload": _rand_payload(),
                })
                session.apply(op)
                session.flush()

            elif op_type == "delete":
                row_id = random.choice(id_pool)
                op = table.new_delete({"id": row_id})
                session.apply(op)
                session.flush()
                id_pool.remove(row_id)

            else:  # upsert — 50% target an existing ID, 50% a new one
                if id_pool and random.random() < 0.5:
                    row_id = random.choice(id_pool)
                else:
                    row_id = next_id
                    next_id += 1
                    id_pool.append(row_id)
                op = table.new_upsert({
                    "id": row_id,
                    "event_ts": datetime.utcnow(),
                    "payload": _rand_payload(),
                })
                session.apply(op)
                session.flush()

        except kudu.KuduBadStatus:
            for err in session.get_pending_errors():
                log.error("Write error (%s id=%s): %s", op_type, row_id, err)

        counts[op_type] += 1
        ops_since_log += 1
        if ops_since_log >= LOG_EVERY:
            log.info(
                "total  insert=%d  update=%d  delete=%d  upsert=%d  pool=%d",
                counts["insert"], counts["update"], counts["delete"],
                counts["upsert"], len(id_pool),
            )
            ops_since_log = 0

        time.sleep(interval)


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--master",
        default=os.environ.get("KUDU_MASTER", MASTER_DEFAULT),
        metavar="HOST:PORT",
        help="Kudu master address (default: %(default)s)",
    )
    parser.add_argument(
        "--table",
        default=os.environ.get("KUDU_TABLE", TABLE_NAME_DEFAULT),
        help="Kudu table name (default: %(default)s)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.environ.get("INGEST_INTERVAL", INTERVAL_DEFAULT)),
        metavar="SECONDS",
        help="Delay between operations (default: %(default)s)",
    )
    parser.add_argument(
        "--insert-rate",
        type=float,
        default=float(os.environ.get("INSERT_RATE", 0.4)),
        help="Relative weight for INSERT ops (default: %(default)s)",
    )
    parser.add_argument(
        "--update-rate",
        type=float,
        default=float(os.environ.get("UPDATE_RATE", 0.2)),
        help="Relative weight for UPDATE ops (default: %(default)s)",
    )
    parser.add_argument(
        "--delete-rate",
        type=float,
        default=float(os.environ.get("DELETE_RATE", 0.15)),
        help="Relative weight for DELETE ops (default: %(default)s)",
    )
    parser.add_argument(
        "--upsert-rate",
        type=float,
        default=float(os.environ.get("UPSERT_RATE", 0.25)),
        help="Relative weight for UPSERT ops (default: %(default)s)",
    )
    args = parser.parse_args()

    total = args.insert_rate + args.update_rate + args.delete_rate + args.upsert_rate
    if total == 0:
        raise ValueError("All operation rates are zero -- nothing to do.")

    insert_w = args.insert_rate / total
    update_w = args.update_rate / total
    delete_w = args.delete_rate / total
    upsert_w = args.upsert_rate / total

    host, port = _parse_master(args.master)
    log.info("Connecting to Kudu master %s:%d ...", host, port)
    try:
        client = kudu.connect(host=host, port=port)
    except Exception as exc:
        log.error("Could not connect to %s:%d: %s", host, port, exc)
        raise

    log.info("Connected.")
    create_table_if_missing(client, args.table)
    ingest_loop(client, args.table, args.interval, insert_w, update_w, delete_w, upsert_w)


if __name__ == "__main__":
    main()
