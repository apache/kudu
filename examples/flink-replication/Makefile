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

# Kudu Replication Demo — Makefile
#
# Prerequisites: Docker + Docker Compose v2, Java 17+ (for build-jar only)
#
# build-jar     build the Flink shadow JAR (once per code change)
# up            start all containers and wait for readiness
# start-ingest  start the ingest simulator (creates demo_table)
# stop-ingest   pause ingestion (simulate source going offline)
# submit-job    submit the Flink replication job
# verify        compare source vs sink row counts
# down          stop all services
# clean         stop and remove all volumes
# logs          tail all container logs
# ui            print all web UI URLs

DEMO_DIR  := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
REPO_ROOT := $(abspath $(DEMO_DIR)/../..)
JAR_DIR   := $(DEMO_DIR)flink/jars

.PHONY: help build-jar up down clean logs \
        start-ingest stop-ingest logs-ingest \
        submit-job stop-job verify logs-flink ui

# Default target
help:
	@echo ""
	@echo "  Kudu Replication Demo"
	@echo "  ─────────────────────────────────────────────────────────────────"
	@echo "  make build-jar        Build the Flink replication shadow JAR"
	@echo "  make up               Start Kudu + Flink infrastructure (waits for readiness)"
	@echo "  make down             Stop all services"
	@echo "  make clean            Stop services and remove all volumes"
	@echo "  ─────────────────────────────────────────────────────────────────"
	@echo "  make start-ingest     Start the ingest container (INSERT/UPDATE/DELETE mix)"
	@echo "  make stop-ingest      Pause ingestion (container kept, data preserved)"
	@echo "  make submit-job       Submit (or resume from savepoint) the Flink job"
	@echo "  make stop-job         Stop the job and write a savepoint"
	@echo "  make verify           Compare source vs sink row counts"
	@echo "  ─────────────────────────────────────────────────────────────────"
	@echo "  make logs             Tail all container logs"
	@echo "  make logs-ingest      Tail ingest container logs"
	@echo "  make logs-flink       Tail Flink container logs"
	@echo "  make ui               Print all web UI URLs"
	@echo ""

# JAR build

build-jar:
	@echo "==> Building kudu-replication shadow JAR ..."
	cd "$(REPO_ROOT)/java" && ./gradlew :kudu-replication:shadowJar
	@mkdir -p "$(JAR_DIR)"
	@rm -f "$(JAR_DIR)"/kudu-replication-*SNAPSHOT.jar
	cp "$(REPO_ROOT)"/java/kudu-replication/build/libs/kudu-replication-*SNAPSHOT.jar \
	   "$(JAR_DIR)/"
	@count=$$(ls "$(JAR_DIR)"/kudu-replication-*SNAPSHOT.jar 2>/dev/null | wc -l); \
	if [ "$$count" -ne 1 ]; then \
	  echo "ERROR: Expected exactly 1 JAR in $(JAR_DIR) but found $$count."; \
	  echo "       Check java/kudu-replication/build/libs/ for duplicate SNAPSHOTs."; \
	  exit 1; \
	fi
	@echo ""
	@echo "JAR installed to $(JAR_DIR):"
	@ls -lh "$(JAR_DIR)/"*.jar

# Infrastructure lifecycle

# --profile ingest is included for down/clean so the ingest container is also
# stopped and removed. It is intentionally omitted from 'up' so that ingest
# does not start automatically — use 'make start-ingest' explicitly instead.
INGEST_PROFILE = --profile ingest

up:
	@if [ ! -f "$(JAR_DIR)/"*.jar ] 2>/dev/null; then \
	  echo "WARNING: No JAR found in $(JAR_DIR)/"; \
	  echo "         Run 'make build-jar' before submitting the Flink job."; \
	  echo ""; \
	fi
	docker compose up -d
	@bash "$(DEMO_DIR)scripts/ready.sh"

down:
	docker compose $(INGEST_PROFILE) down

clean:
	docker compose $(INGEST_PROFILE) down -v
	@echo "All containers and named volumes removed."

logs:
	docker compose logs -f

# Demo workflow

# Starts ingest/ingest.py inside apache/kudu:${KUDU_PYTHON_VERSION} (set in .env).
# Applies a mix of INSERT / UPDATE / DELETE / UPSERT ops.  No host Python setup required.
start-ingest:
	docker compose $(INGEST_PROFILE) up -d kudu-ingest
	@echo ""
	@echo "Ingest started.  Tail logs: make logs-ingest   Pause: make stop-ingest"

# Stop (pause) ingestion without removing the container.
# Simulates source ingestion going offline.  Resume with: make start-ingest
stop-ingest:
	docker compose $(INGEST_PROFILE) stop kudu-ingest
	@echo ""
	@echo "Ingest paused.  Resume with: make start-ingest"

logs-ingest:
	docker compose $(INGEST_PROFILE) logs -f kudu-ingest

submit-job:
	@bash "$(DEMO_DIR)flink/submit-job.sh"

stop-job:
	@docker compose exec -T flink-jobmanager bash -c '\
	  jobs=$$(flink list -r 2>/dev/null | grep -oE "[a-f0-9]{32}"); \
	  if [ -z "$$jobs" ]; then \
	    echo "No running Flink jobs found."; \
	  else \
	    mkdir -p /checkpoints/savepoints && chmod 777 /checkpoints/savepoints; \
	    for job in $$jobs; do \
	      echo "Stopping job $$job with savepoint ..."; \
	      flink stop --savepointPath /checkpoints/savepoints "$$job"; \
	    done; \
	  fi'

verify:
	@bash "$(DEMO_DIR)scripts/verify-replication.sh"

# Convenience

logs-flink:
	docker compose logs -f flink-jobmanager flink-taskmanager

ui:
	@echo ""
	@echo "  Web UIs"
	@echo "  ─────────────────────────────────────────────────────────────────"
	@echo "  Source Kudu Master  : http://localhost:8051"
	@echo "  Source TServer 1    : http://localhost:8050"
	@echo "  Source TServer 2    : http://localhost:8150"
	@echo "  Sink Kudu Master    : http://localhost:18051"
	@echo "  Sink TServer 1      : http://localhost:18050"
	@echo "  Sink TServer 2      : http://localhost:18150"
	@echo "  Flink Dashboard     : http://localhost:8081"
	@echo "  ─────────────────────────────────────────────────────────────────"
	@echo "  Prometheus          : http://localhost:9090"
	@echo "  Grafana             : http://localhost:3000  (admin / admin)"
	@echo ""
