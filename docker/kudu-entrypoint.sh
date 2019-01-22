#!/bin/bash
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

# This script follows the pattern described in the docker best practices here:
# https://docs.docker.com/develop/develop-images/dockerfile_best-practices/#entrypoint
#
# KUDU_FLAGS can be set by users to do more configuration of the
# kudu-master and kudu-tserver.

set -xe

# Wait until the master hosts can be resolved.
#
# Without this Kudu will fail with "Name or service not known" errors
# on startup.
#
# Gives a maximum of 5 attempts/seconds to each host. On failure
# falls through without failing to still give Kudu a chance to startup
# or fail on it's own.
wait_for_master_hosts() {
  IFS=","
  for HOST in $KUDU_MASTERS
  do
    MAX_ATTEMPTS=5
    ATTEMPTS=0
    until `ping -c1 $HOST &>/dev/null;` || [ $ATTEMPTS -eq $MAX_ATTEMPTS ]; do
      ATTEMPTS=$((ATTEMPTS + 1))
      sleep 1;
    done
  done
  unset IFS
}

if [[ "$1" = 'kudu-master' ]]; then
    # Create the data directory.
    mkdir -p /var/lib/kudu/master
    # TODO: Remove use_hybrid_clock=false when ntpd is setup
    KUDU_OPTS="--master_addresses=$KUDU_MASTERS
         --fs_wal_dir=/var/lib/kudu/master \
         --webserver_doc_root=/opt/kudu/www \
         --logtostderr \
         --use_hybrid_clock=false \
         $KUDU_FLAGS"
    wait_for_master_hosts
    exec kudu-master $KUDU_OPTS
elif [[ "$1" = 'kudu-tserver' ]]; then
    # Create the data directory.
    mkdir -p /var/lib/kudu/tserver
    # TODO: Remove use_hybrid_clock=false when ntpd is setup
    KUDU_OPTS="--tserver_master_addrs=$KUDU_MASTERS
      --fs_wal_dir=/var/lib/kudu/tserver \
      --webserver_doc_root=/opt/kudu/www \
      --logtostderr \
      --use_hybrid_clock=false \
      $KUDU_FLAGS"
    wait_for_master_hosts
    exec kudu-tserver $KUDU_OPTS
fi

# Support calling anything else in the container.
exec "$@"