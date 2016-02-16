#/bin/bash
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

EXPORTER="com.yahoo.ycsb.measurements.exporter.JSONArrayMeasurementsExporter"
COMMON_FLAGS="-p recordcount=100000000 -p columnfamily=family -cp /etc/hbase/conf -p exporter=$EXPORTER -p table=ycsb_100m"
OUT_DIR=uniform-hbase

mkdir -p $OUT_DIR
if true ; then
	./bin/ycsb load hbase10 $COMMON_FLAGS  -p exportfile=$OUT_DIR/load.json -p clientbuffering=true \
	  -P workloads/workloada -p recordcount=100000000 -threads 16 -s 2>&1 | tee $OUT_DIR/load-100M.log
fi
for x in a b c d ; do
  dist_param=
  if [ "$x" != "d" ]; then
    dist_param="-p requestdistribution=uniform"
  fi
  ./bin/ycsb run hbase10 -P workloads/workload$x -p recordcount=100000000 -p operationcount=10000000 \
      $COMMON_FLAGS -p exportfile=$OUT_DIR/$x.json \
      $dist_param \
      -threads 64 -s 2>&1 | tee $OUT_DIR/run-workload$x.log
done

