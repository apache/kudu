#!/bin/bash
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

prepend_lines() {
  tail --lines=+2 $1 | perl -p -e "s,^,$2,g;"
}

printf "dist\tsys\tworkload\ttime\ttput\n"
for dist in zipfian uniform ; do
  for sys in hbase kudu ; do
    prepend_lines $dist-$sys/load-100M.log.tsv "${dist}\t${sys}\tload\t"
    prepend_lines $dist-$sys/run-workloada.log.tsv "${dist}\t${sys}\ta\t"
    prepend_lines $dist-$sys/run-workloadb.log.tsv "${dist}\t${sys}\tb\t"
    prepend_lines $dist-$sys/run-workloadc.log.tsv "${dist}\t${sys}\tc\t"
    prepend_lines $dist-$sys/run-workloadd.log.tsv "latest\t${sys}\td\t"
  done
done
