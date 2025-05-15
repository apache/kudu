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

# This script should be run from the '$KUDU_HOME/java' directory.
set -euo pipefail

SUMMARY_FILE="build/jacocoLogAggregatedCoverage.txt"
HTML_REPORT="build/reports/jacoco/jacocoAggregatedReport/html/index.html"

echo "Running JaCoCo CLI summary..."
# Run the jacocoLogAggregatedCoverage task and extract only the summary section
# (starting with "Test Coverage:" until the next blank line), stripping all other Gradle logs.
if ! ./gradlew jacocoLogAggregatedCoverage | awk '/^Test Coverage:/,/^$/' > "$SUMMARY_FILE"; then
  echo "Gradle jacocoLogAggregatedCoverage task failed."
  exit 1
fi

if [ ! -f "$HTML_REPORT" ]; then
  echo "HTML report not found at $HTML_REPORT"
  exit 1
fi

echo "Appending summary to HTML report..."
{
  echo "<pre>"
  cat "$SUMMARY_FILE"
  echo "</pre>"
} >> "$HTML_REPORT"
echo "Summary appended to $HTML_REPORT"
