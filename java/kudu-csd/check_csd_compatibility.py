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

# This tool checks for compatibility between two CSD jars.
# Namely, it ensures that no metrics have been removed.

import argparse
import json
import sys
import zipfile

# The path within a CSD that contains the MDL file.
MDL_PATH = "descriptor/service.mdl"


def metrics_from_mdl(mdl):
  """
  Return a set() representing the metrics in the given MDL JSON object.

  Each entry is a tuple of either:
    ('entity', entity_name, metric_name)
  or
    ('role', role_name, metric_name)
  """
  metrics = set()
  for entity_def in mdl['metricEntityTypeDefinitions']:
    for metric_def in entity_def['metricDefinitions']:
      metrics.add(('entity', entity_def['name'], metric_def['name']))
  for role_def in mdl['roles']:
    for metric_def in role_def['metricDefinitions']:
      metrics.add(('role', role_def['name'], metric_def['name']))
  return metrics


def check_mdl_compat(old_zip, new_zip, args):
  old_mdl = json.load(old_zip.open(MDL_PATH))
  new_mdl = json.load(new_zip.open(MDL_PATH))

  old_metrics = metrics_from_mdl(old_mdl)
  new_metrics = metrics_from_mdl(new_mdl)

  added_metrics = new_metrics.difference(old_metrics)
  removed_metrics = old_metrics.difference(new_metrics)

  print "Added %d metric(s):" % len(added_metrics)
  for m_type, m_entity, m_name in added_metrics:
    print "  %s metric %s" % (m_entity, m_name)
  if removed_metrics:
    print "Removed %d metric(s):" % len(removed_metrics)
    for m_type, m_entity, m_name in removed_metrics:
      print "  %s metric %s" % (m_entity, m_name)
    print "Compatibility check FAILED"
    sys.exit(1)
  print "No metrics were removed."
  print "Compatibility check PASSED"


def main():
  p = argparse.ArgumentParser(
    description=("Checks for compatibility between CSD JARs. " +
                 "May also generate a file containing JSON for " +
                 "metrics that were removed between the old version "
                 "of the CSD and the specified new one."))
  p.add_argument("old_jar", metavar="KUDU-old.jar", type=str,
                 help="The old CSD JAR to compare")
  p.add_argument("new_jar", metavar="KUDU-new.jar", type=str,
                 help="The new CSD JAR to compare")
  args = p.parse_args()

  old_zip = zipfile.ZipFile(args.old_jar)
  new_zip = zipfile.ZipFile(args.new_jar)
  check_mdl_compat(old_zip, new_zip, args)


if __name__ == "__main__":
  main()
