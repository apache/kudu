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
#################################################################
# WARNING: This code is not stable and is intended for internal testing only.
#
# Backup and Restore performance test driver.
#
# Example invocation:
#   backup-perf.py \
#      --spark-submit-command spark2-submit \
#      --kudu-spark-tools-jar kudu-spark2-tools_2.11-1.9.0-SNAPSHOT.jar
#      --kudu-backup-jar kudu-backup2_2.11-1.9.0-SNAPSHOT.jar
#      --num-tasks 20 \
#      --master-addresses a123.example.com \
#      --impalad-address a123.example.com \
#      --backup-path hdfs:///user/foo/backups \
#      --partitions 450 \
#      --table-data-size-mb 500000 \
#      test_table_1
#################################################################

import argparse
import datetime
import json
import subprocess
import sys
import timeit

from collections import OrderedDict

class TickingTimer:
  """ Timer to keep track of the period between ticks. """
  def __init__(self):
    self.last_tick_ = timeit.default_timer()

  def tick(self):
    """
    Resets the tick timer and returns the duration between the last two calls
    to tick(), or construction of this object, whichever was more recent.
    """
    prev_last_tick = self.last_tick_
    self.last_tick_ = timeit.default_timer()
    latest_tick_period = self.last_tick_ - prev_last_tick
    return latest_tick_period

  def last_tick_time(self):
    """
    Returns the clock time of the last tick, or construction of this object,
    whichever was more recent.
    """
    return self.last_tick_

def check_output(*popenargs, **kwargs):
  r"""Run command with arguments and return its output as a byte string.
  Backported from Python 2.7 as it's implemented as pure python on stdlib.
  >>> check_output(['/usr/bin/python', '--version'])
  Python 2.6.2
  """
  process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
  output, unused_err = process.communicate()
  retcode = process.poll()
  if retcode:
    cmd = kwargs.get("args")
    if cmd is None:
      cmd = popenargs[0]
    error = subprocess.CalledProcessError(retcode, cmd)
    error.output = output
    raise error
  return output

def parse_bool(s):
  if s.lower() == 'true': return True
  if s.lower() == 'false': return False
  raise argparse.ArgumentTypeError('value must be true or false')

def timestamp():
  return datetime.datetime.now().isoformat()

def run_command(opts, cmd):
  """ Print the command and run it if not in dry-run mode. """
  print(cmd)
  if not opts.dryrun:
    print(check_output(cmd, shell=True))

def ensure_commands_available(opts):
  run_command(opts, 'which impala-shell')
  run_command(opts, 'which kudu')

def get_restored_table_name(opts):
  return opts.table_name + opts.table_restore_suffix

# TODO: Change this to use the Kudu python API.
#  It's good dog-fooding and removes the Impala requirement.
def create_table(opts, stats):
  """ Create a Kudu table via impala-shell """
  print("--------------------------------------")
  print("Creating table %s" % (opts.table_name,))
  print("--------------------------------------")
  print(timestamp())
  create_table_ddl = "CREATE TABLE %s (" % (opts.table_name,)
  num_bigint_cols = opts.columns - opts.num_string_columns
  assert(num_bigint_cols > 0)
  for i in range(opts.columns):
    coltype = 'STRING'
    if i < num_bigint_cols: coltype = 'BIGINT'
    if i > 0: create_table_ddl += ', '
    create_table_ddl += "f%d %s" % (i, coltype)
    if i == 0: create_table_ddl += ' PRIMARY KEY'
  create_table_ddl += ") PARTITION BY HASH(f0) PARTITIONS %d STORED AS KUDU " % \
                      (opts.partitions, )
  create_table_ddl += "TBLPROPERTIES ('kudu.num_tablet_replicas' = '%d')" % \
                      (opts.replication_factor, )

  cmd = 'echo "%s" | impala-shell -i %s -f -' % (create_table_ddl, opts.impalad_address)
  run_command(opts, cmd)

def drop_created_table(opts, stats):
  """ Drop the created Kudu table via impala-shell """
  print("--------------------------------------")
  print("Dropping created table %s" % (opts.table_name, ))
  print("--------------------------------------")
  print(timestamp())
  sql = "DROP TABLE %s" % (opts.table_name, )
  cmd = 'echo "%s" | impala-shell -i %s -f -' % (sql, opts.impalad_address)
  run_command(opts, cmd)

def drop_restored_table(opts, stats):
  """ Drop the restored Kudu table via the kudu table delete command """
  # TODO: This may no longer be needed if and when we integrate
  # restoring HMS metadata and the table is restored as "Impala-managed".
  print("--------------------------------------")
  print("Dropping restored table %s" % (get_restored_table_name(opts), ))
  print("--------------------------------------")
  print(timestamp())
  cmd = 'kudu table delete %s %s' % (opts.master_addresses, opts.table_prefix +
                                     get_restored_table_name(opts))
  run_command(opts, cmd)

def load_table(opts, stats):
  """ Load a table with data using the DistributedDataGenerator spark job """
  print("--------------------------------------")
  print("Loading table %s" % (opts.table_name,))
  print("--------------------------------------")
  print(timestamp())
  # Example invocation:
  #   spark-submit --class org.apache.kudu.spark.tools.DistributedDataGenerator \
  #     kudu-spark2-tools_2.11-1.8.0-SNAPSHOT.jar \
  #     --type random \
  #     --num-rows 10000000 \
  #     --num-tasks 20 \
  #     impala::default.foo_test3 m123.example.com
  CLASS_NAME = 'org.apache.kudu.spark.tools.DistributedDataGenerator'
  # TODO: Non-string columns are assumed to be 8 bytes.
  row_size_bytes = opts.num_string_columns * opts.string_field_len + \
                   (opts.columns - opts.num_string_columns) * 8
  num_rows = opts.table_data_size_mb * 1024 * 1024 / row_size_bytes
  print("INFO: Inserting %d rows of %d bytes each" % (num_rows, row_size_bytes))
  stats['row_size_bytes'] = row_size_bytes
  stats['num_rows'] = num_rows
  cmd = "%s --class %s %s --type %s --num-rows %d --num-tasks %d %s %s" % \
    (opts.spark_submit_command, CLASS_NAME, opts.kudu_spark_tools_jar,
     opts.load_policy, num_rows, opts.load_num_tasks, opts.table_prefix + opts.table_name,
     opts.master_addresses)
  run_command(opts, cmd)

def backup_table(opts, stats):
  print("--------------------------------------")
  print("Backing up table %s" % (opts.table_name,))
  print("--------------------------------------")
  print(timestamp())
  CLASS_NAME = "org.apache.kudu.backup.KuduBackup"
  cmd = "%s --class %s %s --kuduMasterAddresses %s --scanRequestTimeoutMs %d --path %s %s" % \
    (opts.spark_submit_command, CLASS_NAME, opts.kudu_backup_jar,
     opts.master_addresses, opts.scan_request_timeout_ms,
     opts.backup_path, opts.table_prefix + opts.table_name)
  run_command(opts, cmd)

def restore_table(opts, stats):
  print("--------------------------------------")
  print("Restoring table %s as %s" % (opts.table_name, get_restored_table_name(opts)))
  print("--------------------------------------")
  print(timestamp())
  CLASS_NAME = "org.apache.kudu.backup.KuduRestore"
  cmd = "%s --class %s %s --tableSuffix %s --kuduMasterAddresses %s --path %s %s" % \
    (opts.spark_submit_command, CLASS_NAME, opts.kudu_backup_jar,
     opts.table_restore_suffix, opts.master_addresses, opts.backup_path, opts.table_prefix + opts.table_name)
  run_command(opts, cmd)

def parse_args():
  """ Parse command-line arguments """
  parser = argparse.ArgumentParser(description='Run a Kudu backup and restore performance test',
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)

  # Kudu Configuration
  parser.add_argument('--master-addresses', required=True, help='The Kudu master addresses')

  # Impala Configuration
  parser.add_argument('--impalad-address', required=True, help='The Impala daemon address')
  parser.add_argument('--table-prefix', default='impala::default.',
                      help='Kudu table name prefix in the Hive metastore')

  # Spark Job Configuration
  parser.add_argument('--spark-submit-command', default='spark-submit',
                      help='The name of the spark-submit binary')
  ## Spark Loader Configuration
  parser.add_argument('--kudu-spark-tools-jar', default='kudu-spark*-tools*.jar',
                      help='The path to the kudu-spark-tools jar (for --load-table)')
  parser.add_argument('--load-num-tasks', type=int, default=20,
                      help='Number of Spark tasks to create when loading data')
  parser.add_argument('--load-policy', default='sequential', choices=['sequential', 'random'],
                      help='The data loading policy for the data generator')
  parser.add_argument('--string-field-len', type=int, default=128,
                      help='The length, in bytes, of generated string column values')

  ## Spark Backup/Restore Job Configuration
  parser.add_argument('--kudu-backup-jar', default='kudu-backup*.jar',
                      help='The path to the kudu-backup jar')
  parser.add_argument('--backup-path', default='hdfs:///kudu-backup-tests',
                      help='The Hadoop-compatible path at which to store the backup')
  parser.add_argument('--backup-file-format', default='parquet',
                      help='The file format of the backup: must be parquet')
  parser.add_argument('--scan-request-timeout-ms', type=int, default=30000,
                      help='The default scan timeout for backup, in milliseconds')
  parser.add_argument('--table-restore-suffix', default='-restore',
                      help='Kudu table name suffix to append on restore')

  # Table Configuration
  parser.add_argument('--columns', type=int, default=10,
                      help='The number of columns in the Kudu table')
  parser.add_argument('--num-string-columns', type=int, default=9,
                      help='The number of string columns in the table; the rest will be bigints')
  parser.add_argument('--partitions', type=int, default=10,
                      help='The number of hash partitions of the table. '
                           'This script only supports hash partitions')
  parser.add_argument('--table-data-size-mb', type=int, default=1024,
                      help='The uncompressed data size of the table, in MB')
  parser.add_argument('--replication-factor', type=int, default=3,
                      help='The replication factor of the table')

  # Positional
  parser.add_argument('table_name', help='The name of the Kudu table to create/backup')

  # Actions
  parser.add_argument('--create-table', type=parse_bool, choices=[True, False], default=False,
                      help='Whether to create the table for loading')
  parser.add_argument('--drop-created-table', type=parse_bool, choices=[True, False], default=False,
                      help='Whether to drop the created table after a successful test run')
  parser.add_argument('--load-table', type=parse_bool, choices=[True, False], default=False,
                      help='Whether to load the table with data')
  parser.add_argument('--backup-table', type=parse_bool, choices=[True, False], default=False,
                      help='Whether to back up the table')
  parser.add_argument('--restore-table', type=parse_bool, choices=[True, False], default=False,
                      help='Whether to restore the table')
  parser.add_argument('--drop-restored-table', type=parse_bool, choices=[True, False], default=False,
                      help='Whether to drop the restored table after a successful test run')

  # Utility
  parser.add_argument('--dryrun', action='store_true',
                      help='Do not execute any commands, only print what would be executed')

  return parser.parse_args()

def main():
  start_timestamp = timestamp()
  print(start_timestamp)
  print("Starting perf test...")

  opts = parse_args()

  stats = OrderedDict()
  stats['start_timestamp'] = start_timestamp

  stats['columns'] = opts.columns
  stats['num_string_columns'] = opts.num_string_columns
  stats['partitions'] = opts.partitions
  stats['table_data_size_mb'] = opts.table_data_size_mb
  stats['replication_factor'] = opts.replication_factor
  stats['load_num_tasks'] = opts.load_num_tasks
  stats['load_policy'] = opts.load_policy
  stats['string_field_len'] = opts.load_policy

  timer = TickingTimer()
  start = timer.last_tick_time()

  if opts.create_table:
    create_table(opts, stats)
    stats['create_table_duration_sec'] = timer.tick()

  if opts.load_table:
    load_table(opts, stats)
    stats['load_table_duration_sec'] = timer.tick()

  if opts.backup_table:
    backup_table(opts, stats)
    stats['backup_table_duration_sec'] = timer.tick()

  if opts.restore_table:
    restore_table(opts, stats)
    stats['restore_table_duration_sec'] = timer.tick()

  if opts.drop_created_table:
    drop_created_table(opts, stats)
    stats['drop_created_table_duration_sec'] = timer.tick()

  if opts.drop_restored_table:
    drop_restored_table(opts, stats)
    stats['drop_restored_table_duration_sec'] = timer.tick()

  end = timer.last_tick_time()

  stats['end_timestamp'] = timestamp()
  print(stats['end_timestamp'])
  print("Ending perf test")
  total_duration = end - start
  stats['total_duration_sec'] = total_duration
  print("Total time elapsed: %s s" % (total_duration, ))

  print("")
  print("--------------------------------------")
  print("[ BEGIN STATS ]")
  print(json.dumps(stats,
                   indent=4, separators=(',', ': ')))
  print("[ END STATS ]")
  print("--------------------------------------")

if __name__ == "__main__":
  main()
