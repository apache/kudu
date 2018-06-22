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

from datetime import datetime
import argparse

import kudu
from kudu.client import Partitioning


# Parse arguments
parser = argparse.ArgumentParser(description='Basic Example for Kudu Python.')
parser.add_argument('--masters', '-m', nargs='+', default='localhost',
                    help='The master address(es) to connect to Kudu.')
parser.add_argument('--ports', '-p', nargs='+', default='7051',
                    help='The master server port(s) to connect to Kudu.')
args = parser.parse_args()


# Connect to Kudu master server(s).
client = kudu.connect(host=args.masters, port=args.ports)

# Define a schema for a new table.
builder = kudu.schema_builder()
builder.add_column('key').type(kudu.int64).nullable(False).primary_key()
builder.add_column('ts_val', type_=kudu.unixtime_micros, nullable=False, compression='lz4')
schema = builder.build()

# Define the partitioning schema.
partitioning = Partitioning().add_hash_partitions(column_names=['key'], num_buckets=3)

# Delete table if it already exists.
if client.table_exists('python-example'):
  client.delete_table('python-example')

# Create a new table.
client.create_table('python-example', schema, partitioning)

# Open a table.
table = client.table('python-example')

# Create a new session so that we can apply write operations.
session = client.new_session()

# Insert a row.
op = table.new_insert({'key': 1, 'ts_val': datetime.utcnow()})
session.apply(op)

# Upsert a row.
op = table.new_upsert({'key': 2, 'ts_val': "2016-01-01T00:00:00.000000"})
session.apply(op)

# Update a row.
op = table.new_update({'key': 1, 'ts_val': ("2017-01-01", "%Y-%m-%d")})
session.apply(op)

# Delete a row.
op = table.new_delete({'key': 2})
session.apply(op)

# Flush write operations, if failures occur, print them.
try:
  session.flush()
except kudu.KuduBadStatus:
  print(session.get_pending_errors())

# Create a scanner and add a predicate.
scanner = table.scanner()
scanner.add_predicate(table['ts_val'] == datetime(2017, 1, 1))

# Open scanner and print all tuples.
# Note: This doesn't scale for large scans
# Output: [(1, datetime.datetime(2017, 1, 1, 0, 0, tzinfo=<UTC>))]
print(scanner.open().read_all_tuples())
