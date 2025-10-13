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

import argparse

import kudu
from kudu.client import Partitioning


def create_schema():
  """
  Create a schema for a table with array columns.

  Similar to CreateSchema() in the C++ example, this creates a schema with:
  - A primary key column (key)
  - A regular INT32 column (int_val)
  - Array columns for INT64, STRING, and DOUBLE types
  """
  builder = kudu.schema_builder()
  builder.add_column('key').type(kudu.int32).nullable(False).primary_key()
  builder.add_column('int_val', type_=kudu.int32, nullable=False)

  # Add array columns for different data types.
  # Array of INT64 values.
  builder.add_column('arr_int64').nested_type(kudu.array_type(kudu.int64))
  # Array of STRING values.
  builder.add_column('arr_string').nested_type(kudu.array_type(kudu.string))
  # Array of DOUBLE values.
  builder.add_column('arr_double').nested_type(kudu.array_type(kudu.double))

  return builder.build()


def insert_rows(table, session, num_rows):
  """
  Insert rows with array data into the table.

  Arrays can contain None to represent NULL values, and can have
  different lengths.
  """
  for i in range(num_rows):
    op = table.new_insert({'key': i, 'int_val': i * 2})
    # Array with three elements, two non-nulls and one null in the middle.
    op['arr_int64'] = [i * 6, None, i * 7]
    op['arr_string'] = ['row_{0}'.format(i), 'data_{0}'.format(i), None]
    # Arrays can have different lengths.
    if i % 2 == 0:
      op['arr_double'] = [i * 1.1, i * 2.2, i * 3.3]
    else:
      op['arr_double'] = [i * 5.5]
    session.apply(op)

  # Flush write operations, if failures occur, print them and exit.
  try:
    session.flush()
  except kudu.KuduBadStatus:
    print(session.get_pending_errors())
    raise


def scan_rows(table, lower_bound=None, upper_bound=None, limit=None):
  """
  Scan and print rows from the table.

  Optionally filter by key range [lower_bound, upper_bound] and limit
  the number of rows printed.
  """
  scanner = table.scanner()

  # Add predicates if bounds are specified.
  if lower_bound is not None:
    scanner.add_predicate(table['key'] >= lower_bound)
  if upper_bound is not None:
    scanner.add_predicate(table['key'] <= upper_bound)

  scanner.open()
  row_count = 0
  while scanner.has_more_rows():
    batch = scanner.read_next_batch_tuples()
    for row in batch:
      if limit is None or row_count < limit:
        print('(key: {0}, int_val: {1}, arr_int64: {2}, arr_string: {3}, arr_double: {4})'.format(
            row[0], row[1], row[2], row[3], row[4]))
      row_count += 1

  return row_count


def update_row(table, session, key, int_val, arr_int64, arr_string, arr_double):
  """Update a row with new array values."""
  op = table.new_update({'key': key, 'int_val': int_val})
  op['arr_int64'] = arr_int64
  op['arr_string'] = arr_string
  op['arr_double'] = arr_double
  session.apply(op)

  try:
    session.flush()
  except kudu.KuduBadStatus:
    print(session.get_pending_errors())
    raise


def delete_rows(table, session, keys):
  """Delete rows with the specified keys."""
  for key in keys:
    op = table.new_delete({'key': key})
    session.apply(op)

  try:
    session.flush()
  except kudu.KuduBadStatus:
    print(session.get_pending_errors())
    raise


# Parse arguments.
parser = argparse.ArgumentParser(description='Array Data Type Example for Kudu Python.')
parser.add_argument('--masters', '-m', nargs='+', default='localhost',
                    help='The master address(es) to connect to Kudu.')
parser.add_argument('--ports', '-p', nargs='+', default='7051',
                    help='The master server port(s) to connect to Kudu.')
args = parser.parse_args()

# Connect to Kudu master server(s).
client = kudu.connect(host=args.masters, port=args.ports)

# Create a schema with array columns.
schema = create_schema()

# Define the partitioning schema.
partitioning = Partitioning().add_hash_partitions(column_names=['key'], num_buckets=3)

# Delete table if it already exists.
table_name = 'python-array-example'
if client.table_exists(table_name):
  client.delete_table(table_name)

# Create a new table.
client.create_table(table_name, schema, partitioning)

# Open a table.
table = client.table(table_name)

# Create a new session so that we can apply write operations.
session = client.new_session()

# Insert rows with array data.
insert_rows(table, session, 10)

# Scan and read array data.
print('Scanning first 3 rows:')
total = scan_rows(table, limit=3)
print('Total rows: {0}\n'.format(total))

# Scan with predicates.
print('Scanning rows with key >= 5 AND key <= 7:')
scan_rows(table, lower_bound=5, upper_bound=7)
print('')

# Update a row with new array data (different length arrays).
update_row(table, session, key=3, int_val=999,
           arr_int64=[100, 200, 300, 400],
           arr_string=['updated', 'data'],
           arr_double=[99.9])

print('Updated row (key=3):')
scan_rows(table, lower_bound=3, upper_bound=3)
print('')

# Insert a row with empty arrays.
op = table.new_insert({'key': 100, 'int_val': 0})
op['arr_int64'] = []
op['arr_string'] = []
op['arr_double'] = []
session.apply(op)

try:
  session.flush()
except kudu.KuduBadStatus:
  print(session.get_pending_errors())
  raise

print('Row with empty arrays (key=100):')
scan_rows(table, lower_bound=100, upper_bound=100)
print('')

# Delete specific rows.
delete_rows(table, session, [5, 6, 7])

# Verify deletion.
print('Remaining keys in range [4-8] after deletion:')
scanner = table.scanner()
scanner.add_predicate(table['key'] >= 4)
scanner.add_predicate(table['key'] <= 8)
remaining_keys = [row[0] for row in scanner.open().read_all_tuples()]
print(sorted(remaining_keys))

# Clean up - delete the table.
client.delete_table(table_name)
