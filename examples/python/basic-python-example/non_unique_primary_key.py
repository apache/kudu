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
from kudu import Schema
from kudu.client import Partitioning

# Pretty print scan results according to the schema
def print_tuple(t):
  print('(non_unique_key: {0}, {1}: {2}, int_val: {3})'.format(t[0],\
        Schema.get_auto_incrementing_column_name(), t[1], t[2]))

def print_rows(scanner):
  scanner = scanner.open()
  # The rows contains the auto-incrementing column. If one doesn't requre it, it can be
  # discarded through a projection.
  while scanner.has_more_rows():
    for t in scanner.read_next_batch_tuples():
      print_tuple(t)

def update_rows(client, table, scanner, new_val):
  # It's necessary to specify the entire set of key columns when updating a particular row.
  # An auto-incrementing column is auto-populated at the server side, and one way to retrieve
  # its values is scanning the table with a projection that includes the auto-incrementing column.
  session = client.new_session()
  while scanner.has_more_rows():
    for t in scanner.read_next_batch_tuples():
      op = table.new_update()
      op['non_unique_key'] = t[0]
      op[Schema.get_auto_incrementing_column_name()] = t[1]
      op['int_val'] =  new_val
      session.apply(op)

  try:
    session.flush()
  except kudu.KuduBadStatus:
    print(session.get_pending_errors())

def delete_rows(client, table, scanner):
  # It's necessary to specify the entire set of key columns when updating a particular row.
  # An auto-incrementing column is auto-populated at the server side, and one way to retrieve
  # its values is scanning the table with a projection that includes the auto-incrementing column.
  session = client.new_session()
  while scanner.has_more_rows():
    for t in scanner.read_next_batch_tuples():
      op = table.new_delete()
      op['non_unique_key'] = t[0]
      op[Schema.get_auto_incrementing_column_name()] = t[1]
      session.apply(op)

  try:
    session.flush()
  except kudu.KuduBadStatus:
    print(session.get_pending_errors())

# Parse arguments
parser = argparse.ArgumentParser(description='Basic Example for Kudu Python.')
parser.add_argument('--masters', '-m', nargs='+', default='localhost',
                    help='The master address(es) to connect to Kudu.')
parser.add_argument('--ports', '-p', nargs='+', default='7051',
                    help='The master server port(s) to connect to Kudu.')
args = parser.parse_args()

client = kudu.connect(host=args.masters, port=args.ports)

builder = kudu.schema_builder()
# Columns which are not uniquely identifiable can still be used as primary keys by
# specifying them as non-unique primary key.
builder.add_column('non_unique_key').type(kudu.int32).nullable(False).non_unique_primary_key()
builder.add_column('int_val', type_=kudu.int32, nullable=False)
schema = builder.build()

# The schema stringification shows the presence of the auto-incrementing column,
# and the resulting composite primary key.
print(schema)

partitioning = Partitioning().add_hash_partitions(column_names=['non_unique_key'], num_buckets=2)

table_name = 'non_unique_key-example'
if client.table_exists(table_name):
  client.delete_table(table_name)
client.create_table(table_name, schema, partitioning)

table = client.table(table_name)
session = client.new_session()
stale_counter = 0
num_rows = 10
divisor = 3
for i in range(num_rows):
  if i % divisor == 0:
    stale_counter += 1

  op = table.new_insert()
  # The auto-incrementing column is populated on the server-side automatically.
  op['non_unique_key'] = stale_counter
  op['int_val'] = i % divisor
  session.apply(op)

try:
  session.flush()
except kudu.KuduBadStatus:
  print(session.get_pending_errors())

print('Demonstrating scanning ...')
scanner = table.scanner()
print_rows(scanner)

non_unique_key_equals = 1
print('Scanned some row(s) WHERE non_unique_key = {0}'\
  .format(non_unique_key_equals))
scanner = table.scanner()
scanner.add_predicate(table['non_unique_key'] == non_unique_key_equals).open()
print_rows(scanner)

print('Demonstrating UPDATE ...')
# Updating based upon a predicate on a non-unique PK and on a non-PK column
non_unique_key_equals = 1
int_val_equals = 2
new_val = 98
scanner = table.scanner()
scanner.add_predicate(table['non_unique_key'] == non_unique_key_equals)\
  .add_predicate(table['int_val'] == int_val_equals)\
  .open()
update_rows(client, table, scanner, new_val)

print('Updated row(s) WHERE non_unique_key = {0} AND int_val = {1} to int_val = {2}'\
  .format(non_unique_key_equals, int_val_equals, new_val))
scanner = table.scanner()
print_rows(scanner)

# Updating based upon a predicate on a non-unique PK
non_unique_key_equals = 2
new_val = 99
scanner = table.scanner()
scanner.add_predicate(table['non_unique_key'] == non_unique_key_equals).open()
update_rows(client, table, scanner, new_val)

print('Updated row(s) WHERE non_unique_key = {0} to int_val = {1}'\
  .format(non_unique_key_equals, new_val))
scanner = table.scanner()
print_rows(scanner)

# Updating based upon a predicate on a non-unique PK and on the auto-incrementing column
non_unique_key_equals = 2
auto_incrementing_counter_val = 5
new_val = 100
scanner = table.scanner()
scanner.add_predicate(table['non_unique_key'] == non_unique_key_equals)\
  .add_predicate(table[Schema.get_auto_incrementing_column_name()] ==\
                 auto_incrementing_counter_val)\
  .open()
update_rows(client, table, scanner, new_val)

print('Updated row(s) WHERE non_unique_key = {0} AND {1} = {2} to int_val = {3}'\
  .format(non_unique_key_equals, Schema.get_auto_incrementing_column_name(),\
          auto_incrementing_counter_val, new_val))
scanner = table.scanner()
print_rows(scanner)

print('Demonstrating DELETE ...')
# Deleting based upon a predicate on a non-unique PK and on a non-PK column
non_unique_key_equals = 3
int_val_equals = 1
scanner = table.scanner()
scanner.add_predicate(table['non_unique_key'] == non_unique_key_equals)\
  .add_predicate(table['int_val'] == int_val_equals)\
  .open()
delete_rows(client, table, scanner)

print('Deleted row(s) WHERE non_unique_key = {0} AND int_val = {1}'\
  .format(non_unique_key_equals, int_val_equals))
scanner = table.scanner()
print_rows(scanner)

# Deleting based upon a predicate on a non-unique PK
non_unique_key_equals = 2
scanner = table.scanner()
scanner.add_predicate(table['non_unique_key'] == non_unique_key_equals)\
  .open()
delete_rows(client, table, scanner)

print('Deleted row(s) WHERE non_unique_key = {0}'\
  .format(non_unique_key_equals, int_val_equals))
scanner = table.scanner()
print_rows(scanner)

# Deleting based upon a predicate on a non-unique PK and on the auto-incrementing column
non_unique_key_equals = 3
auto_incrementing_counter_val = 3
scanner = table.scanner()
scanner.add_predicate(table['non_unique_key'] == non_unique_key_equals)\
  .add_predicate(table[Schema.get_auto_incrementing_column_name()] ==\
                 auto_incrementing_counter_val)\
  .open()
delete_rows(client, table, scanner)

print('Deleted row(s) WHERE non_unique_key = {0} AND {1} = {2}'\
  .format(non_unique_key_equals, Schema.get_auto_incrementing_column_name(),\
          auto_incrementing_counter_val))
scanner = table.scanner()
print_rows(scanner)

client.delete_table(table_name)
print('Deleted the table')
print('Done')