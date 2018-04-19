# -*- coding: utf-8 -*-
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

from decimal import Decimal
from kudu.compat import unittest
from kudu.client import Partitioning
from kudu.tests.common import KuduTestBase
import kudu
import datetime
import pytz

class TestScanBase(KuduTestBase, unittest.TestCase):

    @classmethod
    def setUpClass(self):
        """
        Parent class for both the Scan tests and the
        Scan Token tests
        """
        super(TestScanBase, self).setUpClass()

        self.nrows = 100
        table = self.client.table(self.ex_table)
        session = self.client.new_session()

        tuples = []
        for i in range(self.nrows):
            op = table.new_insert()
            tup = i, \
                  i * 2, \
                  'hello_%d' % i if i % 2 == 0 else None, \
                  datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
            op['key'] = tup[0]
            op['int_val'] = tup[1]
            if i % 2 == 0:
                op['string_val'] = tup[2]
            op['unixtime_micros_val'] = tup[3]
            session.apply(op)
            tuples.append(tup)
        session.flush()

        self.table = table
        self.tuples = []

        # Replace missing values w/ defaults to test default values.
        for tuple in tuples:
            if tuple[2] == None:
                tuple = (tuple[0], tuple[1], 'nothing', tuple[3])
            self.tuples.append(tuple)

        # Create table to test all types
        # for various predicate tests
        table_name = 'type-test'
        # Create schema, partitioning and then table
        builder = kudu.schema_builder()
        builder.add_column('key').type(kudu.int64).nullable(False)
        builder.add_column('unixtime_micros_val', type_=kudu.unixtime_micros, nullable=False)
        if kudu.CLIENT_SUPPORTS_DECIMAL:
            builder.add_column('decimal_val', type_=kudu.decimal, precision=5, scale=2)
        builder.add_column('string_val', type_=kudu.string, compression=kudu.COMPRESSION_LZ4, encoding='prefix')
        builder.add_column('bool_val', type_=kudu.bool)
        builder.add_column('double_val', type_=kudu.double)
        builder.add_column('int8_val', type_=kudu.int8)
        builder.add_column('binary_val', type_='binary', compression=kudu.COMPRESSION_SNAPPY, encoding='prefix')
        builder.add_column('float_val', type_=kudu.float)
        builder.set_primary_keys(['key', 'unixtime_micros_val'])
        schema = builder.build()

        self.projected_names_w_o_float = [
            col for col in schema.names if col != 'float_val'
        ]

        partitioning = Partitioning() \
            .add_hash_partitions(column_names=['key'], num_buckets=3)\
            .set_range_partition_columns(['unixtime_micros_val'])\
            .add_range_partition(
                upper_bound={'unixtime_micros_val': ("2016-01-01", "%Y-%m-%d")},
                upper_bound_type=kudu.EXCLUSIVE_BOUND
            )\
            .add_range_partition(
                lower_bound={'unixtime_micros_val': datetime.datetime(2016, 1, 1)},
                lower_bound_type='INCLUSIVE',
                upper_bound={'unixtime_micros_val': datetime.datetime(9999, 12, 31)}
            )


        self.client.create_table(table_name, schema, partitioning)
        self.type_table = self.client.table(table_name)

        # Insert new rows
        if kudu.CLIENT_SUPPORTS_DECIMAL:
            self.type_test_rows = [
                (1, datetime.datetime(2016, 1, 1).replace(tzinfo=pytz.utc), Decimal('111.11'),
                 "Test One", True, 1.7976931348623157 * (10^308), 127,
                 b'\xce\x99\xce\xbf\xcf\x81\xce\xb4\xce\xb1\xce\xbd\xce\xaf\xce\xb1',
                 3.402823 * (10^38)),
                (2, datetime.datetime.utcnow().replace(tzinfo=pytz.utc), Decimal('0.99'),
                 "测试二", False, 200.1, -1,
                 b'\xd0\x98\xd0\xbe\xd1\x80\xd0\xb4\xd0\xb0\xd0\xbd\xd0\xb8\xd1\x8f',
                 -150.2)
            ]
        else:
            self.type_test_rows = [
                (1, datetime.datetime(2016, 1, 1).replace(tzinfo=pytz.utc),
                 "Test One", True, 1.7976931348623157 * (10 ^ 308), 127,
                 b'\xce\x99\xce\xbf\xcf\x81\xce\xb4\xce\xb1\xce\xbd\xce\xaf\xce\xb1',
                 3.402823 * (10 ^ 38)),
                (2, datetime.datetime.utcnow().replace(tzinfo=pytz.utc),
                 "测试二", False, 200.1, -1,
                 b'\xd0\x98\xd0\xbe\xd1\x80\xd0\xb4\xd0\xb0\xd0\xbd\xd0\xb8\xd1\x8f',
                 -150.2)
            ]
        session = self.client.new_session()
        for row in self.type_test_rows:
            op = self.type_table.new_insert(row)
            session.apply(op)
        session.flush()

        # Remove the float values from the type_test_rows tuples so we can
        # compare the other vals
        self.type_test_rows = [
            tuple[:-1] for tuple in self.type_test_rows
        ]

    def setUp(self):
        pass

    def insert_new_unixtime_micros_rows(self):
        # Get current UTC datetime to be used for read at snapshot test
        # Not using the easter time value below as that may not be the
        # actual local timezone of the host executing this test and as
        # such does would not accurately be offset to UTC
        self.snapshot_timestamp = datetime.datetime.utcnow()

        # Insert new rows
        # Also test a timezone other than UTC to confirm that
        # conversion to UTC is properly applied
        eastern = datetime.datetime.now()\
            .replace(tzinfo=pytz.timezone("America/New_York"))
        rows = [[100, "2016-09-14T23:11:32.432019"],
                [101, ("2016-09-15", "%Y-%m-%d")],
                [102, eastern]]
        session = self.client.new_session()
        for row in rows:
            op = self.table.new_insert()
            list = [row[0],
                    row[0]*2,
                    'hello_%d' % row[0] if row[0] % 2 == 0 else None,
                    row[1]]
            for i, val in enumerate(list):
                op[i] = val
            session.apply(op)
            # convert datetime if needed to validate rows
            if not isinstance(list[3], datetime.datetime):
                if type(list[3]) is tuple:
                    list[3] = datetime.datetime \
                        .strptime(list[3][0], list[3][1])
                else:
                    list[3] = datetime.datetime \
                        .strptime(list[3], "%Y-%m-%dT%H:%M:%S.%f")
            else:
                # Convert Eastern Time datetime to UTC for confirmation
                list[3] -= list[3].utcoffset()
            # Apply timezone
            list[3] = list[3].replace(tzinfo=pytz.utc)
            self.tuples.append(tuple(list))
        session.flush()

    def delete_insert_row_for_read_test(self):

        # Retrive row to delete so it can be reinserted into the table so
        # that other tests do not fail
        row = self.table.scanner()\
                    .set_fault_tolerant()\
                    .open()\
                    .read_all_tuples()[0]

        # Delete row from table
        session = self.client.new_session()
        op = self.table.new_delete()
        op['key'] = row[0]
        session.apply(op)
        session.flush()

        # Get latest observed timestamp for snapshot
        self.snapshot_timestamp = self.client.latest_observed_timestamp()

        # Insert row back into table so that other tests don't fail.
        session = self.client.new_session()
        op = self.table.new_insert()
        for idx, val in enumerate(row):
            op[idx] = val
        session.apply(op)
        session.flush()

    def _test_unixtime_micros_pred(self):
        # Test unixtime_micros value predicate
        self.verify_pred_type_scans(
            preds=[
                self.type_table['unixtime_micros_val'] == ("2016-01-01", "%Y-%m-%d")
            ],
            row_indexes=slice(0,1)
        )

    def _test_bool_pred(self):
        # Test a boolean value predicate
        self.verify_pred_type_scans(
            preds=[
                self.type_table['bool_val'] == False
            ],
            row_indexes=slice(1,2)
        )

    def _test_double_pred(self):
        # Test a double precision float predicate
        self.verify_pred_type_scans(
            preds=[
                self.type_table['double_val'] < 200.11
            ],
            row_indexes=slice(1,2)
        )

    def _test_float_pred(self):
        self.verify_pred_type_scans(
            preds=[
                self.type_table['float_val'] == 3.402823 * (10^38)
            ],
            row_indexes=slice(0, 1),
            count_only=True
        )

    def _test_decimal_pred(self):
        if kudu.CLIENT_SUPPORTS_DECIMAL:
            self.verify_pred_type_scans(
                preds=[
                    self.type_table['decimal_val'] == Decimal('111.11')
                ],
                row_indexes=slice(0, 1),
            )

    def _test_binary_pred(self):
        self.verify_pred_type_scans(
            preds=[
                self.type_table['binary_val'] == 'Иордания'
            ],
            row_indexes=slice(1, 2)
        )
