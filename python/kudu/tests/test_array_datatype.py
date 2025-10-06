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

import kudu
from kudu.compat import CompatUnitTest
from kudu.tests.common import KuduTestBase
from kudu.client import Partitioning
import datetime
from decimal import Decimal
from pytz import utc

class TestArrayDataTypeIntegration(KuduTestBase, CompatUnitTest):

    # All array types supported by Python client
    SUPPORTED_ARRAY_TYPES = [
        ('int8', kudu.int8),
        ('int16', kudu.int16),
        ('int32', kudu.int32),
        ('int64', kudu.int64),
        ('float', kudu.float_),
        ('double', kudu.double),
        ('bool', kudu.bool),
        ('string', kudu.string),
        ('binary', kudu.binary),
        ('unixtime_micros', kudu.unixtime_micros),
        ('date', kudu.date),
    ]

    # Types that require special parameters
    SPECIAL_PARAM_TYPES = [
        ('varchar', kudu.varchar, {'length': 50}),
        ('decimal', kudu.decimal, {'precision': 8, 'scale': 2}),
    ]

    @classmethod
    def setUpClass(cls):
        super(TestArrayDataTypeIntegration, cls).setUpClass()

        builder = kudu.schema_builder()
        builder.add_column('id', kudu.int32, nullable=False).primary_key()

        # Add basic array types
        for type_name, kudu_type in cls.SUPPORTED_ARRAY_TYPES:
            col_name = 'arr_' + type_name
            builder.add_column(col_name).nested_type(kudu.array_type(kudu_type))

        # Add special parameter types
        for type_name, kudu_type, params in cls.SPECIAL_PARAM_TYPES:
            col_name = 'arr_' + type_name
            col_spec = builder.add_column(col_name).nested_type(kudu.array_type(kudu_type))
            if 'length' in params:
                col_spec.length(params['length'])
            if 'precision' in params:
                col_spec.precision(params['precision'])
            if 'scale' in params:
                col_spec.scale(params['scale'])

        cls.array_schema = builder.build()
        cls.partitioning = Partitioning().set_range_partition_columns(['id'])

    def setUp(self):
        self.table_name = 'array-datatype-test-table'

        if self.client.table_exists(self.table_name):
            self.client.delete_table(self.table_name)

        self.client.create_table(self.table_name, self.array_schema, self.partitioning)

    def tearDown(self):
        if self.client.table_exists(self.table_name):
            self.client.delete_table(self.table_name)

    def _get_test_data_for_all_types(self, num_values=2, include_null=True, base_value=1):
        """
        Generate test data for all array types.
        Args:
            num_values: Number of non-NULL values to generate
            include_null: Whether to include a NULL value at the end
            base_value: Base value to use for generating data
        """
        from pytz import utc

        values = []
        for i in range(num_values):
            values.append(base_value + i)
        if include_null:
            values.append(None)
        return [
            ('arr_int8', [v if v is None else int(v % 100) for v in values]),
            ('arr_int16', [v if v is None else int(v * 10) for v in values]),
            ('arr_int32', [v if v is None else int(v * 100) for v in values]),
            ('arr_int64', [v if v is None else int(v * 1000) for v in values]),
            ('arr_float', [v if v is None else float(v + 0.5) for v in values]),
            ('arr_double', [v if v is None else float(v + 0.1) for v in values]),
            ('arr_bool', [v if v is None else bool(v % 2) for v in values]),
            ('arr_string', [v if v is None else 'text{0}'.format(v) for v in values]),
            ('arr_binary', [v if v is None else 'data{0}'.format(v).encode() for v in values]),
            ('arr_unixtime_micros', [v if v is None else datetime.datetime(2020, 1, min(v, 28), tzinfo=utc) for v in values]),
            ('arr_date', [v if v is None else datetime.date(2020, 1, min(v, 28)) for v in values]),
            ('arr_varchar', [v if v is None else 'varchar{0}'.format(v) for v in values]),
            ('arr_decimal', [None if v is None else Decimal(v / 100).quantize(Decimal('0.01')) for v in values]),
        ]

    def test_insert_all_array_types(self):
        table = self.client.table(self.table_name)
        session = self.client.new_session()

        insert = table.new_insert()
        insert['id'] = 1

        test_data = self._get_test_data_for_all_types()
        for col_name, data in test_data:
            insert[col_name] = data

        session.apply(insert)
        session.flush()

        scanner = table.scanner()
        scanner.add_predicate(table['id'] == 1)
        scanner.open()
        tuples = scanner.read_all_tuples()

        self.assertEqual(len(tuples), 1)
        row = tuples[0]
        self.assertEqual(row[0], 1)

        for idx, (col_name, expected_data) in enumerate(test_data, start=1):
            actual_data = row[idx]
            self.assertEqual(actual_data, expected_data,
                           "Array {0} data mismatch".format(col_name))

    def test_update_all_array_types(self):
        table = self.client.table(self.table_name)
        session = self.client.new_session()

        insert = table.new_insert()
        insert['id'] = 1
        initial_data = self._get_test_data_for_all_types()
        for col_name, data in initial_data:
            insert[col_name] = data
        session.apply(insert)
        session.flush()

        update = table.new_update()
        update['id'] = 1

        updated_data = self._get_test_data_for_all_types(num_values=2, include_null=True, base_value=10)
        for col_name, data in updated_data:
            update[col_name] = data

        session.apply(update)
        session.flush()

        scanner = table.scanner()
        scanner.add_predicate(table['id'] == 1)
        scanner.open()
        tuples = scanner.read_all_tuples()

        self.assertEqual(len(tuples), 1)
        row = tuples[0]

        for idx, (col_name, expected_data) in enumerate(updated_data, start=1):
            actual_data = row[idx]
            self.assertEqual(actual_data, expected_data,
                           "Updated array {0} data mismatch".format(col_name))

    def test_upsert_all_array_types(self):
        table = self.client.table(self.table_name)
        session = self.client.new_session()

        upsert = table.new_upsert()
        upsert['id'] = 1

        upsert_data = self._get_test_data_for_all_types()
        for col_name, data in upsert_data:
            upsert[col_name] = data

        session.apply(upsert)
        session.flush()

        scanner = table.scanner()
        scanner.add_predicate(table['id'] == 1)
        scanner.open()
        tuples = scanner.read_all_tuples()

        self.assertEqual(len(tuples), 1)
        row = tuples[0]

        for idx, (col_name, expected_data) in enumerate(upsert_data, start=1):
            actual_data = row[idx]
            self.assertEqual(actual_data, expected_data,
                           "Upserted array {0} data mismatch".format(col_name))

    def test_delete_with_arrays(self):
        table = self.client.table(self.table_name)
        session = self.client.new_session()

        for row_id in [1, 2, 3]:
            insert = table.new_insert()
            insert['id'] = row_id
            insert['arr_int64'] = [row_id * 10, row_id * 20]
            insert['arr_string'] = ['row', str(row_id)]
            insert['arr_double'] = [row_id * 1.1]
            insert['arr_bool'] = [True]
            remaining_types = ['int8', 'int16', 'int32', 'float', 'binary', 'unixtime_micros', 'date', 'decimal']
            for type_name in remaining_types:
                col_name = 'arr_' + type_name
                insert[col_name] = []
            insert['arr_varchar'] = []
            session.apply(insert)
        session.flush()

        scanner = table.scanner()
        scanner.open()
        all_tuples = scanner.read_all_tuples()
        self.assertEqual(len(all_tuples), 3)

        delete = table.new_delete()
        delete['id'] = 2
        session.apply(delete)
        session.flush()

        scanner = table.scanner()
        scanner.open()
        remaining_tuples = scanner.read_all_tuples()
        self.assertEqual(len(remaining_tuples), 2)

        remaining_ids = [row[0] for row in remaining_tuples]
        self.assertIn(1, remaining_ids)
        self.assertIn(3, remaining_ids)
        self.assertNotIn(2, remaining_ids)

    def test_insert_and_scan_empty_arrays_all_types(self):
        table = self.client.table(self.table_name)
        session = self.client.new_session()

        insert = table.new_insert()
        insert['id'] = 1

        for type_name, kudu_type in self.SUPPORTED_ARRAY_TYPES:
            col_name = 'arr_' + type_name
            insert[col_name] = []
        insert['arr_varchar'] = []
        insert['arr_decimal'] = []

        session.apply(insert)
        session.flush()

        scanner = table.scanner()
        scanner.add_predicate(table['id'] == 1)
        scanner.open()
        tuples = scanner.read_all_tuples()

        self.assertEqual(len(tuples), 1)
        row = tuples[0]
        self.assertEqual(row[0], 1)  # id

        for idx, (type_name, kudu_type) in enumerate(self.SUPPORTED_ARRAY_TYPES, start=1):
            if type_name != 'bool':
                self.assertEqual(row[idx], [], "arr_{0} should be empty".format(type_name))

        special_start_idx = len(self.SUPPORTED_ARRAY_TYPES) + 1
        for offset, (type_name, kudu_type, params) in enumerate(self.SPECIAL_PARAM_TYPES):
            col_idx = special_start_idx + offset
            self.assertEqual(row[col_idx], [], "arr_{0} should be empty".format(type_name))

    def test_scan_multiple_rows_all_array_types(self):
        table = self.client.table(self.table_name)
        session = self.client.new_session()

        test_rows = {
            1: self._get_test_data_for_all_types(num_values=2, include_null=True, base_value=1),
            2: self._get_test_data_for_all_types(num_values=1, include_null=False, base_value=200),
            3: self._get_test_data_for_all_types(num_values=3, include_null=False, base_value=50),
        }

        for row_id, test_data in test_rows.items():
            insert = table.new_insert()
            insert['id'] = row_id
            for col_name, data in test_data:
                insert[col_name] = data
            session.apply(insert)

        session.flush()

        scanner = table.scanner()
        scanner.open()
        all_tuples = scanner.read_all_tuples()
        self.assertEqual(len(all_tuples), 3)

        for row in all_tuples:
            row_id = row[0]
            self.assertIn(row_id, test_rows, "Unexpected row ID: {0}".format(row_id))
            expected_data = test_rows[row_id]
            for idx, (col_name, expected_array) in enumerate(expected_data, start=1):
                actual_array = row[idx]
                self.assertEqual(actual_array, expected_array,
                               "Row {0} {1} mismatch".format(row_id, col_name))

    def test_decimal32_array(self):
        builder = kudu.schema_builder()
        builder.add_column('key', kudu.int32, nullable=False).primary_key()
        builder.add_column('decimal32_arr').nested_type(kudu.array_type(kudu.decimal)).precision(9).scale(4)

        schema = builder.build()
        partitioning = Partitioning().set_range_partition_columns(['key'])
        table_name = 'decimal32-array-test'

        if self.client.table_exists(table_name):
            self.client.delete_table(table_name)

        self.client.create_table(table_name, schema, partitioning)
        table = self.client.table(table_name)
        session = self.client.new_session()

        mixed_values = [
            Decimal('12345.6789'),
            Decimal('0.0001'),
            Decimal('-9999.9999'),
            None,
            Decimal('0.0000'),
        ]
        all_nulls = [None, None, None]

        insert1 = table.new_insert()
        insert1['key'] = 1
        insert1['decimal32_arr'] = mixed_values
        session.apply(insert1)

        insert2 = table.new_insert()
        insert2['key'] = 2
        insert2['decimal32_arr'] = all_nulls
        session.apply(insert2)

        session.flush()

        scanner = table.scanner().open()
        tuples = scanner.read_all_tuples()
        self.assertEqual(len(tuples), 2)

        for row in tuples:
            if row[0] == 1:
                self.assertEqual(row[1], mixed_values)
            elif row[0] == 2:
                self.assertEqual(row[1], all_nulls)

        self.client.delete_table(table_name)

    def test_decimal64_array(self):
        builder = kudu.schema_builder()
        builder.add_column('key', kudu.int32, nullable=False).primary_key()
        builder.add_column('decimal64_arr').nested_type(kudu.array_type(kudu.decimal)).precision(18).scale(6)

        schema = builder.build()
        partitioning = Partitioning().set_range_partition_columns(['key'])
        table_name = 'decimal64-array-test'

        if self.client.table_exists(table_name):
            self.client.delete_table(table_name)

        self.client.create_table(table_name, schema, partitioning)
        table = self.client.table(table_name)
        session = self.client.new_session()

        mixed_values = [
            Decimal('123456789012.123456'),
            Decimal('0.000001'),
            Decimal('-999999999999.999999'),
            None,
            Decimal('1.000000'),
        ]
        all_nulls = [None, None, None, None]

        insert1 = table.new_insert()
        insert1['key'] = 1
        insert1['decimal64_arr'] = mixed_values
        session.apply(insert1)

        insert2 = table.new_insert()
        insert2['key'] = 2
        insert2['decimal64_arr'] = all_nulls
        session.apply(insert2)

        session.flush()

        scanner = table.scanner().open()
        tuples = scanner.read_all_tuples()
        self.assertEqual(len(tuples), 2)

        for row in tuples:
            if row[0] == 1:
                self.assertEqual(row[1], mixed_values)
            elif row[0] == 2:
                self.assertEqual(row[1], all_nulls)

        self.client.delete_table(table_name)
