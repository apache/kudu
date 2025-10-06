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

from __future__ import division

from kudu.compat import CompatUnitTest
from kudu.errors import KuduInvalidArgument
import kudu

from kudu.schema import Schema
import datetime
from decimal import Decimal

class TestSchema(CompatUnitTest):

    def setUp(self):
        self.columns = [('one', 'int32', False),
                        ('two', 'int8', False),
                        ('three', 'double', True),
                        ('four', 'string', False)]

        self.primary_keys = ['one', 'two']

        self.builder = kudu.schema_builder()
        for name, typename, nullable in self.columns:
            self.builder.add_column(name, typename, nullable=nullable)

        self.builder.set_primary_keys(self.primary_keys)
        self.schema = self.builder.build()

    def test_repr(self):
        result = repr(self.schema)
        for name, _, _ in self.columns:
            assert name in result

        assert 'PRIMARY KEY (one, two)' in result

    def test_schema_length(self):
        assert len(self.schema) == 4

    def test_names(self):
        assert self.schema.names == ['one', 'two', 'three', 'four']

    def test_primary_keys(self):
        assert self.schema.primary_key_indices() == [0, 1]
        assert self.schema.primary_keys() == ['one', 'two']

    def test_getitem_boundschecking(self):
        idx = 4
        error_msg = 'Column index {0} is not in range'.format(idx)
        with self.assertRaisesRegex(IndexError, error_msg):
            self.schema[idx]

    def test_getitem_wraparound(self):
        # wraparound
        result = self.schema[-1]
        expected = self.schema[3]

        assert result.equals(expected)

    def test_getitem_string(self):
        result = self.schema['three']
        expected = self.schema[2]

        assert result.equals(expected)

        error_msg = 'not_found'
        with self.assertRaisesRegex(KeyError, error_msg):
            self.schema['not_found']

    def test_schema_equals(self):
        assert self.schema.equals(self.schema)

        builder = kudu.schema_builder()
        builder.add_column('key', 'int64', nullable=False, primary_key=True)
        schema = builder.build()

        assert not self.schema.equals(schema)

    def test_column_equals(self):
        assert not self.schema[0].equals(self.schema[1])

    def test_type(self):
        builder = kudu.schema_builder()
        (builder.add_column('key')
         .type('int32')
         .primary_key()
         .block_size(1048576)
         .nullable(False))
        schema = builder.build()

        tp = schema[0].type
        assert tp.name == 'int32'
        assert tp.type == kudu.schema.INT32

    def test_compression(self):
        builder = kudu.schema_builder()
        builder.add_column('key', 'int64', nullable=False)

        foo = builder.add_column('foo', 'string').compression('lz4')
        assert foo is not None

        bar = builder.add_column('bar', 'string')
        bar.compression(kudu.COMPRESSION_ZLIB)

        compression = 'unknown'
        error_msg = 'Invalid compression type: {0}'.format(compression)
        with self.assertRaisesRegex(ValueError, error_msg):
            bar = builder.add_column('qux', 'string', compression=compression)

        builder.set_primary_keys(['key'])
        builder.build()

        # TODO; The C++ client does not give us an API to see the storage
        # attributes of a column

    def test_encoding(self):
        builder = kudu.schema_builder()
        builder.add_column('key', 'int64', nullable=False)

        available_encodings = ['auto', 'plain', 'prefix', 'bitshuffle',
                               'rle', 'dict', kudu.ENCODING_DICT]
        for enc in available_encodings:
            foo = builder.add_column('foo_%s' % enc, 'string').encoding(enc)
            assert foo is not None
            del foo

        bar = builder.add_column('bar', 'string')
        bar.encoding(kudu.ENCODING_PLAIN)

        error_msg = 'Invalid encoding type'
        with self.assertRaisesRegex(ValueError, error_msg):
            builder.add_column('qux', 'string', encoding='unknown')

        builder.set_primary_keys(['key'])
        builder.build()
        # TODO(wesm): The C++ client does not give us an API to see the storage
        # attributes of a column

    def test_decimal(self):
        builder = kudu.schema_builder()
        (builder.add_column('key')
         .type('decimal')
         .primary_key()
         .nullable(False)
         .precision(9)
         .scale(2))
        schema = builder.build()

        column = schema[0]
        tp = column.type
        assert tp.name == 'decimal'
        assert tp.type == kudu.schema.DECIMAL
        ta = column.type_attributes
        assert ta.precision == 9
        assert ta.scale == 2

    def test_decimal_without_precision(self):
        builder = kudu.schema_builder()
        (builder.add_column('key')
         .type('decimal')
         .primary_key()
         .nullable(False))

        error_msg = 'no precision provided for DECIMAL column: key'
        with self.assertRaisesRegex(kudu.KuduInvalidArgument, error_msg):
            builder.build()

    def test_precision_on_non_decimal_column(self):
        builder = kudu.schema_builder()
        (builder.add_column('key')
         .type('int32')
         .primary_key()
         .nullable(False)
         .precision(9)
         .scale(2))

        error_msg = 'precision is not applicable to INT32 column: key'
        with self.assertRaisesRegex(kudu.KuduInvalidArgument, error_msg):
            builder.build()

    def test_date(self):
        builder = kudu.schema_builder()
        (builder.add_column('key')
         .type('date')
         .primary_key()
         .nullable(False))
        schema = builder.build()

        column = schema[0]
        tp = column.type
        assert tp.name == 'date'
        assert tp.type == kudu.schema.DATE

    def test_varchar(self):
        builder = kudu.schema_builder()
        (builder.add_column('key')
         .type('varchar')
         .primary_key()
         .nullable(False)
         .length(10))
        schema = builder.build()

        column = schema[0]
        tp = column.type
        assert tp.name == 'varchar'
        assert tp.type == kudu.schema.VARCHAR
        ta = column.type_attributes
        assert ta.length == 10

    def test_varchar_without_length(self):
        builder = kudu.schema_builder()
        (builder.add_column('key')
         .type('varchar')
         .primary_key()
         .nullable(False))

        error_msg = 'no length provided for VARCHAR column: key'
        with self.assertRaisesRegex(kudu.KuduInvalidArgument, error_msg):
            builder.build()

    def test_varchar_invalid_length(self):
        builder = kudu.schema_builder()
        (builder.add_column('key')
         .type('varchar')
         .primary_key()
         .length(0)
         .nullable(False))

        error_msg = 'length must be between 1 and 65535: key'
        with self.assertRaisesRegex(kudu.KuduInvalidArgument, error_msg):
            builder.build()

    def test_length_on_non_varchar_column(self):
        builder = kudu.schema_builder()
        (builder.add_column('key')
         .type('decimal')
         .primary_key()
         .nullable(False)
         .length(10))

        error_msg = 'no precision provided for DECIMAL column: key'
        with self.assertRaisesRegex(kudu.KuduInvalidArgument, error_msg):
            builder.build()

    def test_unsupported_col_spec_methods_for_create_table(self):
        builder = kudu.schema_builder()
        builder.add_column('test', 'int64').rename('test')
        error_msg = 'cannot rename a column during CreateTable: test'
        with self.assertRaisesRegex(kudu.KuduNotSupported, error_msg):
            builder.build()

        builder.add_column('test', 'int64').remove_default()
        error_msg = 'cannot rename a column during CreateTable: test'
        with self.assertRaisesRegex(kudu.KuduNotSupported, error_msg):
            builder.build()

    def test_set_column_spec_pk(self):
        builder = kudu.schema_builder()
        key = (builder.add_column('key', 'int64', nullable=False)
               .primary_key())
        assert key is not None
        schema = builder.build()
        assert 'key' in schema.primary_keys()

        builder = kudu.schema_builder()
        key = (builder.add_column('key', 'int64', nullable=False,
                                  primary_key=True))
        schema = builder.build()
        assert 'key' in schema.primary_keys()

    def test_partition_schema(self):
        pass

    def test_nullable_not_null(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64', nullable=False)
         .primary_key())

        builder.add_column('data1', 'double').nullable(True)
        builder.add_column('data2', 'double').nullable(False)
        builder.add_column('data3', 'double', nullable=True)
        builder.add_column('data4', 'double', nullable=False)

        schema = builder.build()

        assert not schema[0].nullable
        assert schema[1].nullable
        assert not schema[2].nullable

        assert schema[3].nullable
        assert not schema[4].nullable

    def test_mutable_immutable(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64', nullable=False)
         .primary_key())

        builder.add_column('data1', 'double').mutable(True)
        builder.add_column('data2', 'double').mutable(False)

        schema = builder.build()

        assert schema[0].mutable
        assert schema[1].mutable
        assert not schema[2].mutable

    def test_column_comment(self):
        comment = "test_comment"
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64', nullable=False)
         .primary_key()
         .comment(comment))

        builder.add_column('data1', 'double').nullable(True)
        schema = builder.build()
        assert isinstance(schema[0].comment, str)
        assert len(schema[0].comment) > 0
        assert schema[0].comment == comment
        assert isinstance(schema[1].comment, str)
        assert len(schema[1].comment) == 0

    def test_auto_incrementing_column_name(self):
        name = Schema.get_auto_incrementing_column_name()
        assert isinstance(name, str)
        assert len(name) > 0

    def test_non_unique_primary_key(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64')
         .nullable(False)
         .non_unique_primary_key())
        builder.add_column('data1', 'double')
        schema = builder.build()
        assert len(schema) == 3
        assert len(schema.primary_keys()) == 2
        assert Schema.get_auto_incrementing_column_name() in schema.primary_keys()

    def test_set_non_unique_primary_keys(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64')
         .nullable(False))
        builder.add_column('data1', 'double')
        builder.set_non_unique_primary_keys(['key'])
        schema = builder.build()
        assert len(schema) == 3
        assert len(schema.primary_keys()) == 2
        assert Schema.get_auto_incrementing_column_name() in schema.primary_keys()

    def test_set_non_unique_primary_keys_wrong_order(self):
        builder = kudu.schema_builder()
        builder.add_column('key1', 'int64').nullable(False)
        builder.add_column('key2', 'double').nullable(False)
        builder.set_non_unique_primary_keys(['key2', 'key1'])
        error_msg = 'primary key columns must be listed first in the schema: key'
        with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
            schema = builder.build()

    def test_set_non_unique_primary_keys_not_first(self):
        builder = kudu.schema_builder()
        builder.add_column('data1', 'double')
        (builder.add_column('key', 'int64')
         .nullable(False))
        builder.set_non_unique_primary_keys(['key'])
        error_msg = 'primary key columns must be listed first in the schema: key'
        with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
            schema = builder.build()

    def test_set_non_unique_primary_keys_same_name_twice(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64')
         .nullable(False))
        builder.add_column('data1', 'double')
        builder.set_non_unique_primary_keys(['key', 'key'])
        error_msg = 'primary key columns must be listed first in the schema: key'
        with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
            schema = builder.build()

    def test_unique_and_non_unique_primary_key_on_same_column(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64')
         .nullable(False)
         .primary_key()
         .non_unique_primary_key())
        builder.add_column('data1', 'double')
        schema = builder.build()
        assert len(schema) == 3
        assert len(schema.primary_keys()) == 2
        assert Schema.get_auto_incrementing_column_name() in schema.primary_keys()

    def test_non_unique_and_unique_primary_key_on_same_column(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64')
         .nullable(False)
         .non_unique_primary_key()
         .primary_key())
        builder.add_column('data1', 'double')
        schema = builder.build()
        assert len(schema) == 2
        assert len(schema.primary_keys()) == 1
        assert Schema.get_auto_incrementing_column_name() not in schema.primary_keys()

    def test_non_unique_primary_key_not_first(self):
        builder = kudu.schema_builder()
        builder.add_column('data1', 'int64')
        (builder.add_column('key', 'double')
         .nullable(False)
         .non_unique_primary_key())
        error_msg = 'primary key column must be the first column'
        with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
            builder.build()

    def test_unique_and_non_unique_primary_key_on_different_cols(self):
        builder = kudu.schema_builder()
        (builder.add_column('key1', 'double')
         .nullable(False)
         .primary_key())
        (builder.add_column('key2', 'double')
         .nullable(False)
         .non_unique_primary_key())
        error_msg = 'multiple columns specified for primary key: key1, key2'
        with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
            builder.build()

    def test_non_unique_and_unique_primary_key_on_different_cols(self):
        builder = kudu.schema_builder()
        (builder.add_column('key1', 'double')
         .nullable(False)
         .non_unique_primary_key())
        (builder.add_column('key2', 'double')
         .nullable(False)
         .primary_key())
        error_msg = 'multiple columns specified for primary key: key1, key2'
        with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
            builder.build()

    def test_multiple_non_unique_primary_keys(self):
        builder = kudu.schema_builder()
        (builder.add_column('key1', 'double')
         .nullable(False)
         .non_unique_primary_key())
        (builder.add_column('key2', 'double')
         .nullable(False)
         .non_unique_primary_key())
        error_msg = 'multiple columns specified for primary key: key1, key2'
        with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
            builder.build()

    def test_non_unique_primary_key_and_set_non_unique_primary_keys(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64')
         .nullable(False)
         .non_unique_primary_key())
        builder.add_column('data1', 'double')
        builder.set_non_unique_primary_keys(['key'])
        error_msg = ('primary key specified by both SetNonUniquePrimaryKey\(\)'
                     ' and on a specific column: key')
        with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
            builder.build()

    def test_primary_key_and_set_non_unique_primary_keys(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64')
         .nullable(False)
         .primary_key())
        builder.add_column('data1', 'double')
        builder.set_non_unique_primary_keys(['key'])
        error_msg = ('primary key specified by both SetNonUniquePrimaryKey\(\)'
                     ' and on a specific column: key')
        with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
            builder.build()

    def test_primary_key_and_set_primary_keys(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64')
         .nullable(False)
         .primary_key())
        builder.add_column('data1', 'double')
        builder.set_primary_keys(['key'])
        error_msg = ('primary key specified by both SetPrimaryKey\(\)'
                     ' and on a specific column: key')
        with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
            builder.build()

    def test_non_unique_primary_key_and_set_primary_keys(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64')
         .nullable(False)
         .non_unique_primary_key())
        builder.add_column('data1', 'double')
        builder.set_primary_keys(['key'])
        error_msg = ('primary key specified by both SetPrimaryKey\(\)'
                     ' and on a specific column: key')
        with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
            builder.build()

    def test_set_non_unique_and_set_unique_primary_key(self):
        builder = kudu.schema_builder()
        builder.add_column('key1', 'int64').nullable(False)
        builder.add_column('key2', 'double').nullable(False)
        builder.set_non_unique_primary_keys(['key1', 'key2'])
        builder.set_primary_keys(['key1', 'key2'])
        schema = builder.build()
        assert len(schema) == 2
        assert len(schema.primary_keys()) == 2
        assert Schema.get_auto_incrementing_column_name() not in schema.primary_keys()

    def test_set_unique_and_set_non_unique_primary_key(self):
        builder = kudu.schema_builder()
        builder.add_column('key1', 'int64').nullable(False)
        builder.add_column('key2', 'double').nullable(False)
        builder.set_primary_keys(['key1', 'key2'])
        builder.set_non_unique_primary_keys(['key1', 'key2'])
        schema = builder.build()
        assert len(schema) == 3
        assert len(schema.primary_keys()) == 3
        assert Schema.get_auto_incrementing_column_name() in schema.primary_keys()

    def test_reserved_column_name(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64')
         .nullable(False)
         .primary_key())
        builder.add_column(Schema.get_auto_incrementing_column_name(), 'double')
        error_msg = 'auto_incrementing_id is a reserved column name'
        with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
            builder.build()

    def test_default_value(self):
        pass

    def test_column_schema_repr(self):
        result = repr(self.schema[0])
        expected = 'ColumnSchema(name=one, type=int32, nullable=False)'
        self.assertEqual(result, expected)


class TestArrayDataTypeSchema(CompatUnitTest):

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

    SPECIAL_PARAM_TYPES = [
        ('varchar', kudu.varchar, {'length': 50}),
        ('decimal', kudu.decimal, {'precision': 8, 'scale': 2}),
    ]

    EXPECTED_ARRAY_TYPE_STRINGS = {
        'arr_int8': 'INT8 1D-ARRAY NULLABLE',
        'arr_int16': 'INT16 1D-ARRAY NULLABLE',
        'arr_int32': 'INT32 1D-ARRAY NULLABLE',
        'arr_int64': 'INT64 1D-ARRAY NULLABLE',
        'arr_float': 'FLOAT 1D-ARRAY NULLABLE',
        'arr_double': 'DOUBLE 1D-ARRAY NULLABLE',
        'arr_bool': 'BOOL 1D-ARRAY NULLABLE',
        'arr_string': 'STRING 1D-ARRAY NULLABLE',
        'arr_binary': 'BINARY 1D-ARRAY NULLABLE',
        'arr_unixtime_micros': 'UNIXTIME_MICROS 1D-ARRAY NULLABLE',
        'arr_date': 'DATE 1D-ARRAY NULLABLE',
        'arr_varchar': 'VARCHAR(50) 1D-ARRAY NULLABLE',
        'arr_decimal': 'DECIMAL(8, 2) 1D-ARRAY NULLABLE',
    }

    def test_array_type_descriptors_all_types(self):
        for type_name, kudu_type in self.SUPPORTED_ARRAY_TYPES:
            arr = kudu.array_type(kudu_type)
            self.assertIsNotNone(arr, "Failed to create array type for {0}".format(type_name))
            self.assertTrue(arr.is_array(), "Array type {0} not recognized".format(type_name))
            self.assertEqual(str(arr), 'NestedTypeDescriptor(type=array)')

        for type_name, kudu_type, params in self.SPECIAL_PARAM_TYPES:
            arr = kudu.array_type(kudu_type)
            self.assertIsNotNone(arr, "Failed to create array type for {0}".format(type_name))
            self.assertTrue(arr.is_array(), "Array type {0} not recognized".format(type_name))
            self.assertEqual(str(arr), 'NestedTypeDescriptor(type=array)')

    def test_comprehensive_array_schema(self):
        builder = kudu.schema_builder()

        builder.add_column('id', kudu.int32, nullable=False).primary_key()
        builder.add_column('name', kudu.string)
        builder.add_column('age', kudu.int32)

        for type_name, kudu_type in self.SUPPORTED_ARRAY_TYPES:
            col_name = 'arr_' + type_name
            builder.add_column(col_name).nested_type(kudu.array_type(kudu_type))

        for type_name, kudu_type, params in self.SPECIAL_PARAM_TYPES:
            col_name = 'arr_' + type_name
            col_spec = builder.add_column(col_name).nested_type(kudu.array_type(kudu_type))
            if 'length' in params:
                col_spec.length(params['length'])
            if 'precision' in params:
                col_spec.precision(params['precision'])
            if 'scale' in params:
                col_spec.scale(params['scale'])

        schema = builder.build()

        # Verify schema structure
        # 3 scalar + 11 basic arrays + 2 special arrays (varchar, decimal)
        self.assertEqual(len(schema), 16)

        # Verify complete schema string representation
        expected_schema_str = """kudu.Schema {
  id                   INT32 NOT NULL
  name                 STRING NULLABLE
  age                  INT32 NULLABLE
  arr_int8             INT8 1D-ARRAY NULLABLE
  arr_int16            INT16 1D-ARRAY NULLABLE
  arr_int32            INT32 1D-ARRAY NULLABLE
  arr_int64            INT64 1D-ARRAY NULLABLE
  arr_float            FLOAT 1D-ARRAY NULLABLE
  arr_double           DOUBLE 1D-ARRAY NULLABLE
  arr_bool             BOOL 1D-ARRAY NULLABLE
  arr_string           STRING 1D-ARRAY NULLABLE
  arr_binary           BINARY 1D-ARRAY NULLABLE
  arr_unixtime_micros  UNIXTIME_MICROS 1D-ARRAY NULLABLE
  arr_date             DATE 1D-ARRAY NULLABLE
  arr_varchar          VARCHAR(50) 1D-ARRAY NULLABLE
  arr_decimal          DECIMAL(8, 2) 1D-ARRAY NULLABLE
  PRIMARY KEY (id)
}"""
        self.assertEqual(str(schema), expected_schema_str)

        self.assertEqual(schema[0].name, 'id')
        self.assertEqual(schema[0].type.name, 'int32')
        self.assertFalse(schema[0].nullable)  # Primary key

        self.assertEqual(schema[1].name, 'name')
        self.assertEqual(schema[1].type.name, 'string')

        self.assertEqual(schema[2].name, 'age')
        self.assertEqual(schema[2].type.name, 'int32')

        # Verify all array columns have nested type and are nullable by default
        # Start at index 3 (after scalars)
        for idx, (type_name, kudu_type) in enumerate(self.SUPPORTED_ARRAY_TYPES, start=3):
            col = schema[idx]
            expected_name = 'arr_' + type_name
            self.assertEqual(col.name, expected_name)
            self.assertEqual(col.type.name, 'nested')
            # Arrays nullable by default
            self.assertTrue(col.nullable)
            # Verify type_to_string matches expected format
            self.assertEqual(col.type_to_string(), self.EXPECTED_ARRAY_TYPE_STRINGS[expected_name])

        # Verify special parameter types (with element type attributes)
        varchar_col = schema[14]
        self.assertEqual(varchar_col.name, 'arr_varchar')
        self.assertEqual(varchar_col.type.name, 'nested')
        self.assertEqual(varchar_col.type_attributes.length, 50)
        self.assertEqual(varchar_col.type_to_string(), 'VARCHAR(50) 1D-ARRAY NULLABLE')

        decimal_col = schema[15]
        self.assertEqual(decimal_col.name, 'arr_decimal')
        self.assertEqual(decimal_col.type.name, 'nested')
        self.assertEqual(decimal_col.type_attributes.precision, 8)
        self.assertEqual(decimal_col.type_attributes.scale, 2)
        self.assertEqual(decimal_col.type_to_string(), 'DECIMAL(8, 2) 1D-ARRAY NULLABLE')

    def test_array_schema_introspection_and_writing(self):
        builder = kudu.schema_builder()
        builder.add_column('key', kudu.int32, nullable=False).primary_key()

        for type_name, kudu_type in self.SUPPORTED_ARRAY_TYPES:
            col_name = 'arr_' + type_name
            builder.add_column(col_name).nested_type(kudu.array_type(kudu_type))

        for type_name, kudu_type, params in self.SPECIAL_PARAM_TYPES:
            col_name = 'arr_' + type_name
            col_spec = builder.add_column(col_name).nested_type(kudu.array_type(kudu_type))
            if 'length' in params:
                col_spec.length(params['length'])
            if 'precision' in params:
                col_spec.precision(params['precision'])
            if 'scale' in params:
                col_spec.scale(params['scale'])

        schema = builder.build()

        row = schema.new_row()
        row['key'] = 1

        test_data = [
            ('arr_int8', [1, 2, None]),
            ('arr_int16', [10, 20, None]),
            ('arr_int32', [100, 200, None]),
            ('arr_int64', [1000, 2000, None]),
            ('arr_float', [1.5, 2.5, None]),
            ('arr_double', [1.1, 2.2, None]),
            ('arr_bool', [True, False, None]),
            ('arr_string', ['hello', 'world', None]),
            ('arr_binary', [b'data1', b'data2', None]),
            ('arr_unixtime_micros', [datetime.datetime(2020, 1, 1), datetime.datetime(2020, 1, 2), None]),
            ('arr_date', [datetime.date(2020, 1, 1), datetime.date(2020, 1, 2), None]),
            ('arr_varchar', ['short', 'text', None]),
            ('arr_decimal', [Decimal('1.23'), Decimal('4.56'), None]),
        ]

        for col_name, data in test_data:
            row[col_name] = data

        self.assertIsNotNone(row)

        row2 = schema.new_row()
        row2['key'] = 2

        empty_test_data = [
            ('arr_int8', []),
            ('arr_int16', []),
            ('arr_int32', []),
            ('arr_int64', []),
            ('arr_float', []),
            ('arr_double', []),
            ('arr_bool', []),
            ('arr_string', []),
            ('arr_binary', []),
            ('arr_unixtime_micros', []),
            ('arr_date', []),
            ('arr_varchar', []),
            ('arr_decimal', []),
        ]

        for col_name, data in empty_test_data:
            row2[col_name] = data

        self.assertIsNotNone(row2)

        row3 = schema.new_row()
        row3['key'] = 3

        null_test_data = [
            ('arr_int8', None),
            ('arr_int16', None),
            ('arr_int32', None),
            ('arr_int64', None),
            ('arr_float', None),
            ('arr_double', None),
            ('arr_bool', None),
            ('arr_string', None),
            ('arr_binary', None),
            ('arr_unixtime_micros', None),
            ('arr_date', None),
            ('arr_varchar', None),
            ('arr_decimal', None),
        ]

        for col_name, data in null_test_data:
            row3[col_name] = data

        self.assertIsNotNone(row3)
