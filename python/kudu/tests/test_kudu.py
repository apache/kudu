#!/usr/bin/env python

# Copyright (c) 2014, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

from __future__ import division

import nose
import unittest
import time

import kudu


def example_schema():
    col1 = kudu.ColumnSchema.create('key', kudu.INT32)
    col2 = kudu.ColumnSchema.create('int_val', kudu.INT32)
    col3 = kudu.ColumnSchema.create('string_val', kudu.STRING)

    return kudu.schema_from_list([col1, col2, col3], 1)


class TestSchema(unittest.TestCase):

    def test_column_schema(self):
        pass

    def test_create_schema(self):
        col1 = kudu.ColumnSchema.create('key', kudu.INT32)
        col2 = kudu.ColumnSchema.create('int_val', kudu.INT32)
        col3 = kudu.ColumnSchema.create('string_val', kudu.STRING)

        cols = [col1, col2, col3]

        # One key column
        schema = kudu.schema_from_list(cols, 1)
        self.assertEqual(len(schema), 3)

        # Question whether we want to go the overloading route
        self.assertTrue(schema.at(0).equals(col1))
        self.assertTrue(schema.at(1).equals(col2))
        self.assertTrue(schema.at(2).equals(col3))

        # This isn't yet very easy
        # self.assertEqual(schema['key'], col1)
        # self.assertEqual(schema['int_val'], col2)
        # self.assertEqual(schema['string_val'], col3)

    def test_column_schema_repr(self):
        col1 = kudu.ColumnSchema.create('key', kudu.INT32)

        result = repr(col1)
        expected = 'ColumnSchema(name=key, type=int32, nullable=False)'
        self.assertEqual(result, expected)

    def test_column_schema_default_value(self):
        pass


class KuduBasicsBase(object):

    @classmethod
    def setUpClass(cls):
        cls.client = kudu.Client('127.0.0.1')

        cls.schema = example_schema()

        cls.ex_table = 'example-table'
        if cls.client.table_exists(cls.ex_table):
            cls.client.delete_table(cls.ex_table)
        cls.client.create_table(cls.ex_table, cls.schema)

    @classmethod
    def tearDownClass(cls):
        cls.client.delete_table(cls.ex_table)


class TestTable(KuduBasicsBase, unittest.TestCase):

    def setUp(self):
        pass

    def test_table_basics(self):
        table = self.client.open_table(self.ex_table)

        self.assertEqual(table.name, self.ex_table)
        self.assertEqual(table.num_columns, len(self.schema))

    def test_table_exists(self):
        self.assertFalse(self.client.table_exists('nonexistent-table'))
        self.assertTrue(self.client.table_exists(self.ex_table))

    def test_delete_table(self):
        name = "peekaboo"
        self.client.create_table(name, self.schema)
        self.assertTrue(self.client.delete_table(name))
        self.assertFalse(self.client.table_exists(name))

        # Should raise a more meaningful exception at some point
        val = self.client.delete_table(name)
        self.assertFalse(val)

    def test_open_table_nonexistent(self):
        self.assertRaises(kudu.KuduException, self.client.open_table,
                          '__donotexist__')

    def test_insert_nonexistent_field(self):
        table = self.client.open_table(self.ex_table)
        op = table.insert()
        self.assertRaises(KeyError, op.__setitem__, 'doesntexist', 12)

    def test_insert_rows_and_delete(self):
        nrows = 100
        table = self.client.open_table(self.ex_table)
        session = self.client.new_session()
        for i in range(nrows):
            op = table.insert()
            op['key'] = i
            op['int_val'] = i * 2
            op['string_val'] = 'hello_%d' % i
            session.apply(op)

        # Cannot apply the same insert twice, does not blow up in C++
        self.assertRaises(Exception, session.apply, op)

        # synchronous
        self.assertTrue(session.flush())

        # Delete the rows we just wrote
        for i in range(nrows):
            op = table.delete()
            op['key'] = i
            session.apply(op)
        session.flush()
        # TODO: verify the table is now empty

    def test_capture_kudu_error(self):
        pass


class TestScanner(KuduBasicsBase, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(TestScanner, cls).setUpClass()

        cls.nrows = 100
        table = cls.client.open_table(cls.ex_table)
        session = cls.client.new_session()

        tuples = []
        for i in range(cls.nrows):
            op = table.insert()
            tup = i, i * 2, 'hello_%d' % i
            op['key'] = tup[0]
            op['int_val'] = tup[1]
            op['string_val'] = tup[2]
            session.apply(op)
            tuples.append(tup)
        session.flush()

        cls.table = table
        cls.tuples = tuples

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def test_scan_rows_basic(self):
        # Let's scan with no predicates
        scanner = self.table.scanner().open()

        batch = scanner.read_all()
        self.assertEqual(len(batch), self.nrows)

        result_tuples = batch.as_tuples()
        self.assertEqual(result_tuples, self.tuples)

    def test_scan_rows_simple_predicate(self):
        scanner = self.table.scanner()

        # Not the best API for now
        pred = scanner.range_predicate(0, 20, 49)
        scanner.add_predicate(pred)
        scanner.open()

        batch = scanner.read_all()
        tuples = batch.as_tuples()

        self.assertEqual(tuples, self.tuples[20:50])




if __name__ == '__main__':
    nose.runmodule(argv=[__file__, '-vvs', '-x', '--pdb',
                         '--pdb-failure', '-s'], exit=False)
