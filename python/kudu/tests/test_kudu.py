#!/usr/bin/env python

# Copyright (c) 2014, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

from __future__ import division

import json
import fnmatch
import nose
import os
import shutil
import subprocess
import tempfile
import time
import unittest
import signal

import kudu

class KuduBasicsBase(object):
    """Base test class that will start a configurable number of master and tablet
    servers."""

    BASE_PORT = 37000
    NUM_TABLET_SERVERS = 3

    @classmethod
    def start_cluster(cls):
        local_path = tempfile.mkdtemp(dir=os.getenv("TEST_TMPDIR", None))
        bin_path="{0}/build/latest".format(os.getenv("KUDU_HOME"))

        os.makedirs("{0}/master/".format(local_path))
        os.makedirs("{0}/master/data".format(local_path))
        os.makedirs("{0}/master/logs".format(local_path))

        path = ["{0}/kudu-master".format(bin_path),
                "-rpc_server_allow_ephemeral_ports",
                "-rpc_bind_addresses=0.0.0.0:0",
                "-fs_wal_dir={0}/master/data".format(local_path),
                "-fs_data_dirs={0}/master/data".format(local_path),
                "-log_dir={0}/master/logs".format(local_path),
                "-logtostderr",
                "-webserver_port=0",
                "-server_dump_info_path={0}/master/config.json".format(local_path)
              ]

        p = subprocess.Popen(path, shell=False)
        fid = open("{0}/master/kudu-master.pid".format(local_path), "w+")
        fid.write("{0}".format(p.pid))
        fid.close()

        # We have to wait for the master to settle before the config file appears
        config_file = "{0}/master/config.json".format(local_path)
        for _ in range(30):
            if os.path.exists(config_file):
                break
            time.sleep(1)
        else:
            raise Exception("Could not find kudu-master config file")

        # If the server was started get the bind port from the config dump
        master_config = json.load(open("{0}/master/config.json".format(local_path), "r"))
        # One master bound on local host
        master_port = master_config["bound_rpc_addresses"][0]["port"]

        for m in range(cls.NUM_TABLET_SERVERS):
            os.makedirs("{0}/ts/{1}".format(local_path, m))
            os.makedirs("{0}/ts/{1}/logs".format(local_path, m))

            path = ["{0}/kudu-tserver".format(bin_path),
                    "-rpc_server_allow_ephemeral_ports",
                    "-rpc_bind_addresses=0.0.0.0:0",
                    "-tserver_master_addrs=127.0.0.1:{0}".format(master_port),
                    "-webserver_port=0",
                    "-log_dir={0}/master/logs".format(local_path),
                    "-logtostderr",
                    "-fs_data_dirs={0}/ts/{1}/data".format(local_path, m),
                    "-fs_wal_dir={0}/ts/{1}/data".format(local_path, m),
                  ]
            p = subprocess.Popen(path, shell=False)
            fid = open("{0}/ts/{1}/kudu-tserver.pid".format(local_path, m), "w+")
            fid.write("{0}".format(p.pid))
            fid.close()

        return local_path, master_port

    @classmethod
    def stop_cluster(cls, path):
        for root, dirnames, filenames in os.walk('{0}/..'.format(path)):
            for filename in fnmatch.filter(filenames, '*.pid'):
                with open(os.path.join(root, filename)) as fid:
                    a = fid.read()
                    r = subprocess.Popen(["kill", "{0}".format(a)])
                    r.wait()
                    os.remove(os.path.join(root, filename))
        shutil.rmtree(path, True)

    @classmethod
    def setUpClass(cls):
        cls.cluster_path, master_port = cls.start_cluster()
        time.sleep(1)
        cls.client = kudu.Client('127.0.0.1:{0}'.format(master_port))

        cls.schema = cls.example_schema()

        cls.ex_table = 'example-table'
        if cls.client.table_exists(cls.ex_table):
            cls.client.delete_table(cls.ex_table)
        cls.client.create_table(cls.ex_table, cls.schema)

    @classmethod
    def tearDownClass(cls):
        cls.stop_cluster(cls.cluster_path)

    @classmethod
    def example_schema(cls):
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
        scanner.add_comparison_predicate("key", kudu.GREATER_EQUAL, 20)
        scanner.add_comparison_predicate("key", kudu.LESS_EQUAL, 49)
        scanner.open()

        batch = scanner.read_all()
        tuples = batch.as_tuples()

        self.assertEqual(tuples, self.tuples[20:50])

    def test_scan_rows_string_predicate(self):
        scanner = self.table.scanner()

        scanner.add_comparison_predicate("string_val", kudu.GREATER_EQUAL, "hello_20")
        scanner.add_comparison_predicate("string_val", kudu.LESS_EQUAL, "hello_25")
        scanner.open()

        batch = scanner.read_all()
        tuples = batch.as_tuples()

        self.assertEqual(tuples, self.tuples[20:26])

    def test_scan_invalid_predicates(self):
        scanner = self.table.scanner()
        try:
            scanner.add_comparison_predicate("foo", kudu.GREATER_EQUAL, "x")
        except Exception, e:
            self.assertEqual("Not found: column not found: foo", str(e))

        try:
            scanner.add_comparison_predicate("string_val", kudu.GREATER_EQUAL, 1)
        except Exception, e:
            self.assertEqual("Invalid argument: non-string value " +
                             "for string column string_val", str(e))

        try:
            scanner.add_comparison_predicate("string_val", kudu.GREATER_EQUAL, None)
        except Exception, e:
            self.assertEqual("unable to convert python type <type 'NoneType'>", str(e))


if __name__ == '__main__':
    nose.runmodule(argv=[__file__, '-vvs', '-x', '--pdb',
                         '--pdb-failure', '-s'], exit=False)
