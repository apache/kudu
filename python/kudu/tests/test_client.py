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

from kudu.compat import unittest, long
from kudu.tests.common import KuduTestBase
from kudu.client import Partitioning
import kudu
import datetime
from pytz import utc


class TestClient(KuduTestBase, unittest.TestCase):

    def setUp(self):
        pass

    def test_table_basics(self):
        table = self.client.table(self.ex_table)

        self.assertEqual(table.name, self.ex_table)
        self.assertEqual(table.num_columns, len(self.schema))
        self.assertIsNotNone(table.id)

    def test_table_column(self):
        table = self.client.table(self.ex_table)
        cols = [(table['key'], 'key', 'int32'),
                (table[1], 'int_val', 'int32'),
                (table[-1], 'unixtime_micros_val', 'unixtime_micros')]

        for col, name, type in cols:
            assert col.name == name
            assert col.parent is table

            result_repr = repr(col)
            expected_repr = ('Column({0}, parent={1}, type={2})'
                             .format(name, self.ex_table, type))
            assert result_repr == expected_repr

    def test_table_schema_retains_reference(self):
        import gc

        table = self.client.table(self.ex_table)
        schema = table.schema
        table = None

        gc.collect()
        repr(schema)

    def test_table_exists(self):
        self.assertFalse(self.client.table_exists('nonexistent-table'))
        self.assertTrue(self.client.table_exists(self.ex_table))

    def test_list_tables(self):
        schema = self.example_schema()
        partitioning = self.example_partitioning()

        to_create = ['foo1', 'foo2', 'foo3']
        for name in to_create:
            self.client.create_table(name, schema, partitioning)

        result = self.client.list_tables()
        expected = [self.ex_table] + to_create
        assert sorted(result) == expected

        result = self.client.list_tables('foo')
        assert sorted(result) == to_create

        for name in to_create:
            self.client.delete_table(name)

    def test_is_multimaster(self):
        assert self.client.is_multimaster

    def test_delete_table(self):
        name = "peekaboo"
        self.client.create_table(name, self.schema, self.partitioning)
        self.client.delete_table(name)
        assert not self.client.table_exists(name)

        # Should raise a more meaningful exception at some point
        with self.assertRaises(kudu.KuduNotFound):
            self.client.delete_table(name)

    def test_table_nonexistent(self):
        self.assertRaises(kudu.KuduNotFound, self.client.table,
                          '__donotexist__')

    def test_create_table_with_different_replication_factors(self):
        name = "different_replica_table"

        # Test setting the number of replicas for 1, 3 and 5 provided that the
        # number does not exceed the number of tservers
        for n_replicas in [n for n in [1, 3, 5] if n <= self.NUM_TABLET_SERVERS]:
            try:
                self.client.create_table(
                    name, self.schema,
                    partitioning=Partitioning().add_hash_partitions(['key'], 2),
                    n_replicas=n_replicas)

                assert n_replicas == self.client.table(name).num_replicas

            finally:
                try:
                    self.client.delete_table(name)
                except:
                    pass

    def test_create_partitioned_table(self):
        name = 'partitioned_table'
        try:
            self.client.create_table(
                name, self.schema,
                partitioning=Partitioning().add_hash_partitions(['key'], 2))
            # TODO: once the Python client can list partition info, assert that it was
            # created successfully here.
            self.client.delete_table(name)

            self.client.create_table(
                name, self.schema,
                partitioning=Partitioning()
                    .set_range_partition_columns(['key'])
                    .add_range_partition_split({'key': 10})
                    .add_range_partition_split([20])
                    .add_range_partition_split((30,))
            )
            self.client.delete_table(name)

            self.client.create_table(
                name, self.schema,
                partitioning=Partitioning().add_hash_partitions(['key'],
                                                                2,
                                                                seed=342310))
            self.client.delete_table(name)

        finally:
            try:
                self.client.delete_table(name)
            except:
                pass

    def test_insert_nonexistent_field(self):
        table = self.client.table(self.ex_table)
        op = table.new_insert()
        self.assertRaises(KeyError, op.__setitem__, 'doesntexist', 12)

    def test_insert_and_mutate_rows(self):
        nrows = 100
        table = self.client.table(self.ex_table)
        session = self.client.new_session()
        for i in range(nrows):
            op = table.new_insert((i, i*2, 'hello_%d' % i))
            session.apply(op)

        # Cannot apply the same insert twice, C++ client does not indicate an
        # error
        self.assertRaises(Exception, session.apply, op)

        # synchronous
        session.flush()

        # Update a row, upsert another one
        op = table.new_update()
        op['key'] = 1
        op['int_val'] = 111
        op['string_val'] = 'updated'
        # Insert datetime without timezone specified, will be assumed
        # to be UTC
        op['unixtime_micros_val'] = datetime.datetime(2016, 10, 30, 10, 12)
        session.apply(op)

        op = table.new_upsert({0: 2,
                               1: 222,
                               2: 'upserted'})
        session.apply(op)
        session.flush()

        scanner = table.scanner().open()
        rows = dict((t[0], t) for t in scanner.read_all_tuples())
        assert len(rows) == nrows
        assert rows[1] == (1, 111, 'updated',
                           datetime.datetime(2016, 10, 30, 10, 12)
                           .replace(tzinfo=utc))
        assert rows[2] == (2, 222, 'upserted', None)

        # Delete the rows we just wrote
        for i in range(nrows):
            op = table.new_delete({'key': i})
            session.apply(op)
        session.flush()

        scanner = table.scanner().open()
        assert len(scanner.read_all_tuples()) == 0

    def test_failed_write_op(self):
        # Insert row
        table = self.client.table(self.ex_table)
        session = self.client.new_session()
        session.apply(table.new_insert({'key': 1}))
        session.flush()

        # Attempt to insert row again
        session.apply(table.new_insert({'key': 1}))
        self.assertRaises(kudu.KuduBadStatus, session.flush)

        # Check errors
        errors, overflowed = session.get_pending_errors()
        self.assertFalse(overflowed)
        self.assertEqual(len(errors), 1)
        error = errors[0]
        # This test passes because we currently always return
        # True.
        self.assertTrue(error.was_possibly_successful())
        self.assertEqual(error.failed_op(), 'INSERT int32 key=1')

        # Delete inserted row
        session.apply(table.new_delete({'key': 1}))
        session.flush()

    def test_session_auto_open(self):
        table = self.client.table(self.ex_table)
        scanner = table.scanner()
        result = scanner.read_all_tuples()
        assert len(result) == 0

    def test_session_open_idempotent(self):
        table = self.client.table(self.ex_table)
        scanner = table.scanner().open().open()
        result = scanner.read_all_tuples()
        assert len(result) == 0

    def test_session_flush_modes(self):
        self.client.new_session(flush_mode=kudu.FLUSH_MANUAL)
        self.client.new_session(flush_mode=kudu.FLUSH_AUTO_SYNC)
        self.client.new_session(flush_mode=kudu.FLUSH_AUTO_BACKGROUND)

        self.client.new_session(flush_mode='manual')
        self.client.new_session(flush_mode='sync')
        self.client.new_session(flush_mode='background')

        with self.assertRaises(ValueError):
            self.client.new_session(flush_mode='foo')


    def test_session_mutation_buffer_settings(self):
        self.client.new_session(flush_mode=kudu.FLUSH_AUTO_BACKGROUND,
                                mutation_buffer_sz= 10*1024*1024,
                                mutation_buffer_watermark=0.5,
                                mutation_buffer_flush_interval=2000,
                                mutation_buffer_max_num=3)

        session = self.client.new_session(flush_mode=kudu.FLUSH_AUTO_BACKGROUND)
        session.set_mutation_buffer_space(10*1024*1024)
        session.set_mutation_buffer_flush_watermark(0.5)
        session.set_mutation_buffer_flush_interval(2000)
        session.set_mutation_buffer_max_num(3)

    def test_session_mutation_buffer_errors(self):
        session = self.client.new_session(flush_mode=kudu.FLUSH_AUTO_BACKGROUND)

        with self.assertRaises(OverflowError):
            session.set_mutation_buffer_max_num(-1)

        with self.assertRaises(kudu.errors.KuduInvalidArgument):
            session.set_mutation_buffer_flush_watermark(1.2)

        with self.assertRaises(OverflowError):
            session.set_mutation_buffer_flush_interval(-1)

        with self.assertRaises(OverflowError):
            session.set_mutation_buffer_space(-1)

    def test_connect_timeouts(self):
        # it works! any other way to check
        kudu.connect(self.master_hosts, self.master_ports,
                     admin_timeout_ms=1000,
                     rpc_timeout_ms=1000)

    def test_capture_kudu_error(self):
        pass

    def test_list_tablet_server(self):
        # Not confirming the number of tablet servers in this test because
        # that is confirmed in the beginning of testing. This test confirms
        # that all of the KuduTabletServer methods returned results and that
        # a result is returned from the list_tablet_servers method.
        tservers = self.client.list_tablet_servers()
        self.assertTrue(tservers)
        for tserver in tservers:
            assert tserver.uuid() is not None
            assert tserver.hostname() is not None
            assert tserver.port() is not None

    def test_bad_partialrow(self):
        table = self.client.table(self.ex_table)
        op = table.new_insert()
        # Test bad keys or indexes
        keys = [
            ('not-there', KeyError),
            (len(self.schema) + 1, IndexError),
            (-1, IndexError)
        ]

        for key in keys:
            with self.assertRaises(key[1]):
                op[key[0]] = 'test'

        # Test incorrectly typed data
        with self.assertRaises(TypeError):
            op['int_val'] = 'incorrect'

        # Test setting NULL in a not-null column
        with self.assertRaises(kudu.errors.KuduInvalidArgument):
            op['key'] = None

    def test_alter_table_rename(self):
        try:
            self.client.create_table('alter-rename',
                                     self.schema,
                                     self.partitioning)
            table = self.client.table('alter-rename')
            alterer = self.client.new_table_alterer(table)
            table = alterer.rename('alter-newname').alter()
            self.assertEqual(table.name, 'alter-newname')
        finally:
            self.client.delete_table('alter-newname')

    def test_alter_column(self):
        try:
            self.client.create_table('alter-column',
                                     self.schema,
                                     self.partitioning)
            table = self.client.table('alter-column')
            alterer = self.client.new_table_alterer(table)
            alterer.alter_column('string_val',rename_to='string_val_renamed')
            table = alterer.alter()

            # Confirm column rename
            col = table['string_val_renamed']

        finally:
            self.client.delete_table('alter-column')

    def test_alter_table_add_drop_column(self):
        table = self.client.table(self.ex_table)
        alterer = self.client.new_table_alterer(table)
        alterer.add_column('added-column', type_='int64', default=0)
        table = alterer.alter()

        # Confirm column was added
        expected_repr = 'Column(added-column, parent={0}, type=int64)'\
            .format(self.ex_table)
        self.assertEqual(expected_repr, repr(table['added-column']))

        alterer = self.client.new_table_alterer(table)
        alterer.drop_column('added-column')
        table = alterer.alter()

        # Confirm column has been dropped.
        with self.assertRaises(KeyError):
            col = table['added-column']

    def test_alter_table_direct_instantiation(self):
        # Run the add_drop_column test with direct instantiation of
        # the TableAlterer
        table = self.client.table(self.ex_table)
        alterer = kudu.client.TableAlterer(table)
        alterer.add_column('added-column', type_='int64', default=0)
        table = alterer.alter()

        # Confirm column was added
        expected_repr = 'Column(added-column, parent={0}, type=int64)' \
            .format(self.ex_table)
        self.assertEqual(expected_repr, repr(table['added-column']))

        alterer = self.client.new_table_alterer(table)
        alterer.drop_column('added-column')
        table = alterer.alter()

        # Confirm column has been dropped.
        with self.assertRaises(KeyError):
            col = table['added-column']

    def test_alter_table_add_drop_partition(self):
        # Add Range Partition
        table = self.client.table(self.ex_table)
        alterer = self.client.new_table_alterer(table)
        # Drop the unbounded range partition.
        alterer.drop_range_partition()
        # Add a partition from 0 to 100
        alterer.add_range_partition(
            lower_bound={'key': 0},
            upper_bound={'key': 100}
        )
        table = alterer.alter()
        # TODO(jtbirdsell): Once C++ client can list partition schema
        # then this test should confirm that the partition was added.
        alterer = self.client.new_table_alterer(table)
        # Drop the partition from 0 to 100
        alterer.drop_range_partition(
            lower_bound={'key': 0},
            upper_bound={'key': 100}
        )
        # Add back the unbounded range partition
        alterer.add_range_partition()
        table = alterer.alter()


class TestMonoDelta(unittest.TestCase):

    def test_empty_ctor(self):
        delta = kudu.TimeDelta()
        assert repr(delta) == 'kudu.TimeDelta()'

    def test_static_ctors(self):
        delta = kudu.timedelta(3.5)
        assert delta.to_seconds() == 3.5

        delta = kudu.timedelta(millis=3500)
        assert delta.to_millis() == 3500

        delta = kudu.timedelta(micros=3500)
        assert delta.to_micros() == 3500

        delta = kudu.timedelta(micros=1000)
        assert delta.to_nanos() == long(1000000)

        delta = kudu.timedelta(nanos=3500)
        assert delta.to_nanos() == 3500
