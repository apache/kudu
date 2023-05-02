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

from kudu.compat import CompatUnitTest, long
from kudu.tests.common import KuduTestBase
from kudu.client import (Partitioning,
                         RangePartition,
                         ENCRYPTION_OPTIONAL,
                         ENCRYPTION_REQUIRED,
                         ENCRYPTION_REQUIRED_REMOTE)
from kudu.errors import (KuduInvalidArgument,
                         KuduBadStatus)
from kudu.schema import (Schema,
                         KuduValue)
import kudu
import datetime
from pytz import utc


class TestClient(KuduTestBase, CompatUnitTest):

    def setUp(self):
        pass

    def test_table_basics(self):
        table = self.client.table(self.ex_table)

        self.assertEqual(table.name, self.ex_table)
        self.assertEqual(table.num_columns, len(self.schema))
        self.assertIsNotNone(table.id)
        self.assertIsNotNone(table.owner)

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
        error_msg = 'the table does not exist: table_name: "{0}"'.format(name)
        with self.assertRaisesRegex(kudu.KuduNotFound, error_msg):
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

    def test_create_table_with_range_specific_hash_schemas(self):
        table_name = 'create_table_range_specific_hash_schemas'
        try:
            # define range with custom hash schema
            p = RangePartition({'key': 0}, {'key': 100})
            p.add_hash_partitions(['key'], 5)

            self.client.create_table(
                table_name, self.schema,
                partitioning=Partitioning()
                    .set_range_partition_columns(['key'])
                    .add_hash_partitions(['key'], 2)
                    .add_range_partition(
                        lower_bound={'key': -100},
                        upper_bound={'key': 0})
                    .add_custom_range_partition(p)
            )

            # rely on 1-1 mapping between tokens and tablets for full table scan
            table = self.client.table(table_name)
            builder = table.scan_token_builder()
            builder.set_fault_tolerant()
            tokens = builder.build()
            self.assertEqual(7, len(tokens))

            session = self.client.new_session()
            for i in range(-100, 100):
                op = table.new_insert((i, i))
                session.apply(op)
            session.flush()

            self.client.delete_table(table_name)
        finally:
            try:
                self.client.delete_table(table_name)
            except:
                pass

    def test_create_table_with_different_owner(self):
        name = 'table_with_different_owner'
        try:
            self.client.create_table(
                    name, self.schema,
                    partitioning=Partitioning().add_hash_partitions(['key'], 2),
                    owner='alice')

            self.assertEqual('alice', self.client.table(name).owner)

        finally:
            try:
                self.client.delete_table(name)
            except:
                pass

    def test_create_table_with_different_comment(self):
        name = 'table_with_different_comment'
        try:
            self.client.create_table(
                name, self.schema,
                partitioning=Partitioning().add_hash_partitions(['key'], 2),
                comment='new comment')

            self.assertEqual('new comment', self.client.table(name).comment)

        finally:
            try:
                self.client.delete_table(name)
            except:
                pass

    def test_create_table_with_auto_incrementing_column(self):
        table_name = 'create_table_with_auto_incrementing_column'
        try:
            builder = kudu.schema_builder()
            (builder.add_column('key', kudu.int32)
            .nullable(False)
            .non_unique_primary_key())
            builder.add_column('data', kudu.string)
            schema = builder.build()

            self.client.create_table(
                table_name, schema,
                partitioning=Partitioning().add_hash_partitions(['key'], 2))

            table = self.client.table(table_name)
            session = self.client.new_session()
            nrows = 10
            for _ in range(nrows):
                op = table.new_insert()
                op['key'] = 1
                op['data'] = 'text'
                session.apply(op)

            session.flush()

            scanner = table.scanner()
            results = scanner.open().read_all_tuples()
            assert nrows == len(results)
            for i in range(len(results)):
                auto_incrementing_value = results[i][1]
                assert auto_incrementing_value == i+1
        finally:
            try:
                self.client.delete_table(table_name)
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
            if i % 2 == 0:
                op = table.new_insert_ignore((i, i*2, 'hello_%d' % i))
            else:
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

        # Insert ignore existing row
        op = table.new_insert_ignore((3, 1, 'hello_1'))
        session.apply(op)

        # Update ignore a missing row
        op = table.new_update_ignore((-999, 1, 'hello_1'))
        session.apply(op)

        # Delete ignore a missing row
        op = table.new_delete_ignore({'key': -998})
        session.apply(op)

        scanner = table.scanner().open()
        rows = dict((t[0], t) for t in scanner.read_all_tuples())
        assert len(rows) == nrows
        assert rows[1] == (1, 111, 'updated',
                           datetime.datetime(2016, 10, 30, 10, 12)
                           .replace(tzinfo=utc))
        assert rows[2] == (2, 222, 'upserted', None)
        assert rows[3] == (3, 6, 'hello_3', None)

        # Delete the rows we just wrote
        for i in range(nrows):
            if i % 2 == 0:
                op = table.new_delete({'key': i})
            else:
                op = table.new_delete_ignore({'key': i})
            session.apply(op)
        session.flush()

        scanner = table.scanner().open()
        assert len(scanner.read_all_tuples()) == 0

    def test_insert_and_mutate_immutable_column(self):
        table_name = 'insert_and_mutate_immutable_column'
        try:
            builder = kudu.schema_builder()
            (builder.add_column('key', kudu.int32)
            .nullable(False)
            .primary_key())
            builder.add_column('immutable_data', kudu.string).mutable(False)
            builder.add_column('mutable_data', kudu.string).mutable(True)
            schema = builder.build()

            self.client.create_table(
                table_name, schema,
                partitioning=Partitioning().add_hash_partitions(['key'], 2))

            table = self.client.table(table_name)
            session = self.client.new_session()
            op = table.new_insert()
            op['key'] = 1
            op['immutable_data'] = 'text'
            op['mutable_data'] = 'text'
            session.apply(op)
            session.flush()

            # Update the mutable columns
            op = table.new_update()
            op['key'] = 1
            op['mutable_data'] = 'new_text'
            session.apply(op)
            session.flush()

            # Update the immutable column
            op = table.new_update()
            op['key'] = 1
            op['immutable_data'] = 'new_text'
            session.apply(op)
            try:
                session.flush()
            except KuduBadStatus:
                message = 'Immutable: UPDATE not allowed for immutable column'
                errors, overflow = session.get_pending_errors()
                assert not overflow
                assert len(errors) == 1
                assert message in repr(errors[0])

            # Update ignore on both mutable and immutable columns. The error is ignored.
            op = table.new_update_ignore()
            op['key'] = 1
            op['immutable_data'] = 'new_text'
            op['mutable_data'] = 'new_text'
            session.apply(op)
            session.flush()

            # Update ignore the immutable column
            op = table.new_update_ignore()
            op['key'] = 1
            op['immutable_data'] = 'new_text'
            session.apply(op)
            try:
                session.flush()
            except KuduBadStatus:
                message = 'Invalid argument: No fields updated'
                errors, overflow = session.get_pending_errors()
                assert not overflow
                assert len(errors) == 1
                assert message in repr(errors[0])

            # TODO: test upsert ignore, once it is supported by the Python client.

            # Upsert the mutable columns
            op = table.new_upsert()
            op['key'] = 1
            op['mutable_data'] = 'new_text'
            session.apply(op)
            session.flush()

            # Upsert the immutable column
            op = table.new_upsert()
            op['key'] = 1
            op['immutable_data'] = 'new_text'
            session.apply(op)
            try:
                session.flush()
            except KuduBadStatus:
                message = 'Immutable: UPDATE not allowed for immutable column'
                errors, overflow = session.get_pending_errors()
                assert not overflow
                assert len(errors) == 1
                assert message in repr(errors[0])

        finally:
            try:
                self.client.delete_table(table_name)
            except:
                pass

    def test_insert_with_auto_incrementing_column(self):

        table_name = 'test_insert_with_auto_incrementing_column'
        try:
            builder = kudu.schema_builder()
            (builder.add_column('key', kudu.int32)
            .nullable(False)
            .non_unique_primary_key())
            builder.add_column('data', kudu.string)
            schema = builder.build()

            self.client.create_table(
                table_name, schema,
                partitioning=Partitioning().add_hash_partitions(['key'], 2))

            table = self.client.table(table_name)
            session = self.client.new_session()

            # Insert with auto incrementing column specified
            op = table.new_insert()
            op['key'] = 1
            op[Schema.get_auto_incrementing_column_name()] = 1
            session.apply(op)
            try:
                session.flush()
            except KuduBadStatus:
                message = 'is incorrectly set'
                errors, overflow = session.get_pending_errors()
                assert not overflow
                assert len(errors) == 1
                assert message in repr(errors[0])

            # TODO: Upsert should be rejected as of now. However the test segfaults: KUDU-3454
            # TODO: Upsert ignore should be rejected. Once Python client supports upsert ignore.

            # With non-unique primary key, one can't use the tuple/list initialization for new
            # inserts. In this case, at the second position it would like to get an int64 (the type
            # of the auto-incrementing counter), therefore we get type error. (Specifying the
            # auto-incremeintg counter is obviously rejected from the server side)
            error_msg = 'an integer is required'
            with self.assertRaisesRegex(TypeError, error_msg):
                op = table.new_insert((1,'text'))

            error_msg = 'an integer is required'
            with self.assertRaisesRegex(TypeError, error_msg):
                op = table.new_insert([1,'text'])

        finally:
            try:
                self.client.delete_table(table_name)
            except:
                pass

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

        flush_mode = 'foo'
        error_msg = 'Invalid flush mode: {0}'.format(flush_mode)
        with self.assertRaisesRegex(ValueError, error_msg):
            self.client.new_session(flush_mode=flush_mode)

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

        error_msg = 'can\'t convert negative value to unsigned int'
        with self.assertRaisesRegex(OverflowError, error_msg):
            session.set_mutation_buffer_max_num(-1)

        error_msg = '120: watermark must be between 0 and 100 inclusive'
        with self.assertRaisesRegex(kudu.errors.KuduInvalidArgument, error_msg):
            session.set_mutation_buffer_flush_watermark(1.2)

        error_msg = 'can\'t convert negative value to unsigned int'
        with self.assertRaisesRegex(OverflowError, error_msg):
            session.set_mutation_buffer_flush_interval(-1)

        error_msg = 'can\'t convert negative value to size_t'
        with self.assertRaisesRegex(OverflowError, error_msg):
            session.set_mutation_buffer_space(-1)

    def test_connect_timeouts(self):
        # it works! any other way to check
        kudu.connect(self.master_hosts, self.master_ports, admin_timeout_ms=1000, rpc_timeout_ms=1000)

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
        error_msg = 'an integer is required'
        with self.assertRaisesRegex(TypeError, error_msg):
            op['int_val'] = 'incorrect'

        # Test setting NULL in a not-null column
        error_msg = 'column not nullable: key'
        with self.assertRaisesRegex(kudu.errors.KuduInvalidArgument, error_msg):
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

    def test_alter_table_change_owner(self):
        name = 'alter-owner'
        try:
            self.client.create_table(name,
                                     self.schema,
                                     self.partitioning)
            table = self.client.table(name)
            alterer = self.client.new_table_alterer(table)
            table = alterer.set_owner('alice').alter()
            self.assertEqual('alice', self.client.table(name).owner)
            with self.assertRaises(TypeError):
                alterer.set_owner(None).alter()
        finally:
            self.client.delete_table(name)

    def test_alter_table_change_comment(self):
        name = 'alter-comment'
        try:
            self.client.create_table(name,
                                     self.schema,
                                     self.partitioning)
            table = self.client.table(name)
            alterer = self.client.new_table_alterer(table)
            table = alterer.set_comment('change comment').alter()
            self.assertEqual('change comment', self.client.table(name).comment)
            with self.assertRaises(TypeError):
                alterer.set_comment(None).alter()
        finally:
            self.client.delete_table(name)

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

            # Check if existing comment can be altered, and new comment can be specified
            alterer = self.client.new_table_alterer(table)
            alterer.alter_column('key').comment('new_key_comment')
            alterer.alter_column('int_val').comment('int_val_comment')
            table = alterer.alter()

            assert table['key'].spec.comment == 'new_key_comment'
            assert table['int_val'].spec.comment == 'int_val_comment'
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

    def test_alter_table_add_partition_with_custom_hash_schema(self):
        table_name = 'add_partition_with_custom_hash_schema'
        try:
            # create table with [-100, 0) range having table-wide hash schema
            self.client.create_table(
                table_name, self.schema,
                partitioning=Partitioning()
                    .set_range_partition_columns(['key'])
                    .add_hash_partitions(['key'], 3)
                    .add_range_partition(
                    lower_bound={'key': -100},
                    upper_bound={'key': 0})
            )

            # open the newly created table
            table = self.client.table(table_name)

            # define range with custom hash schema
            p = RangePartition({'key': 0}, {'key': 100})
            p.add_hash_partitions(['key'], 2, 8)

            alterer = self.client.new_table_alterer(table)
            alterer.add_custom_range_partition(p)
            table = alterer.alter()

            # rely on 1-1 mapping between tokens and tablets for full table scan
            builder = table.scan_token_builder()
            builder.set_fault_tolerant()
            tokens = builder.build()
            self.assertEqual(5, len(tokens))

            session = self.client.new_session()
            for i in range(-100, 100):
                op = table.new_insert((i, i))
                session.apply(op)
            session.flush()

            # drop the new custom range partition that hash just been added
            alterer = self.client.new_table_alterer(table)
            alterer.drop_range_partition({'key': 0}, {'key': 100})
            table = alterer.alter()

            # rely on 1-1 mapping between tokens and tablets for full table scan
            builder = table.scan_token_builder()
            builder.set_fault_tolerant()
            tokens = builder.build()
            self.assertEqual(3, len(tokens))

            # drop the range partition that have table-wide hash schema
            alterer = self.client.new_table_alterer(table)
            alterer.drop_range_partition({'key': -100}, {'key': 0})
            table = alterer.alter()

            # rely on 1-1 mapping between tokens and tablets for full table scan
            builder = table.scan_token_builder()
            builder.set_fault_tolerant()
            tokens = builder.build()
            self.assertEqual(0, len(tokens))

            self.client.delete_table(table_name)
        finally:
            try:
                self.client.delete_table(table_name)
            except:
                pass

    def test_alter_table_auto_incrementing_column(self):
        table_name = 'alter_table_with_auto_incrementing_column'
        try:
            builder = kudu.schema_builder()
            (builder.add_column('key', kudu.int32)
            .nullable(False)
            .non_unique_primary_key())
            schema = builder.build()

            self.client.create_table(
                table_name, schema,
                partitioning=Partitioning().add_hash_partitions(['key'], 2))

            col_name = Schema.get_auto_incrementing_column_name()
            table = self.client.table(table_name)

            # negatives
            alterer = self.client.new_table_alterer(table)
            alterer.drop_column(col_name)
            error_msg = 'can\'t drop column: {0}'\
                        .format(Schema.get_auto_incrementing_column_name())
            with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
                alterer.alter()

            alterer = self.client.new_table_alterer(table)
            alterer.add_column(col_name)
            error_msg = 'can\'t add a column with reserved name: {0}'\
                        .format(Schema.get_auto_incrementing_column_name())
            with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
                alterer.alter()

            alterer = self.client.new_table_alterer(table)
            alterer.alter_column(col_name, "new_column_name")
            error_msg = 'can\'t change name for column: {0}'\
                        .format(Schema.get_auto_incrementing_column_name())
            with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
                alterer.alter()

            alterer = self.client.new_table_alterer(table)
            alterer.alter_column(col_name).remove_default()
            error_msg = 'can\'t change remove default for column: {0}'\
                        .format(Schema.get_auto_incrementing_column_name())
            with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
                alterer.alter()

            # positives
            alterer = self.client.new_table_alterer(table)
            alterer.alter_column(col_name).block_size(1)
            alterer.alter()

            alterer = self.client.new_table_alterer(table)
            alterer.alter_column(col_name).encoding('plain')
            alterer.alter()

            alterer = self.client.new_table_alterer(table)
            alterer.alter_column(col_name).compression('none')
            alterer.alter()

            alterer = self.client.new_table_alterer(table)
            alterer.alter_column(col_name).comment('new_comment')
            alterer.alter()

        finally:
            try:
                self.client.delete_table(table_name)
            except:
                pass

class TestAuthAndEncription(KuduTestBase, CompatUnitTest):
    def test_require_encryption(self):
        client = kudu.connect(self.master_hosts, self.master_ports,
                              encryption_policy=ENCRYPTION_REQUIRED)

    def test_require_authn(self):
        # Kerberos is not enabled on the cluster, so requiring
        # authentication is expected to fail.
        error_msg = 'client requires authentication, but server does not have Kerberos enabled'
        with self.assertRaisesRegex(kudu.KuduBadStatus, error_msg):
            client = kudu.connect(self.master_hosts, self.master_ports,
                     require_authentication=True)

class TestJwt(KuduTestBase, CompatUnitTest):
    def test_jwt(self):
        jwt = self.get_jwt(valid=True)
        client = kudu.connect(self.master_hosts, self.master_ports,
                              require_authentication=True, jwt=jwt)

        jwt = self.get_jwt(valid=False)
        error_msg = ('FATAL_INVALID_JWT: Not authorized: Verification failed, error: ' +
        'failed to verify signature: VerifyFinal failed')
        with self.assertRaisesRegex(kudu.KuduBadStatus, error_msg):
            client = kudu.connect(self.master_hosts, self.master_ports,
                              require_authentication=True, jwt=jwt)

class TestMonoDelta(CompatUnitTest):

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
