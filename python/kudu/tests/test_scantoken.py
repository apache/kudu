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

from kudu.compat import unittest
from kudu.tests.util import TestScanBase
from kudu.tests.common import KuduTestBase
import kudu
from multiprocessing import Pool
import datetime

def _get_scan_token_results(input):
    client = kudu.Client("{0}:{1}".format(input[1], input[2]))
    scanner = client.deserialize_token_into_scanner(input[0])
    scanner.open()
    return scanner.read_all_tuples()

class TestScanToken(TestScanBase):

    @classmethod
    def setUpClass(self):
        super(TestScanToken, self).setUpClass()

    def setUp(self):
        pass

    def _subtest_serialize_thread_and_verify(self, tokens, expected_tuples):
        """
        Given the input serialized tokens, spawn new threads,
        execute them and validate the results
        """
        input =  [(token.serialize(), self.master_host, self.master_port)
                for token in tokens]

        # Begin process pool
        pool = Pool(len(input))
        results = pool.map(_get_scan_token_results, input)

        #Validate results
        actual_tuples = []
        for result in results:
            actual_tuples += result

        self.assertEqual(sorted(expected_tuples), sorted(actual_tuples))

    def test_scan_token_serde_threaded_with_named_projection(self):
        """
        Creates scan tokens, serializes them, delivers them to new
        threads then executes them in parallel with seperate clients.
        """
        builder = self.table.scan_token_builder()
        builder.set_projected_column_names(['key', 'string_val']).set_fault_tolerant()

        # Serialize execute and verify
        self._subtest_serialize_thread_and_verify(builder.build(),
                                                  [(x[0], x[2]) for x in self.tuples])

    def test_scan_token_serde_threaded_simple_predicate_and_index_projection(self):
        """
        Creates scan tokens with predicates and an index projection,
        serializes them, delivers them to new threads then executes
        them in parallel with seperate clients.
        """
        key = self.table['key']
        preds = [key > 19, key < 50]

        builder = self.table.scan_token_builder()
        builder.set_projected_column_indexes([0, 1])\
            .set_fault_tolerant()\
            .add_predicates(preds)

        # Serialize execute and verify
        self._subtest_serialize_thread_and_verify(builder.build(),
                                                  [x[0:2] for x in self.tuples[20:50]])

    def test_scan_token_serde_threaded_with_bounds(self):
        """
        Creates scan tokens with bounds, serializes them,
        delivers them to new threads then executes them
        in parallel with seperate clients.
        """
        builder = self.table.scan_token_builder()
        lower_bound = builder.new_bound()
        lower_bound['key'] = 50
        upper_bound = builder.new_bound()
        upper_bound['key'] = 55
        builder.set_fault_tolerant()\
            .add_lower_bound(lower_bound)\
            .add_upper_bound(upper_bound)

        # Serialize execute and verify
        self._subtest_serialize_thread_and_verify(builder.build(),
                                                  self.tuples[50:55])

    def test_scan_token_invalid_predicates(self):
        builder = self.table.scan_token_builder()
        sv = self.table['string_val']

        with self.assertRaises(TypeError):
            builder.add_predicates([sv >= None])

        with self.assertRaises(kudu.KuduInvalidArgument):
            builder.add_predicates([sv >= 1])

    def test_scan_token_batch_by_batch_with_local_scanner(self):
        builder = self.table.scan_token_builder()
        lower_bound = builder.new_bound()
        lower_bound['key'] = 10
        upper_bound = builder.new_bound()
        upper_bound['key'] = 90
        builder.set_fault_tolerant() \
            .add_lower_bound(lower_bound) \
            .add_upper_bound(upper_bound)
        tokens = builder.build()

        tuples = []
        for token in tokens:
            scanner = token.into_kudu_scanner()
            scanner.open()

            while scanner.has_more_rows():
                batch = scanner.next_batch()
                tuples.extend(batch.as_tuples())

        self.assertEqual(sorted(tuples), self.tuples[10:90])

    def test_unixtime_micros(self):
        """
        Test setting and getting unixtime_micros fields
        """
        # Insert new rows
        self.insert_new_unixtime_micros_rows()

        # Validate results
        builder = self.table.scan_token_builder()
        tokens = builder.set_fault_tolerant().build()

        tuples = []
        for token in tokens:
            scanner = token.into_kudu_scanner()
            scanner.open()

            while scanner.has_more_rows():
                batch = scanner.next_batch()
                tuples.extend(batch.as_tuples())

        self.assertEqual(sorted(self.tuples), tuples)

    def test_scan_rows_in_list_predicate(self):
        """
        Test scan token builder/scanner with an InList predicate and
        a string comparison predicate
        """
        key_list = [2, 98]
        builder = self.table.scan_token_builder()
        builder.set_fault_tolerant() \
            .add_predicates([
            self.table[0].in_list(key_list),
            self.table['string_val'] >= 'hello_9'
        ])

        # Serialize execute and verify
        self._subtest_serialize_thread_and_verify(builder.build(),
                                                  [self.tuples[98]])

    def test_read_mode(self):
        """
        Test setting the read mode and scanning against a
        snapshot and latest
        """
        # Delete row
        self.delete_insert_row_for_read_test()

        # Check scanner results prior to delete
        builder = self.table.scan_token_builder()
        tokens = builder.set_read_mode('snapshot') \
            .set_snapshot(self.snapshot_timestamp) \
            .build()

        tuples = []
        for token in tokens:
            scanner = token.into_kudu_scanner().open()
            tuples.extend(scanner.read_all_tuples())

        self.assertEqual(sorted(self.tuples[1:]), sorted(tuples))

        #Check scanner results after insterts
        builder = self.table.scan_token_builder()
        tokens = builder.set_read_mode(kudu.READ_LATEST) \
            .build()

        tuples = []
        for token in tokens:
            scanner = token.into_kudu_scanner().open()
            tuples.extend(scanner.read_all_tuples())

        self.assertEqual(sorted(self.tuples), sorted(tuples))
