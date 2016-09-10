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
from kudu.tests.common import KuduTestBase
import kudu
from multiprocessing import Pool

def _get_scan_token_results(input):
    client = kudu.Client("{0}:{1}".format(input[1], input[2]))
    scanner = client.deserialize_token_into_scanner(input[0])
    scanner.open()
    return scanner.read_all_tuples()

class TestScanToken(KuduTestBase, unittest.TestCase):

    @classmethod
    def setUpClass(self):
        """
        Stolen from the the test scanner given the similarity in
        functionality.
        """
        super(TestScanToken, self).setUpClass()

        self.nrows = 100
        table = self.client.table(self.ex_table)
        session = self.client.new_session()

        tuples = []
        for i in range(self.nrows):
            op = table.new_insert()
            tup = i, i * 2, 'hello_%d' % i if i % 2 == 0 else None
            op['key'] = tup[0]
            op['int_val'] = tup[1]
            if i % 2 == 0:
                op['string_val'] = tup[2]
            elif i % 3 == 0:
                op['string_val'] = None
            session.apply(op)
            tuples.append(tup)
        session.flush()

        self.table = table
        self.tuples = tuples

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
        preds = [key >= 20, key <= 49]

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
