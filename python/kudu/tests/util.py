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
            elif i % 3 == 0:
                op['string_val'] = None
            op['unixtime_micros_val'] = tup[3]
            session.apply(op)
            tuples.append(tup)
        session.flush()

        self.table = table
        self.tuples = tuples

    def setUp(self):
        pass

    def insert_new_unixtime_micros_rows(self):
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