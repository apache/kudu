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

from kudu.compat import unittest
from kudu.util import *


class TestUtil(unittest.TestCase):

    def setUp(self):
        context = getcontext()
        # By default Decimal objects in Python use a precision of 28
        # Kudu can support up to 38. We support passing this context
        # on a per call basis if the user wants to adjust this value.
        context.prec = 38

    def test_to_unscaled_decimal(self):
        self.assertEqual(0, to_unscaled_decimal(Decimal('0')))
        self.assertEqual(12345, to_unscaled_decimal(Decimal('123.45')))
        self.assertEqual(-12345, to_unscaled_decimal(Decimal('-123.45')))
        self.assertEqual(12345, to_unscaled_decimal(Decimal('12345')))
        self.assertEqual(10000, to_unscaled_decimal(Decimal('10000')))
        self.assertEqual(10000, to_unscaled_decimal(Decimal('0.10000')))
        self.assertEqual(1, to_unscaled_decimal(Decimal('1')))
        self.assertEqual(1, to_unscaled_decimal(Decimal('.1')))
        self.assertEqual(1, to_unscaled_decimal(Decimal('0.1')))
        self.assertEqual(1, to_unscaled_decimal(Decimal('0.01')))
        self.assertEqual(999999999, to_unscaled_decimal(Decimal('0.999999999')))
        self.assertEqual(999999999999999999, to_unscaled_decimal(Decimal('0.999999999999999999')))
        self.assertEqual(99999999999999999999999999999999999999,
               to_unscaled_decimal(Decimal('0.99999999999999999999999999999999999999')))

    def test_from_unscaled_decimal(self):
        self.assertEqual(0, from_unscaled_decimal(0, 0))
        self.assertEqual(Decimal('123.45'), from_unscaled_decimal(12345, 2))
        self.assertEqual(Decimal('-123.45'), from_unscaled_decimal(-12345, 2))
        self.assertEqual(Decimal('12345'), from_unscaled_decimal(12345, 0))
        self.assertEqual(Decimal('10000'), from_unscaled_decimal(10000, 0))
        self.assertEqual(Decimal('0.10000'), from_unscaled_decimal(10000, 5))
        self.assertEqual(Decimal('1'), from_unscaled_decimal(1, 0))
        self.assertEqual(Decimal('.1'), from_unscaled_decimal(1, 1))
        self.assertEqual(Decimal('0.1'), from_unscaled_decimal(1, 1))
        self.assertEqual(Decimal('0.01'), from_unscaled_decimal(1, 2))
        self.assertEqual(Decimal('0.999999999'), from_unscaled_decimal(999999999, 9))
        self.assertEqual(Decimal('0.999999999999999999'),
               from_unscaled_decimal(999999999999999999, 18))
        self.assertEqual(Decimal('0.99999999999999999999999999999999999999'),
               from_unscaled_decimal(99999999999999999999999999999999999999, 38))

    def test_get_decimal_scale(self):
        self.assertEqual(0, get_decimal_scale(Decimal('0')))
        self.assertEqual(2, get_decimal_scale(Decimal('123.45')))
        self.assertEqual(2, get_decimal_scale(Decimal('-123.45')))
        self.assertEqual(0, get_decimal_scale(Decimal('12345')))
        self.assertEqual(0, get_decimal_scale(Decimal('10000')))
        self.assertEqual(5, get_decimal_scale(Decimal('0.10000')))
        self.assertEqual(0, get_decimal_scale(Decimal('1')))
        self.assertEqual(1, get_decimal_scale(Decimal('.1')))
        self.assertEqual(1, get_decimal_scale(Decimal('0.1')))
        self.assertEqual(2, get_decimal_scale(Decimal('0.01')))
        self.assertEqual(9, get_decimal_scale(Decimal('0.999999999')))
        self.assertEqual(18, get_decimal_scale(Decimal('0.999999999999999999')))
        self.assertEqual(38, get_decimal_scale(Decimal('0.99999999999999999999999999999999999999')))
