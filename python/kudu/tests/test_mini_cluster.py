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

import kudu
from kudu.client import Partitioning
from kudu.compat import CompatUnitTest
from kudu.errors import KuduNotSupported
from kudu.tests.common import KuduTestBase, master_flags


class TestMasterTserverFlags(KuduTestBase, CompatUnitTest):
    """Tests demonstrating per-test master/tserver flag injection.

    Uses --master_support_auto_incrementing_column as a concrete example of
    a feature-flag-guarded feature.  The positive test runs on the shared
    cluster (flag defaults to true) and verifies the feature works; the
    negative test uses @master_flags to start a dedicated cluster with the
    flag disabled and verifies the client raises KuduNotSupported.
    """

    def _auto_increment_schema(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', kudu.int32)
         .nullable(False)
         .non_unique_primary_key())
        builder.add_column('data', kudu.string)
        return builder.build()

    def test_auto_incrementing_column_enabled_by_default(self):
        """Auto-incrementing columns work when the feature flag is on (default)."""
        # Verify setUp did not inject a dedicated cluster: self.client resolves
        # to the class-level client (shared cluster).
        shared_cluster_client = type(self).client
        self.assertIs(self.client, shared_cluster_client)

        table_name = 'test_auto_inc_enabled'
        try:
            self.client.create_table(
                table_name, self._auto_increment_schema(),
                partitioning=Partitioning().add_hash_partitions(['key'], 2))
            table = self.client.table(table_name)
            session = self.client.new_session()
            op = table.new_insert()
            op['key'] = 1
            op['data'] = 'hello'
            session.apply(op)
            session.flush()
            rows = table.scanner().open().read_all_tuples()
            self.assertEqual(1, len(rows))
        finally:
            try:
                self.client.delete_table(table_name)
            except Exception:
                pass

    @master_flags("--master_support_auto_incrementing_column=false")
    def test_auto_incrementing_column_rejected_when_disabled(self):
        """Disabling the feature flag causes KuduNotSupported on table creation."""
        # Verify setUp injected a dedicated cluster for this test.
        dedicated_cluster_client = self.client
        shared_cluster_client = type(self).client
        self.assertIsNot(dedicated_cluster_client, shared_cluster_client)

        error_msg = (r"cluster does not support CreateTable with "
                     r"feature\(s\) AUTO_INCREMENTING_COLUMN")
        with self.assertRaisesRegex(KuduNotSupported, error_msg):
            self.client.create_table(
                'test_auto_inc_disabled', self._auto_increment_schema(),
                partitioning=Partitioning().add_hash_partitions(['key'], 2))
