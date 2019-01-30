// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.kudu.examples;

import static org.junit.Assert.assertTrue;

import org.apache.kudu.client.KuduException;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.Rule;
import org.junit.Test;

/**
 * An example integration test class that spins up a local Kudu cluster.<br/>
 * <br/>
 * See the <a href="https://kudu.apache.org/docs/developing.html#_jvm_based_integration_testing">JVM Testing Docs</a>
 * for more details.
 */
public class ExampleTest {

    /**
     * A Junit Rule that manages a Kudu cluster and clients for testing.
     * This rule also includes utility methods for the cluster
     * and clients.
     */
    @Rule
    public KuduTestHarness harness = new KuduTestHarness();

    @Test
    public void testCreateExampleTable() throws KuduException {
        String tableName = "test_create_example";
        Example.createExampleTable(harness.getClient(), tableName);
        assertTrue(harness.getClient().tableExists(tableName));
    }
}
