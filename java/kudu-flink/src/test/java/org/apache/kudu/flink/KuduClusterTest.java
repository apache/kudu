/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kudu.flink;

import java.util.ArrayList;
import java.util.List;
import org.apache.kudu.client.FakeDNS;
import org.apache.kudu.client.MiniKuduCluster;
import org.apache.kudu.shaded.com.google.common.net.HostAndPort;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class KuduClusterTest implements AutoCloseable {

    private static MiniKuduCluster miniCluster;
    protected static String hostsCluster;

    @BeforeClass
    public static void initCluster() throws Exception {
        KuduBinaries.initialize();
        FakeDNS.getInstance().install();
        miniCluster = new MiniKuduCluster.MiniKuduClusterBuilder().numMasters(3).numTservers(3).build();

        hostsCluster = miniCluster.getMasterAddresses();
    }

    @AfterClass
    public static void tearDownCluster() throws Exception {
        if (miniCluster != null) {
            miniCluster.shutdown();
        }
        KuduBinaries.terminate();
    }

    @Test
    public void testClusterConfig() {
        Assert.assertNotNull("cluster not configurated", miniCluster);
        Assert.assertNotNull("hosts not configurated", hostsCluster);
    }

    @Override
    public void close() throws Exception {
        tearDownCluster();
    }

}
