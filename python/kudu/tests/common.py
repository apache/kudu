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

import json
import os
import subprocess

import kudu
from kudu.client import Partitioning

# There's no built-in timeout error in Python 2.
# See https://stackoverflow.com/questions/2281850.
class TimeoutError(Exception):
    pass

class KuduTestBase(object):

    """
    Base test class that will start a configurable number of master and
    tablet servers.
    """

    NUM_MASTER_SERVERS = 3
    NUM_TABLET_SERVERS = 3

    @classmethod
    def send_and_receive(cls, proc, request):
        binary_req = (json.dumps(request) + "\n").encode("utf-8")
        proc.stdin.write(binary_req)
        proc.stdin.flush()
        binary_resp = proc.stdout.readline()
        response = json.loads(binary_resp[:-1].decode("utf-8"))
        if "error" in response:
            raise Exception("Error in response: {0}".format(response["error"]))
        return response

    @classmethod
    def start_cluster(cls):
        kudu_build = os.getenv("KUDU_BUILD")
        if not kudu_build:
            kudu_build = os.path.join(os.getenv("KUDU_HOME"), "build", "latest")
        bin_path = "{0}/bin".format(kudu_build)

        master_hosts = []
        master_ports = []

        # Start the mini-cluster control process.
        args = ["{0}/kudu".format(bin_path), "test", "mini_cluster"]
        p = subprocess.Popen(args, shell=False,
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE)

        # Create and start a cluster.
        #
        # Only make one replica so that our tests don't need to worry about
        # setting consistency modes.
        cls.send_and_receive(
            p, { "create_cluster" :
                 { "numMasters" : cls.NUM_MASTER_SERVERS,
                   "numTservers" : cls.NUM_TABLET_SERVERS,
                   "extraMasterFlags" : [ "--default_num_replicas=1" ]}})
        cls.send_and_receive(p, { "start_cluster" : {}})

        # Get information about the cluster's masters.
        masters = cls.send_and_receive(p, { "get_masters" : {}})
        for m in masters["getMasters"]["masters"]:
            master_hosts.append(m["boundRpcAddress"]["host"])
            master_ports.append(m["boundRpcAddress"]["port"])

        return p, master_hosts, master_ports

    @classmethod
    def stop_cluster(cls):
        cls.cluster_proc.stdin.close()
        ret = cls.cluster_proc.wait()
        if ret != 0:
            raise Exception("Minicluster process exited with code {0}".format(ret))

    @classmethod
    def setUpClass(cls):
        cls.cluster_proc, cls.master_hosts, cls.master_ports = cls.start_cluster()
        cls.client = kudu.connect(cls.master_hosts, cls.master_ports)

        cls.schema = cls.example_schema()
        cls.partitioning = cls.example_partitioning()

        cls.ex_table = 'example-table'
        if cls.client.table_exists(cls.ex_table):
            cls.client.delete_table(cls.ex_table)
        cls.client.create_table(cls.ex_table, cls.schema, cls.partitioning)

    @classmethod
    def tearDownClass(cls):
        cls.stop_cluster()

    @classmethod
    def example_schema(cls):
        builder = kudu.schema_builder()
        builder.add_column('key', kudu.int32, nullable=False)
        builder.add_column('int_val', kudu.int32)
        builder.add_column('string_val', kudu.string, default='nothing')
        builder.add_column('unixtime_micros_val', kudu.unixtime_micros)
        builder.set_primary_keys(['key'])

        return builder.build()

    @classmethod
    def example_partitioning(cls):
        return Partitioning().set_range_partition_columns(['key'])
