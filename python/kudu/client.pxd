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

from libkudu_client cimport *
from kudu.schema cimport Schema


cdef class Session:
    cdef:
        shared_ptr[KuduSession] s


cdef class PartialRow:
    cdef:
        KuduPartialRow* row
        Schema schema
        public bint _own

    cpdef set_field(self, key, value)

    cpdef set_loc(self, int i, value)

    cpdef set_field_null(self, key)

    cpdef set_loc_null(self, int i)

    cdef add_to_session(self, Session s)
