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

# distutils: language = c++
# cython: embedsignature = True

include "config.pxi"

from libcpp.string cimport string
from libcpp cimport bool as c_bool
from libcpp.map cimport map

cimport cpython
from cython.operator cimport dereference as deref

from libkudu_client cimport *
from kudu.compat import tobytes, frombytes, dict_iter
from kudu.schema cimport Schema, ColumnSchema, ColumnSpec, KuduValue, KuduType
from kudu.errors cimport check_status
from kudu.util import to_unixtime_micros, from_unixtime_micros, \
    from_hybridtime, to_unscaled_decimal, from_unscaled_decimal
from errors import KuduException

import six

# True if this python client was compiled with a
# compiler that supports __int128 which is required
# for decimal type support. This is generally true
# except for EL6 environments.
IF PYKUDU_INT128_SUPPORTED == 1:
    CLIENT_SUPPORTS_DECIMAL = True
ELSE:
    CLIENT_SUPPORTS_DECIMAL = False

try:
    import pandas
    CLIENT_SUPPORTS_PANDAS = True
except ImportError:
    CLIENT_SUPPORTS_PANDAS = False

# Replica selection enums
LEADER_ONLY = ReplicaSelection_Leader
CLOSEST_REPLICA = ReplicaSelection_Closest
FIRST_REPLICA = ReplicaSelection_First

cdef dict _replica_selection_policies = {
    'leader': ReplicaSelection_Leader,
    'closest': ReplicaSelection_Closest,
    'first': ReplicaSelection_First
}

# Read mode enums
READ_LATEST = ReadMode_Latest
READ_AT_SNAPSHOT = ReadMode_Snapshot
READ_YOUR_WRITES = ReadMode_ReadYourWrites

cdef dict _read_modes = {
    'latest': ReadMode_Latest,
    'snapshot': ReadMode_Snapshot,
    'read_your_writes': ReadMode_ReadYourWrites
}

cdef dict _type_names = {
    KUDU_INT8 : "KUDU_INT8",
    KUDU_INT16 : "KUDU_INT16",
    KUDU_INT32 : "KUDU_INT32",
    KUDU_INT64 : "KUDU_INT64",
    KUDU_STRING : "KUDU_STRING",
    KUDU_BOOL : "KUDU_BOOL",
    KUDU_FLOAT : "KUDU_FLOAT",
    KUDU_DOUBLE : "KUDU_DOUBLE",
    KUDU_BINARY : "KUDU_BINARY",
    KUDU_UNIXTIME_MICROS : "KUDU_UNIXTIME_MICROS",
    KUDU_DECIMAL : "KUDU_DECIMAL",
    KUDU_VARCHAR : "KUDU_VARCHAR"
}

# Range Partition Bound Type enums
EXCLUSIVE_BOUND = PartitionType_Exclusive
INCLUSIVE_BOUND = PartitionType_Inclusive

cdef dict _partition_bound_types = {
    'exclusive': PartitionType_Exclusive,
    'inclusive': PartitionType_Inclusive
}

def _check_convert_range_bound_type(bound):
    # Convert bounds types to constants and raise exception if invalid.
    def invalid_bound_type(bound_type):
        raise ValueError('Invalid range partition bound type: {0}'
                         .format(bound_type))

    if isinstance(bound, int):
        if bound >= len(_partition_bound_types) \
                or bound < 0:
            invalid_bound_type(bound)
        else:
            return bound
    else:
        try:
            return _partition_bound_types[bound.lower()]
        except KeyError:
            invalid_bound_type(bound)

def _correct_pandas_data_type(dtype):
    """
    This method returns the correct Pandas data type for some data types that
    are converted incorrectly by Pandas.

    Returns
    -------
    pdtype : type
    """
    import numpy as np
    if dtype == "int8":
        return np.int8
    if dtype == "int16":
        return np.int16
    if dtype == "int32":
        return np.int32
    if dtype == "float":
        return np.float32
    else:
        return None

cdef class TimeDelta:
    """
    Wrapper interface for kudu MonoDelta class, which is used to specify
    timedeltas for timeouts and other uses.
    """

    cdef:
        MonoDelta delta

    def __cinit__(self):
        pass

    @staticmethod
    def from_seconds(seconds):
        """
        Construct a new TimeDelta from fractional seconds.

        Parameters
        ----------
        seconds : double

        Returns
        -------
        delta : TimeDelta
        """
        cdef TimeDelta result = TimeDelta()
        result.init(MonoDelta.FromSeconds(seconds))
        return result

    @staticmethod
    def from_millis(int64_t ms):
        """
        Construct a new TimeDelta from integer milliseconds.

        Parameters
        ----------
        ms : int

        Returns
        -------
        delta : TimeDelta
        """
        cdef TimeDelta result = TimeDelta()
        result.init(MonoDelta.FromMilliseconds(ms))
        return result

    @staticmethod
    def from_micros(int64_t us):
        """
        Construct a new TimeDelta from integer microseconds.

        Parameters
        ----------
        us : int

        Returns
        -------
        delta : TimeDelta
        """
        cdef TimeDelta result = TimeDelta()
        result.init(MonoDelta.FromMicroseconds(us))
        return result

    @staticmethod
    def from_nanos(seconds):
        """
        Construct a new TimeDelta from integer nanoseconds.

        Parameters
        ----------
        ns : int

        Returns
        -------
        delta : TimeDelta
        """
        cdef TimeDelta result = TimeDelta()
        result.init(MonoDelta.FromNanoseconds(seconds))
        return result

    cpdef double to_seconds(self):
        """
        Return timedelta as fractional seconds.
        """
        return self.delta.ToSeconds()

    cpdef int64_t to_millis(self):
        """
        Return timedelta as exact milliseconds.
        """
        return self.delta.ToMilliseconds()

    cpdef int64_t to_micros(self):
        """
        Return timedelta as exact microseconds.
        """
        return self.delta.ToMicroseconds()

    cpdef int64_t to_nanos(self):
        """
        Return timedelta as exact nanoseconds.
        """
        return self.delta.ToNanoseconds()

    cdef init(self, const MonoDelta& val):
        self.delta = val

    def __repr__(self):
        cdef object as_string

        if self.delta.Initialized():
            as_string = self.delta.ToString()
            return 'kudu.TimeDelta({0})'.format(as_string)
        else:
            return 'kudu.TimeDelta()'

    def __richcmp__(TimeDelta self, TimeDelta other, int op):
        if op == cpython.Py_EQ:
            return self.delta.Equals(other.delta)
        elif op == cpython.Py_NE:
            return not self.delta.Equals(other.delta)
        elif op == cpython.Py_LT:
            return self.delta.LessThan(other.delta)
        elif op == cpython.Py_LE:
            return not self.delta.MoreThan(other.delta)
        elif op == cpython.Py_GT:
            return self.delta.MoreThan(other.delta)
        elif op == cpython.Py_GE:
            return not self.delta.LessThan(other.delta)
        else:
            raise ValueError('invalid operation: {0}'.format(op))


cdef class Client:

    """
    The primary class for interacting with a Kudu cluster. Can connect to one
    or more Kudu master servers. Do not instantiate this class directly; use
    kudu.connect instead.
    """

    def __cinit__(self, addr_or_addrs, admin_timeout_ms=None,
                  rpc_timeout_ms=None):
        cdef:
            string c_addr
            vector[string] c_addrs
            KuduClientBuilder builder
            TimeDelta timeout

        # Python programs will often have already imported _ssl, which
        # has the side effect of initializing OpenSSL. So, we detect
        # whether _ssl is present, and if we can import it, we disable
        # Kudu's initialization to avoid a conflict.
        try:
            import _ssl
        except:
            pass
        else:
            check_status(DisableOpenSSLInitialization())

        if isinstance(addr_or_addrs, six.string_types):
            addr_or_addrs = [addr_or_addrs]
        elif not isinstance(addr_or_addrs, list):
            addr_or_addrs = list(addr_or_addrs)

        # Raise exception for empty iters, otherwise the connection call
        # will hang
        if not addr_or_addrs:
            raise ValueError("Empty iterator for addr_or_addrs.")

        self.master_addrs = addr_or_addrs
        for addr in addr_or_addrs:
            c_addrs.push_back(tobytes(addr))

        builder.master_server_addrs(c_addrs)

        if admin_timeout_ms is not None:
            timeout = TimeDelta.from_millis(admin_timeout_ms)
            builder.default_admin_operation_timeout(timeout.delta)

        if rpc_timeout_ms is not None:
            timeout = TimeDelta.from_millis(rpc_timeout_ms)
            builder.default_rpc_timeout(timeout.delta)

        check_status(builder.Build(&self.client))

        # A convenience
        self.cp = self.client.get()

    def __dealloc__(self):
        self.close()

    property is_multimaster:

        def __get__(self):
            return self.cp.IsMultiMaster()

    cpdef close(self):
        # Nothing yet to clean up here
        pass

    def latest_observed_timestamp(self):
        """
        Get the highest timestamp observed by the client in UTC. This
        is intended to gain external consistency across clients.

        Note: The latest observed timestamp can also be used to start a
        snapshot scan on a table which is guaranteed to contain all data
        written or previously read by this client. This should be treated
        as experimental as it this method will change or disappear in a
        future release. Additionally, note that 1 must be added to the
        value to be used in snapshot reads (this is taken care of in the
        from_hybridtime method).

        Returns
        -------
        latest : datetime.datetime
        """
        return from_hybridtime(self.cp.GetLatestObservedTimestamp())

    def create_table(self, table_name, Schema schema, partitioning, n_replicas=None):
        """
        Creates a new Kudu table from the passed Schema and options.

        Parameters
        ----------
        table_name : string
        schema : kudu.Schema
          Create using kudu.schema_builder
        partitioning : Partitioning object
        n_replicas : int Number of replicas to set. This should be an odd number.
          If not provided (or if <= 0), falls back to the server-side default.
        """
        cdef:
            KuduTableCreator* c
            Status s
        c = self.cp.NewTableCreator()
        try:
            c.table_name(tobytes(table_name))
            c.schema(schema.schema)
            self._apply_partitioning(c, partitioning, schema)
            if n_replicas:
                c.num_replicas(n_replicas)
            s = c.Create()
            check_status(s)
        finally:
            del c

    cdef _apply_partitioning(self, KuduTableCreator* c, part, Schema schema):
        cdef:
            vector[string] v
            PartialRow lower_bound
            PartialRow upper_bound
            PartialRow split_row

        # Apply hash partitioning.
        for col_names, num_buckets, seed in part._hash_partitions:
            v.clear()
            for n in col_names:
                v.push_back(tobytes(n))
            if seed:
                c.add_hash_partitions(v, num_buckets, seed)
            else:
                c.add_hash_partitions(v, num_buckets)
        # Apply range partitioning
        if part._range_partition_cols is not None:
            v.clear()
            for n in part._range_partition_cols:
                v.push_back(tobytes(n))
            c.set_range_partition_columns(v)
            if part._range_partitions:
                for partition in part._range_partitions:
                    if not isinstance(partition[0], PartialRow):
                        lower_bound = schema.new_row(partition[0])
                    else:
                        lower_bound = partition[0]
                    lower_bound._own = 0
                    if not isinstance(partition[1], PartialRow):
                        upper_bound = schema.new_row(partition[1])
                    else:
                        upper_bound = partition[1]
                    upper_bound._own = 0
                    c.add_range_partition(
                        lower_bound.row,
                        upper_bound.row,
                        _check_convert_range_bound_type(partition[2]),
                        _check_convert_range_bound_type(partition[3])
                    )
            if part._range_partition_splits:
                for split in part._range_partition_splits:
                    if not isinstance(split, PartialRow):
                        split_row = schema.new_row(split)
                    else:
                        split_row = split
                    split_row._own = 0
                    c.add_range_partition_split(split_row.row)

    def delete_table(self, table_name):
        """
        Delete a Kudu table. Raises KuduNotFound if the table does not exist.

        Parameters
        ----------
        table_name : string
        """
        check_status(self.cp.DeleteTable(tobytes(table_name)))

    def table_exists(self, table_name):
        """Return True if the indicated table exists in the Kudu cluster.

        Parameters
        ----------
        table_name : string

        Returns
        -------
        exists : bool

        """
        cdef:
            string c_name = tobytes(table_name)
            c_bool exists

        check_status(self.cp.TableExists(c_name, &exists))
        return exists

    def deserialize_token_into_scanner(self, serialized_token):
        """
        Deserializes a ScanToken using the client and returns a scanner.

        Parameters
        ----------
        serialized_token : String
          Serialized form of a ScanToken.

        Returns
        -------
        scanner : Scanner
        """
        token = ScanToken()
        return token.deserialize_into_scanner(self, serialized_token)

    def table(self, table_name):
        """
        Construct a kudu.Table and retrieve its schema from the cluster.

        Raises KuduNotFound if the table does not exist.

        Parameters
        ----------
        table_name : string

        Returns
        -------
        table : kudu.Table
        """
        table_name = tobytes(table_name)
        cdef Table table = Table(table_name, self)

        check_status(self.cp.OpenTable(table_name, &table.table))
        table.init()
        return table

    def list_tables(self, match_substring=None):
        """
        Retrieve a list of table names in the Kudu cluster with an optional
        substring filter.

        Parameters
        ----------
        match_substring : string, optional
          If passed, the string must be exactly contained in the table names

        Returns
        -------
        tables : list[string]
          Table names returned from Kudu
        """
        cdef:
            vector[string] tables
            string c_match
            size_t i

        if match_substring is not None:
            c_match = tobytes(match_substring)
            check_status(self.cp.ListTables(&tables, c_match))
        else:
            check_status(self.cp.ListTables(&tables))

        result = []
        for i in range(tables.size()):
            result.append(frombytes(tables[i]))
        return result

    def list_tablet_servers(self):
        """
        Retrieve a list of tablet servers currently running in the Kudu cluster

        Returns
        -------
        tservers : list[TabletServer]
          List of TabletServer objects
        """
        cdef:
            vector[KuduTabletServer*] tservers
            size_t i

        check_status(self.cp.ListTabletServers(&tservers))

        result = []
        for i in range(tservers.size()):
            ts = TabletServer()
            ts._own = 1
            result.append(ts._init(tservers[i]))
        return result

    def new_session(self, flush_mode='manual', timeout_ms=5000, **kwargs):
        """
        Create a new KuduSession for applying write operations.

        Parameters
        ----------
        flush_mode : {'manual', 'sync', 'background'}, default 'manual'
          See Session.set_flush_mode
        timeout_ms : int, default 5000
          Timeout in milliseconds
        mutation_buffer_sz : Size in bytes of the buffer space.
        mutation_buffer_watermark : Watermark level as percentage of the mutation buffer size,
            this is used to trigger a flush in AUTO_FLUSH_BACKGROUND mode.
        mutation_buffer_flush_interval : The duration of the interval for the time-based
            flushing, in milliseconds. In some cases, while running in AUTO_FLUSH_BACKGROUND
            mode, the size of the mutation buffer for pending operations and the flush
            watermark for fresh operations may be too high for the rate of incoming data:
            it would take too long to accumulate enough data in the buffer to trigger
            flushing. I.e., it makes sense to flush the accumulated operations if the
            prior flush happened long time ago. This parameter sets the wait interval for
            the time-based flushing which takes place along with the flushing triggered
            by the over-the-watermark criterion. By default, the interval is set to
            1000 ms (i.e. 1 second).
        mutation_buffer_max_num : The maximum number of mutation buffers per KuduSession
            object to hold the applied operations. Use 0 to set the maximum number of
            concurrent mutation buffers to unlimited

        Returns
        -------
        session : kudu.Session
        """

        cdef Session result = Session()
        result.s = self.cp.NewSession()

        result.set_flush_mode(flush_mode)
        result.set_timeout_ms(timeout_ms)

        if "mutation_buffer_sz" in kwargs:
            result.set_mutation_buffer_space(kwargs["mutation_buffer_sz"])
        if "mutation_buffer_watermark" in kwargs:
            result.set_mutation_buffer_flush_watermark(kwargs["mutation_buffer_watermark"])
        if "mutation_buffer_flush_interval" in kwargs:
            result.set_mutation_buffer_flush_interval(kwargs["mutation_buffer_flush_interval"])
        if "mutation_buffer_max_num" in kwargs:
            result.set_mutation_buffer_max_num(kwargs["mutation_buffer_max_num"])

        return result

    def new_table_alterer(self, Table table):
        """
        Create a TableAlterer object that can be used to apply a set of steps
        to alter a table.

        Parameters
        ----------
        table : Table
          Table to alter. NOTE: The TableAlterer.alter() method will return
          a new Table object with the updated information.

        Examples
        --------
        table = client.table('example')
        alterer = client.new_table_alterer(table)
        table = alterer.rename('example2').alter()

        Returns
        -------
        alterer : TableAlterer
        """
        return TableAlterer(table)


#----------------------------------------------------------------------
# Handle marshalling Python values to raw values. Since range predicates
# require a const void*, this is one valid (though a bit verbose)
# approach. Note that later versions of Cython handle many Python -> C type
# casting problems (and integer overflows), but these should all be tested
# rigorously in our test suite


cdef class RawValue:
    cdef:
        void* data

    def __cinit__(self):
        self.data = NULL


cdef class Int8Val(RawValue):
    cdef:
        int8_t val

    def __cinit__(self, obj):
        self.val = <int8_t> obj
        self.data = &self.val


cdef class Int16Val(RawValue):
    cdef:
        int16_t val

    def __cinit__(self, obj):
        self.val = <int16_t> obj
        self.data = &self.val


cdef class Int32Val(RawValue):
    cdef:
        int32_t val

    def __cinit__(self, obj):
        self.val = <int32_t> obj
        self.data = &self.val


cdef class Int64Val(RawValue):
    cdef:
        int64_t val

    def __cinit__(self, obj):
        self.val = <int64_t> obj
        self.data = &self.val


cdef class DoubleVal(RawValue):
    cdef:
        double val

    def __cinit__(self, obj):
        self.val = <double> obj
        self.data = &self.val


cdef class FloatVal(RawValue):
    cdef:
        float val

    def __cinit__(self, obj):
        self.val = <float> obj
        self.data = &self.val


cdef class BoolVal(RawValue):
    cdef:
        c_bool val

    def __cinit__(self, obj):
        self.val = <c_bool> obj
        self.data = &self.val


cdef class StringVal(RawValue):
    cdef:
        # Python "str" object that was passed into the constructor.
        # We hold a reference to this so that the underlying data
        # doesn't go out of scope.
        object py_str
        # Heap-allocated Slice object, owned by this instance,
        # which points to the data in 'py_str'
        cdef Slice* val

    def __cinit__(self, obj):
        self.py_str = obj
        self.val = new Slice(<char*>self.py_str, len(self.py_str))
        # The C++ API expects a Slice* to be passed to the range predicate
        # constructor.
        self.data = self.val

    def __dealloc__(self):
        del self.val

cdef class UnixtimeMicrosVal(RawValue):
    cdef:
        int64_t val

    def __cinit__(self, obj):
        self.val = to_unixtime_micros(obj)
        self.data = &self.val

#----------------------------------------------------------------------
cdef class TabletServer:
    """
    Represents a Kudu tablet server, containing the uuid, hostname and port.
    Create a list of TabletServers by using the kudu.Client.list_tablet_servers
    method after connecting to a cluster
    """

    cdef:
        const KuduTabletServer* _tserver
        public bint _own

    cdef _init(self, const KuduTabletServer* tserver):
        self._tserver = tserver
        self._own = 0
        return self

    def __dealloc__(self):
        if self._tserver != NULL and self._own:
            del self._tserver

    def __richcmp__(TabletServer self, TabletServer other, int op):
        if op == 2: # ==
            return ((self.uuid(), self.hostname(), self.port()) ==
                    (other.uuid(), other.hostname(), other.port()))
        elif op == 3: # !=
            return ((self.uuid(), self.hostname(), self.port()) !=
                    (other.uuid(), other.hostname(), other.port()))
        else:
            raise NotImplementedError

    def uuid(self):
        return frombytes(self._tserver.uuid())

    def hostname(self):
        return frombytes(self._tserver.hostname())

    def port(self):
        return self._tserver.port()


cdef class Table:

    """
    Represents a Kudu table, containing the schema and other tools. Create by
    using the kudu.Client.table method after connecting to a cluster.
    """

    def __cinit__(self, name, Client client):
        self._name = name
        self.parent = client

        # Users should not instantiate directly
        self.schema = Schema()

    cdef init(self):
        # Called after the refptr has been populated
        self.schema.schema = &self.ptr().schema()
        self.schema.own_schema = 0
        self.schema.parent = self
        self.num_replicas = self.ptr().num_replicas()

    def __len__(self):
        # TODO: is this cheaply knowable?
        raise NotImplementedError

    def __getitem__(self, key):
        spec = self.schema[key]
        return Column(self, spec)

    property name:
        """Name of the table."""
        def __get__(self):
            return frombytes(self.ptr().name())

    property id:
        """Identifier string for the table."""
        def __get__(self):
            return frombytes(self.ptr().id())

    # XXX: don't love this name
    property num_columns:
        """Number of columns in the table's schema."""
        def __get__(self):
            return len(self.schema)

    def rename(self, new_name):
        raise NotImplementedError

    def drop(self):
        raise NotImplementedError

    def new_insert(self, record=None):
        """
        Create a new Insert operation. Pass the completed Insert to a Session.
        If a record is provided, a PartialRow will be initialized with values
        from the input record. The record can be in the form of a tuple, dict,
        or list. Dictionary keys can be either column names, indexes, or a
        mix of both names and indexes.

        Parameters
        ----------
        record : tuple/list/dict

        Returns
        -------
        insert : Insert
        """
        return Insert(self, record)

    def new_insert_ignore(self, record=None):
        """
        Create a new InsertIgnore operation. Pass the completed InsertIgnore to a Session.
        If a record is provided, a PartialRow will be initialized with values
        from the input record. The record can be in the form of a tuple, dict,
        or list. Dictionary keys can be either column names, indexes, or a
        mix of both names and indexes.

        Parameters
        ----------
        record : tuple/list/dict

        Returns
        -------
        insertIgnore : InsertIgnore
        """
        return InsertIgnore(self, record)

    def new_upsert(self, record=None):
        """
        Create a new Upsert operation. Pass the completed Upsert to a Session.
        If a record is provided, a PartialRow will be initialized with values
        from the input record. The record can be in the form of a tuple, dict,
        or list. Dictionary keys can be either column names, indexes, or a
        mix of both names and indexes.

        Parameters
        ----------
        record : tuple/list/dict

        Returns
        -------
        upsert : Upsert
        """
        return Upsert(self, record)

    def new_update(self, record=None):
        """
        Create a new Update operation. Pass the completed Update to a Session.
        If a record is provided, a PartialRow will be initialized with values
        from the input record. The record can be in the form of a tuple, dict,
        or list. Dictionary keys can be either column names, indexes, or a
        mix of both names and indexes.

        Parameters
        ----------
        record : tuple/list/dict

        Returns
        -------
        update : Update
        """
        return Update(self, record)

    def new_delete(self, record=None):
        """
        Create a new Delete operation. Pass the completed Update to a Session.
        If a record is provided, a PartialRow will be initialized with values
        from the input record. The record can be in the form of a tuple, dict,
        or list. Dictionary keys can be either column names, indexes, or a
        mix of both names and indexes.

        Parameters
        ----------
        record : tuple/list/dict

        Returns
        -------
        delete : Delete
        """
        return Delete(self, record)

    def scanner(self):
        """
        Create a new scanner for this table for retrieving a selection of table
        rows.

        Examples
        --------
        scanner = table.scanner()
        scanner.add_predicate(table['key'] > 10)
        scanner.open()
        batch = scanner.read_all()
        tuples = batch.as_tuples()

        Returns
        -------
        scanner : kudu.Scanner
        """
        cdef Scanner result = Scanner(self)
        result.scanner = new KuduScanner(self.ptr())
        return result

    def scan_token_builder(self):
        """
        Create a new ScanTokenBuilder for this table to build a series of
        scan tokens.

        Examples
        --------
        builder = table.scan_token_builder()
        builder.set_fault_tolerant().add_predicate(table['key'] > 10)
        tokens = builder.build()
        for token in tokens:
            scanner = token.into_kudu_scanner()
            scanner.open()
            tuples = scanner.read_all_tuples()

        Returns
        -------
        builder : ScanTokenBuilder
        """
        return ScanTokenBuilder(self)


cdef class Column:

    """
    A reference to a Kudu table column intended to simplify creating predicates
    and other column-specific operations.

    Write arithmetic comparisons to create new Predicate objects that can be
    passed to a Scanner.

    Examples
    --------
    scanner.add_predicate(table[col_name] <= 10)
    """
    cdef readonly:
        Table parent
        ColumnSchema spec
        str name

    def __cinit__(self, Table parent, ColumnSchema spec):
        self.name = spec.name
        self.parent = parent
        self.spec = spec

    def __repr__(self):
        result = ('Column({0}, parent={1}, type={2})'
                  .format(self.name,
                          self.parent.name,
                          self.spec.type.name))
        return result

    def __richcmp__(Column self, value, int op):
        cdef:
            KuduPredicate* pred
            KuduValue val
            Slice col_name_slice
            ComparisonOp cmp_op
            Predicate result
            object _name = tobytes(self.name)

        col_name_slice = Slice(<char*> _name, len(_name))
        if op == 0: # <
            cmp_op = KUDU_LESS
        elif op == 1: # <=
            cmp_op = KUDU_LESS_EQUAL
        elif op == 2: # ==
            cmp_op = KUDU_EQUAL
        elif op == 4: # >
            cmp_op = KUDU_GREATER
        elif op == 5: # >=
            cmp_op = KUDU_GREATER_EQUAL
        else:
            raise NotImplementedError

        val = self.spec.type.new_value(value)
        pred = (self.parent.ptr()
                .NewComparisonPredicate(col_name_slice,
                                        cmp_op, val._value))

        result = Predicate()
        result.init(pred)

        return result

    def in_list(Column self, values):
        """
        Creates a new InListPredicate for the Column. If a single value is
        provided, then an equality comparison predicate is created.

        Parameters
        ----------
        values : list

        Examples
        --------
        scanner.add_predicate(table['key'].in_list([1, 2, 3])

        Returns
        -------
        pred : Predicate
        """
        cdef:
            KuduPredicate* pred
            KuduValue kval
            vector[C_KuduValue*] vals
            Slice col_name_slice
            Predicate result
            object _name = tobytes(self.name)

        col_name_slice = Slice(<char*> _name, len(_name))

        try:
            for val in values:
                kval = self.spec.type.new_value(val)
                vals.push_back(kval._value)
        except TypeError:
            while not vals.empty():
                _val = vals.back()
                del _val
                vals.pop_back()
            raise

        pred = (self.parent.ptr()
                .NewInListPredicate(col_name_slice, &vals))

        result = Predicate()
        result.init(pred)

        return result

    def is_not_null(Column self):
        """
        Creates a new IsNotNullPredicate for the Column which can be used for scanners
        on this table.

        Examples
        --------
        scanner.add_predicate(table[col_name].is_not_null())

        Returns
        -------
        pred : Predicate
        """

        cdef:
            KuduPredicate* pred
            Slice col_name_slice
            Predicate result
            object _name = tobytes(self.name)

        col_name_slice = Slice(<char*> _name, len(_name))

        pred = (self.parent.ptr()
                .NewIsNotNullPredicate(col_name_slice))

        result = Predicate()
        result.init(pred)

        return result

    def is_null(Column self):
        """
        Creates a new IsNullPredicate for the Column which can be used for scanners on this table.

        Examples
        --------
        scanner.add_predicate(table[col_name].is_null())

        Returns
        -------
        pred : Predicate
        """

        cdef:
            KuduPredicate* pred
            Slice col_name_slice
            Predicate result
            object _name = tobytes(self.name)

        col_name_slice = Slice(<char*> _name, len(_name))

        pred = (self.parent.ptr()
                .NewIsNullPredicate(col_name_slice))

        result = Predicate()
        result.init(pred)

        return result

class Partitioning(object):
    """ Argument to Client.create_table(...) to describe table partitioning. """

    def __init__(self):
        self._hash_partitions = []
        self._range_partition_cols = None
        self._range_partitions = []
        self._range_partition_splits = []

    def add_hash_partitions(self, column_names, num_buckets, seed=None):
        """
        Adds a set of hash partitions to the table.

        For each set of hash partitions added to the table, the total number of
        table partitions is multiplied by the number of buckets. For example, if a
        table is created with 3 split rows, and two hash partitions with 4 and 5
        buckets respectively, the total number of table partitions will be 80
        (4 range partitions * 4 hash buckets * 5 hash buckets). Optionally, a
        seed can be used to randomize the mapping of rows to hash buckets.
        Setting the seed may provide some amount of protection against denial
        of service attacks when the hashed columns contain user provided values.

        Parameters
        ----------
        column_names : list of string column names on which to partition
        num_buckets : the number of buckets to create
        seed : int - optional
          Hash: seed for mapping rows to hash buckets.

        Returns
        -------
        self: this object
        """
        if isinstance(column_names, str):
            column_names = [column_names]
        self._hash_partitions.append( (column_names, num_buckets, seed) )
        return self

    def set_range_partition_columns(self, column_names):
        """
        Sets the columns on which the table will be range-partitioned.

        Every column must be a part of the table's primary key. If not set, the
        table will be created with the primary-key columns as the range-partition
        columns. If called with an empty vector, the table will be created without
        range partitioning.

        Parameters
        ----------
        column_names : list of string column names on which to partition

        Returns
        -------
        self: this object
        """
        if isinstance(column_names, str):
            column_names = [column_names]
        self._range_partition_cols = column_names
        return self

    def add_range_partition(self, lower_bound=None,
                                  upper_bound=None,
                                  lower_bound_type='inclusive',
                                  upper_bound_type='exclusive'):
        """
        Add a range partition to the table.

        Multiple range partitions may be added, but they must not overlap.
        All range splits specified by add_range_partition_split must fall
        in a range partition. The lower bound must be less than or equal
        to the upper bound.

        If this method is not called, the table's range will be unbounded.

        Parameters
        ----------
        lower_bound : PartialRow/list/tuple/dict
        upper_bound : PartialRow/list/tuple/dict
        lower_bound_type : {'inclusive', 'exclusive'} or constants
          kudu.EXCLUSIVE_BOUND and kudu.INCLUSIVE_BOUND
        upper_bound_type : {'inclusive', 'exclusive'} or constants
          kudu.EXCLUSIVE_BOUND and kudu.INCLUSIVE_BOUND

        Returns
        -------
        self : Partitioning
        """
        if self._range_partition_cols:
            self._range_partitions.append(
                (lower_bound, upper_bound, lower_bound_type, upper_bound_type)
            )
        else:
            raise ValueError("Range Partition Columns must be set before " +
                             "adding a range partition.")

        return self

    def add_range_partition_split(self, split_row):
        """
        Add a range partition split at the provided row.

        Parameters
        ----------
        split_row : PartialRow/list/tuple/dict

        Returns
        -------
        self : Partitioning
        """
        if self._range_partition_cols:
            self._range_partition_splits.append(split_row)
        else:
            raise ValueError("Range Partition Columns must be set before " +
                             "adding a range partition split.")

        return self


cdef class Predicate:

    """
    Wrapper for a KuduPredicate. Pass to Scanner.add_predicates
    """

    cdef:
        KuduPredicate* pred

    def __cinit__(self):
        self.pred = NULL

    def __dealloc__(self):
        if self.pred != NULL:
            del self.pred

    cdef init(self, KuduPredicate* pred):
        self.pred = pred


FLUSH_AUTO_SYNC = FlushMode_AutoSync
FLUSH_AUTO_BACKGROUND = FlushMode_AutoBackground
FLUSH_MANUAL = FlushMode_Manual

cdef dict _flush_modes = {
    'manual': FlushMode_Manual,
    'sync': FlushMode_AutoSync,
    'background': FlushMode_AutoBackground
}


cdef class Session:
    """
    Wrapper for a client KuduSession to build up write operations to interact
    with the cluster.
    """

    def __cinit__(self):
        pass

    def set_flush_mode(self, flush_mode='manual'):
        """
        Set the session operation flush mode

        Parameters
        ----------
        flush_mode : {'manual', 'sync', 'background'}, default 'manual'
          You can also use the constants FLUSH_MANUAL, FLUSH_AUTO_SYNC,
          and FLUSH_AUTO_BACKGROUND
        """
        cdef Status status
        cdef FlushMode fmode

        if isinstance(flush_mode, int):
            # todo: validation
            fmode = <FlushMode> flush_mode
        else:
            try:
                fmode = _flush_modes[flush_mode.lower()]
            except KeyError:
                raise ValueError('Invalid flush mode: {0}'
                                 .format(flush_mode))

        status = self.s.get().SetFlushMode(fmode)

        check_status(status)

    def set_timeout_ms(self, int64_t ms):
        """
        Set the session timeout in milliseconds
        """
        self.s.get().SetTimeoutMillis(ms)

    def set_mutation_buffer_space(self, size_t size_bytes):
        """
        Set the amount of buffer space used by this session for outbound writes.

        The effect of the buffer size varies based on the flush mode of the session:

            AUTO_FLUSH_SYNC: since no buffering is done, this has no effect.
            AUTO_FLUSH_BACKGROUND: if the buffer space is exhausted, then write calls
                will block until there is space available in the buffer.
            MANUAL_FLUSH: if the buffer space is exhausted, then write calls will return
                an error

        By default, the buffer space is set to 7 MiB (i.e. 7 * 1024 * 1024 bytes).

        Parameters
        ----------
        size_bytes : Size of the buffer space to set (number of bytes)
        """
        status = self.s.get().SetMutationBufferSpace(size_bytes)

        check_status(status)

    def set_mutation_buffer_flush_watermark(self, double watermark_pct):
        """
        Set the buffer watermark to trigger flush in AUTO_FLUSH_BACKGROUND mode.

        This method sets the watermark for fresh operations in the buffer when running
        in AUTO_FLUSH_BACKGROUND mode: once the specified threshold is reached, the
        session starts sending the accumulated write operations to the appropriate
        tablet servers. The flush watermark determines how much of the buffer space is
        taken by newly submitted operations. Setting this level to 100% results in
        flushing the buffer only when the newly applied operation would overflow the
        buffer. By default, the buffer flush watermark is set to 50%.

        Parameters
        ----------
        watermark_pct : Watermark level as percentage of the mutation buffer size
        """
        status = self.s.get().SetMutationBufferFlushWatermark(watermark_pct)

        check_status(status)

    def set_mutation_buffer_flush_interval(self, unsigned int millis):
        """
        Set the interval for time-based flushing of the mutation buffer.

        In some cases, while running in AUTO_FLUSH_BACKGROUND mode, the size of the
        mutation buffer for pending operations and the flush watermark for fresh
        operations may be too high for the rate of incoming data: it would take too
        long to accumulate enough data in the buffer to trigger flushing. I.e., it
        makes sense to flush the accumulated operations if the prior flush happened
        long time ago. This method sets the wait interval for the time-based flushing
        which takes place along with the flushing triggered by the over-the-watermark
        criterion. By default, the interval is set to 1000 ms (i.e. 1 second).

        Parameters
        ----------
        millis : The duration of the interval for the time-based flushing, in milliseconds.
        """

        status = self.s.get().SetMutationBufferFlushInterval(millis)

        check_status(status)

    def set_mutation_buffer_max_num(self, unsigned int max_num):
        """
        Set the maximum number of mutation buffers per Session object.

        A Session accumulates write operations submitted via the Apply() method in
        mutation buffers. A Session always has at least one mutation buffer. The
        mutation buffer which accumulates new incoming operations is called the current
        mutation buffer. The current mutation buffer is flushed using the
        Session.flush() method or it's done by the Session automatically if
        running in AUTO_FLUSH_BACKGROUND mode. After flushing the current mutation buffer,
        a new buffer is created upon calling Session.apply(), provided the limit is
        not exceeded. A call to Session.apply() blocks if it's at the maximum number
        of buffers allowed; the call unblocks as soon as one of the pending batchers
        finished flushing and a new batcher can be created.

        The minimum setting for this parameter is 1 (one). The default setting for this
        parameter is 2 (two).

        Parameters
        ----------
        max_num : The maximum number of mutation buffers per Session object to hold
            the applied operations. Use 0 to set the maximum number of concurrent mutation
            buffers to unlimited.
        """

        status = self.s.get().SetMutationBufferMaxNum(max_num)

        check_status(status)

    def apply(self, WriteOperation op):
        """
        Apply the indicated write operation

        Examples
        --------
        # Executes a single Insert operation
        session = client.new_session()
        op = table.new_insert()
        op['key'] = 0
        op['value1'] = 5
        op['value2'] = 3.5
        session.apply(op)
        session.flush()
        """
        return op.add_to_session(self)

    def flush(self):
        """
        Flush pending operations
        """
        check_status(self.s.get().Flush())

    def get_pending_errors(self):
        """
        Returns a list of buffered Kudu errors. A second value is returned
        indicating if there were more errors than could be stored in the
        session's error buffer (i.e. False means there was no error overflow)

        Returns
        -------
        errors, overflowed : list, bool
        """
        cdef:
            KuduError error
            vector[C_KuduError*] v_errors
            c_bool overflowed
            size_t i

        self.s.get().GetPendingErrors(&v_errors, &overflowed)

        result = []
        for i in range(v_errors.size()):
            error = KuduError()
            error.error = v_errors[i]
            result.append(error)

        return result, overflowed


cdef class Row:

    """
    A single row from a row batch
    """

    cdef:
        RowBatch parent

        # This object is owned by the parent RowBatch
        KuduRowPtr row

    def __cinit__(self, batch):
        self.parent = batch

    def __dealloc__(self):
        pass

    cpdef tuple as_tuple(self):
        """
        Return the row as a Python tuple
        """
        cdef:
            int i, k
            tuple tup

        k = self.parent.batch.projection_schema().num_columns()
        tup = cpython.PyTuple_New(k)
        for i in range(k):
            val = None

            if not self.is_null(i):
                val = self.get_slot(i)

            cpython.Py_INCREF(val)
            cpython.PyTuple_SET_ITEM(tup, i, val)

        return tup

    cdef inline get_bool(self, int i):
        cdef c_bool val
        check_status(self.row.GetBool(i, &val))
        # The built-in bool is masked by the libcpp typedef
        return bool(val)

    cdef inline get_int8(self, int i):
        cdef int8_t val
        check_status(self.row.GetInt8(i, &val))
        return val

    cdef inline get_int16(self, int i):
        cdef int16_t val
        check_status(self.row.GetInt16(i, &val))
        return val

    cdef inline get_int32(self, int i):
        cdef int32_t val
        check_status(self.row.GetInt32(i, &val))
        return val

    cdef inline get_int64(self, int i):
        cdef int64_t val
        check_status(self.row.GetInt64(i, &val))
        return val

    cdef inline get_double(self, int i):
        cdef double val
        check_status(self.row.GetDouble(i, &val))
        return val

    cdef inline get_float(self, int i):
        cdef float val
        check_status(self.row.GetFloat(i, &val))
        return val

    cdef inline get_string(self, int i):
        cdef Slice val
        check_status(self.row.GetString(i, &val))
        return cpython.PyBytes_FromStringAndSize(<char*> val.mutable_data(),
                                                 val.size())

    cdef inline get_binary(self, int i):
        cdef Slice val
        check_status(self.row.GetBinary(i, &val))
        return cpython.PyBytes_FromStringAndSize(<char*> val.mutable_data(),
                                                 val.size())

    cdef inline get_unixtime_micros(self, int i):
        cdef int64_t val
        check_status(self.row.GetUnixTimeMicros(i, &val))
        return val

    cdef inline __get_unscaled_decimal(self, int i):
        IF PYKUDU_INT128_SUPPORTED == 1:
            cdef int128_t val
            check_status(self.row.GetUnscaledDecimal(i, &val))
            return val
        ELSE:
            raise KuduException("The decimal type is not supported when GCC version is < 4.6.0" % self)

    cdef inline get_decimal(self, int i):
        scale = self.parent.batch.projection_schema().Column(i).type_attributes().scale()
        return from_unscaled_decimal(self.__get_unscaled_decimal(i), scale)

    cdef inline get_varchar(self, int i):
        cdef Slice val
        check_status(self.row.GetVarchar(i, &val))
        return cpython.PyBytes_FromStringAndSize(<char*> val.mutable_data(),
                                                 val.size())

    cdef inline get_slot(self, int i):
        cdef:
            Status s
            DataType t = self.parent.batch.projection_schema().Column(i).type()

        if t == KUDU_BOOL:
            return self.get_bool(i)
        elif t == KUDU_INT8:
            return self.get_int8(i)
        elif t == KUDU_INT16:
            return self.get_int16(i)
        elif t == KUDU_INT32:
            return self.get_int32(i)
        elif t == KUDU_INT64:
            return self.get_int64(i)
        elif t == KUDU_DOUBLE:
            return self.get_double(i)
        elif t == KUDU_FLOAT:
            return self.get_float(i)
        elif t == KUDU_STRING:
            return frombytes(self.get_string(i))
        elif t == KUDU_BINARY:
            return self.get_binary(i)
        elif t == KUDU_UNIXTIME_MICROS:
            return from_unixtime_micros(self.get_unixtime_micros(i))
        elif t == KUDU_DECIMAL:
            return self.get_decimal(i)
        elif t == KUDU_VARCHAR:
            return frombytes(self.get_varchar(i))
        else:
            raise TypeError("Cannot get kudu type <{0}>"
                                .format(_type_names[t]))

    cdef inline bint is_null(self, int i):
        return self.row.IsNull(i)


cdef class RowBatch:
    """
    Class holding a batch of rows from a Scanner
    """
    # This class owns the KuduScanBatch data
    cdef:
        KuduScanBatch batch

    def __len__(self):
        return self.batch.NumRows()

    def __getitem__(self, i):
        return self.get_row(i).as_tuple()

    def __iter__(self):
        cdef int i = 0
        for i in range(len(self)):
            yield self.get_row(i).as_tuple()

    def as_tuples(self):
        """
        Return RowBatch as a list of Python tuples

        To simplify testing for the moment.
        """
        cdef list tuples = []
        for i in range(self.batch.NumRows()):
            tuples.append(self.get_row(i).as_tuple())
        return tuples

    cdef Row get_row(self, i):
        # TODO: boundscheck

        # For safety, we need to increment the parent reference count and hold
        # on to a reference internally so that if the RowBatch goes out of
        # scope we won't end up with orphaned Row objects. This isn't the best,
        # but an intermediate solution until we can do something better..
        cdef Row row = Row(self)
        row.row = self.batch.Row(i)
        return row


cdef class Scanner:
    """
    A class for defining a selection of data we wish to scan out of a Kudu
    table. Create a scanner using Table.scanner.
    """

    cdef:
        Table table
        KuduScanner* scanner
        bint is_open

    def __cinit__(self, Table table = None):
        self.table = table
        self.scanner = NULL
        self.is_open = 0

    def __dealloc__(self):
        # We own this one
        if self.scanner != NULL:
            del self.scanner

    cdef inline ensure_open(self):
        if not self.is_open:
            self.open()

    def add_predicates(self, preds):
        """
        Add a list of scan predicates to the scanner. Select columns from the
        parent table and make comparisons to create predicates. Returns a
        reference to itself to facilitate chaining.

        Examples
        --------
        c = table[col_name]
        preds = [c >= 0, c <= 10]
        scanner.add_predicates(preds)

        Parameters
        ----------
        preds : list of Predicate

        Returns
        -------
        self : scanner
        """
        for pred in preds:
            self.add_predicate(pred)

        return self

    cpdef add_predicate(self, Predicate pred):
        """
        Add a scan predicates to the scanner. Select columns from the
        parent table and make comparisons to create predicates. Returns
        a reference to itself to facilitate chaining.

        Examples
        --------
        pred = table[col_name] <= 10
        scanner.add_predicate(pred)

        Parameters
        ----------
        pred : kudu.Predicate

        Returns
        -------
        self : scanner
        """
        cdef KuduPredicate* clone

        # We clone the KuduPredicate so that the Predicate wrapper class can be
        # reused
        clone = pred.pred.Clone()
        check_status(self.scanner.AddConjunctPredicate(clone))

        return self

    def set_projected_column_names(self, names):
        """
        Sets the columns to be scanned.

        Parameters
        ----------
        names : list of string

        Returns
        -------
        self : Scanner
        """
        if isinstance(names, str):
            names = [names]
        cdef vector[string] v_names
        for name in names:
            v_names.push_back(tobytes(name))
        check_status(self.scanner.SetProjectedColumnNames(v_names))
        return self

    def set_selection(self, replica_selection):
        """
        Set the replica selection policy while scanning.

        Parameters
        ----------
        replica_selection : {'leader', 'closest', 'first'}
          You can also use the constants LEADER_ONLY, CLOSEST_REPLICA,
          and FIRST_REPLICA

        Returns
        -------
        self : Scanner
        """
        cdef ReplicaSelection selection

        def invalid_selection_policy():
            raise ValueError('Invalid replica selection policy: {0}'
                             .format(replica_selection))

        if isinstance(replica_selection, int):
            if 0 <= replica_selection < len(_replica_selection_policies):
                check_status(self.scanner.SetSelection(
                             <ReplicaSelection> replica_selection))
            else:
                invalid_selection_policy()
        else:
            try:
                check_status(self.scanner.SetSelection(
                    _replica_selection_policies[replica_selection.lower()]))
            except KeyError:
                invalid_selection_policy()

        return self

    def set_projected_column_indexes(self, indexes):
        """
        Sets the columns to be scanned.

        Parameters
        ----------
        indexes : list of integers representing column indexes

        Returns
        -------
        self : Scanner
        """
        cdef vector[int] v_indexes = indexes
        check_status(self.scanner.SetProjectedColumnIndexes(v_indexes))
        return self

    def set_read_mode(self, read_mode):
        """
        Set the read mode for scanning.

        Parameters
        ----------
        read_mode : {'latest', 'snapshot', 'read_your_writes'}
          You can also use the constants READ_LATEST, READ_AT_SNAPSHOT,
          READ_YOUR_WRITES

        Returns
        -------
        self : Scanner
        """
        cdef ReadMode rmode

        def invalid_selection_policy():
            raise ValueError('Invalid read mode: {0}'
                             .format(read_mode))

        if isinstance(read_mode, int):
            if 0 <= read_mode < len(_read_modes):
                check_status(self.scanner.SetReadMode(
                             <ReadMode> read_mode))
            else:
                invalid_selection_policy()
        else:
            try:
                check_status(self.scanner.SetReadMode(
                    _read_modes[read_mode.lower()]))
            except KeyError:
                invalid_selection_policy()

        return self

    def set_snapshot(self, timestamp, format=None):
        """
        Set the snapshot timestamp for this scanner.

        Parameters
        ---------
        timestamp : datetime.datetime or string
          If a string is provided, a format must be provided as well.
          NOTE: This should be in UTC. If a timezone aware datetime
          object is provided, it will be converted to UTC, otherwise,
          all other input is assumed to be UTC.
        format : Required if a string timestamp is provided
          Uses the C strftime() function, see strftime(3) documentation.

        Returns
        -------
        self : Scanner
        """
        # Confirm that a format is provided if timestamp is a string
        if isinstance(timestamp, six.string_types) and not format:
            raise ValueError(
                "To use a string timestamp you must provide a format. " +
                "See the strftime(3) documentation.")

        snapshot_micros = to_unixtime_micros(timestamp, format)

        if snapshot_micros >= 0:
            check_status(self.scanner.SetSnapshotMicros(
                         <uint64_t> snapshot_micros))
        else:
            raise ValueError(
                "Snapshot Timestamps be greater than the unix epoch.")

        return self

    def set_limit(self, limit):
        """
        Set a limit on the number of rows returned by this scanner.
        Must be a positive value.

        Parameters
        ----------
        limit : the maximum number of rows to return

        Returns
        -------
        self : Scanner
        """
        if limit <= 0:
            raise ValueError("Limit must be positive.")
        check_status(self.scanner.SetLimit(<int64_t> limit))

    def set_fault_tolerant(self):
        """
        Makes the underlying KuduScanner fault tolerant.
        Returns a reference to itself to facilitate chaining.

        Returns
        -------
        self : Scanner
        """
        check_status(self.scanner.SetFaultTolerant())
        return self

    def new_bound(self):
        """
        Returns a new instance of a PartialRow to be later set with
        add_lower_bound()/add_exclusive_upper_bound().

        Returns
        -------
        bound : PartialRow
        """
        return self.table.schema.new_row()

    def add_lower_bound(self, bound):
        """
        Sets the (inclusive) lower bound of the scan.
        Returns a reference to itself to facilitate chaining.

        Parameters
        ----------
        bound : PartialRow/tuple/list/dictionary

        Returns
        -------
        self : Scanner
        """
        cdef:
            PartialRow row
        # Convert record to bound
        if not isinstance(bound, PartialRow):
            row = self.table.schema.new_row(bound)
        else:
            row = bound

        check_status(self.scanner.AddLowerBound(deref(row.row)))
        return self

    def add_exclusive_upper_bound(self, bound):
        """
        Sets the (exclusive) upper bound of the scan.
        Returns a reference to itself to facilitate chaining.

        Parameters
        ----------
        bound : PartialRow/tuple/list/dictionary

        Returns
        -------
        self : Scanner
        """
        cdef:
            PartialRow row
        # Convert record to bound
        if not isinstance(bound, PartialRow):
            row = self.table.schema.new_row(bound)
        else:
            row = bound

        check_status(self.scanner.AddExclusiveUpperBound(deref(row.row)))
        return self

    def get_projection_schema(self):
        """
        Returns the schema of the projection being scanned

        Returns
        -------
        schema : kudu.Schema
        """
        result = Schema()
        # Had to instantiate a new schema to return a pointer since the
        # GetProjectionSchema method does not
        cdef KuduSchema* schema = new KuduSchema(self.scanner.
                                                 GetProjectionSchema())
        result.schema = schema
        return result

    def get_resource_metrics(self):
        """
        Return the cumulative resource metrics since the scan was started.

        Returns
        -------
        metrics : Dictionary
        """
        _map = self.scanner.GetResourceMetrics().Get()

        # Convert map to python dictionary
        result = {}
        for it in _map:
            result[frombytes(it.first)] = it.second
        return result

    def open(self):
        """
        Returns a reference to itself to facilitate chaining

        Returns
        -------
        self : Scanner
        """
        if not self.is_open:
            check_status(self.scanner.Open())
            self.is_open = 1
        return self

    def has_more_rows(self):
        """
        Returns True if there are more rows to be read.
        """
        return self.scanner.HasMoreRows()

    def read_all_tuples(self):
        """
        Compute a RowBatch containing all rows from the scan operation (which
        hopefully fit into memory, probably not handled gracefully at the
        moment).
        """
        cdef list tuples = []
        cdef RowBatch batch

        self.ensure_open()

        while self.has_more_rows():
            batch = self.next_batch()
            tuples.extend(batch.as_tuples())

        return tuples

    def xbatches(self):
        """
        This method acts as a generator to enable more effective memory management
        by yielding batches of tuples.
        """

        self.ensure_open()

        while self.has_more_rows():
            yield self.next_batch().as_tuples()


    def read_next_batch_tuples(self):
        return self.next_batch().as_tuples()

    cpdef RowBatch next_batch(self):
        """
        Retrieve the next batch of rows from the scanner.

        Returns
        -------
        batch : RowBatch
        """
        if not self.has_more_rows():
            raise StopIteration

        cdef RowBatch batch = RowBatch()
        check_status(self.scanner.NextBatch(&batch.batch))
        return batch

    def set_cache_blocks(self, cache_blocks):
        """
        Sets the block caching policy.
        Returns a reference to itself to facilitate chaining.

        Parameters
        ----------
        cache_blocks : bool

        Returns
        -------
        self : Scanner
        """
        check_status(self.scanner.SetCacheBlocks(cache_blocks))
        return self

    def keep_alive(self):
        """
        Keep the current remote scanner alive.

        Keep the current remote scanner alive on the Tablet server for an
        additional time-to-live (set by a configuration flag on the tablet
        server). This is useful if the interval in between NextBatch() calls is
        big enough that the remote scanner might be garbage collected (default
        ttl is set to 60 secs.). This does not invalidate any previously
        fetched results.

        Returns
        -------
        self : Scanner
        """
        check_status(self.scanner.KeepAlive())
        return self

    def get_current_server(self):
        """
        Get the TabletServer that is currently handling the scan.

        More concretely, this is the server that handled the most recent open()
        or next_batch() RPC made by the server.

        Returns
        -------
        tserver : TabletServer
        """
        cdef:
            TabletServer tserver = TabletServer()
            KuduTabletServer* tserver_p = NULL

        check_status(self.scanner.GetCurrentServer(&tserver_p))
        tserver._own = 1
        tserver._init(tserver_p)
        return tserver

    def close(self):
        """
        Close the scanner.

        Closing the scanner releases resources on the server. This call does
        not block, and will not ever fail, even if the server cannot be
        contacted.

        Note: The scanner is reset to its initial state by this function.
        You'll have to re-add any projection, predicates, etc if you want to
        reuse this object.
        Note: When the Scanner object is garbage collected, this method is run.
        This method call is only needed if you want to explicitly release the
        resources on the server.
        """
        self.scanner.Close()

    def to_pandas(self, index=None, coerce_float=False):
        """
        Returns the contents of this Scanner to a Pandas DataFrame.

        This is only available if Pandas is installed.

        Note: This should only be used if the results from the scanner are expected
        to be small, as Pandas will load the entire contents into memory.

        Parameters
        ----------
        index : string, list of fields
            Field or list of fields to use as the index
        coerce_float : boolean
            Attempt to convert decimal values to floating point (double precision).

        Returns
        -------
        dataframe : DataFrame
        """
        import pandas as pd

        self.ensure_open()

        # Here we are using list comprehension with the batch generator to avoid
        # doubling our memory footprint.
        dfs = [ pd.DataFrame.from_records(batch,
                                          index=index,
                                          coerce_float=coerce_float,
                                          columns=self.get_projection_schema().names)
                for batch in self.xbatches()
                if len(batch) != 0
                ]

        df = pd.concat(dfs, ignore_index=not(bool(index)))

        types = {}
        for column in self.get_projection_schema():
            pandas_type = _correct_pandas_data_type(column.type.name)

            if pandas_type is not None and \
                not(column.nullable and df[column.name].isnull().any()):
                types[column.name] = pandas_type

        for col, dtype in types.items():
            df[col] = df[col].astype(dtype, copy=False)

        return df




cdef class ScanToken:
    """
    A ScanToken describes a partial scan of a Kudu table limited to a single
    contiguous physical location. Using the KuduScanTokenBuilder, clients
    can describe the desired scan, including predicates, bounds, timestamps,
    and caching, and receive back a collection of scan tokens.
    """
    cdef:
        KuduScanToken* _token

    def __cinit__(self):
        self._token = NULL

    def __dealloc__(self):
        if self._token != NULL:
            del self._token

    cdef _init(self, KuduScanToken* token):
        self._token = token
        return self

    def into_kudu_scanner(self):
        """
        Returns a scanner under the current client.

        Returns
        -------
        scanner : Scanner
        """
        cdef:
            Scanner result = Scanner()
            KuduScanner* _scanner = NULL
        check_status(self._token.IntoKuduScanner(&_scanner))
        result.scanner = _scanner
        return result


    def tablet(self):
        """
        Returns the Tablet associated with this ScanToken

        Returns
        -------
        tablet : Tablet
        """
        tablet = Tablet()
        return tablet._init(&self._token.tablet())

    def serialize(self):
        """
        Serialize token into a string.

        Returns
        -------
        serialized_token : string
        """
        cdef string buf
        check_status(self._token.Serialize(&buf))
        return buf

    def deserialize_into_scanner(self, Client client, serialized_token):
        """
        Returns a new scanner from the serialized token created under
        the provided Client.

        Parameters
        ----------
        client : Client
        serialized_token : string

        Returns
        -------
        scanner : Scanner
        """
        cdef:
            Scanner result = Scanner()
            KuduScanner* _scanner
        check_status(self._token.DeserializeIntoScanner(client.cp, serialized_token, &_scanner))
        result.scanner = _scanner
        return result


cdef class ScanTokenBuilder:
    """
    This class builds ScanTokens for a Table.
    """
    cdef:
        KuduScanTokenBuilder* _builder
        Table _table

    def __cinit__(self, Table table):
        self._table = table
        self._builder = new KuduScanTokenBuilder(table.ptr())

    def __dealloc__(self):
        if self._builder != NULL:
            del self._builder

    def set_projected_column_names(self, names):
        """
        Sets the columns to be scanned.
        Returns a reference to itself to facilitate chaining.

        Parameters
        ----------
        names : list of strings

        Returns
        -------
        self : ScanTokenBuilder
        """
        if isinstance(names, str):
            names = [names]
        cdef vector[string] v_names
        for name in names:
            v_names.push_back(tobytes(name))
        check_status(self._builder.SetProjectedColumnNames(v_names))
        return self

    def set_projected_column_indexes(self, indexes):
        """
        Sets the columns to be scanned.
        Returns a reference to itself to facilitate chaining.

        Parameters
        ----------
        indexes : list of integers representing column indexes

        Returns
        -------
        self : ScanTokenBuilder
        """
        cdef vector[int] v_indexes = indexes
        check_status(self._builder.SetProjectedColumnIndexes(v_indexes))
        return self

    def set_batch_size_bytes(self, batch_size):
        """
        Sets the batch size in bytes.
        Returns a reference to itself to facilitate chaining.

        Parameters
        ----------
        batch_size : Size of batch in bytes

        Returns
        -------
        self : ScanTokenBuilder
        """
        check_status(self._builder.SetBatchSizeBytes(batch_size))
        return self

    def set_read_mode(self, read_mode):
        """
        Set the read mode for scanning.

        Parameters
        ----------
        read_mode : {'latest', 'snapshot'}
          You can also use the constants READ_LATEST, READ_AT_SNAPSHOT

        Returns
        -------
        self : ScanTokenBuilder
        """
        cdef ReadMode rmode

        def invalid_selection_policy():
            raise ValueError('Invalid read mode: {0}'
                             .format(read_mode))

        if isinstance(read_mode, int):
            if 0 <= read_mode < len(_read_modes):
                check_status(self._builder.SetReadMode(
                             <ReadMode> read_mode))
            else:
                invalid_selection_policy()
        else:
            try:
                check_status(self._builder.SetReadMode(
                    _read_modes[read_mode.lower()]))
            except KeyError:
                invalid_selection_policy()

        return self

    def set_snapshot(self, timestamp, format=None):
        """
        Set the snapshot timestamp for this ScanTokenBuilder.

        Parameters
        ---------
        timestamp : datetime.datetime or string
          If a string is provided, a format must be provided as well.
          NOTE: This should be in UTC. If a timezone aware datetime
          object is provided, it will be converted to UTC, otherwise,
          all other input is assumed to be UTC.
        format : Required if a string timestamp is provided
          Uses the C strftime() function, see strftime(3) documentation.

        Returns
        -------
        self : ScanTokenBuilder
        """
        # Confirm that a format is provided if timestamp is a string
        if isinstance(timestamp, six.string_types) and not format:
            raise ValueError(
                "To use a string timestamp you must provide a format. " +
                "See the strftime(3) documentation.")

        snapshot_micros = to_unixtime_micros(timestamp, format)

        if snapshot_micros >= 0:
            check_status(self._builder.SetSnapshotMicros(
                         <uint64_t> snapshot_micros))
        else:
            raise ValueError(
                "Snapshot Timestamps be greater than the unix epoch.")

        return self

    def set_timeout_millis(self, millis):
        """
        Sets the scan request timeout in milliseconds.
        Returns a reference to itself to facilitate chaining.

        Parameters
        ----------
        millis : int64_t
          timeout in milliseconds

        Returns
        -------
        self : ScanTokenBuilder
        """
        check_status(self._builder.SetTimeoutMillis(millis))
        return self

    def set_timout_millis(self, millis):
        """
        See set_timeout_millis().

        This method is deprecated due to having a typo in the method name and
        will be removed in a future release.
        """
        return self.set_timeout_millis(millis)

    def set_fault_tolerant(self):
        """
        Makes the underlying KuduScanner fault tolerant.
        Returns a reference to itself to facilitate chaining.

        Returns
        -------
        self : ScanTokenBuilder
        """
        check_status(self._builder.SetFaultTolerant())
        return self

    def set_selection(self, replica_selection):
        """
        Set the replica selection policy while scanning.

        Parameters
        ----------
        replica_selection : {'leader', 'closest', 'first'}
          You can also use the constants LEADER_ONLY, CLOSEST_REPLICA,
          and FIRST_REPLICA

        Returns
        -------
        self : ScanTokenBuilder
        """
        cdef ReplicaSelection selection

        def invalid_selection_policy():
            raise ValueError('Invalid replica selection policy: {0}'
                             .format(replica_selection))

        if isinstance(replica_selection, int):
            if 0 <= replica_selection < len(_replica_selection_policies):
                check_status(self._builder.SetSelection(
                             <ReplicaSelection> replica_selection))
            else:
                invalid_selection_policy()
        else:
            try:
                check_status(self._builder.SetSelection(
                    _replica_selection_policies[replica_selection.lower()]))
            except KeyError:
                invalid_selection_policy()

        return self

    def add_predicates(self, preds):
        """
        Add a list of scan predicates to the ScanTokenBuilder. Select columns
        from the parent table and make comparisons to create predicates.

        Examples
        --------
        c = table[col_name]
        preds = [c >= 0, c <= 10]
        builder.add_predicates(preds)

        Parameters
        ----------
        preds : list of Predicate
        """
        for pred in preds:
            self.add_predicate(pred)

    cpdef add_predicate(self, Predicate pred):
        """
        Add a scan predicates to the scan token. Select columns from the
        parent table and make comparisons to create predicates.

        Examples
        --------
        pred = table[col_name] <= 10
        builder.add_predicate(pred)

        Parameters
        ----------
        pred : kudu.Predicate

        Returns
        -------
        self : ScanTokenBuilder
        """
        cdef KuduPredicate* clone

        # We clone the KuduPredicate so that the Predicate wrapper class can be
        # reused
        clone = pred.pred.Clone()
        check_status(self._builder.AddConjunctPredicate(clone))

    def new_bound(self):
        """
        Returns a new instance of a PartialRow to be later set with
        add_lower_bound()/add_upper_bound().

        Returns
        -------
        bound : PartialRow
        """
        return self._table.schema.new_row()

    def add_lower_bound(self, bound):
        """
        Sets the lower bound of the scan.
        Returns a reference to itself to facilitate chaining.

        Parameters
        ----------
        bound : PartialRow/list/tuple/dict

        Returns
        -------
        self : ScanTokenBuilder
        """
        cdef:
            PartialRow row
        # Convert record to bound
        if not isinstance(bound, PartialRow):
            row = self._table.schema.new_row(bound)
        else:
            row = bound

        check_status(self._builder.AddLowerBound(deref(row.row)))
        return self

    def add_upper_bound(self, bound):
        """
        Sets the upper bound of the scan.
        Returns a reference to itself to facilitate chaining.

        Parameters
        ----------
        bound : PartialRow/list/tuple/dict

        Returns
        -------
        self : ScanTokenBuilder
        """
        cdef:
            PartialRow row
        # Convert record to bound
        if not isinstance(bound, PartialRow):
            row = self._table.schema.new_row(bound)
        else:
            row = bound

        check_status(self._builder.AddUpperBound(deref(row.row)))
        return self

    def set_cache_blocks(self, cache_blocks):
        """
        Sets the block caching policy.
        Returns a reference to itself to facilitate chaining.

        Parameters
        ----------
        cache_blocks : bool

        Returns
        -------
        self : ScanTokenBuilder
        """
        check_status(self._builder.SetCacheBlocks(cache_blocks))
        return self

    def build(self):
        """
        Build the set of scan tokens. The builder may be reused after
        this call. Returns a list of ScanTokens to be serialized and
        executed in parallel with seperate client instances.

        Returns
        -------
        tokens : List[ScanToken]
        """

        cdef:
            vector[KuduScanToken*] tokens
            size_t i

        check_status(self._builder.Build(&tokens))

        result = []
        for i in range(tokens.size()):
            token = ScanToken()
            result.append(token._init(tokens[i]))
        return result


cdef class Tablet:
    """
    Represents a remote Tablet. Contains the tablet id and Replicas associated
    with the Kudu Tablet. Retrieved by the ScanToken.tablet() method.
    """
    cdef:
        const KuduTablet* _tablet
        vector[KuduReplica*] _replicas

    cdef _init(self, const KuduTablet* tablet):
        self._tablet = tablet
        return self

    def id(self):
        return frombytes(self._tablet.id())

    def replicas(self):
        cdef size_t i

        result = []
        _replicas = self._tablet.replicas()
        for i in range(_replicas.size()):
            replica = Replica()
            result.append(replica._init(_replicas[i]))
        return result

cdef class Replica:
    """
    Represents a remote Tablet's replica. Retrieve a list of Replicas with the
    Tablet.replicas() method. Contains the boolean is_leader and its
    respective TabletServer object.
    """
    cdef const KuduReplica* _replica

    cdef _init(self, const KuduReplica* replica):
        self._replica = replica
        return self

    def is_leader(self):
        return self._replica.is_leader()

    def ts(self):
        ts = TabletServer()
        return ts._init(&self._replica.ts())

cdef class KuduError:

    """
    Wrapper for a C++ KuduError indicating a client error resulting from
    applying operations in a session.
    """

    cdef:
        C_KuduError* error

    def __cinit__(self):
        self.error = NULL

    def __dealloc__(self):
        # We own this object
        if self.error != NULL:
            del self.error

    def failed_op(self):
        """
        Get debug string representation of the failed operation.

        Returns
        -------
        op : str
        """
        return frombytes(self.error.failed_op().ToString())

    def was_possibly_successful(self):
        """
        Check if there is a chance that the requested operation was successful.

        In some cases, it is possible that the server did receive and
        successfully perform the requested operation, but the client can't
        tell whether or not it was successful. For example, if the call times
        out, the server may still succeed in processing at a later time.

        Returns
        -------
        result : bool
        """
        return self.error.was_possibly_successful()

    def __repr__(self):
        return "KuduError('%s')" % (self.error.status().ToString())


cdef class PartialRow:

    def __cinit__(self, Schema schema):
        # This gets called before any subclass cinit methods
        self.schema = schema
        self._own = 1

    def __dealloc__(self):
        if self._own and self.row != NULL:
            del self.row

    def __setitem__(self, key, value):
        if isinstance(key, basestring):
            self.set_field(key, value)
        else:
            if 0 <= key < len(self.schema):
                self.set_loc(key, value)
            else:
                raise IndexError("Column index {0} is out of bounds."
                                 .format(key))

    def from_record(self, record):
        """
        Initializes PartialRow with values from an input record. The record
        can be in the form of a tuple, dict, or list. Dictionary keys can
        be either column names or indexes.

        Parameters
        ----------
        record : tuple/list/dict

        Returns
        -------
        self : PartialRow
        """
        if isinstance(record, (tuple, list)):
            for indx, val in enumerate(record):
                self[indx] = val
        elif isinstance(record, dict):
            for key, val in dict_iter(record):
                self[key] = val
        else:
            raise TypeError("Invalid record type <{0}> for " +
                            "PartialRow.from_record."
                            .format(type(record).__name__))

        return self

    cpdef set_field(self, key, value):
        cdef:
            int i = self.schema.get_loc(key)

        self.set_loc(i, value)

    cpdef set_loc(self, int i, value):
        cdef:
            DataType t = self.schema.loc_type(i)
            Slice slc

        if value is None:
            check_status(self.row.SetNull(i))
            return

        # Leave it to Cython to do the coercion and complain if it doesn't
        # work. Cython will catch many casting problems but we should verify
        # with unit tests.
        if t == KUDU_BOOL:
            check_status(self.row.SetBool(i, <c_bool> value))
        elif t == KUDU_INT8:
            check_status(self.row.SetInt8(i, <int8_t> value))
        elif t == KUDU_INT16:
            check_status(self.row.SetInt16(i, <int16_t> value))
        elif t == KUDU_INT32:
            check_status(self.row.SetInt32(i, <int32_t> value))
        elif t == KUDU_INT64:
            check_status(self.row.SetInt64(i, <int64_t> value))
        elif t == KUDU_FLOAT:
            check_status(self.row.SetFloat(i, <float> value))
        elif t == KUDU_DOUBLE:
            check_status(self.row.SetDouble(i, <double> value))
        elif t == KUDU_STRING:
            if isinstance(value, unicode):
                value = value.encode('utf8')
            slc = Slice(<char*> value, len(value))
            check_status(self.row.SetStringCopy(i, slc))
        elif t == KUDU_BINARY:
            if isinstance(value, unicode):
                raise TypeError("Unicode objects must be explicitly encoded " +
                                "before storing in a Binary field.")

            slc = Slice(<char*> value, len(value))
            check_status(self.row.SetBinaryCopy(i, slc))
        elif t == KUDU_VARCHAR:
            if isinstance(value, unicode):
                value = value.encode('utf8')
            slc = Slice(<char*> value, len(value))
            check_status(self.row.SetVarchar(i, slc))
        elif t == KUDU_UNIXTIME_MICROS:
            check_status(self.row.SetUnixTimeMicros(i, <int64_t>
                to_unixtime_micros(value)))
        elif t == KUDU_DECIMAL:
            IF PYKUDU_INT128_SUPPORTED == 1:
                check_status(self.row.SetUnscaledDecimal(i, <int128_t>to_unscaled_decimal(value)))
            ELSE:
                raise KuduException("The decimal type is not supported when GCC version is < 4.6.0" % self)
        else:
            raise TypeError("Cannot set kudu type <{0}>.".format(_type_names[t]))

    cpdef set_field_null(self, key):
        pass

    cpdef set_loc_null(self, int i):
        pass

    cdef add_to_session(self, Session s):
        pass


cdef class WriteOperation:
    cdef:
        # Whether the WriteOperation has been applied.
        # Set by subclasses.
        bint applied
        KuduWriteOperation* op
        PartialRow py_row

    def __cinit__(self, Table table, record=None):
        self.applied = 0
        self.py_row = PartialRow(table.schema)
        self.py_row._own = 0

    cdef add_to_session(self, Session s):
        if self.applied:
            raise Exception

        check_status(s.s.get().Apply(self.op))
        self.op = NULL
        self.applied = 1

    def __setitem__(self, key, value):
        # Since the write operation is no longer a sub-class of the PartialRow
        # we need to explicitly retain the item setting functionality and API
        # style.
        self.py_row[key] = value


cdef class Insert(WriteOperation):
    def __cinit__(self, Table table, record=None):
        self.op = table.ptr().NewInsert()
        self.py_row.row = self.op.mutable_row()
        if record:
            self.py_row.from_record(record)

    def __dealloc__(self):
        del self.op

cdef class InsertIgnore(WriteOperation):
    def __cinit__(self, Table table, record=None):
        self.op = table.ptr().NewInsertIgnore()
        self.py_row.row = self.op.mutable_row()
        if record:
            self.py_row.from_record(record)

    def __dealloc__(self):
        del self.op


cdef class Upsert(WriteOperation):
    def __cinit__(self, Table table, record=None):
        self.op = table.ptr().NewUpsert()
        self.py_row.row = self.op.mutable_row()
        if record:
            self.py_row.from_record(record)
    def __dealloc__(self):
        del self.op


cdef class Update(WriteOperation):
    def __cinit__(self, Table table, record=None):
        self.op = table.ptr().NewUpdate()
        self.py_row.row = self.op.mutable_row()
        if record:
            self.py_row.from_record(record)

    def __dealloc__(self):
        del self.op


cdef class Delete(WriteOperation):
    def __cinit__(self, Table table, record=None):
        self.op = table.ptr().NewDelete()
        self.py_row.row = self.op.mutable_row()
        if record:
            self.py_row.from_record(record)

    def __dealloc__(self):
        del self.op



cdef inline cast_pyvalue(DataType t, object o):
    if t == KUDU_BOOL:
        return BoolVal(o)
    elif t == KUDU_INT8:
        return Int8Val(o)
    elif t == KUDU_INT16:
        return Int16Val(o)
    elif t == KUDU_INT32:
        return Int32Val(o)
    elif t == KUDU_INT64:
        return Int64Val(o)
    elif t == KUDU_DOUBLE:
        return DoubleVal(o)
    elif t == KUDU_FLOAT:
        return FloatVal(o)
    elif t == KUDU_STRING:
        return StringVal(o)
    elif t == KUDU_UNIXTIME_MICROS:
        return UnixtimeMicrosVal(o)
    elif t == KUDU_BINARY:
        return StringVal(o)
    elif t == KUDU_VARCHAR:
        return StringVal(o)
    else:
        raise TypeError("Cannot cast kudu type <{0}>".format(_type_names[t]))


cdef class TableAlterer:
    """
    Alters an existing table based on the provided steps.
    """

    def __cinit__(self, Table table):
        self._table = table
        self._new_name = None
        self._init(self._table.parent.cp
                   .NewTableAlterer(tobytes(self._table.name)))

    def __dealloc__(self):
        if self._alterer != NULL:
            del self._alterer

    cdef _init(self, KuduTableAlterer* alterer):
        self._alterer = alterer

    def rename(self, table_name):
        """
        Rename the table. Returns a reference to itself to facilitate chaining.

        Parameters
        ----------
        table_name : str
          The new name for the table.

        Return
        ------
        self : TableAlterer
        """
        self._alterer.RenameTo(tobytes(table_name))
        self._new_name = table_name
        return self

    def add_column(self, name, type_=None, nullable=None, compression=None,
                   encoding=None, default=None):
        """
        Add a new column to the table.

        When adding a column, you must specify the default value of the new
        column using ColumnSpec.default(...) or the default parameter in this
        method.

        Parameters
        ----------
        name : string
        type_ : string or KuduType
          Data type e.g. 'int32' or kudu.int32
        nullable : boolean, default None
          New columns are nullable by default. Set boolean value for explicit
          nullable / not-nullable
        compression : string or int
          One of kudu.COMPRESSION_* constants or their string equivalent.
        encoding : string or int
          One of kudu.ENCODING_* constants or their string equivalent.
        default : obj
          Use this to set the column default value

        Returns
        -------
        spec : ColumnSpec
        """
        cdef:
            ColumnSpec result = ColumnSpec()

        result.spec = self._alterer.AddColumn(tobytes(name))

        if type_ is not None:
            result.type(type_)

        if nullable is not None:
            result.nullable(nullable)

        if compression is not None:
            result.compression(compression)

        if encoding is not None:
            result.encoding(encoding)

        if default:
            result.default(default)

        return result

    def alter_column(self, name, rename_to=None):
        """
        Alter an existing column.

        Parameters
        ----------
        name : string
        rename_to : str
          If set, the column will be renamed to this

        Returns
        -------
        spec : ColumnSpec
        """
        cdef:
            ColumnSpec result = ColumnSpec()

        result.spec = self._alterer.AlterColumn(tobytes(name))

        if rename_to:
            result.rename(rename_to)

        return result

    def drop_column(self, name):
        """
        Drops an existing column from the table.

        Parameters
        ----------
        name : str
          The name of the column to drop.

        Returns
        -------
        self : TableAlterer
        """
        self._alterer.DropColumn(tobytes(name))
        return self

    def add_range_partition(self, lower_bound=None,
                            upper_bound=None,
                            lower_bound_type='inclusive',
                            upper_bound_type='exclusive'):
        """
        Add a range partition to the table with the specified lower bound and
        upper bound.

        Multiple range partitions may be added as part of a single alter table
        transaction by calling this method multiple times on the table alterer.

        This client may immediately write and scan the new tablets when Alter()
        returns success, however other existing clients may have to wait for a
        timeout period to elapse before the tablets become visible. This period
        is configured by the master's 'table_locations_ttl_ms' flag, and
        defaults to 5 minutes.

        Parameters
        ----------
        lower_bound : PartialRow/list/tuple/dict
        upper_bound : PartialRow/list/tuple/dict
        lower_bound_type : {'inclusive', 'exclusive'} or constants
          kudu.EXCLUSIVE_BOUND and kudu.INCLUSIVE_BOUND
        upper_bound_type : {'inclusive', 'exclusive'} or constants
          kudu.EXCLUSIVE_BOUND and kudu.INCLUSIVE_BOUND

        Returns
        -------
        self : TableAlterer
        """
        cdef:
            PartialRow lbound
            PartialRow ubound

        if not isinstance(lower_bound, PartialRow):
            lbound = self._table.schema.new_row(lower_bound)
        else:
            lbound = lower_bound
        lbound._own = 0
        if not isinstance(upper_bound, PartialRow):
            ubound = self._table.schema.new_row(upper_bound)
        else:
            ubound = upper_bound
        ubound._own = 0
        self._alterer.AddRangePartition(
            lbound.row,
            ubound.row,
            _check_convert_range_bound_type(lower_bound_type),
            _check_convert_range_bound_type(upper_bound_type)
        )

    def drop_range_partition(self, lower_bound=None,
                             upper_bound=None,
                             lower_bound_type='inclusive',
                             upper_bound_type='exclusive'):
        """
        Drop the range partition from the table with the specified lower bound
        and upper bound. The bounds must match an existing range partition
        exactly, and may not span multiple range partitions.

        Multiple range partitions may be dropped as part of a single alter
        table transaction by calling this method multiple times on the
        table alterer.

        Parameters
        ----------
        lower_bound : PartialRow/list/tuple/dict
        upper_bound : PartialRow/list/tuple/dict
        lower_bound_type : {'inclusive', 'exclusive'} or constants
          kudu.EXCLUSIVE_BOUND and kudu.INCLUSIVE_BOUND
        upper_bound_type : {'inclusive', 'exclusive'} or constants
          kudu.EXCLUSIVE_BOUND and kudu.INCLUSIVE_BOUND

        Returns
        -------
        self : TableAlterer
        """
        cdef:
            PartialRow lbound
            PartialRow ubound

        if not isinstance(lower_bound, PartialRow):
            lbound = self._table.schema.new_row(lower_bound)
        else:
            lbound = lower_bound
        lbound._own = 0
        if not isinstance(upper_bound, PartialRow):
            ubound = self._table.schema.new_row(upper_bound)
        else:
            ubound = upper_bound
        ubound._own = 0
        self._alterer.DropRangePartition(
            lbound.row,
            ubound.row,
            _check_convert_range_bound_type(lower_bound_type),
            _check_convert_range_bound_type(upper_bound_type)
        )

    def alter(self):
        """
        Alter table. Returns a new table object upon completion of the alter.

        Returns
        -------
        table :Table
        """
        check_status(self._alterer.Alter())
        return self._table.parent.table(self._new_name or self._table.name)
