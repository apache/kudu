# Copyright (c) 2014, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

# distutils: language = c++
# cython: embedsignature = True

from client cimport *

from libcpp.string cimport string
from libcpp.map cimport map

cimport cpython
from cython.operator cimport dereference as deref

import __builtin__

BOOL = KUDU_BOOL
STRING = KUDU_STRING

INT8 = KUDU_INT8
INT16 = KUDU_INT16
INT32 = KUDU_INT32
INT64 = KUDU_INT64

FLOAT = KUDU_FLOAT
DOUBLE = KUDU_DOUBLE

LESS_EQUAL = KUDU_LESS_EQUAL
GREATER_EQUAL = KUDU_GREATER_EQUAL
EQUAL = KUDU_EQUAL

cdef dict _type_names = {
    INT8: 'int8',
    INT16: 'int16',
    INT32: 'int32',
    INT64: 'int64',
    STRING: 'string',
    BOOL: 'bool',
    FLOAT: 'float',
    DOUBLE: 'double'
}

cdef get_type_name(int dtype):
    return _type_names[dtype]


class KuduException(Exception):
    pass


cdef raise_on_failure(Status* status):
    if not status.ok():
        # TODO: Implement an exception class hierarchy and raise the
        # appropriate type of exception from the common types offered by Status
        raise KuduException(status.ToString())



cdef class Client:
    cdef:
        shared_ptr[KuduClient] client
        KuduClient* cp

    def __cinit__(self, master_addr):
        cdef:
            Status s
            vector[string] c_addrs

        c_addrs.push_back(master_addr)
        s = KuduClientBuilder()\
            .master_server_addrs(c_addrs)\
            .Build(&self.client)

        if not s.ok():
            raise KuduException('Could not connect to master')

        # A convenience
        self.cp = self.client.get()

    def __dealloc__(self):
        self.close()

    cpdef close(self):
        # Nothing yet to clean up here
        pass

    def create_table(self, table_name, Schema schema):
        cdef:
            KuduTableCreator* c
            Status s
        c = self.cp.NewTableCreator()
        try:
            s = (c.table_name(table_name)
                 .schema(&schema.schema)
                 .Create())
            return s.ok()
        finally:
            del c

    def delete_table(self, table_name):
        cdef Status s = self.cp.DeleteTable(table_name)
        return s.ok()

    def table_exists(self, table_name):
        cdef:
            shared_ptr[KuduTable] table
            string c_name = table_name

        cdef Status s = self.cp.OpenTable(c_name, &table)
        if s.ok():
            return True
        elif s.IsNotFound():
            return False
        else:
            raise_on_failure(&s)

    def open_table(self, table_name):
        """

        """
        cdef:
            Table table = Table()

        cdef Status s = self.cp.OpenTable(table_name, &table.table)
        raise_on_failure(&s)

        table.table_set()

        return table

    def new_session(self, flush_mode='manual', timeout=5000):
        """

        """
        cdef Session result = Session()
        result.s = self.cp.NewSession()

        result.set_flush_mode(flush_mode)
        result.set_timeout(timeout)

        return result



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
        bool val

    def __cinit__(self, obj):
        self.val = <bool> obj
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

#----------------------------------------------------------------------


cdef class ColumnSchema:
    """
    Wraps a Kudu client ColumnSchema object
    """
    cdef:
        KuduColumnSchema* schema

    def __cinit__(self):
        self.schema = NULL

    def __dealloc__(self):
        if self.schema != NULL:
            del self.schema

    @classmethod
    def create(cls, name, typenum, is_nullable=False):
        cdef string c_name = name

        cdef ColumnSchema result = ColumnSchema()

        # TODO: This can fail in numerous ways due to bad user input. The input
        # values should be validated / sanitized in some way.
        result.schema = new KuduColumnSchema(c_name, typenum, is_nullable)

        return result

    cdef inline cast_pyvalue(self, object o):
        cdef DataType t = self.schema.type()
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
        else:
            raise TypeError(t)

    property dtype:
        def __get__(self):
            return self.schema.type()

    property is_nullable:
        def __get__(self):
            return self.schema.is_nullable()

    property name:
        def __get__(self):
            return self.schema.name()

    def equals(self, other):
        if not isinstance(other, ColumnSchema):
            return False
        return self.schema.Equals(deref((<ColumnSchema> other).schema))

    def __repr__(self):
        return ('ColumnSchema(name=%s, type=%s, nullable=%s)'
                % (self.name, get_type_name(self.dtype),
                   self.is_nullable))



#----------------------------------------------------------------------

cdef class Schema:
    cdef:
        KuduSchema schema
        int num_key_columns

    def __cinit__(self):
        # Users should not call this directly
        pass

    def repr(self):
        # Got to be careful with huge schemas, maybe some kind of summary repr
        # when more than 20-30 columns?
        pass

    @classmethod
    def create(cls, columns, num_key_columns):
        cdef:
            ColumnSchema col
            Schema result = Schema()
            vector[KuduColumnSchema] vcols

        for col in columns:
            # This copies the KuduColumnSchema, but that may be OK.
            vcols.push_back(deref(col.schema))

        result.num_key_columns = num_key_columns
        # TODO: don't use KuduSchema::Reset (deprecated)
        cdef Status s = result.schema.Reset(vcols, num_key_columns)
        raise_on_failure(&s)

        return result

    def __len__(self):
        return self.schema.num_columns()

    def at(self, i):
        cdef:
            ColumnSchema result = ColumnSchema()

        # TODO: boundschecking
        result.schema = new KuduColumnSchema(self.schema.Column(i))
        return result


cdef class Table:
    cdef:
        shared_ptr[KuduTable] table
        const KuduSchema* schema
        map[string, int] _col_mapping
        bint _mapping_initialized

    def __cinit__(self):
        # Users should not instantiate directly
        self.schema = NULL

    cdef table_set(self):
        # Called after the refptr has been populated
        self.schema = &self.table.get().schema()
        self._col_mapping.clear()
        self._mapping_initialized = 0

    cdef inline int get_loc(self, name) except -1:
        if not self._mapping_initialized:
            for i in range(self.schema.num_columns()):
                self._col_mapping[self.schema.Column(i).name()] = i

        # TODO: std::map is slightly verbose and inefficient here (O(lg n)
        # lookups), may consider replacing with a better / different hash table
        # should it become a performance bottleneck
        cdef map[string, int].iterator it = self._col_mapping.find(name)
        if it == self._col_mapping.end():
            raise KeyError(name)
        return self._col_mapping[name]

    cdef inline DataType loc_type(self, int i):
        return self.schema.Column(i).type()

    cdef ColumnSchema col_schema(self, int i):
        cdef ColumnSchema result = ColumnSchema()
        result.schema = new KuduColumnSchema(self.table.get()
                                             .schema().Column(i))
        return result

    def __len__(self):
        # TODO: is this cheaply knowable?
        raise NotImplementedError

    property name:

        def __get__(self):
            return self.table.get().name()

    # XXX: don't love this name
    property num_columns:

        def __get__(self):
            return self.table.get().schema().num_columns()

    def column(self, i):
        return self.col_schema(i)

    def column_by_name(self, name):
        return self.col_schema(self.get_loc(name))

    def rename(self, new_name):
        pass

    def rename_column(self):
        pass

    def insert(self):
        return Insert(self)

    def update(self):
        return Update(self)

    def delete(self):
        return Delete(self)

    def scanner(self):
        """
        Create a new scanner for this table
        """
        cdef Scanner result = Scanner(self)
        result.scanner = new KuduScanner(self.table.get())
        return result


cdef class Session:
    """
    Create a (fully local) session to build up operations to interact with the
    cluster.
    """
    cdef:
        shared_ptr[KuduSession] s

    def __cinit__(self):
        pass

    def set_flush_mode(self, flush_mode='manual'):
        cdef Status status

        if flush_mode == 'manual':
            status = self.s.get().SetFlushMode(MANUAL_FLUSH)
        elif flush_mode == 'sync':
            status = self.s.get().SetFlushMode(AUTO_FLUSH_SYNC)
        elif flush_mode == 'background':
            status = self.s.get().SetFlushMode(AUTO_FLUSH_BACKGROUND)

        raise_on_failure(&status)

    def set_timeout(self, milliseconds):
        self.s.get().SetTimeoutMillis(5000)

    def apply(self, WriteOperation op):
        return op.add_to_session(self)

    def flush(self):
        cdef Status status = self.s.get().Flush()
        raise_on_failure(&status)
        return True

    def flush_async(self):
        raise NotImplementedError

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
            bool overflowed
            int i

        self.s.get().GetPendingErrors(&v_errors, &overflowed)

        result = []
        for i in range(v_errors.size()):
            error = KuduError()
            error.error = v_errors[i]
            result.append(error)

        return result, overflowed


cdef class Row:
    cdef:
        # So we can access the schema information
        Table table

        RowBatch parent

        # This object is owned by the parent RowBatch
        KuduRowResult* row

    def __cinit__(self, batch, table):
        self.parent = batch
        self.table = table
        self.row = NULL

    def __dealloc__(self):
        pass

    def as_tuple(self):
        cdef:
            int i, k
            tuple tup

        k = self.table.num_columns
        tup = cpython.PyTuple_New(k)
        for i in range(k):
            val = self.get_slot(i)
            cpython.PyTuple_SET_ITEM(tup, i, val)
            cpython.Py_INCREF(val)

        return tup

    cdef inline get_bool(self, int i):
        cdef bool val
        cdef Status s = self.row.GetBool(i, &val)
        raise_on_failure(&s)
        # The built-in bool is masked by the libcpp typedef
        return __builtin__.bool(val)

    cdef inline get_int8(self, int i):
        cdef int8_t val
        cdef Status s = self.row.GetInt8(i, &val)
        raise_on_failure(&s)
        return val

    cdef inline get_int16(self, int i):
        cdef int16_t val
        cdef Status s = self.row.GetInt16(i, &val)
        raise_on_failure(&s)
        return val

    cdef inline get_int32(self, int i):
        cdef int32_t val
        cdef Status s = self.row.GetInt32(i, &val)
        raise_on_failure(&s)
        return val

    cdef inline get_int64(self, int i):
        cdef int64_t val
        cdef Status s = self.row.GetInt64(i, &val)
        raise_on_failure(&s)
        return val

    cdef inline get_double(self, int i):
        cdef double val
        cdef Status s = self.row.GetDouble(i, &val)
        raise_on_failure(&s)
        return val

    cdef inline get_float(self, int i):
        cdef float val
        cdef Status s = self.row.GetFloat(i, &val)
        raise_on_failure(&s)
        return val

    cdef inline get_string(self, int i):
        cdef Slice val
        cdef Status s = self.row.GetString(i, &val)
        raise_on_failure(&s)

        return cpython.PyBytes_FromStringAndSize(<char*> val.mutable_data(),
                                                 val.size())

    cdef inline get_slot(self, int i):
        cdef:
            Status s
            DataType t = self.table.loc_type(i)

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
            return self.get_string(i)
        else:
            raise TypeError(t)


cdef class RowBatch:
    """
    Class holding a batch of rows
    """
    # This class owns the KuduRowResult data
    cdef:
        Table table
        vector[KuduRowResult] rows

    def __cinit__(self, Table table):
        self.table = table

    def __len__(self):
        return self.rows.size()

    def __iter__(self):
        pass

    def as_tuples(self):
        """
        To simplify testing for the moment
        """
        tuples = []
        for i in range(self.rows.size()):
            tuples.append(self.get_row(i).as_tuple())
        return tuples

    cpdef get_row(self, i):
        # TODO: boundscheck

        # For safety, we need to increment the parent reference count and hold
        # on to a reference internally so that if the RowBatch goes out of
        # scope we won't end up with orphaned Row objects. This isn't the best,
        # but an intermediate solution until we can do something better..
        #
        # One alternative is to copy the KuduRowResult into the Row, but that
        # doesn't feel right.
        cdef Row row = Row(self, self.table)
        row.row = &self.rows[i]

        return row

cdef class Scanner:
    cdef:
        Table table
        KuduScanner* scanner

    def __cinit__(self, Table table):
        self.table = table
        self.scanner = NULL

    def __dealloc__(self):
        # We own this one
        if self.scanner != NULL:
            del self.scanner

    def add_comparison_predicate(self, col_name, op, value):
        cdef:
            KuduValue* val
            Slice* slc
        if type(col_name) != str:
            raise TypeError("column name must be a string")

        if type(value) == str:
            slc = new Slice(<char*> value, len(value))
            val = KuduValue.CopyString(deref(slc))
            del slc
        elif type(value) == int:
            val = KuduValue.FromInt(<int64_t> value)
        else:
            raise TypeError("unable to convert python type %s" % str(type(value)))

        cdef Slice* col_name_slice = new Slice(<char*> col_name, len(col_name))
        cdef KuduPredicate* pred = self.table.table.get().NewComparisonPredicate(
            deref(col_name_slice), op, val)
        del col_name_slice
        cdef Status s = self.scanner.AddConjunctPredicate(pred)
        raise_on_failure(&s)

    def open(self):
        """
        Returns a reference to itself to faciliate chaining

        Returns
        -------
        self : Scanner
        """
        cdef Status s = self.scanner.Open()
        raise_on_failure(&s)
        return self

    def has_more_rows(self):
        return self.scanner.HasMoreRows()

    def read_all(self):
        """
        Compute a RowBatch containing all rows from the scan operation (which
        hopefully fit into memory, probably not handled gracefully at the
        moment).
        """
        cdef RowBatch batch = RowBatch(self.table)

        while self.has_more_rows():
            self.next_batch(batch)

        return batch

    cpdef next_batch(self, RowBatch batch=None):
        """

        Returns
        -------
        batch : RowBatch
        """
        if not self.has_more_rows():
            raise StopIteration

        if batch is None:
            batch = RowBatch(self.table)
        cdef Status status = self.scanner.NextBatch(&batch.rows)
        raise_on_failure(&status)

        return batch


cdef class KuduError:
    cdef:
        C_KuduError* error

    def __cinit__(self):
        self.error = NULL

    def __dealloc__(self):
        # We own this object
        if self.error != NULL:
            del self.error

    def failed_op(self):
        raise NotImplementedError

    def __repr__(self):
        return "KuduError('%s')" % (self.error.status().ToString())


cdef class WriteOperation:
    cdef:
        Table table
        KuduPartialRow* row
        bint applied

    def __cinit__(self, Table table):
        # This gets called before any subclass cinit methods
        self.table = table
        self.applied = 0

    def __setitem__(self, key, value):
        if isinstance(key, basestring):
            self.set_field(key, value)
        else:
            self.set_loc(key, value)

    cpdef set_field(self, key, value):
        cdef:
            int i = self.table.get_loc(key)
            DataType t = self.table.loc_type(i)
            cdef Slice* slc

        # Leave it to Cython to do the coercion and complain if it doesn't
        # work. Cython will catch many casting problems but we should verify
        # with unit tests.
        if t == KUDU_BOOL:
            self.row.SetBool(i, <bool> value)
        elif t == KUDU_INT8:
            self.row.SetInt8(i, <int8_t> value)
        elif t == KUDU_INT16:
            self.row.SetInt16(i, <int16_t> value)
        elif t == KUDU_INT32:
            self.row.SetInt32(i, <int32_t> value)
        elif t == KUDU_INT64:
            self.row.SetInt64(i, <int64_t> value)
        elif t == KUDU_FLOAT:
            self.row.SetFloat(i, <float> value)
        elif t == KUDU_DOUBLE:
            self.row.SetDouble(i, <double> value)
        elif t == KUDU_STRING:
            # TODO: automatic conversion of unicode to UTF8
            if not cpython.PyBytes_Check(value):
                raise TypeError('Only support byte strings for now')

            # TODO: It would be much better not to heap-allocate a Slice object
            slc = new Slice(cpython.PyBytes_AsString(value))

            # Not safe to take a reference to PyBytes data for now
            self.row.SetStringCopy(i, deref(slc))
            del slc

    cpdef set_loc(self, int i, value):
        pass

    cpdef set_field_null(self, key):
        pass

    cpdef set_loc_null(self, int i):
        pass

    cdef add_to_session(self, Session s):
        pass


cdef class Insert(WriteOperation):
    cdef:
        KuduInsert* op

    def __cinit__(self, Table table):
        self.op = self.table.table.get().NewInsert()
        self.row = self.op.mutable_row()

    def __dealloc__(self):
        del self.op

    cdef add_to_session(self, Session s):
        if self.applied:
            raise Exception

        s.s.get().Apply(self.op)
        self.op = NULL
        self.applied = 1


cdef class Update(WriteOperation):
    cdef:
        KuduUpdate* op

    def __cinit__(self, Table table):
        self.table = table
        self.op = table.table.get().NewUpdate()
        self.row = self.op.mutable_row()

    def __dealloc__(self):
        del self.op

    cdef add_to_session(self, Session s):
        pass


cdef class Delete(WriteOperation):
    cdef:
        KuduDelete* op

    def __cinit__(self, Table table):
        self.table = table
        self.op = table.table.get().NewDelete()
        self.row = self.op.mutable_row()

    def __dealloc__(self):
        del self.op

    cdef add_to_session(self, Session s):
        if self.applied:
            raise Exception

        s.s.get().Apply(self.op)
        self.applied = 1
        self.op = NULL


cdef class Column:
    cdef:
        Table parent

    def __cinit__(self, parent):
        self.parent = parent
