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

from libc.stdint cimport *
from libcpp cimport bool as c_bool
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.map cimport map

# This must be included for cerr and other things to work
cdef extern from "<iostream>":
    pass

#----------------------------------------------------------------------
# Smart pointers and such

cdef extern from "kudu/client/shared_ptr.h" namespace "kudu::client::sp" nogil:

    cdef cppclass shared_ptr[T]:
        T* get()
        void reset()
        void reset(T* p)

cdef extern from "kudu/util/status.h" namespace "kudu" nogil:

    # We can later add more of the common status factory methods as needed
    cdef Status Status_OK "Status::OK"()

    cdef cppclass Status:
        Status()

        string ToString()

        Slice message()

        c_bool ok()
        c_bool IsNotFound()
        c_bool IsCorruption()
        c_bool IsNotSupported()
        c_bool IsIOError()
        c_bool IsInvalidArgument()
        c_bool IsAlreadyPresent()
        c_bool IsRuntimeError()
        c_bool IsNetworkError()
        c_bool IsIllegalState()
        c_bool IsNotAuthorized()
        c_bool IsAborted()

cdef extern from "kudu/util/int128.h" namespace "kudu":
    # see https://gist.github.com/ricrogz/7f9c405450689866139c49476b959044
    # This can be defined on the Python side even if it's not defined on the
    # C++ side.
    ctypedef int int128_t

cdef extern from "kudu/util/monotime.h" namespace "kudu" nogil:

    # These classes are not yet needed directly but will need to be completed
    # from the C++ API
    cdef cppclass MonoDelta:
        MonoDelta()

        @staticmethod
        MonoDelta FromSeconds(double seconds)

        @staticmethod
        MonoDelta FromMilliseconds(int64_t ms)

        @staticmethod
        MonoDelta FromMicroseconds(int64_t us)

        @staticmethod
        MonoDelta FromNanoseconds(int64_t ns)

        c_bool Initialized()
        c_bool LessThan(const MonoDelta& other)
        c_bool MoreThan(const MonoDelta& other)
        c_bool Equals(const MonoDelta& other)

        string ToString()

        double ToSeconds()
        int64_t ToMilliseconds()
        int64_t ToMicroseconds()
        int64_t ToNanoseconds()

        # TODO, when needed
        # void ToTimeVal(struct timeval *tv)
        # void ToTimeSpec(struct timespec *ts)

        # @staticmethod
        # void NanosToTimeSpec(int64_t nanos, struct timespec* ts);


    cdef cppclass MonoTime:
        pass


cdef extern from "kudu/client/schema.h" namespace "kudu::client" nogil:

    enum DataType" kudu::client::KuduColumnSchema::DataType":
        KUDU_INT8 " kudu::client::KuduColumnSchema::INT8"
        KUDU_INT16 " kudu::client::KuduColumnSchema::INT16"
        KUDU_INT32 " kudu::client::KuduColumnSchema::INT32"
        KUDU_INT64 " kudu::client::KuduColumnSchema::INT64"
        KUDU_STRING " kudu::client::KuduColumnSchema::STRING"
        KUDU_BOOL " kudu::client::KuduColumnSchema::BOOL"
        KUDU_FLOAT " kudu::client::KuduColumnSchema::FLOAT"
        KUDU_DOUBLE " kudu::client::KuduColumnSchema::DOUBLE"
        KUDU_BINARY " kudu::client::KuduColumnSchema::BINARY"
        KUDU_UNIXTIME_MICROS " kudu::client::KuduColumnSchema::UNIXTIME_MICROS"
        KUDU_DECIMAL " kudu::client::KuduColumnSchema::DECIMAL"
        KUDU_VARCHAR " kudu::client::KuduColumnSchema::VARCHAR"
        KUDU_DATE " kudu::client::KuduColumnSchema::DATE"

    enum EncodingType" kudu::client::KuduColumnStorageAttributes::EncodingType":
        EncodingType_AUTO " kudu::client::KuduColumnStorageAttributes::AUTO_ENCODING"
        EncodingType_PLAIN " kudu::client::KuduColumnStorageAttributes::PLAIN_ENCODING"
        EncodingType_PREFIX " kudu::client::KuduColumnStorageAttributes::PREFIX_ENCODING"
        EncodingType_BIT_SHUFFLE " kudu::client::KuduColumnStorageAttributes::BIT_SHUFFLE"
        EncodingType_RLE " kudu::client::KuduColumnStorageAttributes::RLE"
        EncodingType_DICT " kudu::client::KuduColumnStorageAttributes::DICT_ENCODING"

    enum CompressionType" kudu::client::KuduColumnStorageAttributes::CompressionType":
        CompressionType_DEFAULT " kudu::client::KuduColumnStorageAttributes::DEFAULT_COMPRESSION"
        CompressionType_NONE " kudu::client::KuduColumnStorageAttributes::NO_COMPRESSION"
        CompressionType_SNAPPY " kudu::client::KuduColumnStorageAttributes::SNAPPY"
        CompressionType_LZ4 " kudu::client::KuduColumnStorageAttributes::LZ4"
        CompressionType_ZLIB " kudu::client::KuduColumnStorageAttributes::ZLIB"

    cdef struct KuduColumnStorageAttributes:
        KuduColumnStorageAttributes()

        EncodingType encoding
        CompressionType compression
        string ToString()

    cdef cppclass KuduColumnTypeAttributes:
        KuduColumnTypeAttributes()
        KuduColumnTypeAttributes(const KuduColumnTypeAttributes& other)
        KuduColumnTypeAttributes(int8_t precision, int8_t scale)
        KuduColumnTypeAttributes(uint16_t length)

        int8_t precision()
        int8_t scale()
        uint16_t length()

        c_bool Equals(KuduColumnTypeAttributes& other)
        void CopyFrom(KuduColumnTypeAttributes& other)

    cdef cppclass KuduColumnSchema:
        KuduColumnSchema(const KuduColumnSchema& other)
        KuduColumnSchema(const string& name, DataType type)
        KuduColumnSchema(const string& name, DataType type, c_bool is_nullable)
        KuduColumnSchema(const string& name, DataType type, c_bool is_nullable,
                         const void* default_value)

        string& name()
        c_bool is_nullable()
        DataType type()
        KuduColumnTypeAttributes type_attributes()

        c_bool Equals(KuduColumnSchema& other)
        void CopyFrom(KuduColumnSchema& other)

    cdef cppclass KuduSchema:
        KuduSchema()
        KuduSchema(const KuduSchema& schema)
        KuduSchema(vector[KuduColumnSchema]& columns, int key_columns)

        c_bool Equals(const KuduSchema& other)
        KuduColumnSchema Column(size_t idx)
        size_t num_columns()

        void GetPrimaryKeyColumnIndexes(vector[int]* indexes)

        KuduPartialRow* NewRow()

    cdef cppclass KuduColumnSpec:

         KuduColumnSpec* Default(C_KuduValue* value)
         KuduColumnSpec* RemoveDefault()

         KuduColumnSpec* Compression(CompressionType compression)
         KuduColumnSpec* Encoding(EncodingType encoding)
         KuduColumnSpec* BlockSize(int32_t block_size)

         KuduColumnSpec* PrimaryKey()
         KuduColumnSpec* NotNull()
         KuduColumnSpec* Nullable()
         KuduColumnSpec* Type(DataType type_)

         KuduColumnSpec* Precision(int8_t precision);
         KuduColumnSpec* Scale(int8_t scale);
         KuduColumnSpec* Length(uint16_t length);

         KuduColumnSpec* RenameTo(const string& new_name)


    cdef cppclass KuduSchemaBuilder:

        KuduColumnSpec* AddColumn(string& name)
        KuduSchemaBuilder* SetPrimaryKey(vector[string]& key_col_names);

        Status Build(KuduSchema* schema)

cdef extern from "kudu/client/scan_batch.h" namespace "kudu::client" nogil:

    cdef cppclass KuduScanBatch:
        int NumRows() const;
        KuduRowPtr Row(int idx) const;
        const KuduSchema* projection_schema() const;

    cdef cppclass KuduRowPtr " kudu::client::KuduScanBatch::RowPtr":
        c_bool IsNull(Slice& col_name)
        c_bool IsNull(int col_idx)

        # These getters return a bad Status if the type does not match,
        # the value is unset, or the value is NULL. Otherwise they return
        # the current set value in *val.
        Status GetBool(Slice& col_name, c_bool* val)
        Status GetBool(int col_idx, c_bool* val)

        Status GetInt8(Slice& col_name, int8_t* val)
        Status GetInt8(int col_idx, int8_t* val)

        Status GetInt16(Slice& col_name, int16_t* val)
        Status GetInt16(int col_idx, int16_t* val)

        Status GetInt32(Slice& col_name, int32_t* val)
        Status GetInt32(int col_idx, int32_t* val)

        Status GetInt64(Slice& col_name, int64_t* val)
        Status GetInt64(int col_idx, int64_t* val)

        Status GetUnixTimeMicros(const Slice& col_name,
                            int64_t* micros_since_utc_epoch)
        Status GetUnixTimeMicros(int col_idx,
                            int64_t* micros_since_utc_epoch)


        Status GetFloat(Slice& col_name, float* val)
        Status GetFloat(int col_idx, float* val)

        Status GetDouble(Slice& col_name, double* val)
        Status GetDouble(int col_idx, double* val)

        Status GetUnscaledDecimal(Slice& col_name, int128_t* val)
        Status GetUnscaledDecimal(int col_idx, int128_t* val)

        Status GetString(Slice& col_name, Slice* val)
        Status GetString(int col_idx, Slice* val)

        Status GetBinary(const Slice& col_name, Slice* val)
        Status GetBinary(int col_idx, Slice* val)

        Status GetVarchar(const Slice& col_name, Slice* val)
        Status GetVarchar(int col_idx, Slice* val)

        Status GetDate(Slice& col_name, int32_t* val)
        Status GetDate(int col_idx, int32_t* val)

        const void* cell(int col_idx)
        string ToString()

cdef extern from "kudu/util/slice.h" namespace "kudu" nogil:

    cdef cppclass Slice:
        Slice()
        Slice(const uint8_t* data, size_t n)
        Slice(const char* data, size_t n)

        Slice(string& s)
        Slice(const char* s)

        # Many other constructors have been omitted; we can return and add them
        # as needed for the code generation.

        const uint8_t* data()
        uint8_t* mutable_data()
        size_t size()
        c_bool empty()

        uint8_t operator[](size_t n)

        void clear()
        void remove_prefix(size_t n)
        void truncate(size_t n)

        Status check_size(size_t expected_size)

        string ToString()

        string ToDebugString()
        string ToDebugString(size_t max_len)

        int compare(Slice& b)

        c_bool starts_with(Slice& x)

        void relocate(uint8_t* d)

        # Many other API methods omitted


cdef extern from "kudu/common/partial_row.h" namespace "kudu" nogil:

    cdef cppclass KuduPartialRow:
        # Schema must not be garbage-collected
        # KuduPartialRow(const Schema* schema)

        #----------------------------------------------------------------------
        # Setters

        # Slice setters
        Status SetBool(Slice& col_name, c_bool val)

        Status SetInt8(Slice& col_name, int8_t val)
        Status SetInt16(Slice& col_name, int16_t val)
        Status SetInt32(Slice& col_name, int32_t val)
        Status SetInt64(Slice& col_name, int64_t val)

        Status SetUnixTimeMicros(const Slice& col_name,
                                 int64_t micros_since_utc_epoch)
        Status SetUnixTimeMicros(int col_idx, int64_t micros_since_utc_epoch)

        Status SetDouble(Slice& col_name, double val)
        Status SetFloat(Slice& col_name, float val)

        Status SetUnscaledDecimal(const Slice& col_name, int128_t val)

        # Integer setters
        Status SetBool(int col_idx, c_bool val)

        Status SetInt8(int col_idx, int8_t val)
        Status SetInt16(int col_idx, int16_t val)
        Status SetInt32(int col_idx, int32_t val)
        Status SetInt64(int col_idx, int64_t val)

        Status SetDouble(int col_idx, double val)
        Status SetFloat(int col_idx, float val)

        Status SetUnscaledDecimal(int col_idx, int128_t val)

        # Set, but does not copy string
        Status SetString(Slice& col_name, Slice& val)
        Status SetString(int col_idx, Slice& val)

        Status SetStringCopy(Slice& col_name, Slice& val)
        Status SetStringCopy(int col_idx, Slice& val)

        Status SetBinary(Slice& col_name, Slice& val)
        Status SetBinary(int col_idx, Slice&val)

        Status SetBinaryCopy(const Slice& col_name, const Slice& val)
        Status SetBinaryCopy(int col_idx, const Slice& val)

        Status SetVarchar(Slice& col_name, Slice& val)
        Status SetVarchar(int col_idx, Slice& val)

        Status SetDate(Slice& col_name, int32_t val)
        Status SetDate(int col_idx, int32_t val)

        Status SetNull(Slice& col_name)
        Status SetNull(int col_idx)

        Status Unset(Slice& col_name)
        Status Unset(int col_idx)

        #----------------------------------------------------------------------
        # Getters

        c_bool IsColumnSet(Slice& col_name)
        c_bool IsColumnSet(int col_idx)

        c_bool IsNull(Slice& col_name)
        c_bool IsNull(int col_idx)

        Status GetBool(Slice& col_name, c_bool* val)
        Status GetBool(int col_idx, c_bool* val)

        Status GetInt8(Slice& col_name, int8_t* val)
        Status GetInt8(int col_idx, int8_t* val)

        Status GetInt16(Slice& col_name, int16_t* val)
        Status GetInt16(int col_idx, int16_t* val)

        Status GetInt32(Slice& col_name, int32_t* val)
        Status GetInt32(int col_idx, int32_t* val)

        Status GetInt64(Slice& col_name, int64_t* val)
        Status GetInt64(int col_idx, int64_t* val)

        Status GetUnixTimeMicros(const Slice& col_name,
                            int64_t* micros_since_utc_epoch)
        Status GetUnixTimeMicros(int col_idx, int64_t* micros_since_utc_epoch)

        Status GetDouble(Slice& col_name, double* val)
        Status GetDouble(int col_idx, double* val)

        Status GetFloat(Slice& col_name, float* val)
        Status GetFloat(int col_idx, float* val)

        Status GetUnscaledDecimal(Slice& col_name, int128_t* val)
        Status GetUnscaledDecimal(int col_idx, int128_t* val)

        # Gets the string but does not copy the value. Callers should
        # copy the resulting Slice if necessary.
        Status GetString(Slice& col_name, Slice* val)
        Status GetString(int col_idx, Slice* val)

        Status GetBinary(const Slice& col_name, Slice* val)
        Status GetBinary(int col_idx, Slice* val)

        Status GetVarchar(Slice& col_name, Slice* val)
        Status GetVarchar(int col_idx, Slice* val)

        Status EncodeRowKey(string* encoded_key)
        string ToEncodedRowKeyOrDie()

        # Return true if all of the key columns have been specified
        # for this mutation.
        c_bool IsKeySet()

        # Return true if all columns have been specified.
        c_bool AllColumnsSet()
        string ToString()

        # const Schema* schema()


cdef extern from "kudu/client/write_op.h" namespace "kudu::client" nogil:

    enum WriteType" kudu::client::KuduWriteOperation::Type":
        INSERT " kudu::client::KuduWriteOperation::INSERT"
        UPDATE " kudu::client::KuduWriteOperation::UPDATE"
        DELETE " kudu::client::KuduWriteOperation::DELETE"

    cdef cppclass KuduWriteOperation:
        KuduPartialRow& row()
        KuduPartialRow* mutable_row()

        # This is a pure virtual function implemented on each of the cppclass
        # subclasses
        string ToString()

        # Also a pure virtual
        WriteType type()

    cdef cppclass KuduInsert(KuduWriteOperation):
        pass

    cdef cppclass KuduInsertIgnore(KuduWriteOperation):
        pass

    cdef cppclass KuduUpsert(KuduWriteOperation):
        pass

    cdef cppclass KuduDelete(KuduWriteOperation):
        pass

    cdef cppclass KuduUpdate(KuduWriteOperation):
        pass


cdef extern from "kudu/client/scan_predicate.h" namespace "kudu::client" nogil:
    enum ComparisonOp" kudu::client::KuduPredicate::ComparisonOp":
        KUDU_LESS_EQUAL    " kudu::client::KuduPredicate::LESS_EQUAL"
        KUDU_GREATER_EQUAL " kudu::client::KuduPredicate::GREATER_EQUAL"
        KUDU_EQUAL         " kudu::client::KuduPredicate::EQUAL"
        KUDU_LESS          " kudu::client::KuduPredicate::LESS"
        KUDU_GREATER       " kudu::client::KuduPredicate::GREATER"

    cdef cppclass KuduPredicate:
        KuduPredicate* Clone()


cdef extern from "kudu/client/value.h" namespace "kudu::client" nogil:

    cdef cppclass C_KuduValue "kudu::client::KuduValue":
        @staticmethod
        C_KuduValue* FromInt(int64_t val);

        @staticmethod
        C_KuduValue* FromFloat(float val);

        @staticmethod
        C_KuduValue* FromDouble(double val);

        @staticmethod
        C_KuduValue* FromBool(c_bool val);

        @staticmethod
        C_KuduValue* FromDecimal(int128_t val, int8_t scale);

        @staticmethod
        C_KuduValue* CopyString(const Slice& s);


cdef extern from "kudu/client/client.h" namespace "kudu::client" nogil:

    enum ReplicaSelection" kudu::client::KuduClient::ReplicaSelection":
        ReplicaSelection_Leader " kudu::client::KuduClient::LEADER_ONLY"
        ReplicaSelection_Closest " kudu::client::KuduClient::CLOSEST_REPLICA"
        ReplicaSelection_First " kudu::client::KuduClient::FIRST_REPLICA"

    enum ReadMode" kudu::client::KuduScanner::ReadMode":
        ReadMode_Latest " kudu::client::KuduScanner::READ_LATEST"
        ReadMode_Snapshot " kudu::client::KuduScanner::READ_AT_SNAPSHOT"
        ReadMode_ReadYourWrites " kudu::client::KuduScanner::READ_YOUR_WRITES"

    enum RangePartitionBound" kudu::client::KuduTableCreator::RangePartitionBound":
        PartitionType_Exclusive " kudu::client::KuduTableCreator::EXCLUSIVE_BOUND"
        PartitionType_Inclusive " kudu::client::KuduTableCreator::INCLUSIVE_BOUND"

    Status DisableOpenSSLInitialization()

    cdef cppclass KuduClient:

        Status DeleteTable(const string& table_name)
        Status OpenTable(const string& table_name,
                         shared_ptr[KuduTable]* table)
        Status GetTableSchema(const string& table_name, KuduSchema* schema)

        KuduTableCreator* NewTableCreator()
        Status IsCreateTableInProgress(const string& table_name,
                                       c_bool* create_in_progress)

        c_bool IsMultiMaster()

        Status ListTables(vector[string]* tables)
        Status ListTables(vector[string]* tables, const string& filter)

        Status ListTabletServers(vector[KuduTabletServer*]* tablet_servers)

        Status TableExists(const string& table_name, c_bool* exists)

        KuduTableAlterer* NewTableAlterer(const string& table_name)
        Status IsAlterTableInProgress(const string& table_name,
                                      c_bool* alter_in_progress)
        uint64_t GetLatestObservedTimestamp()

        shared_ptr[KuduSession] NewSession()

    cdef cppclass KuduClientBuilder:
        KuduClientBuilder()
        KuduClientBuilder& master_server_addrs(const vector[string]& addrs)
        KuduClientBuilder& add_master_server_addr(const string& addr)

        KuduClientBuilder& default_admin_operation_timeout(
            const MonoDelta& timeout)

        KuduClientBuilder& default_rpc_timeout(const MonoDelta& timeout)

        Status Build(shared_ptr[KuduClient]* client)

    cdef cppclass KuduTabletServer:
        KuduTabletServer()
        string& uuid()
        string& hostname()
        uint16_t port()

    cdef cppclass KuduTableCreator:
        KuduTableCreator& table_name(string& name)
        KuduTableCreator& schema(KuduSchema* schema)
        KuduTableCreator& add_hash_partitions(vector[string]& columns,
                                              int num_buckets)
        KuduTableCreator& add_hash_partitions(vector[string]& columns,
                                              int num_buckets,
                                              int seed)
        KuduTableCreator& set_range_partition_columns(vector[string]& columns)
        KuduTableCreator& add_range_partition(KuduPartialRow* lower_bound,
                                              KuduPartialRow* upper_bound,
                                              RangePartitionBound lower_bound_type,
                                              RangePartitionBound upper_bound_type)
        KuduTableCreator& add_range_partition_split(KuduPartialRow* split_row)
        KuduTableCreator& split_rows(vector[const KuduPartialRow*]& split_rows)
        KuduTableCreator& num_replicas(int n_replicas)
        KuduTableCreator& wait(c_bool wait)

        Status Create()

    cdef cppclass KuduTableAlterer:
        KuduTableAlterer& RenameTo(const string& new_name)
        KuduColumnSpec* AddColumn(const string& name)
        KuduColumnSpec* AlterColumn(const string& name)
        KuduTableAlterer& DropColumn(const string& name)
        KuduTableAlterer& AddRangePartition(KuduPartialRow* lower_bound,
                                            KuduPartialRow* upper_bound,
                                            RangePartitionBound lower_bound_type,
                                            RangePartitionBound upper_bound_type)
        KuduTableAlterer& DropRangePartition(KuduPartialRow* lower_bound,
                                             KuduPartialRow* upper_bound,
                                             RangePartitionBound lower_bound_type,
                                             RangePartitionBound upper_bound_type)

        KuduTableAlterer& wait(c_bool wait)

        Status Alter()

    # Instances of KuduTable are not directly instantiated by users of the
    # client.
    cdef cppclass KuduTable:

        string& name()
        string& id()
        KuduSchema& schema()
        int num_replicas()

        KuduInsert* NewInsert()
        KuduInsertIgnore* NewInsertIgnore()
        KuduUpsert* NewUpsert()
        KuduUpdate* NewUpdate()
        KuduDelete* NewDelete()

        KuduPredicate* NewComparisonPredicate(const Slice& col_name,
                                              ComparisonOp op,
                                              C_KuduValue* value);
        KuduPredicate* NewInListPredicate(const Slice& col_name,
                                          vector[C_KuduValue*]* values)
        KuduPredicate* NewIsNotNullPredicate(const Slice& col_name)
        KuduPredicate* NewIsNullPredicate(const Slice& col_name)

        KuduClient* client()
        # const PartitionSchema& partition_schema()

    enum FlushMode" kudu::client::KuduSession::FlushMode":
        FlushMode_AutoSync " kudu::client::KuduSession::AUTO_FLUSH_SYNC"
        FlushMode_AutoBackground " kudu::client::KuduSession::AUTO_FLUSH_BACKGROUND"
        FlushMode_Manual " kudu::client::KuduSession::MANUAL_FLUSH"

    cdef cppclass KuduSession:

        Status SetFlushMode(FlushMode m)

        Status SetMutationBufferSpace(size_t size)
        Status SetMutationBufferFlushWatermark(double watermark_pct)
        Status SetMutationBufferFlushInterval(unsigned int millis)
        Status SetMutationBufferMaxNum(unsigned int max_num)

        void SetTimeoutMillis(int millis)

        void SetPriority(int priority)

        Status Apply(KuduWriteOperation* write_op)
        Status Apply(KuduInsert* write_op)
        Status Apply(KuduInsertIgnore* write_op)
        Status Apply(KuduUpsert* write_op)
        Status Apply(KuduUpdate* write_op)
        Status Apply(KuduDelete* write_op)

        # This is thread-safe
        Status Flush()

        # TODO: Will need to decide on a strategy for exposing the session's
        # async API to Python

        # Status ApplyAsync(KuduWriteOperation* write_op,
        #                   KuduStatusCallback cb)
        # Status ApplyAsync(KuduInsert* write_op,
        #                   KuduStatusCallback cb)
        # Status ApplyAsync(KuduUpdate* write_op,
        #                   KuduStatusCallback cb)
        # Status ApplyAsync(KuduDelete* write_op,
        #                   KuduStatusCallback cb)
        # void FlushAsync(KuduStatusCallback& cb)


        Status Close()
        c_bool HasPendingOperations()
        int CountBufferedOperations()

        int CountPendingErrors()
        void GetPendingErrors(vector[C_KuduError*]* errors, c_bool* overflowed)

        KuduClient* client()


    cdef cppclass KuduScanner:
        KuduScanner(KuduTable* table)

        Status AddConjunctPredicate(KuduPredicate* pred)

        Status Open()
        void Close()

        c_bool HasMoreRows()
        Status NextBatch(KuduScanBatch* batch)
        Status SetBatchSizeBytes(uint32_t batch_size)
        Status SetSelection(ReplicaSelection selection)
        Status SetCacheBlocks(c_bool cache_blocks)
        Status SetReadMode(ReadMode read_mode)
        Status SetSnapshotMicros(uint64_t snapshot_timestamp_micros)
        Status SetTimeoutMillis(int millis)
        Status SetProjectedColumnNames(const vector[string]& col_names)
        Status SetProjectedColumnIndexes(const vector[int]& col_indexes)
        Status SetFaultTolerant()
        Status AddLowerBound(const KuduPartialRow& key)
        Status AddExclusiveUpperBound(const KuduPartialRow& key)
        Status SetLimit(int64_t limit)
        Status KeepAlive()
        Status GetCurrentServer(KuduTabletServer** server)

        KuduSchema GetProjectionSchema()
        const ResourceMetrics& GetResourceMetrics()
        string ToString()

    cdef cppclass KuduScanToken:
        KuduScanToken()

        const KuduTablet& tablet()

        Status IntoKuduScanner(KuduScanner** scanner)
        Status Serialize(string* buf)
        Status DeserializeIntoScanner(KuduClient* client,
                                      const string& serialized_token,
                                      KuduScanner** scanner)

    cdef cppclass KuduScanTokenBuilder:
        KuduScanTokenBuilder(KuduTable* table)

        Status SetProjectedColumnNames(const vector[string]& col_names)
        Status SetProjectedColumnIndexes(const vector[int]& col_indexes)
        Status SetBatchSizeBytes(uint32_t batch_size)
        Status SetReadMode(ReadMode read_mode)
        Status SetFaultTolerant()
        Status SetSnapshotMicros(uint64_t snapshot_timestamp_micros)
        Status SetSelection(ReplicaSelection selection)
        Status SetTimeoutMillis(int millis)
        Status AddConjunctPredicate(KuduPredicate* pred)
        Status AddLowerBound(const KuduPartialRow& key)
        Status AddUpperBound(const KuduPartialRow& key)
        Status SetCacheBlocks(c_bool cache_blocks)
        Status Build(vector[KuduScanToken*]* tokens)

    cdef cppclass KuduTablet:
        KuduTablet()

        const string& id()
        const vector[const KuduReplica*]& replicas()

    cdef cppclass KuduReplica:
        KuduReplica()

        c_bool is_leader()
        const KuduTabletServer& ts()

    cdef cppclass C_KuduError " kudu::client::KuduError":

        Status& status()

        KuduWriteOperation& failed_op()
        KuduWriteOperation* release_failed_op()

        c_bool was_possibly_successful()

cdef extern from "kudu/client/resource_metrics.h" namespace "kudu::client" nogil:

    cdef cppclass ResourceMetrics:
        ResourceMetrics()

        map[string, int64_t] Get()
        int64_t GetMetric(const string& name)
