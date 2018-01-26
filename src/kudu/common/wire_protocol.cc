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

#include "kudu/common/wire_protocol.h"

#include <cstdint>
#include <cstring>
#include <ostream>
#include <string>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/common/columnblock.h"
#include "kudu/common/column_predicate.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/row.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/fixedarray.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/fastmem.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/compression/compression.pb.h"
#include "kudu/util/faststring.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/safe_math.h"
#include "kudu/util/slice.h"

using google::protobuf::RepeatedPtrField;
using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {

void StatusToPB(const Status& status, AppStatusPB* pb) {
  pb->Clear();
  bool is_unknown = false;
  if (status.ok()) {
    pb->set_code(AppStatusPB::OK);
    // OK statuses don't have any message or posix code.
    return;
  } else if (status.IsNotFound()) {
    pb->set_code(AppStatusPB::NOT_FOUND);
  } else if (status.IsCorruption()) {
    pb->set_code(AppStatusPB::CORRUPTION);
  } else if (status.IsNotSupported()) {
    pb->set_code(AppStatusPB::NOT_SUPPORTED);
  } else if (status.IsInvalidArgument()) {
    pb->set_code(AppStatusPB::INVALID_ARGUMENT);
  } else if (status.IsIOError()) {
    pb->set_code(AppStatusPB::IO_ERROR);
  } else if (status.IsAlreadyPresent()) {
    pb->set_code(AppStatusPB::ALREADY_PRESENT);
  } else if (status.IsRuntimeError()) {
    pb->set_code(AppStatusPB::RUNTIME_ERROR);
  } else if (status.IsNetworkError()) {
    pb->set_code(AppStatusPB::NETWORK_ERROR);
  } else if (status.IsIllegalState()) {
    pb->set_code(AppStatusPB::ILLEGAL_STATE);
  } else if (status.IsNotAuthorized()) {
    pb->set_code(AppStatusPB::NOT_AUTHORIZED);
  } else if (status.IsAborted()) {
    pb->set_code(AppStatusPB::ABORTED);
  } else if (status.IsRemoteError()) {
    pb->set_code(AppStatusPB::REMOTE_ERROR);
  } else if (status.IsServiceUnavailable()) {
    pb->set_code(AppStatusPB::SERVICE_UNAVAILABLE);
  } else if (status.IsTimedOut()) {
    pb->set_code(AppStatusPB::TIMED_OUT);
  } else if (status.IsUninitialized()) {
    pb->set_code(AppStatusPB::UNINITIALIZED);
  } else if (status.IsConfigurationError()) {
    pb->set_code(AppStatusPB::CONFIGURATION_ERROR);
  } else if (status.IsIncomplete()) {
    pb->set_code(AppStatusPB::INCOMPLETE);
  } else if (status.IsEndOfFile()) {
    pb->set_code(AppStatusPB::END_OF_FILE);
  } else {
    LOG(WARNING) << "Unknown error code translation from internal error "
                 << status.ToString() << ": sending UNKNOWN_ERROR";
    pb->set_code(AppStatusPB::UNKNOWN_ERROR);
    is_unknown = true;
  }
  if (is_unknown) {
    // For unknown status codes, include the original stringified error
    // code.
    pb->set_message(status.CodeAsString() + ": " +
                    status.message().ToString());
  } else {
    // Otherwise, just encode the message itself, since the other end
    // will reconstruct the other parts of the ToString() response.
    pb->set_message(status.message().ToString());
  }
  if (status.posix_code() != -1) {
    pb->set_posix_code(status.posix_code());
  }
}

Status StatusFromPB(const AppStatusPB& pb) {
  int posix_code = pb.has_posix_code() ? pb.posix_code() : -1;

  switch (pb.code()) {
    case AppStatusPB::OK:
      return Status::OK();
    case AppStatusPB::NOT_FOUND:
      return Status::NotFound(pb.message(), "", posix_code);
    case AppStatusPB::CORRUPTION:
      return Status::Corruption(pb.message(), "", posix_code);
    case AppStatusPB::NOT_SUPPORTED:
      return Status::NotSupported(pb.message(), "", posix_code);
    case AppStatusPB::INVALID_ARGUMENT:
      return Status::InvalidArgument(pb.message(), "", posix_code);
    case AppStatusPB::IO_ERROR:
      return Status::IOError(pb.message(), "", posix_code);
    case AppStatusPB::ALREADY_PRESENT:
      return Status::AlreadyPresent(pb.message(), "", posix_code);
    case AppStatusPB::RUNTIME_ERROR:
      return Status::RuntimeError(pb.message(), "", posix_code);
    case AppStatusPB::NETWORK_ERROR:
      return Status::NetworkError(pb.message(), "", posix_code);
    case AppStatusPB::ILLEGAL_STATE:
      return Status::IllegalState(pb.message(), "", posix_code);
    case AppStatusPB::NOT_AUTHORIZED:
      return Status::NotAuthorized(pb.message(), "", posix_code);
    case AppStatusPB::ABORTED:
      return Status::Aborted(pb.message(), "", posix_code);
    case AppStatusPB::REMOTE_ERROR:
      return Status::RemoteError(pb.message(), "", posix_code);
    case AppStatusPB::SERVICE_UNAVAILABLE:
      return Status::ServiceUnavailable(pb.message(), "", posix_code);
    case AppStatusPB::TIMED_OUT:
      return Status::TimedOut(pb.message(), "", posix_code);
    case AppStatusPB::UNINITIALIZED:
      return Status::Uninitialized(pb.message(), "", posix_code);
    case AppStatusPB::CONFIGURATION_ERROR:
      return Status::ConfigurationError(pb.message(), "", posix_code);
    case AppStatusPB::INCOMPLETE:
      return Status::Incomplete(pb.message(), "", posix_code);
    case AppStatusPB::END_OF_FILE:
      return Status::EndOfFile(pb.message(), "", posix_code);
    case AppStatusPB::UNKNOWN_ERROR:
    default:
      LOG(WARNING) << "Unknown error code in status: " << SecureShortDebugString(pb);
      return Status::RuntimeError("(unknown error code)", pb.message(), posix_code);
  }
}

Status HostPortToPB(const HostPort& host_port, HostPortPB* host_port_pb) {
  host_port_pb->set_host(host_port.host());
  host_port_pb->set_port(host_port.port());
  return Status::OK();
}

Status HostPortFromPB(const HostPortPB& host_port_pb, HostPort* host_port) {
  host_port->set_host(host_port_pb.host());
  host_port->set_port(host_port_pb.port());
  return Status::OK();
}

Status AddHostPortPBs(const vector<Sockaddr>& addrs,
                      RepeatedPtrField<HostPortPB>* pbs) {
  for (const Sockaddr& addr : addrs) {
    HostPortPB* pb = pbs->Add();
    if (addr.IsWildcard()) {
      RETURN_NOT_OK(GetFQDN(pb->mutable_host()));
    } else {
      pb->set_host(addr.host());
    }
    pb->set_port(addr.port());
  }
  return Status::OK();
}

Status SchemaToPB(const Schema& schema, SchemaPB *pb, int flags) {
  pb->Clear();
  return SchemaToColumnPBs(schema, pb->mutable_columns(), flags);
}

Status SchemaFromPB(const SchemaPB& pb, Schema *schema) {
  return ColumnPBsToSchema(pb.columns(), schema);
}

void ColumnSchemaToPB(const ColumnSchema& col_schema, ColumnSchemaPB *pb, int flags) {
  pb->Clear();
  pb->set_name(col_schema.name());
  pb->set_type(col_schema.type_info()->type());
  pb->set_is_nullable(col_schema.is_nullable());
  if (!(flags & SCHEMA_PB_WITHOUT_STORAGE_ATTRIBUTES)) {
    pb->set_encoding(col_schema.attributes().encoding);
    pb->set_compression(col_schema.attributes().compression);
    pb->set_cfile_block_size(col_schema.attributes().cfile_block_size);
  }
  if (col_schema.has_read_default()) {
    if (col_schema.type_info()->physical_type() == BINARY) {
      const Slice *read_slice = static_cast<const Slice *>(col_schema.read_default_value());
      pb->set_read_default_value(read_slice->data(), read_slice->size());
    } else {
      const void *read_value = col_schema.read_default_value();
      pb->set_read_default_value(read_value, col_schema.type_info()->size());
    }
  }
  if (col_schema.has_write_default() && !(flags & SCHEMA_PB_WITHOUT_WRITE_DEFAULT)) {
    if (col_schema.type_info()->physical_type() == BINARY) {
      const Slice *write_slice = static_cast<const Slice *>(col_schema.write_default_value());
      pb->set_write_default_value(write_slice->data(), write_slice->size());
    } else {
      const void *write_value = col_schema.write_default_value();
      pb->set_write_default_value(write_value, col_schema.type_info()->size());
    }
  }
}

ColumnSchema ColumnSchemaFromPB(const ColumnSchemaPB& pb) {
  const void *write_default_ptr = nullptr;
  const void *read_default_ptr = nullptr;
  Slice write_default;
  Slice read_default;
  const TypeInfo* typeinfo = GetTypeInfo(pb.type());
  if (pb.has_read_default_value()) {
    read_default = Slice(pb.read_default_value());
    if (typeinfo->physical_type() == BINARY) {
      read_default_ptr = &read_default;
    } else {
      read_default_ptr = read_default.data();
    }
  }
  if (pb.has_write_default_value()) {
    write_default = Slice(pb.write_default_value());
    if (typeinfo->physical_type() == BINARY) {
      write_default_ptr = &write_default;
    } else {
      write_default_ptr = write_default.data();
    }
  }

  ColumnStorageAttributes attributes;
  if (pb.has_encoding()) {
    attributes.encoding = pb.encoding();
  }
  if (pb.has_compression()) {
    attributes.compression = pb.compression();
  }
  if (pb.has_cfile_block_size()) {
    attributes.cfile_block_size = pb.cfile_block_size();
  }
  return ColumnSchema(pb.name(), pb.type(), pb.is_nullable(),
                      read_default_ptr, write_default_ptr,
                      attributes);
}

void ColumnSchemaDeltaToPB(const ColumnSchemaDelta& col_delta, ColumnSchemaDeltaPB *pb) {
  pb->Clear();
  pb->set_name(col_delta.name);
  if (col_delta.new_name) {
    pb->set_new_name(*col_delta.new_name);
  }
  if (col_delta.default_value) {
    pb->set_default_value(col_delta.default_value->data(),
                          col_delta.default_value->size());
  }
  if (col_delta.remove_default) {
    pb->set_remove_default(true);
  }
  if (col_delta.encoding) {
    pb->set_encoding(*col_delta.encoding);
  }
  if (col_delta.compression) {
    pb->set_compression(*col_delta.compression);
  }
  if (col_delta.cfile_block_size) {
    pb->set_block_size(*col_delta.cfile_block_size);
  }
}

ColumnSchemaDelta ColumnSchemaDeltaFromPB(const ColumnSchemaDeltaPB& pb) {
  ColumnSchemaDelta col_delta(pb.name());
  if (pb.has_new_name()) {
    col_delta.new_name = boost::optional<string>(pb.new_name());
  }
  if (pb.has_default_value()) {
    col_delta.default_value = boost::optional<Slice>(Slice(pb.default_value()));
  }
  if (pb.has_remove_default()) {
    col_delta.remove_default = true;
  }
  if (pb.has_encoding()) {
    col_delta.encoding = boost::optional<EncodingType>(pb.encoding());
  }
  if (pb.has_compression()) {
    col_delta.compression = boost::optional<CompressionType>(pb.compression());
  }
  if (pb.has_block_size()) {
    col_delta.cfile_block_size = boost::optional<int32_t>(pb.block_size());
  }
  return col_delta;
}

Status ColumnPBsToSchema(const RepeatedPtrField<ColumnSchemaPB>& column_pbs,
                         Schema* schema) {

  vector<ColumnSchema> columns;
  vector<ColumnId> column_ids;
  columns.reserve(column_pbs.size());
  int num_key_columns = 0;
  bool is_handling_key = true;
  for (const ColumnSchemaPB& pb : column_pbs) {
    columns.push_back(ColumnSchemaFromPB(pb));
    if (pb.is_key()) {
      if (!is_handling_key) {
        return Status::InvalidArgument(
          "Got out-of-order key column", SecureShortDebugString(pb));
      }
      num_key_columns++;
    } else {
      is_handling_key = false;
    }
    if (pb.has_id()) {
      column_ids.emplace_back(pb.id());
    }
  }

  DCHECK_LE(num_key_columns, columns.size());

  // TODO(perf): could make the following faster by adding a
  // Reset() variant which actually takes ownership of the column
  // vector.
  return schema->Reset(columns, column_ids, num_key_columns);
}

Status SchemaToColumnPBs(const Schema& schema,
                         RepeatedPtrField<ColumnSchemaPB>* cols,
                         int flags) {
  cols->Clear();
  int idx = 0;
  for (const ColumnSchema& col : schema.columns()) {
    ColumnSchemaPB* col_pb = cols->Add();
    ColumnSchemaToPB(col, col_pb, flags);
    col_pb->set_is_key(idx < schema.num_key_columns());

    if (schema.has_column_ids() && !(flags & SCHEMA_PB_WITHOUT_IDS)) {
      col_pb->set_id(schema.column_id(idx));
    }

    idx++;
  }
  return Status::OK();
}

namespace {
// Copies a predicate lower or upper bound from 'bound_src' into 'bound_dst'.
void CopyPredicateBoundToPB(const ColumnSchema& col, const void* bound_src, string* bound_dst) {
  const void* src;
  size_t size;
  if (col.type_info()->physical_type() == BINARY) {
    // Copying a string involves an extra level of indirection through its
    // owning slice.
    const Slice* s = reinterpret_cast<const Slice*>(bound_src);
    src = s->data();
    size = s->size();
  } else {
    src = bound_src;
    size = col.type_info()->size();
  }
  bound_dst->assign(reinterpret_cast<const char*>(src), size);
}

// Extract a void* pointer suitable for use in a ColumnRangePredicate from the
// string protobuf bound. This validates that the pb_value has the correct
// length, copies the data into 'arena', and sets *result to point to it.
// Returns bad status if the user-specified value is the wrong length.
Status CopyPredicateBoundFromPB(const ColumnSchema& schema,
                                const string& pb_value,
                                Arena* arena,
                                const void** result) {
  // Copy the data from the protobuf into the Arena.
  uint8_t* data_copy = static_cast<uint8_t*>(arena->AllocateBytes(pb_value.size()));
  memcpy(data_copy, &pb_value[0], pb_value.size());

  // If the type is of variable length, then we need to return a pointer to a Slice
  // element pointing to the string. Otherwise, just verify that the provided
  // value was the right size.
  if (schema.type_info()->physical_type() == BINARY) {
    *result = arena->NewObject<Slice>(data_copy, pb_value.size());
  } else {
    // TODO: add test case for this invalid request
    size_t expected_size = schema.type_info()->size();
    if (pb_value.size() != expected_size) {
      return Status::InvalidArgument(
          strings::Substitute("Bad predicate on $0. Expected value size $1, got $2",
                              schema.ToString(), expected_size, pb_value.size()));
    }
    *result = data_copy;
  }

  return Status::OK();
}
} // anonymous namespace

void ColumnPredicateToPB(const ColumnPredicate& predicate,
                         ColumnPredicatePB* pb) {
  pb->set_column(predicate.column().name());
  switch (predicate.predicate_type()) {
    case PredicateType::Equality: {
      CopyPredicateBoundToPB(predicate.column(),
                             predicate.raw_lower(),
                             pb->mutable_equality()->mutable_value());
      return;
    };
    case PredicateType::Range: {
      auto range_pred = pb->mutable_range();
      if (predicate.raw_lower() != nullptr) {
        CopyPredicateBoundToPB(predicate.column(),
                               predicate.raw_lower(),
                               range_pred->mutable_lower());
      }
      if (predicate.raw_upper() != nullptr) {
        CopyPredicateBoundToPB(predicate.column(),
                               predicate.raw_upper(),
                               range_pred->mutable_upper());
      }
      return;
    };
    case PredicateType::IsNotNull: {
      pb->mutable_is_not_null();
      return;
    };
    case PredicateType::IsNull: {
      pb->mutable_is_null();
      return;
    }
    case PredicateType::InList: {
      auto* values = pb->mutable_in_list()->mutable_values();
      for (const void* value : predicate.raw_values()) {
        CopyPredicateBoundToPB(predicate.column(), value, values->Add());
      }
      return;
    };
    case PredicateType::None: LOG(FATAL) << "None predicate may not be converted to protobuf";
  }
  LOG(FATAL) << "unknown predicate type";
}

Status ColumnPredicateFromPB(const Schema& schema,
                             Arena* arena,
                             const ColumnPredicatePB& pb,
                             boost::optional<ColumnPredicate>* predicate) {
  if (!pb.has_column()) {
    return Status::InvalidArgument("Column predicate must include a column", SecureDebugString(pb));
  }
  const string& column = pb.column();
  int32_t idx = schema.find_column(column);
  if (idx == Schema::kColumnNotFound) {
    return Status::InvalidArgument("unknown column in predicate", SecureDebugString(pb));
  }
  const ColumnSchema& col = schema.column(idx);

  switch (pb.predicate_case()) {
    case ColumnPredicatePB::kRange: {
      const auto& range = pb.range();
      if (!range.has_lower() && !range.has_upper()) {
        return Status::InvalidArgument("Invalid range predicate on column: no bounds",
                                        col.name());
      }

      const void* lower = nullptr;
      const void* upper = nullptr;
      if (range.has_lower()) {
        RETURN_NOT_OK(CopyPredicateBoundFromPB(col, range.lower(), arena, &lower));
      }
      if (range.has_upper()) {
        RETURN_NOT_OK(CopyPredicateBoundFromPB(col, range.upper(), arena, &upper));
      }

      *predicate = ColumnPredicate::Range(col, lower, upper);
      break;
    };
    case ColumnPredicatePB::kEquality: {
      const auto& equality = pb.equality();
      if (!equality.has_value()) {
        return Status::InvalidArgument("Invalid equality predicate on column: no value",
                                        col.name());
      }
      const void* value = nullptr;
      RETURN_NOT_OK(CopyPredicateBoundFromPB(col, equality.value(), arena, &value));
      *predicate = ColumnPredicate::Equality(col, value);
      break;
    };
    case ColumnPredicatePB::kInList: {
      const auto& inlist = pb.in_list();
      vector<const void*> values;
      for (const string& pb_value : inlist.values()) {
        const void* value = nullptr;
        RETURN_NOT_OK(CopyPredicateBoundFromPB(col, pb_value, arena, &value));
        values.push_back(value);
      }
      *predicate = ColumnPredicate::InList(col, &values);
      break;
    };
    case ColumnPredicatePB::kIsNotNull: {
      *predicate = ColumnPredicate::IsNotNull(col);
      break;
    };
    case ColumnPredicatePB::kIsNull: {
        *predicate = ColumnPredicate::IsNull(col);
        break;
      }
    default: return Status::InvalidArgument("Unknown predicate type for column", col.name());
  }
  return Status::OK();
}

// Because we use a faststring here, ASAN tests become unbearably slow
// with the extra verifications.
ATTRIBUTE_NO_ADDRESS_SAFETY_ANALYSIS
Status RewriteRowBlockPointers(const Schema& schema, const RowwiseRowBlockPB& rowblock_pb,
                               const Slice& indirect_data_slice, Slice* row_data_slice,
                               bool pad_unixtime_micros_to_16_bytes) {
  // TODO(todd): cheating here so we can rewrite the request as it arrived and
  // change any indirect data pointers back to "real" pointers instead of
  // on-the-wire pointers. Maybe the RPC layer should give us a non-const
  // request? Maybe we should suck it up and copy the data when we mutate?

  size_t total_padding = 0;
  int num_binary_cols = 0;
  for (int i = 0; i < schema.num_columns(); i++) {
    // If we're padding UNIXTIME_MICROS for Impala we need to calculate the total padding
    // size to adjust the row_stride.
    if (pad_unixtime_micros_to_16_bytes &&
        schema.column(i).type_info()->type() == UNIXTIME_MICROS) {
      total_padding += 8;
    }
    if (schema.column(i).type_info()->physical_type() == BINARY) {
      num_binary_cols++;
    }
  }

  const size_t row_stride = ContiguousRowHelper::row_size(schema) + total_padding;

  // We don't need a const-cast because we can just use Slice's lack of
  // const-safety.
  uint8_t* row_data = row_data_slice->mutable_data();
  const uint8_t* indir_data = indirect_data_slice.data();
  const size_t expected_data_size = rowblock_pb.num_rows() * row_stride;
  const size_t null_bitmap_offset = schema.byte_size() + total_padding;

  if (PREDICT_FALSE(row_data_slice->size() != expected_data_size)) {
    return Status::Corruption(
      Substitute("Row block has $0 bytes of data but expected $1 for $2 rows",
                   row_data_slice->size(), expected_data_size, rowblock_pb.num_rows()));
  }

  if (num_binary_cols == 0) return Status::OK();

  // Calculate the offset information for the columns which need rewriting.
  // Calculating this up front means we can avoid re-calculating this redundant
  // information once per row.
  struct ToRewrite {
    int col_idx;
    int col_offset;
    bool nullable;
  };
  FixedArray<ToRewrite> to_rewrite(num_binary_cols);

  int padding_so_far = 0;
  int j = 0;
  for (int i = 0; i < schema.num_columns(); i++) {
    const ColumnSchema& col = schema.column(i);
    if (pad_unixtime_micros_to_16_bytes &&
        col.type_info()->type() == UNIXTIME_MICROS) {
      padding_so_far += 8;
    }
    if (col.type_info()->physical_type() == BINARY) {
      int column_offset = schema.column_offset(i) + padding_so_far;
      to_rewrite[j++] = { i, column_offset, col.is_nullable() };
    }
  }
  DCHECK_EQ(j, num_binary_cols);

  // Iterate through the rows and rewrite columns as necessary.
  // NOTE: we do this row-by-row instead of column-by-column because
  // the input data is typically much larger than L1 cache, and thus
  // doing one pass over the memory is faster.
  uint8_t* row_ptr = row_data;
  for (int row_idx = 0;
       row_idx < rowblock_pb.num_rows();
       row_idx++) {
    for (const auto& t : to_rewrite) {
      uint8_t* cell_ptr = row_ptr + t.col_offset;

      if (t.nullable && BitmapTest(row_ptr + null_bitmap_offset, t.col_idx)) {
        // No need to rewrite null values.
        continue;
      }

      // The pointer is currently an offset into indir_data. Need to replace it
      // with the actual pointer into indir_data
      Slice *slice = reinterpret_cast<Slice *>(cell_ptr);
      size_t offset_in_indirect = reinterpret_cast<uintptr_t>(slice->data());

      // Ensure the updated pointer is within the bounds of the indirect data.
      bool overflowed = false;
      size_t max_offset = AddWithOverflowCheck(offset_in_indirect, slice->size(), &overflowed);
      if (PREDICT_FALSE(overflowed || max_offset > indirect_data_slice.size())) {
        const auto& col = schema.column(t.col_idx);
        return Status::Corruption(
            Substitute("Row #$0 contained bad indirect slice for column $1: ($2, $3)",
                       row_idx, col.ToString(), offset_in_indirect, slice->size()));
      }
      *slice = Slice(&indir_data[offset_in_indirect], slice->size());
    }
    row_ptr += row_stride;
  }

  return Status::OK();
}

Status ExtractRowsFromRowBlockPB(const Schema& schema,
                                 const RowwiseRowBlockPB& rowblock_pb,
                                 const Slice& indirect_data,
                                 Slice* rows_data,
                                 vector<const uint8_t*>* rows) {
  RETURN_NOT_OK(RewriteRowBlockPointers(schema, rowblock_pb, indirect_data, rows_data));

  int n_rows = rowblock_pb.num_rows();
  if (PREDICT_FALSE(n_rows == 0)) {
    // Early-out here to avoid a UBSAN failure.
    return Status::OK();
  }

  // Doing this resize and array indexing turns out to be noticeably faster
  // than using reserve and push_back.
  size_t row_size = ContiguousRowHelper::row_size(schema);
  const uint8_t* src = rows_data->data();
  int dst_index = rows->size();
  rows->resize(rows->size() + n_rows);
  const uint8_t** dst = &(*rows)[dst_index];
  while (n_rows > 0) {
    *dst++ = src;
    src += row_size;
    n_rows--;
  }

  return Status::OK();
}

Status FindLeaderHostPort(const RepeatedPtrField<ServerEntryPB>& entries,
                          HostPort* leader_hostport) {
  for (const ServerEntryPB& entry : entries) {
    if (entry.has_error()) {
      LOG(WARNING) << "Error encountered for server entry " << SecureShortDebugString(entry)
                   << ": " << StatusFromPB(entry.error()).ToString();
      continue;
    }
    if (!entry.has_role()) {
      return Status::IllegalState(
          strings::Substitute("Every server in must have a role, but entry ($0) has no role.",
                              SecureShortDebugString(entry)));
    }
    if (entry.role() == consensus::RaftPeerPB::LEADER) {
      return HostPortFromPB(entry.registration().rpc_addresses(0), leader_hostport);
    }
  }
  return Status::NotFound("No leader found.");
}

template<class RowType>
void AppendRowToString(const RowType& row, string* buf);

template<>
void AppendRowToString<ConstContiguousRow>(const ConstContiguousRow& row, string* buf) {
  buf->append(reinterpret_cast<const char*>(row.row_data()), row.row_size());
}

template<>
void AppendRowToString<RowBlockRow>(const RowBlockRow& row, string* buf) {
  size_t row_size = ContiguousRowHelper::row_size(*row.schema());
  size_t appended_offset = buf->size();
  buf->resize(buf->size() + row_size);
  uint8_t* copied_rowdata = reinterpret_cast<uint8_t*>(&(*buf)[appended_offset]);
  ContiguousRow copied_row(row.schema(), copied_rowdata);
  CHECK_OK(CopyRow(row, &copied_row, reinterpret_cast<Arena*>(NULL)));
}

// Copy a column worth of data from the given RowBlock into the output
// protobuf.
//
// IS_NULLABLE: true if the column is nullable
// IS_VARLEN: true if the column is of variable length
//
// These are template parameters rather than normal function arguments
// so that there are fewer branches inside the loop.
//
// NOTE: 'dst_schema' must either be NULL or a subset of the specified's
// RowBlock's schema. If not NULL, then column at 'col_idx' in 'block' will
// be copied to column 'dst_col_idx' in the output protobuf; otherwise,
// dst_col_idx must be equal to col_idx.
template<bool IS_NULLABLE, bool IS_VARLEN>
static void CopyColumn(const RowBlock& block, int col_idx, int dst_col_idx, uint8_t* dst_base,
                       faststring* indirect_data, const Schema* dst_schema, size_t row_stride,
                       size_t schema_byte_size, size_t column_offset) {
  DCHECK(dst_schema);
  ColumnBlock column_block = block.column_block(col_idx);
  uint8_t* dst = dst_base + column_offset;
  size_t offset_to_null_bitmap = schema_byte_size - column_offset;

  size_t cell_size = column_block.stride();
  const uint8_t* src = column_block.cell_ptr(0);

  BitmapIterator selected_row_iter(block.selection_vector()->bitmap(), block.nrows());
  int run_size;
  bool selected;
  int row_idx = 0;
  while ((run_size = selected_row_iter.Next(&selected))) {
    if (!selected) {
      src += run_size * cell_size;
      row_idx += run_size;
      continue;
    }
    for (int i = 0; i < run_size; i++) {
      if (IS_NULLABLE && column_block.is_null(row_idx)) {
        BitmapChange(dst + offset_to_null_bitmap, dst_col_idx, true);
      } else if (IS_VARLEN) {
        const Slice *slice = reinterpret_cast<const Slice *>(src);
        size_t offset_in_indirect = indirect_data->size();
        indirect_data->append(reinterpret_cast<const char*>(slice->data()), slice->size());

        Slice *dst_slice = reinterpret_cast<Slice *>(dst);
        *dst_slice = Slice(reinterpret_cast<const uint8_t*>(offset_in_indirect),
                           slice->size());
        if (IS_NULLABLE) {
          BitmapChange(dst + offset_to_null_bitmap, dst_col_idx, false);
        }
      } else { // non-string, non-null
        strings::memcpy_inlined(dst, src, cell_size);
        if (IS_NULLABLE) {
          BitmapChange(dst + offset_to_null_bitmap, dst_col_idx, false);
        }
      }
      dst += row_stride;
      src += cell_size;
      row_idx++;
    }
  }
}

// Because we use a faststring here, ASAN tests become unbearably slow
// with the extra verifications.
ATTRIBUTE_NO_ADDRESS_SAFETY_ANALYSIS
void SerializeRowBlock(const RowBlock& block,
                       RowwiseRowBlockPB* rowblock_pb,
                       const Schema* projection_schema,
                       faststring* data_buf,
                       faststring* indirect_data,
                       bool pad_unixtime_micros_to_16_bytes) {
  DCHECK_GT(block.nrows(), 0);
  const Schema& tablet_schema = block.schema();

  if (projection_schema == nullptr) {
    projection_schema = &tablet_schema;
  }

  // Check whether we need to pad or if there are nullable columns, this will dictate whether
  // we need to set memory to zero.
  size_t total_padding = 0;
  bool has_nullable_cols = false;
  for (int i = 0; i < projection_schema->num_columns(); i++) {
    if (projection_schema->column(i).is_nullable()) {
      has_nullable_cols = true;
    }
    // If we're padding UNIXTIME_MICROS for Impala we need to calculate the total padding
    // size to adjust the row_stride.
    if (pad_unixtime_micros_to_16_bytes &&
        projection_schema->column(i).type_info()->type() == UNIXTIME_MICROS) {
      total_padding += 8;
    }
  }

  size_t old_size = data_buf->size();
  size_t row_stride = ContiguousRowHelper::row_size(*projection_schema) + total_padding;
  size_t num_rows = block.selection_vector()->CountSelected();
  size_t schema_byte_size = projection_schema->byte_size() + total_padding;
  size_t additional_size = row_stride * num_rows;

  data_buf->resize(old_size + additional_size);
  uint8_t* base = &(*data_buf)[old_size];

  // Zero out the memory if we have nullable columns or if we're padding slots so that we don't leak
  // unrelated data to the client.
  if (total_padding != 0 || has_nullable_cols) {
    memset(base, 0, additional_size);
  }

  size_t t_schema_idx = 0;
  size_t padding_so_far = 0;
  for (int p_schema_idx = 0; p_schema_idx < projection_schema->num_columns(); p_schema_idx++) {
    const ColumnSchema& col = projection_schema->column(p_schema_idx);
    t_schema_idx = tablet_schema.find_column(col.name());
    DCHECK_NE(t_schema_idx, -1);

    size_t column_offset = projection_schema->column_offset(p_schema_idx) + padding_so_far;

    // Generating different functions for each of these cases makes them much less
    // branch-heavy -- we do the branch once outside the loop, and then have a
    // compiled version for each combination below.
    // TODO: Using LLVM to build a specialized CopyColumn on the fly should have
    // even bigger gains, since we could inline the constant cell sizes and column
    // offsets.
    if (col.is_nullable() && col.type_info()->physical_type() == BINARY) {
      CopyColumn<true, true>(block, t_schema_idx, p_schema_idx, base, indirect_data,
                             projection_schema, row_stride, schema_byte_size, column_offset);
    } else if (col.is_nullable() && col.type_info()->physical_type() != BINARY) {
      CopyColumn<true, false>(block, t_schema_idx, p_schema_idx, base, indirect_data,
                              projection_schema, row_stride, schema_byte_size, column_offset);
    } else if (!col.is_nullable() && col.type_info()->physical_type() == BINARY) {
      CopyColumn<false, true>(block, t_schema_idx, p_schema_idx, base, indirect_data,
                              projection_schema, row_stride, schema_byte_size, column_offset);
    } else if (!col.is_nullable() && col.type_info()->physical_type() != BINARY) {
      CopyColumn<false, false>(block, t_schema_idx, p_schema_idx, base, indirect_data,
                               projection_schema, row_stride, schema_byte_size, column_offset);
    } else {
      LOG(FATAL) << "cannot reach here";
    }

    if (col.type_info()->type() == UNIXTIME_MICROS && pad_unixtime_micros_to_16_bytes) {
      padding_so_far += 8;
    }
  }
  rowblock_pb->set_num_rows(rowblock_pb->num_rows() + num_rows);
}

} // namespace kudu
