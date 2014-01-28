// Copyright (c) 2013, Cloudera, inc.

#include "common/wire_protocol.h"

#include <string>
#include <vector>

#include "common/row.h"
#include "common/rowblock.h"
#include "common/row_changelist.h"
#include "gutil/stl_util.h"
#include "gutil/strings/fastmem.h"
#include "util/net/net_util.h"
#include "util/safe_math.h"

using google::protobuf::RepeatedPtrField;
using std::vector;

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
  } else if (status.IsAlreadyPresent()) {
    pb->set_code(AppStatusPB::ALREADY_PRESENT);
  } else if (status.IsIOError()) {
    pb->set_code(AppStatusPB::IO_ERROR);
  } else if (status.IsRuntimeError()) {
    pb->set_code(AppStatusPB::RUNTIME_ERROR);
  } else if (status.IsNetworkError()) {
    pb->set_code(AppStatusPB::NETWORK_ERROR);
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
    case AppStatusPB::UNKNOWN_ERROR:
    default:
      LOG(WARNING) << "Unknown error code in status: " << pb.ShortDebugString();
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

Status SchemaToPB(const Schema& schema, SchemaPB *pb) {
  pb->Clear();
  return SchemaToColumnPBs(schema, pb->mutable_columns());
}

Status SchemaFromPB(const SchemaPB& pb, Schema *schema) {
  return ColumnPBsToSchema(pb.columns(), schema);
}

void ColumnSchemaToPB(const ColumnSchema& col_schema, ColumnSchemaPB *pb) {
  pb->Clear();
  pb->set_name(col_schema.name());
  pb->set_type(col_schema.type_info().type());
  pb->set_is_nullable(col_schema.is_nullable());
  pb->set_encoding(col_schema.attributes().encoding());
  pb->set_compression(col_schema.attributes().compression());
  if (col_schema.has_read_default()) {
    if (col_schema.type_info().type() == STRING) {
      const Slice *read_slice = static_cast<const Slice *>(col_schema.read_default_value());
      pb->set_read_default_value(read_slice->data(), read_slice->size());
    } else {
      const void *read_value = col_schema.read_default_value();
      pb->set_read_default_value(read_value, col_schema.type_info().size());
    }
  }
  if (col_schema.has_write_default()) {
    if (col_schema.type_info().type() == STRING) {
      const Slice *write_slice = static_cast<const Slice *>(col_schema.write_default_value());
      pb->set_write_default_value(write_slice->data(), write_slice->size());
    } else {
      const void *write_value = col_schema.write_default_value();
      pb->set_write_default_value(write_value, col_schema.type_info().size());
    }
  }
}

ColumnSchema ColumnSchemaFromPB(const ColumnSchemaPB& pb) {
  const void *write_default_ptr = NULL;
  const void *read_default_ptr = NULL;
  Slice write_default;
  Slice read_default;
  if (pb.has_read_default_value()) {
    read_default = Slice(pb.read_default_value());
    if (pb.type() == STRING) {
      read_default_ptr = &read_default;
    } else {
      read_default_ptr = read_default.data();
    }
  }
  if (pb.has_write_default_value()) {
    write_default = Slice(pb.write_default_value());
    if (pb.type() == STRING) {
      write_default_ptr = &write_default;
    } else {
      write_default_ptr = write_default.data();
    }
  }
  // TODO: decide if we want to store this elsewhere, or take another
  // approach. We could also avoid storing compression and encoding
  // info if they are (respectively) NO_COMPRESSION and AUTO_ENCODING.
  ColumnStorageAttributes attributes(pb.encoding(), pb.compression());
  return ColumnSchema(pb.name(), pb.type(), pb.is_nullable(),
                      read_default_ptr, write_default_ptr,
                      attributes);
}

Status ColumnPBsToSchema(const RepeatedPtrField<ColumnSchemaPB>& column_pbs,
                         Schema* schema) {

  vector<ColumnSchema> columns;
  vector<size_t> column_ids;
  columns.reserve(column_pbs.size());
  int num_key_columns = 0;
  bool is_handling_key = true;
  BOOST_FOREACH(const ColumnSchemaPB& pb, column_pbs) {
    columns.push_back(ColumnSchemaFromPB(pb));
    if (pb.is_key()) {
      if (!is_handling_key) {
        return Status::InvalidArgument(
          "Got out-of-order key column", pb.ShortDebugString());
      }
      num_key_columns++;
    } else {
      is_handling_key = false;
    }
    if (pb.has_id()) {
      column_ids.push_back(pb.id());
    }
  }

  DCHECK_LE(num_key_columns, columns.size());

  // TODO(perf): could make the following faster by adding a
  // Reset() variant which actually takes ownership of the column
  // vector.
  return schema->Reset(columns, column_ids, num_key_columns);
}

Status SchemaToColumnPBsWithoutIds(const Schema& schema,
                                   RepeatedPtrField<ColumnSchemaPB>* cols) {
  cols->Clear();
  int idx = 0;
  BOOST_FOREACH(const ColumnSchema& col, schema.columns()) {
    ColumnSchemaPB* col_pb = cols->Add();
    ColumnSchemaToPB(col, col_pb);
    col_pb->set_is_key(idx < schema.num_key_columns());
    idx++;
  }
  return Status::OK();
}

Status SchemaToColumnPBs(const Schema& schema,
                         RepeatedPtrField<ColumnSchemaPB>* cols) {
  RETURN_NOT_OK(SchemaToColumnPBsWithoutIds(schema, cols));
  if (schema.has_column_ids()) {
    for (int i = 0; i < schema.num_columns(); ++i) {
      ColumnSchemaPB* col_pb = cols->Mutable(i);
      col_pb->set_id(schema.column_id(i));
    }
  }
  return Status::OK();
}


Status ExtractRowsFromRowBlockPB(const Schema& schema,
                                 RowwiseRowBlockPB *rowblock_pb,
                                 vector<const uint8_t*>* rows) {
  // TODO: cheating here so we can rewrite the request as it arrived and
  // change any indirect data pointers back to "real" pointers instead of
  // on-the-wire pointers. Maybe the RPC layer should give us a non-const
  // request? Maybe we should suck it up and copy the data when we mutate?
  string* row_data = rowblock_pb->mutable_rows();
  const string& indir_data = rowblock_pb->indirect_data();
  size_t row_size = ContiguousRowHelper::row_size(schema);
  size_t expected_data_size = rowblock_pb->num_rows() * row_size;

  if (PREDICT_FALSE(row_data->size() != expected_data_size)) {
    return Status::Corruption(
      StringPrintf("Row block has %zd bytes of data but expected %zd for %"PRIu32" rows",
                   row_data->size(), expected_data_size, rowblock_pb->num_rows()));
  }

  for (int i = 0; i < schema.num_columns(); i++) {
    const ColumnSchema& col = schema.column(i);
    if (col.type_info().type() != STRING) {
      continue;
    }

    int row_idx = 0;
    size_t offset = 0;
    while (offset < row_data->size()) {
      ContiguousRow row(schema, reinterpret_cast<uint8_t*>(&(*row_data)[offset]));
      uint8_t* dst_cell = row.mutable_cell_ptr(i);

      if (!col.is_nullable() || !row.is_null(i)) {
        // The pointer is currently an offset into indir_data. Need to replace it
        // with the actual pointer into indir_data
        Slice *slice = reinterpret_cast<Slice *>(dst_cell);
        size_t offset_in_indirect = reinterpret_cast<uintptr_t>(slice->data());
        bool overflowed = false;
        size_t max_offset = AddWithOverflowCheck(offset_in_indirect, slice->size(), &overflowed);
        if (PREDICT_FALSE(overflowed || max_offset > indir_data.size())) {
          return Status::Corruption(
            StringPrintf("Row #%d contained bad indirect slice for column %s: (%zd, %zd)",
                         row_idx, col.ToString().c_str(),
                         reinterpret_cast<uintptr_t>(slice->data()),
                         slice->size()));
        }
        *slice = Slice(&indir_data[offset_in_indirect], slice->size());
      }

      // Advance to next row
      offset += row_size;
      row_idx++;
    }
  }

  int n_rows = rowblock_pb->num_rows();
  if (PREDICT_FALSE(n_rows == 0)) {
    // Early-out here to avoid a UBSAN failure.
    return Status::OK();
  }

  // Doing this resize and array indexing turns out to be noticeably faster
  // than using reserve and push_back.
  const uint8_t* src = reinterpret_cast<const uint8_t*>(&(*row_data)[0]);
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

Status ExtractMutationsFromBuffer(uint32_t n_mutations,
                                const uint8_t* buffer,
                                uint32_t buffer_size,
                                vector<const RowChangeList *> *mutations) {
  Slice remaining(buffer, buffer_size);
  for (int i = 0; i < n_mutations; i++) {
    if (PREDICT_FALSE(remaining.size() < sizeof(uint32_t))) {
      return Status::Corruption("Missing expected length prefix in mutation buffer");
    }
    uint32_t mutation_size = DecodeFixed32(remaining.data());
    remaining.remove_prefix(sizeof(uint32_t));
    if (PREDICT_FALSE(mutation_size > remaining.size())) {
      return Status::Corruption("Mutation size overflows the mutations buffer");
    }
    Slice mutation_slice(remaining.data(), mutation_size);
    mutations->push_back(new RowChangeList(mutation_slice));
    remaining.remove_prefix(mutation_size);
  }
  return Status::OK();
}

template<class RowType>
void AppendRowToString(const RowType& row, string* buf);

template<>
void AppendRowToString<ConstContiguousRow>(const ConstContiguousRow& row, string* buf) {
  buf->append(reinterpret_cast<const char*>(row.row_data()), row.row_size());
}

template<>
void AppendRowToString<RowBlockRow>(const RowBlockRow& row, string* buf) {
  size_t row_size = ContiguousRowHelper::row_size(row.schema());
  size_t appended_offset = buf->size();
  buf->resize(buf->size() + row_size);
  uint8_t* copied_rowdata = reinterpret_cast<uint8_t*>(&(*buf)[appended_offset]);
  ContiguousRow copied_row(row.schema(), copied_rowdata);
  CHECK_OK(CopyRow(row, &copied_row, reinterpret_cast<Arena*>(NULL)));
}


template<class RowType>
void DoAddRowToRowBlockPB(const RowType& row, RowwiseRowBlockPB* pb) {
  const Schema& schema = row.schema();
  // Append the row directly to the data.
  // This will append a host-local pointer for any slice data, so we need
  // to then relocate those pointers into the 'indirect_data' part of the protobuf.
  string* data_buf = pb->mutable_rows();
  size_t appended_offset = data_buf->size();
  AppendRowToString(row, data_buf);

  uint8_t* copied_rowdata = reinterpret_cast<uint8_t*>(&(*data_buf)[appended_offset]);
  ContiguousRow copied_row(schema, copied_rowdata);
  for (int i = 0; i < schema.num_columns(); i++) {
    const ColumnSchema& col = schema.column(i);
    uint8_t* dst_cell = copied_row.mutable_cell_ptr(i);
    if (col.is_nullable() && row.is_null(i)) {
      // Zero the data so we don't leak any uninitialized memory to another
      // host/security domain.
      memset(dst_cell, 0, col.type_info().size());
      continue;
    }

    if (col.type_info().type() == STRING) {
      // Copy the slice data into the 'indirect_data' field, and replace
      // the pointer with an offset into that field.
      Slice *slice = reinterpret_cast<Slice *>(dst_cell);
      size_t offset_in_indirect = pb->mutable_indirect_data()->size();
      pb->mutable_indirect_data()->append(reinterpret_cast<const char*>(slice->data()),
                                          slice->size());
      *slice = Slice(reinterpret_cast<const uint8_t*>(offset_in_indirect),
                     slice->size());
    }
  }
  pb->set_num_rows(pb->num_rows() + 1);
}

void AddRowToRowBlockPB(const ConstContiguousRow& row, RowwiseRowBlockPB* pb) {
  DoAddRowToRowBlockPB(row, pb);
}
void AddRowToRowBlockPB(const RowBlockRow& row, RowwiseRowBlockPB* pb) {
  DoAddRowToRowBlockPB(row, pb);
}

// Copy a column worth of data from the given RowBlock into the output
// protobuf.
//
// IS_NULLABLE: true if the column is nullable
// IS_STRING: true if the column is STRING typed
//
// These are template parameters rather than normal function arguments
// so that there are fewer branches inside the loop.
template<bool IS_NULLABLE, bool IS_STRING>
static void CopyColumn(const RowBlock& block, int col_idx, uint8_t* dst_base,
                       string* indirect_data) {
  const Schema& schema = block.schema();
  ColumnBlock cblock = block.column_block(col_idx);
  size_t row_stride = ContiguousRowHelper::row_size(schema);
  uint8_t* dst = dst_base + schema.column_offset(col_idx);
  size_t offset_to_null_bitmap = schema.byte_size() - schema.column_offset(col_idx);

  size_t cell_size = cblock.stride();
  const uint8_t* src = cblock.cell_ptr(0);

  BitmapIterator selected_row_iter(block.selection_vector()->bitmap(),
                                   block.nrows());
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
      if (IS_NULLABLE && cblock.is_null(row_idx)) {
        memset(dst, 0, cell_size);
        BitmapChange(dst + offset_to_null_bitmap, col_idx, true);
      } else if (IS_STRING) {
        const Slice *slice = reinterpret_cast<const Slice *>(src);
        size_t offset_in_indirect = indirect_data->size();
        indirect_data->append(reinterpret_cast<const char*>(slice->data()),
                              slice->size());

        Slice *dst_slice = reinterpret_cast<Slice *>(dst);
        *dst_slice = Slice(reinterpret_cast<const uint8_t*>(offset_in_indirect),
                           slice->size());
      } else { // non-string, non-null
        strings::memcpy_inlined(dst, src, cell_size);
      }
      dst += row_stride;
      src += cell_size;
      row_idx++;
    }
  }
}

void ConvertRowBlockToPB(const RowBlock& block, RowwiseRowBlockPB* pb) {
  const Schema& schema = block.schema();
  string* data_buf = pb->mutable_rows();
  size_t old_size = data_buf->size();
  size_t row_stride = ContiguousRowHelper::row_size(schema);
  int num_rows = block.selection_vector()->CountSelected();
  data_buf->resize(old_size + row_stride * num_rows);
  uint8_t* base = reinterpret_cast<uint8_t*>(&(*data_buf)[old_size]);

  for (int i = 0; i < schema.num_columns(); i++) {
    const ColumnSchema& col = schema.column(i);

    // Generating different functions for each of these cases makes them much less
    // branch-heavy -- we do the branch once outside the loop, and then have a
    // compiled version for each combination below.
    // TODO: Using LLVM to build a specialized CopyColumn on the fly should have
    // even bigger gains, since we could inline the constant cell sizes and column
    // offsets.
    if (col.is_nullable() && col.type_info().type() == STRING) {
      CopyColumn<true, true>(block, i, base, pb->mutable_indirect_data());
    } else if (col.is_nullable() && col.type_info().type() != STRING) {
      CopyColumn<true, false>(block, i, base, pb->mutable_indirect_data());
    } else if (!col.is_nullable() && col.type_info().type() == STRING) {
      CopyColumn<false, true>(block, i, base, pb->mutable_indirect_data());
    } else if (!col.is_nullable() && col.type_info().type() != STRING) {
      CopyColumn<false, false>(block, i, base, pb->mutable_indirect_data());
    } else {
      LOG(FATAL) << "cannot reach here";
    }
  }
  pb->set_num_rows(pb->num_rows() + num_rows);
}

} // namespace kudu
