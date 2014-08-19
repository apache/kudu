// Copyright 2014 Cloudera inc.

#ifndef KUDU_CODEGEN_ROW_PROJECTOR_H
#define KUDU_CODEGEN_ROW_PROJECTOR_H

#include <iosfwd>
#include <vector>

#include "kudu/common/row.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace llvm {
class ExecutionEngine;
class Function;
} // namespace llvm

namespace kudu {

class Arena;
class Schema;

namespace codegen {

class ModuleBuilder;

// This projector behaves the almost the same way as a tablet/RowProjector except that
// it only supports certain row types, and expects a regular Arena. Furthermore,
// the Reset() public method is unsupported.
//
// See documentation for RowProjector. Any differences in the API will be explained
// in this class.
//
// A single RowProjector is tied to a ModuleBuilder at creation time,
// so functions will always be compiled in the context of the thread that called
// this object's constructor.
//
// Currently, the RowProjector guarantees its code is valid for the duration
// of its lifetime, so long as it's compiled before it is used.
// TODO: remove the above paragraph when TakeEngine() method is removed.
class RowProjector {
 public:
  typedef kudu::RowProjector::ProjectionIdxMapping ProjectionIdxMapping;
  // Requires that both schemas remain valid for the lifetime of this
  // object.
  //
  // Factory uses the provided builder to build the functions. It
  // does not generate any code, make any optimizations, or finalize
  // the current object. The builder's Compile() method should be called
  // before any of its functions are used.
  //
  // Upon success, writes the output to the parameter pointer.
  static Status Create(const Schema* base_schema, const Schema* projection,
                       ModuleBuilder* builder, gscoped_ptr<RowProjector>* out);
  ~RowProjector();

  // Nop method for interface compatibility with kudu::RowProjector.
  Status Init() { return Status::OK(); }

  // Takes ownership of execution engine. This is only a temporary
  // method employed to maintain the engine's lifetime exactly
  // as long as it needs to be maintained (that is, the lifetime of
  // this class, the only code that uses it as of now).
  // TODO: RowProjector should not be aware of the execution engine.
  void TakeEngine(gscoped_ptr<llvm::ExecutionEngine> engine);

  template<class ContiguousRowType>
  Status ProjectRowForRead(const ContiguousRowType& src_row,
                           RowBlockRow* dst_row,
                           Arena* dst_arena) const {
    DCHECK_SCHEMA_EQ(*base_schema(), *src_row.schema());
    DCHECK_SCHEMA_EQ(*projection(), *dst_row->schema());
    DCHECK(read_f_ != NULL)
      << "Promise to compile read function not fullfilled by ModuleBuilder";
    if (PREDICT_TRUE(read_f_(src_row.row_data(), dst_row, dst_arena))) {
      return Status::OK();
    }
    return Status::IOError("out of memory copying slice during projection. "
                           "Base schema row: ", base_schema()->DebugRow(src_row));
  }

  // Warning: the projection schema should have write-defaults defined
  // if it has default columns. There was no check for default write
  // columns during this class' initialization.
  template<class ContiguousRowType>
  Status ProjectRowForWrite(const ContiguousRowType& src_row,
                            RowBlockRow* dst_row,
                            Arena* dst_arena) const {
    DCHECK_SCHEMA_EQ(*base_schema(), *src_row.schema());
    DCHECK_SCHEMA_EQ(*projection(), *dst_row->schema());
    DCHECK(write_f_ != NULL)
      << "Promise to compile write function not fullfilled by ModuleBuilder";
    if (PREDICT_TRUE(write_f_(src_row.row_data(), dst_row, dst_arena))) {
      return Status::OK();
    }
    return Status::IOError("out of memory copying slice during projection. "
                           "Base schema row: ", base_schema()->DebugRow(src_row));
  }

  const vector<ProjectionIdxMapping>& base_cols_mapping() const {
    return projector_.base_cols_mapping();
  }
  const vector<ProjectionIdxMapping>& adapter_cols_mapping() const {
    return projector_.adapter_cols_mapping();
  }
  const vector<size_t>& projection_defaults() const {
    return projector_.projection_defaults();
  }
  bool is_identity() const { return projector_.is_identity(); }
  const Schema* projection() const { return projector_.projection(); }
  const Schema* base_schema() const { return projector_.base_schema(); }

 private:
  RowProjector(const Schema* base_schema, const Schema* projection);

  gscoped_ptr<llvm::ExecutionEngine> engine_;

  // Initially set to null, the function pointers are initialized once
  // the functions are jitted.
  typedef bool(*ProjectionFunction)(const uint8_t*, RowBlockRow*, Arena*);
  ProjectionFunction read_f_;
  ProjectionFunction write_f_;

  kudu::RowProjector projector_;

  DISALLOW_COPY_AND_ASSIGN(RowProjector);
};

extern std::ostream& operator<<(std::ostream& o, const RowProjector& rp);

} // namespace codegen
} // namespace kudu

#endif
