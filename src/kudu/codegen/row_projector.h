// Copyright 2014 Cloudera inc.

#ifndef KUDU_CODEGEN_ROW_PROJECTOR_H
#define KUDU_CODEGEN_ROW_PROJECTOR_H

#include <iosfwd>

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
// the Reset() and Init() public methods are unsupported.
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
  // Requires that all parameters remain valid for the lifetime of this
  // object, except the builder, which is only needed for the duration
  // of the constructor.
  //
  // Constructor uses the provided builder to build the functions. It
  // does not generate any code, make any optimizations, or finalize
  // the current object. The builder's Compile() method should be called
  // before any of its functions are used.
  RowProjector(const Schema* base_schema, const Schema* projection,
               ModuleBuilder* builder);
  ~RowProjector();

  template<class ContiguousRowType>
  Status ProjectRowForRead(const ContiguousRowType& src_row,
                           RowBlockRow* dst_row,
                           Arena* dst_arena) const {
    DCHECK_SCHEMA_EQ(*base_schema_, *src_row.schema());
    DCHECK_SCHEMA_EQ(*projection_, *dst_row->schema());
    DCHECK(read_f_ != NULL)
      << "Promise to compile read function not fullfilled by ModuleBuilder";
    read_f_(src_row.row_data(), dst_row, dst_arena);
    return Status::OK(); // TODO use acutal error checking
  }

  // Takes ownership of execution engine. This is only a temporary
  // method employed to maintain the engine's lifetime exactly
  // as long as it needs to be maintained (that is, the lifetime of
  // this class, the only code that uses it as of now).
  // TODO: RowProjector should not be aware of the execution engine.
  void TakeEngine(gscoped_ptr<llvm::ExecutionEngine> engine);

  // Warning: the projection schema should have write-defaults for columns
  // not present in the base. There was no check for default write
  // columns during this class' initialization. Calling this method without
  // specified defaults yields undefined behavior.
  template<class ContiguousRowType>
  Status ProjectRowForWrite(const ContiguousRowType& src_row,
                            RowBlockRow* dst_row,
                            Arena* dst_arena) const {
    DCHECK_SCHEMA_EQ(*base_schema_, *src_row.schema());
    DCHECK_SCHEMA_EQ(*projection_, *dst_row->schema());
    DCHECK(write_f_ != NULL)
      << "Promise to compile write function not fullfilled by ModuleBuilder";
    write_f_(src_row.row_data(), dst_row, dst_arena);
    return Status::OK(); // TODO use acutal error checking
  }

  bool is_identity() const { return is_identity_; }
  const Schema* projection() const { return projection_; }
  const Schema* base_schema() const { return base_schema_; }

 private:
  gscoped_ptr<llvm::ExecutionEngine> engine_;

  // Initially set to null, the function pointers are initialized once
  // the functions are jitted.
  typedef void(*ProjectionFunction)(const uint8_t*, RowBlockRow*, Arena*);
  ProjectionFunction read_f_;
  ProjectionFunction write_f_;

  const Schema* const base_schema_;
  const Schema* const projection_;
  const bool is_identity_;

  DISALLOW_COPY_AND_ASSIGN(RowProjector);
};

extern std::ostream& operator<<(std::ostream& o, const RowProjector& rp);

} // namespace codegen
} // namespace kudu

#endif
