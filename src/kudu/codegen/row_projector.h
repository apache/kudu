// Copyright 2014 Cloudera inc.

#ifndef KUDU_CODEGEN_ROW_PROJECTOR_H
#define KUDU_CODEGEN_ROW_PROJECTOR_H

#include <iosfwd>
#include <vector>

#include "kudu/common/row.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
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

class JITCodeOwner;
class ModuleBuilder;

// This projector behaves the almost the same way as a tablet/RowProjector except that
// it only supports certain row types, and expects a regular Arena. Furthermore,
// the Reset() public method is unsupported.
//
// See documentation for RowProjector. Any differences in the API will be explained
// in this class.
//
// A RowProjector is built on top of compiled functions made for its pair
// of schemas. If the row projector functions it is given at initialization
// are incompatible with the schemas that are assigned to it, undefined behavior
// will occur.
class RowProjector {
 public:
  typedef kudu::RowProjector::ProjectionIdxMapping ProjectionIdxMapping;

  // Class for containing compiled functions, whose validity is dependent
  // on builder's ExecutionEngine.
  class CodegenFunctions {
   public:
    CodegenFunctions()
      : read_f_(NULL), write_f_(NULL) {}

    // Adds the parameter schema's projection functions to the builder's
    // module.
    // Schemas need to be valid only for duration of this call.
    // 'out' is not initialized until builder->Compile() is called and
    // fulfills its JITPromises.
    static Status Create(const Schema& base_schema, const Schema& projection,
                         ModuleBuilder* builder, CodegenFunctions* out);

    typedef bool(*ProjectionFunction)(const uint8_t*, RowBlockRow*, Arena*);
    ProjectionFunction read() const { return read_f_; }
    ProjectionFunction write() const { return write_f_; }

#ifndef NDEBUG
    const Schema* base_schema() const { return &base_schema_; }
    const Schema* projection() const { return &projection_; }
#endif

   private:
    ProjectionFunction read_f_;
    ProjectionFunction write_f_;
#ifndef NDEBUG
    // In DEBUG mode, retain local copies of the schema to make sure
    // that the projections a CodegenFunctions instance is used for
    // make sense.
    Schema base_schema_;
    Schema projection_;
#endif
  };

  // This method defines what makes (base, projection) schema pairs compatible.
  // In other words, this method can be thought of as the equivalence relation
  // on the set of all well-formed (base, projection) schema pairs that
  // partitions the set into equivalence classes which will have the exact
  // same projection function code.
  //
  // This function can be decomposed as:
  //   ProjectionsCompatible(base1, proj1, base2, proj2) :=
  //     WELLFORMED(base1, proj1) &&
  //     WELLFORMED(base2, proj2) &&
  //     PROJEQUALS(base1, base2) &&
  //     PROJEQUALS(proj1, proj2) &&
  //     MAP(base1, proj1) == MAP(base2, proj2)
  // where WELLFORMED checks that a projection is well-formed (i.e., a
  // kudu::RowProjector can be initialized with the schema pair), PROJEQUAL
  // is a relaxed version of the Schema::Equals() operator that is
  // independent of column names and column IDs, and MAP addresses
  // the actual dependency on column identification - which is the effect
  // that those attributes have on the RowProjector's mapping (i.e., different
  // names and IDs are ok, so long as the mapping is the same).
  //
  // Status::OK corresponds to true in the equivalence relation and other
  // statuses correspond to false, explaining why the projections are
  // incompatible.
  static Status ProjectionsCompatible(const Schema& base1, const Schema& proj1,
                                      const Schema& base2, const Schema& proj2);

  // Requires that both schemas remain valid for the lifetime of this
  // object. Also requires that both schemas are compatible with
  // the schemas used to create the parameter codegen functions, which
  // should already be compiled.
  RowProjector(const Schema* base_schema, const Schema* projection,
               const CodegenFunctions& functions,
               const scoped_refptr<JITCodeOwner>& code);

  ~RowProjector();

  Status Init();

  // Ignores relocations if dst_arena == NULL
  template<class ContiguousRowType>
  Status ProjectRowForRead(const ContiguousRowType& src_row,
                           RowBlockRow* dst_row,
                           Arena* dst_arena) const {
    DCHECK_SCHEMA_EQ(*base_schema(), *src_row.schema());
    DCHECK_SCHEMA_EQ(*projection(), *dst_row->schema());
    CodegenFunctions::ProjectionFunction f = functions_.read();
    if (PREDICT_TRUE(f(src_row.row_data(), dst_row, dst_arena))) {
      return Status::OK();
    }
    return Status::IOError("out of memory copying slice during projection. "
                           "Base schema row: ", base_schema()->DebugRow(src_row));
  }

  // Warning: the projection schema should have write-defaults defined
  // if it has default columns. There was no check for default write
  // columns during this class' initialization.
  // Ignores relocations if dst_arena == NULL
  template<class ContiguousRowType>
  Status ProjectRowForWrite(const ContiguousRowType& src_row,
                            RowBlockRow* dst_row,
                            Arena* dst_arena) const {
    DCHECK_SCHEMA_EQ(*base_schema(), *src_row.schema());
    DCHECK_SCHEMA_EQ(*projection(), *dst_row->schema());
    CodegenFunctions::ProjectionFunction f = functions_.write();
    if (PREDICT_TRUE(f(src_row.row_data(), dst_row, dst_arena))) {
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

  kudu::RowProjector projector_;
  CodegenFunctions functions_;
  scoped_refptr<JITCodeOwner> code_;

  DISALLOW_COPY_AND_ASSIGN(RowProjector);
};

extern std::ostream& operator<<(std::ostream& o, const RowProjector& rp);

} // namespace codegen
} // namespace kudu

#endif
