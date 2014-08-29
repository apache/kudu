// Copyright 2014 Cloudera inc.

#include "kudu/codegen/jit_schema_pair.h"

#include <boost/foreach.hpp>
#include "kudu/codegen/llvm_include.h"
#include <llvm/ExecutionEngine/ExecutionEngine.h>

#include "kudu/codegen/row_projector.h"
#include "kudu/common/schema.h"
#include "kudu/common/row.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/slice.h"

using llvm::ExecutionEngine;

namespace kudu {
namespace codegen {

JITSchemaPair::JITSchemaPair(gscoped_ptr<ExecutionEngine> engine,
                             const RowProjector::CodegenFunctions& rp_functions) :
  JITCodeOwner(engine.Pass()), rp_functions_(rp_functions) {}

JITSchemaPair::~JITSchemaPair() {}

namespace {
// Convenience method which appends to a faststring
template<typename T>
void AddNext(faststring* fs, const T& val) {
  fs->append(&val, sizeof(T));
}
} // anonymous namespace

// Allocates space for and generates a key for a pair of schemas. The key
// is unique according to the criteria defined in the CodeCache class'
// block comment. In order to achieve this, we encode the schemas into
// a contiguous array of bytes as follows, in sequence.
//
// (8 bytes) number, as unsigned long, of base columns
// (5 bytes each) base column types, in order
//   4 bytes for enum type
//   1 byte for nullability
// (8 bytes) number, as unsigned long, of projection columns
// (5 bytes each) projection column types, in order
//   4 bytes for enum type
//   1 byte for nullablility
// (8 bytes) number, as unsigned long, of base projection mappings
// (16 bytes each) base projection mappings, in order
// (24 bytes each) default projection columns, in order
//   8 bytes for the index
//   8 bytes for the read default
//   8 bytes for the write default
//
// This could be made more efficient by removing unnecessary information
// such as the top bits for many numbers, and using a thread-local buffer
// (the code cache copies its own key anyway).
//
// Writes to 'out' upon success.
Status JITSchemaPair::EncodeKey(const Schema& base, const Schema& proj,
                                faststring* out) {
  kudu::RowProjector projector(&base, &proj);
  RETURN_NOT_OK(projector.Init());

  AddNext(out, base.num_columns());
  BOOST_FOREACH(const ColumnSchema& col, base.columns()) {
    AddNext(out, col.type_info()->type());
    AddNext(out, col.is_nullable());
  }
  AddNext(out, proj.num_columns());
  BOOST_FOREACH(const ColumnSchema& col, proj.columns()) {
    AddNext(out, col.type_info()->type());
    AddNext(out, col.is_nullable());
  }
  AddNext(out, projector.base_cols_mapping().size());
  BOOST_FOREACH(const kudu::RowProjector::ProjectionIdxMapping& map,
                projector.base_cols_mapping()) {
    AddNext(out, map);
  }
  BOOST_FOREACH(size_t dfl_idx, projector.projection_defaults()) {
    const ColumnSchema& col = proj.column(dfl_idx);
    AddNext(out, dfl_idx);
    AddNext(out, col.read_default_value());
    AddNext(out, col.write_default_value());
  }

  return Status::OK();
}

namespace {

struct DefaultEquals {
  template<class T>
  bool operator()(const T& t1, const T& t2) { return t1 == t2; }
};

struct ColumnSchemaEqualsType {
  bool operator()(const ColumnSchema& s1, const ColumnSchema& s2) {
    return s1.EqualsType(s2);
  }
};

template<class T, class Equals>
bool ContainerEquals(const T& t1, const T& t2, const Equals& equals) {
  if (t1.size() != t2.size()) return false;
  if (!std::equal(t1.begin(), t1.end(), t2.begin(), equals)) return false;
  return true;
}

template<class T>
bool ContainerEquals(const T& t1, const T& t2) {
  return ContainerEquals(t1, t2, DefaultEquals());
}

} // anonymous namespace

// This presumes the order of the row projectors' mappings is the same, which
// they will be because they were constructed in the same way.
Status JITSchemaPair::ProjectionsCompatible(const Schema& base1,
                                            const Schema& proj1,
                                            const Schema& base2,
                                            const Schema& proj2) {
  kudu::RowProjector rp1(&base1, &proj1), rp2(&base2, &proj2);
  RETURN_NOT_OK_PREPEND(rp1.Init(), "(base1, proj1) projection "
                        "schema pair not well formed: ");
  RETURN_NOT_OK_PREPEND(rp2.Init(), "(base2, proj2) projection "
                        "schema pair not well formed: ");

  if (!ContainerEquals(base1.columns(), base2.columns(),
                       ColumnSchemaEqualsType())) {
    return Status::IllegalState("base schema types unequal");
  }
  if (!ContainerEquals(proj1.columns(), proj2.columns(),
                       ColumnSchemaEqualsType())) {
    return Status::IllegalState("projection schema types unequal");
  }

  if (!ContainerEquals(rp1.base_cols_mapping(), rp2.base_cols_mapping())) {
    return Status::IllegalState("base column mappings do not match");
  }
  if (!ContainerEquals(rp1.adapter_cols_mapping(), rp2.adapter_cols_mapping())) {
    return Status::IllegalState("adapter column mappings do not match");
  }
  if (!ContainerEquals(rp1.projection_defaults(), rp2.projection_defaults())) {
    return Status::IllegalState("projection default indices do not match");
  }

  return Status::OK();
}

} // namespace codegen
} // namespace kudu
