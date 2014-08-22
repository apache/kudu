// Copyright 2014 Cloudera inc.

#include "kudu/codegen/code_cache.h"

#include <boost/foreach.hpp>

#include "kudu/codegen/jit_owner.h"
#include "kudu/codegen/row_projector.h"
#include "kudu/common/schema.h"
#include "kudu/common/row.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/cache.h"
#include "kudu/util/faststring.h"
#include "kudu/util/slice.h"

namespace kudu {
namespace codegen {

CodeCache::JITPayload::JITPayload(const scoped_refptr<JITCodeOwner>& owner,
                                  const RowProjector::CodegenFunctions& rp_functions) :
  rp_functions_(rp_functions) {
  Reset(owner.get());
}

CodeCache::JITPayload::~JITPayload() {}

CodeCache::CodeCache(size_t capacity)
  : cache_(NewLRUCache(capacity)) {}

CodeCache::~CodeCache() {}

namespace {

// Convenience method which appends to a faststring
template<typename T>
void AddNext(faststring* fs, const T& val) {
  fs->append(&val, sizeof(T));
}

// Allocates space for and generates a key for a pair of schemas. The key
// is unique according to the criteria defined in the CodeCache class'
// block comment. In order to achieve this, we encode the schemas into
// a slices as follows, in sequence.
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
Status SchemaPairToKey(const Schema& base, const Schema& proj,
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

  // Return the slice
  return Status::OK();
}

void CodeCacheDeleter(const Slice& key, void* value) {
  // The Cache from cache.h deletes the memory that it allocates for its
  // own copy of key, but it expects its users to delete their own
  // void* values. To delete, we just release our shared ownership.
  static_cast<CodeCache::JITPayload*>(value)->Release();
}

} // anonymous namespace

Status CodeCache::AddEntry(const Schema& base, const Schema& proj,
                           const scoped_refptr<JITPayload>& payload) {
  // Because Cache only accepts void* values, we store just the JITPayload*
  // and increase its ref count.
  faststring fs;
  RETURN_NOT_OK(SchemaPairToKey(base, proj, &fs));
  Slice key(fs.data(), fs.size());
  payload->AddRef();
  void* value = payload.get();

  // Insert into cache and release the handle (we have a local copy of a refptr)
  Cache::Handle* inserted = cache_->Insert(key, value, 1, CodeCacheDeleter);
  cache_->Release(inserted);
  return Status::OK();
}

scoped_refptr<CodeCache::JITPayload> CodeCache::Lookup(const Schema& base,
                                                       const Schema& proj) {
  // Look up in Cache after generating key, returning NULL if not found.
  faststring fs;
  if (!SchemaPairToKey(base, proj, &fs).ok()) {
    return scoped_refptr<JITPayload>();
  }
  Cache::Handle* found = cache_->Lookup(Slice(fs.data(), fs.size()));
  if (!found) return scoped_refptr<JITPayload>();

  // Retrieve the value
  scoped_refptr<JITPayload> value =
    static_cast<JITPayload*>(cache_->Value(found));

  // No need to hold on to handle after we have our copy
  cache_->Release(found);

  return value;
}

} // namespace codegen
} // namespace kudu
