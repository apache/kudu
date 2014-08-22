// Copyright 2014 Cloudera inc.

#ifndef KUDU_CODEGEN_CODE_CACHE_H
#define KUDU_CODEGEN_CODE_CACHE_H

#include "kudu/codegen/jit_owner.h"
#include "kudu/codegen/row_projector.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"

namespace kudu {

class Cache;
class Schema;

namespace codegen {

// A code cache is a specialized LRU cache with the following services:
//   1. It supports only one writer at a time, but multiple concurrent
//      readers.
//   2. If its items are taking too much space, it evicts the least-
//      recently-used member of the cache.
//   3. The cache maintains the mapping from (base schema, projection schema)
//      to (JIT-ted functions). This mapping does NOT respect Schema::Equals(),
//      because it does not directly depend on column names and column ids.
//      Please see codegen::RowProjector::ProjectionsCompatible() for a formal
//      equivalence definition.
//
// The cache takes shared ownership of its entry values, the JITPayloads.
//
// The cache owns its own internal keys (the schema pairs), independent of
// any schemas provided as parameters to its function calls.
//
// LRU eviction does not guarantee that a JITPayload is free, only that
// the cache releases its shared ownership of the payload.
class CodeCache {
 public:
  // A JITPayload is the combination of jitted-code-using classes and
  // jitted-code-owning classes. It is a structure which aggregates
  // the two types of objects in order to ensure that executable code
  // lifetime dependencies are satisfied. Furthermore, it is intended
  // to support these lifetime guarantees across multiple threads.
  class JITPayload : public JITCodeOwner {
   public:
    JITPayload(const scoped_refptr<JITCodeOwner>& owner,
               const RowProjector::CodegenFunctions& rp_functions);

    const RowProjector::CodegenFunctions& row_projector_functions() const {
      return rp_functions_;
    }

   private:
    virtual ~JITPayload();

    RowProjector::CodegenFunctions rp_functions_;

    DISALLOW_COPY_AND_ASSIGN(JITPayload);
  };

  // TODO: currently CodeCache is implemented using the Cache in
  // kudu/util/cache.h, which requires some transformation to nongeneric
  // Slice-type keys, void* values, and C-style deleters. Furthermore, Cache
  // provides concurrent write guarantees (thus relies on locks heavily), which
  // is unnecessary for the CodeCache. A potential improvement would be to
  // implement a single-writer multi-reader LRU cache with proper generics.

  // TODO: a potential improvment would be for the cache to monitor its memory
  // consumption explicity and keep its usage under a size limit specified at
  // construction time. In order to do this, the cache would have to inject
  // a custom memory manager into the CodeGenerator's execution engine which
  // intercepts allocation calls and tracks code size.

  // Generates an empty code cache which stores at most 'capacity' JIT payloads.
  // A JIT payload is defined to be the combination of objects which rely on jitted
  // code and the classes which own the jitted code.
  explicit CodeCache(size_t capacity);
  ~CodeCache();

  // This function is NOT thread safe.
  // Adds a new entry ((base, proj)->(generated jit payload)) to cache.
  // Overwrites the previous value if one exists. If insertion
  // results in excess capacity, LRU eviction occurs.
  // Method will fail if the schema pair (base, proj) is not a well-formed
  // projection.
  Status AddEntry(const Schema& base, const Schema& proj,
                  const scoped_refptr<JITPayload>& payload);

  // This function may be called from any thread concurrently with other
  // writes and reads to the cache. Looks up the cache for a (base, proj) pair.
  // Returns a reference to the associated payload, or NULL if no such entry
  // exists in the cache. If (base, proj) are not a well-formed projection,
  // then NULL is returned.
  scoped_refptr<JITPayload> Lookup(const Schema& base, const Schema& proj);

 private:
  gscoped_ptr<Cache> cache_;

  DISALLOW_COPY_AND_ASSIGN(CodeCache);
};

} // namespace codegen
} // namespace kudu

#endif
