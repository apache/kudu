// Copyright 2014 Cloudera inc.

// This file contains all of the functions that must be precompiled
// to an LLVM IR format (note: not bitcode to preserve function
// names for retrieval later).
//
// Note namespace scope is just for convenient symbol resolution.
// To preserve function names, extern "C" linkage is used, so these
// functions (1) must not be duplicated in any of the above headers
// and (2) do not belong to namespace kudu.
//
// NOTE: This file may rely on external definitions from any part of Kudu
// because the code generator will resolve external symbols at load time.
// However, the code generator relies on the fact that our Kudu binaries
// are built with unstripped visible symbols, so this style of code generation
// cannot be used in builds with settings that conflict with the required
// visibility (e.g., the client library).

#include <cstdlib>
#include <cstring>

#include "kudu/common/rowblock.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/memory/arena.h"

namespace kudu {

static void BasicCopyCell(uint64_t size, uint8_t* src, uint8_t* dst,
                          bool is_string, Arena* arena) {
  // Relocate indirect data
  if (is_string) {
    // TODO: there should be some error checking here to make sure that
    // the relocation was successful. Returning the error value would be
    // preferrable over bringing in glog code in the middle of
    // the projection function.
    if (PREDICT_FALSE(!arena->RelocateSlice(*reinterpret_cast<Slice*>(src),
                                            reinterpret_cast<Slice*>(dst)))) {
      abort();
    }
    return;
  }

  // Copy direct data
  memcpy(dst, src, size);
}

extern "C" {

// Preface all used functions with _Precompiled to avoid the possibility
// of name clashes. Notice all the nontrivial types must be passed as
// void* parameters, otherwise LLVM will complain that the type does not match
// (and it is not possible to consistently extract the llvm::Type* from a
// parsed module which has the same signature as the one that would be passed
// as a parameter for the below functions if the did not use void* types).
//
// Note that:
//   (1) There is no void* type in LLVM, instead i8* is used.
//   (2) The functions below are all prefixed with _Precompiled to avoid
//       any potential naming conflicts.


// declare void @_PrecompiledCopyCellToRowBlock(
//   i64 size, i8* src, RowBlockRow* dst, i64 col, i1 is_string, Arena* arena)
//
//   Performs the same function as CopyCell, copying size bytes of the
//   cell pointed to by src to the cell of column col in the row pointed
//   to by dst, copying indirect data to the parameter arena if is_string
//   is true. Will hard crash if insufficient memory is available for
//   relocation. Copies size bytes directly from the src cell.
void _PrecompiledCopyCellToRowBlock(uint64_t size, uint8_t* src, void* dst,
                                    uint64_t col, bool is_string, void* arena) {
  uint8_t* dst_cell = static_cast<RowBlockRow*>(dst)->mutable_cell_ptr(col);
  BasicCopyCell(size, src, dst_cell, is_string, static_cast<Arena*>(arena));
}

// declare void @_PrecompiledCopyCellToRowBlockNullable(
//   i64 size, i8* src, RowBlockRow* dst, i64 col, i1 is_string, Arena* arena,
//   i8* src_bitmap, i64 bitmap_idx)
//
//   Performs the same function as _PrecompiledCopyCellToRowBlock but for nullable
//   columns. Checks the parameter bitmap at the specified index and updates
//   The row's bitmap accordingly. Then goes on to copy the cell over if it
//   is not null
void _PrecompiledCopyCellToRowBlockNullable(
  uint64_t size, uint8_t* src, void* dst, uint64_t col, bool is_string,
  void* arena, uint8_t* src_bitmap, uint64_t bitmap_idx) {
  // Using this method implies the nullablity of the column.
  // Write whether the column is nullable to the RowBlock's ColumnBlock's bitmap
  bool is_null = BitmapTest(src_bitmap, bitmap_idx);
  static_cast<RowBlockRow*>(dst)->cell(col).set_null(is_null);
  // No more copies necessary if null
  if (is_null) return;
  _PrecompiledCopyCellToRowBlock(size, src, dst, col, is_string, arena);
}

} // extern "C"
} // namespace kudu
