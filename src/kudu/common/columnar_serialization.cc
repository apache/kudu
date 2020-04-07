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

#include "kudu/common/columnar_serialization.h"

#include <emmintrin.h>
#include <immintrin.h>

#include <cstring>
#include <ostream>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "kudu/common/columnblock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/common/zp7.h"
#include "kudu/gutil/cpu.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/fastmem.h"
#include "kudu/util/alignment.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/slice.h"

using std::vector;

namespace kudu {

namespace {

// Utility to write variable bit-length values to a pre-allocated buffer.
//
// This is similar to the BitWriter class in util/bit-stream-utils.h except that
// the other implementation manages growing an underlying 'faststring' rather
// than writing to existing memory.
struct BitWriter {

  // Start writing data to 'dst', but skip over the first 'skip_initial_bits'
  // bits.
  BitWriter(uint8_t* dst, int skip_initial_bits) : dst_(dst) {
    DCHECK_GE(skip_initial_bits, 0);
    dst_ += skip_initial_bits / 8;

    // The "skip" may place us in the middle of a byte. To simplify this,
    // we just position ourselves at the start of that byte and buffer the
    // pre-existing bits, thus positioning ourselves at the right offset.
    int preexisting_bits = skip_initial_bits % 8;
    uint8_t preexisting_val = *dst_ & ((1 << preexisting_bits) - 1);
    Put(preexisting_val, preexisting_bits);
  }

  ~BitWriter() {
    CHECK(flushed_) << "must flush";
  }

  void Put(uint64_t v, int num_bits) {
    DCHECK(!flushed_);
    DCHECK_LE(num_bits, 64);
    buffered_values_ |= v << num_buffered_bits_;
    num_buffered_bits_ += num_bits;

    if (PREDICT_FALSE(num_buffered_bits_ >= 64)) {
      memcpy(dst_, &buffered_values_, 8);
      buffered_values_ = 0;
      num_buffered_bits_ -= 64;
      int shift = num_bits - num_buffered_bits_;
      buffered_values_ = (shift >= 64) ? 0 : v >> shift;
      dst_ += 8;
    }
    DCHECK_LT(num_buffered_bits_, 64);
  }

  void Flush() {
    CHECK(!flushed_) << "must only flush once";
    while (num_buffered_bits_ > 0) {
      *dst_++ = buffered_values_ & 0xff;
      buffered_values_ >>= 8;
      num_buffered_bits_ -= 8;
    }
    flushed_ = true;
  }

  uint8_t* dst_;

  // Accumulated bits that haven't been flushed to the destination buffer yet.
  uint64_t buffered_values_ = 0;

  // The number of accumulated bits in buffered_values_.
  int num_buffered_bits_ = 0;

  bool flushed_ = false;
};

} // anonymous namespace

////////////////////////////////////////////////////////////
// ZeroNullValues
////////////////////////////////////////////////////////////

namespace internal {

namespace {
// Implementation of ZeroNullValues, specialized for a particular type size.
template<int sizeof_type>
ATTRIBUTE_NOINLINE
void ZeroNullValuesImpl(int dst_idx,
                        int n_rows,
                        uint8_t* __restrict__ dst_values_buf,
                        uint8_t* __restrict__ non_null_bitmap) {
  int aligned_dst_idx = KUDU_ALIGN_DOWN(dst_idx, 8);
  int aligned_n_sel = n_rows + (dst_idx - aligned_dst_idx);

  uint8_t* aligned_values_base = dst_values_buf + aligned_dst_idx * sizeof_type;

  // TODO(todd): this code path benefits from the BMI instruction set. We should
  // compile it twice, once with BMI supported.
  ForEachUnsetBit(non_null_bitmap + aligned_dst_idx/8,
                  aligned_n_sel,
                  [&](int position) {
                    // The position here is relative to our aligned bitmap.
                    memset(aligned_values_base + position * sizeof_type, 0, sizeof_type);
                  });
}

} // anonymous namespace

// Zero out any values in 'dst_values_buf' which are indicated as null in 'non_null_bitmap'.
//
// 'n_rows' cells are processed, starting at index 'dst_idx' within the buffers.
// 'sizeof_type' indicates the size of each cell in bytes.
//
// NOTE: this assumes that dst_values_buf and non_null_bitmap are valid for the full range
// of indices [0, dst_idx + n_rows). The implementation may redundantly re-zero cells
// at indexes less than dst_idx.
void ZeroNullValues(int sizeof_type,
                    int dst_idx,
                    int n_rows,
                    uint8_t* dst_values_buf,
                    uint8_t* dst_non_null_bitmap) {
  // Delegate to specialized implementations for each type size.
  // This changes variable-length memsets into inlinable single instructions.
  switch (sizeof_type) {
#define CASE(size)                                                      \
    case size:                                                          \
      ZeroNullValuesImpl<size>(dst_idx, n_rows, dst_values_buf, dst_non_null_bitmap); \
      break;
    CASE(1);
    CASE(2);
    CASE(4);
    CASE(8);
    CASE(16);
#undef CASE
    default:
      LOG(FATAL) << "bad size: " << sizeof_type;
  }
}


////////////////////////////////////////////////////////////
// CopyNonNullBitmap
////////////////////////////////////////////////////////////

namespace {
template<class PextImpl>
void CopyNonNullBitmapImpl(
    const uint8_t* __restrict__ non_null_bitmap,
    const uint8_t* __restrict__ sel_bitmap,
    int dst_idx,
    int n_rows,
    uint8_t* __restrict__ dst_non_null_bitmap) {
  BitWriter bw(dst_non_null_bitmap, dst_idx);

  int num_64bit_words = n_rows / 64;
  for (int i = 0; i < num_64bit_words; i++) {
    uint64_t sel_mask = UnalignedLoad<uint64_t>(sel_bitmap + i * 8);
    int num_bits = __builtin_popcountll(sel_mask);

    uint64_t non_nulls = UnalignedLoad<uint64_t>(non_null_bitmap + i * 8);
    uint64_t extracted = PextImpl::call(non_nulls, sel_mask);
    bw.Put(extracted, num_bits);
  }

  int rem_rows = n_rows % 64;
  non_null_bitmap += num_64bit_words * 8;
  sel_bitmap += num_64bit_words * 8;
  while (rem_rows > 0) {
    uint8_t non_nulls = *non_null_bitmap;
    uint8_t sel_mask = *sel_bitmap;

    uint64_t extracted = PextImpl::call(non_nulls, sel_mask);
    int num_bits = __builtin_popcountl(sel_mask);
    bw.Put(extracted, num_bits);

    sel_bitmap++;
    non_null_bitmap++;
    rem_rows -= 8;
  }
  bw.Flush();
}

struct PextZp7Clmul {
  inline static uint64_t call(uint64_t val, uint64_t mask) {
    return zp7_pext_64_clmul(val, mask);
  }
};
struct PextZp7Simple {
  inline static uint64_t call(uint64_t val, uint64_t mask) {
    return zp7_pext_64_simple(val, mask);
  }
};

#ifdef __x86_64__
struct PextInstruction {
  __attribute__((target("bmi2")))
  inline static uint64_t call(uint64_t val, uint64_t mask) {
#if !defined(__clang__) && defined(__GNUC__) && __GNUC__ < 5
    // GCC <5 doesn't properly handle the _pext_u64 intrinsic inside
    // a function with a specified target attribute. So, use inline
    // assembly instead.
    //
    // Though this assembly works on clang as well, it has two downsides:
    // - the "multiple constraint" 'rm' for 'mask' is supposed to indicate to
    //   the compiler that the mask could either be in memory or in a register,
    //   but clang doesn't support this, and will always spill it to memory
    //   even if the value is already in a register. That results in an extra couple
    //   cycles.
    // - using the intrinsic means that clang optimization passes have some opportunity
    //   to better understand what's going on and make appropriate downstream optimizations.
    uint64_t dst;
    asm ("pextq %[mask], %[val], %[dst]"
        : [dst] "=r" (dst)
        : [val] "r" (val),
          [mask] "rm" (mask));
    return dst;
#else
    return _pext_u64(val, mask);
#endif // compiler check
  }
};
// Explicit instantiation of the template for the PextInstruction case
// allows us to apply the 'bmi2' target attribute for just this version.
template
__attribute__((target("bmi2")))
void CopyNonNullBitmapImpl<PextInstruction>(
    const uint8_t* __restrict__ non_null_bitmap,
    const uint8_t* __restrict__ sel_bitmap,
    int dst_idx,
    int n_rows,
    uint8_t* __restrict__ dst_non_null_bitmap);
#endif // __x86_64__

} // anonymous namespace

// Return a prioritized list of methods that can be used for extracting bits from the non-null
// bitmap.
vector<PextMethod> GetAvailablePextMethods() {
  vector<PextMethod> ret;
#ifdef __x86_64__
  base::CPU cpu;
  // Even though recent AMD chips support pext, it's extremely slow,
  // so only use BMI2 on Intel, and instead use the 'zp7' software
  // implementation on AMD.
  if (cpu.has_bmi2() && cpu.vendor_name() == "GenuineIntel") {
    ret.push_back(PextMethod::kPextInstruction);
  }
  if (cpu.has_pclmulqdq()) {
    ret.push_back(PextMethod::kClmul);
  }
#endif
  ret.push_back(PextMethod::kSimple);
  return ret;
}

PextMethod g_pext_method = GetAvailablePextMethods()[0];

void CopyNonNullBitmap(const uint8_t* non_null_bitmap,
                       const uint8_t* sel_bitmap,
                       int dst_idx,
                       int n_rows,
                       uint8_t* dst_non_null_bitmap) {
  switch (g_pext_method) {
#ifdef __x86_64__
    case PextMethod::kPextInstruction:
      CopyNonNullBitmapImpl<PextInstruction>(
          non_null_bitmap, sel_bitmap, dst_idx, n_rows, dst_non_null_bitmap);
      break;
    case PextMethod::kClmul:
      CopyNonNullBitmapImpl<PextZp7Clmul>(
          non_null_bitmap, sel_bitmap, dst_idx, n_rows, dst_non_null_bitmap);
      break;
#endif
    case PextMethod::kSimple:
      CopyNonNullBitmapImpl<PextZp7Simple>(
          non_null_bitmap,  sel_bitmap, dst_idx, n_rows, dst_non_null_bitmap);
      break;
    default:
      __builtin_unreachable();
  }
}

////////////////////////////////////////////////////////////
// CopySelectedRows
////////////////////////////////////////////////////////////

namespace {

const bool kHasAvx2 = base::CPU().has_avx2();

// Return the number of rows copied through an AVX-optimized implementation.
// These implementations leave a "tail" of non-vectorizable rows that get
// handled by the scalar implementation.
template<int sizeof_type>
int CopySelectedRowsAvx(
    const uint16_t* __restrict__ /* sel_rows */,
    int /* n_sel_rows */,
    const uint8_t* __restrict__ /* src_buf */,
    uint8_t* __restrict__ /* dst_buf */) {
  return 0;
}

// Define AVX2-optimized variants where possible.
// These are disabled on GCC4 because it doesn't support per-function
// enabling of intrinsics.
#if __x86_64__ && (defined(__clang__) || (defined(__GNUC__) && __GNUC__ >= 5))
template<>
__attribute__((target("avx2")))
int CopySelectedRowsAvx<4>(
    const uint16_t* __restrict__ sel_rows,
    int n_sel_rows,
    const uint8_t* __restrict__ src_buf,
    uint8_t* __restrict__ dst_buf) {

  static constexpr int sizeof_type = 4;
  static constexpr int ints_per_vector = sizeof(__m256i)/sizeof_type;
  int iters = n_sel_rows / ints_per_vector;
  while (iters--) {
    // Load 8x16-bit indexes from sel_rows, zero-extending them to 8x32-bit integers
    // since the 'gather' instructions don't support 16-bit indexes.
    __m256i indexes = _mm256_cvtepu16_epi32(*reinterpret_cast<const __m128i*>(sel_rows));
    // Gather 8x32-bit elements from src_buf[index*sizeof_type] for each index.
    // We need this cast to compile on some versions of GCC.
    const auto* src_i32 = reinterpret_cast<const int32_t*>(src_buf);
    __m256i elems = _mm256_i32gather_epi32(src_i32, indexes, sizeof_type);
    // Store the 8x32-bit elements into the destination.
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst_buf), elems);
    dst_buf += ints_per_vector * sizeof_type;
    sel_rows += ints_per_vector;
  }
  return KUDU_ALIGN_DOWN(n_sel_rows, ints_per_vector);
}

template<>
__attribute__((target("avx2")))
int CopySelectedRowsAvx<8>(
    const uint16_t* __restrict__ sel_rows,
    int n_sel_rows,
    const uint8_t* __restrict__ src_buf,
    uint8_t* __restrict__ dst_buf) {

  static constexpr int sizeof_type = 8;
  static constexpr int ints_per_vector = sizeof(__m256i)/sizeof_type;
  int iters = n_sel_rows / ints_per_vector;
  while (iters--) {
    // Load 4x16-bit indexes from sel_rows into 'indexes'. This compiles down
    // into a single vpmovzxwd instruction despite looking like four separate loads.
    __m128i indexes = _mm_set_epi32(sel_rows[3],
                                    sel_rows[2],
                                    sel_rows[1],
                                    sel_rows[0]);
    // Load 4x64-bit integers from src_buf[index * sizeof_type] for each index.
    const auto* src_lli = reinterpret_cast<const long long int*>(src_buf); // NOLINT(*)
    __m256i elems = _mm256_i32gather_epi64(src_lli, indexes, sizeof_type);
    // Store the 4x64-bit integers in the destination.
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst_buf), elems);
    dst_buf += ints_per_vector * sizeof_type;
    sel_rows += ints_per_vector;
  }
  return KUDU_ALIGN_DOWN(n_sel_rows, ints_per_vector);
}
#endif

template<int sizeof_type>
ATTRIBUTE_NOINLINE
void CopySelectedRowsImpl(const uint16_t* __restrict__ sel_rows,
                          int n_sel_rows,
                          const uint8_t* __restrict__ src_buf,
                          uint8_t* __restrict__ dst_buf) {
  int rem = n_sel_rows;
  if (kHasAvx2) {
    int copied = CopySelectedRowsAvx<sizeof_type>(sel_rows, n_sel_rows, src_buf, dst_buf);
    rem -= copied;
    dst_buf += copied * sizeof_type;
    sel_rows += copied;
  }

  while (rem--) {
    int idx = *sel_rows++;
    memcpy(dst_buf, src_buf + idx * sizeof_type, sizeof_type);
    dst_buf += sizeof_type;
  }
  // TODO(todd): should we zero out nulls first or otherwise avoid
  // copying them?
}

template<int sizeof_type>
ATTRIBUTE_NOINLINE
void CopySelectedRowsImpl(const vector<uint16_t>& sel_rows,
                          const uint8_t* __restrict__ src_buf,
                          uint8_t* __restrict__ dst_buf) {
  CopySelectedRowsImpl<sizeof_type>(sel_rows.data(), sel_rows.size(), src_buf, dst_buf);
}

} // anonymous namespace

// Copy the selected cells from the column data 'src_buf' into 'dst_buf' as indicated by
// the indices in 'sel_rows'. 'sizeof_type' is the size in bytes of each cell.
void CopySelectedRows(const vector<uint16_t>& sel_rows,
                      int sizeof_type,
                      const uint8_t* __restrict__ src_buf,
                      uint8_t* __restrict__ dst_buf) {
  switch (sizeof_type) {
#define CASE(size)                                            \
    case size:                                                \
      CopySelectedRowsImpl<size>(sel_rows, src_buf, dst_buf); \
      break;
    CASE(1);
    CASE(2);
    CASE(4);
    CASE(8);
    CASE(16);
#undef CASE
    default:
      LOG(FATAL) << "unexpected type size: " << sizeof_type;
  }
}

namespace {

// Specialized division for the known type sizes. Despite having some branching here,
// this is faster than a 'div' instruction which has a 20+ cycle latency.
size_t div_sizeof_type(size_t s, size_t divisor) {
  switch (divisor) {
    case 1: return s;
    case 2: return s/2;
    case 4: return s/4;
    case 8: return s/8;
    case 16: return s/16;
    default: return s/divisor;
  }
}

// Copy the selected primitive cells (and non-null-bitmap bits) from 'cblock' into 'dst'
// according to the given 'sel_rows'.
void CopySelectedCellsFromColumn(const ColumnBlock& cblock,
                                 const SelectedRows& sel_rows,
                                 ColumnarSerializedBatch::Column* dst) {
  DCHECK(cblock.type_info()->physical_type() != BINARY);
  size_t sizeof_type = cblock.type_info()->size();
  int n_sel = sel_rows.num_selected();

  // Number of initial rows in the dst values and null_bitmap.
  DCHECK_EQ(dst->data.size() % sizeof_type, 0);
  size_t initial_rows = div_sizeof_type(dst->data.size(), sizeof_type);
  size_t new_num_rows = initial_rows + n_sel;

  dst->data.resize_with_extra_capacity(sizeof_type * new_num_rows);
  uint8_t* dst_buf = dst->data.data() + sizeof_type * initial_rows;
  const uint8_t* src_buf = cblock.cell_ptr(0);

  if (sel_rows.all_selected()) {
    memcpy(dst_buf, src_buf, sizeof_type * n_sel);
  } else {
    CopySelectedRows(sel_rows.indexes(), sizeof_type, src_buf, dst_buf);
  }

  if (cblock.is_nullable()) {
    DCHECK_EQ(dst->non_null_bitmap->size(), BitmapSize(initial_rows));
    dst->non_null_bitmap->resize_with_extra_capacity(BitmapSize(new_num_rows));
    CopyNonNullBitmap(cblock.non_null_bitmap(),
                      sel_rows.bitmap(),
                      initial_rows, cblock.nrows(),
                      dst->non_null_bitmap->data());
    ZeroNullValues(sizeof_type, initial_rows, n_sel,
        dst->data.data(), dst->non_null_bitmap->data());
  }
}


// For each of the Slices in 'cells_buf', copy the pointed-to data into 'varlen' and
// write the _end_ offset of the copied data into 'offsets_out'. This assumes (and
// DCHECKs) that the _start_ offset of each cell was already previously written by a
// previous invocation of this function.
void CopySlicesAndWriteEndOffsets(const Slice* __restrict__ cells_buf,
                                  const SelectedRows& sel_rows,
                                  uint32_t* __restrict__ offsets_out,
                                  faststring* varlen) {
  const Slice* cell_slices = reinterpret_cast<const Slice*>(cells_buf);
  size_t total_added_size = 0;
  sel_rows.ForEachIndex(
      [&](uint16_t i) {
        total_added_size += cell_slices[i].size();
      });

  // The output array should already have an entry for the start offset
  // of our first cell.
  DCHECK_EQ(offsets_out[-1], varlen->size());

  int old_size = varlen->size();
  varlen->resize_with_extra_capacity(old_size + total_added_size);

  uint8_t* dst_base = varlen->data();
  uint8_t* dst = dst_base + old_size;

  sel_rows.ForEachIndex(
      [&](uint16_t i) {
        const Slice* s = &cell_slices[i];
        if (!s->empty()) {
          strings::memcpy_inlined(dst, s->data(), s->size());
        }
        dst += s->size();
        *offsets_out++ = dst - dst_base;
      });
}

// Copy variable-length cells into 'dst' using an Arrow-style serialization:
// a list of offsets in the 'data' array and the data itself in the 'varlen_data'
// array.
//
// The offset array has a length one greater than the number of cells.
void CopySelectedVarlenCellsFromColumn(const ColumnBlock& cblock,
                                       const SelectedRows& sel_rows,
                                       ColumnarSerializedBatch::Column* dst) {
  using offset_type = uint32_t;
  DCHECK(cblock.type_info()->physical_type() == BINARY);
  int n_sel = sel_rows.num_selected();
  DCHECK_GT(n_sel, 0);

  // If this is the first call, append a '0' entry for the offset of the first string.
  if (dst->data.size() == 0) {
    CHECK_EQ(dst->varlen_data->size(), 0);
    offset_type zero_offset = 0;
    dst->data.append(&zero_offset, sizeof(zero_offset));
  }

  // Number of initial rows in the dst values and null_bitmap.
  DCHECK_EQ(dst->data.size() % sizeof(offset_type), 0);
  size_t initial_offset_count = div_sizeof_type(dst->data.size(), sizeof(offset_type));
  size_t initial_rows = initial_offset_count - 1;
  size_t new_offset_count = initial_offset_count + n_sel;
  size_t new_num_rows = initial_rows + n_sel;

  if (cblock.is_nullable()) {
    DCHECK_EQ(dst->non_null_bitmap->size(), BitmapSize(initial_rows));
    dst->non_null_bitmap->resize_with_extra_capacity(BitmapSize(new_num_rows));
    CopyNonNullBitmap(cblock.non_null_bitmap(),
                      sel_rows.bitmap(),
                      initial_rows, cblock.nrows(),
                      dst->non_null_bitmap->data());
    ZeroNullValues(sizeof(Slice), 0, cblock.nrows(),
                   const_cast<ColumnBlock&>(cblock).data(), cblock.non_null_bitmap());
  }
  dst->data.resize_with_extra_capacity(sizeof(offset_type) * new_offset_count);
  offset_type* dst_offset = reinterpret_cast<offset_type*>(dst->data.data()) + initial_offset_count;
  const Slice* src_slices = reinterpret_cast<const Slice*>(cblock.cell_ptr(0));
  CopySlicesAndWriteEndOffsets(src_slices, sel_rows,
                               dst_offset, boost::get_pointer(dst->varlen_data));
}

} // anonymous namespace
} // namespace internal

int SerializeRowBlockColumnar(
    const RowBlock& block,
    const Schema* projection_schema,
    ColumnarSerializedBatch* out) {
  DCHECK_GT(block.nrows(), 0);
  const Schema* tablet_schema = block.schema();

  if (projection_schema == nullptr) {
    projection_schema = tablet_schema;
  }

  // Initialize buffers for the columns.
  // TODO(todd) don't pre-size these to 1MB per column -- quite
  // expensive if there are a lot of columns!
  if (out->columns.size() != projection_schema->num_columns()) {
    CHECK_EQ(out->columns.size(), 0);
    out->columns.reserve(projection_schema->num_columns());
    for (const auto& col : projection_schema->columns()) {
      out->columns.emplace_back();
      out->columns.back().data.reserve(1024 * 1024);
      if (col.type_info()->physical_type() == BINARY) {
        out->columns.back().varlen_data.emplace();
      }
      if (col.is_nullable()) {
        out->columns.back().non_null_bitmap.emplace();
      }
    }
  }

  SelectedRows sel = block.selection_vector()->GetSelectedRows();
  if (sel.num_selected() == 0) {
    return 0;
  }

  int col_idx = 0;
  for (const auto& col : projection_schema->columns()) {
    int t_schema_idx = tablet_schema->find_column(col.name());
    CHECK_NE(t_schema_idx, -1);
    const ColumnBlock& column_block = block.column_block(t_schema_idx);

    if (column_block.type_info()->physical_type() == BINARY) {
      internal::CopySelectedVarlenCellsFromColumn(
          column_block,
          sel,
          &out->columns[col_idx]);
    } else {
      internal::CopySelectedCellsFromColumn(
          column_block,
          sel,
          &out->columns[col_idx]);
    }
    col_idx++;
  }

  return sel.num_selected();
}


} // namespace kudu
