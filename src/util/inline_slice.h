// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_UTIL_INLINE_SLICE_H
#define KUDU_UTIL_INLINE_SLICE_H

#include <boost/static_assert.hpp>

#include "gutil/casts.h"
#include "util/memory/arena.h"

namespace kudu {

#if __BYTE_ORDER != __LITTLE_ENDIAN
#error This needs to be ported for big endian
#endif

// Class which represents short strings inline, and stores longer ones
// by instead storing a pointer.
//
// Internal format:
// The buffer must be at least as large as a pointer (eg 8 bytes for 64-bit).
// Let ptr = bit-casting the first 8 bytes as a pointer:
// If buf_[0] < 0xff:
//   buf_[0] == length of stored data
//   buf_[1..1 + buf_[0]] == inline data
// If buf_[0] == 0xff:
//   buf_[1..sizeof(uint8_t *)] == pointer to indirect data, minus the MSB.
//   buf_[sizeof(uint8_t *)..] == inline prefix of stored key
// 
// The indirect data which is pointed to is stored as a 4 byte length followed by
// the actual data.
//
// This class relies on the fact that the most significant bit of any x86 pointer is
// 0 (i.e pointers only use the bottom 48 bits)
template<size_t STORAGE_SIZE>
class InlineSlice {
  BOOST_STATIC_ASSERT(STORAGE_SIZE >= sizeof(uint8_t *));
  BOOST_STATIC_ASSERT(STORAGE_SIZE <= 256);
public:
  InlineSlice() {
  }

  const Slice as_slice() const {
    if (is_indirect()) {
      const uint8_t *indir_data = indirect_ptr();
      uint32_t len = *reinterpret_cast<const uint32_t *>(indir_data);
      indir_data += sizeof(uint32_t);
      return Slice(indir_data, (size_t)len);
    } else {
      uint8_t len = buf_[0];
      DCHECK_LE(len, STORAGE_SIZE - 1);
      return Slice(&buf_[1], len);
    }
  }

  template<class ArenaType>
  void set(const Slice &src, ArenaType *alloc_arena) {
    set(src.data(), src.size(), alloc_arena);
  }

  template<class ArenaType>
  void set(const uint8_t *src, size_t len,
           ArenaType *alloc_arena) {
    if (len <= kMaxInlineData) {
      buf_[0] = len;
      memcpy(&buf_[1], src, len);
    } else {

      if (STORAGE_SIZE > sizeof(uint8_t *) + 1) {
        memcpy(&buf_[sizeof(uint8_t *)], src, prefix_len());
      }

      // TODO: if already indirect and the current storage has enough space, just reuse that.
      void *in_arena = CHECK_NOTNULL(alloc_arena->AllocateBytes(len + sizeof(uint32_t)));

      set_ptr(in_arena);
      *reinterpret_cast<uint32_t *>(in_arena) = len;
      memcpy(reinterpret_cast<uint8_t *>(in_arena) + sizeof(uint32_t), src, len);
    }
  }

private:
  bool is_indirect() const {
    return buf_[0] == 0xff;
  }

  size_t prefix_len() const {
    ssize_t ret = STORAGE_SIZE - 1 - sizeof(void *);
    DCHECK_GE(ret, 0);
    return ret;
  }

  // Return the indirect pointer which is currently stored.
  // In debug builds, this will fire an assertion failure if called on a non-indirect
  // object instance.
  const uint8_t *indirect_ptr() const {
    uint8_t indicator_byte = buf_[0];
    DCHECK_EQ(indicator_byte, 0xff);

    uintptr_t ptr_val = *reinterpret_cast<const uintptr_t *>(buf_);
    // Remove the 0xff indicator by shifting back to the right
    ptr_val >>= 8;
    return reinterpret_cast<const uint8_t *>(ptr_val);
  }

  // Set the internal storage to be an indirect pointer to the given
  // address.
  void set_ptr(void *ptr) {
    uintptr_t ptr_int = reinterpret_cast<uintptr_t>(ptr);
    CHECK_EQ(ptr_int >> (kPointerBitWidth - 8), 0) <<
      "bad pointer (should have 0x00 MSB): " << ptr;
    ptr_int <<= 8;
    ptr_int |= 0xff;
    memcpy(&buf_[0], &ptr_int, sizeof(ptr_int));
  }

  enum {
    kPointerBitWidth = sizeof(uint32_t *) * 8,
    kPointerMask = ~((uintptr_t)0xff),
    kMaxInlineData = STORAGE_SIZE - 1
  };

  uint8_t buf_[STORAGE_SIZE];

} PACKED;

}

#endif
