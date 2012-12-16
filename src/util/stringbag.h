// Copyright (c) 2012, Cloudera, inc.
// Code based on stringbag.hh from MassTree (MIT license),
// replicated below.
//
// The code has been changed to use Kudu's Slice class, coding
// style, etc.

/* Masstree
 * Eddie Kohler, Yandong Mao, Robert Morris
 * Copyright (c) 2012 President and Fellows of Harvard College
 * Copyright (c) 2012 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, subject to the conditions
 * listed in the Masstree LICENSE file. These conditions include: you must
 * preserve this copyright notice, and you cannot mention the copyright
 * holders in advertising related to the Software without their permission.
 * The Software is provided WITHOUT ANY WARRANTY, EXPRESS OR IMPLIED. This
 * notice is a summary of the Masstree LICENSE file; the license in that file is
 * legally binding.
 */
#ifndef KUDU_UTIL_STRINGBAG_H
#define KUDU_UTIL_STRINGBAG_H

#include <glog/logging.h>
#include <string>

#include "gutil/stringprintf.h"
#include "util/slice.h"

namespace kudu {

// A StringBag is an array-like structure which contains a number
// of strings kept in contiguous memory. This class should always
// be constructed on top of existing memory.
//
// The template parameter InfoType should be an integer type. This
// type will be split into top and bottom halves. One half should
// must be big enough to store the number of items in the bag,
// and the other must be big enough to store the maximum item length.
template <typename InfoType>
class StringBag {
  // The internal layout of the data area is as follows:
  //
  // header_type main_:
  //    free_space_pos: position to the first unallocated area,
  //                    relative to 's_'
  //    data_size:      size of the data hanging off the end of this
  //                    structure, pointed to by 's_'.
  //                    This size counts the size for both the info_type
  //                    array and the metadata.
  // info_type[width]: variable size based on constructor parameter
  //    pos: pointer of the item in slot N, relative to 's_'
  //    len: length of the item in slot N
  // <item data>

  static constexpr int half_num_bits = (8 * sizeof(InfoType)) / 2;
  static constexpr int max_halfinfo = (1 << half_num_bits) - 1;

  struct header_type {
    unsigned int free_space_pos : half_num_bits;
    unsigned int data_size : half_num_bits;
  } PACKED;

  struct info_type {
    unsigned int pos : half_num_bits;
    unsigned int len : half_num_bits;
  } PACKED;

public:

  StringBag(int width, size_t allocated_size) {
    #ifndef NDEBUG
    debug_width_ = width;
    #endif

    // The position where the first element will get
    // inserted, relative to _s
    size_t firstpos = width * sizeof(info_type);

    CHECK_GE(allocated_size, firstpos + sizeof(*this))
      << "Allocated size " << allocated_size << " not big enough "
      << "to contain " << width << " info elements of size "
      << sizeof(info_type);

    CHECK_LE(allocated_size, (size_t) max_halfinfo)
      << "Info type of size " << sizeof(InfoType) << " not big "
      << "enough to address " << allocated_size << "bytes";

    header_.free_space_pos = firstpos;
    header_.data_size = allocated_size - sizeof(*this);

    memset(info_, 0, sizeof(info_type) * width);
  }

  Slice Get(int p) const {
    CheckWidth(p);

    info_type info = *(info_ + p);
    return Slice(s_ + info.pos, info.len);
  }

  bool Assign(int p, const char *s, int len) {
    CheckWidth(p);

    info_type *dst_info = info_ + p;
    int old_len = dst_info->len;
    if (old_len >= len) {
      // Slot has space for the new data, so copy the new string into
      // the same spot.
      dst_info->len = len;
    } else if (!AllocateSpace(len, dst_info)) {
      // No space for this assignment
      return false;
    }

    memcpy(s_ + dst_info->pos, s, len);
    return true;
  }

  bool Assign(int p, const Slice &s) {
    return Assign(p, s.data(), s.size());
  }

  // Insert a new data item into slot numbered 'slot',
  // shifting all data items higher than it to the right.
  // This only affects the pointers -- the actual data storage
  // is not affected.
  //
  // num_valid_slots is the total number of elements in the
  // bag prior to the insertion operation -- it may be less
  // than the allocated 'width' to achieve faster performance
  // when the bag is not full.
  // This value must be strictly less than the allocated width:
  // if it is equal to width, then there is no room to insert
  // further data, and this will likely corrupt the first element.
  //
  //
  // Returns false if there is no more space for data available
  // in the bag.
  bool Insert(int slot, int num_valid_slots, const Slice &s) {
    DCHECK_LE(slot, num_valid_slots);
    #ifndef NDEBUG
    DCHECK_LT(num_valid_slots, debug_width_);
    #endif

    // First, try to allocate space for the new slice, and
    // copy it in to our storage.
    info_type new_info;
    if (!AllocateSpace(s.size(), &new_info)) {
      // No space
      return false;
    }

    // Copy the actual data
    memcpy(s_ + new_info.pos, s.data(), s.size());

    // Shift later 'info' records up to make space for insertion.
    for (int i=num_valid_slots; i >= slot; i--) {
      *(info_ + i + 1) = *(info_ + i);
    }

    // Copy the new info into the free space.
    *(info_ + slot) = new_info;

    return true;
  }

  std::string ToString(int width, const char *prefix="", int indent=0) {
    #ifndef NDEBUG
    DCHECK_EQ(width, debug_width_);
    #endif

    std::string ret;
    ret.reserve(header_.data_size * 5 / 4);

    StringAppendF(
      &ret, "%s%*s%p %d:%d [%d]...\n", prefix, indent, "",
      this,
      header_.free_space_pos,
      header_.data_size,
      max_halfinfo + 1);
    for (int i = 0; i < width; ++i) {
      info_type info = *(info_ + i);
	    if (info.len) {
        StringAppendF(
          &ret, "%s%*s  #%x %d:%d %.*s\n", prefix, indent, "",
          i, info.pos, info.len,
          std::min((int)info.len, 40),
          s_ + info.pos);
      }
    }
    return ret;
  }

private:
  // Return the amount of free space available at the
  // end of the array.
  size_t space_available() const {
    return header_.data_size - header_.free_space_pos;
  }

  bool AllocateSpace(size_t size, info_type *info) {
    if (space_available() >= size) {
      info->pos = header_.free_space_pos;
      info->len = size;
      header_.free_space_pos += size;
      return true;
    } else {
      return false;
    }
  }

  void CheckWidth(int idx) const {
    #ifndef NDEBUG
    CHECK_LT(idx, debug_width_);
    #endif
  }

#ifndef NDEBUG
  size_t debug_width_;
#endif

  header_type header_;

  union {
    info_type info_[];
    char s_[];
  };

  static info_type make_info(int pos, int len) {
    info_type ret;
    ret.pos = pos;
    ret.len = len;
    return ret;
  }
} PACKED;

} // namespace kudu

#endif
