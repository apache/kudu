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
  //    free_space_pos: position to the first unallocated area
  //    allocated_size: total size of this structure, as passed into the
  //                    constructor
  // info_type[width]: variable size based on constructor parameter
  //    pos: pointer of the item in slot N, relative to the start of storage
  //    len: length of the item in slot N
  // <item data>

  static constexpr int half_num_bits = (8 * sizeof(InfoType)) / 2;
  static constexpr int max_halfinfo = (1 << half_num_bits) - 1;

  struct header_type {
    unsigned int free_space_pos : half_num_bits;
    unsigned int allocated_size : half_num_bits;
  };

  struct info_type {
    unsigned int pos : half_num_bits;
    unsigned int len : half_num_bits;
  };

public:

  StringBag(int width, size_t allocated_size) {

    // The position where the first element will get
    // inserted.
    size_t firstpos = overhead(width);
    CHECK_GE(allocated_size, firstpos)
      << "Allocated size " << allocated_size << " not big enough "
      << "to contain " << width << " info elements of size "
      << sizeof(info_type);
    CHECK_LE(allocated_size, (size_t) max_halfinfo)
      << "Info type of size " << sizeof(InfoType) << " not big "
      << "enough to address " << allocated_size << "bytes";

    header_.free_space_pos = firstpos;
    header_.allocated_size = allocated_size;

    memset(info_, 0, sizeof(info_type) * width);
  }

  static size_t overhead(int width) {
    return sizeof(StringBag<InfoType>) + width * sizeof(info_type);
  }

  Slice get(int p) const {
    info_type info = info_[p];
    return Slice(s_ + info.pos, info.len);
  }

  bool assign(int p, const char *s, int len) {
    int old_len = (info_ + p)->len;

    int pos;
    if (old_len >= len) {
      // Slot has space for the new data, so copy the new string into
      // the same spot.
	    pos = (info_ + p)->pos;
    } else if (header_.free_space_pos + len + sizeof(*this) <=
               (size_t) header_.allocated_size) {
	    pos = header_.free_space_pos;
	    header_.free_space_pos += len;
    } else {
      // No space to assign into this slot.
	    return false;
    }
    memcpy(s_ + pos, s, len);
    info_[p] = make_info(pos, len);
    return true;
  }

  bool assign(int p, const Slice &s) {
    return assign(p, s.data(), s.size());
  }

  std::string ToString(int width, const char *prefix="", int indent=0) {
    std::string ret;
    ret.reserve(header_.allocated_size * 5 / 4);

    StringAppendF(
      &ret, "%s%*s%p (%d:)%d:%d [%d]...\n", prefix, indent, "",
      this, (int) overhead(width),
      header_.free_space_pos,
      header_.allocated_size,
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

  union {
    struct {
	    header_type header_;
	    info_type info_[];
    };
    char s_[];
  };

  static info_type make_info(int pos, int len) {
    info_type ret;
    ret.pos = pos;
    ret.len = len;
    return ret;
  }

};

} // namespace kudu

#endif
