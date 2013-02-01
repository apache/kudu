// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_CFILE_SEEK_FLAGS_H
#define KUDU_CFILE_SEEK_FLAGS_H

namespace kudu { namespace cfile {

typedef uint32_t SeekFlags;

enum {
  SEEK_NO_FLAGS = 0,

  // If set, then a seek operation must exactly hit the given key.
  // If this flag is not set, and key is not present, then the seek may end
  // up leaving the iterator in any arbitrary position in the file.
  //
  // If the key is present, then the position of the iterator will point exactly
  // at the key, same as if the flag was not specified.
  SEEK_FORCE_EXACT_MATCH = 1 << 0
};

}
}
#endif
